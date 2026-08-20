//! The peer transport: mutually authenticated TLS carrying
//! [`blockd_core::peer`] frames between daemons. The channel
//! contract is at-least-once with drops tolerated (the daemon's retry
//! timers re-drive everything), so this layer is deliberately dumb:
//! sends are fire-and-forget — a dead connection, a full queue, or an
//! unreachable peer just drops the frame; inbound frames that fail
//! verification close the connection and the peer reconnects.
//!
//! Plain TCP remains available for migration-only development fixtures, but
//! the runtime refuses to enable passive durability without TLS.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use blockd_core::layout::{peer_membership_key, peer_membership_prefix};
use blockd_core::peer::encode_peer_routed;
use blockd_core::protocol::{PeerMsg, StoreFault};
use blockd_core::types::HostId;
use blockd_transport::{
    DecodePolicy, ReceiveLimits, ReceiveMetrics,
    receive_loop_while_authorized_with_limits_and_metrics,
    receive_routed_loop_with_limits_and_metrics, write_frame,
};
use prost::Message;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, Error, ServerConfig,
    SignatureScheme,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender, error::TrySendError};
use tokio::time::timeout;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::Instrument as _;

use crate::store::ObjectStore;

/// What the transport does with a verified inbound message.
type Deliver = dyn Fn(HostId, PeerMsg) + Send + Sync;
type MembershipChanged = dyn Fn(Vec<HostId>) + Send + Sync;

pub(crate) enum PeerStartError {
    Listener(std::io::Error),
    Membership(StoreFault),
}

impl From<PeerStartError> for std::io::Error {
    fn from(error: PeerStartError) -> Self {
        match error {
            PeerStartError::Listener(error) => error,
            PeerStartError::Membership(error) => std::io::Error::other(format!(
                "initial peer membership publication failed: {error:?}"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PeerConfig {
    /// Local socket to bind. This may use an unspecified address.
    pub listen: SocketAddr,
    /// Reachable cluster-internal endpoint published for other nodes.
    pub advertise: SocketAddr,
}

#[derive(Clone)]
struct PeerTlsConfig {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
    /// Exact leaf certificate DER → authenticated host identity.
    certificate_identities: PeerIdentities,
    membership_leases: MembershipLeases,
    membership: BucketMembership,
    initial_targets: BTreeMap<HostId, PeerTarget>,
}

#[derive(Clone, Debug, Default)]
struct PeerIdentities(Arc<RwLock<BTreeMap<Vec<u8>, HostId>>>);

impl PeerIdentities {
    fn identity(&self, certificate: &[u8]) -> Option<HostId> {
        self.0
            .read()
            .expect("peer identities lock")
            .get(certificate)
            .copied()
    }

    fn contains(&self, certificate: &[u8]) -> bool {
        self.0
            .read()
            .expect("peer identities lock")
            .contains_key(certificate)
    }

    fn replace(&self, identities: BTreeMap<Vec<u8>, HostId>) {
        *self.0.write().expect("peer identities lock") = identities;
    }
}

#[derive(Clone)]
struct BucketMembership {
    store: Arc<dyn ObjectStore>,
    own_key: String,
    own_record: Arc<Mutex<Vec<u8>>>,
    generation: Arc<Mutex<u64>>,
}

const DEFAULT_PEER_SERVER_NAME: &str = "peer.invalid";
#[cfg(not(test))]
const MEMBERSHIP_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(test)]
const MEMBERSHIP_REFRESH_INTERVAL: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const MAX_MEMBERSHIP_STALENESS: Duration = Duration::from_secs(30);
#[cfg(test)]
const MAX_MEMBERSHIP_STALENESS: Duration = Duration::from_millis(500);
#[cfg(not(test))]
const MEMBERSHIP_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(test)]
const MEMBERSHIP_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const MEMBERSHIP_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const MEMBERSHIP_OPERATION_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const MEMBERSHIP_JOIN_REFRESH_DELAYS: [Duration; 5] = [
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
];
#[cfg(test)]
const MEMBERSHIP_JOIN_REFRESH_DELAYS: [Duration; 5] = [
    Duration::from_millis(10),
    Duration::from_millis(20),
    Duration::from_millis(40),
    Duration::from_millis(80),
    Duration::from_millis(160),
];
const MAX_PEER_CERTIFICATE_BYTES: usize = 16 * 1024;
const MEMBERSHIP_MAGIC: u32 = u32::from_le_bytes(*b"BNOD");
const MEMBERSHIP_FORMAT_VERSION: u32 = 1;
const MAX_MEMBERSHIP_PAYLOAD_BYTES: usize = MAX_PEER_CERTIFICATE_BYTES + 128;
const MAX_MEMBERSHIP_RECORD_BYTES: usize =
    blockd_core::format::FRAME_HEADER + MAX_MEMBERSHIP_PAYLOAD_BYTES;

#[derive(Clone, PartialEq, Message)]
struct MemberRecordWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bool, tag = "2")]
    drained: bool,
    #[prost(oneof = "member_record_wire::Address", tags = "3, 4")]
    address: Option<member_record_wire::Address>,
    #[prost(uint32, tag = "5")]
    port: u32,
    #[prost(uint32, tag = "6")]
    flowinfo: u32,
    #[prost(uint32, tag = "7")]
    scope_id: u32,
    #[prost(bytes = "vec", tag = "8")]
    certificate: Vec<u8>,
}

mod member_record_wire {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Address {
        #[prost(bytes, tag = "3")]
        Ipv4(Vec<u8>),
        #[prost(bytes, tag = "4")]
        Ipv6(Vec<u8>),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MemberRecord {
    endpoint: SocketAddr,
    certificate: Vec<u8>,
    drained: bool,
}

impl MemberRecord {
    fn encode(&self) -> Vec<u8> {
        assert!(
            !self.certificate.is_empty() && self.certificate.len() <= MAX_PEER_CERTIFICATE_BYTES,
            "peer certificate size"
        );
        let (address, port, flowinfo, scope_id) = match self.endpoint {
            SocketAddr::V4(endpoint) => (
                member_record_wire::Address::Ipv4(endpoint.ip().octets().to_vec()),
                endpoint.port(),
                0,
                0,
            ),
            SocketAddr::V6(endpoint) => (
                member_record_wire::Address::Ipv6(endpoint.ip().octets().to_vec()),
                endpoint.port(),
                endpoint.flowinfo(),
                endpoint.scope_id(),
            ),
        };
        let payload = MemberRecordWire {
            version: MEMBERSHIP_FORMAT_VERSION,
            drained: self.drained,
            address: Some(address),
            port: u32::from(port),
            flowinfo,
            scope_id,
            certificate: self.certificate.clone(),
        }
        .encode_to_vec();
        assert!(payload.len() <= MAX_MEMBERSHIP_PAYLOAD_BYTES);
        blockd_core::format::seal_frame(MEMBERSHIP_MAGIC, &payload)
    }

    fn decode(encoded: &[u8]) -> Option<Self> {
        if encoded.len() > MAX_MEMBERSHIP_RECORD_BYTES {
            return None;
        }
        let payload = blockd_core::format::open_frame(MEMBERSHIP_MAGIC, encoded).ok()?;
        if payload.len() > MAX_MEMBERSHIP_PAYLOAD_BYTES {
            return None;
        }
        let wire = MemberRecordWire::decode(payload).ok()?;
        if wire.version != MEMBERSHIP_FORMAT_VERSION
            || wire.encode_to_vec() != payload
            || wire.port == 0
            || wire.port > u32::from(u16::MAX)
            || wire.certificate.is_empty()
            || wire.certificate.len() > MAX_PEER_CERTIFICATE_BYTES
        {
            return None;
        }
        let port = u16::try_from(wire.port).ok()?;
        let endpoint = match wire.address? {
            member_record_wire::Address::Ipv4(address)
                if address.len() == 4 && wire.flowinfo == 0 && wire.scope_id == 0 =>
            {
                SocketAddr::new(
                    std::net::Ipv4Addr::from(<[u8; 4]>::try_from(address.as_slice()).ok()?).into(),
                    port,
                )
            }
            member_record_wire::Address::Ipv6(address) if address.len() == 16 => {
                SocketAddr::V6(std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::from(<[u8; 16]>::try_from(address.as_slice()).ok()?),
                    port,
                    wire.flowinfo,
                    wire.scope_id,
                ))
            }
            _ => return None,
        };
        Some(Self {
            endpoint,
            certificate: wire.certificate,
            drained: wire.drained,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PeerTarget {
    identity: HostId,
    endpoint: SocketAddr,
    /// `None` is reserved for the plaintext integration-test fixture.
    certificate: Option<Vec<u8>>,
    drained: bool,
}

#[derive(Clone, Debug, Default)]
struct MembershipLeases(Arc<Mutex<BTreeMap<HostId, ObservedLease>>>);

#[derive(Clone, Debug)]
struct ObservedLease {
    generation: u64,
    fingerprint: Option<String>,
    renewed_at: Option<Instant>,
    member: MemberRecord,
}

impl PeerTlsConfig {
    /// Generate an ephemeral node identity, publish its public certificate,
    /// and use the object-store directory as the live cluster trust set.
    async fn generate_in_object_store(
        store: Arc<dyn ObjectStore>,
        identity: HostId,
        endpoint: SocketAddr,
    ) -> Result<Self, StoreFault> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(vec![DEFAULT_PEER_SERVER_NAME.to_owned()])
                .expect("generate peer TLS identity");
        let certificate = cert.der().to_vec();
        let private_key = signing_key.serialize_der();
        let member = MemberRecord {
            endpoint,
            certificate: certificate.clone(),
            drained: false,
        };
        let own_key = peer_membership_key(identity);
        let own_record = Arc::new(Mutex::new(member.encode()));
        let initial_record = own_record.lock().expect("own membership lock").clone();
        let generation = Arc::clone(&store)
            .put(own_key.clone(), initial_record)
            .await?;

        let identities = PeerIdentities::default();
        let membership_leases = MembershipLeases::default();
        let initial_targets =
            refresh_bucket_membership(&store, &identities, &membership_leases).await?;
        let algorithms = rustls::crypto::ring::default_provider().signature_verification_algorithms;
        let server_verifier = Arc::new(BucketClientCertVerifier {
            identities: identities.clone(),
            algorithms,
        });
        let client_verifier = Arc::new(BucketServerCertVerifier {
            identities: identities.clone(),
            algorithms,
        });
        let certificate = CertificateDer::from(certificate);
        let key = || PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(private_key.clone()));
        let server = ServerConfig::builder()
            .with_client_cert_verifier(server_verifier)
            .with_single_cert(vec![certificate.clone()], key())
            .expect("server identity");
        let client = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(client_verifier)
            .with_client_auth_cert(vec![certificate], key())
            .expect("client identity");
        Ok(Self {
            server: Arc::new(server),
            client: Arc::new(client),
            certificate_identities: identities,
            membership_leases,
            membership: BucketMembership {
                store,
                own_key,
                own_record,
                generation: Arc::new(Mutex::new(generation)),
            },
            initial_targets,
        })
    }
}

#[derive(Debug)]
struct BucketServerCertVerifier {
    identities: PeerIdentities,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for BucketServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        self.identities
            .contains(end_entity.as_ref())
            .then(ServerCertVerified::assertion)
            .ok_or(Error::InvalidCertificate(CertificateError::UnknownIssuer))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[derive(Debug)]
struct BucketClientCertVerifier {
    identities: PeerIdentities,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for BucketClientCertVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        self.identities
            .contains(end_entity.as_ref())
            .then(ClientCertVerified::assertion)
            .ok_or(Error::InvalidCertificate(CertificateError::UnknownIssuer))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

fn host_from_membership_key(key: &str) -> Option<HostId> {
    let name = key.strip_prefix(&peer_membership_prefix())?;
    let id = name.strip_suffix(".member")?;
    let host = (id.len() == 8)
        .then(|| u32::from_str_radix(id, 16).ok().map(HostId::new))
        .flatten()?;
    (key == peer_membership_key(host)).then_some(host)
}

async fn refresh_bucket_membership(
    store: &Arc<dyn ObjectStore>,
    identities: &PeerIdentities,
    leases: &MembershipLeases,
) -> Result<BTreeMap<HostId, PeerTarget>, StoreFault> {
    let prefix = peer_membership_prefix();
    let listed = Arc::clone(store).list_prefix_versioned(prefix).await?;
    let cached = leases.0.lock().expect("membership leases lock").clone();
    let mut members = BTreeMap::<HostId, (u64, Option<String>, Instant, MemberRecord)>::new();
    for entry in listed {
        let Some(host) = host_from_membership_key(&entry.key) else {
            continue;
        };
        let content_unchanged = cached.get(&host).is_some_and(|lease| {
            lease.generation == entry.generation
                || entry
                    .fingerprint
                    .as_ref()
                    .is_some_and(|fingerprint| lease.fingerprint.as_ref() == Some(fingerprint))
        });
        let member = if content_unchanged {
            let lease = cached.get(&host).expect("cache presence checked");
            lease.member.clone()
        } else {
            let Some((generation, encoded)) = Arc::clone(store)
                .get_range(
                    entry.key,
                    0,
                    u64::try_from(MAX_MEMBERSHIP_RECORD_BYTES + 1)
                        .expect("membership read bound fits u64"),
                )
                .await?
            else {
                continue;
            };
            if generation != entry.generation || encoded.len() > MAX_MEMBERSHIP_RECORD_BYTES {
                continue;
            }
            let Some(member) = MemberRecord::decode(&encoded) else {
                continue;
            };
            member
        };
        members.insert(
            host,
            (entry.generation, entry.fingerprint, Instant::now(), member),
        );
    }
    let now = Instant::now();
    {
        let mut observed = leases.0.lock().expect("membership leases lock");
        observed.retain(|host, _| members.contains_key(host));
        members.retain(|host, (generation, fingerprint, observed_at, member)| {
            let lease = observed.entry(*host).or_insert(ObservedLease {
                generation: *generation,
                fingerprint: fingerprint.clone(),
                renewed_at: None,
                member: member.clone(),
            });
            if lease.generation != *generation {
                lease.renewed_at = Some(*observed_at);
            }
            lease.generation = *generation;
            lease.fingerprint.clone_from(fingerprint);
            lease.member = member.clone();
            lease
                .renewed_at
                .is_some_and(|renewed_at| now.duration_since(renewed_at) < MAX_MEMBERSHIP_STALENESS)
        });
    }
    let mut certificates = BTreeMap::<Vec<u8>, Option<HostId>>::new();
    for (&host, (_, _, _, member)) in &members {
        certificates
            .entry(member.certificate.clone())
            .and_modify(|identity| *identity = None)
            .or_insert(Some(host));
    }
    let certificate_identities = certificates
        .into_iter()
        .filter_map(|(certificate, identity)| identity.map(|identity| (certificate, identity)))
        .collect::<BTreeMap<_, _>>();
    members.retain(|host, (_, _, _, member)| {
        certificate_identities.get(&member.certificate) == Some(host)
    });
    identities.replace(certificate_identities);
    Ok(members
        .into_iter()
        .map(|(host, (_, _, _, member))| {
            (
                host,
                PeerTarget {
                    identity: host,
                    endpoint: member.endpoint,
                    certificate: Some(member.certificate),
                    drained: member.drained,
                },
            )
        })
        .collect())
}

#[derive(Clone)]
struct ClientTls {
    config: Arc<ClientConfig>,
    identities: PeerIdentities,
    expected: HostId,
}

#[derive(Clone)]
struct ServerTls {
    config: Arc<ServerConfig>,
    identities: PeerIdentities,
}

/// Per-peer outbound queue depth. A full queue drops the newest frame —
/// bounded memory beats delivery the protocol never needed guaranteed.
const SEND_QUEUE: usize = 128;
/// Outbound workers are allocated only for peers with current traffic. The
/// object-store roster can be much larger without creating one task per host.
const MAX_OUTBOUND_WORKERS: usize = 256;
/// Global queue budgets prevent many active targets from multiplying the
/// per-worker queue bound into unbounded retained frames.
const MAX_OUTBOUND_QUEUED_MESSAGES: usize = 16 * 1024;
const MAX_OUTBOUND_QUEUED_BYTES: usize = 256 * 1024 * 1024;
const OUTBOUND_WORKER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound how long an outbound connection can retain an old peer certificate.
/// Renewal is prepared in parallel while the established stream keeps serving
/// frames, so rotation never puts a handshake in the next frame's path.
const MAX_CONNECTION_AGE: Duration = Duration::from_mins(5);

/// Large frames contain replica artifacts. Their checksum and owned payload
/// decode are CPU/memory work and must not monopolize the peer I/O thread.
const DECODE_OFFLOAD_THRESHOLD: usize = 1024 * 1024;
const PEER_FRAME_READ_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_INBOUND_CONNECTIONS: usize = 256;
const MAX_INBOUND_CONNECTIONS_PER_PEER: usize = 8;
const MAX_INFLIGHT_PEER_BYTES: usize = 256 * 1024 * 1024;

pub struct PeerNet {
    connections: ConnectionSet,
    /// Frames dropped on the floor (queue full or peer down) — the demo's
    /// visibility into how hard the retry timers are working.
    pub dropped_sends: Arc<AtomicU64>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    authenticated: bool,
    healthy: Arc<AtomicBool>,
    membership_owned: Arc<AtomicBool>,
    listener: tokio::task::AbortHandle,
    failed: Arc<tokio::sync::Notify>,
    overload_rejections: Arc<AtomicU64>,
    receive_metrics: ReceiveMetrics,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PeerResourceMetrics {
    pub overload_rejections: u64,
    pub outbound_worker_rejections: u64,
    pub outbound_queue_rejections: u64,
    pub outbound_active_workers: u64,
    pub outbound_buffered_messages: u64,
    pub outbound_buffered_bytes: u64,
    pub payload_budget_waits: u64,
    pub frame_read_timeouts: u64,
    pub idle_timeouts: u64,
}

struct PeerConnection {
    target: PeerTarget,
    worker: Option<OutboundWorker>,
}

struct OutboundWorker {
    sender: Sender<OutboundFrame>,
    connected: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

struct OutboundFrame {
    bytes: Vec<u8>,
    reconnect: bool,
    _message_permit: tokio::sync::OwnedSemaphorePermit,
    _byte_permits: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Clone, Copy)]
struct OutboundLimits {
    workers: usize,
    queued_messages: usize,
    queued_bytes: usize,
    idle_timeout: Duration,
}

impl Default for OutboundLimits {
    fn default() -> Self {
        Self {
            workers: MAX_OUTBOUND_WORKERS,
            queued_messages: MAX_OUTBOUND_QUEUED_MESSAGES,
            queued_bytes: MAX_OUTBOUND_QUEUED_BYTES,
            idle_timeout: OUTBOUND_WORKER_IDLE_TIMEOUT,
        }
    }
}

#[derive(Clone)]
struct ConnectionSet {
    peers: Arc<Mutex<BTreeMap<HostId, PeerConnection>>>,
    self_id: HostId,
    dropped_sends: Arc<AtomicU64>,
    overload_rejections: Arc<AtomicU64>,
    worker_rejections: Arc<AtomicU64>,
    queue_rejections: Arc<AtomicU64>,
    worker_slots: Arc<tokio::sync::Semaphore>,
    message_slots: Arc<tokio::sync::Semaphore>,
    byte_slots: Arc<tokio::sync::Semaphore>,
    limits: OutboundLimits,
    tls: Option<PeerTlsConfig>,
}

struct MembershipRefresher {
    membership: BucketMembership,
    identities: PeerIdentities,
    leases: MembershipLeases,
    connections: ConnectionSet,
    changed: Arc<MembershipChanged>,
    last_members: Vec<HostId>,
}

struct Outbound {
    rx: Receiver<OutboundFrame>,
    addr: SocketAddr,
    peer: HostId,
    connected: Arc<AtomicBool>,
    dropped_sends: Arc<AtomicU64>,
    tls: Option<ClientTls>,
    conn: Option<Box<dyn PeerIo>>,
    renewal: Option<tokio::task::JoinHandle<Option<Box<dyn PeerIo>>>>,
    idle_timeout: Duration,
}

impl Drop for OutboundWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ConnectionSet {
    fn new(self_id: HostId, tls: Option<PeerTlsConfig>) -> Self {
        Self::new_with_limits(
            self_id,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            tls,
            OutboundLimits::default(),
        )
    }

    fn new_with_limits(
        self_id: HostId,
        dropped_sends: Arc<AtomicU64>,
        overload_rejections: Arc<AtomicU64>,
        tls: Option<PeerTlsConfig>,
        limits: OutboundLimits,
    ) -> Self {
        assert!(limits.workers > 0, "outbound worker limit");
        assert!(limits.queued_messages > 0, "outbound message limit");
        assert!(limits.queued_bytes > 0, "outbound byte limit");
        Self {
            peers: Arc::new(Mutex::new(BTreeMap::new())),
            self_id,
            dropped_sends,
            overload_rejections,
            worker_rejections: Arc::new(AtomicU64::new(0)),
            queue_rejections: Arc::new(AtomicU64::new(0)),
            worker_slots: Arc::new(tokio::sync::Semaphore::new(limits.workers)),
            message_slots: Arc::new(tokio::sync::Semaphore::new(limits.queued_messages)),
            byte_slots: Arc::new(tokio::sync::Semaphore::new(limits.queued_bytes)),
            limits,
            tls,
        }
    }

    fn reconcile(&self, targets: &BTreeMap<HostId, PeerTarget>) {
        let mut peers = self.peers.lock().expect("peer connections lock");
        peers.retain(|host, connection| {
            targets
                .get(host)
                .is_some_and(|target| *host != self.self_id && *target == connection.target)
        });
        for (&peer, target) in targets {
            if peer == self.self_id || peers.contains_key(&peer) {
                continue;
            }
            peers.insert(
                peer,
                PeerConnection {
                    target: target.clone(),
                    worker: None,
                },
            );
        }
    }

    fn reject_worker(&self) {
        self.worker_rejections.fetch_add(1, Ordering::Relaxed);
        self.overload_rejections.fetch_add(1, Ordering::Relaxed);
        self.dropped_sends.fetch_add(1, Ordering::SeqCst);
    }

    fn reject_queue(&self) {
        self.queue_rejections.fetch_add(1, Ordering::Relaxed);
        self.overload_rejections.fetch_add(1, Ordering::Relaxed);
        self.dropped_sends.fetch_add(1, Ordering::SeqCst);
    }

    fn spawn_worker(
        &self,
        peer: HostId,
        target: &PeerTarget,
        worker_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> OutboundWorker {
        let (sender, receiver) = tokio::sync::mpsc::channel::<OutboundFrame>(SEND_QUEUE);
        let connected = Arc::new(AtomicBool::new(false));
        let client_tls = self.tls.as_ref().map(|tls| ClientTls {
            config: tls.client.clone(),
            identities: tls.certificate_identities.clone(),
            expected: target.identity,
        });
        let task_connected = Arc::clone(&connected);
        let dropped = Arc::clone(&self.dropped_sends);
        let endpoint = target.endpoint;
        let idle_timeout = self.limits.idle_timeout;
        let task = tokio::spawn(async move {
            let _worker_permit = worker_permit;
            Outbound::new(
                receiver,
                endpoint,
                peer,
                task_connected,
                dropped,
                client_tls,
                idle_timeout,
            )
            .run()
            .await;
        });
        OutboundWorker {
            sender,
            connected,
            task,
        }
    }

    fn send(&self, recipient: HostId, bytes: Vec<u8>, reconnect: bool) {
        let byte_count = u32::try_from(bytes.len()).ok();
        let Some(byte_count) = byte_count else {
            self.reject_queue();
            return;
        };
        let Ok(message_permit) = Arc::clone(&self.message_slots).try_acquire_owned() else {
            self.reject_queue();
            return;
        };
        let Ok(byte_permits) = Arc::clone(&self.byte_slots).try_acquire_many_owned(byte_count)
        else {
            self.reject_queue();
            return;
        };

        let mut peers = self.peers.lock().expect("peer connections lock");
        let Some(connection) = peers
            .get_mut(&recipient)
            .filter(|connection| connection.target.identity == recipient)
        else {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
            return;
        };
        if connection
            .worker
            .as_ref()
            .is_some_and(|worker| worker.task.is_finished())
        {
            connection.worker = None;
        }
        if connection.worker.is_none() {
            let Ok(worker_permit) = Arc::clone(&self.worker_slots).try_acquire_owned() else {
                drop(peers);
                self.reject_worker();
                return;
            };
            connection.worker =
                Some(self.spawn_worker(recipient, &connection.target, worker_permit));
        }
        let frame = OutboundFrame {
            bytes,
            reconnect,
            _message_permit: message_permit,
            _byte_permits: byte_permits,
        };
        let sender = connection
            .worker
            .as_ref()
            .expect("worker installed")
            .sender
            .clone();
        drop(peers);
        if let Err(TrySendError::Full(_) | TrySendError::Closed(_)) = sender.try_send(frame) {
            self.reject_queue();
        }
    }

    fn resource_metrics(&self) -> PeerResourceMetrics {
        PeerResourceMetrics {
            overload_rejections: self.overload_rejections.load(Ordering::Relaxed),
            outbound_worker_rejections: self.worker_rejections.load(Ordering::Relaxed),
            outbound_queue_rejections: self.queue_rejections.load(Ordering::Relaxed),
            outbound_active_workers: u64::try_from(
                self.limits
                    .workers
                    .saturating_sub(self.worker_slots.available_permits()),
            )
            .expect("worker count fits u64"),
            outbound_buffered_messages: u64::try_from(
                self.limits
                    .queued_messages
                    .saturating_sub(self.message_slots.available_permits()),
            )
            .expect("message count fits u64"),
            outbound_buffered_bytes: u64::try_from(
                self.limits
                    .queued_bytes
                    .saturating_sub(self.byte_slots.available_permits()),
            )
            .expect("byte count fits u64"),
            ..PeerResourceMetrics::default()
        }
    }
}

async fn publish_membership_heartbeat(
    membership: BucketMembership,
    membership_owned: Arc<AtomicBool>,
) {
    loop {
        tokio::time::sleep(MEMBERSHIP_HEARTBEAT_INTERVAL).await;
        let expected = *membership
            .generation
            .lock()
            .expect("membership generation lock");
        let own_record = membership
            .own_record
            .lock()
            .expect("own membership lock")
            .clone();
        match timeout(
            MEMBERSHIP_OPERATION_TIMEOUT,
            Arc::clone(&membership.store).put_cas(
                membership.own_key.clone(),
                Some(expected),
                own_record,
            ),
        )
        .await
        {
            Ok(Ok(generation)) => {
                membership_owned.store(true, Ordering::SeqCst);
                *membership
                    .generation
                    .lock()
                    .expect("membership generation lock") = generation;
            }
            Ok(Err(StoreFault::CasConflict { .. })) => {
                match timeout(
                    MEMBERSHIP_OPERATION_TIMEOUT,
                    Arc::clone(&membership.store).get_range(
                        membership.own_key.clone(),
                        0,
                        u64::try_from(MAX_MEMBERSHIP_RECORD_BYTES + 1)
                            .expect("membership read bound fits u64"),
                    ),
                )
                .await
                {
                    Ok(Ok(Some((generation, record))))
                        if record
                            == *membership.own_record.lock().expect("own membership lock") =>
                    {
                        membership_owned.store(true, Ordering::SeqCst);
                        *membership
                            .generation
                            .lock()
                            .expect("membership generation lock") = generation;
                    }
                    Ok(Ok(_)) => {
                        membership_owned.store(false, Ordering::SeqCst);
                        tracing::warn!(
                            membership_key = membership.own_key,
                            "peer membership ownership was replaced"
                        );
                        return;
                    }
                    Ok(Err(error)) => {
                        membership_owned.store(false, Ordering::SeqCst);
                        tracing::warn!(?error, "failed to verify peer membership ownership");
                    }
                    Err(_) => {
                        membership_owned.store(false, Ordering::SeqCst);
                        tracing::warn!("peer membership ownership check timed out");
                    }
                }
            }
            Ok(Err(StoreFault::Unavailable)) => {
                membership_owned.store(false, Ordering::SeqCst);
                tracing::warn!("failed to renew peer TLS membership");
            }
            Err(_) => {
                membership_owned.store(false, Ordering::SeqCst);
                tracing::warn!("peer TLS membership renewal timed out");
            }
        }
    }
}

impl MembershipRefresher {
    async fn run(mut self) {
        let mut join_delays = MEMBERSHIP_JOIN_REFRESH_DELAYS.into_iter();
        let mut last_success = Instant::now();
        loop {
            tokio::time::sleep(join_delays.next().unwrap_or(MEMBERSHIP_REFRESH_INTERVAL)).await;
            let elapsed = last_success.elapsed();
            if elapsed >= MAX_MEMBERSHIP_STALENESS {
                self.identities.replace(BTreeMap::new());
                self.connections.reconcile(&BTreeMap::new());
                if !self.last_members.is_empty() {
                    (self.changed)(Vec::new());
                    self.last_members.clear();
                }
            }
            let refresh_budget = MAX_MEMBERSHIP_STALENESS
                .checked_sub(elapsed)
                .unwrap_or(MAX_MEMBERSHIP_STALENESS);
            match timeout(
                refresh_budget,
                refresh_bucket_membership(&self.membership.store, &self.identities, &self.leases),
            )
            .await
            {
                Ok(Ok(targets)) => {
                    last_success = Instant::now();
                    self.connections.reconcile(&targets);
                    let members = targets
                        .values()
                        .filter_map(|target| (!target.drained).then_some(target.identity))
                        .collect::<Vec<_>>();
                    if members != self.last_members {
                        (self.changed)(members.clone());
                        self.last_members = members;
                    }
                }
                Ok(Err(error)) => {
                    tracing::warn!(?error, "failed to refresh peer TLS membership");
                }
                Err(_) => tracing::warn!("peer TLS membership refresh timed out"),
            }
            if last_success.elapsed() >= MAX_MEMBERSHIP_STALENESS {
                self.identities.replace(BTreeMap::new());
                self.connections.reconcile(&BTreeMap::new());
                if !self.last_members.is_empty() {
                    (self.changed)(Vec::new());
                    self.last_members.clear();
                }
            }
        }
    }
}

impl PeerNet {
    /// Start the listener, publish this node, and dynamically reconcile lazy
    /// senders for every object-store member. Verified inbound frames reach
    /// `deliver` (the runtime injects them into the actor peer inbox).
    pub async fn start(
        config: &PeerConfig,
        self_id: HostId,
        store: Arc<dyn ObjectStore>,
        deliver: impl Fn(HostId, PeerMsg) + Send + Sync + 'static,
    ) -> std::io::Result<Arc<PeerNet>> {
        Self::start_with_membership(config, self_id, store, deliver, |_| {}).await
    }

    /// Start peer discovery and report each distinct live membership snapshot.
    /// The callback runs on the network runtime and must not block.
    pub async fn start_with_membership(
        config: &PeerConfig,
        self_id: HostId,
        store: Arc<dyn ObjectStore>,
        deliver: impl Fn(HostId, PeerMsg) + Send + Sync + 'static,
        membership_changed: impl Fn(Vec<HostId>) + Send + Sync + 'static,
    ) -> std::io::Result<Arc<PeerNet>> {
        Self::start_with_membership_result(config, self_id, store, deliver, move |members| {
            membership_changed(members);
        })
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn start_with_membership_result(
        config: &PeerConfig,
        self_id: HostId,
        store: Arc<dyn ObjectStore>,
        deliver: impl Fn(HostId, PeerMsg) + Send + Sync + 'static,
        membership_changed: impl Fn(Vec<HostId>) + Send + Sync + 'static,
    ) -> Result<Arc<PeerNet>, PeerStartError> {
        if config.listen.port() == 0
            || config.advertise.port() == 0
            || config.advertise.ip().is_unspecified()
        {
            return Err(PeerStartError::Listener(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "peer listen port and reachable advertised endpoint are required",
            )));
        }
        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(PeerStartError::Listener)?;
        let tls = PeerTlsConfig::generate_in_object_store(store, self_id, config.advertise)
            .await
            .map_err(PeerStartError::Membership)?;
        let initial_targets = tls.initial_targets.clone();
        let membership_changed = Arc::new(membership_changed) as Arc<MembershipChanged>;
        let initial_members = initial_targets
            .values()
            .filter_map(|target| (!target.drained).then_some(target.identity))
            .collect::<Vec<_>>();
        membership_changed(initial_members.clone());
        Ok(Self::start_configured(
            listener,
            self_id,
            &initial_targets,
            Some(&tls),
            deliver,
            Some((&membership_changed, &initial_members)),
        ))
    }

    #[doc(hidden)]
    pub async fn start_plaintext(
        config: &PeerConfig,
        self_id: HostId,
        peers: BTreeMap<HostId, SocketAddr>,
        deliver: impl Fn(HostId, PeerMsg) + Send + Sync + 'static,
    ) -> std::io::Result<Arc<PeerNet>> {
        let listener = TcpListener::bind(config.listen).await?;
        let targets = peers
            .into_iter()
            .map(|(host, endpoint)| {
                (
                    host,
                    PeerTarget {
                        identity: host,
                        endpoint,
                        certificate: None,
                        drained: false,
                    },
                )
            })
            .collect();
        Ok(Self::start_configured(
            listener, self_id, &targets, None, deliver, None,
        ))
    }

    fn start_configured(
        listener: TcpListener,
        self_id: HostId,
        initial_targets: &BTreeMap<HostId, PeerTarget>,
        tls: Option<&PeerTlsConfig>,
        deliver: impl Fn(HostId, PeerMsg) + Send + Sync + 'static,
        membership_changed: Option<(&Arc<MembershipChanged>, &[HostId])>,
    ) -> Arc<PeerNet> {
        let node_span = tracing::Span::current();
        let connections = ConnectionSet::new(self_id, tls.cloned());
        let dropped_sends = Arc::clone(&connections.dropped_sends);
        let overload_rejections = Arc::clone(&connections.overload_rejections);
        let mut tasks = Vec::new();
        connections.reconcile(initial_targets);
        let deliver: Arc<Deliver> = Arc::new(deliver);
        let server_tls = tls.map(|tls| ServerTls {
            config: tls.server.clone(),
            identities: tls.certificate_identities.clone(),
        });
        let inbound_connections = Arc::new(tokio::sync::Semaphore::new(MAX_INBOUND_CONNECTIONS));
        let inbound_by_peer = Arc::new(Mutex::new(BTreeMap::new()));
        let inflight_bytes = Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT_PEER_BYTES));
        let membership_owned = Arc::new(AtomicBool::new(true));
        let receive_metrics = ReceiveMetrics::default();
        let listener_resources = ListenerResources {
            recipient: self_id,
            inbound_connections,
            inbound_by_peer,
            inflight_bytes,
            overload_rejections: Arc::clone(&overload_rejections),
            read_timeout: PEER_FRAME_READ_TIMEOUT,
            receive_metrics: receive_metrics.clone(),
        };
        if let Some(tls) = tls {
            tasks.push(tokio::spawn(
                publish_membership_heartbeat(tls.membership.clone(), Arc::clone(&membership_owned))
                    .instrument(node_span.clone()),
            ));
            tasks.push(tokio::spawn(
                MembershipRefresher {
                    membership: tls.membership.clone(),
                    identities: tls.certificate_identities.clone(),
                    leases: tls.membership_leases.clone(),
                    connections: connections.clone(),
                    changed: membership_changed
                        .as_ref()
                        .expect("TLS membership callback")
                        .0
                        .clone(),
                    last_members: membership_changed
                        .as_ref()
                        .expect("TLS membership callback")
                        .1
                        .to_vec(),
                }
                .run()
                .instrument(node_span.clone()),
            ));
        }
        let listener_task = tokio::spawn(
            listener_loop(listener, deliver, server_tls, listener_resources)
                .instrument(node_span.clone()),
        );
        let listener_handle = listener_task.abort_handle();
        tasks.push(listener_task);
        let critical_tasks = tasks
            .iter()
            .map(tokio::task::JoinHandle::abort_handle)
            .collect::<Vec<_>>();
        let healthy = Arc::new(AtomicBool::new(true));
        let failed = Arc::new(tokio::sync::Notify::new());
        let supervisor_healthy = Arc::clone(&healthy);
        let supervisor_failed = Arc::clone(&failed);
        tasks.push(tokio::spawn(
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if critical_tasks
                        .iter()
                        .any(tokio::task::AbortHandle::is_finished)
                    {
                        supervisor_healthy.store(false, Ordering::SeqCst);
                        supervisor_failed.notify_waiters();
                        for task in &critical_tasks {
                            task.abort();
                        }
                        return;
                    }
                }
            }
            .instrument(node_span),
        ));
        Arc::new(PeerNet {
            connections,
            dropped_sends,
            tasks: Mutex::new(tasks),
            authenticated: tls.is_some(),
            healthy,
            membership_owned,
            listener: listener_handle,
            failed,
            overload_rejections,
            receive_metrics,
        })
    }

    pub fn authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    pub fn membership_owned(&self) -> bool {
        self.membership_owned.load(Ordering::SeqCst)
    }

    /// Whether the externally reachable peer listener is still running.
    pub fn listener_healthy(&self) -> bool {
        !self.listener.is_finished()
    }

    pub async fn critical_failure(&self) {
        loop {
            let failed = self.failed.notified();
            if !self.healthy() {
                return;
            }
            failed.await;
        }
    }

    pub fn overload_rejections(&self) -> u64 {
        self.overload_rejections.load(Ordering::Relaxed)
    }

    pub fn resource_metrics(&self) -> PeerResourceMetrics {
        let receive = self.receive_metrics.snapshot();
        PeerResourceMetrics {
            payload_budget_waits: receive.payload_budget_waits,
            frame_read_timeouts: receive.frame_read_timeouts,
            idle_timeouts: receive.idle_timeouts,
            ..self.connections.resource_metrics()
        }
    }

    pub async fn publish_drained(&self) -> Result<(), StoreFault> {
        let Some(tls) = self.connections.tls.as_ref() else {
            return Ok(());
        };
        let mut record = MemberRecord::decode(
            &tls.membership
                .own_record
                .lock()
                .expect("own membership lock"),
        )
        .ok_or(StoreFault::Unavailable)?;
        record.drained = true;
        let bytes = record.encode();
        tls.membership
            .own_record
            .lock()
            .expect("own membership lock")
            .clone_from(&bytes);
        let expected = *tls
            .membership
            .generation
            .lock()
            .expect("membership generation lock");
        match Arc::clone(&tls.membership.store)
            .put_cas(
                tls.membership.own_key.clone(),
                Some(expected),
                bytes.clone(),
            )
            .await
        {
            Ok(generation) => {
                *tls.membership
                    .generation
                    .lock()
                    .expect("membership generation lock") = generation;
                Ok(())
            }
            Err(StoreFault::CasConflict { .. }) => {
                let Some((generation, found)) = Arc::clone(&tls.membership.store)
                    .get(tls.membership.own_key.clone())
                    .await?
                else {
                    return Err(StoreFault::CasConflict { actual: None });
                };
                if found != bytes {
                    return Err(StoreFault::CasConflict {
                        actual: Some(generation),
                    });
                }
                *tls.membership
                    .generation
                    .lock()
                    .expect("membership generation lock") = generation;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Fire-and-forget: encode and queue. Never blocks the daemon thread;
    /// unknown peers and full queues drop the frame (retries re-drive).
    pub fn send(&self, _self_id: HostId, to: HostId, msg: &PeerMsg) {
        let identity = self
            .connections
            .peers
            .lock()
            .expect("peer connections lock")
            .get(&to)
            .map(|peer| peer.target.identity);
        let Some(identity) = identity else {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
            return;
        };
        self.send_identity(identity, msg);
    }

    pub fn send_identity(&self, to: HostId, msg: &PeerMsg) {
        self.connections.send(
            to,
            encode_peer_routed(self.connections.self_id, to, msg),
            // Replica release is rare and may be initiated by a restarted
            // primary. Its acknowledgement must not remain on an otherwise
            // healthy-looking connection to the prior process instance.
            matches!(msg, PeerMsg::ReplicaReleaseAck { .. }),
        );
    }

    pub fn connections(&self) -> Vec<(HostId, bool)> {
        self.connections
            .peers
            .lock()
            .expect("peer connections lock")
            .iter()
            .map(|(&peer, connection)| {
                (
                    peer,
                    connection.worker.as_ref().is_some_and(|worker| {
                        worker.connected.load(Ordering::Relaxed) && !worker.task.is_finished()
                    }),
                )
            })
            .collect()
    }

    pub async fn shutdown(&self) -> Result<(), StoreFault> {
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("peer tasks lock"));
        for task in &tasks {
            task.abort();
        }
        self.connections
            .peers
            .lock()
            .expect("peer connections lock")
            .clear();
        let withdrawal = self.connections.tls.as_ref().map(|tls| {
            let generation = *tls
                .membership
                .generation
                .lock()
                .expect("membership generation lock");
            (
                Arc::clone(&tls.membership.store),
                tls.membership.own_key.clone(),
                generation,
            )
        });
        for task in tasks {
            let _ = task.await;
        }
        if let Some((store, key, generation)) = withdrawal {
            store.delete_cas(key, generation).await.map(|_| ())
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    fn stop_membership_heartbeat(&self) {
        self.tasks
            .lock()
            .expect("peer tasks lock")
            .first()
            .expect("membership heartbeat task")
            .abort();
    }

    #[cfg(test)]
    fn stop_membership_refresher(&self) {
        self.tasks
            .lock()
            .expect("peer tasks lock")
            .get(1)
            .expect("membership refresher task")
            .abort();
    }

    #[cfg(test)]
    fn stop_listener(&self) {
        self.tasks
            .lock()
            .expect("peer tasks lock")
            .get(2)
            .expect("listener task")
            .abort();
    }
}

impl Drop for PeerNet {
    fn drop(&mut self) {
        for task in self.tasks.lock().expect("lock").drain(..) {
            task.abort();
        }
        self.connections
            .peers
            .lock()
            .expect("peer connections lock")
            .clear();
    }
}

/// One outbound connection, made lazily and remade on any error. A frame
/// that hits a connect or write failure is dropped with its connection —
/// the protocol's retry timers own recovery, not this loop.
trait PeerIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

impl<T> PeerIo for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

impl Outbound {
    fn new(
        rx: Receiver<OutboundFrame>,
        addr: SocketAddr,
        peer: HostId,
        connected: Arc<AtomicBool>,
        dropped_sends: Arc<AtomicU64>,
        tls: Option<ClientTls>,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            rx,
            addr,
            peer,
            connected,
            dropped_sends,
            tls,
            conn: None,
            renewal: None,
            idle_timeout,
        }
    }

    async fn run(mut self) {
        loop {
            if self.conn.is_none()
                && let Some(task) = self.renewal.take()
            {
                task.abort();
            }
            if let Some(task) = self.renewal.as_mut() {
                tokio::select! {
                    renewed = task => {
                        self.renewal = None;
                        if let Ok(Some(stream)) = renewed {
                            self.conn = Some(stream);
                            self.connected.store(true, Ordering::Relaxed);
                        }
                        if self.conn.is_some() {
                            self.renewal = Some(spawn_connection_renewal(self.addr, self.tls.clone()));
                        }
                        continue;
                    }
                    frame = self.rx.recv() => {
                        let Some(frame) = frame else { break };
                        self.process_frame(frame).await;
                        continue;
                    }
                    () = tokio::time::sleep(self.idle_timeout) => {
                        self.drain_accepted_frames().await;
                        break;
                    },
                }
            }

            match tokio::time::timeout(self.idle_timeout, self.rx.recv()).await {
                Ok(Some(frame)) => self.process_frame(frame).await,
                Ok(None) => break,
                Err(_) => {
                    self.drain_accepted_frames().await;
                    break;
                }
            }
        }
        self.connected.store(false, Ordering::Relaxed);
    }

    async fn drain_accepted_frames(&mut self) {
        self.rx.close();
        while let Some(frame) = self.rx.recv().await {
            self.process_frame(frame).await;
        }
    }

    async fn process_frame(&mut self, frame: OutboundFrame) {
        if frame.reconnect || self.conn.is_none() {
            if let Some(task) = self.renewal.take() {
                task.abort();
            }
            self.conn = connect(self.addr, self.tls.as_ref()).await;
            self.connected.store(self.conn.is_some(), Ordering::Relaxed);
            if self.conn.is_some() {
                self.renewal = Some(spawn_connection_renewal(self.addr, self.tls.clone()));
            }
        }
        if self.conn.is_none() {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
            return;
        }
        if !write_peer_frame(&mut self.conn, &frame.bytes).await {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
            self.connected.store(false, Ordering::Relaxed);
            tracing::warn!(peer_host = self.peer.get(), addr = %self.addr, "peer connection lost");
            self.conn = None;
        }
    }
}

fn spawn_connection_renewal(
    addr: SocketAddr,
    tls: Option<ClientTls>,
) -> tokio::task::JoinHandle<Option<Box<dyn PeerIo>>> {
    tokio::spawn(async move {
        tokio::time::sleep(MAX_CONNECTION_AGE).await;
        connect(addr, tls.as_ref()).await
    })
}

async fn write_peer_frame(conn: &mut Option<Box<dyn PeerIo>>, frame: &[u8]) -> bool {
    let Some(stream) = conn.as_mut() else {
        return false;
    };
    matches!(
        timeout(Duration::from_secs(5), write_frame(stream, frame)).await,
        Ok(Ok(()))
    )
}

async fn connect(addr: SocketAddr, tls: Option<&ClientTls>) -> Option<Box<dyn PeerIo>> {
    let stream = timeout(Duration::from_secs(1), TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;
    stream.set_nodelay(true).ok()?;
    let Some(ClientTls {
        config,
        identities,
        expected,
    }) = tls
    else {
        return Some(Box::new(stream) as Box<dyn PeerIo>);
    };
    let name = ServerName::try_from(DEFAULT_PEER_SERVER_NAME).ok()?;
    let stream = timeout(
        Duration::from_secs(5),
        TlsConnector::from(config.clone()).connect(name, stream),
    )
    .await
    .ok()?
    .ok()?;
    let certificate = stream.get_ref().1.peer_certificates()?.first()?.as_ref();
    if identities.identity(certificate) != Some(*expected) {
        return None;
    }
    Some(Box::new(stream))
}

/// One inbound connection: length-delimited by the frame header itself,
/// verified by `decode_peer` (magic, length cap, crc, strict layout). Any
/// violation closes the connection; the peer reconnects and retries.
async fn listener_loop(
    listener: TcpListener,
    deliver: Arc<Deliver>,
    tls: Option<ServerTls>,
    resources: ListenerResources,
) {
    let mut readers = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let Ok(connection_permit) = Arc::clone(&resources.inbound_connections).try_acquire_owned() else {
                        resources.overload_rejections.fetch_add(1, Ordering::Relaxed);
                        continue;
                    };
                    let deliver = deliver.clone();
                    let tls = tls.clone();
                    let resources = resources.clone();
                    readers.spawn(async move {
                        let _connection_permit = connection_permit;
                        accepted_reader(stream, tls.as_ref(), deliver, resources).await;
                    });
                }
                Err(error) => {
                    tracing::error!(%error, "peer listener stopped");
                    return;
                }
            },
            _ = readers.join_next(), if !readers.is_empty() => {
            }
        }
    }
}

#[derive(Clone)]
struct ListenerResources {
    recipient: HostId,
    inbound_connections: Arc<tokio::sync::Semaphore>,
    inbound_by_peer: Arc<Mutex<BTreeMap<HostId, usize>>>,
    inflight_bytes: Arc<tokio::sync::Semaphore>,
    overload_rejections: Arc<AtomicU64>,
    read_timeout: Duration,
    receive_metrics: ReceiveMetrics,
}

struct PeerConnectionGuard {
    host: HostId,
    counts: Arc<Mutex<BTreeMap<HostId, usize>>>,
}

impl Drop for PeerConnectionGuard {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().expect("inbound peer count lock");
        if let Some(count) = counts.get_mut(&self.host) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.host);
            }
        }
    }
}

fn enter_peer_connection(
    host: HostId,
    counts: Arc<Mutex<BTreeMap<HostId, usize>>>,
) -> Option<PeerConnectionGuard> {
    {
        let mut held = counts.lock().expect("inbound peer count lock");
        let count = held.entry(host).or_default();
        if *count >= MAX_INBOUND_CONNECTIONS_PER_PEER {
            return None;
        }
        *count += 1;
    }
    Some(PeerConnectionGuard { host, counts })
}

async fn accepted_reader(
    stream: TcpStream,
    tls: Option<&ServerTls>,
    deliver: Arc<Deliver>,
    resources: ListenerResources,
) {
    let _ = stream.set_nodelay(true);
    let Some(ServerTls { config, identities }) = tls else {
        let _ = receive_loop_while_authorized_with_limits_and_metrics(
            stream,
            None,
            DecodePolicy::BlockingAbove(DECODE_OFFLOAD_THRESHOLD),
            ReceiveLimits {
                read_timeout: resources.read_timeout,
                inflight_bytes: resources.inflight_bytes,
                metrics: resources.receive_metrics,
            },
            |_| true,
            move |from, message| deliver(from, message),
        )
        .await;
        return;
    };
    let Ok(Ok(stream)) = timeout(
        Duration::from_secs(5),
        TlsAcceptor::from(config.clone()).accept(stream),
    )
    .await
    else {
        return;
    };
    let Some(certificate) = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certificates| certificates.first())
    else {
        return;
    };
    let Some(identity) = identities.identity(certificate.as_ref()) else {
        return;
    };
    let Some(_peer_permit) = enter_peer_connection(identity, resources.inbound_by_peer) else {
        resources
            .overload_rejections
            .fetch_add(1, Ordering::Relaxed);
        return;
    };
    let certificate = certificate.as_ref().to_vec();
    let identities = identities.clone();
    let _ = receive_routed_loop_with_limits_and_metrics(
        stream,
        Some(identity),
        resources.recipient,
        DecodePolicy::BlockingAbove(DECODE_OFFLOAD_THRESHOLD),
        ReceiveLimits {
            read_timeout: resources.read_timeout,
            inflight_bytes: resources.inflight_bytes,
            metrics: resources.receive_metrics,
        },
        move |from| identities.identity(&certificate) == Some(from),
        move |from, message| deliver(from, message),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakegcs::FakeGcs;
    use crate::{GcsConfig, GcsStore};
    use blockd_core::peer::{MAGIC_PEER, MAX_PEER_PAYLOAD};
    use blockd_core::types::VolumeId;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    fn free_addr() -> SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("address")
    }

    async fn fake_store() -> (crate::fakegcs::FakeGcsServer, Arc<GcsStore>) {
        let (fake, endpoint) = FakeGcs::start().await;
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "test/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        (fake, store)
    }

    #[derive(Clone, Default)]
    struct DeliveryCounts(Arc<Mutex<BTreeMap<HostId, u64>>>);

    impl DeliveryCounts {
        fn record(&self, from: HostId) {
            let mut counts = self.0.lock().expect("delivery counts lock");
            *counts.entry(from).or_default() += 1;
        }

        fn from(&self, host: HostId) -> u64 {
            self.0
                .lock()
                .expect("delivery counts lock")
                .get(&host)
                .copied()
                .unwrap_or_default()
        }
    }

    struct TestNode {
        net: Arc<PeerNet>,
        deliveries: DeliveryCounts,
    }

    async fn start_node(
        host: HostId,
        addresses: &BTreeMap<HostId, SocketAddr>,
        store: Arc<GcsStore>,
    ) -> TestNode {
        let deliveries = DeliveryCounts::default();
        let delivered = deliveries.clone();
        let net = PeerNet::start(
            &PeerConfig {
                listen: addresses[&host],
                advertise: addresses[&host],
            },
            host,
            store,
            move |from, _| delivered.record(from),
        )
        .await
        .expect("start test node");
        TestNode { net, deliveries }
    }

    fn addresses(hosts: u16) -> BTreeMap<HostId, SocketAddr> {
        (0..hosts)
            .map(|host| (HostId::new(u32::from(host)), free_addr()))
            .collect()
    }

    async fn stored_member(store: &Arc<GcsStore>, host: HostId) -> MemberRecord {
        let (_, encoded) = store
            .clone()
            .get(peer_membership_key(host))
            .await
            .expect("read member")
            .expect("published member");
        MemberRecord::decode(&encoded).expect("valid member record")
    }

    struct TestListener {
        address: SocketAddr,
        task: tokio::task::JoinHandle<()>,
        overload_rejections: Arc<AtomicU64>,
        receive_metrics: ReceiveMetrics,
        inbound_connections: Arc<tokio::sync::Semaphore>,
        inbound_by_peer: Arc<Mutex<BTreeMap<HostId, usize>>>,
        inflight_bytes: Arc<tokio::sync::Semaphore>,
    }

    impl Drop for TestListener {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn test_tls_pair(store: Arc<GcsStore>) -> (PeerTlsConfig, PeerTlsConfig) {
        let server =
            PeerTlsConfig::generate_in_object_store(store.clone(), HostId::new(40), free_addr())
                .await
                .expect("generate server identity");
        let client =
            PeerTlsConfig::generate_in_object_store(store.clone(), HostId::new(41), free_addr())
                .await
                .expect("generate client identity");
        let server_member = stored_member(&store, HostId::new(40)).await;
        let client_member = stored_member(&store, HostId::new(41)).await;
        server.certificate_identities.replace(BTreeMap::from([(
            client_member.certificate,
            HostId::new(41),
        )]));
        client.certificate_identities.replace(BTreeMap::from([(
            server_member.certificate,
            HostId::new(40),
        )]));
        (server, client)
    }

    async fn start_test_listener(
        tls: &PeerTlsConfig,
        global_limit: usize,
        byte_budget: usize,
        read_timeout: Duration,
    ) -> TestListener {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let overload_rejections = Arc::new(AtomicU64::new(0));
        let receive_metrics = ReceiveMetrics::default();
        let inbound_connections = Arc::new(tokio::sync::Semaphore::new(global_limit));
        let inbound_by_peer = Arc::new(Mutex::new(BTreeMap::new()));
        let inflight_bytes = Arc::new(tokio::sync::Semaphore::new(byte_budget));
        let resources = ListenerResources {
            recipient: HostId::new(40),
            inbound_connections: Arc::clone(&inbound_connections),
            inbound_by_peer: Arc::clone(&inbound_by_peer),
            inflight_bytes: Arc::clone(&inflight_bytes),
            overload_rejections: Arc::clone(&overload_rejections),
            read_timeout,
            receive_metrics: receive_metrics.clone(),
        };
        let task = tokio::spawn(listener_loop(
            listener,
            Arc::new(|_, _| {}),
            Some(ServerTls {
                config: tls.server.clone(),
                identities: tls.certificate_identities.clone(),
            }),
            resources,
        ));
        TestListener {
            address,
            task,
            overload_rejections,
            receive_metrics,
            inbound_connections,
            inbound_by_peer,
            inflight_bytes,
        }
    }

    async fn connect_authenticated(
        listener: &TestListener,
        client: &PeerTlsConfig,
    ) -> tokio_rustls::client::TlsStream<TcpStream> {
        let tcp = TcpStream::connect(listener.address)
            .await
            .expect("connect test listener");
        TlsConnector::from(client.client.clone())
            .connect(
                ServerName::try_from(DEFAULT_PEER_SERVER_NAME)
                    .expect("server name")
                    .to_owned(),
                tcp,
            )
            .await
            .expect("authenticate test connection")
    }

    async fn wait_for(condition: impl Fn() -> bool) {
        timeout(Duration::from_secs(1), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test listener reached expected state");
    }

    async fn assert_tls_closed(
        socket: &mut tokio_rustls::client::TlsStream<TcpStream>,
        context: &str,
    ) {
        let mut byte = [0_u8; 1];
        let result = timeout(Duration::from_millis(500), socket.read(&mut byte))
            .await
            .unwrap_or_else(|_| panic!("{context} did not close promptly"));
        assert!(
            matches!(result, Ok(0))
                || matches!(result, Err(ref error) if error.kind() == std::io::ErrorKind::UnexpectedEof),
            "{context} remained readable: {result:?}"
        );
    }

    fn declared_frame(payload_len: u32) -> Vec<u8> {
        let mut header = Vec::from(MAGIC_PEER.to_le_bytes());
        header.extend_from_slice(&payload_len.to_le_bytes());
        header.extend_from_slice(&0_u32.to_le_bytes());
        header
    }

    fn spawn_traffic(net: Arc<PeerNet>, from: HostId, to: HostId) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut fence = 0;
            loop {
                net.send(
                    from,
                    to,
                    &PeerMsg::Released {
                        volume: VolumeId(19),
                        release_fence: fence,
                    },
                );
                fence = fence.wrapping_add(1);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    fn spawn_member_renewal(
        store: Arc<GcsStore>,
        host: HostId,
        member: MemberRecord,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                store
                    .clone()
                    .put(peer_membership_key(host), member.encode())
                    .await
                    .expect("renew test membership");
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
    }

    async fn wait_for_count(counts: &DeliveryCounts, from: HostId, minimum: u64) {
        timeout(Duration::from_secs(3), async {
            while counts.from(from) < minimum {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("traffic made progress");
    }

    async fn assert_progress(counts: &DeliveryCounts, from: HostId) {
        let before = counts.from(from);
        wait_for_count(counts, from, before + 3).await;
    }

    async fn wait_for_peer(net: &PeerNet, host: HostId, present: bool) {
        timeout(Duration::from_secs(3), async {
            loop {
                let found = net
                    .connections()
                    .into_iter()
                    .any(|(candidate, _)| candidate == host);
                if found == present {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dynamic peer set converged");
    }

    async fn wait_until_quiet(counts: &DeliveryCounts, from: HostId) -> u64 {
        timeout(Duration::from_secs(3), async {
            loop {
                let before = counts.from(from);
                tokio::time::sleep(Duration::from_millis(150)).await;
                if counts.from(from) == before {
                    return before;
                }
            }
        })
        .await
        .expect("revoked traffic became quiet")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn object_store_writers_publish_and_join_mutual_tls() {
        let (_fake, store) = fake_store().await;
        let addresses = [free_addr(), free_addr()];
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let _b = PeerNet::start(
            &PeerConfig {
                listen: addresses[1],
                advertise: addresses[1],
            },
            HostId::new(1),
            store.clone(),
            move |from, message| {
                let _ = tx.send((from, message));
            },
        )
        .await
        .expect("start host 1");
        let a = PeerNet::start(
            &PeerConfig {
                listen: addresses[0],
                advertise: addresses[0],
            },
            HostId::new(0),
            store.clone(),
            |_, _| {},
        )
        .await
        .expect("start host 0");

        for host in [HostId::new(0), HostId::new(1)] {
            let member = stored_member(&store, host).await;
            assert!(member.certificate.len() <= MAX_PEER_CERTIFICATE_BYTES);
            assert_eq!(
                member.endpoint,
                addresses[usize::try_from(host.get()).expect("test host fits usize")]
            );
        }

        let message = PeerMsg::Released {
            volume: VolumeId(11),
            release_fence: 7,
        };
        let mut received = None;
        for _ in 0..30 {
            a.send(HostId::new(0), HostId::new(1), &message);
            if let Ok(next) = timeout(Duration::from_millis(100), rx.recv()).await {
                received = next;
                break;
            }
        }
        assert_eq!(received, Some((HostId::new(0), message)));

        let message = PeerMsg::Released {
            volume: VolumeId(12),
            release_fence: 8,
        };
        a.send(HostId::new(9), HostId::new(1), &message);
        assert_eq!(
            timeout(Duration::from_millis(250), rx.recv())
                .await
                .expect("authenticated sender delivered"),
            Some((HostId::new(0), message)),
            "certificate identity must override the claimed sender",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_or_deleted_public_certificates_are_not_members() {
        let (_fake, store) = fake_store().await;
        let tls =
            PeerTlsConfig::generate_in_object_store(store.clone(), HostId::new(4), free_addr())
                .await
                .expect("publish host 4");
        let member = stored_member(&store, HostId::new(4)).await;
        store
            .clone()
            .put(peer_membership_key(HostId::new(4)), member.encode())
            .await
            .expect("renew host 4");
        refresh_bucket_membership(
            &tls.membership.store,
            &tls.certificate_identities,
            &tls.membership_leases,
        )
        .await
        .expect("observe host 4 renewal");
        store
            .clone()
            .put(peer_membership_key(HostId::new(5)), member.encode())
            .await
            .expect("publish duplicate");
        refresh_bucket_membership(
            &tls.membership.store,
            &tls.certificate_identities,
            &tls.membership_leases,
        )
        .await
        .expect("observe duplicate");
        store
            .clone()
            .put(peer_membership_key(HostId::new(5)), member.encode())
            .await
            .expect("renew duplicate");
        refresh_bucket_membership(
            &tls.membership.store,
            &tls.certificate_identities,
            &tls.membership_leases,
        )
        .await
        .expect("refresh duplicates");
        assert_eq!(
            tls.certificate_identities.identity(&member.certificate),
            None
        );

        store
            .clone()
            .delete(peer_membership_key(HostId::new(5)))
            .await
            .expect("delete membership fixture");
        refresh_bucket_membership(
            &tls.membership.store,
            &tls.certificate_identities,
            &tls.membership_leases,
        )
        .await
        .expect("refresh deletion");
        assert_eq!(
            tls.certificate_identities.identity(&member.certificate),
            Some(HostId::new(4))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oversized_membership_objects_are_read_with_a_strict_bound() {
        let (fake, store) = fake_store().await;
        store
            .clone()
            .put(
                peer_membership_key(HostId::new(7)),
                vec![0; MAX_MEMBERSHIP_RECORD_BYTES * 4],
            )
            .await
            .expect("publish oversized member");

        let object_store: Arc<dyn ObjectStore> = store;
        let targets = refresh_bucket_membership(
            &object_store,
            &PeerIdentities::default(),
            &MembershipLeases::default(),
        )
        .await
        .expect("refresh bounded member");
        assert!(targets.is_empty());

        let range = format!("bytes=0-{MAX_MEMBERSHIP_RECORD_BYTES}");
        assert!(
            fake.seen
                .lock()
                .expect("seen requests lock")
                .iter()
                .any(|request| request.method == "GET"
                    && request
                        .path
                        .ends_with("/cluster/tls/public-keys/00000007.member")
                    && request.headers.get("range") == Some(&range)),
            "membership objects must never be fetched without a size bound"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::too_many_lines)] // one request-accounting matrix across roster sizes and lease phases
    async fn membership_refresh_scales_by_pages_and_changed_bodies() {
        for hosts in [1_u16, 17, 1_024] {
            let (fake, store) = fake_store().await;
            let object_store: Arc<dyn ObjectStore> = store.clone();
            let records = (0..hosts)
                .map(|host| {
                    (
                        HostId::new(u32::from(host)),
                        MemberRecord {
                            endpoint: free_addr(),
                            certificate: host.wrapping_add(1).to_le_bytes().to_vec(),
                            drained: false,
                        },
                    )
                })
                .collect::<Vec<_>>();
            for (host, member) in &records {
                store
                    .clone()
                    .put(peer_membership_key(*host), member.encode())
                    .await
                    .expect("publish member");
            }
            let identities = PeerIdentities::default();
            let leases = MembershipLeases::default();
            let pages = usize::from(hosts).div_ceil(1_000);
            let before = fake.method_count("GET");
            let first = refresh_bucket_membership(&object_store, &identities, &leases)
                .await
                .expect("initial refresh");
            assert!(first.is_empty(), "first observation is not a lease renewal");
            assert_eq!(
                fake.method_count("GET") - before,
                pages + usize::from(hosts),
                "initial {hosts}-member refresh should read each body once"
            );

            for (host, member) in &records {
                store
                    .clone()
                    .put(peer_membership_key(*host), member.encode())
                    .await
                    .expect("renew member");
            }
            let before = fake.method_count("GET");
            let renewed = refresh_bucket_membership(&object_store, &identities, &leases)
                .await
                .expect("renewal refresh");
            assert_eq!(renewed.len(), usize::from(hosts));
            assert_eq!(
                fake.method_count("GET") - before,
                pages,
                "identical-body renewals should use LIST fingerprints without body reads"
            );

            let changed_host = records.len() / 2;
            let (host, member) = &records[changed_host];
            let mut changed = member.clone();
            changed.drained = true;
            store
                .clone()
                .put(peer_membership_key(*host), changed.encode())
                .await
                .expect("change one member body");
            let before = fake.method_count("GET");
            let changed_members = refresh_bucket_membership(&object_store, &identities, &leases)
                .await
                .expect("changed-body refresh");
            assert_eq!(changed_members.len(), usize::from(hosts));
            assert!(changed_members[host].drained);
            assert_eq!(
                fake.method_count("GET") - before,
                pages + 1,
                "one changed body should cause exactly one bounded body read"
            );

            let before = fake.method_count("GET");
            let unchanged = refresh_bucket_membership(&object_store, &identities, &leases)
                .await
                .expect("unchanged refresh");
            assert_eq!(unchanged.len(), usize::from(hosts));
            assert_eq!(
                fake.method_count("GET") - before,
                pages,
                "unchanged {hosts}-member refresh should perform LIST pages only"
            );

            tokio::time::sleep(MAX_MEMBERSHIP_STALENESS + Duration::from_millis(20)).await;
            let before = fake.method_count("GET");
            let expired = refresh_bucket_membership(&object_store, &identities, &leases)
                .await
                .expect("expiry refresh");
            assert!(
                expired.is_empty(),
                "stale {hosts}-member roster remained live"
            );
            assert_eq!(
                fake.method_count("GET") - before,
                pages,
                "expiry should use cached bodies and bounded LIST pages"
            );

            let range = format!("bytes=0-{MAX_MEMBERSHIP_RECORD_BYTES}");
            assert!(
                fake.seen
                    .lock()
                    .expect("seen requests lock")
                    .iter()
                    .filter(|request| {
                        request.method == "GET" && request.path.ends_with(".member")
                    })
                    .all(|request| request.headers.get("range") == Some(&range)),
                "all membership bodies must use the strict range bound"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn real_listener_rejects_connections_above_the_global_limit() {
        let (_fake, store) = fake_store().await;
        let (server, _client) = test_tls_pair(store).await;
        let listener = start_test_listener(&server, 2, 64, Duration::from_millis(500)).await;
        let _first = TcpStream::connect(listener.address)
            .await
            .expect("first socket");
        let _second = TcpStream::connect(listener.address)
            .await
            .expect("second socket");
        wait_for(|| listener.inbound_connections.available_permits() == 0).await;

        let mut rejected = TcpStream::connect(listener.address)
            .await
            .expect("over-limit socket");
        wait_for(|| listener.overload_rejections.load(Ordering::Relaxed) == 1).await;
        let mut byte = [0_u8; 1];
        assert_eq!(
            timeout(Duration::from_millis(250), rejected.read(&mut byte))
                .await
                .expect("global rejection closed promptly")
                .expect("read rejected socket"),
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn real_listener_rejects_connections_above_the_authenticated_peer_limit() {
        let (_fake, store) = fake_store().await;
        let (server, client) = test_tls_pair(store).await;
        let listener = start_test_listener(&server, 32, 64, Duration::from_secs(2)).await;
        let mut held = Vec::new();
        for _ in 0..MAX_INBOUND_CONNECTIONS_PER_PEER {
            held.push(connect_authenticated(&listener, &client).await);
        }
        wait_for(|| {
            listener
                .inbound_by_peer
                .lock()
                .expect("peer counts")
                .get(&HostId::new(41))
                .copied()
                == Some(MAX_INBOUND_CONNECTIONS_PER_PEER)
        })
        .await;

        let mut rejected = connect_authenticated(&listener, &client).await;
        wait_for(|| listener.overload_rejections.load(Ordering::Relaxed) == 1).await;
        assert_tls_closed(&mut rejected, "per-peer rejection").await;
        assert_eq!(held.len(), MAX_INBOUND_CONNECTIONS_PER_PEER);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn authenticated_idle_socket_is_closed_and_counted() {
        let (_fake, store) = fake_store().await;
        let (server, client) = test_tls_pair(store).await;
        let listener = start_test_listener(&server, 4, 64, Duration::from_millis(25)).await;
        let mut socket = connect_authenticated(&listener, &client).await;
        assert_tls_closed(&mut socket, "idle deadline").await;
        assert_eq!(listener.receive_metrics.snapshot().idle_timeouts, 1);
        assert_eq!(listener.receive_metrics.snapshot().frame_read_timeouts, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn real_listener_backpressures_aggregate_declared_payload_bytes() {
        let (_fake, store) = fake_store().await;
        let (server, client) = test_tls_pair(store).await;
        let listener = start_test_listener(&server, 4, 8, Duration::from_millis(100)).await;
        let header = declared_frame(8);
        let mut first = connect_authenticated(&listener, &client).await;
        first.write_all(&header).await.expect("first frame header");
        wait_for(|| listener.inflight_bytes.available_permits() == 0).await;
        let mut second = connect_authenticated(&listener, &client).await;
        second
            .write_all(&header)
            .await
            .expect("second frame header");
        wait_for(|| listener.receive_metrics.snapshot().payload_budget_waits == 1).await;
        wait_for(|| listener.receive_metrics.snapshot().frame_read_timeouts == 2).await;
        assert_eq!(listener.inflight_bytes.available_permits(), 8);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn real_listener_accepts_the_exact_maximum_declared_frame() {
        let (_fake, store) = fake_store().await;
        let (server, client) = test_tls_pair(store).await;
        let listener = start_test_listener(
            &server,
            2,
            MAX_PEER_PAYLOAD as usize,
            Duration::from_millis(25),
        )
        .await;
        let mut socket = connect_authenticated(&listener, &client).await;
        socket
            .write_all(&declared_frame(MAX_PEER_PAYLOAD))
            .await
            .expect("maximum frame header");
        assert_tls_closed(&mut socket, "maximum frame body deadline").await;
        assert_eq!(listener.receive_metrics.snapshot().frame_read_timeouts, 1);
        assert_eq!(listener.receive_metrics.snapshot().idle_timeouts, 0);
        assert_eq!(
            listener.inflight_bytes.available_permits(),
            MAX_PEER_PAYLOAD as usize
        );
    }

    /// Regression PROD-018: a peer that supplies only a partial frame must not retain
    /// an inbound reader forever.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn partial_peer_frame_is_closed_after_body_deadline() {
        use tokio::io::AsyncWriteExt as _;

        let (mut writer, reader) = tokio::io::duplex(64);
        writer.write_all(&[0_u8]).await.expect("partial frame");
        let deliver: Arc<Deliver> = Arc::new(|_, _| {});

        assert!(
            timeout(
                PEER_FRAME_READ_TIMEOUT + Duration::from_secs(1),
                receive_loop_while_authorized_with_limits_and_metrics(
                    reader,
                    None,
                    DecodePolicy::BlockingAbove(DECODE_OFFLOAD_THRESHOLD),
                    ReceiveLimits {
                        read_timeout: PEER_FRAME_READ_TIMEOUT,
                        inflight_bytes: Arc::new(tokio::sync::Semaphore::new(64)),
                        metrics: ReceiveMetrics::default(),
                    },
                    |_| true,
                    move |from, message| deliver(from, message),
                )
            )
            .await
            .is_ok(),
            "partial frame retained an inbound task past its read deadline"
        );
    }

    /// Regression PROD-008: loss of a critical membership task must stop the peer
    /// subsystem so the daemon cannot remain apparently healthy.
    #[tokio::test(flavor = "current_thread")]
    async fn membership_heartbeat_failure_stops_the_peer_subsystem() {
        let (_fake, store) = fake_store().await;
        let address = free_addr();
        let net = PeerNet::start(
            &PeerConfig {
                listen: address,
                advertise: address,
            },
            HostId::new(8),
            store,
            |_, _| {},
        )
        .await
        .expect("start peer subsystem");

        net.stop_membership_heartbeat();
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(25)).await;

        assert!(
            net.tasks
                .lock()
                .expect("peer task lock")
                .iter()
                .all(tokio::task::JoinHandle::is_finished),
            "peer subsystem kept running after its membership heartbeat died"
        );
        assert!(!net.healthy());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replaced_membership_generation_revokes_local_ownership() {
        let (_fake, store) = fake_store().await;
        let address = free_addr();
        let host = HostId::new(28);
        let abstract_store: Arc<dyn ObjectStore> = store.clone();
        let net = PeerNet::start(
            &PeerConfig {
                listen: address,
                advertise: address,
            },
            host,
            abstract_store,
            |_, _| {},
        )
        .await
        .expect("start peer subsystem");
        assert!(net.membership_owned());

        store
            .clone()
            .put(
                peer_membership_key(host),
                b"replacement membership owner".to_vec(),
            )
            .await
            .expect("replace membership");
        tokio::time::timeout(Duration::from_secs(1), net.critical_failure())
            .await
            .expect("replacement stopped peer subsystem");
        assert!(!net.membership_owned());
        assert!(!net.healthy());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn membership_refresher_and_listener_failures_stop_the_peer_subsystem() {
        for (host, stop) in [
            (
                HostId::new(18),
                PeerNet::stop_membership_refresher as fn(&PeerNet),
            ),
            (HostId::new(19), PeerNet::stop_listener as fn(&PeerNet)),
        ] {
            let (_fake, store) = fake_store().await;
            let address = free_addr();
            let net = PeerNet::start(
                &PeerConfig {
                    listen: address,
                    advertise: address,
                },
                host,
                store,
                |_, _| {},
            )
            .await
            .expect("start peer subsystem");
            stop(&net);
            tokio::time::timeout(Duration::from_secs(1), net.critical_failure())
                .await
                .expect("critical failure propagated");
            assert!(!net.healthy());
            assert!(
                net.tasks
                    .lock()
                    .expect("peer task lock")
                    .iter()
                    .all(tokio::task::JoinHandle::is_finished)
            );
        }
    }

    #[test]
    fn inbound_connections_are_bounded_per_authenticated_peer() {
        let counts = Arc::new(Mutex::new(BTreeMap::new()));
        let mut guards = Vec::new();
        for _ in 0..MAX_INBOUND_CONNECTIONS_PER_PEER {
            guards.push(
                enter_peer_connection(HostId::new(9), Arc::clone(&counts))
                    .expect("connection within per-peer limit"),
            );
        }
        assert!(enter_peer_connection(HostId::new(9), Arc::clone(&counts)).is_none());
        assert!(enter_peer_connection(HostId::new(10), Arc::clone(&counts)).is_some());
        drop(guards.pop());
        assert!(enter_peer_connection(HostId::new(9), counts).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn large_rosters_allocate_outbound_resources_only_for_bounded_active_targets() {
        let dropped = Arc::new(AtomicU64::new(0));
        let overload = Arc::new(AtomicU64::new(0));
        let connections = ConnectionSet::new_with_limits(
            HostId::new(0),
            Arc::clone(&dropped),
            Arc::clone(&overload),
            None,
            OutboundLimits {
                workers: 2,
                queued_messages: 3,
                queued_bytes: 64,
                idle_timeout: OUTBOUND_WORKER_IDLE_TIMEOUT,
            },
        );
        let targets = (1..=10_000_u16)
            .map(|host| {
                let host = HostId::new(u32::from(host));
                (
                    host,
                    PeerTarget {
                        identity: host,
                        endpoint: "203.0.113.1:9".parse().expect("test endpoint"),
                        certificate: None,
                        drained: false,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        connections.reconcile(&targets);
        assert_eq!(
            connections
                .peers
                .lock()
                .expect("peer connections lock")
                .len(),
            targets.len()
        );
        assert_eq!(
            connections
                .peers
                .lock()
                .expect("peer connections lock")
                .values()
                .filter(|connection| connection.worker.is_some())
                .count(),
            0,
            "roster discovery must not allocate outbound tasks"
        );

        for host in 1..=2_u16 {
            let identity = targets[&HostId::new(u32::from(host))].identity;
            connections.send(
                identity,
                vec![u8::try_from(host).expect("small host"); 16],
                false,
            );
        }
        connections.send(targets[&HostId::new(1)].identity, vec![3; 33], false);
        connections.send(targets[&HostId::new(3)].identity, vec![4; 16], false);
        connections.send(targets[&HostId::new(1)].identity, vec![4; 16], false);
        connections.send(targets[&HostId::new(1)].identity, vec![5; 16], false);

        let metrics = connections.resource_metrics();
        assert_eq!(metrics.outbound_active_workers, 2);
        assert_eq!(metrics.outbound_buffered_messages, 3);
        assert_eq!(metrics.outbound_buffered_bytes, 48);
        assert_eq!(metrics.outbound_worker_rejections, 1);
        assert_eq!(metrics.outbound_queue_rejections, 2);
        assert_eq!(metrics.overload_rejections, 3);
        assert_eq!(overload.load(Ordering::Relaxed), 3);
        assert_eq!(dropped.load(Ordering::Relaxed), 3);
        assert_eq!(
            connections
                .peers
                .lock()
                .expect("peer connections lock")
                .values()
                .filter(|connection| connection.worker.is_some())
                .count(),
            2,
            "abusive target fanout exceeded the task budget"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn idle_workers_release_global_slots_for_later_peers() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer sink");
        let endpoint = listener.local_addr().expect("peer sink address");
        let sink = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
                });
            }
        });
        let connections = ConnectionSet::new_with_limits(
            HostId::new(0),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            None,
            OutboundLimits {
                workers: 2,
                queued_messages: 8,
                queued_bytes: 128,
                idle_timeout: Duration::from_millis(20),
            },
        );
        let targets = (1..=3_u16)
            .map(|host| {
                let host = HostId::new(u32::from(host));
                (
                    host,
                    PeerTarget {
                        identity: host,
                        endpoint,
                        certificate: None,
                        drained: false,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        connections.reconcile(&targets);
        connections.send(targets[&HostId::new(1)].identity, vec![1; 16], false);
        connections.send(targets[&HostId::new(2)].identity, vec![2; 16], false);
        assert_eq!(connections.resource_metrics().outbound_active_workers, 2);
        wait_for(|| connections.resource_metrics().outbound_active_workers == 0).await;
        assert_eq!(connections.resource_metrics().outbound_buffered_messages, 0);
        assert_eq!(connections.resource_metrics().outbound_buffered_bytes, 0);

        connections.send(targets[&HostId::new(3)].identity, vec![3; 16], false);
        assert_eq!(connections.resource_metrics().outbound_active_workers, 1);
        assert_eq!(connections.resource_metrics().outbound_worker_rejections, 0);
        sink.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retirement_drains_a_frame_accepted_at_the_idle_timeout_boundary() {
        let message_slots = Arc::new(tokio::sync::Semaphore::new(1));
        let byte_slots = Arc::new(tokio::sync::Semaphore::new(8));
        let message_permit = Arc::clone(&message_slots)
            .try_acquire_owned()
            .expect("message permit");
        let byte_permits = Arc::clone(&byte_slots)
            .try_acquire_many_owned(3)
            .expect("byte permits");
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .try_send(OutboundFrame {
                bytes: vec![7, 8, 9],
                reconnect: false,
                _message_permit: message_permit,
                _byte_permits: byte_permits,
            })
            .expect("frame accepted immediately before retirement");
        let (client, mut server) = tokio::io::duplex(16);
        let dropped = Arc::new(AtomicU64::new(0));
        let mut outbound = Outbound {
            rx: receiver,
            addr: "127.0.0.1:1".parse().expect("test endpoint"),
            peer: HostId::new(1),
            connected: Arc::new(AtomicBool::new(true)),
            dropped_sends: Arc::clone(&dropped),
            tls: None,
            conn: Some(Box::new(client)),
            renewal: None,
            idle_timeout: Duration::from_millis(1),
        };

        outbound.drain_accepted_frames().await;
        let mut frame_bytes = [0_u8; 3];
        server
            .read_exact(&mut frame_bytes)
            .await
            .expect("boundary frame delivered");
        assert_eq!(frame_bytes, [7, 8, 9]);
        assert!(sender.is_closed(), "retired worker queue remained open");
        assert_eq!(message_slots.available_permits(), 1);
        assert_eq!(byte_slots.available_permits(), 8);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn node_joins_while_existing_operations_continue() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        let to_unknown = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(2));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;
        assert!(
            !node_0
                .net
                .connections()
                .into_iter()
                .any(|(host, _)| host == HostId::new(2))
        );

        let node_2 = start_node(HostId::new(2), &addresses, store).await;
        let joining = spawn_traffic(node_2.net.clone(), HostId::new(2), HostId::new(1));
        assert_progress(&node_1.deliveries, HostId::new(0)).await;
        wait_for_count(&node_1.deliveries, HostId::new(2), 3).await;
        wait_for_count(&node_2.deliveries, HostId::new(0), 3).await;
        wait_for_peer(&node_0.net, HostId::new(2), true).await;

        steady.abort();
        to_unknown.abort();
        joining.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multiple_nodes_join_concurrently_during_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(4);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;

        let (node_2, node_3) = tokio::join!(
            start_node(HostId::new(2), &addresses, store.clone()),
            start_node(HostId::new(3), &addresses, store)
        );
        let join_2 = spawn_traffic(node_2.net.clone(), HostId::new(2), HostId::new(1));
        let join_3 = spawn_traffic(node_3.net.clone(), HostId::new(3), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(2), 3).await;
        wait_for_count(&node_1.deliveries, HostId::new(3), 3).await;
        assert_progress(&node_1.deliveries, HostId::new(0)).await;

        steady.abort();
        join_2.abort();
        join_3.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn certificate_deletion_revokes_a_live_node_without_stopping_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let node_2 = start_node(HostId::new(2), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        let leaving = spawn_traffic(node_2.net.clone(), HostId::new(2), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;
        wait_for_count(&node_1.deliveries, HostId::new(2), 3).await;

        store
            .clone()
            .delete(peer_membership_key(HostId::new(2)))
            .await
            .expect("delete membership fixture");
        let revoked_at = wait_until_quiet(&node_1.deliveries, HostId::new(2)).await;
        wait_for_peer(&node_1.net, HostId::new(2), false).await;
        assert_progress(&node_1.deliveries, HostId::new(0)).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_1.deliveries.from(HostId::new(2)), revoked_at);

        steady.abort();
        leaving.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crashed_node_restarts_with_a_fresh_certificate_during_operations() {
        let (_fake, store) = fake_store().await;
        let mut addresses = addresses(3);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let node_2 = start_node(HostId::new(2), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        let before_restart = spawn_traffic(node_2.net.clone(), HostId::new(2), HostId::new(1));
        let toward_restarting = spawn_traffic(node_1.net.clone(), HostId::new(1), HostId::new(2));
        wait_for_count(&node_1.deliveries, HostId::new(2), 3).await;
        wait_for_count(&node_2.deliveries, HostId::new(1), 3).await;
        let old_member = stored_member(&store, HostId::new(2)).await;

        before_restart.abort();
        let _ = before_restart.await;
        drop(node_2);
        addresses.insert(HostId::new(2), free_addr());
        let node_2 = start_node(HostId::new(2), &addresses, store.clone()).await;
        let new_member = stored_member(&store, HostId::new(2)).await;
        assert_ne!(old_member.certificate, new_member.certificate);
        assert_ne!(old_member.endpoint, new_member.endpoint);
        let after_restart = spawn_traffic(node_2.net.clone(), HostId::new(2), HostId::new(1));
        let before = node_1.deliveries.from(HostId::new(2));
        wait_for_count(&node_1.deliveries, HostId::new(2), before + 3).await;
        wait_for_count(&node_2.deliveries, HostId::new(1), 3).await;
        assert_progress(&node_1.deliveries, HostId::new(0)).await;

        steady.abort();
        after_restart.abort();
        toward_restarting.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn quick_restart_renews_membership_without_a_roster_gap() {
        let (_fake, store) = fake_store().await;
        let mut addresses = addresses(2);
        let snapshots = Arc::new(Mutex::new(Vec::<Vec<HostId>>::new()));
        let observed_snapshots = Arc::clone(&snapshots);
        let deliveries = DeliveryCounts::default();
        let delivered = deliveries.clone();
        let observer = PeerNet::start_with_membership(
            &PeerConfig {
                listen: addresses[&HostId::new(0)],
                advertise: addresses[&HostId::new(0)],
            },
            HostId::new(0),
            store.clone(),
            move |from, _| delivered.record(from),
            move |members| {
                observed_snapshots
                    .lock()
                    .expect("membership snapshots lock")
                    .push(members);
            },
        )
        .await
        .expect("start observer");
        let restarting = start_node(HostId::new(1), &addresses, store.clone()).await;

        timeout(Duration::from_secs(3), async {
            loop {
                if snapshots
                    .lock()
                    .expect("membership snapshots lock")
                    .iter()
                    .any(|members| members.contains(&HostId::new(1)))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("restarting host admitted");
        let old_member = stored_member(&store, HostId::new(1)).await;
        let admitted_at = snapshots
            .lock()
            .expect("membership snapshots lock")
            .iter()
            .position(|members| members.contains(&HostId::new(1)))
            .expect("admission snapshot");

        drop(restarting);
        addresses.insert(HostId::new(1), free_addr());
        let restarted = start_node(HostId::new(1), &addresses, store.clone()).await;
        let new_member = stored_member(&store, HostId::new(1)).await;
        assert_ne!(old_member.certificate, new_member.certificate);

        let traffic = spawn_traffic(restarted.net.clone(), HostId::new(1), HostId::new(0));
        wait_for_count(&deliveries, HostId::new(1), 3).await;

        let roster_history = snapshots.lock().expect("membership snapshots lock").clone();
        assert!(
            roster_history[admitted_at..]
                .iter()
                .all(|members| members.contains(&HostId::new(1))),
            "quick restart must not remove the host from membership: {roster_history:?}"
        );

        traffic.abort();
        drop(observer);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_certificate_chaos_isolated_and_heals_during_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let node_2 = start_node(HostId::new(2), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        let chaotic = spawn_traffic(node_2.net.clone(), HostId::new(2), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(2), 3).await;
        let member = stored_member(&store, HostId::new(2)).await;

        let duplicate = spawn_member_renewal(store.clone(), HostId::new(9), member);
        wait_for_peer(&node_1.net, HostId::new(2), false).await;
        wait_until_quiet(&node_1.deliveries, HostId::new(2)).await;
        let isolated_at = node_1.deliveries.from(HostId::new(2));
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert_eq!(node_1.deliveries.from(HostId::new(2)), isolated_at);
        assert_progress(&node_1.deliveries, HostId::new(0)).await;

        duplicate.abort();
        let _ = duplicate.await;
        store
            .clone()
            .delete(peer_membership_key(HostId::new(9)))
            .await
            .expect("delete membership fixture");
        wait_for_count(&node_1.deliveries, HostId::new(2), isolated_at + 3).await;
        assert_progress(&node_1.deliveries, HostId::new(0)).await;

        steady.abort();
        chaotic.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn repeated_join_leave_chaos_preserves_surviving_traffic() {
        let (_fake, store) = fake_store().await;
        let mut addresses = addresses(3);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;

        for _ in 0..3 {
            let node_2 = start_node(HostId::new(2), &addresses, store.clone()).await;
            let churn = spawn_traffic(node_2.net.clone(), HostId::new(2), HostId::new(1));
            let joined_at = node_1.deliveries.from(HostId::new(2));
            wait_for_count(&node_1.deliveries, HostId::new(2), joined_at + 3).await;
            store
                .clone()
                .delete(peer_membership_key(HostId::new(2)))
                .await
                .expect("delete membership fixture");
            wait_until_quiet(&node_1.deliveries, HostId::new(2)).await;
            assert_progress(&node_1.deliveries, HostId::new(0)).await;
            churn.abort();
            let _ = churn.await;
            drop(node_2);
            addresses.insert(HostId::new(2), free_addr());
        }

        steady.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn certificate_replacement_at_same_endpoint_revokes_old_server_during_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_2 = start_node(HostId::new(2), &addresses, store.clone()).await;
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        let toward_replaced = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(2));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;
        wait_for_count(&node_2.deliveries, HostId::new(0), 3).await;

        let old_member = stored_member(&store, HostId::new(2)).await;
        let CertifiedKey { cert, .. } =
            generate_simple_self_signed(vec![DEFAULT_PEER_SERVER_NAME.to_owned()])
                .expect("generate replacement identity");
        let replacement = MemberRecord {
            endpoint: old_member.endpoint,
            certificate: cert.der().to_vec(),
            drained: false,
        };
        assert_ne!(replacement.certificate, old_member.certificate);
        store
            .clone()
            .put(peer_membership_key(HostId::new(2)), replacement.encode())
            .await
            .expect("replace certificate at the same endpoint");

        timeout(Duration::from_secs(3), async {
            loop {
                let discovered = node_0
                    .net
                    .connections
                    .peers
                    .lock()
                    .expect("peer connections lock")
                    .get(&HostId::new(2))
                    .map(|connection| connection.target.clone());
                if discovered.as_ref().is_some_and(|target| {
                    target.certificate.as_ref() == Some(&replacement.certificate)
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replacement certificate discovered");
        let revoked_at = wait_until_quiet(&node_2.deliveries, HostId::new(0)).await;
        assert_progress(&node_1.deliveries, HostId::new(0)).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_2.deliveries.from(HostId::new(0)), revoked_at);

        steady.abort();
        toward_replaced.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_host_process_replaces_endpoint_and_revokes_old_process() {
        let (_fake, store) = fake_store().await;
        let mut first_addresses = addresses(3);
        let node_1 = start_node(HostId::new(1), &first_addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &first_addresses, store.clone()).await;
        let old_node_2 = start_node(HostId::new(2), &first_addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        let old_traffic = spawn_traffic(old_node_2.net.clone(), HostId::new(2), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(2), 3).await;

        let replacement_endpoint = free_addr();
        first_addresses.insert(HostId::new(2), replacement_endpoint);
        let new_node_2 = start_node(HostId::new(2), &first_addresses, store.clone()).await;
        let new_traffic = spawn_traffic(new_node_2.net.clone(), HostId::new(2), HostId::new(1));
        let before_replacement = node_1.deliveries.from(HostId::new(2));
        wait_for_count(&node_1.deliveries, HostId::new(2), before_replacement + 3).await;
        timeout(Duration::from_secs(3), async {
            loop {
                let endpoint = node_1
                    .net
                    .connections
                    .peers
                    .lock()
                    .expect("peer connections lock")
                    .get(&HostId::new(2))
                    .map(|connection| connection.target.endpoint);
                if endpoint == Some(replacement_endpoint) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("replacement endpoint discovered");

        new_traffic.abort();
        wait_until_quiet(&node_1.deliveries, HostId::new(2)).await;
        assert_progress(&node_1.deliveries, HostId::new(0)).await;
        let resumed = spawn_traffic(new_node_2.net.clone(), HostId::new(2), HostId::new(1));
        assert_progress(&node_1.deliveries, HostId::new(2)).await;

        steady.abort();
        old_traffic.abort();
        resumed.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_membership_chaos_fails_closed_during_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;

        store
            .clone()
            .put(peer_membership_key(HostId::new(9)), vec![0xff; 64])
            .await
            .expect("publish malformed member");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !node_0
                .net
                .connections()
                .into_iter()
                .any(|(host, _)| host == HostId::new(9))
        );
        assert_progress(&node_1.deliveries, HostId::new(0)).await;

        steady.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::too_many_lines)]
    async fn seeded_membership_chaos_corpus_preserves_steady_operations() {
        for seed in [1_u64, 7, 19, 41] {
            let (fake, store) = fake_store().await;
            let mut addresses = addresses(3);
            let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
            let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
            let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
            wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;
            let mut node_2 = None::<TestNode>;
            let mut churn = None::<tokio::task::JoinHandle<()>>;
            let mut state = seed;

            for _ in 0..12 {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                match state % 6 {
                    0 => {
                        if node_2.is_none() {
                            let joined =
                                start_node(HostId::new(2), &addresses, store.clone()).await;
                            let before = node_1.deliveries.from(HostId::new(2));
                            churn = Some(spawn_traffic(
                                joined.net.clone(),
                                HostId::new(2),
                                HostId::new(1),
                            ));
                            node_2 = Some(joined);
                            wait_for_count(&node_1.deliveries, HostId::new(2), before + 3).await;
                        }
                    }
                    1 => {
                        if node_2.is_some() {
                            store
                                .clone()
                                .delete(peer_membership_key(HostId::new(2)))
                                .await
                                .expect("delete membership fixture");
                            wait_until_quiet(&node_1.deliveries, HostId::new(2)).await;
                            if let Some(task) = churn.take() {
                                task.abort();
                                let _ = task.await;
                            }
                            drop(node_2.take());
                            addresses.insert(HostId::new(2), free_addr());
                        }
                    }
                    2 => {
                        if node_2.is_some() {
                            if let Some(task) = churn.take() {
                                task.abort();
                                let _ = task.await;
                            }
                            drop(node_2.take());
                            addresses.insert(HostId::new(2), free_addr());
                            let restarted =
                                start_node(HostId::new(2), &addresses, store.clone()).await;
                            let before = node_1.deliveries.from(HostId::new(2));
                            churn = Some(spawn_traffic(
                                restarted.net.clone(),
                                HostId::new(2),
                                HostId::new(1),
                            ));
                            node_2 = Some(restarted);
                            wait_for_count(&node_1.deliveries, HostId::new(2), before + 3).await;
                        }
                    }
                    3 => {
                        if node_2.is_some() {
                            let member = stored_member(&store, HostId::new(2)).await;
                            let duplicate =
                                spawn_member_renewal(store.clone(), HostId::new(9), member);
                            wait_for_peer(&node_1.net, HostId::new(2), false).await;
                            wait_until_quiet(&node_1.deliveries, HostId::new(2)).await;
                            wait_for_peer(&node_1.net, HostId::new(2), false).await;
                            let isolated = node_1.deliveries.from(HostId::new(2));
                            tokio::time::sleep(Duration::from_millis(75)).await;
                            assert_eq!(node_1.deliveries.from(HostId::new(2)), isolated);
                            duplicate.abort();
                            let _ = duplicate.await;
                            store
                                .clone()
                                .delete(peer_membership_key(HostId::new(9)))
                                .await
                                .expect("delete membership fixture");
                            wait_for_count(&node_1.deliveries, HostId::new(2), isolated + 3).await;
                        }
                    }
                    4 => {
                        store
                            .clone()
                            .put(peer_membership_key(HostId::new(9)), vec![0xa5; 48])
                            .await
                            .expect("publish malformed member");
                        tokio::time::sleep(Duration::from_millis(75)).await;
                        store
                            .clone()
                            .delete(peer_membership_key(HostId::new(9)))
                            .await
                            .expect("delete membership fixture");
                    }
                    _ => {
                        fake.outage.store(true, Ordering::SeqCst);
                        assert_progress(&node_1.deliveries, HostId::new(0)).await;
                        fake.outage.store(false, Ordering::SeqCst);
                    }
                }
                assert_progress(&node_1.deliveries, HostId::new(0)).await;
            }

            if let Some(task) = churn {
                task.abort();
            }
            steady.abort();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn membership_store_outage_during_leave_preserves_existing_operations() {
        let (fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        let node_2 = start_node(HostId::new(2), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        let leaving = spawn_traffic(node_2.net.clone(), HostId::new(2), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(2), 3).await;

        store
            .clone()
            .delete(peer_membership_key(HostId::new(2)))
            .await
            .expect("delete membership fixture");
        fake.outage.store(true, Ordering::SeqCst);
        assert_progress(&node_1.deliveries, HostId::new(0)).await;
        fake.outage.store(false, Ordering::SeqCst);
        wait_until_quiet(&node_1.deliveries, HostId::new(2)).await;
        assert_progress(&node_1.deliveries, HostId::new(0)).await;

        steady.abort();
        leaving.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prolonged_membership_store_outage_expires_cached_authorization_and_recovers() {
        let (fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store).await;
        let traffic = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;

        fake.outage.store(true, Ordering::SeqCst);
        let expired_at = wait_until_quiet(&node_1.deliveries, HostId::new(0)).await;
        wait_for_peer(&node_1.net, HostId::new(0), false).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_1.deliveries.from(HostId::new(0)), expired_at);

        fake.outage.store(false, Ordering::SeqCst);
        wait_for_count(&node_1.deliveries, HostId::new(0), expired_at + 3).await;
        wait_for_peer(&node_1.net, HostId::new(0), true).await;

        traffic.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crashed_node_membership_expires_even_though_its_object_persists() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store.clone()).await;
        wait_for_peer(&node_1.net, HostId::new(0), true).await;

        drop(node_0);
        wait_for_peer(&node_1.net, HostId::new(0), false).await;
        assert!(
            store
                .clone()
                .get(peer_membership_key(HostId::new(0)))
                .await
                .expect("read crashed member")
                .is_some(),
            "lease expiry must not depend on crash-time cleanup"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn node_that_cannot_renew_membership_is_revoked_while_still_running() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store).await;
        let traffic = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;

        node_0.net.stop_membership_heartbeat();
        let revoked_at = wait_until_quiet(&node_1.deliveries, HostId::new(0)).await;
        wait_for_peer(&node_1.net, HostId::new(0), false).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_1.deliveries.from(HostId::new(0)), revoked_at);

        traffic.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_membership_refresh_cannot_bypass_authorization_expiry() {
        let (fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId::new(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId::new(0), &addresses, store).await;
        let traffic = spawn_traffic(node_0.net.clone(), HostId::new(0), HostId::new(1));
        wait_for_count(&node_1.deliveries, HostId::new(0), 3).await;

        fake.latency_ms.store(1_000, Ordering::SeqCst);
        let expired_at = wait_until_quiet(&node_1.deliveries, HostId::new(0)).await;
        wait_for_peer(&node_1.net, HostId::new(0), false).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_1.deliveries.from(HostId::new(0)), expired_at);

        fake.latency_ms.store(0, Ordering::SeqCst);
        wait_for_count(&node_1.deliveries, HostId::new(0), expired_at + 3).await;
        wait_for_peer(&node_1.net, HostId::new(0), true).await;

        traffic.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn release_ack_reconnects_after_the_destination_restarts() {
        use blockd_core::protocol::ReplicaCommitInfo;
        use blockd_core::types::JournalSeq;

        let addresses = addresses(2);
        let config_a = PeerConfig {
            listen: addresses[&HostId::new(0)],
            advertise: addresses[&HostId::new(0)],
        };
        let config_b = PeerConfig {
            listen: addresses[&HostId::new(1)],
            advertise: addresses[&HostId::new(1)],
        };
        let (first_send, mut first_receive) = tokio::sync::mpsc::unbounded_channel();
        let first = PeerNet::start_plaintext(
            &config_a,
            HostId::new(0),
            addresses.clone(),
            move |from, message| {
                let _ = first_send.send((from, message));
            },
        )
        .await
        .expect("start first destination");
        let source =
            PeerNet::start_plaintext(&config_b, HostId::new(1), addresses.clone(), |_, _| {})
                .await
                .expect("start source");

        let warmup = PeerMsg::ReleasedAck {
            volume: VolumeId(7),
            release_fence: 3,
        };
        source.send(HostId::new(1), HostId::new(0), &warmup);
        assert_eq!(
            timeout(Duration::from_secs(5), first_receive.recv())
                .await
                .expect("warmup delivery timeout")
                .expect("warmup receiver closed"),
            (HostId::new(1), warmup)
        );
        drop(first);
        tokio::task::yield_now().await;

        let (restarted_send, mut restarted_receive) = tokio::sync::mpsc::unbounded_channel();
        let restarted = loop {
            let restarted_send = restarted_send.clone();
            match PeerNet::start_plaintext(
                &config_a,
                HostId::new(0),
                addresses.clone(),
                move |from, message| {
                    let _ = restarted_send.send((from, message));
                },
            )
            .await
            {
                Ok(peer) => break peer,
                Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("restart destination: {error}"),
            }
        };
        let ack = PeerMsg::ReplicaReleaseAck {
            volume: VolumeId(7),
            assignment_epoch: 2,
            through: ReplicaCommitInfo {
                writer_fence: 4,
                seq: JournalSeq(5),
                sync_covered_through: 6,
            },
        };
        source.send(HostId::new(1), HostId::new(0), &ack);
        assert_eq!(
            timeout(Duration::from_secs(5), restarted_receive.recv())
                .await
                .expect("release ack delivery timeout")
                .expect("restarted receiver closed"),
            (HostId::new(1), ack)
        );

        drop(restarted);
        drop(source);
    }

    #[test]
    fn membership_directory_accepts_only_canonical_host_keys() {
        assert_eq!(
            host_from_membership_key("cluster/tls/public-keys/000000af.member"),
            Some(HostId::new(0x00af))
        );
        assert_eq!(
            host_from_membership_key("cluster/tls/public-keys/00af.member"),
            None
        );
        assert_eq!(
            host_from_membership_key("cluster/tls/public-keys/000000AF.member"),
            None
        );
        assert_eq!(
            host_from_membership_key("cluster/tls/public-keys/000000af.member/extra"),
            None
        );
    }

    #[test]
    fn membership_record_is_a_framed_protobuf_endpoint() {
        #[derive(Clone, PartialEq, prost::Message)]
        struct Probe {
            #[prost(uint32, tag = "1")]
            version: u32,
            #[prost(bool, tag = "2")]
            drained: bool,
            #[prost(oneof = "probe::Address", tags = "3, 4")]
            address: Option<probe::Address>,
            #[prost(uint32, tag = "5")]
            port: u32,
            #[prost(uint32, tag = "6")]
            flowinfo: u32,
            #[prost(uint32, tag = "7")]
            scope_id: u32,
            #[prost(bytes = "vec", tag = "8")]
            certificate: Vec<u8>,
        }

        mod probe {
            #[derive(Clone, PartialEq, prost::Oneof)]
            pub enum Address {
                #[prost(bytes, tag = "3")]
                Ipv4(Vec<u8>),
                #[prost(bytes, tag = "4")]
                Ipv6(Vec<u8>),
            }
        }

        let record = MemberRecord {
            endpoint: "127.0.0.1:7001".parse().expect("endpoint"),
            certificate: vec![1, 2, 3],
            drained: false,
        };
        let encoded = record.encode();
        let payload =
            blockd_core::format::open_frame(MEMBERSHIP_MAGIC, &encoded).expect("membership frame");
        let probe = Probe::decode(payload).expect("protobuf membership payload");

        assert_eq!(probe.version, 1);
        assert_eq!(
            probe.address,
            Some(probe::Address::Ipv4(vec![127, 0, 0, 1]))
        );
        assert_eq!(probe.port, 7001);
        assert_eq!(probe.certificate, [1, 2, 3]);
    }

    #[test]
    fn membership_record_round_trips_ipv6_scope_and_rejects_malformed_bytes() {
        let record = MemberRecord {
            endpoint: SocketAddr::V6(std::net::SocketAddrV6::new(
                "fe80::1234".parse().expect("IPv6"),
                8443,
                7,
                9,
            )),
            certificate: vec![9; 32],
            drained: false,
        };
        let encoded = record.encode();
        assert_eq!(MemberRecord::decode(&encoded), Some(record));

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 1;
        assert_eq!(MemberRecord::decode(&bad_magic), None);
        let payload =
            blockd_core::format::open_frame(MEMBERSHIP_MAGIC, &encoded).expect("membership frame");
        let mut zero_port_wire = MemberRecordWire::decode(payload).expect("membership payload");
        zero_port_wire.port = 0;
        let zero_port =
            blockd_core::format::seal_frame(MEMBERSHIP_MAGIC, &zero_port_wire.encode_to_vec());
        assert_eq!(MemberRecord::decode(&zero_port), None);
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(MemberRecord::decode(&trailing), None);
    }
}
