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
use blockd_core::peer::encode_peer;
use blockd_core::protocol::{PeerMsg, StoreFault};
use blockd_core::types::HostId;
use blockd_transport::{DecodePolicy, receive_loop, receive_loop_while_authorized, write_frame};
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

use crate::store::ObjectStore;

/// What the transport does with a verified inbound message.
type Deliver = dyn Fn(HostId, PeerMsg) + Send + Sync;
type MembershipChanged = dyn Fn(Vec<HostId>) + Send + Sync;

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
    own_record: Arc<Vec<u8>>,
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
const MEMBERSHIP_HEADER_BYTES: usize = 4 + 1 + 2 + 16 + 4 + 4 + 4;
const MAX_MEMBERSHIP_RECORD_BYTES: usize = MEMBERSHIP_HEADER_BYTES + MAX_PEER_CERTIFICATE_BYTES;

#[derive(Clone, Debug, PartialEq, Eq)]
struct MemberRecord {
    endpoint: SocketAddr,
    certificate: Vec<u8>,
}

impl MemberRecord {
    fn encode(&self) -> Vec<u8> {
        assert!(
            !self.certificate.is_empty() && self.certificate.len() <= MAX_PEER_CERTIFICATE_BYTES,
            "peer certificate size"
        );
        let certificate_len =
            u32::try_from(self.certificate.len()).expect("certificate length fits");
        let mut encoded = Vec::with_capacity(MEMBERSHIP_HEADER_BYTES + self.certificate.len());
        encoded.extend_from_slice(&MEMBERSHIP_MAGIC.to_le_bytes());
        match self.endpoint {
            SocketAddr::V4(endpoint) => {
                encoded.push(4);
                encoded.extend_from_slice(&endpoint.port().to_le_bytes());
                encoded.extend_from_slice(&endpoint.ip().octets());
                encoded.extend_from_slice(&[0; 12]);
                encoded.extend_from_slice(&0_u32.to_le_bytes());
                encoded.extend_from_slice(&0_u32.to_le_bytes());
            }
            SocketAddr::V6(endpoint) => {
                encoded.push(6);
                encoded.extend_from_slice(&endpoint.port().to_le_bytes());
                encoded.extend_from_slice(&endpoint.ip().octets());
                encoded.extend_from_slice(&endpoint.flowinfo().to_le_bytes());
                encoded.extend_from_slice(&endpoint.scope_id().to_le_bytes());
            }
        }
        encoded.extend_from_slice(&certificate_len.to_le_bytes());
        encoded.extend_from_slice(&self.certificate);
        encoded
    }

    fn decode(encoded: &[u8]) -> Option<Self> {
        if encoded.len() < MEMBERSHIP_HEADER_BYTES {
            return None;
        }
        let magic = u32::from_le_bytes(encoded[0..4].try_into().ok()?);
        if magic != MEMBERSHIP_MAGIC {
            return None;
        }
        let family = encoded[4];
        let port = u16::from_le_bytes(encoded[5..7].try_into().ok()?);
        if port == 0 {
            return None;
        }
        let address: [u8; 16] = encoded[7..23].try_into().ok()?;
        let flowinfo = u32::from_le_bytes(encoded[23..27].try_into().ok()?);
        let scope_id = u32::from_le_bytes(encoded[27..31].try_into().ok()?);
        let certificate_len =
            usize::try_from(u32::from_le_bytes(encoded[31..35].try_into().ok()?)).ok()?;
        if certificate_len == 0
            || certificate_len > MAX_PEER_CERTIFICATE_BYTES
            || encoded.len() != MEMBERSHIP_HEADER_BYTES + certificate_len
        {
            return None;
        }
        let endpoint = match family {
            4 if address[4..] == [0; 12] && flowinfo == 0 && scope_id == 0 => SocketAddr::new(
                std::net::Ipv4Addr::from(<[u8; 4]>::try_from(&address[..4]).ok()?).into(),
                port,
            ),
            6 => SocketAddr::V6(std::net::SocketAddrV6::new(
                std::net::Ipv6Addr::from(address),
                port,
                flowinfo,
                scope_id,
            )),
            _ => return None,
        };
        Some(Self {
            endpoint,
            certificate: encoded[MEMBERSHIP_HEADER_BYTES..].to_vec(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PeerTarget {
    endpoint: SocketAddr,
    /// `None` is reserved for the plaintext integration-test fixture.
    certificate: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
struct MembershipLeases(Arc<Mutex<BTreeMap<HostId, ObservedLease>>>);

#[derive(Clone, Copy, Debug)]
struct ObservedLease {
    generation: u64,
    renewed_at: Option<Instant>,
}

impl PeerTlsConfig {
    /// Generate an ephemeral node identity, publish its public certificate,
    /// and use the object-store directory as the live cluster trust set.
    async fn generate_in_object_store(
        store: Arc<dyn ObjectStore>,
        host: HostId,
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
        };
        let own_key = peer_membership_key(host);
        let own_record = Arc::new(member.encode());
        let generation = Arc::clone(&store)
            .put(own_key.clone(), own_record.as_ref().clone())
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
    let host = (id.len() == 4)
        .then(|| u16::from_str_radix(id, 16).ok().map(HostId))
        .flatten()?;
    (key == peer_membership_key(host)).then_some(host)
}

async fn refresh_bucket_membership(
    store: &Arc<dyn ObjectStore>,
    identities: &PeerIdentities,
    leases: &MembershipLeases,
) -> Result<BTreeMap<HostId, PeerTarget>, StoreFault> {
    let prefix = peer_membership_prefix();
    let keys = Arc::clone(store).list_prefix(prefix).await?;
    let mut members = BTreeMap::<HostId, (u64, Instant, MemberRecord)>::new();
    for key in keys {
        let Some(host) = host_from_membership_key(&key) else {
            continue;
        };
        let Some((generation, encoded)) = Arc::clone(store)
            .get_range(
                key,
                0,
                u64::try_from(MAX_MEMBERSHIP_RECORD_BYTES + 1)
                    .expect("membership read bound fits u64"),
            )
            .await?
        else {
            continue;
        };
        if encoded.len() > MAX_MEMBERSHIP_RECORD_BYTES {
            continue;
        }
        let Some(member) = MemberRecord::decode(&encoded) else {
            continue;
        };
        members.insert(host, (generation, Instant::now(), member));
    }
    let now = Instant::now();
    {
        let mut observed = leases.0.lock().expect("membership leases lock");
        observed.retain(|host, _| members.contains_key(host));
        members.retain(|host, (generation, observed_at, _)| {
            let lease = observed.entry(*host).or_insert(ObservedLease {
                generation: *generation,
                renewed_at: None,
            });
            if lease.generation != *generation {
                *lease = ObservedLease {
                    generation: *generation,
                    renewed_at: Some(*observed_at),
                };
            }
            lease
                .renewed_at
                .is_some_and(|renewed_at| now.duration_since(renewed_at) < MAX_MEMBERSHIP_STALENESS)
        });
    }
    let mut certificates = BTreeMap::<Vec<u8>, Option<HostId>>::new();
    for (&host, (_, _, member)) in &members {
        certificates
            .entry(member.certificate.clone())
            .and_modify(|identity| *identity = None)
            .or_insert(Some(host));
    }
    let certificate_identities = certificates
        .into_iter()
        .filter_map(|(certificate, host)| host.map(|host| (certificate, host)))
        .collect::<BTreeMap<_, _>>();
    members.retain(|host, (_, _, member)| {
        certificate_identities.get(&member.certificate) == Some(host)
    });
    identities.replace(certificate_identities);
    Ok(members
        .into_iter()
        .map(|(host, (_, _, member))| {
            (
                host,
                PeerTarget {
                    endpoint: member.endpoint,
                    certificate: Some(member.certificate),
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

/// Bound how long an outbound connection can retain an old peer certificate.
/// Renewal is prepared in parallel while the established stream keeps serving
/// frames, so rotation never puts a handshake in the next frame's path.
const MAX_CONNECTION_AGE: Duration = Duration::from_mins(5);

/// Large frames contain replica artifacts. Their checksum and owned payload
/// decode are CPU/memory work and must not monopolize the peer I/O thread.
const DECODE_OFFLOAD_THRESHOLD: usize = 1024 * 1024;

pub struct PeerNet {
    connections: ConnectionSet,
    /// Frames dropped on the floor (queue full or peer down) — the demo's
    /// visibility into how hard the retry timers are working.
    pub dropped_sends: Arc<AtomicU64>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    authenticated: bool,
}

struct PeerConnection {
    target: PeerTarget,
    sender: Sender<OutboundFrame>,
    connected: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

struct OutboundFrame {
    bytes: Vec<u8>,
    reconnect: bool,
}

#[derive(Clone)]
struct ConnectionSet {
    peers: Arc<Mutex<BTreeMap<HostId, PeerConnection>>>,
    self_id: HostId,
    dropped_sends: Arc<AtomicU64>,
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
}

impl Drop for PeerConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ConnectionSet {
    fn new(self_id: HostId, dropped_sends: Arc<AtomicU64>, tls: Option<PeerTlsConfig>) -> Self {
        Self {
            peers: Arc::new(Mutex::new(BTreeMap::new())),
            self_id,
            dropped_sends,
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
            let endpoint = target.endpoint;
            let (sender, receiver) = tokio::sync::mpsc::channel::<OutboundFrame>(SEND_QUEUE);
            let connected = Arc::new(AtomicBool::new(false));
            let client_tls = self.tls.as_ref().map(|tls| ClientTls {
                config: tls.client.clone(),
                identities: tls.certificate_identities.clone(),
                expected: peer,
            });
            let task_connected = connected.clone();
            let dropped = self.dropped_sends.clone();
            let task = tokio::spawn(async move {
                Outbound::new(
                    receiver,
                    endpoint,
                    peer,
                    task_connected,
                    dropped,
                    client_tls,
                )
                .run()
                .await;
            });
            peers.insert(
                peer,
                PeerConnection {
                    target: target.clone(),
                    sender,
                    connected,
                    task,
                },
            );
        }
    }
}

async fn publish_membership_heartbeat(membership: BucketMembership) {
    loop {
        tokio::time::sleep(MEMBERSHIP_HEARTBEAT_INTERVAL).await;
        let expected = *membership
            .generation
            .lock()
            .expect("membership generation lock");
        match timeout(
            MEMBERSHIP_OPERATION_TIMEOUT,
            Arc::clone(&membership.store).put_cas(
                membership.own_key.clone(),
                Some(expected),
                membership.own_record.as_ref().clone(),
            ),
        )
        .await
        {
            Ok(Ok(generation)) => {
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
                        if record.as_slice() == membership.own_record.as_slice() =>
                    {
                        *membership
                            .generation
                            .lock()
                            .expect("membership generation lock") = generation;
                    }
                    Ok(Ok(_)) => {
                        tracing::warn!(
                            membership_key = membership.own_key,
                            "peer membership ownership was replaced"
                        );
                        return;
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(?error, "failed to verify peer membership ownership");
                    }
                    Err(_) => tracing::warn!("peer membership ownership check timed out"),
                }
            }
            Ok(Err(StoreFault::Unavailable)) => {
                tracing::warn!("failed to renew peer TLS membership");
            }
            Err(_) => tracing::warn!("peer TLS membership renewal timed out"),
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
                    let members = targets.keys().copied().collect::<Vec<_>>();
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
        if config.listen.port() == 0
            || config.advertise.port() == 0
            || config.advertise.ip().is_unspecified()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "peer listen port and reachable advertised endpoint are required",
            ));
        }
        let listener = TcpListener::bind(config.listen).await?;
        let tls = PeerTlsConfig::generate_in_object_store(store, self_id, config.advertise)
            .await
            .expect("publish peer TLS identity");
        let initial_targets = tls.initial_targets.clone();
        let membership_changed = Arc::new(membership_changed) as Arc<MembershipChanged>;
        let initial_members = initial_targets.keys().copied().collect::<Vec<_>>();
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
                        endpoint,
                        certificate: None,
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
        let dropped_sends = Arc::new(AtomicU64::new(0));
        let connections = ConnectionSet::new(self_id, dropped_sends.clone(), tls.cloned());
        let mut tasks = Vec::new();
        connections.reconcile(initial_targets);
        let deliver: Arc<Deliver> = Arc::new(deliver);
        let server_tls = tls.map(|tls| ServerTls {
            config: tls.server.clone(),
            identities: tls.certificate_identities.clone(),
        });
        if let Some(tls) = tls {
            tasks.push(tokio::spawn(publish_membership_heartbeat(
                tls.membership.clone(),
            )));
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
                .run(),
            ));
        }
        tasks.push(tokio::spawn(listener_loop(listener, deliver, server_tls)));
        Arc::new(PeerNet {
            connections,
            dropped_sends,
            tasks: Mutex::new(tasks),
            authenticated: tls.is_some(),
        })
    }

    pub fn authenticated(&self) -> bool {
        self.authenticated
    }

    /// Fire-and-forget: encode and queue. Never blocks the daemon thread;
    /// unknown peers and full queues drop the frame (retries re-drive).
    pub fn send(&self, self_id: HostId, to: HostId, msg: &PeerMsg) {
        let sender = self
            .connections
            .peers
            .lock()
            .expect("peer connections lock")
            .get(&to)
            .map(|peer| peer.sender.clone());
        let Some(sender) = sender else {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
            return;
        };
        let frame = OutboundFrame {
            bytes: encode_peer(self_id, msg),
            // Replica release is rare and may be initiated by a restarted
            // primary. Its acknowledgement must not remain on an otherwise
            // healthy-looking connection to the prior process instance.
            reconnect: matches!(msg, PeerMsg::ReplicaReleaseAck { .. }),
        };
        if let Err(TrySendError::Full(_) | TrySendError::Closed(_)) = sender.try_send(frame) {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn connections(&self) -> Vec<(HostId, bool)> {
        self.connections
            .peers
            .lock()
            .expect("peer connections lock")
            .iter()
            .map(|(&peer, connection)| (peer, connection.connected.load(Ordering::Relaxed)))
            .collect()
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
                        if frame.reconnect {
                            if let Some(task) = self.renewal.take() {
                                task.abort();
                            }
                            self.conn = connect(self.addr, self.tls.as_ref()).await;
                            self.connected.store(self.conn.is_some(), Ordering::Relaxed);
                            if self.conn.is_some() {
                                self.renewal = Some(spawn_connection_renewal(self.addr, self.tls.clone()));
                            }
                        }
                        if !write_peer_frame(&mut self.conn, &frame.bytes).await {
                            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
                            self.connected.store(false, Ordering::Relaxed);
                            tracing::warn!(peer_host = self.peer.0, addr = %self.addr, "peer connection lost");
                            self.conn = None;
                        }
                        continue;
                    }
                }
            }

            let Some(frame) = self.rx.recv().await else {
                break;
            };
            if frame.reconnect || self.conn.is_none() {
                self.conn = connect(self.addr, self.tls.as_ref()).await;
                self.connected.store(self.conn.is_some(), Ordering::Relaxed);
                if self.conn.is_some() {
                    self.renewal = Some(spawn_connection_renewal(self.addr, self.tls.clone()));
                }
            }
            if self.conn.is_none() {
                self.dropped_sends.fetch_add(1, Ordering::SeqCst);
                continue; // peer unreachable: drop the frame
            }
            if !write_peer_frame(&mut self.conn, &frame.bytes).await {
                self.dropped_sends.fetch_add(1, Ordering::SeqCst);
                self.connected.store(false, Ordering::Relaxed);
                tracing::warn!(peer_host = self.peer.0, addr = %self.addr, "peer connection lost");
                self.conn = None;
            }
        }
        self.connected.store(false, Ordering::Relaxed);
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
async fn listener_loop(listener: TcpListener, deliver: Arc<Deliver>, tls: Option<ServerTls>) {
    let mut readers = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let deliver = deliver.clone();
                    let tls = tls.clone();
                    readers.spawn(async move {
                        accepted_reader(stream, tls.as_ref(), deliver).await;
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

async fn accepted_reader(stream: TcpStream, tls: Option<&ServerTls>, deliver: Arc<Deliver>) {
    let _ = stream.set_nodelay(true);
    let Some(ServerTls { config, identities }) = tls else {
        reader_loop(stream, None, deliver).await;
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
    let certificate = certificate.as_ref().to_vec();
    let identities = identities.clone();
    let _ = receive_loop_while_authorized(
        stream,
        Some(identity),
        DecodePolicy::BlockingAbove(DECODE_OFFLOAD_THRESHOLD),
        move |from| identities.identity(&certificate) == Some(from),
        move |from, message| deliver(from, message),
    )
    .await;
}

async fn reader_loop(
    stream: impl tokio::io::AsyncRead + Unpin,
    authenticated: Option<HostId>,
    deliver: Arc<Deliver>,
) {
    let _ = receive_loop(
        stream,
        authenticated,
        DecodePolicy::BlockingAbove(DECODE_OFFLOAD_THRESHOLD),
        move |from, message| deliver(from, message),
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakegcs::FakeGcs;
    use crate::{GcsConfig, GcsStore};
    use blockd_core::types::VolumeId;

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
        (0..hosts).map(|host| (HostId(host), free_addr())).collect()
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
            HostId(1),
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
            HostId(0),
            store.clone(),
            |_, _| {},
        )
        .await
        .expect("start host 0");

        for host in [HostId(0), HostId(1)] {
            let member = stored_member(&store, host).await;
            assert!(member.certificate.len() <= MAX_PEER_CERTIFICATE_BYTES);
            assert_eq!(member.endpoint, addresses[usize::from(host.0)]);
        }

        let message = PeerMsg::Released {
            volume: VolumeId(11),
            release_fence: 7,
        };
        let mut received = None;
        for _ in 0..30 {
            a.send(HostId(0), HostId(1), &message);
            if let Ok(next) = timeout(Duration::from_millis(100), rx.recv()).await {
                received = next;
                break;
            }
        }
        assert_eq!(received, Some((HostId(0), message)));

        a.send(
            HostId(9),
            HostId(1),
            &PeerMsg::Released {
                volume: VolumeId(12),
                release_fence: 8,
            },
        );
        assert!(
            timeout(Duration::from_millis(250), rx.recv())
                .await
                .is_err(),
            "certificate identity must override the claimed sender"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_or_deleted_public_certificates_are_not_members() {
        let (_fake, store) = fake_store().await;
        let tls = PeerTlsConfig::generate_in_object_store(store.clone(), HostId(4), free_addr())
            .await
            .expect("publish host 4");
        let member = stored_member(&store, HostId(4)).await;
        store
            .clone()
            .put(peer_membership_key(HostId(4)), member.encode())
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
            .put(peer_membership_key(HostId(5)), member.encode())
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
            .put(peer_membership_key(HostId(5)), member.encode())
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

        store.clone().delete(peer_membership_key(HostId(5))).await;
        refresh_bucket_membership(
            &tls.membership.store,
            &tls.certificate_identities,
            &tls.membership_leases,
        )
        .await
        .expect("refresh deletion");
        assert_eq!(
            tls.certificate_identities.identity(&member.certificate),
            Some(HostId(4))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oversized_membership_objects_are_read_with_a_strict_bound() {
        let (fake, store) = fake_store().await;
        store
            .clone()
            .put(
                peer_membership_key(HostId(7)),
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
                        .ends_with("/cluster/tls/public-keys/0007.member")
                    && request.headers.get("range") == Some(&range)),
            "membership objects must never be fetched without a size bound"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn node_joins_while_existing_operations_continue() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        let to_unknown = spawn_traffic(node_0.net.clone(), HostId(0), HostId(2));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;
        assert!(
            !node_0
                .net
                .connections()
                .into_iter()
                .any(|(host, _)| host == HostId(2))
        );

        let node_2 = start_node(HostId(2), &addresses, store).await;
        let joining = spawn_traffic(node_2.net.clone(), HostId(2), HostId(1));
        assert_progress(&node_1.deliveries, HostId(0)).await;
        wait_for_count(&node_1.deliveries, HostId(2), 3).await;
        wait_for_count(&node_2.deliveries, HostId(0), 3).await;
        wait_for_peer(&node_0.net, HostId(2), true).await;

        steady.abort();
        to_unknown.abort();
        joining.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multiple_nodes_join_concurrently_during_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(4);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;

        let (node_2, node_3) = tokio::join!(
            start_node(HostId(2), &addresses, store.clone()),
            start_node(HostId(3), &addresses, store)
        );
        let join_2 = spawn_traffic(node_2.net.clone(), HostId(2), HostId(1));
        let join_3 = spawn_traffic(node_3.net.clone(), HostId(3), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(2), 3).await;
        wait_for_count(&node_1.deliveries, HostId(3), 3).await;
        assert_progress(&node_1.deliveries, HostId(0)).await;

        steady.abort();
        join_2.abort();
        join_3.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn certificate_deletion_revokes_a_live_node_without_stopping_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let node_2 = start_node(HostId(2), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        let leaving = spawn_traffic(node_2.net.clone(), HostId(2), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;
        wait_for_count(&node_1.deliveries, HostId(2), 3).await;

        store.clone().delete(peer_membership_key(HostId(2))).await;
        let revoked_at = wait_until_quiet(&node_1.deliveries, HostId(2)).await;
        wait_for_peer(&node_1.net, HostId(2), false).await;
        assert_progress(&node_1.deliveries, HostId(0)).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_1.deliveries.from(HostId(2)), revoked_at);

        steady.abort();
        leaving.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crashed_node_restarts_with_a_fresh_identity_during_operations() {
        let (_fake, store) = fake_store().await;
        let mut addresses = addresses(3);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let node_2 = start_node(HostId(2), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        let before_restart = spawn_traffic(node_2.net.clone(), HostId(2), HostId(1));
        let toward_restarting = spawn_traffic(node_1.net.clone(), HostId(1), HostId(2));
        wait_for_count(&node_1.deliveries, HostId(2), 3).await;
        wait_for_count(&node_2.deliveries, HostId(1), 3).await;
        let old_member = stored_member(&store, HostId(2)).await;

        before_restart.abort();
        let _ = before_restart.await;
        drop(node_2);
        addresses.insert(HostId(2), free_addr());
        let node_2 = start_node(HostId(2), &addresses, store.clone()).await;
        let new_member = stored_member(&store, HostId(2)).await;
        assert_ne!(old_member.certificate, new_member.certificate);
        assert_ne!(old_member.endpoint, new_member.endpoint);
        let after_restart = spawn_traffic(node_2.net.clone(), HostId(2), HostId(1));
        let before = node_1.deliveries.from(HostId(2));
        wait_for_count(&node_1.deliveries, HostId(2), before + 3).await;
        wait_for_count(&node_2.deliveries, HostId(1), 3).await;
        assert_progress(&node_1.deliveries, HostId(0)).await;

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
                listen: addresses[&HostId(0)],
                advertise: addresses[&HostId(0)],
            },
            HostId(0),
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
        let restarting = start_node(HostId(1), &addresses, store.clone()).await;

        timeout(Duration::from_secs(3), async {
            loop {
                if snapshots
                    .lock()
                    .expect("membership snapshots lock")
                    .iter()
                    .any(|members| members.contains(&HostId(1)))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("restarting host admitted");
        let old_member = stored_member(&store, HostId(1)).await;
        let admitted_at = snapshots
            .lock()
            .expect("membership snapshots lock")
            .iter()
            .position(|members| members.contains(&HostId(1)))
            .expect("admission snapshot");

        drop(restarting);
        addresses.insert(HostId(1), free_addr());
        let restarted = start_node(HostId(1), &addresses, store.clone()).await;
        let new_member = stored_member(&store, HostId(1)).await;
        assert_ne!(old_member.certificate, new_member.certificate);

        let traffic = spawn_traffic(restarted.net.clone(), HostId(1), HostId(0));
        wait_for_count(&deliveries, HostId(1), 3).await;

        let roster_history = snapshots.lock().expect("membership snapshots lock").clone();
        assert!(
            roster_history[admitted_at..]
                .iter()
                .all(|members| members.contains(&HostId(1))),
            "quick restart must not remove the host from membership: {roster_history:?}"
        );

        traffic.abort();
        drop(observer);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_identity_chaos_isolated_and_heals_during_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let node_2 = start_node(HostId(2), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        let chaotic = spawn_traffic(node_2.net.clone(), HostId(2), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(2), 3).await;
        let member = stored_member(&store, HostId(2)).await;

        let duplicate = spawn_member_renewal(store.clone(), HostId(9), member);
        wait_for_peer(&node_1.net, HostId(2), false).await;
        let isolated_at = node_1.deliveries.from(HostId(2));
        tokio::time::sleep(Duration::from_millis(75)).await;
        assert_eq!(node_1.deliveries.from(HostId(2)), isolated_at);
        assert_progress(&node_1.deliveries, HostId(0)).await;

        duplicate.abort();
        let _ = duplicate.await;
        store.clone().delete(peer_membership_key(HostId(9))).await;
        wait_for_count(&node_1.deliveries, HostId(2), isolated_at + 3).await;
        assert_progress(&node_1.deliveries, HostId(0)).await;

        steady.abort();
        chaotic.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn repeated_join_leave_chaos_preserves_surviving_traffic() {
        let (_fake, store) = fake_store().await;
        let mut addresses = addresses(3);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;

        for _ in 0..3 {
            let node_2 = start_node(HostId(2), &addresses, store.clone()).await;
            let churn = spawn_traffic(node_2.net.clone(), HostId(2), HostId(1));
            let joined_at = node_1.deliveries.from(HostId(2));
            wait_for_count(&node_1.deliveries, HostId(2), joined_at + 3).await;
            store.clone().delete(peer_membership_key(HostId(2))).await;
            wait_until_quiet(&node_1.deliveries, HostId(2)).await;
            assert_progress(&node_1.deliveries, HostId(0)).await;
            churn.abort();
            let _ = churn.await;
            drop(node_2);
            addresses.insert(HostId(2), free_addr());
        }

        steady.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn certificate_replacement_at_same_endpoint_revokes_old_server_during_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(3);
        let node_2 = start_node(HostId(2), &addresses, store.clone()).await;
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        let toward_replaced = spawn_traffic(node_0.net.clone(), HostId(0), HostId(2));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;
        wait_for_count(&node_2.deliveries, HostId(0), 3).await;

        let old_member = stored_member(&store, HostId(2)).await;
        let CertifiedKey { cert, .. } =
            generate_simple_self_signed(vec![DEFAULT_PEER_SERVER_NAME.to_owned()])
                .expect("generate replacement identity");
        let replacement = MemberRecord {
            endpoint: old_member.endpoint,
            certificate: cert.der().to_vec(),
        };
        assert_ne!(replacement.certificate, old_member.certificate);
        store
            .clone()
            .put(peer_membership_key(HostId(2)), replacement.encode())
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
                    .get(&HostId(2))
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
        let revoked_at = wait_until_quiet(&node_2.deliveries, HostId(0)).await;
        assert_progress(&node_1.deliveries, HostId(0)).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_2.deliveries.from(HostId(0)), revoked_at);

        steady.abort();
        toward_replaced.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn host_id_collision_replaces_endpoint_and_revokes_old_process() {
        let (_fake, store) = fake_store().await;
        let mut first_addresses = addresses(3);
        let node_1 = start_node(HostId(1), &first_addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &first_addresses, store.clone()).await;
        let old_node_2 = start_node(HostId(2), &first_addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        let old_traffic = spawn_traffic(old_node_2.net.clone(), HostId(2), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(2), 3).await;

        let replacement_endpoint = free_addr();
        first_addresses.insert(HostId(2), replacement_endpoint);
        let new_node_2 = start_node(HostId(2), &first_addresses, store.clone()).await;
        let new_traffic = spawn_traffic(new_node_2.net.clone(), HostId(2), HostId(1));
        let before_replacement = node_1.deliveries.from(HostId(2));
        wait_for_count(&node_1.deliveries, HostId(2), before_replacement + 3).await;
        timeout(Duration::from_secs(3), async {
            loop {
                let endpoint = node_1
                    .net
                    .connections
                    .peers
                    .lock()
                    .expect("peer connections lock")
                    .get(&HostId(2))
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
        wait_until_quiet(&node_1.deliveries, HostId(2)).await;
        assert_progress(&node_1.deliveries, HostId(0)).await;
        let resumed = spawn_traffic(new_node_2.net.clone(), HostId(2), HostId(1));
        assert_progress(&node_1.deliveries, HostId(2)).await;

        steady.abort();
        old_traffic.abort();
        resumed.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_membership_chaos_fails_closed_during_operations() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;

        store
            .clone()
            .put(peer_membership_key(HostId(9)), vec![0xff; 64])
            .await
            .expect("publish malformed member");
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !node_0
                .net
                .connections()
                .into_iter()
                .any(|(host, _)| host == HostId(9))
        );
        assert_progress(&node_1.deliveries, HostId(0)).await;

        steady.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn seeded_membership_chaos_corpus_preserves_steady_operations() {
        for seed in [1_u64, 7, 19, 41] {
            let (fake, store) = fake_store().await;
            let mut addresses = addresses(3);
            let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
            let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
            let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
            wait_for_count(&node_1.deliveries, HostId(0), 3).await;
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
                            let joined = start_node(HostId(2), &addresses, store.clone()).await;
                            let before = node_1.deliveries.from(HostId(2));
                            churn = Some(spawn_traffic(joined.net.clone(), HostId(2), HostId(1)));
                            node_2 = Some(joined);
                            wait_for_count(&node_1.deliveries, HostId(2), before + 3).await;
                        }
                    }
                    1 => {
                        if node_2.is_some() {
                            store.clone().delete(peer_membership_key(HostId(2))).await;
                            wait_until_quiet(&node_1.deliveries, HostId(2)).await;
                            if let Some(task) = churn.take() {
                                task.abort();
                                let _ = task.await;
                            }
                            drop(node_2.take());
                            addresses.insert(HostId(2), free_addr());
                        }
                    }
                    2 => {
                        if node_2.is_some() {
                            if let Some(task) = churn.take() {
                                task.abort();
                                let _ = task.await;
                            }
                            drop(node_2.take());
                            addresses.insert(HostId(2), free_addr());
                            let restarted = start_node(HostId(2), &addresses, store.clone()).await;
                            let before = node_1.deliveries.from(HostId(2));
                            churn =
                                Some(spawn_traffic(restarted.net.clone(), HostId(2), HostId(1)));
                            node_2 = Some(restarted);
                            wait_for_count(&node_1.deliveries, HostId(2), before + 3).await;
                        }
                    }
                    3 => {
                        if node_2.is_some() {
                            let member = stored_member(&store, HostId(2)).await;
                            let duplicate = spawn_member_renewal(store.clone(), HostId(9), member);
                            wait_for_peer(&node_1.net, HostId(2), false).await;
                            let isolated = node_1.deliveries.from(HostId(2));
                            tokio::time::sleep(Duration::from_millis(75)).await;
                            assert_eq!(node_1.deliveries.from(HostId(2)), isolated);
                            duplicate.abort();
                            let _ = duplicate.await;
                            store.clone().delete(peer_membership_key(HostId(9))).await;
                            wait_for_count(&node_1.deliveries, HostId(2), isolated + 3).await;
                        }
                    }
                    4 => {
                        store
                            .clone()
                            .put(peer_membership_key(HostId(9)), vec![0xa5; 48])
                            .await
                            .expect("publish malformed member");
                        tokio::time::sleep(Duration::from_millis(75)).await;
                        store.clone().delete(peer_membership_key(HostId(9))).await;
                    }
                    _ => {
                        fake.outage.store(true, Ordering::SeqCst);
                        assert_progress(&node_1.deliveries, HostId(0)).await;
                        fake.outage.store(false, Ordering::SeqCst);
                    }
                }
                assert_progress(&node_1.deliveries, HostId(0)).await;
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
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        let node_2 = start_node(HostId(2), &addresses, store.clone()).await;
        let steady = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        let leaving = spawn_traffic(node_2.net.clone(), HostId(2), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(2), 3).await;

        store.clone().delete(peer_membership_key(HostId(2))).await;
        fake.outage.store(true, Ordering::SeqCst);
        assert_progress(&node_1.deliveries, HostId(0)).await;
        fake.outage.store(false, Ordering::SeqCst);
        wait_until_quiet(&node_1.deliveries, HostId(2)).await;
        assert_progress(&node_1.deliveries, HostId(0)).await;

        steady.abort();
        leaving.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn prolonged_membership_store_outage_expires_cached_authorization_and_recovers() {
        let (fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store).await;
        let traffic = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;

        fake.outage.store(true, Ordering::SeqCst);
        let expired_at = wait_until_quiet(&node_1.deliveries, HostId(0)).await;
        wait_for_peer(&node_1.net, HostId(0), false).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_1.deliveries.from(HostId(0)), expired_at);

        fake.outage.store(false, Ordering::SeqCst);
        wait_for_count(&node_1.deliveries, HostId(0), expired_at + 3).await;
        wait_for_peer(&node_1.net, HostId(0), true).await;

        traffic.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crashed_node_membership_expires_even_though_its_object_persists() {
        let (_fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store.clone()).await;
        wait_for_peer(&node_1.net, HostId(0), true).await;

        drop(node_0);
        wait_for_peer(&node_1.net, HostId(0), false).await;
        assert!(
            store
                .clone()
                .get(peer_membership_key(HostId(0)))
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
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store).await;
        let traffic = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;

        node_0.net.stop_membership_heartbeat();
        let revoked_at = wait_until_quiet(&node_1.deliveries, HostId(0)).await;
        wait_for_peer(&node_1.net, HostId(0), false).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_1.deliveries.from(HostId(0)), revoked_at);

        traffic.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_membership_refresh_cannot_bypass_authorization_expiry() {
        let (fake, store) = fake_store().await;
        let addresses = addresses(2);
        let node_1 = start_node(HostId(1), &addresses, store.clone()).await;
        let node_0 = start_node(HostId(0), &addresses, store).await;
        let traffic = spawn_traffic(node_0.net.clone(), HostId(0), HostId(1));
        wait_for_count(&node_1.deliveries, HostId(0), 3).await;

        fake.latency_ms.store(1_000, Ordering::SeqCst);
        let expired_at = wait_until_quiet(&node_1.deliveries, HostId(0)).await;
        wait_for_peer(&node_1.net, HostId(0), false).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(node_1.deliveries.from(HostId(0)), expired_at);

        fake.latency_ms.store(0, Ordering::SeqCst);
        wait_for_count(&node_1.deliveries, HostId(0), expired_at + 3).await;
        wait_for_peer(&node_1.net, HostId(0), true).await;

        traffic.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn release_ack_reconnects_after_the_destination_restarts() {
        use blockd_core::protocol::ReplicaCommitInfo;
        use blockd_core::types::JournalSeq;

        let addresses = addresses(2);
        let config_a = PeerConfig {
            listen: addresses[&HostId(0)],
            advertise: addresses[&HostId(0)],
        };
        let config_b = PeerConfig {
            listen: addresses[&HostId(1)],
            advertise: addresses[&HostId(1)],
        };
        let (first_send, mut first_receive) = tokio::sync::mpsc::unbounded_channel();
        let first = PeerNet::start_plaintext(
            &config_a,
            HostId(0),
            addresses.clone(),
            move |from, message| {
                let _ = first_send.send((from, message));
            },
        )
        .await
        .expect("start first destination");
        let source = PeerNet::start_plaintext(&config_b, HostId(1), addresses.clone(), |_, _| {})
            .await
            .expect("start source");

        let warmup = PeerMsg::ReleasedAck {
            volume: VolumeId(7),
            release_fence: 3,
        };
        source.send(HostId(1), HostId(0), &warmup);
        assert_eq!(
            timeout(Duration::from_secs(5), first_receive.recv())
                .await
                .expect("warmup delivery timeout")
                .expect("warmup receiver closed"),
            (HostId(1), warmup)
        );
        drop(first);
        tokio::task::yield_now().await;

        let (restarted_send, mut restarted_receive) = tokio::sync::mpsc::unbounded_channel();
        let restarted = loop {
            let restarted_send = restarted_send.clone();
            match PeerNet::start_plaintext(
                &config_a,
                HostId(0),
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
        source.send(HostId(1), HostId(0), &ack);
        assert_eq!(
            timeout(Duration::from_secs(5), restarted_receive.recv())
                .await
                .expect("release ack delivery timeout")
                .expect("restarted receiver closed"),
            (HostId(1), ack)
        );

        drop(restarted);
        drop(source);
    }

    #[test]
    fn membership_directory_accepts_only_canonical_host_keys() {
        assert_eq!(
            host_from_membership_key("cluster/tls/public-keys/00af.member"),
            Some(HostId(0x00af))
        );
        assert_eq!(
            host_from_membership_key("cluster/tls/public-keys/af.member"),
            None
        );
        assert_eq!(
            host_from_membership_key("cluster/tls/public-keys/00AF.member"),
            None
        );
        assert_eq!(
            host_from_membership_key("cluster/tls/public-keys/00af.member/extra"),
            None
        );
    }

    #[test]
    fn membership_record_ipv4_encoding_is_byte_pinned() {
        let record = MemberRecord {
            endpoint: "127.0.0.1:7001".parse().expect("endpoint"),
            certificate: vec![1, 2, 3],
        };
        let mut expected = Vec::from(*b"BNOD");
        expected.extend_from_slice(&[4, 0x59, 0x1b, 127, 0, 0, 1]);
        expected.extend_from_slice(&[0; 20]);
        expected.extend_from_slice(&3_u32.to_le_bytes());
        expected.extend_from_slice(&[1, 2, 3]);
        assert_eq!(record.encode(), expected);
        assert_eq!(MemberRecord::decode(&expected), Some(record));
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
        };
        let encoded = record.encode();
        assert_eq!(MemberRecord::decode(&encoded), Some(record));

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 1;
        assert_eq!(MemberRecord::decode(&bad_magic), None);
        let mut zero_port = encoded.clone();
        zero_port[5..7].copy_from_slice(&0_u16.to_le_bytes());
        assert_eq!(MemberRecord::decode(&zero_port), None);
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(MemberRecord::decode(&trailing), None);
    }
}
