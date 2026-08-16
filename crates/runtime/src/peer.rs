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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blockd_core::peer::encode_peer;
use blockd_core::protocol::PeerMsg;
use blockd_core::types::HostId;
use blockd_transport::{DecodePolicy, receive_loop, write_frame};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender, error::TrySendError};
use tokio::time::timeout;
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// What the transport does with a verified inbound message.
type Deliver = dyn Fn(HostId, PeerMsg) + Send + Sync;

#[derive(Clone, Debug)]
pub struct PeerConfig {
    /// Where this daemon listens — the VPC-internal address, never public.
    pub listen: SocketAddr,
    /// The cluster roster: peer identity → address (R6.5: the control
    /// plane's knowledge, carried here by static config).
    pub peers: BTreeMap<HostId, SocketAddr>,
    /// Required for passive durability. The server config must require
    /// client certificates and the client config must present this host's
    /// certificate. Exact leaf DER binds certificates to roster identities.
    pub tls: Option<PeerTlsConfig>,
}

#[derive(Clone, Debug)]
pub struct PeerTlsConfig {
    pub server: Arc<ServerConfig>,
    pub client: Arc<ClientConfig>,
    /// TLS DNS name expected from each destination certificate.
    pub server_names: BTreeMap<HostId, String>,
    /// Exact leaf certificate DER → authenticated host identity.
    pub certificate_identities: BTreeMap<Vec<u8>, HostId>,
}

impl PeerTlsConfig {
    pub fn from_der(
        roots: RootCertStore,
        certificate: Vec<u8>,
        private_key: &[u8],
        server_names: BTreeMap<HostId, String>,
        certificate_identities: BTreeMap<Vec<u8>, HostId>,
    ) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certificate = CertificateDer::from(certificate);
        let key = || PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(private_key.to_vec()));
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
            .build()
            .expect("client verifier");
        let server = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![certificate.clone()], key())
            .expect("server identity");
        let client = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![certificate], key())
            .expect("client identity");
        Self {
            server: Arc::new(server),
            client: Arc::new(client),
            server_names,
            certificate_identities,
        }
    }
}

#[derive(Clone)]
struct ClientTls {
    config: Arc<ClientConfig>,
    server_name: Option<String>,
    identities: BTreeMap<Vec<u8>, HostId>,
    expected: HostId,
}

#[derive(Clone)]
struct ServerTls {
    config: Arc<ServerConfig>,
    identities: BTreeMap<Vec<u8>, HostId>,
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
    senders: BTreeMap<HostId, Sender<Vec<u8>>>,
    /// Frames dropped on the floor (queue full or peer down) — the demo's
    /// visibility into how hard the retry timers are working.
    pub dropped_sends: Arc<AtomicU64>,
    connected: BTreeMap<HostId, Arc<AtomicBool>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    authenticated: bool,
}

impl PeerNet {
    /// Start the listener and one lazy sender per configured peer;
    /// verified inbound frames reach `deliver` (the runtime injects them
    /// into the actor peer inbox).
    pub async fn start(
        config: &PeerConfig,
        self_id: HostId,
        deliver: impl Fn(HostId, PeerMsg) + Send + Sync + 'static,
    ) -> std::io::Result<Arc<PeerNet>> {
        let listener = TcpListener::bind(config.listen).await?;
        let dropped_sends = Arc::new(AtomicU64::new(0));
        let mut senders = BTreeMap::new();
        let mut connected = BTreeMap::new();
        let mut tasks = Vec::new();
        for (&peer, &addr) in &config.peers {
            if peer == self_id {
                continue;
            }
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(SEND_QUEUE);
            let peer_connected = Arc::new(AtomicBool::new(false));
            let tls = config.tls.as_ref().map(|tls| ClientTls {
                config: tls.client.clone(),
                server_name: tls.server_names.get(&peer).cloned(),
                identities: tls.certificate_identities.clone(),
                expected: peer,
            });
            let dropped = dropped_sends.clone();
            let task_connected = peer_connected.clone();
            tasks.push(tokio::spawn(async move {
                sender_loop(rx, addr, peer, task_connected, dropped, tls).await;
            }));
            senders.insert(peer, tx);
            connected.insert(peer, peer_connected);
        }
        let deliver: Arc<Deliver> = Arc::new(deliver);
        let server_tls = config.tls.as_ref().map(|tls| ServerTls {
            config: tls.server.clone(),
            identities: tls.certificate_identities.clone(),
        });
        tasks.push(tokio::spawn(listener_loop(listener, deliver, server_tls)));
        Ok(Arc::new(PeerNet {
            senders,
            dropped_sends,
            connected,
            tasks: Mutex::new(tasks),
            authenticated: config.tls.is_some(),
        }))
    }

    pub fn authenticated(&self) -> bool {
        self.authenticated
    }

    /// Fire-and-forget: encode and queue. Never blocks the daemon thread;
    /// unknown peers and full queues drop the frame (retries re-drive).
    pub fn send(&self, self_id: HostId, to: HostId, msg: &PeerMsg) {
        let Some(sender) = self.senders.get(&to) else {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
            return;
        };
        let frame = encode_peer(self_id, msg);
        if let Err(TrySendError::Full(_) | TrySendError::Closed(_)) = sender.try_send(frame) {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn connections(&self) -> Vec<(HostId, bool)> {
        self.connected
            .iter()
            .map(|(&peer, connected)| (peer, connected.load(Ordering::Relaxed)))
            .collect()
    }
}

impl Drop for PeerNet {
    fn drop(&mut self) {
        for task in self.tasks.lock().expect("lock").drain(..) {
            task.abort();
        }
    }
}

/// One outbound connection, made lazily and remade on any error. A frame
/// that hits a connect or write failure is dropped with its connection —
/// the protocol's retry timers own recovery, not this loop.
trait PeerIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

impl<T> PeerIo for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

async fn sender_loop(
    mut rx: Receiver<Vec<u8>>,
    addr: SocketAddr,
    peer: HostId,
    connected: Arc<AtomicBool>,
    dropped_sends: Arc<AtomicU64>,
    tls: Option<ClientTls>,
) {
    let mut conn: Option<Box<dyn PeerIo>> = None;
    let mut renewal: Option<tokio::task::JoinHandle<Option<Box<dyn PeerIo>>>> = None;
    loop {
        if conn.is_none()
            && let Some(task) = renewal.take()
        {
            task.abort();
        }
        if let Some(task) = renewal.as_mut() {
            tokio::select! {
                renewed = task => {
                    renewal = None;
                    if let Ok(Some(stream)) = renewed {
                        conn = Some(stream);
                        connected.store(true, Ordering::Relaxed);
                    }
                    if conn.is_some() {
                        renewal = Some(spawn_connection_renewal(addr, tls.clone()));
                    }
                    continue;
                }
                frame = rx.recv() => {
                    let Some(frame) = frame else { break };
                    if !write_peer_frame(&mut conn, &frame).await {
                        dropped_sends.fetch_add(1, Ordering::SeqCst);
                        connected.store(false, Ordering::Relaxed);
                        tracing::warn!(peer_host = peer.0, %addr, "peer connection lost");
                        conn = None;
                    }
                    continue;
                }
            }
        }

        let Some(frame) = rx.recv().await else { break };
        if conn.is_none() {
            conn = connect(addr, tls.as_ref()).await;
            connected.store(conn.is_some(), Ordering::Relaxed);
            if conn.is_some() {
                renewal = Some(spawn_connection_renewal(addr, tls.clone()));
            }
        }
        if conn.is_none() {
            dropped_sends.fetch_add(1, Ordering::SeqCst);
            continue; // peer unreachable: drop the frame
        }
        if !write_peer_frame(&mut conn, &frame).await {
            dropped_sends.fetch_add(1, Ordering::SeqCst);
            connected.store(false, Ordering::Relaxed);
            tracing::warn!(peer_host = peer.0, %addr, "peer connection lost");
            conn = None;
        }
    }
    connected.store(false, Ordering::Relaxed);
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
        server_name: Some(name),
        identities,
        expected,
    }) = tls
    else {
        return tls.is_none().then(|| Box::new(stream) as Box<dyn PeerIo>);
    };
    let name = ServerName::try_from(name.clone()).ok()?;
    let stream = timeout(
        Duration::from_secs(5),
        TlsConnector::from(config.clone()).connect(name, stream),
    )
    .await
    .ok()?
    .ok()?;
    let certificate = stream.get_ref().1.peer_certificates()?.first()?.as_ref();
    if identities.get(certificate) != Some(expected) {
        return None;
    }
    Some(Box::new(stream))
}

/// One inbound connection: length-delimited by the frame header itself,
/// verified by `decode_peer` (magic, length cap, crc, strict layout). Any
/// violation closes the connection; the peer reconnects and retries.
async fn listener_loop(listener: TcpListener, deliver: Arc<Deliver>, tls: Option<ServerTls>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let deliver = deliver.clone();
                let tls = tls.clone();
                tokio::spawn(async move { accepted_reader(stream, tls.as_ref(), deliver).await });
            }
            Err(error) => {
                tracing::error!(%error, "peer listener stopped");
                return;
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
    let Some(&identity) = identities.get(certificate.as_ref()) else {
        return;
    };
    reader_loop(stream, Some(identity), deliver).await;
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
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use rustls::RootCertStore;
    use rustls::pki_types::CertificateDer;
    use std::sync::OnceLock;

    use super::*;
    use blockd_core::types::VsetId;

    struct Identity {
        certificate: Vec<u8>,
        private_key: Vec<u8>,
    }

    fn identities() -> &'static [Identity] {
        static IDENTITIES: OnceLock<Vec<Identity>> = OnceLock::new();
        IDENTITIES.get_or_init(|| {
            (0..2)
                .map(|host| {
                    let CertifiedKey { cert, signing_key } =
                        generate_simple_self_signed(vec![format!("host{host}.test")])
                            .expect("generate test TLS identity");
                    Identity {
                        certificate: cert.der().to_vec(),
                        private_key: signing_key.serialize_der(),
                    }
                })
                .collect()
        })
    }

    fn tls(host: usize) -> PeerTlsConfig {
        let identities = identities();
        let mut roots = RootCertStore::empty();
        for identity in identities {
            roots
                .add(CertificateDer::from(identity.certificate.clone()))
                .expect("test trust anchor");
        }
        PeerTlsConfig::from_der(
            roots,
            identities[host].certificate.clone(),
            &identities[host].private_key,
            BTreeMap::from([
                (HostId(0), "host0.test".to_owned()),
                (HostId(1), "host1.test".to_owned()),
            ]),
            identities
                .iter()
                .enumerate()
                .map(|(host, identity)| {
                    (
                        identity.certificate.clone(),
                        HostId(u16::try_from(host).expect("fits")),
                    )
                })
                .collect(),
        )
    }

    fn free_addr() -> SocketAddr {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("address")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mutual_tls_derives_identity_and_rejects_a_spoofed_envelope() {
        let addresses = [free_addr(), free_addr()];
        let roster = BTreeMap::from([(HostId(0), addresses[0]), (HostId(1), addresses[1])]);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let b = PeerNet::start(
            &PeerConfig {
                listen: addresses[1],
                peers: roster.clone(),
                tls: Some(tls(1)),
            },
            HostId(1),
            move |from, msg| {
                let _ = tx.send((from, msg));
            },
        )
        .await
        .unwrap();
        let a = PeerNet::start(
            &PeerConfig {
                listen: addresses[0],
                peers: roster,
                tls: Some(tls(0)),
            },
            HostId(0),
            |_, _| {},
        )
        .await
        .unwrap();
        assert!(a.authenticated() && b.authenticated());
        let msg = PeerMsg::Released {
            vset: VsetId(7),
            release_fence: 3,
        };
        a.send(HostId(0), HostId(1), &msg);
        assert_eq!(
            timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("mTLS delivery timeout")
                .expect("mTLS receiver closed"),
            (HostId(0), msg)
        );

        a.send(
            HostId(9),
            HostId(1),
            &PeerMsg::Released {
                vset: VsetId(8),
                release_fence: 4,
            },
        );
        assert!(
            timeout(Duration::from_millis(250), rx.recv())
                .await
                .is_err(),
            "certificate identity must override the claimed sender"
        );
    }
}
