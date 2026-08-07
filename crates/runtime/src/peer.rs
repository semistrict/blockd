//! The peer transport: mutually authenticated TLS carrying
//! [`blockd_core::peer`] frames between daemons. The channel
//! contract is at-least-once with drops tolerated (the daemon's retry
//! timers re-drive everything), so this layer is deliberately dumb:
//! sends are fire-and-forget — a dead connection, a full queue, or an
//! unreachable peer just drops the frame; inbound frames that fail
//! verification close the connection and the peer reconnects.
//!
//! Plain TCP remains available for migration-only development fixtures, but
//! the runtime refuses to enable peer-stashed durability without TLS.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use blockd_core::format::{Dec, FRAME_HEADER};
use blockd_core::peer::{MAGIC_PEER, MAX_PEER_PAYLOAD, decode_peer, encode_peer_version};
use blockd_core::seam::PeerMsg;
use blockd_core::types::HostId;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection, StreamOwned};

/// What the transport does with a verified inbound message.
type Deliver = dyn Fn(HostId, PeerMsg) + Send + Sync;

fn header_allowed(magic: u32, payload_len: u32) -> bool {
    magic == MAGIC_PEER && payload_len <= MAX_PEER_PAYLOAD
}

#[derive(Clone, Debug)]
pub struct PeerConfig {
    /// Where this daemon listens — the VPC-internal address, never public.
    pub listen: SocketAddr,
    /// The cluster roster: peer identity → address (R6.5: the control
    /// plane's knowledge, carried here by static config).
    pub peers: BTreeMap<HostId, SocketAddr>,
    /// Per-destination wire version during a rolling deployment. Omitted
    /// peers use the current version. Version 1 cannot carry peer-stash data.
    pub outbound_protocol_versions: BTreeMap<HostId, u16>,
    /// Required for peer-stashed durability. The server config must require
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

/// Bound how long an outbound connection can hide a remote restart behind a
/// half-open TCP tunnel. This also makes a newly presented leaf certificate
/// observable during a rolling rotation without paying a handshake per frame.
const MAX_CONNECTION_AGE: Duration = Duration::from_secs(1);

pub struct PeerNet {
    senders: BTreeMap<HostId, SyncSender<Vec<u8>>>,
    outbound_protocol_versions: BTreeMap<HostId, u16>,
    /// Frames dropped on the floor (queue full or peer down) — the demo's
    /// visibility into how hard the retry timers are working.
    pub dropped_sends: AtomicU64,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    authenticated: bool,
}

impl PeerNet {
    /// Start the listener and one lazy sender per configured peer;
    /// verified inbound frames reach `deliver` (the runtime injects them
    /// as `Event::PeerDelivered`).
    pub fn start(
        config: &PeerConfig,
        self_id: HostId,
        deliver: impl Fn(HostId, PeerMsg) + Send + Sync + 'static,
    ) -> Arc<PeerNet> {
        let mut senders = BTreeMap::new();
        for (&peer, &addr) in &config.peers {
            if peer == self_id {
                continue;
            }
            let (tx, rx) = sync_channel::<Vec<u8>>(SEND_QUEUE);
            let tls = config.tls.as_ref().map(|tls| ClientTls {
                config: tls.client.clone(),
                server_name: tls.server_names.get(&peer).cloned(),
                identities: tls.certificate_identities.clone(),
                expected: peer,
            });
            thread::spawn(move || sender_loop(&rx, addr, tls.as_ref()));
            senders.insert(peer, tx);
        }
        let listener = TcpListener::bind(config.listen).expect("peer listen");
        listener
            .set_nonblocking(true)
            .expect("peer listener nonblocking");
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let deliver: Arc<Deliver> = Arc::new(deliver);
            let shutdown = shutdown.clone();
            let tls = config.tls.as_ref().map(|tls| ServerTls {
                config: tls.server.clone(),
                identities: tls.certificate_identities.clone(),
            });
            thread::spawn(move || {
                while !shutdown.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let deliver = deliver.clone();
                            let tls = tls.clone();
                            thread::spawn(move || {
                                accepted_reader(stream, tls.as_ref(), &deliver);
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => return,
                    }
                }
            });
        }
        Arc::new(PeerNet {
            senders,
            outbound_protocol_versions: config.outbound_protocol_versions.clone(),
            dropped_sends: AtomicU64::new(0),
            shutdown,
            authenticated: config.tls.is_some(),
        })
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
        let version = self
            .outbound_protocol_versions
            .get(&to)
            .copied()
            .unwrap_or(2);
        let Ok(frame) = encode_peer_version(self_id, msg, version) else {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
            return;
        };
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) = sender.try_send(frame) {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl Drop for PeerNet {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

/// One outbound connection, made lazily and remade on any error. A frame
/// that hits a connect or write failure is dropped with its connection —
/// the protocol's retry timers own recovery, not this loop.
fn sender_loop(rx: &Receiver<Vec<u8>>, addr: SocketAddr, tls: Option<&ClientTls>) {
    enum Connection {
        Plain(TcpStream),
        Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
    }
    impl Write for Connection {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            match self {
                Connection::Plain(stream) => stream.write(buf),
                Connection::Tls(stream) => stream.write(buf),
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            match self {
                Connection::Plain(stream) => stream.flush(),
                Connection::Tls(stream) => stream.flush(),
            }
        }
    }

    let mut conn: Option<(Connection, Instant)> = None;
    while let Ok(frame) = rx.recv() {
        if conn
            .as_ref()
            .is_some_and(|(_, established)| established.elapsed() >= MAX_CONNECTION_AGE)
        {
            conn = None;
        }
        if conn.is_none() {
            conn = TcpStream::connect_timeout(&addr, Duration::from_secs(1))
                .ok()
                .and_then(|stream| {
                    stream.set_nodelay(true).ok()?;
                    stream
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .ok()?;
                    match &tls {
                        None => Some(Connection::Plain(stream)),
                        Some(ClientTls {
                            config,
                            server_name: Some(name),
                            identities,
                            expected,
                        }) => {
                            let name = ServerName::try_from(name.clone()).ok()?;
                            let connection = ClientConnection::new(config.clone(), name).ok()?;
                            let mut stream = StreamOwned::new(connection, stream);
                            complete_client_handshake(&mut stream, identities, *expected)?;
                            Some(Connection::Tls(Box::new(stream)))
                        }
                        Some(ClientTls {
                            server_name: None, ..
                        }) => None,
                    }
                })
                .map(|connection| (connection, Instant::now()));
        }
        let Some((stream, _)) = conn.as_mut() else {
            continue; // peer unreachable: drop the frame
        };
        if stream.write_all(&frame).is_err() {
            conn = None;
        }
    }
}

fn complete_client_handshake(
    stream: &mut StreamOwned<ClientConnection, TcpStream>,
    identities: &BTreeMap<Vec<u8>, HostId>,
    expected: HostId,
) -> Option<()> {
    while stream.conn.is_handshaking() {
        stream.conn.complete_io(&mut stream.sock).ok()?;
    }
    let certificate = stream.conn.peer_certificates()?.first()?.as_ref();
    (identities.get(certificate) == Some(&expected)).then_some(())
}

fn accepted_reader(stream: TcpStream, tls: Option<&ServerTls>, deliver: &Arc<Deliver>) {
    stream.set_nonblocking(false).ok();
    stream.set_nodelay(true).ok();
    let Some(ServerTls { config, identities }) = tls else {
        reader_loop(stream, None, deliver);
        return;
    };
    let Ok(connection) = ServerConnection::new(config.clone()) else {
        return;
    };
    let mut stream = StreamOwned::new(connection, stream);
    while stream.conn.is_handshaking() {
        if stream.conn.complete_io(&mut stream.sock).is_err() {
            return;
        }
    }
    let Some(certificate) = stream
        .conn
        .peer_certificates()
        .and_then(|certificates| certificates.first())
    else {
        return;
    };
    let Some(&identity) = identities.get(certificate.as_ref()) else {
        return;
    };
    reader_loop(stream, Some(identity), deliver);
}

/// One inbound connection: length-delimited by the frame header itself,
/// verified by `decode_peer` (magic, length cap, crc, strict layout). Any
/// violation closes the connection; the peer reconnects and retries.
fn reader_loop(mut stream: impl Read, authenticated: Option<HostId>, deliver: &Arc<Deliver>) {
    loop {
        let mut header = [0u8; FRAME_HEADER];
        if stream.read_exact(&mut header).is_err() {
            return;
        }
        let mut d = Dec::new(&header);
        let magic = d.u32().expect("12 bytes");
        let len = d.u32().expect("12 bytes");
        if !header_allowed(magic, len) {
            return; // desynced or hostile: drop the connection
        }
        let mut frame = header.to_vec();
        let start = frame.len();
        frame.resize(start + usize::try_from(len).expect("fits"), 0);
        if stream.read_exact(&mut frame[start..]).is_err() {
            return;
        }
        let Ok((from, msg)) = decode_peer(&frame) else {
            return; // damage: drop it and the connection (R8.1)
        };
        if authenticated.is_some_and(|identity| identity != from) {
            return;
        }
        deliver(from, msg);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;
    use std::sync::mpsc::channel;

    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use rustls::RootCertStore;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::server::WebPkiClientVerifier;

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
        let _ = rustls::crypto::ring::default_provider().install_default();
        let identities = identities();
        let mut roots = RootCertStore::empty();
        for identity in identities {
            roots
                .add(CertificateDer::from(identity.certificate.clone()))
                .expect("test trust anchor");
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
            .build()
            .expect("client verifier");
        let certificate = CertificateDer::from(identities[host].certificate.clone());
        let key = || {
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                identities[host].private_key.clone(),
            ))
        };
        let server = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![certificate.clone()], key())
            .expect("server identity");
        let client = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![certificate], key())
            .expect("client identity");
        PeerTlsConfig {
            server: Arc::new(server),
            client: Arc::new(client),
            server_names: BTreeMap::from([
                (HostId(0), "host0.test".to_owned()),
                (HostId(1), "host1.test".to_owned()),
            ]),
            certificate_identities: identities
                .iter()
                .enumerate()
                .map(|(host, identity)| {
                    (
                        identity.certificate.clone(),
                        HostId(u16::try_from(host).expect("fits")),
                    )
                })
                .collect(),
        }
    }

    fn free_addr() -> SocketAddr {
        TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("address")
    }

    #[test]
    fn mutual_tls_derives_identity_and_rejects_a_spoofed_envelope() {
        let addresses = [free_addr(), free_addr()];
        let roster = BTreeMap::from([(HostId(0), addresses[0]), (HostId(1), addresses[1])]);
        let (tx, rx) = channel();
        let b = PeerNet::start(
            &PeerConfig {
                listen: addresses[1],
                peers: roster.clone(),
                outbound_protocol_versions: BTreeMap::new(),
                tls: Some(tls(1)),
            },
            HostId(1),
            move |from, msg| {
                let _ = tx.send((from, msg));
            },
        );
        let a = PeerNet::start(
            &PeerConfig {
                listen: addresses[0],
                peers: roster,
                outbound_protocol_versions: BTreeMap::new(),
                tls: Some(tls(0)),
            },
            HostId(0),
            |_, _| {},
        );
        assert!(a.authenticated() && b.authenticated());
        let msg = PeerMsg::Released { vset: VsetId(7) };
        a.send(HostId(0), HostId(1), &msg);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("mTLS delivery"),
            (HostId(0), msg)
        );

        a.send(HostId(9), HostId(1), &PeerMsg::Released { vset: VsetId(8) });
        assert!(
            rx.recv_timeout(Duration::from_millis(250)).is_err(),
            "certificate identity must override the claimed sender"
        );
    }

    #[test]
    fn hostile_outer_lengths_are_rejected_before_allocation() {
        assert!(header_allowed(MAGIC_PEER, MAX_PEER_PAYLOAD));
        assert!(!header_allowed(MAGIC_PEER, MAX_PEER_PAYLOAD + 1));
        assert!(!header_allowed(0, 0));
    }
}
