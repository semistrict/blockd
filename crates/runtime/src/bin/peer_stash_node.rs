//! Small production-interpreter node used by the destructive multi-machine
//! soak. It intentionally exposes only a fixed validation vset and a tiny
//! line protocol; storage, peer transport, fencing, and guest memory are the
//! real runtime implementations.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("peer_stash_node requires Linux");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::BTreeMap;
    use std::io::{BufRead, BufReader, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use base64::Engine;
    use blockd_core::daemon::{DaemonConfig, ReplicaPlacementConfig};
    use blockd_core::journal::{DurabilityMode, VsetConfig};
    use blockd_core::placement::PeerCandidate;
    use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};
    use blockd_runtime::{GcsConfig, GcsStore, PeerConfig, PeerTlsConfig, Runtime, RuntimeConfig};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::server::WebPkiClientVerifier;
    use rustls::{ClientConfig, RootCertStore, ServerConfig};

    const VSET: VsetId = VsetId(1);
    const VSET_CONFIG: VsetConfig = VsetConfig {
        disk_volumes: 1,
        pages_per_volume: 64,
        durability: DurabilityMode::PeerStashed,
    };

    #[derive(Clone)]
    struct Config {
        host: HostId,
        primary: bool,
        control: SocketAddr,
        peer_listen: SocketAddr,
        peers: BTreeMap<HostId, SocketAddr>,
        protocol_versions: BTreeMap<HostId, u16>,
        server_names: BTreeMap<HostId, String>,
        identities: BTreeMap<HostId, Vec<PathBuf>>,
        certificate: PathBuf,
        private_key: PathBuf,
        blob_dir: PathBuf,
        endpoint: String,
        metadata_endpoint: String,
        bucket: String,
        prefix: String,
    }

    fn read_config(path: &Path) -> Config {
        let text = std::fs::read_to_string(path).expect("read node config");
        let mut values = BTreeMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line.split_once('=').expect("config key=value");
            values.insert(key.trim().to_owned(), value.trim().to_owned());
        }
        let get = |key: &str| values.get(key).unwrap_or_else(|| panic!("missing {key}"));
        let mut peers = BTreeMap::new();
        let mut protocol_versions = BTreeMap::new();
        let mut server_names = BTreeMap::new();
        let mut identities = BTreeMap::new();
        for (key, value) in &values {
            if let Some(id) = key.strip_prefix("peer.") {
                peers.insert(
                    HostId(id.parse().expect("peer id")),
                    value.parse().expect("peer address"),
                );
            } else if let Some(id) = key.strip_prefix("protocol.") {
                protocol_versions.insert(
                    HostId(id.parse().expect("protocol host id")),
                    value.parse().expect("protocol version"),
                );
            } else if let Some(id) = key.strip_prefix("server_name.") {
                server_names.insert(
                    HostId(id.parse().expect("server-name host id")),
                    value.clone(),
                );
            } else if let Some(id) = key.strip_prefix("identity.") {
                identities.insert(
                    HostId(id.parse().expect("identity host id")),
                    value.split(',').map(|path| path.trim().into()).collect(),
                );
            }
        }
        Config {
            host: HostId(get("host").parse().expect("host")),
            primary: get("primary") == "true",
            control: get("control").parse().expect("control address"),
            peer_listen: get("peer_listen").parse().expect("peer listen"),
            peers,
            protocol_versions,
            server_names,
            identities,
            certificate: get("certificate").into(),
            private_key: get("private_key").into(),
            blob_dir: get("blob_dir").into(),
            endpoint: get("endpoint").clone(),
            metadata_endpoint: get("metadata_endpoint").clone(),
            bucket: get("bucket").clone(),
            prefix: get("prefix").clone(),
        }
    }

    fn pem(path: &Path) -> Vec<u8> {
        let text = std::fs::read_to_string(path).expect("read PEM");
        let body: String = text
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect();
        base64::engine::general_purpose::STANDARD
            .decode(body)
            .expect("PEM base64")
    }

    fn tls(config: &Config) -> PeerTlsConfig {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut roots = RootCertStore::empty();
        let mut certificate_identities = BTreeMap::new();
        for (&host, paths) in &config.identities {
            for path in paths {
                let certificate = pem(path);
                roots
                    .add(CertificateDer::from(certificate.clone()))
                    .expect("trust anchor");
                certificate_identities.insert(certificate, host);
            }
        }
        let certificate = CertificateDer::from(pem(&config.certificate));
        let key = || PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pem(&config.private_key)));
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
        PeerTlsConfig {
            server: Arc::new(server),
            client: Arc::new(client),
            server_names: config.server_names.clone(),
            certificate_identities,
        }
    }

    fn runtime_config(config: &Config) -> RuntimeConfig {
        let roster = config
            .peers
            .keys()
            .map(|&host| PeerCandidate {
                host,
                weight: 1,
                failure_domain: host.0 + 1,
                drained: false,
            })
            .collect();
        RuntimeConfig {
            daemon: DaemonConfig {
                host: config.host,
                cache_pages: 256,
                writeback_interval: millis(5),
                backup_retry: millis(50),
                disk_capacity: Some(512 * 1024 * 1024),
                disk_headroom: 8 * 1024 * 1024,
                wedge_ticks: 500,
                replica_placement: Some(ReplicaPlacementConfig {
                    membership_epoch: 1,
                    local_failure_domain: config.host.0 + 1,
                    roster,
                }),
            },
            blob_dir: config.blob_dir.clone(),
            peer: Some(PeerConfig {
                listen: config.peer_listen,
                peers: config.peers.clone(),
                outbound_protocol_versions: config.protocol_versions.clone(),
                tls: Some(tls(config)),
            }),
        }
    }

    fn page() -> PageId {
        PageId {
            volume: VolumeId {
                vset: VSET,
                idx: VolumeIdx(1),
            },
            page: PageNo(3),
        }
    }

    fn reply(stream: &mut TcpStream, text: &str) {
        stream.write_all(text.as_bytes()).expect("control reply");
        stream.write_all(b"\n").expect("control newline");
    }

    fn handle(runtime: &Runtime, stream: &mut TcpStream) {
        let mut command = String::new();
        BufReader::new(stream.try_clone().expect("clone control stream"))
            .read_line(&mut command)
            .expect("read control command");
        let parts: Vec<&str> = command.split_whitespace().collect();
        match parts.as_slice() {
            ["PING"] => reply(stream, "OK"),
            ["WRITE", value] => {
                let value: u64 = value.parse().expect("write value");
                runtime.guest_write(VSET, page(), value);
                assert!(runtime.guest_sync(VSET, VolumeIdx(1)));
                reply(stream, "OK");
            }
            ["READ"] => {
                let bytes = runtime.guest_read(VSET, page());
                let value = u64::from_ne_bytes(bytes[..8].try_into().expect("word"));
                reply(stream, &format!("VALUE {value}"));
            }
            ["METRICS"] => {
                let counters = runtime.counters();
                let metrics = runtime.replica_metrics();
                if let Some(metric) = metrics.iter().find(|metric| metric.vset == VSET) {
                    reply(
                        stream,
                        &format!(
                            "active={} transition={} epoch={} ack={} peer={} store={} queued={} \
                             nonactive={} cleanup={} replacement={} incidents={}",
                            metric.active_peer.map_or(-1, |host| i32::from(host.0)),
                            metric.transition_peer.map_or(-1, |host| i32::from(host.0)),
                            metric.assignment_epoch.unwrap_or(0),
                            metric.sync_ack_through,
                            metric.peer_committed_through,
                            metric.store_published_through,
                            metric.queued_syncs,
                            counters.replica_nonactive_bytes,
                            counters.replica_cleanup_rewrite_bytes,
                            counters.replica_replacement_bytes,
                            runtime.incidents().len(),
                        ),
                    );
                } else {
                    reply(stream, "PASSIVE");
                }
            }
            ["SPOOL"] => {
                let metrics = runtime.replica_spool_metrics();
                reply(stream, &format!("{metrics:?}"));
            }
            _ => reply(stream, "ERROR unknown command"),
        }
    }

    pub fn main() {
        let path = std::env::args().nth(1).unwrap_or_else(|| {
            eprintln!("usage: peer_stash_node CONFIG");
            std::process::exit(2)
        });
        let config = read_config(Path::new(&path));
        let runtime_config = runtime_config(&config);
        let existed = config.blob_dir.exists();
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            endpoint: config.endpoint.clone(),
            metadata_endpoint: config.metadata_endpoint.clone(),
        }));
        let runtime = if existed {
            let vsets = if config.primary {
                BTreeMap::from([(VSET, VSET_CONFIG)])
            } else {
                BTreeMap::new()
            };
            let (runtime, _) = Runtime::recover(&runtime_config, store, &vsets);
            if config.primary {
                let _ = runtime.wait_recovered(VSET);
            }
            runtime
        } else {
            let runtime = Runtime::new(&runtime_config, store);
            if config.primary {
                runtime.create_vset(VSET, VSET_CONFIG);
            }
            runtime
        };
        let listener = TcpListener::bind(config.control).expect("control listen");
        eprintln!("peer-stash node {} ready", config.host.0);
        for stream in listener.incoming() {
            let mut stream = stream.expect("control accept");
            handle(&runtime, &mut stream);
        }
    }
}

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}
