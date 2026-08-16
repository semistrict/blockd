//! Runtime node used by the multi-machine peer-stash soak test.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("peer_stash_node requires Linux");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use base64::Engine;
    use blockd_core::hostmeta::{HostConfig, ReplicaPlacementConfig};
    use blockd_core::journal::VsetConfig;
    use blockd_core::placement::PeerCandidate;
    use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};
    use blockd_runtime::{GcsConfig, GcsStore, PeerConfig, PeerTlsConfig, Runtime, RuntimeConfig};
    use rustls::RootCertStore;
    use rustls::pki_types::CertificateDer;
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    const VSET: VsetId = VsetId(1);
    const VSET_CONFIG: VsetConfig = VsetConfig {
        kind: blockd_core::journal::VsetKind::Compute,
        disk_volumes: 1,
        pages_per_volume: 64,
    };

    #[derive(Clone)]
    struct Config {
        host: HostId,
        primary: bool,
        control: SocketAddr,
        peer_listen: SocketAddr,
        peers: BTreeMap<HostId, SocketAddr>,
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

    async fn read_config(path: &Path) -> Config {
        let text = tokio::fs::read_to_string(path)
            .await
            .expect("read node config");
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
        let mut server_names = BTreeMap::new();
        let mut identities = BTreeMap::new();
        for (key, value) in &values {
            if let Some(id) = key.strip_prefix("peer.") {
                peers.insert(
                    HostId(id.parse().expect("peer id")),
                    value.parse().expect("peer address"),
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

    async fn pem(path: &Path) -> Vec<u8> {
        let text = tokio::fs::read_to_string(path).await.expect("read PEM");
        let body: String = text
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect();
        base64::engine::general_purpose::STANDARD
            .decode(body)
            .expect("PEM base64")
    }

    async fn tls(config: &Config) -> PeerTlsConfig {
        let mut roots = RootCertStore::empty();
        let mut certificate_identities = BTreeMap::new();
        for (&host, paths) in &config.identities {
            for path in paths {
                let certificate = pem(path).await;
                roots
                    .add(CertificateDer::from(certificate.clone()))
                    .expect("trust anchor");
                certificate_identities.insert(certificate, host);
            }
        }
        PeerTlsConfig::from_der(
            roots,
            pem(&config.certificate).await,
            &pem(&config.private_key).await,
            config.server_names.clone(),
            certificate_identities,
        )
    }

    async fn runtime_config(config: &Config) -> RuntimeConfig {
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
            daemon: HostConfig {
                archive: blockd_core::hostmeta::ArchivePolicy::default(),
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
                    authority: None,
                }),
            },
            blob_dir: config.blob_dir.clone(),
            peer: Some(PeerConfig {
                listen: config.peer_listen,
                peers: config.peers.clone(),
                tls: Some(tls(config).await),
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

    async fn reply(stream: &mut tokio::net::tcp::OwnedWriteHalf, text: &str) {
        stream
            .write_all(text.as_bytes())
            .await
            .expect("control reply");
        stream.write_all(b"\n").await.expect("control newline");
    }

    async fn handle(runtime: Arc<Runtime>, stream: TcpStream) {
        let (stream, mut reply_stream) = stream.into_split();
        let mut command = String::new();
        BufReader::new(stream)
            .read_line(&mut command)
            .await
            .expect("read control command");
        let parts: Vec<&str> = command.split_whitespace().collect();
        match parts.as_slice() {
            ["PING"] => reply(&mut reply_stream, "OK").await,
            ["WRITE", value] => {
                let value: u64 = value.parse().expect("write value");
                runtime.guest_write(VSET, page(), value).await;
                assert!(runtime.guest_sync(VSET, VolumeIdx(1)).await);
                reply(&mut reply_stream, "OK").await;
            }
            ["READ"] => {
                let bytes = runtime.guest_read(VSET, page()).await;
                let value = u64::from_ne_bytes(bytes[..8].try_into().expect("word"));
                reply(&mut reply_stream, &format!("VALUE {value}")).await;
            }
            ["METRICS"] => {
                let counters = runtime.counters();
                let metrics = runtime.replica_metrics();
                let response = metrics.iter().find(|metric| metric.vset == VSET).map_or_else(
                    || "PASSIVE".to_owned(),
                    |metric| format!(
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
                reply(&mut reply_stream, &response).await;
            }
            ["SPOOL"] => {
                let response = format!("{:?}", runtime.replica_spool_metrics());
                reply(&mut reply_stream, &response).await;
            }
            _ => reply(&mut reply_stream, "ERROR unknown command").await,
        }
    }

    pub async fn main() {
        let path = std::env::args().nth(1).unwrap_or_else(|| {
            eprintln!("usage: peer_stash_node CONFIG");
            std::process::exit(2)
        });
        let config = read_config(Path::new(&path)).await;
        let runtime_config = runtime_config(&config).await;
        let existed = tokio::fs::try_exists(&config.blob_dir)
            .await
            .expect("inspect blob directory");
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            endpoint: config.endpoint.clone(),
            metadata_endpoint: config.metadata_endpoint.clone(),
        }));
        let runtime = Arc::new(if existed {
            let vsets = if config.primary {
                BTreeMap::from([(VSET, VSET_CONFIG)])
            } else {
                BTreeMap::new()
            };
            let (runtime, _) = Runtime::recover(&runtime_config, store, &vsets).await;
            if config.primary {
                let _ = runtime.wait_recovered(VSET).await;
            }
            runtime
        } else {
            let runtime = Runtime::new(&runtime_config, store).await;
            if config.primary {
                runtime.create_vset(VSET, VSET_CONFIG).await;
            }
            runtime
        });
        let listener = TcpListener::bind(config.control)
            .await
            .expect("control listen");
        eprintln!("peer-stash node {} ready", config.host.0);
        loop {
            let (stream, _) = listener.accept().await.expect("control accept");
            tokio::spawn(handle(Arc::clone(&runtime), stream));
        }
    }
}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() {
    tokio::task::LocalSet::new().run_until(linux::main()).await;
}
