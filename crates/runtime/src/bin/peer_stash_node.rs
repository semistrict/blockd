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

    use blockd_core::hostmeta::{HostConfig, ReplicaPlacementConfig};
    use blockd_core::journal::VsetConfig;
    use blockd_core::placement::PeerCandidate;
    use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};
    use blockd_runtime::{GcsConfig, GcsStore, PeerConfig, Runtime, RuntimeConfig};
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
        placement: Vec<PeerCandidate>,
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
        assert!(
            !values.keys().any(|key| key.starts_with("peer.")),
            "peer endpoints are discovered from object storage"
        );
        let get = |key: &str| values.get(key).unwrap_or_else(|| panic!("missing {key}"));
        let mut placement = Vec::new();
        for (key, value) in &values {
            if let Some(id) = key.strip_prefix("placement.") {
                placement.push(PeerCandidate {
                    host: HostId(id.parse().expect("placement host id")),
                    weight: 1,
                    failure_domain: value.parse().expect("placement failure domain"),
                    drained: false,
                });
            }
        }
        placement.sort_by_key(|candidate| candidate.host);
        Config {
            host: HostId(get("host").parse().expect("host")),
            primary: get("primary") == "true",
            control: get("control").parse().expect("control address"),
            peer_listen: get("peer_listen").parse().expect("peer listen"),
            placement,
            blob_dir: get("blob_dir").into(),
            endpoint: get("endpoint").clone(),
            metadata_endpoint: get("metadata_endpoint").clone(),
            bucket: get("bucket").clone(),
            prefix: get("prefix").clone(),
        }
    }

    fn runtime_config(config: &Config) -> RuntimeConfig {
        let roster = config.placement.clone();
        let local_failure_domain = roster
            .iter()
            .find(|candidate| candidate.host == config.host)
            .map_or(config.host.0 + 1, |candidate| candidate.failure_domain);
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
                    local_failure_domain,
                    roster,
                    authority: None,
                }),
            },
            blob_dir: config.blob_dir.clone(),
            peer: Some(PeerConfig {
                listen: config.peer_listen,
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
        let existed = tokio::fs::try_exists(&config.blob_dir)
            .await
            .expect("inspect blob directory");
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            endpoint: config.endpoint.clone(),
            metadata_endpoint: config.metadata_endpoint.clone(),
        }));
        let runtime_config = runtime_config(&config);
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
