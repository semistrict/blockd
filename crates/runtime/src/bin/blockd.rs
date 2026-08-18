#[cfg(target_os = "linux")]
use std::net::{IpAddr, SocketAddr};
#[cfg(target_os = "linux")]
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[cfg(target_os = "linux")]
use blockd_core::hostmeta::{ArchivePolicy, HostConfig, ReplicaPlacementConfig};
#[cfg(target_os = "linux")]
use blockd_core::placement::PeerCandidate;
#[cfg(target_os = "linux")]
use blockd_runtime::cluster::{GcsStoreUri, bootstrap};
#[cfg(target_os = "linux")]
use blockd_runtime::{ObjectStore, PeerConfig, Runtime, RuntimeConfig};

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("blockd is Linux-only");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
struct ServeArgs {
    store: GcsStoreUri,
    data_dir: PathBuf,
    peer: Option<SocketAddr>,
}

#[cfg(target_os = "linux")]
fn usage() -> ! {
    eprintln!("usage: blockd serve gs://BUCKET/PREFIX [--data-dir PATH] [--peer IP:PORT]");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
fn parse_args() -> Result<ServeArgs, String> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("serve") {
        usage();
    }
    let store = args.next().ok_or_else(|| "missing store URI".to_owned())?;
    let mut data_dir = PathBuf::from("/var/lib/blockd");
    let mut peer = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--data-dir" => {
                data_dir = args
                    .next()
                    .map(PathBuf::from)
                    .ok_or_else(|| "--data-dir requires a path".to_owned())?;
            }
            "--peer" => {
                peer = Some(
                    args.next()
                        .ok_or_else(|| "--peer requires IP:PORT".to_owned())?
                        .parse()
                        .map_err(|_| "invalid --peer address".to_owned())?,
                );
            }
            _ => return Err(format!("unknown argument: {flag}")),
        }
    }
    Ok(ServeArgs {
        store: GcsStoreUri::parse(&store).map_err(|error| error.to_string())?,
        data_dir,
        peer,
    })
}

#[cfg(target_os = "linux")]
fn discover_private_ip() -> Result<IpAddr, String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")
        .map_err(|error| format!("address discovery bind failed: {error}"))?;
    socket
        .connect("169.254.169.254:80")
        .map_err(|error| format!("address discovery failed: {error}"))?;
    socket
        .local_addr()
        .map(|address| address.ip())
        .map_err(|error| format!("address discovery failed: {error}"))
}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() {
    let args = parse_args().unwrap_or_else(|error| {
        eprintln!("{error}");
        usage();
    });
    let concrete_store = args.store.store();
    let store: Arc<dyn ObjectStore> = concrete_store;
    let store_binding = args.store.to_string();
    let (cluster_id, identity) = bootstrap(Arc::clone(&store), &args.data_dir, &store_binding)
        .await
        .unwrap_or_else(|error| {
            eprintln!("cluster bootstrap failed: {error}");
            std::process::exit(1);
        });
    let peer = args.peer.unwrap_or_else(|| {
        SocketAddr::new(
            discover_private_ip().unwrap_or_else(|error| {
                eprintln!("{error}; pass --peer IP:PORT");
                std::process::exit(1);
            }),
            7001,
        )
    });
    let roster = vec![PeerCandidate {
        host: identity.host,
        weight: 1,
        failure_domain: identity.host.0,
        drained: false,
    }];
    let config = RuntimeConfig {
        daemon: HostConfig {
            archive: ArchivePolicy::default(),
            host: identity.host,
            cache_pages: 4096,
            writeback_interval: blockd_core::types::millis(10),
            backup_retry: blockd_core::types::millis(100),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 500,
            replica_placement: Some(ReplicaPlacementConfig {
                membership_epoch: 1,
                local_failure_domain: identity.host.0,
                roster,
                authority: None,
            }),
        },
        blob_dir: args.data_dir.join("blobs"),
        peer: Some(PeerConfig {
            listen: SocketAddr::new("0.0.0.0".parse().expect("wildcard address"), peer.port()),
            advertise: peer,
        }),
    };
    let mut runtime = Runtime::new(&config, store).await;
    eprintln!(
        "joined cluster {cluster_id:016x} as host {} at {peer}",
        identity.host.0
    );
    tokio::signal::ctrl_c()
        .await
        .expect("install signal handler");
    runtime.shutdown().await;
}
