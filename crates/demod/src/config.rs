//! Flat `key = value` configuration — the control plane's knowledge
//! (host identity, roster, bucket, artifact paths) carried by a file.
//!
//! ```text
//! host = 0
//! api = 10.0.0.2:7000
//! peer_listen = 10.0.0.2:7001
//! peer.0 = 10.0.0.2:7001
//! peer.1 = 10.0.0.3:7001
//! gcs_endpoint = https://storage.googleapis.com
//! gcs_metadata = http://metadata.google.internal
//! gcs_bucket = my-bucket
//! gcs_prefix = blockd/
//! blob_dir = /var/opt/blockd/blobs
//! scratch = /var/opt/blockd/scratch
//! shmem_dir = /dev/shm
//! fc_dir = /var/tmp/blockd-fc
//! ```

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use blockd_core::types::HostId;

#[derive(Clone, Debug)]
pub struct DemodConfig {
    pub host: HostId,
    pub api: SocketAddr,
    pub peer_listen: SocketAddr,
    pub peers: BTreeMap<HostId, SocketAddr>,
    pub gcs_endpoint: String,
    pub gcs_metadata: String,
    pub gcs_bucket: String,
    pub gcs_prefix: String,
    pub blob_dir: PathBuf,
    pub scratch: PathBuf,
    pub shmem_dir: PathBuf,
    pub fc_dir: PathBuf,
}

impl DemodConfig {
    pub fn load(path: &str) -> DemodConfig {
        let text = std::fs::read_to_string(path).expect("config file");
        let mut kv: BTreeMap<String, String> = BTreeMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line.split_once('=').expect("key = value");
            kv.insert(key.trim().to_owned(), value.trim().to_owned());
        }
        let get = |key: &str| -> String {
            kv.get(key)
                .unwrap_or_else(|| panic!("config missing `{key}`"))
                .clone()
        };
        let mut peers = BTreeMap::new();
        for (key, value) in &kv {
            if let Some(id) = key.strip_prefix("peer.") {
                peers.insert(
                    HostId(id.parse().expect("peer id")),
                    value.parse().expect("peer addr"),
                );
            }
        }
        DemodConfig {
            host: HostId(get("host").parse().expect("host id")),
            api: get("api").parse().expect("api addr"),
            peer_listen: get("peer_listen").parse().expect("peer_listen addr"),
            peers,
            gcs_endpoint: get("gcs_endpoint"),
            gcs_metadata: get("gcs_metadata"),
            gcs_bucket: get("gcs_bucket"),
            gcs_prefix: get("gcs_prefix"),
            blob_dir: get("blob_dir").into(),
            scratch: get("scratch").into(),
            shmem_dir: get("shmem_dir").into(),
            fc_dir: get("fc_dir").into(),
        }
    }
}
