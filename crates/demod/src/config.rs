//! Flat `key = value` configuration — local identity, placement policy,
//! bucket, and artifact paths carried by a file. Peer endpoints are discovered
//! dynamically from object storage.
//!
//! ```text
//! host = 0
//! api = 10.0.0.2:7000
//! peer_listen = 10.0.0.2:7001
//! placement.0 = 1
//! placement.1 = 2
//! gcs_endpoint = https://storage.googleapis.com
//! gcs_metadata = http://metadata.google.internal
//! gcs_bucket = my-bucket
//! gcs_prefix = blockd/
//! blob_dir = /var/opt/blockd/blobs
//! scratch = /var/opt/blockd/scratch
//! shmem_dir = /dev/shm
//! fc_dir = /var/tmp/blockd-fc
//! cache_pages = 4096
//! writeback_interval_ms = 10
//! backup_retry_ms = 100
//! archive_interval_ms = 10000
//! archive_lag_bytes = 33554432
//! peer_spool_capacity_bytes = 2147483648
//! peer_spool_headroom_bytes = 268435456
//! disk_capacity_bytes = 107374182400
//! disk_headroom_bytes = 10737418240
//! wedge_ticks = 500
//! ```

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use blockd_core::placement::PeerCandidate;
use blockd_core::types::HostId;

#[derive(Clone, Debug)]
pub struct DemodConfig {
    pub host: HostId,
    pub api: SocketAddr,
    pub peer_listen: SocketAddr,
    pub placement: Vec<PeerCandidate>,
    pub gcs_endpoint: String,
    pub gcs_metadata: String,
    pub gcs_bucket: String,
    pub gcs_prefix: String,
    pub blob_dir: PathBuf,
    pub scratch: PathBuf,
    pub shmem_dir: PathBuf,
    pub fc_dir: PathBuf,
    pub cache_pages: usize,
    pub writeback_interval_ms: u64,
    pub backup_retry_ms: u64,
    pub archive_interval_ms: u64,
    pub archive_lag_bytes: u64,
    pub peer_spool_capacity_bytes: u64,
    pub peer_spool_headroom_bytes: u64,
    pub disk_capacity_bytes: Option<u64>,
    pub disk_headroom_bytes: u64,
    pub wedge_ticks: u64,
}

impl DemodConfig {
    pub async fn load(path: &str) -> DemodConfig {
        let text = tokio::fs::read_to_string(path).await.expect("config file");
        Self::parse(&text)
    }

    #[allow(clippy::too_many_lines)]
    fn parse(text: &str) -> DemodConfig {
        let mut kv: BTreeMap<String, String> = BTreeMap::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line.split_once('=').expect("key = value");
            kv.insert(key.trim().to_owned(), value.trim().to_owned());
        }
        assert!(
            !kv.keys().any(|key| key.starts_with("peer.")),
            "peer endpoints are discovered from object storage"
        );
        let get = |key: &str| -> String {
            kv.get(key)
                .unwrap_or_else(|| panic!("config missing `{key}`"))
                .clone()
        };
        let optional = |key: &str| kv.get(key).cloned();
        let parse_or = |key: &str, default: u64| -> u64 {
            optional(key).map_or(default, |value| {
                value.parse().unwrap_or_else(|_| panic!("invalid `{key}`"))
            })
        };
        let mut placement = Vec::new();
        for (key, value) in &kv {
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
        let cache_pages =
            usize::try_from(parse_or("cache_pages", 4096)).expect("cache_pages fits this platform");
        let writeback_interval_ms = parse_or("writeback_interval_ms", 10);
        let backup_retry_ms = parse_or("backup_retry_ms", 100);
        let archive_interval_ms = parse_or("archive_interval_ms", 10_000);
        let archive_lag_bytes = parse_or("archive_lag_bytes", 32 * 1024 * 1024);
        let peer_spool_capacity_bytes =
            parse_or("peer_spool_capacity_bytes", 2 * 1024 * 1024 * 1024);
        let peer_spool_headroom_bytes = parse_or("peer_spool_headroom_bytes", 256 * 1024 * 1024);
        let disk_capacity_bytes = optional("disk_capacity_bytes")
            .map(|value| value.parse().expect("invalid `disk_capacity_bytes`"));
        let disk_headroom_bytes = parse_or("disk_headroom_bytes", 0);
        let wedge_ticks = parse_or("wedge_ticks", 500);
        assert!(cache_pages > 0, "cache_pages must be positive");
        assert!(
            writeback_interval_ms > 0,
            "writeback_interval_ms must be positive"
        );
        assert!(backup_retry_ms > 0, "backup_retry_ms must be positive");
        assert!(
            archive_interval_ms > 0,
            "archive_interval_ms must be positive"
        );
        assert!(archive_lag_bytes > 0, "archive_lag_bytes must be positive");
        assert!(
            peer_spool_headroom_bytes < peer_spool_capacity_bytes,
            "peer_spool_headroom_bytes must be smaller than peer_spool_capacity_bytes"
        );
        if let Some(capacity) = disk_capacity_bytes {
            assert!(
                disk_headroom_bytes < capacity,
                "disk_headroom_bytes must be smaller than disk_capacity_bytes"
            );
        }
        DemodConfig {
            host: HostId(get("host").parse().expect("host id")),
            api: get("api").parse().expect("api addr"),
            peer_listen: get("peer_listen").parse().expect("peer_listen addr"),
            placement,
            gcs_endpoint: get("gcs_endpoint"),
            gcs_metadata: get("gcs_metadata"),
            gcs_bucket: get("gcs_bucket"),
            gcs_prefix: get("gcs_prefix"),
            blob_dir: get("blob_dir").into(),
            scratch: get("scratch").into(),
            shmem_dir: get("shmem_dir").into(),
            fc_dir: get("fc_dir").into(),
            cache_pages,
            writeback_interval_ms,
            backup_retry_ms,
            archive_interval_ms,
            archive_lag_bytes,
            peer_spool_capacity_bytes,
            peer_spool_headroom_bytes,
            disk_capacity_bytes,
            disk_headroom_bytes,
            wedge_ticks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REQUIRED: &str = "\
host = 2
api = 127.0.0.1:7000
peer_listen = 127.0.0.1:7001
placement.1 = 2
gcs_endpoint = http://127.0.0.1:7099
gcs_metadata = http://127.0.0.1:7098
gcs_bucket = test
gcs_prefix = data/
blob_dir = /tmp/blobs
scratch = /tmp/scratch
shmem_dir = /dev/shm
fc_dir = /tmp/fc
";

    #[test]
    fn operational_settings_have_safe_defaults_and_can_be_overridden() {
        let defaults = DemodConfig::parse(REQUIRED);
        assert_eq!(defaults.cache_pages, 4096);
        assert_eq!(defaults.writeback_interval_ms, 10);
        assert_eq!(defaults.backup_retry_ms, 100);
        assert_eq!(defaults.archive_interval_ms, 10_000);
        assert_eq!(defaults.archive_lag_bytes, 32 * 1024 * 1024);
        assert_eq!(defaults.peer_spool_capacity_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(defaults.peer_spool_headroom_bytes, 256 * 1024 * 1024);
        assert_eq!(defaults.disk_capacity_bytes, None);
        assert_eq!(defaults.disk_headroom_bytes, 0);
        assert_eq!(defaults.wedge_ticks, 500);
        assert_eq!(defaults.placement.len(), 1);
        assert_eq!(defaults.placement[0].host, HostId(1));
        assert_eq!(defaults.placement[0].failure_domain, 2);

        let configured = DemodConfig::parse(&format!(
            "{REQUIRED}\ncache_pages = 8192\nwriteback_interval_ms = 25\nbackup_retry_ms = 250\narchive_interval_ms = 5000\narchive_lag_bytes = 123456\npeer_spool_capacity_bytes = 900000\npeer_spool_headroom_bytes = 100000\ndisk_capacity_bytes = 1000000\ndisk_headroom_bytes = 100000\nwedge_ticks = 40\n"
        ));
        assert_eq!(configured.cache_pages, 8192);
        assert_eq!(configured.writeback_interval_ms, 25);
        assert_eq!(configured.backup_retry_ms, 250);
        assert_eq!(configured.archive_interval_ms, 5_000);
        assert_eq!(configured.archive_lag_bytes, 123_456);
        assert_eq!(configured.peer_spool_capacity_bytes, 900_000);
        assert_eq!(configured.peer_spool_headroom_bytes, 100_000);
        assert_eq!(configured.disk_capacity_bytes, Some(1_000_000));
        assert_eq!(configured.disk_headroom_bytes, 100_000);
        assert_eq!(configured.wedge_ticks, 40);
    }

    #[test]
    #[should_panic(expected = "peer endpoints are discovered from object storage")]
    fn static_peer_endpoints_are_rejected() {
        DemodConfig::parse(&format!("{REQUIRED}\npeer.9 = 127.0.0.1:9000\n"));
    }
}
