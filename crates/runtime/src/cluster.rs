//! Object-store-only cluster bootstrap.
//!
//! A bucket and prefix identify one cluster. Each data directory owns a
//! random token and one permanently claimed compact host ID. Claim creation
//! is conditional, so machines starting concurrently cannot select the same
//! identity.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blockd_core::layout::{cluster_metadata_key, node_claim_key, node_claim_prefix};
use blockd_core::protocol::StoreFault;
use blockd_core::types::HostId;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{GcsConfig, GcsStore, ObjectStore};

const CLUSTER_MAGIC: [u8; 4] = *b"BCLU";
const TOKEN_BYTES: usize = 16;

#[derive(Debug)]
pub enum BootstrapError {
    InvalidStoreUri(String),
    Io(std::io::Error),
    Store(StoreFault),
    InvalidIdentity,
    StoreBindingMismatch {
        recorded: String,
        configured: String,
    },
    IdentityClaimedByAnotherNode(HostId),
    IdentityClaimMissing(HostId),
    ClusterBindingMismatch {
        recorded: u64,
        found: u64,
    },
    HostIdsExhausted,
    InvalidClusterMetadata,
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStoreUri(reason) => write!(formatter, "invalid store URI: {reason}"),
            Self::Io(error) => write!(formatter, "local identity I/O failed: {error}"),
            Self::Store(error) => write!(formatter, "object store bootstrap failed: {error:?}"),
            Self::InvalidIdentity => write!(formatter, "invalid local node identity"),
            Self::StoreBindingMismatch {
                recorded,
                configured,
            } => write!(
                formatter,
                "local state belongs to {recorded}, not configured store {configured}"
            ),
            Self::IdentityClaimedByAnotherNode(host) => {
                write!(formatter, "host ID {} is claimed by another node", host.0)
            }
            Self::IdentityClaimMissing(host) => {
                write!(formatter, "durable claim for host ID {} is missing", host.0)
            }
            Self::ClusterBindingMismatch { recorded, found } => write!(
                formatter,
                "local state belongs to cluster {recorded:016x}, but store contains {found:016x}"
            ),
            Self::HostIdsExhausted => write!(formatter, "cluster has no free host IDs"),
            Self::InvalidClusterMetadata => write!(formatter, "invalid cluster metadata"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<std::io::Error> for BootstrapError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<StoreFault> for BootstrapError {
    fn from(error: StoreFault) -> Self {
        Self::Store(error)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcsStoreUri {
    pub bucket: String,
    pub prefix: String,
}

impl GcsStoreUri {
    pub fn parse(uri: &str) -> Result<Self, BootstrapError> {
        let Some(rest) = uri.strip_prefix("gs://") else {
            return Err(BootstrapError::InvalidStoreUri(
                "expected gs://bucket/prefix".to_owned(),
            ));
        };
        if rest.contains(['?', '#']) {
            return Err(BootstrapError::InvalidStoreUri(
                "query strings and fragments are not supported".to_owned(),
            ));
        }
        let (bucket, path) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() || bucket.contains(char::is_whitespace) {
            return Err(BootstrapError::InvalidStoreUri(
                "bucket is missing or malformed".to_owned(),
            ));
        }
        let trimmed = path.trim_matches('/');
        let prefix = if trimmed.is_empty() {
            String::new()
        } else {
            format!("{trimmed}/")
        };
        Ok(Self {
            bucket: bucket.to_owned(),
            prefix,
        })
    }

    pub fn store(&self) -> Arc<GcsStore> {
        Arc::new(GcsStore::new(GcsConfig {
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
            endpoint: "https://storage.googleapis.com".to_owned(),
            metadata_endpoint: "http://metadata.google.internal".to_owned(),
        }))
    }
}

impl fmt::Display for GcsStoreUri {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "gs://{}/{}", self.bucket, self.prefix)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeIdentity {
    pub host: HostId,
    token: [u8; TOKEN_BYTES],
}

#[derive(Clone)]
struct IdentityFile {
    store: String,
    cluster_id: u64,
    token: [u8; TOKEN_BYTES],
    host: Option<HostId>,
}

pub async fn bootstrap(
    store: Arc<dyn ObjectStore>,
    data_dir: &Path,
    store_binding: &str,
) -> Result<(u64, NodeIdentity), BootstrapError> {
    if store_binding.is_empty() || store_binding.contains(['\n', '\r']) {
        return Err(BootstrapError::InvalidIdentity);
    }
    tokio::fs::create_dir_all(data_dir).await?;
    let identity_path = data_dir.join("node.identity");
    let (mut local, cluster_id) = match tokio::fs::read_to_string(&identity_path).await {
        Ok(encoded) => {
            let identity = decode_identity(&encoded)?;
            if identity.store != store_binding {
                return Err(BootstrapError::StoreBindingMismatch {
                    recorded: identity.store,
                    configured: store_binding.to_owned(),
                });
            }
            let found = read_cluster(Arc::clone(&store)).await?;
            if identity.cluster_id != found {
                return Err(BootstrapError::ClusterBindingMismatch {
                    recorded: identity.cluster_id,
                    found,
                });
            }
            (identity, found)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let cluster_id = bootstrap_cluster(Arc::clone(&store)).await?;
            let identity = IdentityFile {
                store: store_binding.to_owned(),
                cluster_id,
                token: random_token().await?,
                host: None,
            };
            write_identity(&identity_path, &identity).await?;
            (identity, cluster_id)
        }
        Err(error) => return Err(error.into()),
    };

    if let Some(host) = local.host {
        verify_claim(Arc::clone(&store), host, &local.token).await?;
        return Ok((
            cluster_id,
            NodeIdentity {
                host,
                token: local.token,
            },
        ));
    }

    if let Some(host) = find_existing_claim(Arc::clone(&store), &local.token).await? {
        local.host = Some(host);
        write_identity(&identity_path, &local).await?;
        return Ok((
            cluster_id,
            NodeIdentity {
                host,
                token: local.token,
            },
        ));
    }

    let start = u16::from_le_bytes([local.token[0], local.token[1]]);
    for offset in 0..=u16::MAX {
        let host = HostId(start.wrapping_add(offset));
        match Arc::clone(&store)
            .put_cas(node_claim_key(host), None, local.token.to_vec())
            .await
        {
            Ok(_) => {
                local.host = Some(host);
                write_identity(&identity_path, &local).await?;
                return Ok((
                    cluster_id,
                    NodeIdentity {
                        host,
                        token: local.token,
                    },
                ));
            }
            Err(StoreFault::CasConflict { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(BootstrapError::HostIdsExhausted)
}

async fn bootstrap_cluster(store: Arc<dyn ObjectStore>) -> Result<u64, BootstrapError> {
    let key = cluster_metadata_key();
    if let Some((_, bytes)) = Arc::clone(&store).get(key.clone()).await? {
        return decode_cluster_metadata(&bytes);
    }
    let random = random_token().await?;
    let cluster_id = u64::from_le_bytes(random[..8].try_into().expect("token width")).max(1);
    let bytes = encode_cluster_metadata(cluster_id);
    match Arc::clone(&store).put_cas(key.clone(), None, bytes).await {
        Ok(_) => Ok(cluster_id),
        Err(StoreFault::CasConflict { .. }) => {
            let Some((_, winner)) = store.get(key).await? else {
                return Err(BootstrapError::InvalidClusterMetadata);
            };
            decode_cluster_metadata(&winner)
        }
        Err(error) => Err(error.into()),
    }
}

async fn read_cluster(store: Arc<dyn ObjectStore>) -> Result<u64, BootstrapError> {
    let Some((_, bytes)) = store.get(cluster_metadata_key()).await? else {
        return Err(BootstrapError::InvalidClusterMetadata);
    };
    decode_cluster_metadata(&bytes)
}

fn encode_cluster_metadata(cluster_id: u64) -> Vec<u8> {
    assert!(cluster_id != 0, "cluster ID must be nonzero");
    let mut bytes = Vec::with_capacity(12);
    bytes.extend_from_slice(&CLUSTER_MAGIC);
    bytes.extend_from_slice(&cluster_id.to_le_bytes());
    bytes
}

fn decode_cluster_metadata(bytes: &[u8]) -> Result<u64, BootstrapError> {
    if bytes.len() != 12 || bytes[..4] != CLUSTER_MAGIC {
        return Err(BootstrapError::InvalidClusterMetadata);
    }
    let cluster_id = u64::from_le_bytes(bytes[4..].try_into().expect("checked width"));
    (cluster_id != 0)
        .then_some(cluster_id)
        .ok_or(BootstrapError::InvalidClusterMetadata)
}

async fn verify_claim(
    store: Arc<dyn ObjectStore>,
    host: HostId,
    token: &[u8; TOKEN_BYTES],
) -> Result<(), BootstrapError> {
    let key = node_claim_key(host);
    match Arc::clone(&store).get(key.clone()).await? {
        Some((_, owner)) if owner == token => Ok(()),
        Some(_) => Err(BootstrapError::IdentityClaimedByAnotherNode(host)),
        None => Err(BootstrapError::IdentityClaimMissing(host)),
    }
}

async fn find_existing_claim(
    store: Arc<dyn ObjectStore>,
    token: &[u8; TOKEN_BYTES],
) -> Result<Option<HostId>, BootstrapError> {
    for key in Arc::clone(&store).list_prefix(node_claim_prefix()).await? {
        let Some(host) = host_from_claim_key(&key) else {
            continue;
        };
        if Arc::clone(&store)
            .get(key)
            .await?
            .is_some_and(|(_, owner)| owner == token)
        {
            return Ok(Some(host));
        }
    }
    Ok(None)
}

fn host_from_claim_key(key: &str) -> Option<HostId> {
    let name = key.strip_prefix(&node_claim_prefix())?;
    let encoded = name.strip_suffix(".claim")?;
    let host = (encoded.len() == 4)
        .then(|| u16::from_str_radix(encoded, 16).ok().map(HostId))
        .flatten()?;
    (key == node_claim_key(host)).then_some(host)
}

async fn random_token() -> Result<[u8; TOKEN_BYTES], BootstrapError> {
    let mut file = tokio::fs::File::open("/dev/urandom").await?;
    let mut token = [0_u8; TOKEN_BYTES];
    file.read_exact(&mut token).await?;
    Ok(token)
}

fn decode_identity(encoded: &str) -> Result<IdentityFile, BootstrapError> {
    let mut store = None;
    let mut cluster_id = None;
    let mut token = None;
    let mut host = None;
    for line in encoded.lines() {
        if let Some(value) = line.strip_prefix("store=") {
            if value.is_empty() {
                return Err(BootstrapError::InvalidIdentity);
            }
            store = Some(value.to_owned());
        } else if let Some(value) = line.strip_prefix("cluster=") {
            let value =
                u64::from_str_radix(value, 16).map_err(|_| BootstrapError::InvalidIdentity)?;
            if value == 0 {
                return Err(BootstrapError::InvalidIdentity);
            }
            cluster_id = Some(value);
        } else if let Some(value) = line.strip_prefix("token=") {
            token = Some(decode_token(value)?);
        } else if let Some(value) = line.strip_prefix("host=") {
            host = Some(HostId(
                value.parse().map_err(|_| BootstrapError::InvalidIdentity)?,
            ));
        } else if !line.is_empty() {
            return Err(BootstrapError::InvalidIdentity);
        }
    }
    Ok(IdentityFile {
        store: store.ok_or(BootstrapError::InvalidIdentity)?,
        cluster_id: cluster_id.ok_or(BootstrapError::InvalidIdentity)?,
        token: token.ok_or(BootstrapError::InvalidIdentity)?,
        host,
    })
}

fn decode_token(encoded: &str) -> Result<[u8; TOKEN_BYTES], BootstrapError> {
    if encoded.len() != TOKEN_BYTES * 2 {
        return Err(BootstrapError::InvalidIdentity);
    }
    let mut token = [0_u8; TOKEN_BYTES];
    for (index, byte) in token.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16)
            .map_err(|_| BootstrapError::InvalidIdentity)?;
    }
    Ok(token)
}

async fn write_identity(path: &Path, identity: &IdentityFile) -> Result<(), BootstrapError> {
    let encoded = format!(
        "store={}\ncluster={:016x}\ntoken={}{}",
        identity.store,
        identity.cluster_id,
        encode_token(&identity.token),
        identity
            .host
            .map_or_else(String::new, |host| format!("\nhost={}", host.0))
    );
    let temporary = temporary_identity_path(path, &identity.token);
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .await?;
    file.write_all(encoded.as_bytes()).await?;
    file.write_all(b"\n").await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary, path).await?;
    Ok(())
}

fn temporary_identity_path(path: &Path, token: &[u8; TOKEN_BYTES]) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(format!(".{}.tmp", &encode_token(token)[..8]));
    PathBuf::from(temporary)
}

fn encode_token(token: &[u8; TOKEN_BYTES]) -> String {
    let mut encoded = String::with_capacity(TOKEN_BYTES * 2);
    for byte in token {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("write to string");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakegcs::FakeGcs;
    use tempfile::tempdir;

    #[test]
    fn parses_cluster_store_uri() {
        assert_eq!(
            GcsStoreUri::parse("gs://fleet/state/").expect("URI"),
            GcsStoreUri {
                bucket: "fleet".to_owned(),
                prefix: "state/".to_owned(),
            }
        );
        assert_eq!(GcsStoreUri::parse("gs://fleet").expect("URI").prefix, "");
        assert!(GcsStoreUri::parse("s3://fleet/state").is_err());
        assert!(GcsStoreUri::parse("gs:///state").is_err());
    }

    #[tokio::test]
    async fn concurrent_empty_directories_claim_distinct_hosts_and_restarts_reuse_identity() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "zero/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let first_dir = tempdir().expect("first data dir");
        let second_dir = tempdir().expect("second data dir");
        let (first, second) = tokio::join!(
            bootstrap(Arc::clone(&store), first_dir.path(), "gs://cluster/zero/"),
            bootstrap(Arc::clone(&store), second_dir.path(), "gs://cluster/zero/")
        );
        let (first_cluster, first) = first.expect("first bootstrap");
        let (second_cluster, second) = second.expect("second bootstrap");
        assert_eq!(first_cluster, second_cluster);
        assert_ne!(first.host, second.host);

        let (restart_cluster, restart) = bootstrap(store, first_dir.path(), "gs://cluster/zero/")
            .await
            .expect("restart bootstrap");
        assert_eq!(restart_cluster, first_cluster);
        assert_eq!(restart, first);
    }

    #[test]
    fn identity_codec_rejects_truncation_and_unknown_fields() {
        let token = [0xabu8; TOKEN_BYTES];
        let encoded = format!(
            "store=gs://cluster/zero/\ncluster=0102030405060708\ntoken={}\nhost=7",
            encode_token(&token)
        );
        assert_eq!(
            decode_identity(&encoded).expect("identity").host,
            Some(HostId(7))
        );
        assert!(decode_identity("token=ab").is_err());
        assert!(decode_identity(&format!("{encoded}\nextra=1")).is_err());
    }

    #[tokio::test]
    async fn local_identity_rejects_a_different_store_before_remote_access() {
        let (fake, endpoint) = FakeGcs::start().await;
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "zero/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let data_dir = tempdir().expect("data dir");
        bootstrap(Arc::clone(&store), data_dir.path(), "gs://cluster/zero/")
            .await
            .expect("initial bootstrap");
        let requests_before = fake.request_count();

        let error = bootstrap(store, data_dir.path(), "gs://other/cluster/")
            .await
            .expect_err("store mismatch");
        assert!(matches!(error, BootstrapError::StoreBindingMismatch { .. }));
        assert_eq!(fake.request_count(), requests_before);
    }

    #[tokio::test]
    async fn local_identity_rejects_removed_durable_remote_state() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "zero/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let data_dir = tempdir().expect("data dir");
        let (_, identity) = bootstrap(Arc::clone(&store), data_dir.path(), "gs://cluster/zero/")
            .await
            .expect("initial bootstrap");
        Arc::clone(&store)
            .delete(node_claim_key(identity.host))
            .await;

        let error = bootstrap(Arc::clone(&store), data_dir.path(), "gs://cluster/zero/")
            .await
            .expect_err("missing claim");
        assert!(
            matches!(error, BootstrapError::IdentityClaimMissing(host) if host == identity.host)
        );

        Arc::clone(&store).delete(cluster_metadata_key()).await;
        let error = bootstrap(store, data_dir.path(), "gs://cluster/zero/")
            .await
            .expect_err("missing cluster metadata");
        assert!(matches!(error, BootstrapError::InvalidClusterMetadata));
    }

    #[test]
    fn cluster_metadata_is_byte_pinned_and_rejects_damage() {
        let encoded = encode_cluster_metadata(0x0102_0304_0506_0708);
        assert_eq!(
            encoded,
            [
                b'B', b'C', b'L', b'U', 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
            ]
        );
        assert_eq!(
            decode_cluster_metadata(&encoded).expect("metadata"),
            0x0102_0304_0506_0708
        );
        let mut damaged = encoded;
        damaged[0] ^= 1;
        assert!(decode_cluster_metadata(&damaged).is_err());
        assert!(decode_cluster_metadata(&damaged[..11]).is_err());
    }
}
