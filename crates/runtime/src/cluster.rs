//! Object-store-only cluster bootstrap.
//!
//! A bucket and prefix identify one cluster. Each data directory owns a
//! random token and one permanently claimed compact host ID. Claim creation
//! is conditional, so machines starting concurrently cannot select the same
//! identity.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use blockd_core::format::{open_frame, seal_frame};
#[cfg(test)]
use blockd_core::layout::node_claim_prefix;
use blockd_core::layout::{cluster_metadata_key, node_claim_key};
use blockd_core::protocol::StoreFault;
use blockd_core::types::HostId;
use prost::Message;
use tokio::io::AsyncReadExt;

#[cfg(test)]
use crate::ListedObject;
use crate::{GcsConfig, GcsStore, ObjectStore};

const CLUSTER_MAGIC: u32 = u32::from_le_bytes(*b"BCLU");
const NODE_IDENTITY_MAGIC: u32 = u32::from_le_bytes(*b"BNID");
const NODE_CLAIM_MAGIC: u32 = u32::from_le_bytes(*b"BNCL");
const METADATA_FORMAT_VERSION: u32 = 1;
const TOKEN_BYTES: usize = 16;
const MAX_HOST_ID_PROBES: u32 = 4_096;
const MAX_CLUSTER_METADATA_PAYLOAD_BYTES: usize = 64;
const MAX_NODE_IDENTITY_PAYLOAD_BYTES: usize = 8 * 1024;
const MAX_NODE_CLAIM_PAYLOAD_BYTES: usize = 256;

#[derive(Clone, PartialEq, Message)]
struct ClusterMetadataWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    cluster_id: u64,
}

#[derive(Clone, PartialEq, Message)]
struct NodeIdentityWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    store: String,
    #[prost(uint64, tag = "3")]
    cluster_id: u64,
    #[prost(bytes = "vec", tag = "4")]
    token: Vec<u8>,
    #[prost(uint32, optional, tag = "5")]
    host: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
struct NodeClaimWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(uint64, tag = "2")]
    cluster_id: u64,
    #[prost(uint32, tag = "3")]
    host: u32,
    #[prost(bytes = "vec", tag = "4")]
    token: Vec<u8>,
}

#[derive(Debug)]
pub enum BootstrapError {
    InvalidStoreUri(String),
    Io(std::io::Error),
    Store(StoreFault),
    InvalidIdentity,
    InterruptedIdentity,
    LocalStateWithoutIdentity,
    StateDirectoryInUse {
        owner: String,
    },
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
    HostIdProbeExhausted,
    ClusterMetadataMissing,
    InvalidClusterMetadata,
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStoreUri(reason) => write!(formatter, "invalid store URI: {reason}"),
            Self::Io(error) => write!(formatter, "local identity I/O failed: {error}"),
            Self::Store(error) => write!(formatter, "object store bootstrap failed: {error:?}"),
            Self::InvalidIdentity => write!(formatter, "invalid local node identity"),
            Self::InterruptedIdentity => write!(
                formatter,
                "interrupted node identity publication requires operator recovery"
            ),
            Self::LocalStateWithoutIdentity => write!(
                formatter,
                "recognized local data exists without node identity; refusing to mint a new identity"
            ),
            Self::StateDirectoryInUse { owner } => {
                write!(
                    formatter,
                    "local state directory is already in use ({owner})"
                )
            }
            Self::StoreBindingMismatch {
                recorded,
                configured,
            } => write!(
                formatter,
                "local state belongs to {recorded}, not configured store {configured}"
            ),
            Self::IdentityClaimedByAnotherNode(host) => {
                write!(
                    formatter,
                    "host ID {} is claimed by another node",
                    host.get()
                )
            }
            Self::IdentityClaimMissing(host) => {
                write!(
                    formatter,
                    "durable claim for host ID {} is missing",
                    host.get()
                )
            }
            Self::ClusterBindingMismatch { recorded, found } => write!(
                formatter,
                "local state belongs to cluster {recorded:016x}, but store contains {found:016x}"
            ),
            Self::HostIdProbeExhausted => {
                write!(
                    formatter,
                    "could not allocate a host ID after bounded probing"
                )
            }
            Self::ClusterMetadataMissing => write!(formatter, "cluster metadata is missing"),
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

#[derive(Clone, Debug)]
pub struct NodeIdentity {
    pub host: HostId,
    token: [u8; TOKEN_BYTES],
    _state_directory_lock: Arc<std::fs::File>,
}

impl PartialEq for NodeIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.host == other.host && self.token == other.token
    }
}

impl Eq for NodeIdentity {}

impl NodeIdentity {
    /// Revalidate the immutable cluster binding and this node's durable claim
    /// without exposing the owner token to the daemon or diagnostics.
    pub async fn remote_bindings_match(
        &self,
        store: Arc<dyn ObjectStore>,
        expected_cluster: u64,
    ) -> Result<bool, StoreFault> {
        let Some((_, metadata)) = Arc::clone(&store).get(cluster_metadata_key()).await? else {
            return Ok(false);
        };
        if decode_cluster_metadata(&metadata).ok() != Some(expected_cluster) {
            return Ok(false);
        }
        Ok(Arc::clone(&store)
            .get(node_claim_key(self.host))
            .await?
            .and_then(|(_, bytes)| decode_node_claim(&bytes))
            .is_some_and(|claim| {
                claim.cluster_id == expected_cluster
                    && claim.host == self.host.get()
                    && claim.token == self.token
            }))
    }
}

#[derive(Clone)]
struct IdentityFile {
    store: String,
    cluster_id: u64,
    token: [u8; TOKEN_BYTES],
    host: Option<HostId>,
}

#[allow(clippy::too_many_lines)]
pub async fn bootstrap(
    store: Arc<dyn ObjectStore>,
    data_dir: &Path,
    store_binding: &str,
) -> Result<(u64, NodeIdentity), BootstrapError> {
    if store_binding.is_empty() || store_binding.contains(['\n', '\r']) {
        return Err(BootstrapError::InvalidIdentity);
    }
    ensure_private_directory(data_dir)?;
    let state_directory_lock = acquire_state_directory_lock(data_dir).await?;
    let identity_path = data_dir.join("node.identity");
    let (mut local, cluster_id) = match read_identity(&identity_path).await {
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
            if interrupted_identity_exists(data_dir).await? {
                return Err(BootstrapError::InterruptedIdentity);
            }
            if has_recognized_local_state(data_dir).await? {
                return Err(BootstrapError::LocalStateWithoutIdentity);
            }
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
        verify_claim(Arc::clone(&store), cluster_id, host, &local.token).await?;
        return Ok((
            cluster_id,
            NodeIdentity {
                host,
                token: local.token,
                _state_directory_lock: Arc::clone(&state_directory_lock),
            },
        ));
    }

    let start = u32::from_le_bytes(local.token[..4].try_into().expect("token width"));
    for offset in 0..MAX_HOST_ID_PROBES {
        let host = HostId::new(start.wrapping_add(offset));
        let claim_key = node_claim_key(host);
        if let Some((_, bytes)) = Arc::clone(&store).get(claim_key.clone()).await? {
            if decode_node_claim(&bytes).is_some_and(|claim| {
                claim.cluster_id == cluster_id
                    && claim.host == host.get()
                    && claim.token == local.token
            }) {
                local.host = Some(host);
                write_identity(&identity_path, &local).await?;
                return Ok((
                    cluster_id,
                    NodeIdentity {
                        host,
                        token: local.token,
                        _state_directory_lock: Arc::clone(&state_directory_lock),
                    },
                ));
            }
            continue;
        }
        match Arc::clone(&store)
            .put_cas(
                claim_key.clone(),
                None,
                encode_node_claim(cluster_id, host, &local.token),
            )
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
                        _state_directory_lock: Arc::clone(&state_directory_lock),
                    },
                ));
            }
            Err(StoreFault::CasConflict { .. }) => {
                if Arc::clone(&store)
                    .get(claim_key)
                    .await?
                    .and_then(|(_, bytes)| decode_node_claim(&bytes))
                    .is_some_and(|claim| {
                        claim.cluster_id == cluster_id
                            && claim.host == host.get()
                            && claim.token == local.token
                    })
                {
                    local.host = Some(host);
                    write_identity(&identity_path, &local).await?;
                    return Ok((
                        cluster_id,
                        NodeIdentity {
                            host,
                            token: local.token,
                            _state_directory_lock: Arc::clone(&state_directory_lock),
                        },
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(BootstrapError::HostIdProbeExhausted)
}

fn ensure_private_directory(path: &Path) -> Result<(), BootstrapError> {
    drop(crate::world::create_private_directory(path)?);
    Ok(())
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
                return Err(BootstrapError::ClusterMetadataMissing);
            };
            decode_cluster_metadata(&winner)
        }
        Err(error) => Err(error.into()),
    }
}

async fn read_cluster(store: Arc<dyn ObjectStore>) -> Result<u64, BootstrapError> {
    let Some((_, bytes)) = store.get(cluster_metadata_key()).await? else {
        return Err(BootstrapError::ClusterMetadataMissing);
    };
    decode_cluster_metadata(&bytes)
}

fn encode_cluster_metadata(cluster_id: u64) -> Vec<u8> {
    assert!(cluster_id != 0, "cluster ID must be nonzero");
    let payload = ClusterMetadataWire {
        version: METADATA_FORMAT_VERSION,
        cluster_id,
    }
    .encode_to_vec();
    seal_frame(CLUSTER_MAGIC, &payload)
}

fn decode_cluster_metadata(bytes: &[u8]) -> Result<u64, BootstrapError> {
    let payload =
        open_frame(CLUSTER_MAGIC, bytes).map_err(|_| BootstrapError::InvalidClusterMetadata)?;
    if payload.len() > MAX_CLUSTER_METADATA_PAYLOAD_BYTES {
        return Err(BootstrapError::InvalidClusterMetadata);
    }
    let wire =
        ClusterMetadataWire::decode(payload).map_err(|_| BootstrapError::InvalidClusterMetadata)?;
    (wire.version == METADATA_FORMAT_VERSION
        && wire.cluster_id != 0
        && wire.encode_to_vec() == payload)
        .then_some(wire.cluster_id)
        .ok_or(BootstrapError::InvalidClusterMetadata)
}

fn encode_node_claim(cluster_id: u64, host: HostId, token: &[u8; TOKEN_BYTES]) -> Vec<u8> {
    let payload = NodeClaimWire {
        version: METADATA_FORMAT_VERSION,
        cluster_id,
        host: host.get(),
        token: token.to_vec(),
    }
    .encode_to_vec();
    seal_frame(NODE_CLAIM_MAGIC, &payload)
}

fn decode_node_claim(bytes: &[u8]) -> Option<NodeClaimWire> {
    let payload = open_frame(NODE_CLAIM_MAGIC, bytes).ok()?;
    if payload.len() > MAX_NODE_CLAIM_PAYLOAD_BYTES {
        return None;
    }
    let wire = NodeClaimWire::decode(payload).ok()?;
    (wire.version == METADATA_FORMAT_VERSION
        && wire.cluster_id != 0
        && wire.token.len() == TOKEN_BYTES
        && wire.encode_to_vec() == payload)
        .then_some(wire)
}

async fn verify_claim(
    store: Arc<dyn ObjectStore>,
    cluster_id: u64,
    host: HostId,
    token: &[u8; TOKEN_BYTES],
) -> Result<(), BootstrapError> {
    let key = node_claim_key(host);
    match Arc::clone(&store).get(key.clone()).await? {
        Some((_, bytes))
            if decode_node_claim(&bytes).is_some_and(|claim| {
                claim.cluster_id == cluster_id && claim.host == host.get() && claim.token == token
            }) =>
        {
            Ok(())
        }
        Some(_) => Err(BootstrapError::IdentityClaimedByAnotherNode(host)),
        None => Err(BootstrapError::IdentityClaimMissing(host)),
    }
}

async fn random_token() -> Result<[u8; TOKEN_BYTES], BootstrapError> {
    let mut file = tokio::fs::File::open("/dev/urandom").await?;
    let mut token = [0_u8; TOKEN_BYTES];
    file.read_exact(&mut token).await?;
    Ok(token)
}

async fn acquire_state_directory_lock(
    data_dir: &Path,
) -> Result<Arc<std::fs::File>, BootstrapError> {
    let data_dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || acquire_state_directory_lock_sync(&data_dir))
        .await
        .map_err(|error| std::io::Error::other(format!("state lock task failed: {error}")))?
}

fn acquire_state_directory_lock_sync(
    data_dir: &Path,
) -> Result<Arc<std::fs::File>, BootstrapError> {
    use std::io::{Read as _, Write as _};
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let directory = open_directory_nofollow(data_dir)?;
    let mut file = std::fs::File::from(
        rustix::fs::openat(
            &directory,
            "node.lock",
            rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::RDWR
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(std::io::Error::from)?,
    );
    let metadata = file.metadata()?;
    crate::world::validate_owner(metadata.uid())?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(BootstrapError::InvalidIdentity);
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    if let Err(error) =
        rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
    {
        if error == rustix::io::Errno::WOULDBLOCK {
            let mut owner = String::new();
            let _ = file.read_to_string(&mut owner);
            if owner.is_empty() {
                "owner metadata unavailable".clone_into(&mut owner);
            }
            return Err(BootstrapError::StateDirectoryInUse {
                owner: owner.trim().to_owned(),
            });
        }
        return Err(BootstrapError::Io(std::io::Error::from(error)));
    }
    file.set_len(0)?;
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    writeln!(
        &file,
        "pid={} started_unix_ms={started}",
        std::process::id()
    )?;
    file.sync_all()?;
    directory.sync_all()?;
    Ok(Arc::new(file))
}

fn open_directory_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    crate::world::open_private_directory(path)
}

async fn interrupted_identity_exists(data_dir: &Path) -> Result<bool, BootstrapError> {
    let data_dir = data_dir.to_owned();
    tokio::task::spawn_blocking(move || {
        let directory = open_directory_nofollow(&data_dir)?;
        let entries = rustix::fs::Dir::read_from(&directory).map_err(std::io::Error::from)?;
        for entry in entries {
            let entry = entry.map_err(std::io::Error::from)?;
            let name = entry.file_name().to_string_lossy();
            if name.starts_with("node.identity.") && name.ends_with(".tmp") {
                return Ok(true);
            }
        }
        Ok(false)
    })
    .await
    .map_err(|error| std::io::Error::other(format!("identity scan task failed: {error}")))?
    .map_err(BootstrapError::Io)
}

async fn has_recognized_local_state(data_dir: &Path) -> Result<bool, BootstrapError> {
    let blob_dir = data_dir.join("blobs");
    tokio::task::spawn_blocking(move || {
        let root = match crate::world::open_root_for_scan(&blob_dir) {
            Ok(root) => root,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        crate::blobscan::scan_blob_fd_for_recovery(&root).map(|blobs| !blobs.is_empty())
    })
    .await
    .map_err(|error| std::io::Error::other(format!("local state scan failed: {error}")))
    .and_then(|result| result)
    .map_err(BootstrapError::Io)
}

fn encode_identity(identity: &IdentityFile) -> Vec<u8> {
    let payload = NodeIdentityWire {
        version: METADATA_FORMAT_VERSION,
        store: identity.store.clone(),
        cluster_id: identity.cluster_id,
        token: identity.token.to_vec(),
        host: identity.host.map(HostId::get),
    }
    .encode_to_vec();
    assert!(payload.len() <= MAX_NODE_IDENTITY_PAYLOAD_BYTES);
    seal_frame(NODE_IDENTITY_MAGIC, &payload)
}

fn decode_identity(encoded: &[u8]) -> Result<IdentityFile, BootstrapError> {
    let payload =
        open_frame(NODE_IDENTITY_MAGIC, encoded).map_err(|_| BootstrapError::InvalidIdentity)?;
    if payload.len() > MAX_NODE_IDENTITY_PAYLOAD_BYTES {
        return Err(BootstrapError::InvalidIdentity);
    }
    let wire = NodeIdentityWire::decode(payload).map_err(|_| BootstrapError::InvalidIdentity)?;
    if wire.version != METADATA_FORMAT_VERSION
        || wire.store.is_empty()
        || wire.store.contains(['\n', '\r'])
        || wire.cluster_id == 0
        || wire.token.len() != TOKEN_BYTES
        || wire.encode_to_vec() != payload
    {
        return Err(BootstrapError::InvalidIdentity);
    }
    Ok(IdentityFile {
        store: wire.store,
        cluster_id: wire.cluster_id,
        token: wire
            .token
            .try_into()
            .map_err(|_| BootstrapError::InvalidIdentity)?,
        host: wire.host.map(HostId::new),
    })
}

async fn write_identity(path: &Path, identity: &IdentityFile) -> Result<(), BootstrapError> {
    let path = path.to_path_buf();
    let identity = identity.clone();
    tokio::task::spawn_blocking(move || write_identity_sync(&path, &identity))
        .await
        .map_err(|error| std::io::Error::other(format!("identity write task failed: {error}")))?
}

async fn read_identity(path: &Path) -> std::io::Result<Vec<u8>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_identity_sync(&path))
        .await
        .map_err(|error| std::io::Error::other(format!("identity read task failed: {error}")))?
}

fn read_identity_sync(path: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "identity path has no parent directory",
        )
    })?;
    let directory = open_directory_nofollow(parent)?;
    let name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "identity path has no name",
        )
    })?;
    let mut file = std::fs::File::from(
        rustix::fs::openat(
            &directory,
            name,
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(std::io::Error::from)?,
    );
    let metadata = file.metadata()?;
    crate::world::validate_owner(metadata.uid())?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsafe identity file type, link count, or permissions; expected an owner-only 0600 regular file",
        ));
    }
    let mut encoded = Vec::new();
    file.read_to_end(&mut encoded)?;
    Ok(encoded)
}

fn write_identity_sync(path: &Path, identity: &IdentityFile) -> Result<(), BootstrapError> {
    write_identity_sync_with_stage_hook(path, identity, |_| Ok(()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdentityPublicationStage {
    TemporaryCreated,
    ContentsWritten,
    FileSynced,
    Renamed,
    DirectorySynced,
}

fn write_identity_sync_with_stage_hook(
    path: &Path,
    identity: &IdentityFile,
    mut after_stage: impl FnMut(IdentityPublicationStage) -> Result<(), BootstrapError>,
) -> Result<(), BootstrapError> {
    use std::io::Write as _;
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let encoded = encode_identity(identity);
    let parent = path.parent().ok_or_else(|| {
        BootstrapError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "identity path has no parent directory",
        ))
    })?;
    let directory = open_directory_nofollow(parent)?;
    let name = path.file_name().ok_or(BootstrapError::InvalidIdentity)?;
    let temporary = temporary_identity_name(path, &identity.token)?;
    let mut file = std::fs::File::from(
        rustix::fs::openat(
            &directory,
            &temporary,
            rustix::fs::OFlags::CREATE
                | rustix::fs::OFlags::EXCL
                | rustix::fs::OFlags::WRONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .map_err(std::io::Error::from)?,
    );
    after_stage(IdentityPublicationStage::TemporaryCreated)?;
    let metadata = file.metadata()?;
    crate::world::validate_owner(metadata.uid())?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(BootstrapError::InvalidIdentity);
    }
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    file.write_all(&encoded)?;
    after_stage(IdentityPublicationStage::ContentsWritten)?;
    file.sync_all()?;
    after_stage(IdentityPublicationStage::FileSynced)?;
    drop(file);
    rustix::fs::renameat(&directory, &temporary, &directory, name).map_err(std::io::Error::from)?;
    after_stage(IdentityPublicationStage::Renamed)?;
    directory.sync_all()?;
    after_stage(IdentityPublicationStage::DirectorySynced)?;
    Ok(())
}

fn temporary_identity_name(
    path: &Path,
    token: &[u8; TOKEN_BYTES],
) -> Result<std::ffi::OsString, BootstrapError> {
    let mut temporary = path
        .file_name()
        .ok_or(BootstrapError::InvalidIdentity)?
        .to_owned();
    temporary.push(format!(".{}.tmp", &encode_token(token)[..8]));
    Ok(temporary)
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
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use super::*;
    use crate::GetResult;

    use crate::fakegcs::FakeGcs;
    use async_trait::async_trait;
    use tempfile::tempdir;

    #[derive(Default)]
    struct CommitThenFaultStore {
        objects: Mutex<BTreeMap<String, (u64, Vec<u8>)>>,
        next_generation: AtomicU64,
        injected: AtomicBool,
        claim_result_unknown: bool,
    }

    #[async_trait]
    impl ObjectStore for CommitThenFaultStore {
        async fn put(self: Arc<Self>, key: String, bytes: Vec<u8>) -> Result<u64, StoreFault> {
            let generation = self.next_generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.objects
                .lock()
                .expect("objects mutex")
                .insert(key, (generation, bytes));
            Ok(generation)
        }

        async fn put_cas(
            self: Arc<Self>,
            key: String,
            expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, StoreFault> {
            let current = self
                .objects
                .lock()
                .expect("objects mutex")
                .get(&key)
                .map(|(generation, _)| *generation);
            if current != expected {
                return Err(StoreFault::CasConflict { actual: current });
            }
            let generation = self.next_generation.fetch_add(1, Ordering::SeqCst) + 1;
            self.objects
                .lock()
                .expect("objects mutex")
                .insert(key.clone(), (generation, bytes));
            if key.starts_with(&node_claim_prefix()) && !self.injected.swap(true, Ordering::SeqCst)
            {
                if self.claim_result_unknown {
                    return Err(StoreFault::Unavailable);
                }
                return Err(StoreFault::CasConflict {
                    actual: Some(generation),
                });
            }
            Ok(generation)
        }

        async fn get(self: Arc<Self>, key: String) -> GetResult {
            Ok(self
                .objects
                .lock()
                .expect("objects mutex")
                .get(&key)
                .cloned())
        }

        async fn get_range(self: Arc<Self>, key: String, offset: u64, len: u64) -> GetResult {
            let object = self
                .objects
                .lock()
                .expect("objects mutex")
                .get(&key)
                .cloned();
            let Some((generation, bytes)) = object else {
                return Ok(None);
            };
            let start = usize::try_from(offset).map_err(|_| StoreFault::Unavailable)?;
            let length = usize::try_from(len).map_err(|_| StoreFault::Unavailable)?;
            Ok(bytes
                .get(start..start.saturating_add(length).min(bytes.len()))
                .map(|slice| (generation, slice.to_vec())))
        }

        async fn delete(self: Arc<Self>, key: String) -> Result<bool, StoreFault> {
            Ok(self
                .objects
                .lock()
                .expect("objects mutex")
                .remove(&key)
                .is_some())
        }

        async fn list_prefix(self: Arc<Self>, prefix: String) -> Result<Vec<String>, StoreFault> {
            Ok(self
                .objects
                .lock()
                .expect("objects mutex")
                .keys()
                .filter(|key| key.starts_with(&prefix))
                .cloned()
                .collect())
        }

        async fn list_prefix_versioned(
            self: Arc<Self>,
            prefix: String,
        ) -> Result<Vec<ListedObject>, StoreFault> {
            let listed = self
                .objects
                .lock()
                .expect("objects mutex")
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .map(|(key, (generation, _))| ListedObject {
                    key: key.clone(),
                    generation: *generation,
                    fingerprint: None,
                })
                .collect::<Vec<_>>();
            Ok(listed)
        }
    }

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

    #[derive(Clone, PartialEq, prost::Message)]
    struct ClusterMetadataProbe {
        #[prost(uint32, tag = "1")]
        version: u32,
        #[prost(uint64, tag = "2")]
        cluster_id: u64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct NodeIdentityProbe {
        #[prost(uint32, tag = "1")]
        version: u32,
        #[prost(string, tag = "2")]
        store: String,
        #[prost(uint64, tag = "3")]
        cluster_id: u64,
        #[prost(bytes = "vec", tag = "4")]
        token: Vec<u8>,
        #[prost(uint32, optional, tag = "5")]
        host: Option<u32>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct NodeClaimProbe {
        #[prost(uint32, tag = "1")]
        version: u32,
        #[prost(uint64, tag = "2")]
        cluster_id: u64,
        #[prost(uint32, tag = "3")]
        host: u32,
        #[prost(bytes = "vec", tag = "4")]
        token: Vec<u8>,
    }

    #[tokio::test]
    async fn bootstrap_publishes_protobuf_cluster_node_and_claim_metadata() {
        let store = Arc::new(CommitThenFaultStore::default());
        let abstract_store: Arc<dyn ObjectStore> = store.clone();
        let data_dir = tempdir().expect("data dir");
        let (cluster_id, identity) = bootstrap(
            Arc::clone(&abstract_store),
            data_dir.path(),
            "gs://cluster/protobuf/",
        )
        .await
        .expect("bootstrap");

        let (_, cluster_bytes) = Arc::clone(&abstract_store)
            .get(cluster_metadata_key())
            .await
            .expect("cluster read")
            .expect("cluster metadata");
        let cluster_payload = open_frame(CLUSTER_MAGIC, &cluster_bytes).expect("cluster frame");
        let cluster = ClusterMetadataProbe::decode(cluster_payload).expect("cluster protobuf");
        assert_eq!((cluster.version, cluster.cluster_id), (1, cluster_id));

        let identity_bytes =
            std::fs::read(data_dir.path().join("node.identity")).expect("local identity bytes");
        let identity_payload =
            open_frame(NODE_IDENTITY_MAGIC, &identity_bytes).expect("identity frame");
        let local = NodeIdentityProbe::decode(identity_payload).expect("identity protobuf");
        assert_eq!(local.cluster_id, cluster_id);
        assert_eq!(local.host, Some(identity.host.get()));

        let (_, claim_bytes) = Arc::clone(&abstract_store)
            .get(node_claim_key(identity.host))
            .await
            .expect("claim read")
            .expect("claim metadata");
        let claim_payload = open_frame(NODE_CLAIM_MAGIC, &claim_bytes).expect("claim frame");
        let claim = NodeClaimProbe::decode(claim_payload).expect("claim protobuf");
        assert_eq!(
            (claim.cluster_id, claim.host),
            (cluster_id, identity.host.get())
        );
        assert_eq!(claim.token, local.token);
    }

    #[tokio::test]
    async fn node_identity_revalidates_remote_cluster_and_claim_bindings() {
        let store = Arc::new(CommitThenFaultStore::default());
        let abstract_store: Arc<dyn ObjectStore> = store.clone();
        let data_dir = tempdir().expect("data dir");
        let (cluster_id, identity) = bootstrap(
            Arc::clone(&abstract_store),
            data_dir.path(),
            "gs://cluster/bindings/",
        )
        .await
        .expect("bootstrap");
        assert!(
            identity
                .remote_bindings_match(Arc::clone(&abstract_store), cluster_id)
                .await
                .expect("binding probe")
        );

        let claim_key = node_claim_key(identity.host);
        let original_claim = Arc::clone(&abstract_store)
            .get(claim_key.clone())
            .await
            .expect("claim read")
            .expect("claim exists")
            .1;
        Arc::clone(&abstract_store)
            .put(claim_key.clone(), b"replacement owner".to_vec())
            .await
            .expect("replace claim");
        assert!(
            !identity
                .remote_bindings_match(Arc::clone(&abstract_store), cluster_id)
                .await
                .expect("binding probe")
        );
        Arc::clone(&abstract_store)
            .put(claim_key, original_claim)
            .await
            .expect("restore claim");
        Arc::clone(&abstract_store)
            .put(cluster_metadata_key(), b"corrupt cluster metadata".to_vec())
            .await
            .expect("replace metadata");
        assert!(
            !identity
                .remote_bindings_match(abstract_store, cluster_id)
                .await
                .expect("binding probe")
        );
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

        let expected_host = first.host;
        let expected_token = first.token;
        drop(first);
        let (restart_cluster, restart) = bootstrap(store, first_dir.path(), "gs://cluster/zero/")
            .await
            .expect("restart bootstrap");
        assert_eq!(restart_cluster, first_cluster);
        assert_eq!(restart.host, expected_host);
        assert_eq!(restart.token, expected_token);
    }

    /// Regression PROD-005: one local state directory may have only one initializer.
    /// The artificial store latency keeps both calls in flight after they have
    /// observed the missing identity, making the cross-process race repeatable.
    #[tokio::test]
    async fn concurrent_bootstrap_of_one_data_directory_has_one_owner() {
        let (fake, endpoint) = FakeGcs::start().await;
        fake.latency_ms.store(20, Ordering::SeqCst);
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "one-owner/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let data_dir = tempdir().expect("data dir");

        let (first, second) = tokio::join!(
            bootstrap(
                Arc::clone(&store),
                data_dir.path(),
                "gs://cluster/one-owner/"
            ),
            bootstrap(store, data_dir.path(), "gs://cluster/one-owner/")
        );
        let successes = usize::from(first.is_ok()) + usize::from(second.is_ok());

        assert_eq!(successes, 1, "two initializers owned one local state dir");
    }

    /// Regression PROD-005: the state-directory lease is an OS lock, not
    /// process-local bookkeeping, and an unclean owner exit releases it.
    #[test]
    fn state_directory_lock_survives_competing_process_and_releases_after_crash() {
        use std::io::BufRead as _;
        use std::process::{Command, Stdio};

        fn child_command(data_dir: &Path, peer: &str) -> Command {
            let mut command =
                Command::new(std::env::current_exe().expect("current runtime test executable"));
            command
                .arg("--exact")
                .arg("cluster::tests::state_directory_lock_child")
                .arg("--nocapture")
                .env("BLOCKD_STATE_LOCK_CHILD", "1")
                .env("BLOCKD_STATE_LOCK_PATH", data_dir)
                .env("BLOCKD_STATE_LOCK_PEER", peer);
            command
        }

        let root = tempdir().expect("state directory");
        let mut owner = child_command(root.path(), "127.0.0.1:40101")
            .stdout(Stdio::piped())
            .spawn()
            .expect("start owner process");
        let mut owner_stdout = std::io::BufReader::new(owner.stdout.take().expect("owner stdout"));
        loop {
            let mut line = String::new();
            assert_ne!(
                owner_stdout
                    .read_line(&mut line)
                    .expect("read owner output"),
                0,
                "owner exited before acquiring the state lock"
            );
            if line.contains("state-lock-ready 127.0.0.1:40101") {
                break;
            }
        }

        let files_before = std::fs::read_dir(root.path())
            .expect("state listing")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        let contender = child_command(root.path(), "127.0.0.1:40102")
            .env("BLOCKD_STATE_LOCK_ONCE", "1")
            .output()
            .expect("start contender process");
        assert!(!contender.status.success());
        assert!(String::from_utf8_lossy(&contender.stderr).contains("already in use"));
        let files_after = std::fs::read_dir(root.path())
            .expect("state listing")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(files_before, files_after);

        owner.kill().expect("unclean owner exit");
        owner.wait().expect("reap owner");
        let reacquired = child_command(root.path(), "127.0.0.1:40103")
            .env("BLOCKD_STATE_LOCK_ONCE", "1")
            .output()
            .expect("reacquire after crash");
        assert!(
            reacquired.status.success(),
            "stale diagnostic metadata blocked the OS lock: {}",
            String::from_utf8_lossy(&reacquired.stderr)
        );
    }

    #[tokio::test]
    async fn state_directory_lock_child() {
        let Some(data_dir) = std::env::var_os("BLOCKD_STATE_LOCK_PATH") else {
            return;
        };
        if std::env::var_os("BLOCKD_STATE_LOCK_CHILD").is_none() {
            return;
        }
        let peer = std::env::var("BLOCKD_STATE_LOCK_PEER").expect("state lock peer");
        let data_dir = std::path::PathBuf::from(data_dir);
        let lease = match ensure_private_directory(&data_dir) {
            Ok(()) => acquire_state_directory_lock(&data_dir).await,
            Err(error) => Err(error),
        };
        let _lease = lease.unwrap_or_else(|error| {
            eprintln!("{error}");
            std::process::exit(1);
        });
        println!("state-lock-ready {peer}");
        if std::env::var_os("BLOCKD_STATE_LOCK_ONCE").is_none() {
            std::future::pending::<()>().await;
        }
    }

    #[test]
    fn identity_codec_rejects_truncation_and_unknown_fields() {
        let token = [0xabu8; TOKEN_BYTES];
        let identity = IdentityFile {
            store: "gs://cluster/zero/".to_owned(),
            cluster_id: 0x0102_0304_0506_0708,
            token,
            host: Some(HostId::new(7)),
        };
        let encoded = encode_identity(&identity);
        assert_eq!(
            decode_identity(&encoded).expect("identity").host,
            Some(HostId::new(7))
        );
        assert!(decode_identity(&encoded[..encoded.len() - 1]).is_err());

        let payload = open_frame(NODE_IDENTITY_MAGIC, &encoded).expect("identity frame");
        let mut unknown = payload.to_vec();
        unknown.extend_from_slice(&[0x30, 1]);
        assert!(decode_identity(&seal_frame(NODE_IDENTITY_MAGIC, &unknown)).is_err());
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
        let host = identity.host;
        drop(identity);
        Arc::clone(&store)
            .delete(node_claim_key(host))
            .await
            .expect("delete claim fixture");

        let error = bootstrap(Arc::clone(&store), data_dir.path(), "gs://cluster/zero/")
            .await
            .expect_err("missing claim");
        assert!(matches!(error, BootstrapError::IdentityClaimMissing(found) if found == host));

        Arc::clone(&store)
            .delete(cluster_metadata_key())
            .await
            .expect("delete metadata fixture");
        let error = bootstrap(store, data_dir.path(), "gs://cluster/zero/")
            .await
            .expect_err("missing cluster metadata");
        assert!(matches!(error, BootstrapError::ClusterMetadataMissing));
    }

    #[test]
    fn cluster_metadata_is_byte_pinned_and_rejects_damage() {
        let encoded = encode_cluster_metadata(0x0102_0304_0506_0708);
        assert_eq!(
            encoded,
            [
                b'B', b'C', b'L', b'U', 0x0c, 0x00, 0x00, 0x00, 0x3a, 0x45, 0x9d, 0x5c, 0x08, 0x01,
                0x10, 0x88, 0x8e, 0x98, 0xa8, 0xc0, 0xe0, 0x80, 0x81, 0x01,
            ]
        );
        assert_eq!(
            decode_cluster_metadata(&encoded).expect("metadata"),
            0x0102_0304_0506_0708
        );
        let mut damaged = encoded.clone();
        damaged[0] ^= 1;
        assert!(decode_cluster_metadata(&damaged).is_err());
        for length in 0..encoded.len() {
            assert!(
                decode_cluster_metadata(&encoded[..length]).is_err(),
                "accepted metadata prefix of length {length}"
            );
        }
        for trailer in [[0_u8], [0xff_u8]] {
            let mut extended = encoded.clone();
            extended.extend_from_slice(&trailer);
            assert!(
                decode_cluster_metadata(&extended).is_err(),
                "accepted metadata with a trailing byte"
            );
        }
    }

    #[tokio::test]
    async fn bootstrap_distinguishes_missing_and_corrupt_cluster_metadata() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let concrete = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "metadata-diagnostics/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let data_dir = tempdir().expect("data dir");
        let (_, identity) = bootstrap(
            Arc::clone(&store),
            data_dir.path(),
            "gs://cluster/metadata-diagnostics/",
        )
        .await
        .expect("initial bootstrap");
        drop(identity);

        Arc::clone(&store)
            .delete(cluster_metadata_key())
            .await
            .expect("remove metadata fixture");
        assert!(matches!(
            bootstrap(
                Arc::clone(&store),
                data_dir.path(),
                "gs://cluster/metadata-diagnostics/"
            )
            .await,
            Err(BootstrapError::ClusterMetadataMissing)
        ));

        concrete
            .put(cluster_metadata_key(), b"corrupt metadata".to_vec())
            .await
            .expect("corrupt metadata fixture");
        assert!(matches!(
            bootstrap(store, data_dir.path(), "gs://cluster/metadata-diagnostics/").await,
            Err(BootstrapError::InvalidClusterMetadata)
        ));
    }

    /// Regression PROD-006: existing durable blobs without a node identity must be a
    /// fail-closed recovery condition, not a request for a fresh identity.
    #[tokio::test]
    async fn missing_identity_with_existing_blobs_fails_closed() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "zero/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let data_dir = tempdir().expect("data dir");
        let blob_dir = data_dir.path().join("blobs/v/0000000000000001/j");
        tokio::fs::create_dir_all(&blob_dir)
            .await
            .expect("blob directory");
        tokio::fs::write(
            blob_dir.join("0000000000000001-0000000000000001.rec"),
            b"durable",
        )
        .await
        .expect("durable local state");

        assert!(
            bootstrap(store, data_dir.path(), "gs://cluster/zero/")
                .await
                .is_err(),
            "startup minted a new identity over existing durable state"
        );
    }

    /// Regression PROD-017: identity material must never be group- or world-readable,
    /// including after permission drift between daemon restarts.
    #[cfg(unix)]
    #[tokio::test]
    async fn node_identity_permissions_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_fake, endpoint) = FakeGcs::start().await;
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "zero/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let data_dir = tempdir().expect("data dir");
        bootstrap(Arc::clone(&store), data_dir.path(), "gs://cluster/zero/")
            .await
            .expect("bootstrap");

        let mode = std::fs::metadata(data_dir.path().join("node.identity"))
            .expect("identity metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "node identity mode was {mode:o}");
        assert_eq!(
            std::fs::metadata(data_dir.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(data_dir.path().join("node.lock"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        std::fs::set_permissions(
            data_dir.path().join("node.identity"),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("make identity unsafe");
        assert!(
            matches!(
                bootstrap(store, data_dir.path(), "gs://cluster/zero/").await,
                Err(BootstrapError::Io(error))
                    if error.kind() == std::io::ErrorKind::InvalidData
            ),
            "restart accepted a group/world-readable node identity"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_rejects_foreign_owned_state_in_existing_and_fresh_modes() {
        let current = rustix::process::geteuid().as_raw();
        let foreign = current
            .checked_add(1)
            .unwrap_or_else(|| current.saturating_sub(1));
        let _owner = crate::world::override_effective_uid(foreign);

        let existing = tempdir().expect("existing data dir");
        let existing_store = Arc::new(CommitThenFaultStore::default());
        let abstract_existing: Arc<dyn ObjectStore> = existing_store.clone();
        assert!(
            matches!(
                bootstrap(
                    abstract_existing,
                    existing.path(),
                    "gs://cluster/foreign-existing/",
                )
                .await,
                Err(BootstrapError::Io(error))
                    if error.kind() == std::io::ErrorKind::PermissionDenied
            ),
            "existing foreign-owned state directory reached identity bootstrap"
        );
        assert!(!existing.path().join("node.identity").exists());
        assert!(
            existing_store
                .objects
                .lock()
                .expect("objects mutex")
                .is_empty(),
            "foreign existing state mutated remote cluster metadata"
        );

        let fresh_parent = tempdir().expect("fresh parent");
        let fresh = fresh_parent.path().join("state");
        let fresh_store = Arc::new(CommitThenFaultStore::default());
        let abstract_fresh: Arc<dyn ObjectStore> = fresh_store.clone();
        assert!(
            matches!(
                bootstrap(
                    abstract_fresh,
                    &fresh,
                    "gs://cluster/foreign-fresh/",
                )
                .await,
                Err(BootstrapError::Io(error))
                    if error.kind() == std::io::ErrorKind::PermissionDenied
            ),
            "fresh state path bypassed descriptor ownership validation"
        );
        assert!(
            fresh.is_dir(),
            "fresh-mode directory creation was not exercised"
        );
        assert!(!fresh.join("node.identity").exists());
        assert!(
            fresh_store
                .objects
                .lock()
                .expect("objects mutex")
                .is_empty(),
            "foreign fresh state mutated remote cluster metadata"
        );
    }

    #[tokio::test]
    async fn interrupted_or_symlinked_identity_publication_fails_closed() {
        use std::os::unix::fs::symlink;

        let (_fake, endpoint) = FakeGcs::start().await;
        let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "identity-crash/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let data_dir = tempdir().expect("data dir");
        std::fs::write(
            data_dir.path().join("node.identity.deadbeef.tmp"),
            b"partial",
        )
        .unwrap();
        assert!(matches!(
            bootstrap(
                Arc::clone(&store),
                data_dir.path(),
                "gs://cluster/identity-crash/"
            )
            .await,
            Err(BootstrapError::InterruptedIdentity)
        ));

        std::fs::remove_file(data_dir.path().join("node.identity.deadbeef.tmp")).unwrap();
        let outside = data_dir.path().join("outside");
        std::fs::write(&outside, b"do not overwrite").unwrap();
        symlink(&outside, data_dir.path().join("node.identity")).unwrap();
        assert!(
            bootstrap(store, data_dir.path(), "gs://cluster/identity-crash/")
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&outside).unwrap(), b"do not overwrite");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn state_root_creation_rejects_an_intermediate_symlink_before_store_access() {
        use std::os::unix::fs::symlink;

        let anchor = tempdir().expect("anchor");
        let outside = tempdir().expect("outside");
        symlink(outside.path(), anchor.path().join("redirect")).expect("intermediate symlink");
        let data_dir = anchor.path().join("redirect/state");
        let concrete = Arc::new(CommitThenFaultStore::default());
        let store: Arc<dyn ObjectStore> = concrete.clone();

        assert!(
            bootstrap(store, &data_dir, "gs://cluster/intermediate-symlink/")
                .await
                .is_err()
        );
        assert!(!outside.path().join("state").exists());
        assert!(
            concrete.objects.lock().expect("objects mutex").is_empty(),
            "unsafe local path reached remote cluster bootstrap"
        );
    }

    /// Regression PROD-006: every identity-publication durability boundary must
    /// leave either explicit crash residue or a restartable published identity.
    #[tokio::test]
    async fn identity_publication_crash_points_restart_safely() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let cluster_id = 0x0102_0304_0506_0708;
        for (index, stage) in [
            IdentityPublicationStage::TemporaryCreated,
            IdentityPublicationStage::ContentsWritten,
            IdentityPublicationStage::FileSynced,
            IdentityPublicationStage::Renamed,
            IdentityPublicationStage::DirectorySynced,
        ]
        .into_iter()
        .enumerate()
        {
            let prefix = format!("identity-stage-{index}/");
            let binding = format!("gs://cluster/{prefix}");
            let concrete = Arc::new(GcsStore::new(GcsConfig {
                bucket: "cluster".to_owned(),
                prefix,
                endpoint: endpoint.clone(),
                metadata_endpoint: endpoint.clone(),
            }));
            let store: Arc<dyn ObjectStore> = concrete.clone();
            Arc::clone(&store)
                .put(cluster_metadata_key(), encode_cluster_metadata(cluster_id))
                .await
                .expect("seed cluster metadata");
            let data_dir = tempdir().expect("data dir");
            let identity_path = data_dir.path().join("node.identity");
            let mut token = [0x40; TOKEN_BYTES];
            token[0] = u8::try_from(index).expect("stage index fits u8");
            let status = std::process::Command::new(
                std::env::current_exe().expect("current runtime test executable"),
            )
            .arg("--exact")
            .arg("cluster::tests::identity_publication_crash_child")
            .arg("--nocapture")
            .env("BLOCKD_IDENTITY_CRASH_CHILD", "1")
            .env("BLOCKD_IDENTITY_CRASH_PATH", &identity_path)
            .env("BLOCKD_IDENTITY_CRASH_STORE", &binding)
            .env("BLOCKD_IDENTITY_CRASH_CLUSTER", cluster_id.to_string())
            .env("BLOCKD_IDENTITY_CRASH_TOKEN_BYTE", index.to_string())
            .env("BLOCKD_IDENTITY_CRASH_STAGE", index.to_string())
            .status()
            .expect("run crash helper subprocess");
            assert!(!status.success(), "stage {stage:?} did not crash");
            #[cfg(target_os = "linux")]
            assert_eq!(status.code(), Some(86));

            if matches!(
                stage,
                IdentityPublicationStage::TemporaryCreated
                    | IdentityPublicationStage::ContentsWritten
                    | IdentityPublicationStage::FileSynced
            ) {
                assert!(!identity_path.exists());
                assert!(matches!(
                    bootstrap(store, data_dir.path(), &binding).await,
                    Err(BootstrapError::InterruptedIdentity)
                ));
            } else {
                assert!(identity_path.exists());
                let (found_cluster, restarted) = bootstrap(store, data_dir.path(), &binding)
                    .await
                    .expect("published identity restarts");
                assert_eq!(found_cluster, cluster_id);
                assert_eq!(restarted.token, token);
            }
        }
    }

    #[test]
    fn identity_publication_crash_child() {
        if std::env::var_os("BLOCKD_IDENTITY_CRASH_CHILD").is_none() {
            return;
        }
        let path = std::path::PathBuf::from(
            std::env::var_os("BLOCKD_IDENTITY_CRASH_PATH").expect("identity crash path"),
        );
        let store = std::env::var("BLOCKD_IDENTITY_CRASH_STORE").expect("identity crash store");
        let cluster_id = std::env::var("BLOCKD_IDENTITY_CRASH_CLUSTER")
            .expect("identity crash cluster")
            .parse()
            .expect("numeric cluster identity");
        let token_byte: u8 = std::env::var("BLOCKD_IDENTITY_CRASH_TOKEN_BYTE")
            .expect("identity crash token")
            .parse()
            .expect("numeric token byte");
        let stage: usize = std::env::var("BLOCKD_IDENTITY_CRASH_STAGE")
            .expect("identity crash stage")
            .parse()
            .expect("numeric publication stage");
        let target = [
            IdentityPublicationStage::TemporaryCreated,
            IdentityPublicationStage::ContentsWritten,
            IdentityPublicationStage::FileSynced,
            IdentityPublicationStage::Renamed,
            IdentityPublicationStage::DirectorySynced,
        ]
        .get(stage)
        .copied()
        .expect("known publication stage");
        let mut token = [0x40; TOKEN_BYTES];
        token[0] = token_byte;
        let identity = IdentityFile {
            store,
            cluster_id,
            token,
            host: None,
        };
        let result = write_identity_sync_with_stage_hook(&path, &identity, |completed| {
            if completed == target {
                #[cfg(target_os = "linux")]
                rustix::runtime::exit_group(86);
                #[cfg(not(target_os = "linux"))]
                std::process::abort();
            }
            Ok(())
        });
        panic!("identity publication completed without crash: {result:?}");
    }

    #[tokio::test]
    async fn existing_identity_symlinks_and_hardlinks_are_rejected() {
        use std::os::unix::fs::symlink;

        let data_dir = tempdir().expect("data dir");
        let identity = data_dir.path().join("node.identity");
        let outside = data_dir.path().join("outside.identity");
        std::fs::write(&outside, b"untrusted").expect("outside fixture");

        symlink(&outside, &identity).expect("identity symlink");
        let symlink_error = read_identity(&identity)
            .await
            .expect_err("identity symlink must be rejected");
        assert_ne!(symlink_error.kind(), std::io::ErrorKind::NotFound);

        std::fs::remove_file(&identity).expect("remove symlink");
        std::fs::hard_link(&outside, &identity).expect("identity hardlink");
        let hardlink_error = read_identity(&identity)
            .await
            .expect_err("identity hardlink must be rejected");
        assert_eq!(hardlink_error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read(&outside).unwrap(), b"untrusted");
    }

    /// Regression PROD-007: a conflict carrying the generation written by an
    /// unknown-outcome CAS must be reconciled by reading the stored owner.
    #[tokio::test]
    async fn committed_claim_with_lost_response_is_reused_on_retry() {
        let concrete = Arc::new(CommitThenFaultStore::default());
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let data_dir = tempdir().expect("data dir");
        let (_, identity) = bootstrap(store, data_dir.path(), "gs://cluster/zero/")
            .await
            .expect("unknown outcome is reconciled");
        let claims = concrete
            .objects
            .lock()
            .expect("objects mutex")
            .keys()
            .filter(|key| key.starts_with(&node_claim_prefix()))
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(claims, [node_claim_key(identity.host)]);
    }

    #[tokio::test]
    async fn committed_claim_with_unavailable_response_is_reused_on_retry() {
        let concrete = Arc::new(CommitThenFaultStore {
            claim_result_unknown: true,
            ..CommitThenFaultStore::default()
        });
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let data_dir = tempdir().expect("data dir");
        let binding = "gs://cluster/unavailable-claim/";

        assert!(matches!(
            bootstrap(Arc::clone(&store), data_dir.path(), binding).await,
            Err(BootstrapError::Store(StoreFault::Unavailable))
        ));
        let (_, identity) = bootstrap(store, data_dir.path(), binding)
            .await
            .expect("retry reuses committed claim");
        let claims = concrete
            .objects
            .lock()
            .expect("objects mutex")
            .keys()
            .filter(|key| key.starts_with(&node_claim_prefix()))
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(claims, [node_claim_key(identity.host)]);
    }

    #[tokio::test]
    async fn different_owner_collision_advances_exactly_one_host_id() {
        let (_fake, endpoint) = FakeGcs::start().await;
        let concrete = Arc::new(GcsStore::new(GcsConfig {
            bucket: "cluster".to_owned(),
            prefix: "claim-collision/".to_owned(),
            endpoint: endpoint.clone(),
            metadata_endpoint: endpoint,
        }));
        let store: Arc<dyn ObjectStore> = concrete.clone();
        let data_dir = tempdir().expect("data dir");
        let binding = "gs://cluster/claim-collision/";
        let cluster_id = 0x1112_1314_1516_1718;
        let mut token = [0x33; TOKEN_BYTES];
        token[..4].copy_from_slice(&0x1234_u32.to_le_bytes());
        write_identity_sync(
            &data_dir.path().join("node.identity"),
            &IdentityFile {
                store: binding.to_owned(),
                cluster_id,
                token,
                host: None,
            },
        )
        .expect("seed local identity");
        Arc::clone(&store)
            .put(cluster_metadata_key(), encode_cluster_metadata(cluster_id))
            .await
            .expect("seed cluster metadata");
        let collided = HostId::new(0x1234);
        Arc::clone(&store)
            .put(node_claim_key(collided), vec![0x99; TOKEN_BYTES])
            .await
            .expect("seed competing owner");

        let (_, identity) = bootstrap(Arc::clone(&store), data_dir.path(), binding)
            .await
            .expect("advance past competing owner");
        assert_eq!(identity.host, HostId::new(collided.get().wrapping_add(1)));
        let mut claims = Arc::clone(&store)
            .list_prefix(node_claim_prefix())
            .await
            .expect("list claims");
        claims.sort();
        assert_eq!(
            claims,
            [node_claim_key(collided), node_claim_key(identity.host)]
        );
    }

    /// Regression PROD-022: metadata corruption in the payload must be detected even
    /// when the magic and record length remain valid.
    #[test]
    fn cluster_metadata_rejects_every_payload_bit_flip() {
        let encoded = encode_cluster_metadata(0x0102_0304_0506_0708);
        for byte in 0..encoded.len() {
            for bit in 0..8 {
                let mut damaged = encoded.clone();
                damaged[byte] ^= 1 << bit;
                assert!(
                    decode_cluster_metadata(&damaged).is_err(),
                    "accepted payload corruption at byte {byte}, bit {bit}"
                );
            }
        }
    }
}
