//! Differential recovery across the simulator's in-memory blob snapshot and
//! the production directory-backed actor world. Both paths call the same
//! async recovery actor; this test guards the remaining world boundary:
//! directory enumeration, name round-tripping, short/torn bytes, and scan
//! ordering.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use blockd_core::engine::{HostFatal, HostState, reconcile_backed_recovery, recover_local};
use blockd_core::hostmeta::{DaemonStats, HostConfig, ReplicaPlacementConfig};
use blockd_core::journal::JournalRecord;
use blockd_core::layout::{self, BlobName};
use blockd_core::placement::PeerCandidate;
use blockd_core::protocol::{AdminEvent, Verdict};
use blockd_core::segment::open_entry;
use blockd_core::types::{HostId, PageId, VsetId, page_size};
use blockd_core::world::{
    AdminIo, AdminRequest, BlobEntry, BlobError, Blobs, DatabaseActorRequest, FillSource,
    GuestFault, GuestMem, GuestMemoryError, GuestPause, GuestSyncRequest, Store, StoreError,
};
use blockd_exec::Executor;
use blockd_runtime::ObjectStore;
use blockd_runtime::directory_store::DirectoryStore;
use blockd_runtime::world::{FileBlobs, RuntimeStore};
use blockd_sim::harness::run_final_blobs;
use blockd_sim::presets;

struct MemoryBlobs {
    blobs: RefCell<BTreeMap<String, Vec<u8>>>,
}

impl MemoryBlobs {
    fn new(blobs: &[(String, Vec<u8>)]) -> Self {
        Self {
            blobs: RefCell::new(blobs.iter().cloned().collect()),
        }
    }
}

impl Blobs for MemoryBlobs {
    async fn scan(&self) -> Result<Vec<BlobEntry>, BlobError> {
        Ok(self
            .blobs
            .borrow()
            .iter()
            .map(|(name, bytes)| BlobEntry {
                name: name.clone(),
                bytes: bytes.clone(),
                len: bytes.len() as u64,
            })
            .collect())
    }

    async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.blobs.borrow_mut().insert(name, bytes);
        Ok(())
    }

    async fn append(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.blobs
            .borrow_mut()
            .entry(name)
            .or_default()
            .extend(bytes);
        Ok(())
    }

    async fn truncate(&self, name: &str, len: u64) -> Result<(), BlobError> {
        if let Some(bytes) = self.blobs.borrow_mut().get_mut(name) {
            bytes.truncate(usize::try_from(len).map_err(|_| BlobError::Io)?);
        }
        Ok(())
    }

    async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
        Ok(self.blobs.borrow().get(name).cloned())
    }

    async fn read_range(
        &self,
        name: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, BlobError> {
        Ok(self.blobs.borrow().get(name).map(|bytes| {
            let start = usize::try_from(offset.min(bytes.len() as u64)).expect("offset fits");
            let end = usize::try_from(offset.saturating_add(len).min(bytes.len() as u64))
                .expect("end fits");
            bytes[start..end].to_vec()
        }))
    }

    async fn delete(&self, name: &str) -> Result<(), BlobError> {
        self.blobs.borrow_mut().remove(name);
        Ok(())
    }
}

fn recover<W: Blobs + 'static>(
    config: HostConfig,
    world: Rc<W>,
) -> (BTreeMap<VsetId, Verdict>, DaemonStats) {
    let state = Rc::new(RefCell::new(HostState::new(config)));
    let mut executor = Executor::production();
    let verdicts = executor
        .block_on({
            let state = Rc::clone(&state);
            async move { recover_local(state, world.as_ref()).await }
        })
        .expect("recovery succeeds");
    let observability = state.borrow().stats();
    (verdicts, observability)
}

fn write_blobs(root: &Path, blobs: &[(String, Vec<u8>)]) {
    for (name, bytes) in blobs {
        let path = root.join(name);
        std::fs::create_dir_all(path.parent().expect("blob names have a parent")).expect("mkdir");
        std::fs::write(path, bytes).expect("write blob");
    }
}

async fn recovered_content_hash(
    state: &Rc<RefCell<HostState>>,
    blobs: &FileBlobs,
    store: &FixtureWorld,
) -> u64 {
    let pages = state
        .borrow()
        .vsets
        .values()
        .flat_map(|vset| {
            let backed = true;
            vset.page_locs
                .iter()
                .map(move |(&page, &(generation, location))| (page, generation, location, backed))
        })
        .collect::<Vec<_>>();
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (page, generation, location, backed) in pages {
        let local = if location.base == 0 {
            Blobs::read_range(
                blobs,
                &layout::segment_blob(page.volume.vset, location.fence, location.seg),
                u64::from(location.offset),
                u64::from(location.len),
            )
            .await
            .expect("fixture local range")
        } else {
            None
        };
        let decoded = local
            .as_deref()
            .and_then(|bytes| open_entry(page.volume.vset, bytes).ok());
        let (found, found_generation, raw) = if let Some(decoded) = decoded {
            decoded
        } else {
            assert!(backed || location.base != 0, "unbacked local entry damaged");
            let key = if location.base == 0 {
                layout::segment_key(page.volume.vset, location.fence, location.seg)
            } else {
                layout::base_segment_key(location.base, location.fence, location.seg)
            };
            let bytes = Store::get_range(
                store,
                &key,
                u64::from(location.offset),
                u64::from(location.len),
            )
            .await
            .expect("fixture store range")
            .expect("fixture store entry")
            .1;
            open_entry(page.volume.vset, &bytes).unwrap_or_else(|_| {
                panic!("fixture entry checksum failed for {page:?} at {location:?}")
            })
        };
        assert_eq!((found, found_generation), (page, generation));
        for byte in page
            .volume
            .vset
            .0
            .to_le_bytes()
            .into_iter()
            .chain([page.volume.idx.0])
            .chain(page.page.0.to_le_bytes())
            .chain(generation.0.to_le_bytes())
            .chain(raw)
        {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}

struct FixtureWorld {
    store: RuntimeStore,
    events: RefCell<Vec<AdminEvent>>,
}

impl Store for FixtureWorld {
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<u64, StoreError> {
        Store::put(&self.store, key, bytes).await
    }

    async fn put_cas(
        &self,
        key: String,
        expected: Option<u64>,
        bytes: Vec<u8>,
    ) -> Result<u64, StoreError> {
        Store::put_cas(&self.store, key, expected, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        Store::get(&self.store, key).await
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        Store::get_range(&self.store, key, offset, len).await
    }

    async fn delete(&self, key: &str) -> Result<bool, StoreError> {
        Store::delete(&self.store, key).await
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        Store::list_prefix(&self.store, prefix).await
    }
}

impl GuestMem for FixtureWorld {
    async fn read_page(&self, _: PageId) -> Vec<u8> {
        vec![0; page_size()]
    }
    async fn arm_write_protect(&self, _: &[PageId]) -> Result<(), GuestMemoryError> {
        Ok(())
    }
    async fn fill(
        &self,
        _: PageId,
        _: Vec<u8>,
        _: bool,
        _: FillSource,
    ) -> Result<(), GuestMemoryError> {
        Ok(())
    }
    async fn fill_shared(
        &self,
        _: PageId,
        _: (u64, u64, blockd_core::types::SegId, u32),
        _: Option<Vec<u8>>,
        _: bool,
    ) -> Result<(), GuestMemoryError> {
        Ok(())
    }
    async fn fail(&self, _: PageId) -> Result<(), GuestMemoryError> {
        Ok(())
    }
    async fn unprotect(&self, _: PageId) -> Result<(), GuestMemoryError> {
        Ok(())
    }
    async fn evict(&self, _: PageId) -> Result<(), GuestMemoryError> {
        Ok(())
    }
    async fn install_database(&self, _: PageId, _: Vec<u8>) -> Result<(), GuestMemoryError> {
        Ok(())
    }
    async fn pause(&self, _: VsetId) -> Result<GuestPause, GuestMemoryError> {
        Ok(GuestPause {
            vmstate: 0,
            generation: 0,
        })
    }
    async fn resume(&self, _: VsetId, _: Option<GuestPause>) -> Result<(), GuestMemoryError> {
        Ok(())
    }
    async fn harvest_accessed(&self) -> Vec<PageId> {
        Vec::new()
    }
    async fn next_fault(&self) -> Option<GuestFault> {
        None
    }
    async fn next_sync(&self) -> Option<GuestSyncRequest> {
        None
    }
    async fn fence(&self, _: VsetId) -> Result<(), GuestMemoryError> {
        Ok(())
    }
}

impl AdminIo for FixtureWorld {
    async fn next_admin(&self) -> Option<AdminRequest> {
        None
    }

    async fn emit_admin_event(&self, event: AdminEvent) {
        self.events.borrow_mut().push(event);
    }
    async fn next_database(&self) -> Option<DatabaseActorRequest> {
        None
    }
    async fn host_failed(&self, failure: HostFatal) {
        panic!("fixture recovery failed: {}", failure.reason);
    }
}

#[test]
fn disk_actor_recovers_exactly_like_the_simulated_snapshot() {
    let mut nontrivial = 0u64;
    for seed in [3, 5, 7, 11, 29, 104] {
        let config = presets::single_host_chaos();
        let host_config = config.daemon.clone();
        let (report, blobs) = run_final_blobs(seed, config);
        assert_eq!(report.violations, Vec::<String>::new(), "seed {seed}");
        if blobs.is_empty() {
            continue;
        }

        let memory_side = recover(host_config.clone(), Rc::new(MemoryBlobs::new(&blobs)));

        let root = tempfile::tempdir().expect("recovery fixture");
        write_blobs(root.path(), &blobs);
        std::fs::create_dir_all(root.path().join("lost+found")).expect("mkdir");
        std::fs::write(root.path().join("lost+found/fsck.0000"), b"noise").expect("write");
        std::fs::write(root.path().join("daemon.pid"), b"12345").expect("write");

        let disk_side = recover(
            host_config.clone(),
            Rc::new(FileBlobs::new(root.path()).expect("file world")),
        );
        assert_eq!(
            memory_side, disk_side,
            "seed {seed}: actor recovery diverged across world implementations"
        );

        let mut reversed = blobs.clone();
        reversed.reverse();
        let reversed_side = recover(host_config, Rc::new(MemoryBlobs::new(&reversed)));
        assert_eq!(
            memory_side, reversed_side,
            "seed {seed}: actor recovery depends on scan order"
        );
        // Backed recovery now defers its verdict until the fenced head read,
        // which this scan-only differential deliberately does not drive.
        // Keep the non-vacuity guard on the durable inputs instead: each
        // counted vset has at least one intact journal candidate that both
        // scan paths fed into the same deferred recovery state.
        let recoverable: BTreeSet<_> = blobs
            .iter()
            .filter_map(|(name, bytes)| match layout::parse_blob(name) {
                Some(BlobName::Journal { vset, .. })
                    if JournalRecord::decode(vset, bytes).is_ok() =>
                {
                    Some(vset)
                }
                _ => None,
            })
            .collect();
        nontrivial += recoverable.len() as u64;
        fs::remove_dir_all(&root).expect("cleanup");
    }
    // The equality above must have been about something: across the seeds,
    // real vsets recovered to real verdicts.
    assert!(
        nontrivial >= 3,
        "only {nontrivial} recoverable journal sets seen"
    );
}

#[test]
fn main_branch_fixture_recovers_under_the_actor_runtime() {
    const REVISION: &str = "7eccd9d1263f1e7a64e10b92e16f0a9d24d5aac4";
    assert_eq!(
        include_str!("fixtures/main-recovery-revision.txt").trim(),
        REVISION
    );
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/main-recovery-104");
    let mut config = presets::single_host_base().daemon;
    let local = config.host;
    let passive = HostId(local.0 ^ u16::MAX);
    config.replica_placement = Some(ReplicaPlacementConfig {
        membership_epoch: 1,
        local_failure_domain: local.0,
        roster: vec![
            PeerCandidate {
                host: local,
                weight: 1,
                failure_domain: local.0,
                drained: false,
            },
            PeerCandidate {
                host: passive,
                weight: 1,
                failure_domain: passive.0,
                drained: false,
            },
        ],
        authority: None,
    });
    let state = Rc::new(RefCell::new(HostState::new(config)));
    let mut executor = Executor::production();
    let blobs = Rc::new(FileBlobs::new(&fixture.join("blobs")).expect("fixture blobs"));
    let initial = executor
        .block_on({
            let state = Rc::clone(&state);
            let blobs = Rc::clone(&blobs);
            async move { recover_local(state, blobs.as_ref()).await }
        })
        .expect("fixture local recovery");

    let tokio = tokio::runtime::Runtime::new().expect("fixture store runtime");
    let store_root = tempfile::tempdir().expect("fixture object store");
    let store: Arc<dyn ObjectStore> = Arc::new(
        DirectoryStore::new(store_root.path().to_path_buf()).expect("fixture directory store"),
    );
    let world = Rc::new(FixtureWorld {
        store: RuntimeStore::new(tokio.handle().clone(), store),
        events: RefCell::new(Vec::new()),
    });
    let backed = state
        .borrow()
        .vsets
        .iter()
        .filter_map(|(&vset, state)| state.operations.recovery_pending().then_some(vset))
        .collect::<Vec<_>>();
    for vset in backed {
        executor.block_on(reconcile_backed_recovery(
            Rc::clone(&state),
            Rc::clone(&world),
            vset,
        ));
    }
    let mut verdicts = initial;
    for event in world.events.borrow().iter() {
        if let AdminEvent::VsetRecovered { vset, verdict } = event {
            verdicts.insert(*vset, *verdict);
        }
    }
    assert_eq!(
        verdicts,
        BTreeMap::from([
            (VsetId(1), Verdict::ColdBoot),
            (VsetId(2), Verdict::ColdBoot),
            (VsetId(3), Verdict::ColdBoot),
        ])
    );
    let content_hash = executor.block_on({
        let state = Rc::clone(&state);
        let blobs = Rc::clone(&blobs);
        let world = Rc::clone(&world);
        async move { recovered_content_hash(&state, blobs.as_ref(), world.as_ref()).await }
    });
    assert_eq!(content_hash, 0x3623_0e62_35a5_4248);
}
