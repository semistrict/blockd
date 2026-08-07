//! VM orchestration: real Firecracker microVMs restored from store-held
//! snapshots (one fill server per snapshot prefix, forks share one copy),
//! each paired with a daemon-managed vset that carries its durable state
//! — checkpointed continuously, backed up to the store when `backed`, and
//! live-migrated over the peer transport.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::seam::Verdict;
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};
use blockd_runtime::fc::{FcVm, ShmemServer, rss_pss_of_pid, upload_mem_parts_async};
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore, PeerConfig, Runtime, RuntimeConfig};

use crate::config::DemodConfig;
use crate::observability::Metrics;

pub const MEM_MIB: u32 = 128;
pub const PART_BYTES: u64 = 8 * 1024 * 1024;
/// Pages the guest fills per work burst (matching the fc-guest arena).
pub const GUEST_FILL_PAGES: u32 = 512;
/// The paired vset: one memory volume + one 256-page disk volume.
pub const VSET_PAGES: u32 = 256;
/// Data words the mirror writes per burst (page 0 is the burst counter).
pub const MIRROR_PAGES: u32 = 63;

fn vset_config(backed: bool) -> VsetConfig {
    VsetConfig {
        disk_volumes: 1,
        pages_per_volume: VSET_PAGES,
        backed_up: backed,
    }
}

fn disk_page(vset: VsetId, page: u32) -> PageId {
    PageId {
        volume: VolumeId {
            vset,
            idx: VolumeIdx(1),
        },
        page: PageNo(page),
    }
}

/// The mirrored value of data page `p` at burst `b` of VM `id` —
/// self-describing, so any host can verify without carried state.
fn mirror_value(id: u64, burst: u64, page: u32) -> u64 {
    id * 1_000_000 + burst * 1_000 + u64::from(page)
}

pub struct Vm {
    pub state: String,
    pub backed: bool,
    /// The snapshot prefix this VM was restored from.
    pub prefix: String,
    fc: Option<Arc<Mutex<FcVm>>>,
}

struct FillServer {
    #[allow(dead_code)]
    server: ShmemServer,
    sock: PathBuf,
    shmem: PathBuf,
}

pub struct Demod {
    pub cfg: DemodConfig,
    pub rt: Runtime,
    pub store: Arc<GcsStore>,
    pub vms: Mutex<BTreeMap<u64, Vm>>,
    pub metrics: Arc<Metrics>,
    next_vm: AtomicU64,
    next_fork: AtomicU64,
    next_server: AtomicU64,
    servers: Mutex<BTreeMap<String, Arc<FillServer>>>,
}

impl Demod {
    pub fn firecracker_fault_latency(
        &self,
    ) -> Vec<(&'static str, blockd_runtime::HistogramSnapshot)> {
        self.servers
            .lock()
            .expect("lock")
            .values()
            .map(|fill| (fill.server.source(), fill.server.fault_latency()))
            .collect()
    }

    pub fn start(cfg: DemodConfig) -> Demod {
        std::fs::create_dir_all(&cfg.scratch).expect("scratch dir");
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: cfg.gcs_bucket.clone(),
            prefix: cfg.gcs_prefix.clone(),
            endpoint: cfg.gcs_endpoint.clone(),
            metadata_endpoint: cfg.gcs_metadata.clone(),
        }));
        let runtime_config = RuntimeConfig {
            daemon: DaemonConfig {
                host: cfg.host,
                cache_pages: cfg.cache_pages,
                writeback_interval: millis(cfg.writeback_interval_ms),
                backup_retry: millis(cfg.backup_retry_ms),
                disk_capacity: cfg.disk_capacity_bytes,
                disk_headroom: cfg.disk_headroom_bytes,
                wedge_ticks: cfg.wedge_ticks,
            },
            blob_dir: cfg.blob_dir.clone(),
            peer: Some(PeerConfig {
                listen: cfg.peer_listen,
                peers: cfg.peers.clone(),
            }),
        };
        let rt = Runtime::new(&runtime_config, store.clone());
        Demod {
            next_vm: AtomicU64::new(u64::from(cfg.host.0) * 1000 + 1),
            cfg,
            rt,
            store,
            vms: Mutex::new(BTreeMap::new()),
            metrics: Arc::new(Metrics::new()),
            next_fork: AtomicU64::new(1),
            next_server: AtomicU64::new(0),
            servers: Mutex::new(BTreeMap::new()),
        }
    }

    fn fc_bin(&self) -> PathBuf {
        self.cfg.fc_dir.join("firecracker")
    }

    /// Bake the base image: boot a template guest, give it a worked
    /// state, snapshot, and publish the snapshot to the store. Returns
    /// the guest's own checksum of the baked state.
    #[tracing::instrument(skip(self), name = "vm.bake_base")]
    pub fn bake_base(&self) -> String {
        let scratch = &self.cfg.scratch;
        let mut vm = FcVm::spawn(&self.fc_bin(), &scratch.join("bake.sock"));
        vm.boot(
            &self.cfg.fc_dir.join("vmlinux"),
            &self.cfg.fc_dir.join("initramfs.cpio"),
            MEM_MIB,
        );
        vm.wait_line("READY");
        vm.cmd("fill 7 4096", "FILLED ");
        let sum = vm.cmd("sum 4096", "SUM ");
        let vmstate = scratch.join("base.vmstate");
        let mem = scratch.join("base.mem");
        let _ = std::fs::remove_file(&vmstate);
        let _ = std::fs::remove_file(&mem);
        vm.pause();
        vm.snapshot(&vmstate, &mem);
        vm.kill();
        // A re-bake replaces the store objects; a cached fill server for
        // the old snapshot would serve stale memory against the new
        // vmstate. Drop it so the next boot re-fetches.
        self.servers.lock().expect("lock").remove("base");
        self.publish_snapshot("base", &vmstate, &mem);
        tokio::runtime::Handle::current()
            .block_on(GcsStore::put(
                self.store.as_ref(),
                "base/sum",
                sum.clone().into_bytes(),
            ))
            .expect("publish sum");
        sum
    }

    /// Upload a snapshot (vmstate + memory parts) under `prefix`.
    #[tracing::instrument(skip(self, vmstate, mem), fields(snapshot_prefix = prefix))]
    fn publish_snapshot(&self, prefix: &str, vmstate: &Path, mem: &Path) {
        // Control operations run in the API's bounded blocking lane because
        // Firecracker and durable files are synchronous. Their GCS futures are
        // still driven by Tokio; this worker only waits for completion.
        let parts = tokio::runtime::Handle::current().block_on(upload_mem_parts_async(
            self.store.clone() as Arc<dyn ObjectStore>,
            mem.to_owned(),
            format!("{prefix}/mem"),
            PART_BYTES,
        ));
        assert_eq!(parts, u64::from(MEM_MIB) * 1024 * 1024 / PART_BYTES);
        let bytes = std::fs::read(vmstate).expect("vmstate file");
        tokio::runtime::Handle::current()
            .block_on(GcsStore::put(
                self.store.as_ref(),
                &format!("{prefix}/vmstate"),
                bytes,
            ))
            .expect("publish vmstate");
    }

    /// The fill server for a snapshot prefix — one per host per prefix;
    /// every VM restored from that prefix shares its one memory copy.
    fn ensure_server(&self, prefix: &str) -> Arc<FillServer> {
        let mut servers = self.servers.lock().expect("lock");
        if let Some(server) = servers.get(prefix) {
            return server.clone();
        }
        let tag = format!(
            "{}-{}",
            self.cfg.host.0,
            self.next_server.fetch_add(1, Ordering::Relaxed)
        );
        let sock = self.cfg.scratch.join(format!("fill-{tag}.sock"));
        let shmem = self.cfg.shmem_dir.join(format!("blockd-{tag}.shmem"));
        let _ = std::fs::remove_file(&sock);
        let _ = std::fs::remove_file(&shmem);
        let listener = std::os::unix::net::UnixListener::bind(&sock).expect("fill sock");
        let server = ShmemServer::start_s3(
            listener,
            self.store.clone() as Arc<dyn ObjectStore>,
            format!("{prefix}/mem"),
            PART_BYTES,
            &shmem,
            u64::from(MEM_MIB) * 1024 * 1024,
            2, // readahead: a sequential cold reader streams
        );
        let entry = Arc::new(FillServer {
            server,
            sock,
            shmem,
        });
        servers.insert(prefix.to_owned(), entry.clone());
        entry
    }

    /// Restore one FC microVM from a store-held snapshot prefix.
    #[tracing::instrument(skip(self), fields(vm_id = id, snapshot_prefix = prefix))]
    fn boot_from(&self, id: u64, prefix: &str) -> FcVm {
        let fill = self.ensure_server(prefix);
        let vmstate = self.cfg.scratch.join(format!("vm{id}.vmstate"));
        let (_, bytes) = tokio::runtime::Handle::current()
            .block_on(GcsStore::get(
                self.store.as_ref(),
                &format!("{prefix}/vmstate"),
            ))
            .expect("store up")
            .expect("vmstate published");
        std::fs::write(&vmstate, bytes).expect("write vmstate");
        let vm = FcVm::spawn(
            &self.fc_bin(),
            &self.cfg.scratch.join(format!("vm{id}.sock")),
        );
        vm.load_snapshot_shmem(&vmstate, &fill.sock, &fill.shmem);
        vm
    }

    /// Start a fresh VM from the base snapshot with its paired vset.
    #[tracing::instrument(skip(self), fields(vm_id = tracing::field::Empty))]
    pub fn start_vm(&self, backed: bool) -> u64 {
        let id = self.next_vm.fetch_add(1, Ordering::SeqCst);
        tracing::Span::current().record("vm_id", id);
        let mut fc = self.boot_from(id, "base");
        fc.cmd("ping", "PONG");
        self.rt.create_vset(VsetId(id), vset_config(backed));
        self.vms.lock().expect("lock").insert(
            id,
            Vm {
                state: "running".to_owned(),
                backed,
                prefix: "base".to_owned(),
                fc: Some(Arc::new(Mutex::new(fc))),
            },
        );
        tracing::info!(vm_id = id, backed, "VM started");
        id
    }

    fn fc_of(&self, id: u64) -> Arc<Mutex<FcVm>> {
        self.vms.lock().expect("lock")[&id]
            .fc
            .clone()
            .expect("vm has no running microVM")
    }

    /// One work burst: the guest computes over fresh memory, and the
    /// burst is mirrored into the vset — counter plus data pages — with a
    /// sync making it a durable consistency point.
    #[tracing::instrument(skip(self), fields(vm_id = id, burst_count = bursts))]
    pub fn work(&self, id: u64, bursts: u64) -> (u64, String) {
        let vset = VsetId(id);
        let fc = self.fc_of(id);
        let mut sum = String::new();
        let mut burst = 0;
        for _ in 0..bursts {
            let counter = self.rt.guest_read(vset, disk_page(vset, 0));
            burst = u64::from_le_bytes(counter[0..8].try_into().expect("sized")) + 1;
            let seed = id * 31 + burst;
            {
                let mut fc = fc.lock().expect("lock");
                fc.cmd(&format!("fill {seed} {GUEST_FILL_PAGES}"), "FILLED ");
                sum = fc.cmd(&format!("sum {GUEST_FILL_PAGES}"), "SUM ");
            }
            for page in 1..=MIRROR_PAGES {
                self.rt
                    .guest_write(vset, disk_page(vset, page), mirror_value(id, burst, page));
            }
            self.rt.guest_write(vset, disk_page(vset, 0), burst);
            assert!(self.rt.guest_sync(vset, VolumeIdx(1)), "sync failed");
        }
        (burst, sum)
    }

    /// Verify the vset against the self-describing model: page 0 names
    /// the burst, every data page must match it. Returns (burst,
    /// mismatches).
    #[tracing::instrument(skip(self), fields(vm_id = id))]
    pub fn verify(&self, id: u64) -> (u64, u32) {
        let vset = VsetId(id);
        let counter = self.rt.guest_read(vset, disk_page(vset, 0));
        let burst = u64::from_le_bytes(counter[0..8].try_into().expect("sized"));
        let mut mismatches = 0;
        for page in 1..=MIRROR_PAGES {
            let bytes = self.rt.guest_read(vset, disk_page(vset, page));
            let got = u64::from_le_bytes(bytes[0..8].try_into().expect("sized"));
            let want = if burst == 0 {
                0
            } else {
                mirror_value(id, burst, page)
            };
            if got != want {
                mismatches += 1;
            }
        }
        if mismatches > 0 {
            tracing::warn!(vm_id = id, burst, mismatches, "VM verification failed");
        }
        (burst, mismatches)
    }

    /// Fork: snapshot the VM's CURRENT state to the store, then start `n`
    /// microVMs off that one snapshot — they share one memory copy on
    /// this host (the fill server's file), diverging copy-on-write.
    /// Returns (fork ids, sum of Rss, sum of Pss, fill-server resident).
    #[tracing::instrument(skip(self), fields(vm_id = id, fork_count = n))]
    pub fn fork(&self, id: u64, n: u32) -> (Vec<u64>, usize, usize, usize) {
        let prefix = format!("vm{id}/f{}", self.next_fork.fetch_add(1, Ordering::SeqCst));
        let vmstate = self.cfg.scratch.join(format!("fork-{id}.vmstate"));
        let mem = self.cfg.scratch.join(format!("fork-{id}.mem"));
        let _ = std::fs::remove_file(&vmstate);
        let _ = std::fs::remove_file(&mem);
        {
            let fc = self.fc_of(id);
            let fc = fc.lock().expect("lock");
            fc.pause();
            fc.snapshot(&vmstate, &mem);
            fc.resume();
        }
        self.publish_snapshot(&prefix, &vmstate, &mem);

        let mut ids = Vec::new();
        let (mut rss_sum, mut pss_sum) = (0, 0);
        for _ in 0..n {
            let fork_id = self.next_vm.fetch_add(1, Ordering::SeqCst);
            let mut fc = self.boot_from(fork_id, &prefix);
            fc.cmd("ping", "PONG");
            let (rss, pss) = rss_pss_of_pid(fc.pid());
            rss_sum += rss;
            pss_sum += pss;
            self.rt.create_vset(VsetId(fork_id), vset_config(false));
            self.vms.lock().expect("lock").insert(
                fork_id,
                Vm {
                    state: "running".to_owned(),
                    backed: false,
                    prefix: prefix.clone(),
                    fc: Some(Arc::new(Mutex::new(fc))),
                },
            );
            ids.push(fork_id);
        }
        let resident = self
            .servers
            .lock()
            .expect("lock")
            .get(&prefix)
            .map_or(0, |s| s.server.resident_bytes());
        tracing::info!(
            vm_id = id,
            fork_count = n,
            rss_bytes = rss_sum,
            pss_bytes = pss_sum,
            base_resident_bytes = resident,
            "VM forks started"
        );
        (ids, rss_sum, pss_sum, resident)
    }

    /// Destination side of a migration: accept the vset (guest memory
    /// pre-created), then — once the handoff lands — restore the microVM
    /// from the snapshot the source published.
    #[tracing::instrument(skip(self, ready), fields(vm_id = id))]
    pub fn expect(self: &Arc<Demod>, id: u64, ready: impl FnOnce()) {
        self.rt.expect_migration(VsetId(id), vset_config(false));
        self.vms.lock().expect("lock").insert(
            id,
            Vm {
                state: "expecting".to_owned(),
                backed: false,
                prefix: format!("vm{id}/mig"),
                fc: None,
            },
        );
        ready();
        let span = tracing::info_span!("migration.receive", vm_id = id);
        span.in_scope(|| {
            let verdict = self.rt.wait_migrated_in(VsetId(id));
            assert!(
                matches!(verdict, Verdict::Resume { .. }),
                "migration verdict {verdict:?}"
            );
            let mut fc = self.boot_from(id, &format!("vm{id}/mig"));
            fc.cmd("ping", "PONG");
            let mut vms = self.vms.lock().expect("lock");
            let vm = vms.get_mut(&id).expect("expected vm");
            vm.fc = Some(Arc::new(Mutex::new(fc)));
            "running".clone_into(&mut vm.state);
            tracing::info!(vm_id = id, "inbound migration resumed");
        });
    }

    /// Source side: pause + snapshot the microVM, publish it, kill it,
    /// and live-migrate the vset over TCP. Returns milliseconds spent
    /// (snapshot+publish, migrate handoff).
    #[tracing::instrument(skip(self), fields(vm_id = id, destination_host = to))]
    pub fn migrate(&self, id: u64, to: u16) -> (u128, u128) {
        let vmstate = self.cfg.scratch.join(format!("mig-{id}.vmstate"));
        let mem = self.cfg.scratch.join(format!("mig-{id}.mem"));
        let _ = std::fs::remove_file(&vmstate);
        let _ = std::fs::remove_file(&mem);
        let snap_started = Instant::now();
        {
            let fc = self.fc_of(id);
            let fc = fc.lock().expect("lock");
            fc.pause();
            fc.snapshot(&vmstate, &mem);
        }
        self.publish_snapshot(&format!("vm{id}/mig"), &vmstate, &mem);
        let snap_ms = snap_started.elapsed().as_millis();

        let handoff_started = Instant::now();
        self.rt.migrate_out(VsetId(id), HostId(to));
        let handoff_ms = handoff_started.elapsed().as_millis();
        let mut vms = self.vms.lock().expect("lock");
        let vm = vms.get_mut(&id).expect("vm");
        if let Some(fc) = vm.fc.take()
            && let Ok(fc) = Arc::try_unwrap(fc)
        {
            fc.into_inner().expect("lock").kill();
        }
        "migrated-out".clone_into(&mut vm.state);
        tracing::info!(
            vm_id = id,
            destination_host = to,
            snapshot_ms = snap_ms,
            handoff_ms,
            "outbound migration completed"
        );
        (snap_ms, handoff_ms)
    }

    /// After the owning host died: restore a backed vset from the store
    /// alone (R6.1) and give it a fresh microVM from the base image.
    #[tracing::instrument(skip(self), fields(vm_id = id))]
    pub fn restore(&self, id: u64) -> String {
        let verdict = self.rt.restore_vset(VsetId(id), vset_config(true));
        let mut fc = self.boot_from(id, "base");
        fc.cmd("ping", "PONG");
        self.vms.lock().expect("lock").insert(
            id,
            Vm {
                state: "restored".to_owned(),
                backed: true,
                prefix: "base".to_owned(),
                fc: Some(Arc::new(Mutex::new(fc))),
            },
        );
        tracing::info!(vm_id = id, ?verdict, "VM restored");
        format!("{verdict:?}")
    }
}
