//! VM orchestration: real Firecracker microVMs restored from store-held
//! snapshots (one fill server per snapshot prefix, forks share one copy),
//! each paired with a daemon-managed vset that carries its durable state
//! — checkpointed continuously, replicated to a passive peer, published to
//! the store, and
//! live-migrated over the peer transport.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use blockd_core::hostmeta::{HostConfig, ReplicaPlacementConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::placement::PeerCandidate;
use blockd_core::protocol::Verdict;
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

#[allow(clippy::struct_field_names)]
pub struct MigrationTimings {
    pub snapshot_write_ms: u128,
    pub publish_ms: u128,
    pub handoff_ms: u128,
    pub total_ms: u128,
    pub overlap_ms: u128,
}

fn vset_config() -> VsetConfig {
    VsetConfig::compute(1, VSET_PAGES)
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
    /// The snapshot prefix this VM was restored from.
    pub prefix: String,
    fc: Option<Arc<tokio::sync::Mutex<FcVm>>>,
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
    servers: Mutex<BTreeMap<String, Arc<tokio::sync::OnceCell<Arc<FillServer>>>>>,
}

impl Demod {
    pub fn firecracker_fault_latency(
        &self,
    ) -> Vec<(&'static str, blockd_runtime::HistogramSnapshot)> {
        self.servers
            .lock()
            .expect("lock")
            .values()
            .filter_map(|cell| cell.get())
            .map(|fill| (fill.server.source(), fill.server.fault_latency()))
            .collect()
    }

    pub async fn start(cfg: DemodConfig) -> Demod {
        tokio::fs::create_dir_all(&cfg.scratch)
            .await
            .expect("scratch dir");
        let store = Arc::new(GcsStore::new(GcsConfig {
            bucket: cfg.gcs_bucket.clone(),
            prefix: cfg.gcs_prefix.clone(),
            endpoint: cfg.gcs_endpoint.clone(),
            metadata_endpoint: cfg.gcs_metadata.clone(),
        }));
        let roster: Vec<PeerCandidate> = cfg.placement.clone();
        let local_failure_domain = roster
            .iter()
            .find(|candidate| candidate.host == cfg.host)
            .map_or(cfg.host.0, |candidate| candidate.failure_domain);
        let runtime_config = RuntimeConfig {
            daemon: HostConfig {
                archive: blockd_core::hostmeta::ArchivePolicy {
                    interval: millis(cfg.archive_interval_ms),
                    max_unpublished_bytes: cfg.archive_lag_bytes,
                    spool_capacity_bytes: cfg.peer_spool_capacity_bytes,
                    spool_headroom_bytes: cfg.peer_spool_headroom_bytes,
                },
                host: cfg.host,
                cache_pages: cfg.cache_pages,
                writeback_interval: millis(cfg.writeback_interval_ms),
                backup_retry: millis(cfg.backup_retry_ms),
                disk_capacity: cfg.disk_capacity_bytes,
                disk_headroom: cfg.disk_headroom_bytes,
                wedge_ticks: cfg.wedge_ticks,
                replica_placement: Some(ReplicaPlacementConfig {
                    membership_epoch: 1,
                    local_failure_domain,
                    roster,
                    authority: None,
                }),
            },
            blob_dir: cfg.blob_dir.clone(),
            peer: Some(PeerConfig {
                listen: cfg.peer_listen,
            }),
        };
        let rt = Runtime::new(&runtime_config, store.clone()).await;
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
    pub async fn bake_base(&self) -> String {
        let scratch = &self.cfg.scratch;
        let mut vm = FcVm::spawn(&self.fc_bin(), &scratch.join("bake.sock")).await;
        vm.boot(
            &self.cfg.fc_dir.join("vmlinux"),
            &self.cfg.fc_dir.join("initramfs.cpio"),
            MEM_MIB,
        )
        .await;
        vm.wait_line("READY").await;
        vm.cmd("fill 7 4096", "FILLED ").await;
        let sum = vm.cmd("sum 4096", "SUM ").await;
        let vmstate = scratch.join("base.vmstate");
        let mem = scratch.join("base.mem");
        let _ = tokio::fs::remove_file(&vmstate).await;
        let _ = tokio::fs::remove_file(&mem).await;
        vm.pause().await;
        vm.snapshot(&vmstate, &mem).await;
        vm.kill().await;
        // A re-bake replaces the store objects; a cached fill server for
        // the old snapshot would serve stale memory against the new
        // vmstate. Drop it so the next boot re-fetches.
        self.servers.lock().expect("lock").remove("base");
        self.publish_snapshot("base", &vmstate, &mem).await;
        GcsStore::put(self.store.as_ref(), "base/sum", sum.clone().into_bytes())
            .await
            .expect("publish sum");
        sum
    }

    /// Upload a snapshot (vmstate + memory parts) under `prefix`.
    #[tracing::instrument(skip(self, vmstate, mem), fields(snapshot_prefix = prefix))]
    async fn publish_snapshot(&self, prefix: &str, vmstate: &Path, mem: &Path) {
        let parts = upload_mem_parts_async(
            self.store.clone() as Arc<dyn ObjectStore>,
            mem.to_owned(),
            format!("{prefix}/mem"),
            PART_BYTES,
        )
        .await;
        assert_eq!(parts, u64::from(MEM_MIB) * 1024 * 1024 / PART_BYTES);
        let bytes = tokio::fs::read(vmstate).await.expect("vmstate file");
        GcsStore::put(self.store.as_ref(), &format!("{prefix}/vmstate"), bytes)
            .await
            .expect("publish vmstate");
        GcsStore::put(self.store.as_ref(), &format!("{prefix}/ready"), Vec::new())
            .await
            .expect("publish snapshot readiness");
    }

    async fn snapshot_generation(&self, prefix: &str) -> Option<u64> {
        GcsStore::get(self.store.as_ref(), &format!("{prefix}/ready"))
            .await
            .expect("store up")
            .map(|(generation, _)| generation)
    }

    async fn wait_snapshot_after(&self, prefix: &str, previous: Option<u64>) {
        let deadline = Instant::now() + Duration::from_mins(5);
        loop {
            let current = self.snapshot_generation(prefix).await;
            if current.is_some() && current != previous {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "migration snapshot was not published"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The fill server for a snapshot prefix — one per host per prefix;
    /// every VM restored from that prefix shares its one memory copy.
    async fn ensure_server(&self, prefix: &str) -> Arc<FillServer> {
        let cell = self
            .servers
            .lock()
            .expect("lock")
            .entry(prefix.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
            .clone();
        cell.get_or_init(|| async {
            let tag = format!(
                "{}-{}",
                self.cfg.host.0,
                self.next_server.fetch_add(1, Ordering::Relaxed)
            );
            let sock = self.cfg.scratch.join(format!("fill-{tag}.sock"));
            let shmem = self.cfg.shmem_dir.join(format!("blockd-{tag}.shmem"));
            let _ = tokio::fs::remove_file(&sock).await;
            let _ = tokio::fs::remove_file(&shmem).await;
            let listener = tokio::net::UnixListener::bind(&sock).expect("fill sock");
            let server = ShmemServer::start_store(
                listener,
                self.store.clone() as Arc<dyn ObjectStore>,
                format!("{prefix}/mem"),
                PART_BYTES,
                &shmem,
                u64::from(MEM_MIB) * 1024 * 1024,
                2,
            )
            .await;
            Arc::new(FillServer {
                server,
                sock,
                shmem,
            })
        })
        .await
        .clone()
    }

    /// Restore one FC microVM from a store-held snapshot prefix.
    #[tracing::instrument(skip(self), fields(vm_id = id, snapshot_prefix = prefix))]
    async fn boot_from(&self, id: u64, prefix: &str) -> FcVm {
        let fill = self.ensure_server(prefix).await;
        let vmstate = self.cfg.scratch.join(format!("vm{id}.vmstate"));
        let (_, bytes) = GcsStore::get(self.store.as_ref(), &format!("{prefix}/vmstate"))
            .await
            .expect("store up")
            .expect("vmstate published");
        tokio::fs::write(&vmstate, bytes)
            .await
            .expect("write vmstate");
        let vm = FcVm::spawn(
            &self.fc_bin(),
            &self.cfg.scratch.join(format!("vm{id}.sock")),
        )
        .await;
        vm.load_snapshot_shmem(&vmstate, &fill.sock, &fill.shmem)
            .await;
        vm
    }

    /// Start a fresh VM from the base snapshot with its paired vset.
    #[tracing::instrument(skip(self), fields(vm_id = tracing::field::Empty))]
    pub async fn start_vm(&self) -> u64 {
        let id = self.next_vm.fetch_add(1, Ordering::SeqCst);
        tracing::Span::current().record("vm_id", id);
        let mut fc = self.boot_from(id, "base").await;
        fc.cmd("ping", "PONG").await;
        self.rt.create_vset(VsetId(id), vset_config()).await;
        self.vms.lock().expect("lock").insert(
            id,
            Vm {
                state: "running".to_owned(),
                prefix: "base".to_owned(),
                fc: Some(Arc::new(tokio::sync::Mutex::new(fc))),
            },
        );
        tracing::info!(vm_id = id, "VM started");
        id
    }

    fn fc_of(&self, id: u64) -> Arc<tokio::sync::Mutex<FcVm>> {
        self.vms.lock().expect("lock")[&id]
            .fc
            .clone()
            .expect("vm has no running microVM")
    }

    /// One work burst: the guest computes over fresh memory, and the
    /// burst is mirrored into the vset — counter plus data pages — with a
    /// sync making it a durable consistency point.
    #[tracing::instrument(skip(self), fields(vm_id = id, burst_count = bursts))]
    pub async fn work(&self, id: u64, bursts: u64) -> (u64, String) {
        let vset = VsetId(id);
        let fc = self.fc_of(id);
        let mut sum = String::new();
        let mut burst = 0;
        for _ in 0..bursts {
            let counter = self.rt.guest_read(vset, disk_page(vset, 0)).await;
            burst = u64::from_le_bytes(counter[0..8].try_into().expect("sized")) + 1;
            let seed = id * 31 + burst;
            {
                let mut fc = fc.lock().await;
                fc.cmd(&format!("fill {seed} {GUEST_FILL_PAGES}"), "FILLED ")
                    .await;
                sum = fc.cmd(&format!("sum {GUEST_FILL_PAGES}"), "SUM ").await;
            }
            for page in 1..=MIRROR_PAGES {
                self.rt
                    .guest_write(vset, disk_page(vset, page), mirror_value(id, burst, page))
                    .await;
            }
            self.rt.guest_write(vset, disk_page(vset, 0), burst).await;
            assert!(self.rt.guest_sync(vset, VolumeIdx(1)).await, "sync failed");
        }
        (burst, sum)
    }

    /// Verify the vset against the self-describing model: page 0 names
    /// the burst, every data page must match it. Returns (burst,
    /// mismatches).
    #[tracing::instrument(skip(self), fields(vm_id = id))]
    pub async fn verify(&self, id: u64) -> (u64, u32) {
        let vset = VsetId(id);
        let counter = self.rt.guest_read(vset, disk_page(vset, 0)).await;
        let burst = u64::from_le_bytes(counter[0..8].try_into().expect("sized"));
        let mut mismatches = 0;
        for page in 1..=MIRROR_PAGES {
            let bytes = self.rt.guest_read(vset, disk_page(vset, page)).await;
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
    pub async fn fork(&self, id: u64, n: u32) -> (Vec<u64>, usize, usize, usize) {
        let prefix = format!("vm{id}/f{}", self.next_fork.fetch_add(1, Ordering::SeqCst));
        let vmstate = self.cfg.scratch.join(format!("fork-{id}.vmstate"));
        let mem = self.cfg.scratch.join(format!("fork-{id}.mem"));
        let _ = tokio::fs::remove_file(&vmstate).await;
        let _ = tokio::fs::remove_file(&mem).await;
        {
            let fc = self.fc_of(id);
            let fc = fc.lock().await;
            fc.pause().await;
            fc.snapshot(&vmstate, &mem).await;
            fc.resume().await;
        }
        self.publish_snapshot(&prefix, &vmstate, &mem).await;

        let mut ids = Vec::new();
        let (mut rss_sum, mut pss_sum) = (0, 0);
        for _ in 0..n {
            let fork_id = self.next_vm.fetch_add(1, Ordering::SeqCst);
            let mut fc = self.boot_from(fork_id, &prefix).await;
            fc.cmd("ping", "PONG").await;
            let (rss, pss) = rss_pss_of_pid(fc.pid()).await;
            rss_sum += rss;
            pss_sum += pss;
            self.rt.create_vset(VsetId(fork_id), vset_config()).await;
            self.vms.lock().expect("lock").insert(
                fork_id,
                Vm {
                    state: "running".to_owned(),
                    prefix: prefix.clone(),
                    fc: Some(Arc::new(tokio::sync::Mutex::new(fc))),
                },
            );
            ids.push(fork_id);
        }
        let server = self
            .servers
            .lock()
            .expect("lock")
            .get(&prefix)
            .and_then(|cell| cell.get().cloned());
        let resident = if let Some(server) = server {
            server.server.resident_bytes().await
        } else {
            0
        };
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
    pub async fn expect(self: &Arc<Demod>, id: u64, ready: impl FnOnce()) {
        let prefix = format!("vm{id}/mig");
        let previous_snapshot = self.snapshot_generation(&prefix).await;
        self.rt.expect_migration(VsetId(id), vset_config());
        self.vms.lock().expect("lock").insert(
            id,
            Vm {
                state: "expecting".to_owned(),
                prefix: prefix.clone(),
                fc: None,
            },
        );
        ready();
        {
            let verdict = self.rt.wait_migrated_in(VsetId(id)).await;
            assert!(
                matches!(verdict, Verdict::Resume { .. }),
                "migration verdict {verdict:?}"
            );
            self.wait_snapshot_after(&prefix, previous_snapshot).await;
            self.servers.lock().expect("lock").remove(&prefix);
            let mut fc = self.boot_from(id, &prefix).await;
            fc.cmd("ping", "PONG").await;
            let mut vms = self.vms.lock().expect("lock");
            let vm = vms.get_mut(&id).expect("expected vm");
            vm.fc = Some(Arc::new(tokio::sync::Mutex::new(fc)));
            "running".clone_into(&mut vm.state);
            tracing::info!(vm_id = id, "inbound migration resumed");
        }
    }

    /// Source side: pause + snapshot the microVM, publish it, kill it,
    /// and live-migrate the vset over TCP. Snapshot publication and the vset
    /// handoff overlap; the returned timings expose the saved serial time.
    #[tracing::instrument(skip(self), fields(vm_id = id, destination_host = to))]
    pub async fn migrate(&self, id: u64, to: u16) -> MigrationTimings {
        let vmstate = self.cfg.scratch.join(format!("mig-{id}.vmstate"));
        let mem = self.cfg.scratch.join(format!("mig-{id}.mem"));
        let _ = tokio::fs::remove_file(&vmstate).await;
        let _ = tokio::fs::remove_file(&mem).await;
        let snap_started = Instant::now();
        {
            let fc = self.fc_of(id);
            let fc = fc.lock().await;
            fc.pause().await;
            fc.snapshot(&vmstate, &mem).await;
        }
        let snapshot_write_ms = snap_started.elapsed().as_millis();
        let prefix = format!("vm{id}/mig");
        let publish = async {
            let publish_started = Instant::now();
            self.publish_snapshot(&prefix, &vmstate, &mem).await;
            publish_started.elapsed().as_millis()
        };
        let handoff = async {
            let handoff_started = Instant::now();
            self.rt.migrate_out(VsetId(id), HostId(to)).await;
            handoff_started.elapsed().as_millis()
        };
        let (publish_ms, handoff_ms) = tokio::join!(publish, handoff);
        let total_ms = snap_started.elapsed().as_millis();
        let serial_ms = snapshot_write_ms + publish_ms + handoff_ms;
        let overlap_ms = serial_ms.saturating_sub(total_ms);
        let fc = {
            let mut vms = self.vms.lock().expect("lock");
            let vm = vms.get_mut(&id).expect("vm");
            "migrated-out".clone_into(&mut vm.state);
            vm.fc.take()
        };
        if let Some(fc) = fc
            && let Ok(fc) = Arc::try_unwrap(fc)
        {
            fc.into_inner().kill().await;
        }
        tracing::info!(
            vm_id = id,
            destination_host = to,
            snapshot_write_ms,
            publish_ms,
            handoff_ms,
            total_ms,
            overlap_ms,
            "outbound migration completed"
        );
        MigrationTimings {
            snapshot_write_ms,
            publish_ms,
            handoff_ms,
            total_ms,
            overlap_ms,
        }
    }

    /// After the owning host died: restore a vset from the store
    /// alone (R6.1) and give it a fresh microVM from the base image.
    #[tracing::instrument(skip(self), fields(vm_id = id))]
    pub async fn restore(&self, id: u64) -> String {
        let verdict = self.rt.restore_vset(VsetId(id), vset_config()).await;
        let mut fc = self.boot_from(id, "base").await;
        fc.cmd("ping", "PONG").await;
        self.vms.lock().expect("lock").insert(
            id,
            Vm {
                state: "restored".to_owned(),
                prefix: "base".to_owned(),
                fc: Some(Arc::new(tokio::sync::Mutex::new(fc))),
            },
        );
        tracing::info!(vm_id = id, ?verdict, "VM restored");
        format!("{verdict:?}")
    }
}
