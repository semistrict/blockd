//! End-to-end against REAL Firecracker microVMs (local only — no object
//! store): boot the ported workload, do work, snapshot, restore, FORK the
//! snapshot into many VMs — especially after work — keep working in every
//! fork, and let the guests themselves prove state carriage, divergence
//! and isolation by checksumming their own memory. The uffd scenario runs
//! blockd's fill door as the VMM's page-fault handler.
//!
//! Requires the staged artifacts (`firecracker`, `vmlinux`,
//! `initramfs.cpio`) in `$BLOCKD_FC_DIR` (default `/var/tmp/blockd-fc`)
//! and /dev/kvm access — the Lima VM is set up exactly so.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_precision_loss)] // presentation math in profiles

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use blockd_hostmem::page_size;
use blockd_runtime::fc::{FcVm, rss_pss_of_pid, serve_uffd};
use blockd_workload::{Backend, Capability, LogicalPage, Operation, VerifyScope, WorkloadModel};

const MEM_MIB: u32 = 128;
const WORKLOAD_PAGES: usize = 4096;

fn arena_host_pages() -> u64 {
    u64::try_from(WORKLOAD_PAGES).expect("fits")
}

struct Artifacts {
    fc: PathBuf,
    kernel: PathBuf,
    initrd: PathBuf,
    scratch: PathBuf,
}

fn artifacts(tag: &str) -> Artifacts {
    let dir = PathBuf::from(
        std::env::var("BLOCKD_FC_DIR").unwrap_or_else(|_| "/var/tmp/blockd-fc".into()),
    );
    for name in ["firecracker", "vmlinux", "initramfs.cpio"] {
        assert!(
            dir.join(name).exists(),
            "missing {name} in {} — stage the Firecracker artifacts first",
            dir.display()
        );
    }
    // Real disk, never tmpfs: snapshot memory files are large, and a
    // tmpfs scratch would silently consume RAM (and fill under a
    // concurrent suite). Fixed names self-clean on rerun.
    let scratch = PathBuf::from(format!("/var/tmp/blockd-scratch/fc-{tag}"));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch");
    Artifacts {
        fc: dir.join("firecracker"),
        kernel: dir.join("vmlinux"),
        initrd: dir.join("initramfs.cpio"),
        scratch,
    }
}

/// The `UffdShmem` backing file must live on a REAL shmem mount: guest
/// memory is RAM, and uffd registration of a private file mapping is only
/// supported for shmem. Disk scratch is for snapshots and sockets.
fn shmem_path(tag: &str) -> PathBuf {
    let path = PathBuf::from(format!("/dev/shm/blockd-{tag}.shmem"));
    let _ = std::fs::remove_file(&path);
    path
}

async fn boot_vm(art: &Artifacts, name: &str) -> FcVm {
    let vm = FcVm::spawn(&art.fc, &art.scratch.join(format!("{name}.sock"))).await;
    vm.boot(&art.kernel, &art.initrd, MEM_MIB).await;
    vm
}

#[derive(Debug, Default)]
struct FirecrackerWorkloadMetrics {
    commands: u64,
    checkpoints: u64,
    restores: u64,
    forks: u64,
    checkpoint_time: Duration,
    restore_time: Duration,
    fork_time: Duration,
}

struct FirecrackerBackend<'a> {
    art: &'a Artifacts,
    vm: Option<FcVm>,
    snapshot: PathBuf,
    memory: PathBuf,
    metrics: FirecrackerWorkloadMetrics,
}

impl<'a> FirecrackerBackend<'a> {
    fn new(art: &'a Artifacts) -> Self {
        Self {
            art,
            vm: None,
            snapshot: art.scratch.join("workload.vmstate"),
            memory: art.scratch.join("workload.mem"),
            metrics: FirecrackerWorkloadMetrics::default(),
        }
    }

    fn vm(&mut self) -> &mut FcVm {
        self.vm.as_mut().expect("microVM is running")
    }

    async fn read_page(&mut self, page: LogicalPage) -> Result<u64, String> {
        if page.volume != 0 {
            return Err(format!(
                "Firecracker arena has no disk volume {}",
                page.volume
            ));
        }
        let observed = self
            .vm()
            .cmd(&format!("read {}", page.page), "VALUE ")
            .await;
        self.metrics.commands += 1;
        observed
            .parse()
            .map_err(|error| format!("invalid value for {page:?}: {observed:?}: {error}"))
    }

    async fn verify(&mut self, model: &WorkloadModel, scope: VerifyScope) -> Result<(), String> {
        for (page, expected) in model.pages(scope) {
            let observed = self.read_page(page).await?;
            if observed != expected {
                return Err(format!(
                    "{page:?}: observed {observed:#x}, expected {expected:#x}"
                ));
            }
        }
        Ok(())
    }
}

impl Backend for FirecrackerBackend<'_> {
    type Error = String;

    fn supports(&self, capability: Capability) -> bool {
        matches!(
            capability,
            Capability::Create
                | Capability::Data
                | Capability::Checkpoint
                | Capability::Crash
                | Capability::Restore
                | Capability::Verify
                | Capability::Fork
        )
    }

    #[allow(clippy::too_many_lines)] // one arm per guest/lifecycle operation
    async fn execute(
        &mut self,
        operation: Operation,
        model: &WorkloadModel,
    ) -> Result<(), Self::Error> {
        match operation {
            Operation::Create => {
                let mut vm = boot_vm(self.art, "workload-base").await;
                vm.wait_line("READY").await;
                self.vm = Some(vm);
            }
            Operation::Read { page } => {
                let observed = self.read_page(page).await?;
                let expected = model.expected(page);
                if observed != expected {
                    return Err(format!(
                        "{page:?}: observed {observed:#x}, expected {expected:#x}"
                    ));
                }
            }
            Operation::Write { page, value } => {
                if page.volume != 0 {
                    return Err(format!(
                        "Firecracker arena has no disk volume {}",
                        page.volume
                    ));
                }
                self.vm()
                    .cmd(&format!("mark {} {value}", page.page), "MARKED")
                    .await;
                self.metrics.commands += 1;
            }
            Operation::Checkpoint => {
                let started = Instant::now();
                let snapshot = self.snapshot.clone();
                let memory = self.memory.clone();
                self.vm().pause().await;
                self.vm().snapshot(&snapshot, &memory).await;
                self.vm().resume().await;
                self.metrics.checkpoint_time += started.elapsed();
                self.metrics.checkpoints += 1;
            }
            Operation::Crash => {
                self.vm.take().expect("microVM is running").kill().await;
            }
            Operation::Restore => {
                let started = Instant::now();
                let restored = FcVm::spawn(
                    &self.art.fc,
                    &self.art.scratch.join("workload-restored.sock"),
                )
                .await;
                restored
                    .load_snapshot(&self.snapshot, &self.memory, None)
                    .await;
                self.vm = Some(restored);
                self.metrics.restore_time += started.elapsed();
                self.metrics.restores += 1;
            }
            Operation::Verify { scope } => self.verify(model, scope).await?,
            Operation::Fork { copies } => {
                let started = Instant::now();
                let mut forks: Vec<FcVm> = Vec::new();
                for copy in 0..copies {
                    let fork = FcVm::spawn(
                        &self.art.fc,
                        &self.art.scratch.join(format!("workload-fork-{copy}.sock")),
                    )
                    .await;
                    fork.load_snapshot(&self.snapshot, &self.memory, None).await;
                    forks.push(fork);
                }
                for fork in &mut forks {
                    for (page, expected) in model.pages(VerifyScope::Memory) {
                        let observed = fork
                            .cmd(&format!("read {}", page.page), "VALUE ")
                            .await
                            .parse::<u64>()
                            .map_err(|error| error.to_string())?;
                        if observed != expected {
                            return Err(format!(
                                "fork {page:?}: observed {observed:#x}, expected {expected:#x}"
                            ));
                        }
                    }
                }
                let mut diverged = Vec::new();
                for (copy, fork) in forks.iter_mut().enumerate() {
                    let value = 0xF000_0000 + u64::try_from(copy).expect("copy fits");
                    fork.cmd(&format!("mark 0 {value}"), "MARKED").await;
                }
                for (copy, fork) in forks.iter_mut().enumerate() {
                    let expected = 0xF000_0000 + u64::try_from(copy).expect("copy fits");
                    let observed = fork.cmd("read 0", "VALUE ").await;
                    if observed != expected.to_string() {
                        return Err(format!(
                            "fork {copy} observed {observed} after isolated write, expected {expected}"
                        ));
                    }
                    diverged.push(observed);
                }
                diverged.sort();
                diverged.dedup();
                if diverged.len() != usize::from(copies) {
                    return Err("fork writes were not isolated".to_owned());
                }
                for fork in forks {
                    fork.kill().await;
                }
                self.metrics.fork_time += started.elapsed();
                self.metrics.forks += u64::from(copies);
            }
            _ => unreachable!("capability checked before execution"),
        }
        Ok(())
    }
}

#[tokio::test]
async fn declarative_memory_snapshot_runs_inside_firecracker() {
    let art = artifacts("shared-workload");
    let spec = blockd_workload::load("memory-snapshot").expect("memory workload");
    let mut backend = FirecrackerBackend::new(&art);
    let outcome = blockd_workload::run(&spec, &mut backend)
        .await
        .expect("Firecracker workload");

    assert_eq!(backend.metrics.checkpoints, outcome.checkpoints);
    assert_eq!(backend.metrics.restores, outcome.restores);
    assert_eq!(backend.metrics.forks, outcome.forks);
    assert!(backend.metrics.commands >= outcome.reads + outcome.writes);
}

/// Boot → the guest works and audits itself.
#[tokio::test]
async fn boot_and_workload_answers() {
    let art = artifacts("boot");
    let mut vm = boot_vm(&art, "vm").await;
    vm.wait_line("READY").await;
    assert_eq!(vm.cmd("ping", "PONG").await, "");
    let filled = vm.cmd("fill 7 4096", "FILLED ").await;
    let sum = vm.cmd("sum 4096", "SUM ").await;
    assert_eq!(filled, sum, "guest checksum diverged from its own fill");
    vm.cmd("off", "BYE").await;
}

/// Pause → full snapshot → kill → restore in a NEW Firecracker process:
/// the guest continues mid-loop with its memory byte-identical (its own
/// checksum says so), and keeps working.
#[tokio::test]
async fn snapshot_restore_preserves_guest_state_exactly() {
    let art = artifacts("snap");
    let mut vm = boot_vm(&art, "a").await;
    vm.wait_line("READY").await;
    vm.cmd("fill 7 4096", "FILLED ").await;
    vm.cmd("mark 3 12345", "MARKED").await;
    let sum_before = vm.cmd("sum 4096", "SUM ").await;

    let snap = art.scratch.join("snap.vmstate");
    let mem = art.scratch.join("snap.mem");
    vm.pause().await;
    vm.snapshot(&snap, &mem).await;
    vm.kill().await; // host death after the snapshot is durable

    let mut restored = FcVm::spawn(&art.fc, &art.scratch.join("b.sock")).await;
    restored.load_snapshot(&snap, &mem, None).await;
    let sum_after = restored.cmd("sum 4096", "SUM ").await;
    assert_eq!(sum_before, sum_after, "restore changed guest memory");
    // Still alive and writable.
    let refilled = restored.cmd("fill 9 2048", "FILLED ").await;
    assert_eq!(restored.cmd("sum 2048", "SUM ").await, refilled);
    restored.cmd("off", "BYE").await;
}

/// THE fork scenario (R5): work FIRST, snapshot once, then fork the
/// snapshot into three concurrently-running VMs. Every fork carries the
/// worked state; each diverges with its own work; checksums prove
/// pairwise isolation; and the kernel's Pss accounting proves the forks
/// SHARE the untouched base memory rather than copying it.
#[tokio::test]
async fn fork_after_work_diverges_in_isolation_and_shares_the_base() {
    let art = artifacts("fork");
    let mut vm = boot_vm(&art, "base").await;
    vm.wait_line("READY").await;
    // The work BEFORE the fork: a 16 MiB seeded arena.
    vm.cmd("fill 7 4096", "FILLED ").await;
    let base_sum = vm.cmd("sum 4096", "SUM ").await;
    let snap = art.scratch.join("base.vmstate");
    let mem = art.scratch.join("base.mem");
    vm.pause().await;
    vm.snapshot(&snap, &mem).await;
    vm.kill().await;

    // Fork: three restores of ONE snapshot, all running at once.
    let mut forks: Vec<FcVm> = Vec::new();
    for n in 0..3 {
        let fork = FcVm::spawn(&art.fc, &art.scratch.join(format!("fork{n}.sock"))).await;
        fork.load_snapshot(&snap, &mem, None).await;
        forks.push(fork);
    }

    // Every fork carries the pre-fork work, byte-exact.
    for fork in &mut forks {
        assert_eq!(fork.cmd("sum 4096", "SUM ").await, base_sum);
    }
    // The arena is resident in fork 0 (Rss counts it fully)...
    let (rss_one, _) = rss_pss_of_pid(forks[0].pid()).await;
    assert!(
        rss_one > 16 * 1024 * 1024,
        "fork 0 has not materialized the arena ({rss_one} bytes)"
    );
    // ...but the fleet SHARES it: proportional accounting says most of
    // those resident pages are mapped by several forks (R5.3 on a real
    // VMM — one snapshot file backs all three).
    let mut resident = 0;
    let mut proportional = 0;
    for fork in &forks {
        let (rss, pss) = rss_pss_of_pid(fork.pid()).await;
        resident += rss;
        proportional += pss;
    }
    assert!(
        proportional * 4 < resident * 3,
        "forks are not sharing the base: Pss {proportional} vs Rss {resident}"
    );

    // Work IN each fork: distinct marks and distinct 4 MiB refills.
    let mut fork_sums = Vec::new();
    for (n, fork) in forks.iter_mut().enumerate() {
        fork.cmd(&format!("mark 0 {}", 1000 + n), "MARKED").await;
        fork.cmd(&format!("fill {} 1024", 100 + n), "FILLED ").await;
        fork_sums.push(fork.cmd("sum 4096", "SUM ").await);
    }
    // Divergence: every fork differs from the base and from every other
    // fork — and each fork's state is stable (isolation both ways).
    for (n, sum) in fork_sums.iter().enumerate() {
        assert_ne!(sum, &base_sum, "fork {n} did not diverge");
        for (m, other) in fork_sums.iter().enumerate().skip(n + 1) {
            assert_ne!(sum, other, "forks {n} and {m} share written state");
        }
    }
    for (fork, expected) in forks.iter_mut().zip(&fork_sums) {
        assert_eq!(&fork.cmd("sum 4096", "SUM ").await, expected);
    }

    // After divergence the base is STILL shared: each fork privatized only
    // its ~4 MiB of writes; the untouched arena remains one copy across
    // the fleet.
    let mut resident_after = 0;
    let mut proportional_after = 0;
    for fork in &forks {
        let (rss, pss) = rss_pss_of_pid(fork.pid()).await;
        resident_after += rss;
        proportional_after += pss;
    }
    assert!(
        proportional_after * 4 < resident_after * 3,
        "divergence destroyed sharing: Pss {proportional_after} vs Rss {resident_after}"
    );
    for fork in forks {
        fork.kill().await;
    }
}

/// Restore through OUR page-fault handler (Firecracker's Uffd memory
/// backend): every guest touch after resume is a fault WE serve from the
/// snapshot memory — blockd's fill door under a real VMM. The guest's own
/// checksum proves every served page was exact, and the served counter
/// proves demand paging actually happened.
#[tokio::test]
async fn uffd_restore_serves_guest_memory_on_demand() {
    let art = artifacts("uffd");
    let mut vm = boot_vm(&art, "src").await;
    vm.wait_line("READY").await;
    vm.cmd("fill 7 4096", "FILLED ").await;
    let sum_before = vm.cmd("sum 4096", "SUM ").await;
    let snap = art.scratch.join("u.vmstate");
    let mem = art.scratch.join("u.mem");
    vm.pause().await;
    vm.snapshot(&snap, &mem).await;
    vm.kill().await;

    let uffd_sock = art.scratch.join("uffd.sock");
    let listener = tokio::net::UnixListener::bind(&uffd_sock).expect("bind");
    let served = Arc::new(AtomicU64::new(0));
    serve_uffd(listener, mem.clone(), served.clone());

    let mut restored = FcVm::spawn(&art.fc, &art.scratch.join("dst.sock")).await;
    restored.load_snapshot(&snap, &mem, Some(&uffd_sock)).await;
    let sum_after = restored.cmd("sum 4096", "SUM ").await;
    assert_eq!(sum_before, sum_after, "a demand-served page was wrong");
    // The 16 MiB arena was touched in full through OUR handler.
    let served_pages = served.load(Ordering::SeqCst);
    assert!(
        served_pages >= arena_host_pages(),
        "demand paging barely happened: {served_pages} pages served"
    );
    // Keep working post-restore: new writes fault through the handler too.
    let refilled = restored.cmd("fill 11 2048", "FILLED ").await;
    assert_eq!(restored.cmd("sum 2048", "SUM ").await, refilled);
    restored.cmd("off", "BYE").await;
}

/// Verify shared clean pages, copy-on-write divergence, and backing reclaim
/// with the patched `UffdShmem` Firecracker memory backend.
#[tokio::test]
async fn shmem_forks_share_one_copy_diverge_and_survive_reclaim() {
    use blockd_runtime::fc::ShmemServer;
    use std::sync::atomic::Ordering;

    let art = artifacts("shmem");
    let mut vm = boot_vm(&art, "base").await;
    vm.wait_line("READY").await;
    vm.cmd("fill 7 4096", "FILLED ").await;
    let base_sum = vm.cmd("sum 4096", "SUM ").await;
    let snap = art.scratch.join("base.vmstate");
    let mem = art.scratch.join("base.mem");
    vm.pause().await;
    vm.snapshot(&snap, &mem).await;
    vm.kill().await;

    // The handler owns the (sparse) shared base; every fork maps it.
    let mem_bytes = u64::from(MEM_MIB) * 1024 * 1024;
    let uffd_sock = art.scratch.join("shmem.sock");
    let shmem = shmem_path("fork-base");
    let listener = tokio::net::UnixListener::bind(&uffd_sock).expect("bind");
    let server = ShmemServer::start(listener, mem.clone(), &shmem, mem_bytes).await;

    let mut forks: Vec<FcVm> = Vec::new();
    for n in 0..3 {
        let fork = FcVm::spawn(&art.fc, &art.scratch.join(format!("shm{n}.sock"))).await;
        fork.load_snapshot_shmem(&snap, &uffd_sock, &shmem).await;
        forks.push(fork);
    }

    // Every fork carries the pre-fork work — every byte arrived through
    // OUR fill door on demand.
    for fork in &mut forks {
        assert_eq!(fork.cmd("sum 4096", "SUM ").await, base_sum);
    }
    // ONE physical copy: three forks touched the whole arena, yet
    // unique fills stayed far below 2× the arena — and the shmem file
    // holds exactly the filled pages, once.
    let filled = server.filled();
    let faults = server.faults.load(Ordering::SeqCst);
    assert!(
        filled >= arena_host_pages(),
        "the arena was not demand-filled: {filled}"
    );
    assert!(
        filled < 2 * arena_host_pages(),
        "forks are not sharing fills: {filled} unique fills for 3 forks"
    );
    assert_eq!(
        server.resident_bytes().await,
        usize::try_from(filled).expect("fits") * page_size(),
        "the base holds copies beyond one per filled page"
    );
    // Warm-page touches by later forks resolved from the page cache with
    // no fault at all — fault volume stays well under 3× the fills.
    assert!(
        faults < 3 * filled,
        "every fork faulted every page: {faults} faults vs {filled} fills"
    );

    // Divergence: private work per fork, isolated pairwise…
    let resident_before_divergence = server.resident_bytes().await;
    let filled_before_divergence = server.filled();
    let mut fork_sums = Vec::new();
    for (n, fork) in forks.iter_mut().enumerate() {
        fork.cmd(&format!("mark 0 {}", 2000 + n), "MARKED").await;
        fork.cmd(&format!("fill {} 1024", 200 + n), "FILLED ").await;
        fork_sums.push(fork.cmd("sum 4096", "SUM ").await);
    }
    for (n, sum) in fork_sums.iter().enumerate() {
        assert_ne!(sum, &base_sum, "fork {n} did not diverge");
        for (m, other) in fork_sums.iter().enumerate().skip(n + 1) {
            assert_ne!(sum, other, "forks {n} and {m} share written state");
        }
    }
    // …and copy-on-write kept every diverged byte OUT of the shared base:
    // the base grew by exactly the NEW first-touch demand fills (a write
    // fault on a never-touched page populates the base once before CoW),
    // never by copies of diverged data. Three forks each rewrote 4 MiB
    // (3072 pages of divergence) — none of it landed here.
    let new_fills = server.filled() - filled_before_divergence;
    assert_eq!(
        server.resident_bytes().await,
        resident_before_divergence + usize::try_from(new_fills).expect("fits") * page_size(),
        "divergence leaked into the shared base"
    );
    assert!(
        new_fills < u64::try_from(1024 * 4096 / page_size()).expect("fits"),
        "divergence wrote through to the base: {new_fills} pages appeared"
    );

    // Backing reclaim (R2.4/R2.7 under a real VMM): free the entire base.
    // Paused vCPUs make the zero-resident check exact — a running guest's
    // timer ticks refault kernel pages through the fill door the instant
    // the punch lands, and that's refill working, not reclaim failing.
    for fork in &forks {
        fork.pause().await;
    }
    server.reclaim_all(mem_bytes).await;
    assert_eq!(
        server.resident_bytes().await,
        0,
        "reclaim left pages behind"
    );
    for fork in &forks {
        fork.resume().await;
    }
    // Every fork still has its exact diverged state: dirty CoW pages
    // survived the punch; clean pages refaulted and refilled through the
    // handler.
    let filled_before_refill = server.filled();
    for (fork, expected) in forks.iter_mut().zip(&fork_sums) {
        assert_eq!(&fork.cmd("sum 4096", "SUM ").await, expected);
    }
    assert!(
        server.filled() > filled_before_refill,
        "reclaim did not cause refills — nothing was actually freed"
    );
    for fork in forks {
        fork.kill().await;
    }
}

/// The R1.3/R5.3 fleet economics on REAL Firecracker: MANY forks of one
/// worked snapshot, each doing a LITTLE work — and the measured memory
/// bill is one base plus a small per-fork marginal, never nominal × N.
#[tokio::test]
async fn many_forks_each_do_small_work_and_memory_stays_marginal() {
    use blockd_runtime::fc::{ShmemServer, rss_pss_of_pid};
    use std::time::Instant;

    const FORKS: usize = 12;
    let art = artifacts("manyforks");
    let mut vm = boot_vm(&art, "base").await;
    vm.wait_line("READY").await;
    vm.cmd("fill 7 4096", "FILLED ").await;
    let base_sum = vm.cmd("sum 4096", "SUM ").await;
    let snap = art.scratch.join("base.vmstate");
    let mem = art.scratch.join("base.mem");
    vm.pause().await;
    vm.snapshot(&snap, &mem).await;
    vm.kill().await;

    let mem_bytes = u64::from(MEM_MIB) * 1024 * 1024;
    let uffd_sock = art.scratch.join("many.sock");
    let shmem = shmem_path("many-forks");
    let listener = tokio::net::UnixListener::bind(&uffd_sock).expect("bind");
    let server = ShmemServer::start(listener, mem, &shmem, mem_bytes).await;

    // Boot storm: N concurrent microVMs from ONE snapshot.
    let storm = Instant::now();
    let mut forks: Vec<FcVm> = Vec::new();
    for n in 0..FORKS {
        let fork = FcVm::spawn(&art.fc, &art.scratch.join(format!("many{n}.sock"))).await;
        fork.load_snapshot_shmem(&snap, &uffd_sock, &shmem).await;
        forks.push(fork);
    }
    for fork in &mut forks {
        fork.cmd("ping", "PONG").await;
    }
    let storm_elapsed = storm.elapsed();

    // A LITTLE work in each fork: one mark + a 64 KiB private refill,
    // verified by the guest's own checksum.
    for (n, fork) in forks.iter_mut().enumerate() {
        fork.cmd(&format!("mark 0 {}", 5000 + n), "MARKED").await;
        let refilled = fork.cmd(&format!("fill {} 16", 300 + n), "FILLED ").await;
        assert_eq!(fork.cmd("sum 16", "SUM ").await, refilled);
    }
    // Isolation across the whole fleet: full-arena sums pairwise distinct
    // (every fork carries the base plus exactly its own writes).
    let mut sums = Vec::new();
    for fork in &mut forks {
        sums.push(fork.cmd("sum 4096", "SUM ").await);
    }
    for (n, sum) in sums.iter().enumerate() {
        assert_ne!(sum, &base_sum, "fork {n} did not diverge");
        for (m, other) in sums.iter().enumerate().skip(n + 1) {
            assert_ne!(sum, other, "forks {n} and {m} share written state");
        }
    }

    // THE BILL. Unique fills stayed one-base-sized (shared by all 12) …
    let filled_pages = server.filled();
    let base_resident = server.resident_bytes().await;
    assert_eq!(
        base_resident,
        usize::try_from(filled_pages).expect("fits") * page_size(),
        "base holds more than one copy per filled page"
    );
    assert!(
        filled_pages < 3 * arena_host_pages(),
        "unique fills scaled with the fleet: {filled_pages} pages for {FORKS} forks"
    );
    // PSS measures the marginal physical memory used by each additional VM.
    let (_, pss_first) = rss_pss_of_pid(forks[0].pid()).await;
    let mut fleet_pss = 0;
    for fork in &forks {
        fleet_pss += rss_pss_of_pid(fork.pid()).await.1;
    }
    let marginal = (fleet_pss - pss_first) / (FORKS - 1);
    eprintln!(
        "── MANY-FORKS BILL: {FORKS} forks in {storm_elapsed:.1?}; base {base_resident} B \
         ({filled_pages} pages, filled once); fleet Pss {:.1} MiB; \
         per-fork marginal {:.1} MiB (vs {MEM_MIB} MiB nominal) ──",
        fleet_pss as f64 / (1024.0 * 1024.0),
        marginal as f64 / (1024.0 * 1024.0),
    );
    let nominal = usize::try_from(mem_bytes).expect("fits");
    assert!(
        fleet_pss < nominal + FORKS * nominal / 4,
        "fleet costs like full copies: {fleet_pss} bytes for {FORKS} forks"
    );
    assert!(
        marginal < nominal / 4,
        "per-fork marginal {marginal} bytes is not marginal against {nominal}"
    );
    for fork in forks {
        fork.kill().await;
    }
}
