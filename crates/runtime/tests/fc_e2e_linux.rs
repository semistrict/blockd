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

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use blockd_runtime::fc::{FcVm, rss_pss_of_pid, serve_uffd};

const MEM_MIB: u32 = 128;

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
    let scratch = std::env::temp_dir().join(format!("blockd-fc-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("scratch");
    Artifacts {
        fc: dir.join("firecracker"),
        kernel: dir.join("vmlinux"),
        initrd: dir.join("initramfs.cpio"),
        scratch,
    }
}

fn boot_vm(art: &Artifacts, name: &str) -> FcVm {
    let vm = FcVm::spawn(&art.fc, &art.scratch.join(format!("{name}.sock")));
    vm.boot(&art.kernel, &art.initrd, MEM_MIB);
    vm
}

/// Boot → the guest works and audits itself.
#[test]
fn boot_and_workload_answers() {
    let art = artifacts("boot");
    let mut vm = boot_vm(&art, "vm");
    vm.wait_line("READY");
    assert_eq!(vm.cmd("ping", "PONG"), "");
    let filled = vm.cmd("fill 7 4096", "FILLED ");
    let sum = vm.cmd("sum 4096", "SUM ");
    assert_eq!(filled, sum, "guest checksum diverged from its own fill");
    vm.cmd("off", "BYE");
}

/// Pause → full snapshot → kill → restore in a NEW Firecracker process:
/// the guest continues mid-loop with its memory byte-identical (its own
/// checksum says so), and keeps working.
#[test]
fn snapshot_restore_preserves_guest_state_exactly() {
    let art = artifacts("snap");
    let mut vm = boot_vm(&art, "a");
    vm.wait_line("READY");
    vm.cmd("fill 7 4096", "FILLED ");
    vm.cmd("mark 3 12345", "MARKED");
    let sum_before = vm.cmd("sum 4096", "SUM ");

    let snap = art.scratch.join("snap.vmstate");
    let mem = art.scratch.join("snap.mem");
    vm.pause();
    vm.snapshot(&snap, &mem);
    vm.kill(); // host death after the snapshot is durable

    let mut restored = FcVm::spawn(&art.fc, &art.scratch.join("b.sock"));
    restored.load_snapshot(&snap, &mem, None);
    let sum_after = restored.cmd("sum 4096", "SUM ");
    assert_eq!(sum_before, sum_after, "restore changed guest memory");
    // Still alive and writable.
    let refilled = restored.cmd("fill 9 2048", "FILLED ");
    assert_eq!(restored.cmd("sum 2048", "SUM "), refilled);
    restored.cmd("off", "BYE");
}

/// THE fork scenario (R5): work FIRST, snapshot once, then fork the
/// snapshot into three concurrently-running VMs. Every fork carries the
/// worked state; each diverges with its own work; checksums prove
/// pairwise isolation; and the kernel's Pss accounting proves the forks
/// SHARE the untouched base memory rather than copying it.
#[test]
fn fork_after_work_diverges_in_isolation_and_shares_the_base() {
    let art = artifacts("fork");
    let mut vm = boot_vm(&art, "base");
    vm.wait_line("READY");
    // The work BEFORE the fork: a 16 MiB seeded arena.
    vm.cmd("fill 7 4096", "FILLED ");
    let base_sum = vm.cmd("sum 4096", "SUM ");
    let snap = art.scratch.join("base.vmstate");
    let mem = art.scratch.join("base.mem");
    vm.pause();
    vm.snapshot(&snap, &mem);
    vm.kill();

    // Fork: three restores of ONE snapshot, all running at once.
    let mut forks: Vec<FcVm> = (0..3)
        .map(|n| {
            let fork = FcVm::spawn(&art.fc, &art.scratch.join(format!("fork{n}.sock")));
            fork.load_snapshot(&snap, &mem, None);
            fork
        })
        .collect();

    // Every fork carries the pre-fork work, byte-exact.
    for fork in &mut forks {
        assert_eq!(fork.cmd("sum 4096", "SUM "), base_sum);
    }
    // The arena is resident in fork 0 (Rss counts it fully)...
    let (rss_one, _) = rss_pss_of_pid(forks[0].pid());
    assert!(
        rss_one > 16 * 1024 * 1024,
        "fork 0 has not materialized the arena ({rss_one} bytes)"
    );
    // ...but the fleet SHARES it: proportional accounting says most of
    // those resident pages are mapped by several forks (R5.3 on a real
    // VMM — one snapshot file backs all three).
    let (resident, proportional): (usize, usize) = forks
        .iter()
        .map(|fork| rss_pss_of_pid(fork.pid()))
        .fold((0, 0), |(r, p), (rss, pss)| (r + rss, p + pss));
    assert!(
        proportional * 4 < resident * 3,
        "forks are not sharing the base: Pss {proportional} vs Rss {resident}"
    );

    // Work IN each fork: distinct marks and distinct 4 MiB refills.
    let mut fork_sums = Vec::new();
    for (n, fork) in forks.iter_mut().enumerate() {
        fork.cmd(&format!("mark 0 {}", 1000 + n), "MARKED");
        fork.cmd(&format!("fill {} 1024", 100 + n), "FILLED ");
        fork_sums.push(fork.cmd("sum 4096", "SUM "));
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
        assert_eq!(&fork.cmd("sum 4096", "SUM "), expected);
    }

    // After divergence the base is STILL shared: each fork privatized only
    // its ~4 MiB of writes; the untouched arena remains one copy across
    // the fleet.
    let (resident_after, proportional_after): (usize, usize) = forks
        .iter()
        .map(|fork| rss_pss_of_pid(fork.pid()))
        .fold((0, 0), |(r, p), (rss, pss)| (r + rss, p + pss));
    assert!(
        proportional_after * 4 < resident_after * 3,
        "divergence destroyed sharing: Pss {proportional_after} vs Rss {resident_after}"
    );
    for fork in forks {
        fork.kill();
    }
}

/// Restore through OUR page-fault handler (Firecracker's Uffd memory
/// backend): every guest touch after resume is a fault WE serve from the
/// snapshot memory — blockd's fill door under a real VMM. The guest's own
/// checksum proves every served page was exact, and the served counter
/// proves demand paging actually happened.
#[test]
fn uffd_restore_serves_guest_memory_on_demand() {
    let art = artifacts("uffd");
    let mut vm = boot_vm(&art, "src");
    vm.wait_line("READY");
    vm.cmd("fill 7 4096", "FILLED ");
    let sum_before = vm.cmd("sum 4096", "SUM ");
    let snap = art.scratch.join("u.vmstate");
    let mem = art.scratch.join("u.mem");
    vm.pause();
    vm.snapshot(&snap, &mem);
    vm.kill();

    let uffd_sock = art.scratch.join("uffd.sock");
    let listener = std::os::unix::net::UnixListener::bind(&uffd_sock).expect("bind");
    let served = Arc::new(AtomicU64::new(0));
    serve_uffd(listener, mem.clone(), served.clone());

    let mut restored = FcVm::spawn(&art.fc, &art.scratch.join("dst.sock"));
    restored.load_snapshot(&snap, &mem, Some(&uffd_sock));
    let sum_after = restored.cmd("sum 4096", "SUM ");
    assert_eq!(sum_before, sum_after, "a demand-served page was wrong");
    // The 16 MiB arena alone is 4096 pages; the checksum touched them all
    // through OUR handler.
    let served_pages = served.load(Ordering::SeqCst);
    assert!(
        served_pages >= 4096,
        "demand paging barely happened: {served_pages} pages served"
    );
    // Keep working post-restore: new writes fault through the handler too.
    let refilled = restored.cmd("fill 11 2048", "FILLED ");
    assert_eq!(restored.cmd("sum 2048", "SUM "), refilled);
    restored.cmd("off", "BYE");
}

/// The FULL blockd memory model under real Firecracker, via our FC patch
/// (`MemBackendType::UffdShmem`): guest memory is a `MAP_PRIVATE` mapping of
/// the handler-owned shared-memory file. One worked snapshot forks into
/// three VMs; every clean page exists ONCE physically (each fault filled
/// once, ever, and the shmem file's residency says exactly how many);
/// writes diverge per fork via copy-on-write without touching the base;
/// hole-punching the base is real backing reclaim — dirty fork state
/// survives, clean pages refault and refill through the handler. No
/// caveats left: fills, sharing, divergence, and reclaim are all ours.
#[test]
fn shmem_forks_share_one_copy_diverge_and_survive_reclaim() {
    use blockd_runtime::fc::ShmemServer;
    use std::sync::atomic::Ordering;

    let art = artifacts("shmem");
    let mut vm = boot_vm(&art, "base");
    vm.wait_line("READY");
    vm.cmd("fill 7 4096", "FILLED ");
    let base_sum = vm.cmd("sum 4096", "SUM ");
    let snap = art.scratch.join("base.vmstate");
    let mem = art.scratch.join("base.mem");
    vm.pause();
    vm.snapshot(&snap, &mem);
    vm.kill();

    // The handler owns the (sparse) shared base; every fork maps it.
    let mem_bytes = u64::from(MEM_MIB) * 1024 * 1024;
    let uffd_sock = art.scratch.join("shmem.sock");
    let shmem = art.scratch.join("base.shmem");
    let listener = std::os::unix::net::UnixListener::bind(&uffd_sock).expect("bind");
    let server = ShmemServer::start(listener, mem.clone(), &shmem, mem_bytes);

    let mut forks: Vec<FcVm> = (0..3)
        .map(|n| {
            let fork = FcVm::spawn(&art.fc, &art.scratch.join(format!("shm{n}.sock")));
            fork.load_snapshot_shmem(&snap, &uffd_sock, &shmem);
            fork
        })
        .collect();

    // Every fork carries the pre-fork work — every byte arrived through
    // OUR fill door on demand.
    for fork in &mut forks {
        assert_eq!(fork.cmd("sum 4096", "SUM "), base_sum);
    }
    // ONE physical copy: three forks touched ≥4096 arena pages each, yet
    // unique fills stayed far below 2× the arena — and the shmem file
    // holds exactly the filled pages, once.
    let filled = server.filled.load(Ordering::SeqCst);
    let faults = server.faults.load(Ordering::SeqCst);
    assert!(filled >= 4096, "the arena was not demand-filled: {filled}");
    assert!(
        filled < 2 * 4096,
        "forks are not sharing fills: {filled} unique fills for 3 forks"
    );
    assert_eq!(
        server.resident_bytes(),
        usize::try_from(filled).expect("fits") * 4096,
        "the base holds copies beyond one per filled page"
    );
    // Warm-page touches by later forks resolved from the page cache with
    // no fault at all — fault volume stays well under 3× the fills.
    assert!(
        faults < 3 * filled,
        "every fork faulted every page: {faults} faults vs {filled} fills"
    );

    // Divergence: private work per fork, isolated pairwise…
    let resident_before_divergence = server.resident_bytes();
    let filled_before_divergence = server.filled.load(Ordering::SeqCst);
    let mut fork_sums = Vec::new();
    for (n, fork) in forks.iter_mut().enumerate() {
        fork.cmd(&format!("mark 0 {}", 2000 + n), "MARKED");
        fork.cmd(&format!("fill {} 1024", 200 + n), "FILLED ");
        fork_sums.push(fork.cmd("sum 4096", "SUM "));
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
    let new_fills = server.filled.load(Ordering::SeqCst) - filled_before_divergence;
    assert_eq!(
        server.resident_bytes(),
        resident_before_divergence + usize::try_from(new_fills).expect("fits") * 4096,
        "divergence leaked into the shared base"
    );
    assert!(
        new_fills < 1024,
        "divergence wrote through to the base: {new_fills} pages appeared"
    );

    // Backing reclaim (R2.4/R2.7 under a real VMM): free the entire base.
    server.reclaim_all(mem_bytes);
    assert_eq!(server.resident_bytes(), 0, "reclaim left pages behind");
    // Every fork still has its exact diverged state: dirty CoW pages
    // survived the punch; clean pages refaulted and refilled through the
    // handler.
    let filled_before_refill = server.filled.load(Ordering::SeqCst);
    for (fork, expected) in forks.iter_mut().zip(&fork_sums) {
        assert_eq!(&fork.cmd("sum 4096", "SUM "), expected);
    }
    assert!(
        server.filled.load(Ordering::SeqCst) > filled_before_refill,
        "reclaim did not cause refills — nothing was actually freed"
    );
    for fork in forks {
        fork.kill();
    }
}
