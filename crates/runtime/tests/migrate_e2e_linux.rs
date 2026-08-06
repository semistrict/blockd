//! Live post-copy migration between two REAL runtimes over REAL TCP: the
//! exact protocol the deterministic simulation proved — offer, two-sided
//! durable handoff, demand fetches from the source, background hydration,
//! release, source reclamation — now with the wire in between. Every byte
//! the destination serves is checked against the workload's model.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockd_core::daemon::DaemonConfig;
use blockd_core::journal::VsetConfig;
use blockd_core::seam::Verdict;
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};
use blockd_runtime::{PeerConfig, Runtime, RuntimeConfig, S3Store};

const VSET: VsetId = VsetId(1);

fn pid(idx: u8, page: u32) -> PageId {
    PageId {
        volume: VolumeId {
            vset: VSET,
            idx: VolumeIdx(idx),
        },
        page: PageNo(page),
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("blockd-mig-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

fn runtime_config(tag: &str, host: u16, peer: PeerConfig) -> RuntimeConfig {
    RuntimeConfig {
        daemon: DaemonConfig {
            host: HostId(host),
            cache_pages: 256,
            writeback_interval: millis(5),
            backup_retry: millis(20),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 500,
        },
        blob_dir: temp_dir(tag),
        peer: Some(peer),
    }
}

/// The same seeded workload model as `e2e_linux.rs`, retargetable at
/// whichever runtime currently runs the vset.
struct Workload {
    lcg: u64,
    config: VsetConfig,
    model: BTreeMap<PageId, u64>,
}

impl Workload {
    fn new(seed: u64, config: VsetConfig) -> Workload {
        Workload {
            lcg: seed.max(1),
            config,
            model: BTreeMap::new(),
        }
    }

    fn next(&mut self) -> u64 {
        self.lcg = self
            .lcg
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.lcg
    }

    fn random_page(&mut self) -> PageId {
        let idx =
            u8::try_from(self.next() % u64::from(self.config.disk_volumes) + 1).expect("fits");
        let page =
            u32::try_from(self.next() % u64::from(self.config.pages_per_volume)).expect("fits");
        pid(idx, page)
    }

    fn step(&mut self, rt: &Runtime, op: u64) {
        let page = self.random_page();
        if self.next().is_multiple_of(4) {
            self.verify_page(rt, page);
        } else {
            let value = 0x1000_0000 + op;
            rt.guest_write(VSET, page, value);
            self.model.insert(page, value);
        }
    }

    fn verify_page(&self, rt: &Runtime, page: PageId) {
        let bytes = rt.guest_read(VSET, page);
        let want = self.model.get(&page).copied().unwrap_or(0);
        assert_eq!(
            u64::from_le_bytes(bytes[0..8].try_into().expect("sized")),
            want,
            "{page:?}: word 0 diverged from the model"
        );
        assert!(
            bytes[8..].iter().all(|&b| b == 0),
            "{page:?}: fill tail corrupted"
        );
    }

    fn verify_all(&self, rt: &Runtime) {
        for idx in 1..=self.config.disk_volumes {
            for page in 0..self.config.pages_per_volume {
                self.verify_page(rt, pid(idx, page));
            }
        }
    }
}

fn files_under(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let path = e.path();
            if path.is_dir() { files_under(&path) } else { 1 }
        })
        .sum()
}

/// A worked non-backed vset moves from host A to host B over TCP: verdict
/// resumes on B, the same workload continues there, EVERY disk byte
/// verifies on B (demand fetches over the wire), hydration drains the
/// tail, the source releases and reclaims to zero blobs.
#[test]
fn migration_moves_a_worked_vset_between_real_runtimes_over_tcp() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let roster: BTreeMap<HostId, SocketAddr> = [(HostId(0), addr_a), (HostId(1), addr_b)]
        .into_iter()
        .collect();
    let peer_a = PeerConfig {
        listen: addr_a,
        peers: roster.clone(),
    };
    let peer_b = PeerConfig {
        listen: addr_b,
        peers: roster,
    };
    let store = Arc::new(S3Store::new());
    let a = Runtime::new(&runtime_config("host-a", 0, peer_a), store.clone());
    let b = Runtime::new(&runtime_config("host-b", 1, peer_b), store.clone());

    // A non-backed vset (the mode that must migrate, R7.2) does real work
    // on A — enough distinct pages that the post-copy tail is nontrivial.
    let vc = VsetConfig {
        disk_volumes: 2,
        pages_per_volume: 64,
        backed_up: false,
    };
    a.create_vset(VSET, vc);
    let mut workload = Workload::new(0xB10C_D001, vc);
    for op in 0..600 {
        workload.step(&a, op);
        if op % 97 == 0 {
            assert!(a.guest_sync(VSET, VolumeIdx(1)), "sync on A");
        }
    }

    // Hand off: B expects the inbound vset BEFORE the offer can arrive.
    b.expect_migration(VSET, vc);
    let paused = Instant::now();
    a.migrate_out(VSET, HostId(1));
    let verdict = b.wait_migrated_in(VSET);
    let pause = paused.elapsed();
    assert!(
        matches!(verdict, Verdict::Resume { .. }),
        "migration verdict was {verdict:?}"
    );
    // R7.1: the whole guest-observed handoff stays far inside 500ms.
    assert!(pause < Duration::from_millis(500), "handoff took {pause:?}");

    // The SAME workload model continues on B: post-migration writes land
    // on the destination, reads verify against everything A ever wrote —
    // served by demand fetches from A over real TCP until hydrated.
    for op in 600..900 {
        workload.step(&b, op);
        if op % 97 == 0 {
            assert!(b.guest_sync(VSET, VolumeIdx(1)), "sync on B");
        }
    }
    workload.verify_all(&b);

    // Hydration drains the tail without the guest's help; the destination
    // releases the source, and the source reclaims the vset to NOTHING.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let blobs = files_under(a.blob_dir());
        if blobs == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "source never reclaimed: {blobs} blobs remain"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Everything still verifies afterwards, from B's own storage alone.
    workload.verify_all(&b);
    assert_eq!(a.incidents(), Vec::<String>::new());
    assert_eq!(b.incidents(), Vec::<String>::new());
    // The wire really carried the drain (hydration pulled pages from A).
    assert!(
        b.counters().hydrate_fills > 0,
        "no hydration happened over the wire"
    );
    // A non-backed vset never touched the store (R4.4).
    let (_, keys) = (0, store.s3.stats.total_requests());
    assert_eq!(keys, 0, "non-backed migration must not touch the store");
}
