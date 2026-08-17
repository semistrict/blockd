//! Linux end-to-end test for post-copy migration over TCP, including durable
//! handoff, hydration, release, and source reclamation.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use blockd_core::hostmeta::{HostConfig, ReplicaPlacementConfig};
use blockd_core::journal::VsetConfig;
use blockd_core::placement::PeerCandidate;
use blockd_core::protocol::Verdict;
use blockd_core::types::{HostId, PageId, PageNo, VolumeId, VolumeIdx, VsetId, millis};
use blockd_runtime::{PeerConfig, Runtime, RuntimeConfig};
use blockd_workload::{Backend, Capability, LogicalPage, Operation, VerifyScope, WorkloadModel};

mod support;

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
        daemon: HostConfig {
            archive: blockd_core::hostmeta::ArchivePolicy::default(),
            host: HostId(host),
            cache_pages: 256,
            writeback_interval: millis(5),
            backup_retry: millis(20),
            disk_capacity: None,
            disk_headroom: 0,
            wedge_ticks: 500,
            replica_placement: Some(ReplicaPlacementConfig {
                membership_epoch: 1,
                local_failure_domain: host + 1,
                roster: (0..2)
                    .map(|candidate| PeerCandidate {
                        host: HostId(candidate),
                        weight: 1,
                        failure_domain: candidate + 1,
                        drained: false,
                    })
                    .collect(),
                authority: None,
            }),
        },
        blob_dir: temp_dir(tag),
        peer: Some(peer),
    }
}

struct MigrationBackend<'a> {
    hosts: [&'a Runtime; 2],
    current: usize,
    config: VsetConfig,
    pause: Duration,
    migrations: u64,
    verified_model: Option<WorkloadModel>,
}

impl<'a> MigrationBackend<'a> {
    fn new(a: &'a Runtime, b: &'a Runtime, config: VsetConfig) -> Self {
        Self {
            hosts: [a, b],
            current: 0,
            config,
            pause: Duration::ZERO,
            migrations: 0,
            verified_model: None,
        }
    }

    fn runtime(&self) -> &Runtime {
        self.hosts[self.current]
    }

    fn page_id(page: LogicalPage) -> PageId {
        pid(page.volume, page.page)
    }

    async fn verify_page(&self, page: LogicalPage, expected: u64) -> Result<(), String> {
        let bytes = self.runtime().guest_read(VSET, Self::page_id(page)).await;
        let actual = u64::from_le_bytes(bytes[0..8].try_into().expect("sized"));
        if actual != expected {
            return Err(format!(
                "{page:?}: observed {actual:#x}, expected {expected:#x}"
            ));
        }
        if bytes[8..].iter().any(|&byte| byte != 0) {
            return Err(format!("{page:?}: fill tail corrupted"));
        }
        Ok(())
    }

    async fn verify(&self, model: &WorkloadModel, scope: VerifyScope) -> Result<(), String> {
        for (page, expected) in model.pages(scope) {
            self.verify_page(page, expected).await?;
        }
        Ok(())
    }
}

impl Backend for MigrationBackend<'_> {
    type Error = String;

    fn supports(&self, capability: Capability) -> bool {
        matches!(
            capability,
            Capability::Create
                | Capability::Data
                | Capability::Sync
                | Capability::Migrate
                | Capability::Verify
        )
    }

    async fn execute(
        &mut self,
        operation: Operation,
        model: &WorkloadModel,
    ) -> Result<(), Self::Error> {
        match operation {
            Operation::Create => self.hosts[0].create_vset(VSET, self.config).await,
            Operation::Read { page } => self.verify_page(page, model.expected(page)).await?,
            Operation::Write { page, value } => {
                self.runtime()
                    .guest_write(VSET, Self::page_id(page), value)
                    .await;
            }
            Operation::Sync { volume } => {
                if !self.runtime().guest_sync(VSET, VolumeIdx(volume)).await {
                    return Err(format!("sync rejected for volume {volume}"));
                }
            }
            Operation::Migrate { to_host } => {
                let destination = usize::from(to_host);
                if self.current != 0 || destination != 1 {
                    return Err(format!(
                        "unsupported migration route {} -> {destination}",
                        self.current
                    ));
                }
                self.hosts[destination].expect_migration(VSET, self.config);
                let started = Instant::now();
                self.hosts[self.current]
                    .migrate_out(VSET, HostId(to_host))
                    .await;
                let verdict = self.hosts[destination].wait_migrated_in(VSET).await;
                self.pause = started.elapsed();
                if !matches!(verdict, Verdict::Resume { .. }) {
                    return Err(format!("migration verdict was {verdict:?}"));
                }
                self.current = destination;
                self.migrations += 1;
            }
            Operation::Verify { scope } => {
                self.verify(model, scope).await?;
                self.verified_model = Some(model.clone());
            }
            _ => unreachable!("capability checked before execution"),
        }
        Ok(())
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

/// A worked vset moves from host A to host B over TCP: verdict
/// resumes on B, the same workload continues there, EVERY disk byte
/// verifies on B (demand fetches over the wire), hydration drains the
/// tail, the source releases and reclaims to zero blobs.
#[tokio::test(flavor = "current_thread")]
async fn migration_moves_a_worked_vset_between_real_runtimes_over_tcp() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let addr_a = free_addr();
            let addr_b = free_addr();
            let peer_a = PeerConfig { listen: addr_a };
            let peer_b = PeerConfig { listen: addr_b };
            let test_gcs = support::test_gcs("migrate").await;
            let store = test_gcs.store.clone();
            let a = Runtime::new(&runtime_config("host-a", 0, peer_a), store.clone()).await;
            let b = Runtime::new(&runtime_config("host-b", 1, peer_b), store.clone()).await;

            let spec = blockd_workload::load("migration").expect("migration workload");
            let vc = VsetConfig::compute(spec.shape.disk_volumes, spec.shape.pages_per_volume);
            let mut backend = MigrationBackend::new(&a, &b, vc);
            let outcome = blockd_workload::run(&spec, &mut backend)
                .await
                .expect("migration workload");

            // R7.1: the whole guest-observed handoff stays far inside 500ms.
            assert!(
                backend.pause < Duration::from_millis(500),
                "handoff took {:?}",
                backend.pause
            );
            assert_eq!(backend.migrations, outcome.migrations);

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
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // Everything still verifies afterwards, from B's own storage alone.
            backend
                .verify(
                    backend
                        .verified_model
                        .as_ref()
                        .expect("workload performed verification"),
                    VerifyScope::Disk,
                )
                .await
                .expect("destination remains readable");
            assert_eq!(a.incidents(), Vec::<String>::new());
            assert_eq!(b.incidents(), Vec::<String>::new());
            // The wire really carried the drain. Foreground demand may win the race
            // with background hydration, so either path is valid post-copy progress.
            let peer_faults = b
                .fault_latency()
                .into_iter()
                .filter(|series| series.vset == VSET && series.source == "peer")
                .map(|series| series.histogram.count)
                .sum::<u64>();
            assert!(
                b.counters().hydrate_fills + peer_faults > 0,
                "no post-copy page crossed the peer wire"
            );
            assert!(
                test_gcs.fake.request_count() > 0,
                "migration must retain the store recovery path"
            );
        })
        .await;
}
