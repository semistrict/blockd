//! demod: the per-host demo daemon — one blockd `Runtime` (GCS or a
//! local fake store), the peer TCP transport, real Firecracker microVMs
//! served from store-held snapshots, and a small HTTP control API:
//!
//!   POST /base                     bake the base snapshot into the store
//!   POST /vm?backed=0|1            start a VM from the base + paired vset
//!   POST /vm/{id}/work?bursts=N    guest work, mirrored into the vset
//!   POST /vm/{id}/verify           re-verify the vset against its model
//!   POST /vm/{id}/fork?n=N         snapshot the VM, start N forks off it
//!   POST /vm/{id}/expect           (destination) accept a migration
//!   POST /vm/{id}/migrate?to=H     live-migrate the vset; VM re-restores
//!   POST /vm/{id}/restore          (after host death) backed vset restore
//!   GET  /status                   vms, counters, store bill
//!
//! Usage: `demod <config>` or `demod fake-gcs <addr>`.
//!
//! Each VM's durable state is a daemon-managed vset (checkpointed,
//! backed up, live-migrated — the storage story). VM RAM divergence is
//! FC-level only in this demo (stated limitation; the `MAP_SHARED` fill
//! backend is a later milestone).

// The demo daemon is the nondeterministic side of the seam, like the
// runtime: threads and wall time are the implementation.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(target_os = "linux")]
mod api;
#[cfg(target_os = "linux")]
mod config;
#[cfg(target_os = "linux")]
mod vm;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("demod is Linux-only (userfaultfd, Firecracker)");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
fn main() {
    use std::sync::Arc;

    use blockd_runtime::fakegcs::FakeGcs;

    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("fake-gcs") => {
            let addr = args
                .get(2)
                .expect("usage: demod fake-gcs <addr>")
                .parse()
                .expect("addr");
            let (fake, endpoint) = FakeGcs::start_on(addr);
            if let Some(ms) = args.get(3) {
                let ms: u64 = ms.parse().expect("latency ms");
                fake.latency_ms
                    .store(ms, std::sync::atomic::Ordering::SeqCst);
                eprintln!("fake-gcs latency {ms}ms");
            }
            eprintln!("fake-gcs serving on {endpoint}");
            loop {
                std::thread::park();
            }
        }
        Some(path) => {
            let cfg = config::DemodConfig::load(path);
            let state = Arc::new(vm::Demod::start(cfg));
            api::serve(&state);
        }
        None => {
            eprintln!("usage: demod <config-file> | demod fake-gcs <addr>");
            std::process::exit(2);
        }
    }
}
