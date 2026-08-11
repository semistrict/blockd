//! Per-host demo daemon with object storage, peer transport, Firecracker, and
//! an HTTP control API.
//!
//! Usage: `demod <config>` or `demod fake-gcs <addr>`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(target_os = "linux")]
mod api;
#[cfg(any(target_os = "linux", test))]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod config;
#[cfg(any(target_os = "linux", test))]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod observability;
#[cfg(target_os = "linux")]
mod vm;

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("demod is Linux-only (userfaultfd, Firecracker)");
    std::process::exit(2);
}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() {
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
            let _telemetry = observability::init(None);
            if let Some(ms) = args.get(3) {
                let ms: u64 = ms.parse().expect("latency ms");
                fake.latency_ms
                    .store(ms, std::sync::atomic::Ordering::SeqCst);
                tracing::info!(latency_ms = ms, "fake GCS latency configured");
            }
            tracing::info!(%endpoint, "fake GCS serving");
            std::future::pending::<()>().await;
        }
        Some(path) => {
            let cfg = config::DemodConfig::load(path);
            let _telemetry = observability::init(Some(cfg.host.0));
            let state = Arc::new(vm::Demod::start(cfg));
            api::serve(state).await;
        }
        None => {
            let _telemetry = observability::init(None);
            tracing::error!("usage: demod <config-file> | demod fake-gcs <addr>");
            std::process::exit(2);
        }
    }
}
