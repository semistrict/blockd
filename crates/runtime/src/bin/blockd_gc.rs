use std::sync::Arc;
use std::time::Duration;

use blockd_runtime::cluster::GcsStoreUri;
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore, StoreCollector};

fn usage() -> ! {
    eprintln!(
        "usage: blockd-gc gs://BUCKET/PREFIX [--interval-secs SECONDS] [--grace-secs SECONDS] [--endpoint URL] [--metadata-endpoint URL]"
    );
    std::process::exit(2);
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let store = args.next().unwrap_or_else(|| usage());
    let mut interval = Duration::from_mins(1);
    let mut grace = Duration::from_mins(10);
    let mut endpoint = "https://storage.googleapis.com".to_owned();
    let mut metadata_endpoint = "http://metadata.google.internal".to_owned();
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--interval-secs" => {
                interval = Duration::try_from_secs_f64(value.parse().unwrap_or_else(|_| usage()))
                    .unwrap_or_else(|_| usage());
            }
            "--grace-secs" => {
                grace = Duration::try_from_secs_f64(value.parse().unwrap_or_else(|_| usage()))
                    .unwrap_or_else(|_| usage());
            }
            "--endpoint" => endpoint = value,
            "--metadata-endpoint" => metadata_endpoint = value,
            _ => usage(),
        }
    }
    if interval.is_zero() || grace.is_zero() {
        usage();
    }
    let uri = GcsStoreUri::parse(&store).unwrap_or_else(|error| {
        eprintln!("{error}");
        usage();
    });
    let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
        bucket: uri.bucket,
        prefix: uri.prefix,
        endpoint,
        metadata_endpoint,
    }));
    let mut collector = StoreCollector::new(store);
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.expect("install signal handler");
                return;
            }
            () = tokio::time::sleep(interval) => {
                match collector.pass(grace).await {
                    Ok(deleted) => eprintln!("collector pass deleted {deleted} objects"),
                    Err(error) => eprintln!("collector pass failed: {error:?}"),
                }
            }
        }
    }
}
