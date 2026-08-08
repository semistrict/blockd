//! Operator recovery for one lost peer-stashed primary. The peer's spool
//! directory may be copied to this host first; every byte is re-verified
//! against the fenced head before `install` claims ownership or writes target.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use blockd_core::head::HeadRecord;
use blockd_core::layout::{self, BlobName};
use blockd_core::replica_recovery::{
    ReplicaRecoveryReport, ReplicaRecoveryStatus, ReplicaResidue, export_replica_recovery,
    report_replica_recovery,
};
use blockd_core::types::{HostId, VsetId};
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore, install_replica_recovery};

struct Args {
    command: String,
    endpoint: String,
    metadata_endpoint: String,
    bucket: String,
    prefix: String,
    source: HostId,
    peer: HostId,
    vset: VsetId,
    residue_root: PathBuf,
    claimant: Option<HostId>,
    target: Option<PathBuf>,
}

fn usage() -> ! {
    eprintln!(
        "usage: peer_stash_recover <report|install> \
         --endpoint URL --metadata-endpoint URL --bucket NAME [--prefix PREFIX] \
         --source HOST --peer HOST --vset ID --residue-root PATH \
         [--claimant HOST --target PATH]"
    );
    std::process::exit(2)
}

fn parse_args() -> Args {
    let mut raw = std::env::args().skip(1);
    let command = raw.next().unwrap_or_else(|| usage());
    if !matches!(command.as_str(), "report" | "install") {
        usage();
    }
    let mut values = BTreeMap::new();
    while let Some(flag) = raw.next() {
        if !flag.starts_with("--") {
            usage();
        }
        values.insert(flag, raw.next().unwrap_or_else(|| usage()));
    }
    let take = |name: &str| values.get(name).cloned().unwrap_or_else(|| usage());
    let parse_host = |name: &str| HostId(take(name).parse::<u16>().unwrap_or_else(|_| usage()));
    let claimant = values
        .get("--claimant")
        .map(|value| HostId(value.parse::<u16>().unwrap_or_else(|_| usage())));
    let target = values.get("--target").map(PathBuf::from);
    if command == "install" && (claimant.is_none() || target.is_none()) {
        usage();
    }
    Args {
        command,
        endpoint: take("--endpoint"),
        metadata_endpoint: take("--metadata-endpoint"),
        bucket: take("--bucket"),
        prefix: values.get("--prefix").cloned().unwrap_or_default(),
        source: parse_host("--source"),
        peer: parse_host("--peer"),
        vset: VsetId(take("--vset").parse::<u64>().unwrap_or_else(|_| usage())),
        residue_root: take("--residue-root").into(),
        claimant,
        target,
    }
}

fn scan_files(base: &Path, directory: &Path, files: &mut Vec<(String, PathBuf)>) {
    let entries = std::fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read residue directory {}: {error}", directory.display()));
    for entry in entries {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            scan_files(base, &path, files);
        } else if let Ok(relative) = path.strip_prefix(base) {
            files.push((relative.to_string_lossy().into_owned(), path));
        }
    }
}

fn load_residues(args: &Args) -> Vec<(u64, Vec<u8>)> {
    let mut files = Vec::new();
    scan_files(&args.residue_root, &args.residue_root, &mut files);
    let mut generations: BTreeMap<u64, BTreeMap<u64, PathBuf>> = BTreeMap::new();
    for (name, path) in files {
        if let Some(BlobName::ReplicaSpool {
            source,
            vset,
            assignment_epoch,
            generation,
        }) = layout::parse_blob(&name)
            && (source, vset) == (args.source, args.vset)
        {
            generations
                .entry(assignment_epoch)
                .or_default()
                .insert(generation, path);
        }
    }
    generations
        .into_iter()
        .map(|(epoch, files)| {
            let mut bytes = Vec::new();
            for path in files.into_values() {
                bytes.extend(std::fs::read(path).expect("read residue spool"));
            }
            (epoch, bytes)
        })
        .collect()
}

fn report_json(report: &ReplicaRecoveryReport) -> String {
    let status = match report.status {
        ReplicaRecoveryStatus::Complete => "complete",
        ReplicaRecoveryStatus::Incomplete => "incomplete",
    };
    let number = |value: Option<u64>| value.map_or_else(|| "null".to_owned(), |v| v.to_string());
    let strings = |values: &[String]| {
        values
            .iter()
            .map(|value| format!("\"{value}\""))
            .collect::<Vec<_>>()
            .join(",")
    };
    format!(
        "{{\"status\":\"{status}\",\"chosen_peer\":{},\"assignment_epoch\":{},\
         \"covered_sync_through\":{},\"missing\":[{}],\"corrupt\":[{}]}}",
        number(report.chosen_source.map(|peer| u64::from(peer.0))),
        number(report.chosen_assignment_epoch),
        number(report.covered_sync_through),
        strings(&report.missing_objects),
        strings(&report.corrupt_objects),
    )
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = parse_args();
    let store: Arc<dyn ObjectStore> = Arc::new(GcsStore::new(GcsConfig {
        bucket: args.bucket.clone(),
        prefix: args.prefix.clone(),
        endpoint: args.endpoint.clone(),
        metadata_endpoint: args.metadata_endpoint.clone(),
    }));
    let (head_version, head_bytes) = store
        .clone()
        .get(layout::head_key(args.vset))
        .await
        .expect("head store available")
        .expect("fenced head exists");
    let head = HeadRecord::decode(args.vset, &head_bytes).expect("fenced head decodes");
    let owned = load_residues(&args);
    assert!(!owned.is_empty(), "no matching peer residue found");
    let residues = || {
        owned
            .iter()
            .map(|(assignment_epoch, bytes)| ReplicaResidue {
                peer: args.peer,
                assignment_epoch: *assignment_epoch,
                bytes,
            })
            .collect::<Vec<_>>()
    };
    let mut store_objects = BTreeMap::new();
    let report = loop {
        let report = report_replica_recovery(
            args.source,
            args.vset,
            head_version,
            &head,
            &residues(),
            &store_objects,
        );
        let mut fetched = false;
        for key in &report.missing_objects {
            if store_objects.contains_key(key) {
                continue;
            }
            if let Some((_, bytes)) = store
                .clone()
                .get(key.clone())
                .await
                .expect("store object read")
            {
                store_objects.insert(key.clone(), bytes);
                fetched = true;
            }
        }
        if !fetched {
            break report;
        }
    };
    println!("{}", report_json(&report));
    println!("{}", report.human_summary());
    if args.command == "report" {
        std::process::exit(i32::from(report.status != ReplicaRecoveryStatus::Complete));
    }
    assert_eq!(
        report.status,
        ReplicaRecoveryStatus::Complete,
        "recovery incomplete"
    );
    let export = export_replica_recovery(
        args.source,
        args.vset,
        head_version,
        &head,
        &residues(),
        &store_objects,
    )
    .expect("verified recovery export");
    let installed = install_replica_recovery(
        args.target.as_deref().expect("install target"),
        store,
        args.claimant.expect("claimant"),
        args.vset,
        head_version,
        &export,
    )
    .await
    .expect("fenced recovery install");
    assert!(installed.writer_fence > head_version);
    println!(
        "installed writer_fence={} head_version={}",
        installed.writer_fence, installed.head_version
    );
}
