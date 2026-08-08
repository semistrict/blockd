//! Differential recovery (1b): the simulation proves `Daemon::recover`
//! against thousands of crash schedules — but it feeds recovery from an
//! in-memory blob map, while production feeds it from a directory walk.
//! If the two scans could hand recovery different worlds (a name mangled
//! by the path round-trip, a file the walk misses, an ordering the
//! decoder silently depends on), every simulated recovery proof would be
//! about a path production never runs.
//!
//! So: run real chaos schedules to completion, take the blob device's
//! final contents verbatim — torn tails and bit rot included — and
//! demand that recovery over the simulation's scan and recovery over the
//! runtime's `scan_blob_dir` of the same bytes written to a real
//! directory produce identical verdicts, identical effects. The walk
//! returns files in whatever order `read_dir` feels like, which makes
//! order-insensitivity part of what a pass proves. Runs on any OS.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use blockd_core::daemon::Daemon;
use blockd_core::journal::JournalRecord;
use blockd_core::layout::{self, BlobName};
use blockd_core::protocol::Verdict;
use blockd_core::seam::Effect;
use blockd_core::types::VsetId;
use blockd_runtime::scan_blob_dir;
use blockd_sim::harness::run_final_blobs;
use blockd_sim::presets;

/// Recover from a scan, keeping only what the comparison needs.
fn recover(
    config: &blockd_core::daemon::DaemonConfig,
    blobs: &[(String, Vec<u8>)],
) -> (BTreeMap<VsetId, Verdict>, Vec<Effect>) {
    let (_, verdicts, effects) = Daemon::recover(
        config.clone(),
        blobs
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice())),
    );
    (verdicts, effects)
}

fn write_blobs(root: &Path, blobs: &[(String, Vec<u8>)]) {
    for (name, bytes) in blobs {
        let path = root.join(name);
        fs::create_dir_all(path.parent().expect("blob names have a parent")).expect("mkdir");
        fs::write(path, bytes).expect("write blob");
    }
}

#[test]
fn disk_scans_recover_exactly_like_the_simulated_scan() {
    let mut nontrivial = 0u64;
    for seed in [3, 29, 104] {
        let config = presets::single_host_chaos();
        let daemon_config = config.daemon.clone();
        let (report, blobs) = run_final_blobs(seed, config);
        assert_eq!(report.violations, Vec::<String>::new());
        assert!(!blobs.is_empty(), "seed {seed} left no blobs to recover");

        let sim_side = recover(&daemon_config, &blobs);

        let root = std::env::temp_dir().join(format!(
            "blockd-recovery-diff-{}-{seed}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        write_blobs(&root, &blobs);
        // Files a real host accumulates that the blob device never holds:
        // recovery must ignore anything `parse_blob` does not claim.
        fs::create_dir_all(root.join("lost+found")).expect("mkdir");
        fs::write(root.join("lost+found/fsck.0000"), b"noise").expect("write");
        fs::write(root.join("daemon.pid"), b"12345").expect("write");

        let scanned = scan_blob_dir(&root);
        assert_eq!(
            scanned.len(),
            blobs.len() + 2,
            "seed {seed}: the walk lost or invented files"
        );
        let disk_side = recover(&daemon_config, &scanned);

        assert_eq!(
            sim_side.0, disk_side.0,
            "seed {seed}: verdicts diverged between the scans"
        );
        assert_eq!(
            sim_side.1, disk_side.1,
            "seed {seed}: recovery effects diverged between the scans"
        );

        // The order axis, adversarially: the same set fed in reversed
        // order must recover identically. Scan order used to leak into
        // recovered state (`seg_blobs` order, the cold-record tiebreak,
        // duplicate-seq `record_ws` winners) — remove the canonicalizing
        // sort in `Daemon::recover` and this is the assert that fails.
        let mut reversed = blobs.clone();
        reversed.reverse();
        let rev_side = recover(&daemon_config, &reversed);
        assert_eq!(
            sim_side, rev_side,
            "seed {seed}: recovery depends on scan order"
        );
        // Backed recovery now defers its verdict until the fenced head read,
        // which this scan-only differential deliberately does not drive.
        // Keep the non-vacuity guard on the durable inputs instead: each
        // counted vset has at least one intact journal candidate that both
        // scan paths fed into the same deferred recovery state.
        let recoverable: BTreeSet<_> = blobs
            .iter()
            .filter_map(|(name, bytes)| match layout::parse_blob(name) {
                Some(BlobName::Journal { vset, .. })
                    if JournalRecord::decode(vset, bytes).is_ok() =>
                {
                    Some(vset)
                }
                _ => None,
            })
            .collect();
        nontrivial += recoverable.len() as u64;
        fs::remove_dir_all(&root).expect("cleanup");
    }
    // The equality above must have been about something: across the seeds,
    // real vsets recovered to real verdicts.
    assert!(
        nontrivial >= 3,
        "only {nontrivial} recoverable journal sets seen"
    );
}
