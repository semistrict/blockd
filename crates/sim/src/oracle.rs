//! The oracle: ghost truth for every vset, kept entirely outside the daemon
//! and checked only through the guest boundary. It knows every write the
//! guest ever made (per volume, in order), every acknowledged sync, and
//! validates:
//!
//! - every served byte against the expected pattern (R8.1: no invented
//!   pages; R1.2: no mixed epochs — a stale or foreign page breaks the
//!   pattern comparison);
//! - every resume against replay of the history up to the checkpoint's
//!   vmstate (R1.2);
//! - every cold boot against some crash-consistent prefix of each disk
//!   volume's write history, at least as new as its last acknowledged sync
//!   (R3.8, R3.7).

use std::collections::BTreeMap;

use blockd_core::journal::VsetConfig;
use blockd_core::types::{PageId, VolumeId, VsetId};

use crate::guest::{claimed_vol_seq, page_pattern};

#[derive(Clone, Debug)]
struct HistEntry {
    /// Guest op number that performed this write (0 after renumbering for
    /// writes that predate the current boot).
    op_index: u64,
    page: PageId,
}

#[derive(Debug, Default)]
struct VolumeGhost {
    /// Every write ever applied, in volume order; entry `i` is `vol_seq`
    /// `i + 1`.
    history: Vec<HistEntry>,
    /// Write count covered by the newest acknowledged sync (R3.8).
    acked: u64,
}

#[derive(Debug)]
struct VsetGhost {
    config: VsetConfig,
    volumes: BTreeMap<VolumeId, VolumeGhost>,
    /// Current expected `vol_seq` per page (absent = zero page).
    current: BTreeMap<PageId, u64>,
    /// Collected claims during a cold-boot fsck.
    cold_claims: BTreeMap<PageId, u64>,
    /// A cold-boot fsck aborted (sanctioned death): ghost truth for this
    /// vset is indeterminate until a complete cold fsck re-infers it, so
    /// checks are suspended rather than reporting phantom violations.
    tainted: bool,
    /// A disk-inference pass started but has not completed: the disk ghost
    /// is a superset of reality until a full fsck re-infers it. A crash can
    /// interrupt an fsck at any point, so even a Resume verdict must run its
    /// verification pass in inference mode while this is set.
    disk_unresolved: bool,
    /// The next recovery may legitimately roll back acknowledged syncs:
    /// restore after host loss is bounded by the backup lag (R4.3), not by
    /// local sync durability.
    sync_loss_ok: bool,
}

impl VsetGhost {
    fn rebuild_current(&mut self) {
        self.current.clear();
        for ghost in self.volumes.values() {
            for (i, entry) in ghost.history.iter().enumerate() {
                self.current.insert(entry.page, i as u64 + 1);
            }
        }
    }
}

pub struct Oracle {
    vsets: BTreeMap<VsetId, VsetGhost>,
    pub violations: Vec<String>,
}

impl Oracle {
    pub fn new() -> Oracle {
        Oracle {
            vsets: BTreeMap::new(),
            violations: Vec::new(),
        }
    }

    fn violate(&mut self, message: String) {
        self.violations.push(message);
    }

    pub fn register(&mut self, vset: VsetId, config: VsetConfig) {
        let mut volumes = BTreeMap::new();
        for volume in config.volumes(vset) {
            volumes.insert(volume, VolumeGhost::default());
        }
        self.vsets.insert(
            vset,
            VsetGhost {
                config,
                volumes,
                current: BTreeMap::new(),
                cold_claims: BTreeMap::new(),
                tainted: false,
                disk_unresolved: false,
                sync_loss_ok: false,
            },
        );
    }

    /// The next write to this volume will be this `vol_seq`.
    pub fn next_vol_seq(&self, volume: VolumeId) -> u64 {
        let ghost = &self.vsets[&volume.vset];
        ghost.volumes[&volume].history.len() as u64 + 1
    }

    pub fn on_write_ok(&mut self, page: PageId, vol_seq: u64, op_index: u64) {
        let ghost = self.vsets.get_mut(&page.volume.vset).expect("registered");
        let vol = ghost.volumes.get_mut(&page.volume).expect("registered");
        assert_eq!(
            vol_seq,
            vol.history.len() as u64 + 1,
            "guest issued writes out of order"
        );
        vol.history.push(HistEntry { op_index, page });
        ghost.current.insert(page, vol_seq);
    }

    pub fn on_sync_ok(&mut self, volume: VolumeId) {
        let ghost = self.vsets.get_mut(&volume.vset).expect("registered");
        let vol = ghost.volumes.get_mut(&volume).expect("registered");
        vol.acked = vol.history.len() as u64;
    }

    /// Validate a fill — the only path by which storage bytes enter guest
    /// memory (R8.1). During a cold-boot fsck, disk-volume fills instead
    /// *infer* the recovered state (collected for
    /// [`Oracle::finish_cold_boot`]); everything else must match ghost truth
    /// exactly.
    pub fn check_fill(&mut self, page: PageId, bytes: &[u8], cold_fsck: bool) {
        let ghost = self.vsets.get_mut(&page.volume.vset).expect("registered");
        if ghost.tainted {
            return;
        }
        if cold_fsck && !page.volume.idx.is_memory() {
            let claimed = claimed_vol_seq(bytes);
            if bytes != page_pattern(page, claimed).as_slice() {
                self.violate(format!(
                    "R8.1: {page:?} served bytes matching no write (claimed seq {claimed})"
                ));
                return;
            }
            self.vsets
                .get_mut(&page.volume.vset)
                .expect("registered")
                .cold_claims
                .insert(page, claimed);
            return;
        }
        let expected = ghost.current.get(&page).copied().unwrap_or(0);
        if bytes != page_pattern(page, expected).as_slice() {
            let claimed = claimed_vol_seq(bytes);
            self.violate(format!(
                "R8.1/R1.2: {page:?} expected seq {expected}, got bytes claiming {claimed}"
            ));
        }
    }

    /// An unservable page killed a guest. Only sanctioned when injected
    /// damage actually touched this vset's blobs (R8.1's loud failure).
    pub fn on_fill_failed(&mut self, page: PageId, sanctioned: bool) {
        if !sanctioned {
            self.violate(format!(
                "R8.1: {page:?} unservable without any injected damage"
            ));
        }
    }

    /// Resume at a checkpoint (R1.2): roll ghost truth back to exactly the
    /// ops the checkpoint's vmstate covers, and verify no acknowledged sync
    /// is being rolled back (R3.8).
    pub fn on_resume(&mut self, vset: VsetId, vmstate: u64) {
        let ghost = self.vsets.get_mut(&vset).expect("registered");
        let sanctioned = ghost.sync_loss_ok;
        let tainted = ghost.tainted || sanctioned;
        // The sanction covers this whole restore: if disk truth is still
        // unresolved (a prior fsck never finished), the inference pass that
        // follows this resume is part of the same restore and inherits it.
        ghost.sync_loss_ok = sanctioned && ghost.disk_unresolved;
        for (volume, vol) in &mut ghost.volumes {
            vol.history.retain(|e| e.op_index <= vmstate);
            if tainted {
                vol.acked = vol.acked.min(vol.history.len() as u64);
            }
            if !tainted && vol.acked > vol.history.len() as u64 {
                self.violations.push(format!(
                    "R3.8: resume of {vset:?} rolls {volume:?} back past an acked sync \
                     ({} > {} writes)",
                    vol.acked,
                    vol.history.len()
                ));
            }
        }
        let ghost = self.vsets.get_mut(&vset).expect("registered");
        ghost.rebuild_current();
        ghost.cold_claims.clear();
    }

    /// Cold boot starts (R3.7): memory is invalid and expected to read as
    /// zeros; disk state is inferred by the fsck pass. Every surviving write
    /// predates the new boot, so it belongs to op 0 immediately — an
    /// interrupted fsck must never leave stale op numbering behind.
    pub fn start_cold_boot(&mut self, vset: VsetId) {
        let ghost = self.vsets.get_mut(&vset).expect("registered");
        ghost.disk_unresolved = true;
        for (volume, vol) in &mut ghost.volumes {
            if volume.idx.is_memory() {
                vol.history.clear();
                vol.acked = 0;
            } else {
                for entry in &mut vol.history {
                    entry.op_index = 0;
                }
            }
        }
        let ghost = self.vsets.get_mut(&vset).expect("registered");
        ghost.cold_claims.clear();
        ghost.rebuild_current();
    }

    /// A cold-boot fsck died before covering every page: ghost truth for
    /// this vset is permanently indeterminate (this only happens after
    /// sanctioned damage), so its checks are retired rather than risking
    /// phantom violations.
    pub fn on_fsck_aborted(&mut self, vset: VsetId) {
        self.vsets.get_mut(&vset).expect("registered").tainted = true;
    }

    /// Cold-boot fsck finished: every disk volume must be a crash-consistent
    /// prefix of its own write history (R3.8), at least as new as its last
    /// acknowledged sync, and ghost truth continues from that prefix.
    pub fn finish_cold_boot(&mut self, vset: VsetId) {
        let ghost = self.vsets.get_mut(&vset).expect("registered");
        if ghost.tainted {
            ghost.cold_claims.clear();
            return;
        }
        let claims = std::mem::take(&mut ghost.cold_claims);
        let sync_loss_ok = ghost.sync_loss_ok;
        ghost.sync_loss_ok = false;
        let volumes: Vec<VolumeId> = ghost.volumes.keys().copied().collect();
        let config = ghost.config;
        for volume in volumes {
            if volume.idx.is_memory() {
                continue;
            }
            let vol_claims: BTreeMap<PageId, u64> = claims
                .iter()
                .filter(|(p, _)| p.volume == volume)
                .map(|(p, s)| (*p, *s))
                .collect();
            assert_eq!(
                vol_claims.len(),
                config.pages_per_volume as usize,
                "complete fsck implies complete claims"
            );
            let k = vol_claims.values().copied().max().unwrap_or(0);
            let ghost = self.vsets.get_mut(&vset).expect("registered");
            let vol = ghost.volumes.get_mut(&volume).expect("registered");
            if k > vol.history.len() as u64 {
                self.violations.push(format!(
                    "R3.8: {volume:?} recovered to write {k}, but only {} writes ever happened",
                    vol.history.len()
                ));
                continue;
            }
            if k < vol.acked && !sync_loss_ok {
                self.violations.push(format!(
                    "R3.8: {volume:?} recovered to write {k}, older than acked sync at {}",
                    vol.acked
                ));
            }
            // The recovered image must equal replay of exactly the first k
            // writes — a crash-consistent point of this device's history.
            let mut expected: BTreeMap<PageId, u64> = BTreeMap::new();
            for (i, entry) in vol.history[..usize::try_from(k).expect("fits")]
                .iter()
                .enumerate()
            {
                expected.insert(entry.page, i as u64 + 1);
            }
            for (page, claimed) in &vol_claims {
                let want = expected.get(page).copied().unwrap_or(0);
                if *claimed != want {
                    self.violations.push(format!(
                        "R3.8: {page:?} recovered at seq {claimed}, replay({k}) says {want}"
                    ));
                }
            }
            // Truth continues from the prefix: later writes never happened,
            // and pre-boot writes belong to op 0.
            vol.history.truncate(usize::try_from(k).expect("fits"));
            for entry in &mut vol.history {
                entry.op_index = 0;
            }
            vol.acked = vol.acked.min(k);
        }
        let ghost = self.vsets.get_mut(&vset).expect("registered");
        ghost.disk_unresolved = false;
        ghost.rebuild_current();
    }

    /// The next recovery of this vset is a restore from backup: sync
    /// rollback up to the backup lag is sanctioned (R4.3).
    pub fn allow_sync_loss(&mut self, vset: VsetId) {
        self.vsets.get_mut(&vset).expect("registered").sync_loss_ok = true;
    }

    /// True while the disk ghost awaits a completed inference pass: the
    /// harness must run the next boot fsck in inference mode even when the
    /// daemon's verdict is Resume.
    pub fn needs_disk_inference(&self, vset: VsetId) -> bool {
        self.vsets[&vset].disk_unresolved
    }

    /// Total completed writes (test assertions).
    pub fn write_count(&self, vset: VsetId) -> u64 {
        self.vsets[&vset]
            .volumes
            .values()
            .map(|v| v.history.len() as u64)
            .sum()
    }
}

impl Default for Oracle {
    fn default() -> Oracle {
        Oracle::new()
    }
}

// Negative tests: the oracle must CATCH broken behavior — a checker that
// never fires is worse than none.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::guest::page_pattern;
    use blockd_core::types::{PageNo, VolumeIdx};

    const VSET: VsetId = VsetId(9);

    fn config() -> VsetConfig {
        VsetConfig::compute(1, 2, false)
    }

    fn page(volume: u8, n: u32) -> PageId {
        PageId {
            volume: VolumeId {
                vset: VSET,
                idx: VolumeIdx(volume),
            },
            page: PageNo(n),
        }
    }

    fn oracle_with_two_writes() -> Oracle {
        let mut o = Oracle::new();
        o.register(VSET, config());
        o.on_write_ok(page(1, 0), 1, 1);
        o.on_write_ok(page(1, 1), 2, 2);
        o
    }

    #[test]
    fn correct_reads_pass_and_wrong_reads_are_caught() {
        let mut o = oracle_with_two_writes();
        o.check_fill(page(1, 0), &page_pattern(page(1, 0), 1), false);
        assert_eq!(o.violations, Vec::<String>::new());
        // Stale content (the zero page) must fire R8.1/R1.2.
        o.check_fill(page(1, 0), &page_pattern(page(1, 0), 0), false);
        assert_eq!(
            o.violations,
            ["R8.1/R1.2: v9/1p0 expected seq 1, got bytes claiming 0"]
        );
    }

    #[test]
    fn resume_past_an_acked_sync_is_caught() {
        let mut o = oracle_with_two_writes();
        o.on_sync_ok(VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        });
        // A checkpoint whose vmstate predates the second (synced) write.
        o.on_resume(VSET, 1);
        assert_eq!(
            o.violations,
            ["R3.8: resume of VsetId(9) rolls v9/1 back past an acked sync (2 > 1 writes)"]
        );
    }

    #[test]
    fn cold_boot_to_a_non_prefix_state_is_caught() {
        let mut o = oracle_with_two_writes();
        o.start_cold_boot(VSET);
        // Recovered image claims write 2 happened but write 1 vanished —
        // not a crash-consistent point of the volume's history.
        o.check_fill(page(1, 0), &page_pattern(page(1, 0), 0), true);
        o.check_fill(page(1, 1), &page_pattern(page(1, 1), 2), true);
        o.finish_cold_boot(VSET);
        assert_eq!(
            o.violations,
            ["R3.8: v9/1p0 recovered at seq 0, replay(2) says 1"]
        );
    }

    #[test]
    fn cold_boot_older_than_an_acked_sync_is_caught() {
        let mut o = oracle_with_two_writes();
        o.on_sync_ok(VolumeId {
            vset: VSET,
            idx: VolumeIdx(1),
        });
        o.start_cold_boot(VSET);
        // Recovered image rolls back to before either acked write.
        o.check_fill(page(1, 0), &page_pattern(page(1, 0), 0), true);
        o.check_fill(page(1, 1), &page_pattern(page(1, 1), 0), true);
        o.finish_cold_boot(VSET);
        assert_eq!(
            o.violations,
            ["R3.8: v9/1 recovered to write 0, older than acked sync at 2"]
        );
    }

    #[test]
    fn unsanctioned_unservable_pages_are_caught() {
        let mut o = oracle_with_two_writes();
        o.on_fill_failed(page(1, 0), true);
        assert_eq!(o.violations, Vec::<String>::new());
        o.on_fill_failed(page(1, 0), false);
        assert_eq!(
            o.violations,
            ["R8.1: v9/1p0 unservable without any injected damage"]
        );
    }
}
