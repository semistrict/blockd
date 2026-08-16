//! Deterministic fault injection points shared by actors and the nemesis.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::rng::Ppm;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum FaultPoint {
    ReplicaRetryTimer = 0,
    DuplicateAck = 1,
    StatusReconciliation = 2,
    ReleaseOverlap = 3,
    AssignmentCasRace = 4,
    StoreUnknownResult = 5,
    RestartScan = 6,
    CrashPeerAfterCommitBeforeAck = 7,
    CrashPrimaryAfterAckBeforeSyncOk = 8,
    CrashPrimaryAfterSyncOk = 9,
    CrashPrimaryAfterHeadBeforeRelease = 11,
    CrashPrimaryBeforeTransitionCas = 12,
    CrashPrimaryAfterSeedBeforeActiveCas = 13,
    CrashPrimaryAfterActiveCasBeforeCommit = 14,
    CrashPrimaryBeforeClosureCapture = 15,
    CrashPrimaryAfterClosureCapture = 16,
    CrashPrimaryDuringArtifactTransfer = 17,
    CrashPeerAfterDataFlushBeforeCommit = 18,
}

#[derive(Clone, Debug)]
pub struct FaultConfig {
    pub(crate) enabled: BTreeSet<FaultPoint>,
    pub(crate) forced: BTreeMap<FaultPoint, VecDeque<bool>>,
    pub(crate) probability: Ppm,
}

impl FaultConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: BTreeSet::new(),
            forced: BTreeMap::new(),
            probability: Ppm::NEVER,
        }
    }

    pub fn randomized(points: impl IntoIterator<Item = FaultPoint>, probability: Ppm) -> Self {
        Self {
            enabled: points.into_iter().collect(),
            forced: BTreeMap::new(),
            probability,
        }
    }

    pub fn force(&mut self, point: FaultPoint, outcomes: impl IntoIterator<Item = bool>) {
        self.forced.insert(point, outcomes.into_iter().collect());
    }
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self::disabled()
    }
}
