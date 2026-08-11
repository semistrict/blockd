//! Deterministic fault injection points shared by actors and the nemesis.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::rng::Ppm;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FaultPoint {
    ReplicaRetryTimer,
    DuplicateAck,
    StatusReconciliation,
    ReleaseOverlap,
    AssignmentCasRace,
    StoreUnknownResult,
    RestartScan,
    CrashPeerAfterCommitBeforeAck,
    CrashPrimaryAfterAckBeforeSyncOk,
    CrashPrimaryAfterSyncOk,
    CrashPeerAfterUploadBeforeHead,
    CrashPrimaryAfterHeadBeforeRelease,
    CrashPrimaryBeforeTransitionCas,
    CrashPrimaryAfterSeedBeforeActiveCas,
    CrashPrimaryAfterActiveCasBeforeCommit,
    CrashPrimaryBeforeClosureCapture,
    CrashPrimaryAfterClosureCapture,
    CrashPrimaryDuringArtifactTransfer,
    CrashPeerAfterDataFlushBeforeCommit,
    CrashPeerDuringUpload,
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
