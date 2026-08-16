use crate::journal::{JournalRecord, RecordKind};
use crate::manifest::RecoveryKind;
use crate::protocol::Verdict;
use crate::types::Epoch;

pub fn record_verdict(record: &JournalRecord) -> Verdict {
    match record.kind {
        RecordKind::Checkpoint { epoch, vmstate, .. }
            if record.capture_seq >= record.sync_covered_through =>
        {
            Verdict::Resume { epoch, vmstate }
        }
        RecordKind::Checkpoint { .. } | RecordKind::Commit => Verdict::ColdBoot,
    }
}

pub fn recovery_metadata(record: &JournalRecord) -> (RecoveryKind, Epoch, u64) {
    match record.kind {
        RecordKind::Checkpoint {
            epoch,
            vmstate_logical_length,
            ..
        } if matches!(record_verdict(record), Verdict::Resume { .. }) => {
            (RecoveryKind::Whole, epoch, vmstate_logical_length)
        }
        RecordKind::Checkpoint { .. } | RecordKind::Commit => (RecoveryKind::DiskOnly, Epoch(0), 0),
    }
}

pub fn manifest_verdict(kind: RecoveryKind, epoch: Epoch, vmstate: u64) -> (Verdict, RecordKind) {
    match kind {
        RecoveryKind::Whole => (
            Verdict::Resume { epoch, vmstate },
            RecordKind::Checkpoint {
                epoch,
                vmstate,
                vmstate_logical_length: vmstate,
            },
        ),
        RecoveryKind::DiskOnly => (Verdict::ColdBoot, RecordKind::Commit),
    }
}
