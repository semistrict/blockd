//! Names for local durable blobs and object-store keys.
//!
//! Every blx and manifest is namespaced by the writer's **fence** — the
//! head record's CAS version at claim time (R6.3). A fenced former holder
//! keeps its own namespace, and since only the head record (updated by CAS)
//! makes state reachable, nothing a fenced holder writes can ever fork
//! durable state (R6.4): its keys simply dangle.
//!
//! Local blobs (relative to the daemon's data root):
//! - `v/<volume:016x>/j/<fence:016x>-<seq:016x>.rec` — journal record
//!   (framed, R10.2), plus a byte-identical `.recm` mirror: the newest
//!   record is the sole carrier of its newly-acked sync watermark, and a
//!   bit rotting it after the ack would silently roll acked syncs back
//!   (R3.8) — recovery accepts whichever copy decodes intact
//! - `v/<volume:016x>/o/<fence:016x>-<object:016x>.blx` — a local BLX data file
//!
//! Object store keys (relative to the cluster's bucket + prefix, R9.1):
//! - `v/<volume:016x>/head`  — head record: CAS assignment authority (R6.3)
//!   and pointer to the newest backed-up manifest
//! - `v/<volume:016x>/m/<fence:016x>-<seq:016x>` — manifest: the selected
//!   journal cut, optionally rewritten to archive-only page locations
//! - `v/<volume:016x>/p/<fence:016x>-<seq:016x>` — pending publication root:
//!   the manifest bytes retained until the head CAS makes them authoritative
//! - `v/<volume:016x>/o/<fence:016x>-<object:016x>.blx` — the same immutable
//!   BLX bytes used locally
//! - `b/<base:016x>/…` — bases
//! - `cluster/tls/public-keys/<host:04x>.member` — a node's current self-signed
//!   TLS certificate and advertised peer endpoint; possession of write access
//!   to this directory grants cluster membership

use crate::types::{HostId, JournalSeq, ObjectId, VolumeId};

pub fn placement_key() -> String {
    "cluster/placement".to_owned()
}

pub fn peer_membership_prefix() -> String {
    "cluster/tls/public-keys/".to_owned()
}

pub fn peer_membership_key(host: HostId) -> String {
    format!("{}{:04x}.member", peer_membership_prefix(), host.0)
}

pub fn host_session_key(host: HostId) -> String {
    format!("hosts/{:04x}/session", host.0)
}

pub fn vnode_authority_key(vnode: crate::authority::VnodeId) -> String {
    format!("vnodes/{:08x}/authority", vnode.0)
}

pub fn vnode_member_blob(vnode: crate::authority::VnodeId) -> String {
    format!("authority/vnodes/{:08x}.state", vnode.0)
}

pub fn vnode_closure_blob(
    vnode: crate::authority::VnodeId,
    volume: VolumeId,
    sequence: u64,
) -> String {
    format!(
        "authority/vnodes/{:08x}/volumes/{:016x}/{sequence:016x}.closure",
        vnode.0, volume.0
    )
}

pub fn journal_blob(volume: VolumeId, fence: u64, seq: JournalSeq) -> String {
    format!("v/{:016x}/j/{fence:016x}-{:016x}.rec", volume.0, seq.0)
}

/// The record's byte-identical mirror (rot redundancy, R3.8/R8.1).
pub fn journal_mirror_blob(volume: VolumeId, fence: u64, seq: JournalSeq) -> String {
    format!("v/{:016x}/j/{fence:016x}-{:016x}.recm", volume.0, seq.0)
}

pub fn blx_blob(volume: VolumeId, fence: u64, object: ObjectId) -> String {
    blx_key(volume, fence, object.0)
}

pub fn head_key(volume: VolumeId) -> String {
    format!("v/{:016x}/head", volume.0)
}

pub fn manifest_key(volume: VolumeId, fence: u64, seq: JournalSeq) -> String {
    archive_manifest_key(volume, fence, seq.0)
}

pub fn archive_manifest_key(volume: VolumeId, fence: u64, archive_seq: u64) -> String {
    format!(
        "v/{:016x}/m/{fence:016x}-{archive_seq:016x}.manifest",
        volume.0
    )
}

pub fn complete_file_list_key(volume: VolumeId, fence: u64, list_id: u64) -> String {
    format!("v/{:016x}/f/{fence:016x}-{list_id:016x}.files", volume.0)
}

pub fn blx_key(origin_volume: VolumeId, fence: u64, object_id: u64) -> String {
    format!(
        "v/{:016x}/o/{fence:016x}-{object_id:016x}.blx",
        origin_volume.0
    )
}

pub fn base_root_key(base: u64) -> String {
    format!("b/{base:016x}/root")
}

pub fn base_manifest_key(base: u64, manifest_id: u64) -> String {
    format!("b/{base:016x}/m/{manifest_id:016x}.manifest")
}

/// Durable root for a manifest publication that has not reached the head CAS.
pub fn pending_manifest_key(volume: VolumeId, fence: u64, seq: JournalSeq) -> String {
    format!("v/{:016x}/p/{fence:016x}-{:016x}", volume.0, seq.0)
}

/// Prefix under which every object of one volume lives (R4.4 audits, GC).
pub fn volume_prefix(volume: VolumeId) -> String {
    format!("v/{:016x}/", volume.0)
}

/// The local outbound-handoff marker (R7.2): its durable presence means
/// this host gave the volume away and may only serve peer fetches for it.
pub fn handoff_blob(volume: VolumeId) -> String {
    format!("v/{:016x}/handoff", volume.0)
}

/// Generation zero of an append-only passive-replica spool.
pub fn replica_spool_blob(source: HostId, volume: VolumeId, assignment_epoch: u64) -> String {
    replica_spool_generation_blob(source, volume, assignment_epoch, 0)
}

/// One bounded append-only spool generation. Callers supply typed fields
/// only; no network-provided path is accepted. Generation zero retains the
/// pre-rotation name, while later generations sort after it numerically.
pub fn replica_spool_generation_blob(
    source: HostId,
    volume: VolumeId,
    assignment_epoch: u64,
    generation: u64,
) -> String {
    let suffix = if generation == 0 {
        format!("{assignment_epoch:016x}.spool")
    } else {
        format!("{assignment_epoch:016x}-{generation:016x}.spool")
    };
    format!("r/{:04x}/{:016x}/{suffix}", source.0, volume.0)
}

/// Parse an object-store key back into its meaning (GC's mark phase).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StoreKey {
    Head {
        volume: VolumeId,
    },
    ArchiveManifest {
        volume: VolumeId,
        fence: u64,
        archive_seq: u64,
    },
    CompleteFileList {
        volume: VolumeId,
        fence: u64,
        list_id: u64,
    },
    Blx {
        origin_volume: VolumeId,
        fence: u64,
        object_id: u64,
    },
    PendingManifest {
        volume: VolumeId,
        fence: u64,
        seq: JournalSeq,
    },
    BaseRoot {
        base: u64,
    },
    BaseManifest {
        base: u64,
        manifest_id: u64,
    },
    ImportedBaseBlx {
        base: u64,
        fence: u64,
        object_id: u64,
    },
}

fn hex_pair(value: &str) -> Option<(u64, u64)> {
    let (left, right) = value.split_once('-')?;
    Some((
        u64::from_str_radix(left, 16).ok()?,
        u64::from_str_radix(right, 16).ok()?,
    ))
}

pub fn parse_key(key: &str) -> Option<StoreKey> {
    if let Some(rest) = key.strip_prefix("v/") {
        let (volume_hex, rest) = rest.split_once('/')?;
        let volume = VolumeId(u64::from_str_radix(volume_hex, 16).ok()?);
        if rest == "head" {
            return Some(StoreKey::Head { volume });
        }
        if let Some(body) = rest.strip_prefix("m/") {
            if let Some(body) = body.strip_suffix(".manifest") {
                let (fence, archive_seq) = hex_pair(body)?;
                return Some(StoreKey::ArchiveManifest {
                    volume,
                    fence,
                    archive_seq,
                });
            }
            return None;
        }
        if let Some(body) = rest
            .strip_prefix("f/")
            .and_then(|body| body.strip_suffix(".files"))
        {
            let (fence, list_id) = hex_pair(body)?;
            return Some(StoreKey::CompleteFileList {
                volume,
                fence,
                list_id,
            });
        }
        if let Some(body) = rest
            .strip_prefix("o/")
            .and_then(|body| body.strip_suffix(".blx"))
        {
            let (fence, object_id) = hex_pair(body)?;
            return Some(StoreKey::Blx {
                origin_volume: volume,
                fence,
                object_id,
            });
        }
        if let Some(body) = rest.strip_prefix("p/") {
            let (fence, seq) = hex_pair(body)?;
            return Some(StoreKey::PendingManifest {
                volume,
                fence,
                seq: JournalSeq(seq),
            });
        }
        return None;
    }
    if let Some(rest) = key.strip_prefix("b/") {
        let (base_hex, rest) = rest.split_once('/')?;
        let base = u64::from_str_radix(base_hex, 16).ok()?;
        if rest == "root" {
            return Some(StoreKey::BaseRoot { base });
        }
        if let Some(body) = rest
            .strip_prefix("m/")
            .and_then(|body| body.strip_suffix(".manifest"))
        {
            let manifest_id = u64::from_str_radix(body, 16).ok()?;
            return Some(StoreKey::BaseManifest { base, manifest_id });
        }
        if let Some(body) = rest
            .strip_prefix("o/")
            .and_then(|body| body.strip_suffix(".blx"))
        {
            let (fence, object_id) = hex_pair(body)?;
            return Some(StoreKey::ImportedBaseBlx {
                base,
                fence,
                object_id,
            });
        }
        return None;
    }
    None
}

/// Parse a local blob name back into its meaning (recovery scan).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BlobName {
    Journal {
        volume: VolumeId,
        fence: u64,
        seq: JournalSeq,
    },
    Blx {
        volume: VolumeId,
        fence: u64,
        object: ObjectId,
    },
    Handoff {
        volume: VolumeId,
    },
    ReplicaSpool {
        source: HostId,
        volume: VolumeId,
        assignment_epoch: u64,
        generation: u64,
    },
}

pub fn parse_blob(name: &str) -> Option<BlobName> {
    if let Some(rest) = name.strip_prefix("r/") {
        let mut parts = rest.split('/');
        let source = HostId(u16::from_str_radix(parts.next()?, 16).ok()?);
        let volume = VolumeId(u64::from_str_radix(parts.next()?, 16).ok()?);
        let file = parts.next()?.strip_suffix(".spool")?;
        let (assignment, generation) = match file.split_once('-') {
            None => (file, 0),
            Some((assignment, generation)) => {
                (assignment, u64::from_str_radix(generation, 16).ok()?)
            }
        };
        let assignment_epoch = u64::from_str_radix(assignment, 16).ok()?;
        if parts.next().is_some() {
            return None;
        }
        return Some(BlobName::ReplicaSpool {
            source,
            volume,
            assignment_epoch,
            generation,
        });
    }
    let rest = name.strip_prefix("v/")?;
    let (volume_hex, rest) = rest.split_once('/')?;
    let volume = VolumeId(u64::from_str_radix(volume_hex, 16).ok()?);
    // A mirror parses as the same journal record: recovery accepts
    // whichever copy decodes intact.
    if let Some(body) = rest
        .strip_prefix("j/")
        .and_then(|r| r.strip_suffix(".recm").or_else(|| r.strip_suffix(".rec")))
    {
        let (fence, seq) = hex_pair(body)?;
        let seq = JournalSeq(seq);
        return Some(BlobName::Journal { volume, fence, seq });
    }
    if let Some(body) = rest.strip_prefix("o/").and_then(|r| r.strip_suffix(".blx")) {
        let (fence, object) = hex_pair(body)?;
        let object = ObjectId(object);
        return Some(BlobName::Blx {
            volume,
            fence,
            object,
        });
    }
    if rest == "handoff" {
        return Some(BlobName::Handoff { volume });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn names_are_pinned_and_parse_back() {
        let volume = VolumeId(0x0BAD_CAFE);
        assert_eq!(
            journal_blob(volume, 2, JournalSeq(0x1F)),
            "v/000000000badcafe/j/0000000000000002-000000000000001f.rec"
        );
        assert_eq!(
            blx_blob(volume, 2, ObjectId(3)),
            "v/000000000badcafe/o/0000000000000002-0000000000000003.blx"
        );
        assert_eq!(head_key(volume), "v/000000000badcafe/head");
        assert_eq!(
            archive_manifest_key(volume, 2, 5),
            "v/000000000badcafe/m/0000000000000002-0000000000000005.manifest"
        );
        assert_eq!(
            complete_file_list_key(volume, 2, 7),
            "v/000000000badcafe/f/0000000000000002-0000000000000007.files"
        );
        assert_eq!(
            blx_key(volume, 2, 3),
            "v/000000000badcafe/o/0000000000000002-0000000000000003.blx"
        );
        assert_eq!(base_root_key(9), "b/0000000000000009/root");
        assert_eq!(
            base_manifest_key(9, 4),
            "b/0000000000000009/m/0000000000000004.manifest"
        );
        assert_eq!(
            manifest_key(volume, 2, JournalSeq(5)),
            "v/000000000badcafe/m/0000000000000002-0000000000000005.manifest"
        );
        assert_eq!(
            blx_key(volume, 2, 3),
            "v/000000000badcafe/o/0000000000000002-0000000000000003.blx"
        );
        assert_eq!(volume_prefix(volume), "v/000000000badcafe/");
        assert_eq!(
            replica_spool_blob(HostId(3), volume, 9),
            "r/0003/000000000badcafe/0000000000000009.spool"
        );
        assert_eq!(
            parse_blob("r/0003/000000000badcafe/0000000000000009.spool"),
            Some(BlobName::ReplicaSpool {
                source: HostId(3),
                volume,
                assignment_epoch: 9,
                generation: 0,
            })
        );
        assert_eq!(
            replica_spool_generation_blob(HostId(3), volume, 9, 2),
            "r/0003/000000000badcafe/0000000000000009-0000000000000002.spool"
        );
        assert_eq!(
            parse_blob("r/0003/000000000badcafe/0000000000000009-0000000000000002.spool"),
            Some(BlobName::ReplicaSpool {
                source: HostId(3),
                volume,
                assignment_epoch: 9,
                generation: 2,
            })
        );
        assert_eq!(
            parse_key("v/000000000badcafe/m/0000000000000002-0000000000000005.manifest"),
            Some(StoreKey::ArchiveManifest {
                volume,
                fence: 2,
                archive_seq: 5,
            })
        );
        assert_eq!(
            parse_key("v/000000000badcafe/f/0000000000000002-0000000000000007.files"),
            Some(StoreKey::CompleteFileList {
                volume,
                fence: 2,
                list_id: 7,
            })
        );
        assert_eq!(
            parse_key("v/000000000badcafe/o/0000000000000002-0000000000000003.blx"),
            Some(StoreKey::Blx {
                origin_volume: volume,
                fence: 2,
                object_id: 3,
            })
        );
        assert_eq!(
            parse_key("b/0000000000000009/root"),
            Some(StoreKey::BaseRoot { base: 9 })
        );
        assert_eq!(
            parse_key("b/0000000000000009/m/0000000000000004.manifest"),
            Some(StoreKey::BaseManifest {
                base: 9,
                manifest_id: 4,
            })
        );
        assert_eq!(
            parse_key("v/000000000badcafe/p/0000000000000002-000000000000001f"),
            Some(StoreKey::PendingManifest {
                volume,
                fence: 2,
                seq: JournalSeq(0x1F)
            })
        );
        assert_eq!(
            parse_blob("v/000000000badcafe/j/0000000000000002-000000000000001f.rec"),
            Some(BlobName::Journal {
                volume,
                fence: 2,
                seq: JournalSeq(0x1F)
            })
        );
        assert_eq!(
            parse_blob("v/000000000badcafe/o/0000000000000002-0000000000000003.blx"),
            Some(BlobName::Blx {
                volume,
                fence: 2,
                object: ObjectId(3)
            })
        );
        assert_eq!(parse_blob("garbage"), None);
        assert_eq!(parse_blob("v/000000000badcafe/o/junk.blx"), None);
        assert_eq!(parse_blob("v/000000000badcafe/j/junk.rec"), None);
    }
}
