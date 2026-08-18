//! The recovery scan: read every blob under a root directory, named by
//! its root-relative path — exactly the bytes actor recovery sees
//! after a real crash. Portable (plain `std::fs`), so the differential
//! recovery test can prove on any OS that this walk and the simulation's
//! in-memory scan hand recovery identical worlds.

use std::path::Path;

use blockd_core::layout::{self, BlobName};

pub(crate) struct ScannedBlob {
    pub(crate) name: String,
    pub(crate) bytes: Vec<u8>,
    pub(crate) len: u64,
}

/// Scan recovery metadata without pulling blx payloads into memory.
/// BLX files are immutable and verified lazily by the fill path; recovery only
/// needs their names and lengths. Unknown files are ignored just as they are
/// by the recovery actor.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn scan_blob_dir_for_recovery(root: &Path) -> Vec<ScannedBlob> {
    let mut out = Vec::new();
    scan_recovery_blobs(root, root, &mut out);
    out
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn scan_recovery_blobs(root: &Path, dir: &Path, out: &mut Vec<ScannedBlob>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_recovery_blobs(root, &path, out);
            continue;
        }
        let name = path
            .strip_prefix(root)
            .expect("under root")
            .to_str()
            .expect("utf8")
            .to_owned();
        let Some(kind) = layout::parse_blob(&name) else {
            continue;
        };
        let len = entry.metadata().expect("blob metadata").len();
        let bytes = if matches!(kind, BlobName::Blx { .. }) {
            Vec::new()
        } else {
            std::fs::read(&path).expect("blob read")
        };
        out.push(ScannedBlob { name, bytes, len });
    }
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::*;
    use blockd_core::layout;
    use blockd_core::types::{ObjectId, VolumeId};

    #[test]
    fn recovery_scan_does_not_read_blx_payloads() {
        let root = tempfile::tempdir().expect("tempdir");
        let blx = layout::blx_blob(VolumeId(7), 3, ObjectId(2));
        let path = root.path().join(&blx);
        std::fs::create_dir_all(path.parent().expect("blx parent")).expect("blx directory");
        std::fs::write(path, vec![0x5a; 1024 * 1024]).expect("blx fixture");

        let scanned = scan_blob_dir_for_recovery(root.path());
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].name, blx);
        assert_eq!(scanned[0].len, 1024 * 1024);
        assert!(scanned[0].bytes.is_empty());
    }

    #[test]
    #[ignore = "performance profile; run explicitly in release mode"]
    fn profile_recovery_scan_skips_large_blx_payloads() {
        let root = tempfile::tempdir().expect("tempdir");
        let blx_files = 8;
        let blx_bytes = 64 * 1024 * 1024;
        for id in 0..blx_files {
            let name = layout::blx_blob(VolumeId(7), 3, ObjectId(id));
            let path = root.path().join(name);
            std::fs::create_dir_all(path.parent().expect("blx parent")).expect("blx directory");
            let file = std::fs::File::create(path).expect("blx fixture");
            file.set_len(blx_bytes).expect("sparse blx length");
        }

        let started = std::time::Instant::now();
        let scanned = scan_blob_dir_for_recovery(root.path());
        let elapsed = started.elapsed();
        let payload_bytes: usize = scanned.iter().map(|blob| blob.bytes.len()).sum();
        eprintln!(
            "recovery scan: blx_files={blx_files}, logical_bytes={}, loaded_bytes={payload_bytes}, elapsed={elapsed:?}",
            blx_bytes * blx_files
        );
        assert_eq!(scanned.len(), usize::try_from(blx_files).expect("fits"));
        assert_eq!(payload_bytes, 0);
    }
}
