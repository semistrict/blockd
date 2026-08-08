//! The recovery scan: read every blob under a root directory, named by
//! its root-relative path — exactly the bytes `Daemon::recover` sees
//! after a real crash. Portable (plain `std::fs`), so the differential
//! recovery test can prove on any OS that this walk and the simulation's
//! in-memory scan hand recovery identical worlds.

use std::path::Path;

use blockd_core::daemon::RecoveryBlob;
use blockd_core::layout::{self, BlobName};

pub(crate) struct ScannedBlob {
    pub(crate) name: String,
    pub(crate) bytes: Vec<u8>,
    pub(crate) len: u64,
}

impl ScannedBlob {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub(crate) fn recovery_blob(&self) -> RecoveryBlob<'_> {
        RecoveryBlob {
            name: &self.name,
            bytes: &self.bytes,
            len: self.len,
        }
    }
}

pub fn scan_blob_dir(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    scan_blobs(root, root, &mut out);
    out
}

/// Scan recovery metadata without pulling segment payloads into memory.
/// Segments are immutable and verified lazily by the fill path; recovery only
/// needs their names and lengths. Unknown files are ignored just as they are
/// by `Daemon::recover`.
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
        let bytes = if matches!(kind, BlobName::Segment { .. }) {
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
    use blockd_core::types::{SegId, VsetId};

    #[test]
    fn recovery_scan_does_not_read_segment_payloads() {
        let root = tempfile::tempdir().expect("tempdir");
        let segment = layout::segment_blob(VsetId(7), 3, SegId(2));
        let path = root.path().join(&segment);
        std::fs::create_dir_all(path.parent().expect("segment parent")).expect("segment directory");
        std::fs::write(path, vec![0x5a; 1024 * 1024]).expect("segment fixture");

        let scanned = scan_blob_dir_for_recovery(root.path());
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].name, segment);
        assert_eq!(scanned[0].len, 1024 * 1024);
        assert!(scanned[0].bytes.is_empty());
    }

    #[test]
    #[ignore = "performance profile; run explicitly in release mode"]
    fn profile_recovery_scan_skips_large_segment_payloads() {
        let root = tempfile::tempdir().expect("tempdir");
        let segments = 8;
        let segment_bytes = 64 * 1024 * 1024;
        for id in 0..segments {
            let name = layout::segment_blob(VsetId(7), 3, SegId(id));
            let path = root.path().join(name);
            std::fs::create_dir_all(path.parent().expect("segment parent"))
                .expect("segment directory");
            let file = std::fs::File::create(path).expect("segment fixture");
            file.set_len(segment_bytes).expect("sparse segment length");
        }

        let started = std::time::Instant::now();
        let scanned = scan_blob_dir_for_recovery(root.path());
        let elapsed = started.elapsed();
        let payload_bytes: usize = scanned.iter().map(|blob| blob.bytes.len()).sum();
        eprintln!(
            "recovery scan: segments={segments}, logical_bytes={}, loaded_bytes={payload_bytes}, elapsed={elapsed:?}",
            segment_bytes * segments
        );
        assert_eq!(scanned.len(), usize::try_from(segments).expect("fits"));
        assert_eq!(payload_bytes, 0);
    }
}

fn scan_blobs(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan_blobs(root, &path, out);
        } else {
            let name = path
                .strip_prefix(root)
                .expect("under root")
                .to_str()
                .expect("utf8")
                .to_owned();
            let bytes = std::fs::read(&path).expect("blob read");
            out.push((name, bytes));
        }
    }
}
