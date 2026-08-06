//! The recovery scan: read every blob under a root directory, named by
//! its root-relative path — exactly the bytes `Daemon::recover` sees
//! after a real crash. Portable (plain `std::fs`), so the differential
//! recovery test can prove on any OS that this walk and the simulation's
//! in-memory scan hand recovery identical worlds.

use std::path::Path;

pub fn scan_blob_dir(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    scan_blobs(root, root, &mut out);
    out
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
