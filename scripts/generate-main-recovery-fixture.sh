#!/usr/bin/env bash
set -euo pipefail

revision=7eccd9d1263f1e7a64e10b92e16f0a9d24d5aac4
repo_root=$(git rev-parse --show-toplevel)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/blockd-main-recovery.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

git -C "$repo_root" archive "$revision" -o "$scratch/main.tar"
tar -xf "$scratch/main.tar" -C "$scratch"

test_file="$scratch/crates/runtime/tests/recovery_differential.rs"
perl -0pi -e 's/presets::single_host_chaos\(\)/presets::single_host_base()/g' "$test_file"
perl -0pi -e 's/(assert!\(!blobs\.is_empty\(\), "seed \{seed\} left no blobs to recover"\);)/$1\n        if let Ok(export) = std::env::var("BLOCKD_EXPORT_RECOVERY_FIXTURE") {\n            let seed_root = Path::new(\&export).join(seed.to_string()).join("blobs");\n            write_blobs(\&seed_root, \&blobs);\n        }/' "$test_file"

store_file="$scratch/crates/sim/src/world/store.rs"
perl -0pi -e 's#(    /// Bit rot:)#    pub fn snapshot_versions(\&self) -> Vec<(String, u64, Vec<u8>)> {\n        self.objects\n            .iter()\n            .map(|(key, (version, _, bytes))| (key.clone(), version.0, bytes.clone()))\n            .collect()\n    }\n\n$1#' "$store_file"

harness_file="$scratch/crates/sim/src/harness.rs"
perl -0pi -e 's|    \(h\.report, blobs\)|    if let Ok(export) = std::env::var("BLOCKD_EXPORT_RECOVERY_FIXTURE") {\n        let root = std::path::Path::new(\&export).join(seed.to_string()).join("store");\n        for (key, version, bytes) in h.store.snapshot_versions() {\n            let path = root.join(key);\n            std::fs::create_dir_all(path.parent().expect("store key has parent")).expect("mkdir");\n            let mut framed = version.to_le_bytes().to_vec();\n            framed.extend_from_slice(\&bytes);\n            std::fs::write(path, framed).expect("write store fixture");\n        }\n    }\n    (h.report, blobs)|' "$harness_file"

export BLOCKD_EXPORT_RECOVERY_FIXTURE="$scratch/export"
export CARGO_TARGET_DIR="$scratch/target"
cargo test --manifest-path "$scratch/Cargo.toml" -p blockd-runtime \
    --test recovery_differential disk_scans_recover_exactly_like_the_simulated_scan

fixture="$repo_root/crates/runtime/tests/fixtures/main-recovery-104"
rm -rf "$fixture"
mkdir -p "$fixture"
cp -R "$scratch/export/104/." "$fixture/"
printf '%s\n' "$revision" > "$repo_root/crates/runtime/tests/fixtures/main-recovery-revision.txt"
