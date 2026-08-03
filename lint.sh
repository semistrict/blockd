#!/bin/sh
# Lint gate: clippy (with the determinism disallowed-lists) and rustfmt.
set -eu

cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
