//! CRC-32C throughput profile for the artifact-sized buffers used by BLX files
//! and replica transfer. The comparison implementation is the bytewise table
//! loop that preceded `crc-fast`; correctness is asserted, timings are printed.

#![allow(
    clippy::cast_precision_loss,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

use std::hint::black_box;
use std::time::Instant;

use blockd_core::format::crc32c;

const BYTES: usize = 32 * 1024 * 1024;
const PASSES: usize = 8;

#[allow(clippy::cast_possible_truncation)]
fn legacy_table_crc32c(bytes: &[u8]) -> u32 {
    const fn table() -> [u32; 256] {
        let poly: u32 = 0x82F6_3B78;
        let mut table = [0u32; 256];
        let mut n = 0;
        while n < 256 {
            let mut crc = n as u32;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ poly
                } else {
                    crc >> 1
                };
                bit += 1;
            }
            table[n] = crc;
            n += 1;
        }
        table
    }
    const TABLE: [u32; 256] = table();
    let mut crc = !0u32;
    for &byte in bytes {
        crc = (crc >> 8) ^ TABLE[usize::from((crc as u8) ^ byte)];
    }
    !crc
}

fn gib_per_second(bytes: usize, passes: usize, elapsed: std::time::Duration) -> f64 {
    (bytes * passes) as f64 / (1024.0 * 1024.0 * 1024.0) / elapsed.as_secs_f64()
}

#[test]
#[ignore = "performance profile; run explicitly in release mode"]
fn profile_crc32c_artifact_throughput() {
    let mut bytes = vec![0u8; BYTES];
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for word in bytes.chunks_exact_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        word.copy_from_slice(&state.to_le_bytes());
    }

    let expected = legacy_table_crc32c(black_box(&bytes));
    assert_eq!(crc32c(black_box(&bytes)), expected);

    let legacy_started = Instant::now();
    let mut legacy = 0;
    for _ in 0..PASSES {
        legacy ^= legacy_table_crc32c(black_box(&bytes));
    }
    let legacy_elapsed = legacy_started.elapsed();

    let accelerated_started = Instant::now();
    let mut accelerated = 0;
    for _ in 0..PASSES {
        accelerated ^= crc32c(black_box(&bytes));
    }
    let accelerated_elapsed = accelerated_started.elapsed();
    black_box((legacy, accelerated));

    let legacy_gib = gib_per_second(BYTES, PASSES, legacy_elapsed);
    let accelerated_gib = gib_per_second(BYTES, PASSES, accelerated_elapsed);
    eprintln!("── PROFILE: CRC-32C over 32 MiB artifacts ({PASSES} passes) ──");
    eprintln!(
        "  bytewise table  {legacy_gib:.2} GiB/s ({legacy_elapsed:.2?}); \
         accelerated  {accelerated_gib:.2} GiB/s ({accelerated_elapsed:.2?}); \
         speedup {:.1}×",
        accelerated_gib / legacy_gib,
    );
}
