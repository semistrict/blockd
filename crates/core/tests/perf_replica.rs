//! Replica preparation profile. It measures the expensive validation/sealing
//! work that formerly occupied the peer I/O callback against the bounded
//! queue submission now performed there.

#![allow(
    clippy::cast_precision_loss,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

use std::sync::mpsc::{channel, sync_channel};
use std::time::Instant;

use blockd_core::format::crc32c;
use blockd_core::protocol::ReplicaArtifact;
use blockd_core::replica_spool::seal_verified_replica_artifact;
use blockd_core::segment::SegmentBuilder;
use blockd_core::types::{
    Gen, HostId, PageId, PageNo, SegId, VolumeId, VolumeIdx, VsetId, page_size,
};

fn artifact_bytes() -> (VsetId, ReplicaArtifact, Vec<u8>) {
    const TARGET_BYTES: usize = 8 * 1024 * 1024;
    let vset = VsetId(7);
    let fence = 11;
    let seg = SegId(13);
    let artifact = ReplicaArtifact::Segment { fence, seg };
    let mut builder = SegmentBuilder::new(vset, fence, seg);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut raw = vec![0u8; page_size()];
    let pages = TARGET_BYTES.div_ceil(page_size());
    for page_no in 0..pages {
        for word in raw.chunks_exact_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            word.copy_from_slice(&state.to_le_bytes());
        }
        builder.add(
            PageId {
                volume: VolumeId {
                    vset,
                    idx: VolumeIdx(1),
                },
                page: PageNo(u32::try_from(page_no).expect("profile page count fits")),
            },
            Gen(u64::try_from(page_no).expect("fits") + 1),
            &raw,
        );
    }
    let (bytes, _) = builder.finish();
    (vset, artifact, bytes)
}

fn prepare(vset: VsetId, artifact: ReplicaArtifact, checksum: u32, bytes: &[u8]) -> Vec<u8> {
    assert_eq!(crc32c(bytes), checksum);
    seal_verified_replica_artifact(HostId(3), vset, 5, artifact, checksum, bytes)
        .expect("valid benchmark artifact")
}

#[test]
#[ignore = "performance profile; run explicitly in release mode"]
fn profile_replica_preparation_queue() {
    const SAMPLES: usize = 5;
    let (vset, artifact, bytes) = artifact_bytes();
    let checksum = crc32c(&bytes);

    let mut inline_samples = Vec::new();
    for _ in 0..SAMPLES {
        let started = Instant::now();
        let frame = prepare(vset, artifact, checksum, &bytes);
        inline_samples.push(started.elapsed());
        assert!(frame.len() > bytes.len());
    }

    let (work, rx) = sync_channel::<Vec<u8>>(2);
    let (done, prepared) = channel();
    let worker = std::thread::spawn(move || {
        for _ in 0..SAMPLES {
            let bytes = rx.recv().expect("profile job");
            done.send(prepare(vset, artifact, checksum, &bytes))
                .expect("profile receiver");
        }
    });
    let mut submit_samples = Vec::new();
    let mut completion_samples = Vec::new();
    for _ in 0..SAMPLES {
        let queued_bytes = bytes.clone();
        let started = Instant::now();
        work.try_send(queued_bytes).expect("empty bounded queue");
        submit_samples.push(started.elapsed());
        let frame = prepared.recv().expect("prepared artifact");
        completion_samples.push(started.elapsed());
        assert!(frame.len() > bytes.len());
    }
    worker.join().expect("preparation worker");

    inline_samples.sort_unstable();
    submit_samples.sort_unstable();
    completion_samples.sort_unstable();
    let inline_elapsed = inline_samples[SAMPLES / 2];
    let submit_elapsed = submit_samples[SAMPLES / 2];
    let completion_elapsed = completion_samples[SAMPLES / 2];

    eprintln!("── PROFILE: replica artifact preparation ──");
    eprintln!(
        "  artifact {:.1} MiB; median of {SAMPLES}: inline peer-I/O work {inline_elapsed:.2?}; \
         bounded queue submission {submit_elapsed:.2?}; worker completion {completion_elapsed:.2?}",
        bytes.len() as f64 / 1024.0 / 1024.0,
    );
    eprintln!(
        "  peer-I/O critical-section reduction: {:.0}×",
        inline_elapsed.as_secs_f64() / submit_elapsed.as_secs_f64().max(f64::EPSILON),
    );
}
