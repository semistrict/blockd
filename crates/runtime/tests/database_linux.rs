#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;

use blockd_core::database::{DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest};
use blockd_core::dbproto::{decode_reply, encode_request};
use blockd_core::format::FRAME_HEADER;
use blockd_core::journal::VsetConfig;
use blockd_core::protocol::{ReqId, StoreFault};
use blockd_core::types::{HostId, VmId, VsetId, millis};
use blockd_runtime::database::serve_database_stream;
use blockd_runtime::{GetResult, ObjectStore, Runtime, RuntimeConfig};

struct EmptyStore;

#[async_trait::async_trait]
impl ObjectStore for EmptyStore {
    async fn put(self: Arc<Self>, _key: String, _bytes: Vec<u8>) -> Result<u64, StoreFault> {
        Ok(1)
    }

    async fn put_cas(
        self: Arc<Self>,
        _key: String,
        _expected: Option<u64>,
        _bytes: Vec<u8>,
    ) -> Result<u64, StoreFault> {
        Ok(1)
    }

    async fn get(self: Arc<Self>, _key: String) -> GetResult {
        Ok(None)
    }

    async fn get_range(self: Arc<Self>, _key: String, _offset: u64, _len: u64) -> GetResult {
        Ok(None)
    }

    async fn delete(self: Arc<Self>, _key: String) {}
}

fn temp_dir() -> PathBuf {
    std::env::temp_dir().join(format!("blockd-database-{}", std::process::id()))
}

fn round_trip(stream: &mut UnixStream, request: &DatabaseRequest) -> DatabaseReply {
    let frame = encode_request(request);
    // Deliberately fragment the frame: stream parsing must not depend on
    // write boundaries.
    let split = frame.len() / 3;
    stream.write_all(&frame[..split]).expect("request head");
    stream.write_all(&frame[split..]).expect("request tail");
    let mut header = [0u8; FRAME_HEADER];
    stream.read_exact(&mut header).expect("reply header");
    let len = usize::try_from(u32::from_le_bytes(
        header[4..8].try_into().expect("length field"),
    ))
    .expect("fits");
    let mut frame = header.to_vec();
    frame.resize(FRAME_HEADER + len, 0);
    stream
        .read_exact(&mut frame[FRAME_HEADER..])
        .expect("reply payload");
    decode_reply(&frame).expect("reply frame")
}

#[test]
fn unix_stream_runs_sqlite_shaped_io_through_the_real_daemon() {
    let dir = temp_dir();
    let _ = std::fs::remove_dir_all(&dir);
    let runtime = Arc::new(Runtime::new(
        &RuntimeConfig {
            daemon: blockd_core::hostmeta::HostConfig {
                host: HostId(0),
                cache_pages: 16,
                writeback_interval: millis(10),
                backup_retry: millis(20),
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 25,
                replica_placement: None,
            },
            blob_dir: dir.clone(),
            peer: None,
        },
        Arc::new(EmptyStore),
    ));
    let vset = VsetId(88);
    let vm = VmId(4);
    runtime.create_vset(vset, VsetConfig::database(32));
    let attachment = runtime.attach_database(vset, vm);

    let (mut client, server) = UnixStream::pair().expect("socketpair");
    let serving = runtime.clone();
    let thread = std::thread::spawn(move || serve_database_stream(&serving, vm, server));
    let request = |req, op| DatabaseRequest {
        req: ReqId(req),
        vset,
        attachment,
        op,
    };
    assert_eq!(
        round_trip(
            &mut client,
            &request(
                1,
                DatabaseOp::Open {
                    handle: 7,
                    file: DatabaseFile::Main,
                    create: true,
                },
            ),
        ),
        DatabaseReply::Opened { req: ReqId(1) }
    );
    let bytes = vec![0xA7; 7000];
    assert!(matches!(
        round_trip(
            &mut client,
            &request(
                2,
                DatabaseOp::Write {
                    handle: 7,
                    offset: 37,
                    bytes: bytes.clone(),
                },
            ),
        ),
        DatabaseReply::Written { req: ReqId(2), .. }
    ));
    assert_eq!(
        round_trip(
            &mut client,
            &request(
                3,
                DatabaseOp::Read {
                    handle: 7,
                    offset: 37,
                    len: 7000,
                },
            ),
        ),
        DatabaseReply::Read {
            req: ReqId(3),
            bytes,
            eof: false,
        }
    );
    assert!(matches!(
        round_trip(&mut client, &request(4, DatabaseOp::Sync { handle: 7 }),),
        DatabaseReply::Synced { req: ReqId(4), .. }
    ));
    drop(client);
    thread.join().expect("server thread").expect("clean EOF");
    drop(runtime);
    let _ = std::fs::remove_dir_all(dir);
}
