#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::Duration;

use blockd_core::database::{
    DatabaseError, DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest,
};
use blockd_core::dbproto::{decode_reply, encode_request};
use blockd_core::format::FRAME_HEADER;
use blockd_core::journal::VsetConfig;
use blockd_core::protocol::ReqId;
use blockd_core::types::{VmId, VsetId};
use blockd_runtime::database::serve_database_stream;
use blockd_runtime::{Runtime, S3Store};

mod support;

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
    let addresses = [
        support::free_addr(),
        support::free_addr(),
        support::free_addr(),
    ];
    let roots: [std::path::PathBuf; 3] =
        std::array::from_fn(|host| support::temp_root(&format!("database-{host}")));
    let store = Arc::new(S3Store::new());
    let runtimes = (0..3)
        .map(|host| {
            Arc::new(Runtime::new(
                &support::three_host_runtime_config(
                    host,
                    roots[usize::from(host)].clone(),
                    addresses,
                ),
                store.clone(),
            ))
        })
        .collect::<Vec<_>>();
    std::thread::sleep(Duration::from_millis(100));
    let runtime = Arc::clone(&runtimes[0]);
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
    let mut synced = false;
    for req in 4..=64 {
        match round_trip(&mut client, &request(req, DatabaseOp::Sync { handle: 7 })) {
            DatabaseReply::Synced { .. } => {
                synced = true;
                break;
            }
            DatabaseReply::Failed {
                error: DatabaseError::Io,
                ..
            } => std::thread::sleep(Duration::from_millis(20)),
            reply => panic!("unexpected sync reply: {reply:?}"),
        }
    }
    assert!(synced, "database sync did not become durable");
    drop(client);
    thread.join().expect("server thread").expect("clean EOF");
    drop(runtime);
    drop(runtimes);
    for root in roots {
        let _ = std::fs::remove_dir_all(root);
    }
}
