//! Prototype VM-bound database transport retained as a differential oracle.
//! Production guest I/O enters through the `vsetfs` FUSE adapter; these Unix
//! streams still exercise the same deterministic durable request seam.

use std::collections::BTreeSet;
use std::io::{self, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use blockd_core::database::{DatabaseError, DatabaseReply, DatabaseRequest};
use blockd_core::dbproto::{MAX_DATABASE_FRAME, decode_request, encode_reply};
use blockd_core::format::FRAME_HEADER;
use blockd_core::seam::ReqId;
use blockd_core::types::VmId;

use crate::Runtime;

const MAX_CONNECTIONS_PER_VM: usize = 64;
const MAX_OUTSTANDING_PER_CONNECTION: usize = 32;
const MAX_OUTSTANDING_PER_VM: usize = 512;
const WORKERS_PER_CONNECTION: usize = 4;
pub const DEFAULT_DATABASE_VSOCK_PORT: u32 = 10_052;

fn firecracker_guest_listener(uds_path: &Path, port: u32) -> PathBuf {
    let mut path = uds_path.as_os_str().to_owned();
    path.push(format!("_{port}"));
    PathBuf::from(path)
}

fn read_frame(stream: &mut UnixStream) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; FRAME_HEADER];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let payload_len = usize::try_from(u32::from_le_bytes(
        header[4..8].try_into().expect("four bytes"),
    ))
    .expect("u32 fits");
    let frame_len = FRAME_HEADER
        .checked_add(payload_len)
        .filter(|len| *len <= MAX_DATABASE_FRAME)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "database frame too large"))?;
    let mut frame = vec![0u8; frame_len];
    frame[..FRAME_HEADER].copy_from_slice(&header);
    stream.read_exact(&mut frame[FRAME_HEADER..])?;
    Ok(Some(frame))
}

/// Serve one already-authenticated VM stream until EOF or the first malformed
/// frame. The VM id is host configuration, never decoded from the stream.
pub fn serve_database_stream(
    runtime: &Runtime,
    vm: VmId,
    mut stream: UnixStream,
) -> io::Result<()> {
    while let Some(frame) = read_frame(&mut stream)? {
        let request = decode_request(vm, &frame)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid database frame"))?;
        let reply = runtime.database_request(request);
        stream.write_all(&encode_reply(&reply))?;
    }
    Ok(())
}

fn write_reply(writer: &Mutex<UnixStream>, reply: &DatabaseReply) -> io::Result<()> {
    writer
        .lock()
        .expect("database writer lock")
        .write_all(&encode_reply(reply))
}

/// Endpoint serving path: read and validate frames on one bounded ingress,
/// then let a small fixed worker set wait on independent daemon completions.
/// Mutation acceptance order still comes from the daemon's per-vset queue;
/// replies may complete out of order and retain the guest request id.
fn serve_database_stream_concurrent(
    runtime: &Arc<Runtime>,
    vm: VmId,
    mut stream: UnixStream,
    vm_outstanding: &Arc<AtomicUsize>,
) -> io::Result<()> {
    let writer = Arc::new(Mutex::new(stream.try_clone()?));
    let outstanding = Arc::new(Mutex::new(BTreeSet::<ReqId>::new()));
    let (work_tx, work_rx) = sync_channel::<DatabaseRequest>(MAX_OUTSTANDING_PER_CONNECTION);
    let work_rx = Arc::new(Mutex::new(work_rx));
    let mut workers = Vec::with_capacity(WORKERS_PER_CONNECTION);
    for _ in 0..WORKERS_PER_CONNECTION {
        let runtime = runtime.clone();
        let work_rx = work_rx.clone();
        let writer = writer.clone();
        let outstanding = outstanding.clone();
        let vm_outstanding = vm_outstanding.clone();
        workers.push(thread::spawn(move || {
            loop {
                let request = {
                    let receiver = work_rx.lock().expect("database work lock");
                    receiver.recv()
                };
                let Ok(request) = request else { break };
                let guest_req = request.req;
                let reply = runtime.database_request(request);
                let _ = write_reply(&writer, &reply);
                outstanding
                    .lock()
                    .expect("database outstanding lock")
                    .remove(&guest_req);
                vm_outstanding.fetch_sub(1, Ordering::AcqRel);
            }
        }));
    }

    let read_result = (|| {
        while let Some(frame) = read_frame(&mut stream)? {
            let request = decode_request(vm, &frame).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid database frame")
            })?;
            let req = request.req;
            {
                let mut ids = outstanding.lock().expect("database outstanding lock");
                if ids.len() >= MAX_OUTSTANDING_PER_CONNECTION || !ids.insert(req) {
                    write_reply(
                        &writer,
                        &DatabaseReply::Failed {
                            req,
                            error: DatabaseError::Busy,
                        },
                    )?;
                    continue;
                }
            }
            if vm_outstanding.fetch_add(1, Ordering::AcqRel) >= MAX_OUTSTANDING_PER_VM {
                vm_outstanding.fetch_sub(1, Ordering::AcqRel);
                outstanding
                    .lock()
                    .expect("database outstanding lock")
                    .remove(&req);
                write_reply(
                    &writer,
                    &DatabaseReply::Failed {
                        req,
                        error: DatabaseError::Busy,
                    },
                )?;
                continue;
            }
            match work_tx.try_send(request) {
                Ok(()) => {}
                Err(TrySendError::Full(request) | TrySendError::Disconnected(request)) => {
                    vm_outstanding.fetch_sub(1, Ordering::AcqRel);
                    outstanding
                        .lock()
                        .expect("database outstanding lock")
                        .remove(&request.req);
                    write_reply(
                        &writer,
                        &DatabaseReply::Failed {
                            req: request.req,
                            error: DatabaseError::Busy,
                        },
                    )?;
                }
            }
        }
        Ok(())
    })();
    drop(work_tx);
    for worker in workers {
        let _ = worker.join();
    }
    read_result
}

/// One VM-specific listener. Hundreds of logical database attachments share
/// it; only active stream connections consume worker threads.
pub struct DatabaseEndpoint {
    path: PathBuf,
    stopping: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl DatabaseEndpoint {
    /// Bind the host listener used for guest-initiated Firecracker vsock
    /// connections. Firecracker maps guest host-CID/port traffic to a Unix
    /// socket named `<uds_path>_<port>`.
    pub fn bind_firecracker(
        runtime: Arc<Runtime>,
        vm: VmId,
        uds_path: &Path,
        port: u32,
    ) -> io::Result<Self> {
        Self::bind_unix(runtime, vm, &firecracker_guest_listener(uds_path, port))
    }

    pub fn bind_unix(runtime: Arc<Runtime>, vm: VmId, path: &Path) -> io::Result<Self> {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let listener = UnixListener::bind(path)?;
        listener.set_nonblocking(true)?;
        let stopping = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let vm_outstanding = Arc::new(AtomicUsize::new(0));
        let run_stopping = stopping.clone();
        let thread = thread::spawn(move || {
            while !run_stopping.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if active.fetch_add(1, Ordering::AcqRel) >= MAX_CONNECTIONS_PER_VM {
                            active.fetch_sub(1, Ordering::AcqRel);
                            drop(stream);
                            continue;
                        }
                        let runtime = runtime.clone();
                        let active = active.clone();
                        let vm_outstanding = vm_outstanding.clone();
                        thread::spawn(move || {
                            let _ = serve_database_stream_concurrent(
                                &runtime,
                                vm,
                                stream,
                                &vm_outstanding,
                            );
                            active.fetch_sub(1, Ordering::AcqRel);
                        });
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::park_timeout(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(DatabaseEndpoint {
            path: path.to_owned(),
            stopping,
            thread: Some(thread),
        })
    }
}

impl Drop for DatabaseEndpoint {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}
