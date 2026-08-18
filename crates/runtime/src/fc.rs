//! Driving REAL Firecracker microVMs: process control, the API socket
//! (HTTP/1.1 over a unix stream — machine config, boot, pause, snapshot,
//! restore), the serial console (the guest workload's command channel),
//! and the snapshot-restore page-fault handler — Firecracker's designed-in
//! integration point for external memory backends like blockd: on
//! `/snapshot/load` with a `Uffd` backend it hands us the guest-memory
//! userfaultfd over a unix socket, and every guest touch becomes OUR fill.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use blockd_hostmem::{Uffd, page_size, recv_with_fd};
use bytes::Bytes;
use futures_util::{FutureExt as _, StreamExt as _};
use http_body_util::{BodyExt as _, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::unix::AsyncFd;
use tokio::io::{
    AsyncBufReadExt as _, AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _, BufReader,
    Lines,
};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;

/// One HTTP request over the Firecracker API socket.
async fn api_request(sock: &Path, method: Method, path: &str, body: Value) -> (StatusCode, String) {
    // The socket file appears at bind time, a beat before Firecracker
    // listens — connect with retry instead of racing it.
    let deadline = Instant::now() + Duration::from_secs(5);
    let stream = loop {
        match tokio::net::UnixStream::connect(sock).await {
            Ok(stream) => break stream,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => panic!("api socket: {e}"),
        }
    };
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("Firecracker HTTP handshake");
    let connection = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(%error, "Firecracker HTTP connection closed");
        }
    });
    let bytes = serde_json::to_vec(&body).expect("serialize Firecracker request");
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(hyper::header::HOST, "localhost")
        .header(hyper::header::ACCEPT, "application/json")
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::CONNECTION, "close")
        .body(Full::new(Bytes::from(bytes)))
        .expect("build Firecracker request");
    let response = tokio::time::timeout(Duration::from_secs(30), sender.send_request(request))
        .await
        .expect("Firecracker API response timeout")
        .expect("Firecracker API response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read Firecracker API response")
        .to_bytes();
    drop(sender);
    let _ = connection.await;
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// A running Firecracker process with its API socket and serial console.
pub struct FcVm {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    api_sock: PathBuf,
}

impl FcVm {
    /// Spawn Firecracker with an API socket; stdout is the guest serial.
    pub async fn spawn(fc_bin: &Path, api_sock: &Path) -> FcVm {
        let _ = tokio::fs::remove_file(api_sock).await;
        let mut child = Command::new(fc_bin)
            .arg("--api-sock")
            .arg(api_sock)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn firecracker");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let stderr = child.stderr.take().expect("stderr");
        tokio::spawn(async move {
            let mut stderr = BufReader::new(stderr).lines();
            while stderr.next_line().await.is_ok_and(|line| line.is_some()) {}
        });
        FcVm {
            child,
            stdin,
            lines: BufReader::new(stdout).lines(),
            api_sock: api_sock.to_owned(),
        }
    }

    pub async fn api(&self, method: Method, path: &str, body: Value) {
        let (status, reply) = api_request(&self.api_sock, method.clone(), path, body).await;
        assert!(
            status.is_success(),
            "{method} {path} failed: {status} {reply}"
        );
    }

    /// Add Firecracker's single virtio-vsock device before boot. Guest
    /// connections to host CID 2 and port P are forwarded to
    /// `<uds_path>_P` on the host.
    pub async fn configure_vsock(&self, guest_cid: u32, uds_path: &Path) {
        self.api(
            Method::PUT,
            "/vsock",
            json!({"guest_cid": guest_cid, "uds_path": uds_path}),
        )
        .await;
    }

    /// Configure and boot a fresh microVM from kernel + initramfs (no
    /// disks: the workload lives in the initramfs, so forks never contend
    /// on a writable block device).
    pub async fn boot(&self, kernel: &Path, initrd: &Path, mem_mib: u32) {
        self.boot_with_vcpus(kernel, initrd, mem_mib, 1).await;
    }

    /// Configure and boot a fresh microVM with an explicit virtual CPU count.
    pub async fn boot_with_vcpus(
        &self,
        kernel: &Path,
        initrd: &Path,
        mem_mib: u32,
        vcpu_count: u8,
    ) {
        assert!(vcpu_count > 0, "microVM must have at least one vCPU");
        self.api(
            Method::PUT,
            "/machine-config",
            json!({"vcpu_count": vcpu_count, "mem_size_mib": mem_mib}),
        )
        .await;
        self.api(
            Method::PUT,
            "/boot-source",
            json!({
                "kernel_image_path": kernel,
                "initrd_path": initrd,
                "boot_args": "keep_bootcon console=ttyS0 reboot=k panic=-1 quiet no-kvmapf rdinit=/init",
            }),
        ).await;
        self.api(
            Method::PUT,
            "/actions",
            json!({"action_type": "InstanceStart"}),
        )
        .await;
    }

    pub async fn pause(&self) {
        self.api(Method::PATCH, "/vm", json!({"state": "Paused"}))
            .await;
    }

    pub async fn resume(&self) {
        self.api(Method::PATCH, "/vm", json!({"state": "Resumed"}))
            .await;
    }

    /// Full snapshot of a paused microVM: vmstate + guest memory file.
    pub async fn snapshot(&self, snapshot_path: &Path, mem_path: &Path) {
        self.api(
            Method::PUT,
            "/snapshot/create",
            json!({"snapshot_type": "Full", "snapshot_path": snapshot_path, "mem_file_path": mem_path}),
        ).await;
    }

    /// Restore from a snapshot with the patched `UffdShmem` backend:
    /// guest memory is a `MAP_PRIVATE` mapping of the handler-owned
    /// shared-memory file; missing faults reach the handler through the
    /// socket. Forks restored from one snapshot share every clean page
    /// physically; writes diverge via copy-on-write.
    pub async fn load_snapshot_shmem(&self, snapshot_path: &Path, uffd_sock: &Path, shmem: &Path) {
        self.api(
            Method::PUT,
            "/snapshot/load",
            json!({
                "snapshot_path": snapshot_path,
                "mem_backend": {"backend_type": "UffdShmem", "backend_path": uffd_sock, "shmem_path": shmem},
                "resume_vm": true,
            }),
        ).await;
    }

    /// Restore (fork) from a snapshot. `uffd_sock` selects the memory
    /// backend: `None` maps the memory file directly (kernel page-cache
    /// sharing across forks); `Some` hands the guest memory to OUR
    /// page-fault handler over the socket (blockd's fill door).
    pub async fn load_snapshot(
        &self,
        snapshot_path: &Path,
        mem_path: &Path,
        uffd_sock: Option<&Path>,
    ) {
        let backend = match uffd_sock {
            None => json!({"backend_type": "File", "backend_path": mem_path}),
            Some(sock) => json!({"backend_type": "Uffd", "backend_path": sock}),
        };
        self.api(
            Method::PUT,
            "/snapshot/load",
            json!({"snapshot_path": snapshot_path, "mem_backend": backend, "resume_vm": true}),
        )
        .await;
    }

    /// Restore from an immutable memory snapshot through a per-VM shared
    /// working copy. Vhost-user backends need shared visibility of virtqueue
    /// writes, while the source snapshot must remain unchanged.
    pub async fn load_snapshot_shared(&self, snapshot_path: &Path, mem_path: &Path) {
        self.api(
            Method::PUT,
            "/snapshot/load",
            json!({
                "snapshot_path": snapshot_path,
                "mem_backend": {"backend_type": "FileShared", "backend_path": mem_path},
                "resume_vm": true,
            }),
        )
        .await;
    }

    /// Send one workload command and wait for its reply line (replies are
    /// uppercase; the tty echo of commands can never match).
    pub async fn cmd(&mut self, command: &str, reply_prefix: &str) -> String {
        self.stdin
            .write_all(command.as_bytes())
            .await
            .expect("serial write");
        self.stdin.write_all(b"\n").await.expect("serial newline");
        self.stdin.flush().await.expect("serial flush");
        self.wait_line(reply_prefix).await
    }

    pub async fn try_cmd(
        &mut self,
        command: &str,
        reply_prefix: &str,
        timeout: Duration,
    ) -> Option<String> {
        self.stdin.write_all(command.as_bytes()).await.ok()?;
        self.stdin.write_all(b"\n").await.ok()?;
        self.stdin.flush().await.ok()?;
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining == Duration::ZERO {
                return None;
            }
            let received = tokio::time::timeout(remaining, self.lines.next_line())
                .await
                .ok()?;
            let line = received.ok()??;
            if let Some(rest) = line.trim_start().strip_prefix(reply_prefix) {
                return Some(rest.trim().to_owned());
            }
        }
    }

    /// Wait for a serial line starting with `prefix`.
    pub async fn wait_line(&mut self, prefix: &str) -> String {
        let deadline = Instant::now() + Duration::from_mins(1);
        let mut skipped: Vec<String> = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                remaining > Duration::ZERO,
                "guest never produced a {prefix:?} line; serial transcript: {skipped:?}"
            );
            if let Ok(Ok(Some(line))) =
                tokio::time::timeout(remaining, self.lines.next_line()).await
            {
                if let Some(rest) = line.trim_start().strip_prefix(prefix) {
                    return rest.trim().to_owned();
                }
                skipped.push(line);
            }
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.id().expect("Firecracker process is running")
    }

    /// Host death for this microVM.
    pub async fn kill(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

impl Drop for FcVm {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Physical memory of a process straight from the kernel's accounting:
/// (Rss, Pss) in bytes. Rss counts every resident page mapped; Pss divides
/// shared pages among their mappers — `Pss < Rss` IS the kernel saying
/// pages are shared.
pub async fn rss_pss_of_pid(pid: u32) -> (usize, usize) {
    let rollup = tokio::fs::read_to_string(format!("/proc/{pid}/smaps_rollup"))
        .await
        .expect("smaps");
    let field = |name: &str| -> usize {
        let kb: usize = rollup
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .expect("field present")
            .trim()
            .trim_end_matches(" kB")
            .trim()
            .parse()
            .expect("kB");
        kb * 1024
    };
    (field("Rss:"), field("Pss:"))
}

// ── the page-fault handler (Firecracker's external-memory-backend hook) ──

/// One guest-memory region as Firecracker describes it to its page-fault
/// handler at snapshot restore.
#[derive(Clone, Copy, Debug, Deserialize)]
struct Region {
    base_host_virt_addr: u64,
    size: u64,
    offset: u64,
}

async fn receive_uffd(stream: &tokio::net::UnixStream) -> (Vec<Region>, Uffd) {
    let mut buf = vec![0u8; 65_536];
    let (n, fd) = loop {
        stream.readable().await.expect("uffd handshake readiness");
        match stream.try_io(tokio::io::Interest::READABLE, || {
            recv_with_fd(stream, &mut buf)
        }) {
            Ok(result) => break result,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("receive uffd handshake: {error}"),
        }
    };
    let regions: Vec<Region> = serde_json::from_slice(&buf[..n])
        .unwrap_or_else(|error| panic!("invalid Firecracker uffd mapping: {error}"));
    assert!(!regions.is_empty(), "Firecracker sent no uffd regions");
    (
        regions,
        Uffd::from_fd_nonblocking(fd.expect("Firecracker sent no uffd descriptor")),
    )
}

/// Serve one restored microVM's page faults from its snapshot memory file:
/// accept Firecracker's handshake (region layout + the uffd via
/// `SCM_RIGHTS`), then fill every fault with `UFFDIO_COPY` from the file.
/// This is blockd's fill door running against a REAL VMM — each served
/// page is counted so tests can prove demand paging actually happened.
pub fn serve_uffd(
    listener: tokio::net::UnixListener,
    mem_file: PathBuf,
    served: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("uffd handshake");
        let (regions, uffd) = receive_uffd(&stream).await;
        let uffd = AsyncFd::new(uffd).expect("register userfaultfd");
        let mut file = tokio::fs::File::open(&mem_file).await.expect("mem file");
        let mut page = vec![0u8; page_size()];
        loop {
            let Ok(mut ready) = uffd.readable().await else {
                return;
            };
            let events = match ready.try_io(|inner| inner.get_ref().read_events()) {
                Ok(Ok(events)) => events,
                Ok(Err(_)) => return,
                Err(_) => continue,
            };
            for event in events {
                let addr = event.address as u64 & !(page_size() as u64 - 1);
                let region = regions
                    .iter()
                    .find(|r| {
                        addr >= r.base_host_virt_addr && addr < r.base_host_virt_addr + r.size
                    })
                    .expect("fault outside every region");
                let offset = addr - region.base_host_virt_addr + region.offset;
                file.seek(std::io::SeekFrom::Start(offset))
                    .await
                    .expect("seek memory snapshot");
                file.read_exact(&mut page).await.expect("mem read");
                match uffd
                    .get_ref()
                    .copy(usize::try_from(addr).expect("fits"), &page)
                {
                    Ok(()) => {
                        served.fetch_add(1, Ordering::SeqCst);
                    }
                    // A racing fault already resolved this page.
                    Err(e) if e.raw_os_error() == Some(libc_exist()) => {}
                    Err(e) => panic!("UFFDIO_COPY failed: {e}"),
                }
            }
        }
    })
}

fn libc_exist() -> i32 {
    17 // EEXIST
}

/// The shmem fill server: blockd's storage tier under a fleet of REAL
/// microVMs. It owns the shared-memory file every fork maps `MAP_PRIVATE`;
/// a MISSING fault from any fork populates the page ONCE from the
/// snapshot source and wakes the faulter — after which every fork maps
/// that one physical page straight from the page cache, no handler
/// involved. Hole-punching the file is backing reclaim; the next touch
/// faults back here and refills.
pub struct ShmemServer {
    parts: Arc<PartTable>,
    /// Faults answered (fills + wakes of already-populated pages).
    pub faults: Arc<AtomicU64>,
    /// Cumulative end-to-end shmem fault latency, bounded in memory.
    fault_latency: Arc<crate::metrics::AtomicHistogram>,
    accept_task: tokio::task::JoinHandle<()>,
}

impl ShmemServer {
    /// Create the shmem file (sparse) and start accepting handshakes from
    /// any number of restoring microVMs.
    pub async fn start(
        listener: tokio::net::UnixListener,
        source_mem: PathBuf,
        shmem_path: &Path,
        mem_bytes: u64,
    ) -> ShmemServer {
        let shmem = create_shmem(shmem_path, mem_bytes).await;
        let parts = PartTable::local(source_mem, &shmem, mem_bytes);
        ShmemServer::accepting(listener, parts)
    }

    /// The cold-tier variant: fills come from object storage at
    /// blx granularity — a fault on any page of a `part_bytes` part
    /// fetches the whole part object with one `GetObject` (the store tier
    /// of R2.3: blx-granular, never per-page round trips). Distinct
    /// parts fetch concurrently, concurrent faults on one part share one
    /// fetch, and each demand fault keeps the next `readahead_parts` parts
    /// in flight ahead of a sequential reader.
    pub async fn start_store(
        listener: tokio::net::UnixListener,
        store: Arc<dyn crate::store::ObjectStore>,
        prefix: String,
        part_bytes: u64,
        shmem_path: &Path,
        mem_bytes: u64,
        readahead_parts: u64,
    ) -> ShmemServer {
        let shmem = create_shmem(shmem_path, mem_bytes).await;
        let parts = PartTable::store(
            store,
            prefix,
            part_bytes,
            &shmem,
            mem_bytes,
            readahead_parts,
        );
        ShmemServer::accepting(listener, parts)
    }

    fn accepting(listener: tokio::net::UnixListener, parts: Arc<PartTable>) -> ShmemServer {
        let faults = Arc::new(AtomicU64::new(0));
        let fault_latency = Arc::new(crate::metrics::AtomicHistogram::default());
        let (task_parts, fault_count, latencies) =
            (parts.clone(), faults.clone(), fault_latency.clone());
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (parts, fault_count, latencies) =
                    (task_parts.clone(), fault_count.clone(), latencies.clone());
                tokio::spawn(async move {
                    serve_one_shmem(&stream, &parts, &fault_count, &latencies).await;
                });
            }
        });
        ShmemServer {
            parts,
            faults,
            fault_latency,
            accept_task,
        }
    }

    /// Pages filled from the source (unique work — the R5.3 measure).
    pub fn filled(&self) -> u64 {
        self.parts.filled.load(Ordering::SeqCst)
    }

    pub fn fault_latency(&self) -> crate::metrics::HistogramSnapshot {
        self.fault_latency.snapshot()
    }

    pub fn source(&self) -> &'static str {
        match &self.parts.filler {
            Filler::File(_) => "local_snapshot",
            Filler::Store { .. } => "object_store_snapshot",
        }
    }

    /// Physical bytes the shared base holds right now.
    pub async fn resident_bytes(&self) -> usize {
        let shmem = self.parts.shmem.clone();
        tokio::task::spawn_blocking(move || {
            blockd_hostmem::file_resident_bytes(&shmem).expect("fstat")
        })
        .await
        .expect("resident-byte task")
    }

    /// Backing reclaim (R2.7): free the whole base file. Forks' private
    /// copy-on-write pages survive; clean pages will refault and refill.
    pub async fn reclaim_all(&self, mem_bytes: u64) {
        self.parts.states.lock().expect("lock").clear();
        let shmem = self.parts.shmem.clone();
        tokio::task::spawn_blocking(move || {
            blockd_hostmem::punch_hole_file(&shmem, 0, mem_bytes).expect("punch");
        })
        .await
        .expect("reclaim task");
    }
}

impl Drop for ShmemServer {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn create_shmem(shmem_path: &Path, mem_bytes: u64) -> Arc<std::fs::File> {
    let shmem = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(shmem_path)
        .await
        .expect("shmem file");
    shmem.set_len(mem_bytes).await.expect("shmem size");
    Arc::new(shmem.into_std().await)
}

/// Where a shmem fill's bytes come from: the snapshot memory file (warm
/// local tier, page granules) or object storage (cold tier,
/// `part_bytes` blx-object granules).
enum Filler {
    File(PathBuf),
    Store {
        store: Arc<dyn crate::store::ObjectStore>,
        prefix: String,
        part_bytes: u64,
    },
}

impl Filler {
    /// Fill the granule starting at `base` into the shmem file.
    async fn fill(&self, shmem: Arc<std::fs::File>, base: u64, granule: u64) -> usize {
        match self {
            Filler::File(path) => {
                let path = path.clone();
                tokio::task::spawn_blocking(move || {
                    use std::os::unix::fs::FileExt;
                    let source = std::fs::File::open(path).expect("source mem");
                    let mut bytes = vec![0u8; usize::try_from(granule).expect("fits")];
                    source.read_exact_at(&mut bytes, base).expect("source read");
                    shmem.write_all_at(&bytes, base).expect("populate");
                    bytes.len()
                })
                .await
                .expect("snapshot fill task")
            }
            Filler::Store {
                store,
                prefix,
                part_bytes,
            } => {
                let part = base / part_bytes;
                let key = format!("{prefix}/{part:08}");
                let (_, bytes) = store
                    .clone()
                    .get(key)
                    .await
                    .expect("store up")
                    .expect("part object exists");
                let len = bytes.len();
                tokio::task::spawn_blocking(move || {
                    use std::os::unix::fs::FileExt;
                    shmem.write_all_at(&bytes, base).expect("populate");
                })
                .await
                .expect("snapshot populate task");
                len
            }
        }
    }
}

/// One fetch granule's state.
enum PartState {
    /// A fetch is in flight; late faulters park on it.
    Fetching(Arc<Fetch>),
    Ready,
}

/// A parked fault's wake action.
type Waker = Box<dyn FnOnce() + Send>;

/// An in-flight part fetch. Waiters are the wake actions of every fault
/// parked on it; `None` marks completion, so a faulter that raced the
/// finish wakes itself inline instead of parking forever.
struct Fetch {
    waiters: Mutex<Option<Vec<Waker>>>,
}

impl Fetch {
    fn new() -> Arc<Fetch> {
        Arc::new(Fetch {
            waiters: Mutex::new(Some(Vec::new())),
        })
    }

    /// Park `waker` until the fetch completes — or run it now if it
    /// already has.
    fn park(&self, waker: impl FnOnce() + Send + 'static) {
        let mut waiters = self.waiters.lock().expect("lock");
        if let Some(waiters) = waiters.as_mut() {
            waiters.push(Box::new(waker));
        } else {
            drop(waiters);
            waker();
        }
    }

    /// Complete: every parked waker runs, late parkers run inline.
    fn finish(&self) {
        let wakers = self
            .waiters
            .lock()
            .expect("lock")
            .take()
            .expect("finished once");
        for waker in wakers {
            waker();
        }
    }
}

/// The part-fetch engine behind [`ShmemServer`]: the cold path must never
/// serialize on the store's latency. Concurrent faults on one part share a
/// single in-flight fetch (one `GetObject` no matter how many forks storm
/// it); distinct parts fetch concurrently on bounded Tokio tasks; and each
/// demand fault keeps the next `readahead` parts in flight, so a
/// sequential reader streams at transfer speed instead of stalling
/// per-part. Fault callers hand over a wake action and never block —
/// a VM's uffd reader stays free to serve its other faults.
pub struct PartTable {
    granule: u64,
    mem_bytes: u64,
    /// Parts to keep in flight ahead of each demand fault (0 = none).
    readahead: u64,
    filler: Filler,
    shmem: Arc<std::fs::File>,
    /// Pages filled from the source (unique work — the R5.3 measure).
    pub filled: Arc<AtomicU64>,
    states: Mutex<BTreeMap<u64, PartState>>,
    fetch_tx: mpsc::Sender<FetchJob>,
}

type FetchJob = (u64, Arc<Fetch>);

const PART_FETCH_WORKERS: usize = 8;

impl PartTable {
    /// Warm local tier: page-granular fills from the snapshot memory file.
    /// Fills cost microseconds, so they run inline under the table lock —
    /// exactly one fill per page across every faulting fork.
    pub fn local(
        source_mem: PathBuf,
        shmem: &Arc<std::fs::File>,
        mem_bytes: u64,
    ) -> Arc<PartTable> {
        let (fetch_tx, fetch_rx) = mpsc::channel::<FetchJob>(256);
        let table = Arc::new(PartTable {
            granule: page_size() as u64,
            mem_bytes,
            readahead: 0,
            filler: Filler::File(source_mem),
            shmem: shmem.clone(),
            filled: Arc::new(AtomicU64::new(0)),
            states: Mutex::new(BTreeMap::new()),
            fetch_tx,
        });
        tokio::spawn(part_fetch_loop(fetch_rx, Arc::downgrade(&table)));
        table
    }

    /// Cold store tier: `part_bytes`-granular fetches, concurrent and
    /// deduplicated, with demand-triggered readahead.
    pub fn store(
        store: Arc<dyn crate::store::ObjectStore>,
        prefix: String,
        part_bytes: u64,
        shmem: &Arc<std::fs::File>,
        mem_bytes: u64,
        readahead: u64,
    ) -> Arc<PartTable> {
        let (fetch_tx, fetch_rx) = mpsc::channel::<FetchJob>(256);
        let table = Arc::new(PartTable {
            granule: part_bytes,
            mem_bytes,
            readahead,
            filler: Filler::Store {
                store,
                prefix,
                part_bytes,
            },
            shmem: shmem.clone(),
            filled: Arc::new(AtomicU64::new(0)),
            states: Mutex::new(BTreeMap::new()),
            fetch_tx,
        });
        let weak = Arc::downgrade(&table);
        tokio::spawn(part_fetch_loop(fetch_rx, weak));
        table
    }

    /// A demand fault at `offset`: run `waker` once the containing part is
    /// populated — inline if it already is. Never blocks on a fetch.
    pub fn fault(self: &Arc<PartTable>, offset: u64, waker: impl FnOnce() + Send + 'static) {
        let base = offset / self.granule * self.granule;
        match self.state_of(base) {
            None => waker(),
            Some(fetch) => fetch.park(waker),
        }
        // Demand-triggered readahead (never chained off readahead fills:
        // that would eagerly stream the whole memory).
        for ahead in 1..=self.readahead {
            let next = base + ahead * self.granule;
            if next < self.mem_bytes {
                self.state_of(next);
            }
        }
    }

    /// The part's fetch to park on (`None` = already populated), queueing
    /// one on the bounded fetch pool if this is the first fault to reach it.
    fn state_of(self: &Arc<PartTable>, base: u64) -> Option<Arc<Fetch>> {
        let mut states = self.states.lock().expect("lock");
        if let Some(state) = states.get(&base) {
            return match state {
                PartState::Ready => None,
                PartState::Fetching(fetch) => Some(fetch.clone()),
            };
        }
        let fetch = Fetch::new();
        states.insert(base, PartState::Fetching(fetch.clone()));
        drop(states);
        self.fetch_tx
            .try_send((base, fetch.clone()))
            .expect("part fetch task lives with table");
        Some(fetch)
    }

    /// Fetch one part and wake everyone parked on it.
    async fn fetch(self: Arc<Self>, base: u64, fetch: Arc<Fetch>) {
        let bytes = self
            .filler
            .fill(self.shmem.clone(), base, self.granule)
            .await;
        self.filled.fetch_add(
            u64::try_from(bytes)
                .expect("fill length fits")
                .div_ceil(page_size() as u64),
            Ordering::SeqCst,
        );
        {
            let mut states = self.states.lock().expect("lock");
            // A reclaim may have cleared the table mid-fetch: the punched
            // part must not be marked Ready (waking is still safe — a
            // still-missing page just refaults).
            if let Some(state @ PartState::Fetching(_)) = states.get_mut(&base) {
                *state = PartState::Ready;
            }
        }
        fetch.finish();
    }
}

async fn part_fetch_loop(mut rx: mpsc::Receiver<FetchJob>, table: std::sync::Weak<PartTable>) {
    let concurrency = Arc::new(tokio::sync::Semaphore::new(PART_FETCH_WORKERS));
    while let Some((base, fetch)) = rx.recv().await {
        let permit = concurrency
            .clone()
            .acquire_owned()
            .await
            .expect("part fetch task semaphore open");
        let Some(table) = table.upgrade() else {
            return;
        };
        tokio::spawn(async move {
            let _permit = permit;
            if std::panic::AssertUnwindSafe(table.fetch(base, fetch))
                .catch_unwind()
                .await
                .is_err()
            {
                tracing::error!(part_base = base, "snapshot part fetch failed");
            }
        });
    }
}

async fn serve_one_shmem(
    stream: &tokio::net::UnixStream,
    parts: &Arc<PartTable>,
    fault_count: &AtomicU64,
    latencies: &Arc<crate::metrics::AtomicHistogram>,
) {
    let (regions, uffd) = receive_uffd(stream).await;
    let uffd = Arc::new(AsyncFd::new(uffd).expect("register userfaultfd"));
    loop {
        let Ok(mut ready) = uffd.readable().await else {
            return;
        };
        let events = match ready.try_io(|inner| inner.get_ref().read_events()) {
            Ok(Ok(events)) => events,
            Ok(Err(_)) => return,
            Err(_) => continue,
        };
        for event in events {
            let started = Instant::now();
            let addr = event.address as u64 & !(page_size() as u64 - 1);
            let region = regions
                .iter()
                .find(|r| addr >= r.base_host_virt_addr && addr < r.base_host_virt_addr + r.size)
                .expect("fault outside every region");
            let offset = addr - region.base_host_virt_addr + region.offset;
            fault_count.fetch_add(1, Ordering::SeqCst);
            let (uffd, latencies) = (uffd.clone(), latencies.clone());
            parts.fault(offset, move || {
                uffd.get_ref()
                    .wake(usize::try_from(addr).expect("fits"), page_size())
                    .expect("wake");
                latencies.observe(started.elapsed());
            });
        }
    }
}

/// Asynchronous production uploader. File reading stays on Tokio's blocking
/// pool because regular files have no portable readiness API; object-store
/// requests themselves are async and capped at the same eight-way concurrency
/// as daemon store operations.
pub async fn upload_mem_parts_async(
    store: Arc<dyn crate::store::ObjectStore>,
    mem_path: PathBuf,
    prefix: String,
    part_bytes: u64,
) -> u64 {
    let part_bytes = usize::try_from(part_bytes).expect("fits");
    assert!(part_bytes > 0, "snapshot part size must be nonzero");
    // A one-part handoff queue starts the first upload as soon as it is read
    // and bounds buffered snapshot memory independently of the file size.
    let (part_tx, part_rx) = mpsc::channel::<(u64, Vec<u8>)>(1);
    let reader = tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(mem_path).expect("mem file");
        let mut part = 0u64;
        loop {
            let mut bytes = vec![0; part_bytes];
            let mut used = 0;
            while used < bytes.len() {
                let read = file.read(&mut bytes[used..]).expect("read snapshot part");
                if read == 0 {
                    break;
                }
                used += read;
            }
            if used == 0 {
                return part;
            }
            bytes.truncate(used);
            if part_tx.blocking_send((part, bytes)).is_err() {
                return part;
            }
            part += 1;
        }
    });
    let uploaded = futures_util::stream::unfold(part_rx, |mut receiver| async move {
        receiver.recv().await.map(|part| (part, receiver))
    })
    .map(|(part, bytes)| {
        let store = store.clone();
        let key = format!("{prefix}/{part:08}");
        async move {
            store.put(key, bytes).await.expect("upload part");
            part
        }
    })
    .buffer_unordered(PART_FETCH_WORKERS)
    .collect::<Vec<_>>()
    .await;
    let count = reader.await.expect("snapshot read task");
    assert_eq!(uploaded.len() as u64, count, "every read part was uploaded");
    count
}
