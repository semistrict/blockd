//! Driving REAL Firecracker microVMs: process control, the API socket
//! (HTTP/1.1 over a unix stream — machine config, boot, pause, snapshot,
//! restore), the serial console (the guest workload's command channel),
//! and the snapshot-restore page-fault handler — Firecracker's designed-in
//! integration point for external memory backends like blockd: on
//! `/snapshot/load` with a `Uffd` backend it hands us the guest-memory
//! userfaultfd over a unix socket, and every guest touch becomes OUR fill.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

use blockd_hostmem::{PAGE_SIZE, Uffd, recv_with_fd};

/// One HTTP request over the Firecracker API socket.
fn api_request(sock: &Path, method: &str, path: &str, body: &str) -> (u16, String) {
    // The socket file appears at bind time, a beat before Firecracker
    // listens — connect with retry instead of racing it.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        match UnixStream::connect(sock) {
            Ok(stream) => break stream,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) && Instant::now() < deadline =>
            {
                thread::park_timeout(Duration::from_millis(5));
            }
            Err(e) => panic!("api socket: {e}"),
        }
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAccept: application/json\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("api write");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("timeout");
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    let (headers_end, header_text) = loop {
        let n = stream.read(&mut buf).expect("api read");
        assert!(n > 0, "api connection closed mid-response");
        raw.extend_from_slice(&buf[..n]);
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break (pos + 4, String::from_utf8_lossy(&raw[..pos]).into_owned());
        }
    };
    let status: u16 = header_text
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status code");
    let content_length: usize = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().expect("length"))
        })
        .unwrap_or(0);
    let mut body_bytes = raw[headers_end..].to_vec();
    while body_bytes.len() < content_length {
        let n = stream.read(&mut buf).expect("api read body");
        assert!(n > 0, "api connection closed mid-body");
        body_bytes.extend_from_slice(&buf[..n]);
    }
    (status, String::from_utf8_lossy(&body_bytes).into_owned())
}

/// A running Firecracker process with its API socket and serial console.
pub struct FcVm {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
    api_sock: PathBuf,
}

impl FcVm {
    /// Spawn Firecracker with an API socket; stdout is the guest serial.
    pub fn spawn(fc_bin: &Path, api_sock: &Path) -> FcVm {
        let _ = std::fs::remove_file(api_sock);
        let mut child = Command::new(fc_bin)
            .arg("--api-sock")
            .arg(api_sock)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn firecracker");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let (line_tx, lines) = channel();
        thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if line_tx.send(line).is_err() {
                    break;
                }
            }
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while !api_sock.exists() {
            assert!(Instant::now() < deadline, "api socket never appeared");
            thread::park_timeout(Duration::from_millis(10));
        }
        FcVm {
            child,
            stdin,
            lines,
            api_sock: api_sock.to_owned(),
        }
    }

    pub fn api(&self, method: &str, path: &str, body: &str) {
        let (status, reply) = api_request(&self.api_sock, method, path, body);
        assert!(
            (200..300).contains(&status),
            "{method} {path} failed: {status} {reply}"
        );
    }

    /// Configure and boot a fresh microVM from kernel + initramfs (no
    /// disks: the workload lives in the initramfs, so forks never contend
    /// on a writable block device).
    pub fn boot(&self, kernel: &Path, initrd: &Path, mem_mib: u32) {
        self.api(
            "PUT",
            "/machine-config",
            &format!("{{\"vcpu_count\": 1, \"mem_size_mib\": {mem_mib}}}"),
        );
        self.api(
            "PUT",
            "/boot-source",
            &format!(
                "{{\"kernel_image_path\": \"{}\", \"initrd_path\": \"{}\", \
                 \"boot_args\": \"keep_bootcon console=ttyS0 reboot=k panic=-1 quiet \
                 rdinit=/init\"}}",
                kernel.display(),
                initrd.display()
            ),
        );
        self.api("PUT", "/actions", "{\"action_type\": \"InstanceStart\"}");
    }

    pub fn pause(&self) {
        self.api("PATCH", "/vm", "{\"state\": \"Paused\"}");
    }

    pub fn resume(&self) {
        self.api("PATCH", "/vm", "{\"state\": \"Resumed\"}");
    }

    /// Full snapshot of a paused microVM: vmstate + guest memory file.
    pub fn snapshot(&self, snapshot_path: &Path, mem_path: &Path) {
        self.api(
            "PUT",
            "/snapshot/create",
            &format!(
                "{{\"snapshot_type\": \"Full\", \"snapshot_path\": \"{}\", \
                 \"mem_file_path\": \"{}\"}}",
                snapshot_path.display(),
                mem_path.display()
            ),
        );
    }

    /// Restore from a snapshot with the patched `UffdShmem` backend:
    /// guest memory is a `MAP_PRIVATE` mapping of the handler-owned
    /// shared-memory file; missing faults reach the handler through the
    /// socket. Forks restored from one snapshot share every clean page
    /// physically; writes diverge via copy-on-write.
    pub fn load_snapshot_shmem(&self, snapshot_path: &Path, uffd_sock: &Path, shmem: &Path) {
        self.api(
            "PUT",
            "/snapshot/load",
            &format!(
                "{{\"snapshot_path\": \"{}\", \"mem_backend\": \
                 {{\"backend_type\": \"UffdShmem\", \"backend_path\": \"{}\", \
                 \"shmem_path\": \"{}\"}}, \"resume_vm\": true}}",
                snapshot_path.display(),
                uffd_sock.display(),
                shmem.display()
            ),
        );
    }

    /// Restore (fork) from a snapshot. `uffd_sock` selects the memory
    /// backend: `None` maps the memory file directly (kernel page-cache
    /// sharing across forks); `Some` hands the guest memory to OUR
    /// page-fault handler over the socket (blockd's fill door).
    pub fn load_snapshot(&self, snapshot_path: &Path, mem_path: &Path, uffd_sock: Option<&Path>) {
        let backend = match uffd_sock {
            None => format!(
                "{{\"backend_type\": \"File\", \"backend_path\": \"{}\"}}",
                mem_path.display()
            ),
            Some(sock) => format!(
                "{{\"backend_type\": \"Uffd\", \"backend_path\": \"{}\"}}",
                sock.display()
            ),
        };
        self.api(
            "PUT",
            "/snapshot/load",
            &format!(
                "{{\"snapshot_path\": \"{}\", \"mem_backend\": {backend}, \
                 \"resume_vm\": true}}",
                snapshot_path.display()
            ),
        );
    }

    /// Send one workload command and wait for its reply line (replies are
    /// uppercase; the tty echo of commands can never match).
    pub fn cmd(&mut self, command: &str, reply_prefix: &str) -> String {
        writeln!(self.stdin, "{command}").expect("serial write");
        self.stdin.flush().expect("serial flush");
        self.wait_line(reply_prefix)
    }

    /// Wait for a serial line starting with `prefix`.
    pub fn wait_line(&mut self, prefix: &str) -> String {
        let deadline = Instant::now() + Duration::from_mins(1);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                remaining > Duration::ZERO,
                "guest never produced a {prefix:?} line"
            );
            if let Ok(line) = self.lines.recv_timeout(remaining)
                && let Some(rest) = line.trim_start().strip_prefix(prefix)
            {
                return rest.trim().to_owned();
            }
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Host death for this microVM.
    pub fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for FcVm {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Physical memory of a process straight from the kernel's accounting:
/// (Rss, Pss) in bytes. Rss counts every resident page mapped; Pss divides
/// shared pages among their mappers — `Pss < Rss` IS the kernel saying
/// pages are shared.
pub fn rss_pss_of_pid(pid: u32) -> (usize, usize) {
    let rollup = std::fs::read_to_string(format!("/proc/{pid}/smaps_rollup")).expect("smaps");
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
#[derive(Clone, Copy, Debug)]
struct Region {
    base_host_virt_addr: u64,
    size: u64,
    offset: u64,
}

fn json_field(object: &str, key: &str) -> u64 {
    let at = object.find(&format!("\"{key}\"")).expect("key present");
    let rest = &object[at..];
    let colon = rest.find(':').expect("colon");
    rest[colon + 1..]
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .expect("number")
}

fn parse_regions(body: &str) -> Vec<Region> {
    let mut regions = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find('{') {
        let end = rest[start..].find('}').expect("object end") + start;
        let object = &rest[start..=end];
        regions.push(Region {
            base_host_virt_addr: json_field(object, "base_host_virt_addr"),
            size: json_field(object, "size"),
            offset: json_field(object, "offset"),
        });
        rest = &rest[end + 1..];
    }
    regions
}

/// Serve one restored microVM's page faults from its snapshot memory file:
/// accept Firecracker's handshake (region layout + the uffd via
/// `SCM_RIGHTS`), then fill every fault with `UFFDIO_COPY` from the file.
/// This is blockd's fill door running against a REAL VMM — each served
/// page is counted so tests can prove demand paging actually happened.
pub fn serve_uffd(listener: UnixListener, mem_file: PathBuf, served: Arc<AtomicU64>) {
    thread::spawn(move || {
        use std::os::unix::fs::FileExt;
        let (stream, _) = listener.accept().expect("uffd handshake");
        let mut buf = vec![0u8; 65536];
        let (n, fd) = recv_with_fd(&stream, &mut buf).expect("recv uffd");
        let body = String::from_utf8_lossy(&buf[..n]).into_owned();
        let regions = parse_regions(&body);
        assert!(!regions.is_empty(), "no regions in handshake: {body}");
        let uffd = Uffd::from_fd(fd.expect("uffd fd"));
        let file = std::fs::File::open(&mem_file).expect("mem file");
        let mut page = vec![0u8; PAGE_SIZE];
        while let Ok(event) = uffd.read_event() {
            let addr = event.address as u64 & !(PAGE_SIZE as u64 - 1);
            let region = regions
                .iter()
                .find(|r| addr >= r.base_host_virt_addr && addr < r.base_host_virt_addr + r.size)
                .expect("fault outside every region");
            let offset = addr - region.base_host_virt_addr + region.offset;
            file.read_exact_at(&mut page, offset).expect("mem read");
            match uffd.copy(usize::try_from(addr).expect("fits"), &page) {
                Ok(()) => {
                    served.fetch_add(1, Ordering::SeqCst);
                }
                // A racing fault already resolved this page.
                Err(e) if e.raw_os_error() == Some(libc_exist()) => {}
                Err(e) => panic!("UFFDIO_COPY failed: {e}"),
            }
        }
    });
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
    /// Per-fault service time in microseconds (the fault-latency profile).
    pub fault_micros: Arc<Mutex<Vec<u64>>>,
}

impl ShmemServer {
    /// Create the shmem file (sparse) and start accepting handshakes from
    /// any number of restoring microVMs.
    pub fn start(
        listener: UnixListener,
        source_mem: PathBuf,
        shmem_path: &Path,
        mem_bytes: u64,
    ) -> ShmemServer {
        let parts = PartTable::local(source_mem, &create_shmem(shmem_path, mem_bytes), mem_bytes);
        ShmemServer::accepting(listener, parts)
    }

    /// The cold-tier variant: fills come from the S3-shaped store at
    /// segment granularity — a fault on any page of a `part_bytes` part
    /// fetches the whole part object with one `GetObject` (the store tier
    /// of R2.3: segment-granular, never per-page round trips). Distinct
    /// parts fetch concurrently, concurrent faults on one part share one
    /// fetch, and each demand fault keeps the next `readahead_parts` parts
    /// in flight ahead of a sequential reader.
    pub fn start_s3(
        listener: UnixListener,
        store: Arc<crate::s3::S3Store>,
        prefix: String,
        part_bytes: u64,
        shmem_path: &Path,
        mem_bytes: u64,
        readahead_parts: u64,
    ) -> ShmemServer {
        let parts = PartTable::store(
            store,
            prefix,
            part_bytes,
            &create_shmem(shmem_path, mem_bytes),
            mem_bytes,
            readahead_parts,
        );
        ShmemServer::accepting(listener, parts)
    }

    fn accepting(listener: UnixListener, parts: Arc<PartTable>) -> ShmemServer {
        let server = ShmemServer {
            parts,
            faults: Arc::new(AtomicU64::new(0)),
            fault_micros: Arc::new(Mutex::new(Vec::new())),
        };
        let (parts, fault_count, latencies) = (
            server.parts.clone(),
            server.faults.clone(),
            server.fault_micros.clone(),
        );
        thread::spawn(move || {
            loop {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                let (parts, fault_count, latencies) =
                    (parts.clone(), fault_count.clone(), latencies.clone());
                thread::spawn(move || {
                    serve_one_shmem(&stream, &parts, &fault_count, &latencies);
                });
            }
        });
        server
    }

    /// Pages filled from the source (unique work — the R5.3 measure).
    pub fn filled(&self) -> u64 {
        self.parts.filled.load(Ordering::SeqCst)
    }

    /// Physical bytes the shared base holds right now.
    pub fn resident_bytes(&self) -> usize {
        blockd_hostmem::file_resident_bytes(&self.parts.shmem).expect("fstat")
    }

    /// Backing reclaim (R2.7): free the whole base file. Forks' private
    /// copy-on-write pages survive; clean pages will refault and refill.
    pub fn reclaim_all(&self, mem_bytes: u64) {
        self.parts.states.lock().expect("lock").clear();
        blockd_hostmem::punch_hole_file(&self.parts.shmem, 0, mem_bytes).expect("punch");
    }
}

fn create_shmem(shmem_path: &Path, mem_bytes: u64) -> Arc<std::fs::File> {
    let shmem = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(shmem_path)
        .expect("shmem file");
    shmem.set_len(mem_bytes).expect("shmem size");
    Arc::new(shmem)
}

/// Where a shmem fill's bytes come from: the snapshot memory file (warm
/// local tier, page granules) or the S3-shaped store (cold tier,
/// `part_bytes` segment-object granules).
enum Filler {
    File(PathBuf),
    Store {
        store: Arc<crate::s3::S3Store>,
        prefix: String,
        part_bytes: u64,
    },
}

impl Filler {
    /// Fill the granule starting at `base` into the shmem file.
    fn fill(&self, shmem: &std::fs::File, base: u64, granule: u64) {
        use std::os::unix::fs::FileExt;
        match self {
            Filler::File(path) => {
                let source = std::fs::File::open(path).expect("source mem");
                let mut bytes = vec![0u8; usize::try_from(granule).expect("fits")];
                source.read_exact_at(&mut bytes, base).expect("source read");
                shmem.write_all_at(&bytes, base).expect("populate");
            }
            Filler::Store {
                store,
                prefix,
                part_bytes,
            } => {
                let part = base / part_bytes;
                let key = format!("{prefix}/{part:08}");
                let (_, bytes) = store
                    .get(&key)
                    .expect("store up")
                    .expect("part object exists");
                shmem.write_all_at(&bytes, base).expect("populate");
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
/// it); distinct parts fetch concurrently on their own threads; and each
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
}

impl PartTable {
    /// Warm local tier: page-granular fills from the snapshot memory file.
    /// Fills cost microseconds, so they run inline under the table lock —
    /// exactly one fill per page across every faulting fork.
    pub fn local(
        source_mem: PathBuf,
        shmem: &Arc<std::fs::File>,
        mem_bytes: u64,
    ) -> Arc<PartTable> {
        Arc::new(PartTable {
            granule: PAGE_SIZE as u64,
            mem_bytes,
            readahead: 0,
            filler: Filler::File(source_mem),
            shmem: shmem.clone(),
            filled: Arc::new(AtomicU64::new(0)),
            states: Mutex::new(BTreeMap::new()),
        })
    }

    /// Cold store tier: `part_bytes`-granular fetches, concurrent and
    /// deduplicated, with demand-triggered readahead.
    pub fn store(
        store: Arc<crate::s3::S3Store>,
        prefix: String,
        part_bytes: u64,
        shmem: &Arc<std::fs::File>,
        mem_bytes: u64,
        readahead: u64,
    ) -> Arc<PartTable> {
        Arc::new(PartTable {
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
        })
    }

    /// A demand fault at `offset`: run `waker` once the containing part is
    /// populated — inline if it already is. Never blocks on a fetch.
    pub fn fault(self: &Arc<PartTable>, offset: u64, waker: impl FnOnce() + Send + 'static) {
        let base = offset / self.granule * self.granule;
        if matches!(self.filler, Filler::File(_)) {
            // Local fills are microseconds: fill under the lock, like a
            // page-cache hit path. ONE fill serves every fork — the page
            // lands in the shared cache where all MAP_PRIVATE mappers
            // find it.
            let mut states = self.states.lock().expect("lock");
            if !matches!(states.get(&base), Some(PartState::Ready)) {
                self.filler.fill(&self.shmem, base, self.granule);
                self.filled.fetch_add(1, Ordering::SeqCst);
                states.insert(base, PartState::Ready);
            }
            drop(states);
            waker();
            return;
        }
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

    /// The part's fetch to park on (`None` = already populated), starting
    /// one on a fresh thread if this is the first fault to reach it.
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
        let (table, started) = (self.clone(), fetch.clone());
        thread::spawn(move || table.fetch(base, &started));
        Some(fetch)
    }

    /// Fetch one part (on its own thread) and wake everyone parked on it.
    fn fetch(self: &Arc<PartTable>, base: u64, fetch: &Arc<Fetch>) {
        self.filler.fill(&self.shmem, base, self.granule);
        self.filled
            .fetch_add(self.granule / PAGE_SIZE as u64, Ordering::SeqCst);
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

fn serve_one_shmem(
    stream: &UnixStream,
    parts: &Arc<PartTable>,
    fault_count: &AtomicU64,
    latencies: &Arc<Mutex<Vec<u64>>>,
) {
    let mut buf = vec![0u8; 65536];
    let (n, fd) = recv_with_fd(stream, &mut buf).expect("recv uffd");
    let body = String::from_utf8_lossy(&buf[..n]).into_owned();
    let regions = parse_regions(&body);
    assert!(!regions.is_empty(), "no regions in handshake: {body}");
    let uffd = Arc::new(Uffd::from_fd(fd.expect("uffd fd")));
    while let Ok(event) = uffd.read_event() {
        let started = Instant::now();
        let addr = event.address as u64 & !(PAGE_SIZE as u64 - 1);
        let region = regions
            .iter()
            .find(|r| addr >= r.base_host_virt_addr && addr < r.base_host_virt_addr + r.size)
            .expect("fault outside every region");
        let offset = addr - region.base_host_virt_addr + region.offset;
        fault_count.fetch_add(1, Ordering::SeqCst);
        let (uffd, latencies) = (uffd.clone(), latencies.clone());
        parts.fault(offset, move || {
            uffd.wake(usize::try_from(addr).expect("fits"), PAGE_SIZE)
                .expect("wake");
            latencies
                .lock()
                .expect("lock")
                .push(u64::try_from(started.elapsed().as_micros()).expect("fits"));
        });
    }
}

/// Upload a snapshot memory file into the store as `part_bytes`-sized
/// segment objects under `prefix` (each well inside the 64 MiB object
/// contract, R4.6). Returns the part count.
pub fn upload_mem_parts(
    store: &crate::s3::S3Store,
    mem_path: &Path,
    prefix: &str,
    part_bytes: u64,
) -> u64 {
    let bytes = std::fs::read(mem_path).expect("mem file");
    let mut part = 0u64;
    for chunk in bytes.chunks(usize::try_from(part_bytes).expect("fits")) {
        store
            .put(&format!("{prefix}/{part:08}"), chunk.to_vec())
            .expect("upload part");
        part += 1;
    }
    part
}
