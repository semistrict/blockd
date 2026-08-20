#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use blockd_core::authority::HostSessionRecord;
use blockd_core::layout::peer_membership_prefix;
use blockd_core::types::HostId;
use blockd_runtime::fakegcs::FakeGcs;
use blockd_runtime::{GcsConfig, GcsStore, ObjectStore};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const TEST_NAME: &str = "shipped_serve_cluster_survives_real_sigkill_and_parent_swap";

fn blockd_binary() -> &'static str {
    env!("CARGO_BIN_EXE_blockd")
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("ephemeral listener")
        .local_addr()
        .expect("ephemeral address")
}

fn sha256(path: &Path) -> String {
    use std::fmt::Write as _;

    let bytes = std::fs::read(path).expect("read executable fixture");
    let digest = ring::digest::digest(&ring::digest::SHA256, &bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest.as_ref() {
        write!(encoded, "{byte:02x}").expect("string write");
    }
    encoded
}

struct ChildGuard {
    child: Option<tokio::process::Child>,
}

impl ChildGuard {
    fn spawn(command: &mut tokio::process::Command) -> std::io::Result<Self> {
        command.kill_on_drop(true);
        command.spawn().map(|child| Self { child: Some(child) })
    }

    fn id(&self) -> u32 {
        self.child
            .as_ref()
            .and_then(tokio::process::Child::id)
            .expect("live child pid")
    }

    async fn kill_and_wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        let child = self.child.as_mut().expect("live child");
        let _ = child.start_kill();
        let status = child.wait().await;
        if status.is_ok() {
            self.child.take();
        }
        status
    }

    async fn wait_with_output(mut self) -> std::io::Result<std::process::Output> {
        self.child
            .take()
            .expect("live child")
            .wait_with_output()
            .await
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.start_kill();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.wait().await;
            });
        }
    }
}

struct XfsNode {
    root: PathBuf,
    mounted: bool,
}

impl XfsNode {
    fn create(base: &Path, index: usize) -> Self {
        let root = base.join(format!("node-{index}"));
        let data = root.join("state");
        let blobs = data.join("blobs");
        std::fs::create_dir_all(&blobs).expect("node directories");
        let image = root.join("data.xfs");
        std::fs::File::create(&image)
            .and_then(|file| file.set_len(512 * 1024 * 1024))
            .expect("sparse XFS image");
        assert!(
            std::process::Command::new("/usr/sbin/mkfs.xfs")
                .args(["-f", "-m", "reflink=0"])
                .arg(&image)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("mkfs.xfs")
                .success(),
            "mkfs.xfs failed"
        );
        assert!(
            std::process::Command::new("/usr/bin/mount")
                .args(["-o", "loop"])
                .arg(&image)
                .arg(&blobs)
                .status()
                .expect("mount XFS")
                .success(),
            "XFS mount failed"
        );
        Self {
            root,
            mounted: true,
        }
    }

    fn data_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    fn relocate(&mut self, destination: PathBuf) {
        std::fs::rename(&self.root, &destination).expect("rename mounted node hierarchy");
        self.root = destination;
    }
}

impl Drop for XfsNode {
    fn drop(&mut self) {
        if self.mounted {
            let _ = std::process::Command::new("/usr/bin/umount")
                .arg(self.root.join("state/blobs"))
                .status();
            self.mounted = false;
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[derive(Clone)]
struct NodeArgs {
    data_dir: PathBuf,
    peer: SocketAddr,
    health: SocketAddr,
    endpoint: String,
    firecracker: PathBuf,
    firecracker_sha256: String,
    test_control: bool,
}

impl NodeArgs {
    fn command(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(blockd_binary());
        command
            .arg("serve")
            .arg("gs://cluster/shipped-entry/")
            .args(["--data-dir", self.data_dir.to_str().expect("UTF-8 path")])
            .args(["--peer", &self.peer.to_string()])
            .args(["--health", &self.health.to_string()])
            .args(["--capacity-bytes", "268435456"])
            .args(["--headroom-bytes", "67108864"])
            .args([
                "--firecracker",
                self.firecracker.to_str().expect("UTF-8 path"),
            ])
            .args(["--firecracker-sha256", &self.firecracker_sha256])
            .args(["--gcs-endpoint", &self.endpoint])
            .args(["--metadata-endpoint", &self.endpoint])
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if self.test_control {
            command.arg("--enable-shipped-test-control");
        }
        command
    }

    fn control(&self) -> PathBuf {
        self.data_dir.join("control.sock")
    }
}

async fn wait_ready(address: SocketAddr) {
    let client = reqwest::Client::new();
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if let Ok(response) = client.get(format!("http://{address}/ready")).send().await
                && response.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("shipped daemon readiness");
}

async fn control(path: &Path, command: &str) -> serde_json::Value {
    let mut stream = tokio::net::UnixStream::connect(path)
        .await
        .expect("connect control socket");
    stream
        .write_all(format!("{command}\n").as_bytes())
        .await
        .expect("write control command");
    stream.shutdown().await.expect("finish control request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read control response");
    serde_json::from_slice(&response).expect("JSON control response")
}

async fn active_sessions(store: &Arc<GcsStore>) -> BTreeSet<HostId> {
    let mut sessions = BTreeSet::new();
    for key in Arc::clone(store)
        .list_prefix("hosts/".to_owned())
        .await
        .expect("list host sessions")
    {
        if !key.ends_with("/session") {
            continue;
        }
        let Some((_, bytes)) = Arc::clone(store)
            .get(key.clone())
            .await
            .expect("get session")
        else {
            continue;
        };
        if matches!(
            HostSessionRecord::decode(&bytes),
            Ok(HostSessionRecord::Active { .. })
        ) {
            let encoded = key
                .strip_prefix("hosts/")
                .and_then(|suffix| suffix.strip_suffix("/session"))
                .expect("host session key shape");
            let host = u32::from_str_radix(encoded, 16).expect("host session key ID");
            sessions.insert(HostId::new(host));
        }
    }
    sessions
}

#[tokio::test]
async fn shipped_entry_rejects_intermediate_symlink_without_outside_mutation() {
    let root = tempfile::Builder::new()
        .prefix("blockd-shipped-symlink-")
        .tempdir_in("/var/tmp")
        .expect("root-disk temporary directory");
    let outside = root.path().join("outside");
    let safe = root.path().join("safe");
    std::fs::create_dir_all(&outside).expect("outside directory");
    std::fs::create_dir_all(&safe).expect("safe directory");
    symlink(&outside, safe.join("redirect")).expect("intermediate symlink");
    let data_dir = safe.join("redirect/state");
    let firecracker = PathBuf::from("/bin/true");
    let args = NodeArgs {
        data_dir,
        peer: free_addr(),
        health: free_addr(),
        endpoint: "http://127.0.0.1:1".to_owned(),
        firecracker_sha256: sha256(&firecracker),
        firecracker,
        test_control: false,
    };
    let output = ChildGuard::spawn(&mut args.command())
        .expect("spawn shipped daemon")
        .wait_with_output()
        .await
        .expect("shipped daemon output");
    assert!(!output.status.success(), "unsafe path unexpectedly started");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("data directory setup failed"), "{stderr}");
    assert!(!outside.join("state").exists(), "outside state was created");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn shipped_serve_cluster_survives_real_sigkill_and_parent_swap() {
    if rustix::process::geteuid().as_raw() != 0 {
        let output = tokio::process::Command::new("/usr/bin/sudo")
            .args(["-n", "/usr/bin/env", "BLOCKD_SHIPPED_ROOT_REEXEC=1"])
            .arg(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST_NAME, "--nocapture"])
            .output()
            .await
            .expect("re-exec shipped test as root");
        assert!(
            output.status.success(),
            "root shipped test failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let base_dir = tempfile::Builder::new()
        .prefix("blockd-shipped-cluster-")
        .tempdir_in("/var/tmp")
        .expect("root-disk cluster directory");
    let base = base_dir.path().to_path_buf();
    let mut mounts = (0..3)
        .map(|index| XfsNode::create(&base, index))
        .collect::<Vec<_>>();
    let (_fake, endpoint) = FakeGcs::start().await;
    let store = Arc::new(GcsStore::new(GcsConfig {
        bucket: "cluster".to_owned(),
        prefix: "shipped-entry/".to_owned(),
        endpoint: endpoint.clone(),
        metadata_endpoint: endpoint.clone(),
    }));
    let firecracker = PathBuf::from("/bin/true");
    let digest = sha256(&firecracker);
    let mut nodes = mounts
        .iter()
        .enumerate()
        .map(|(index, mount)| NodeArgs {
            data_dir: mount.data_dir(),
            peer: free_addr(),
            health: free_addr(),
            endpoint: endpoint.clone(),
            firecracker: firecracker.clone(),
            firecracker_sha256: digest.clone(),
            test_control: index != 0,
        })
        .collect::<Vec<_>>();
    let mut children = nodes
        .iter()
        .map(|node| ChildGuard::spawn(&mut node.command()).expect("spawn shipped daemon"))
        .collect::<Vec<_>>();

    let node_zero_lock = nodes[0].data_dir.join("node.lock");
    tokio::time::timeout(Duration::from_secs(20), async {
        while !node_zero_lock.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("node 0 anchored startup");
    let original_root = mounts[0].root.clone();
    let parked_root = base.join("node-0-parked");
    mounts[0].relocate(parked_root.clone());
    nodes[0].data_dir = parked_root.join("state");
    let substitute_data = original_root.join("state");
    std::fs::create_dir_all(&substitute_data).expect("substitute state hierarchy");
    let victim = base.join("outside-victim");
    std::fs::write(&victim, b"untouched").expect("outside victim");
    symlink(&victim, substitute_data.join("control.sock")).expect("control substitution");

    for node in &nodes {
        wait_ready(node.health).await;
    }
    let rejected_probe = control(
        &nodes[0].control(),
        r#"{"operation":"write-page","volume":0,"page":4294967295,"value":1}"#,
    )
    .await;
    assert_eq!(
        rejected_probe["error"],
        "operation is unavailable on the production control protocol"
    );
    assert!(
        control(&nodes[0].control(), r#"{"operation":"inventory"}"#).await["volumes"].is_array(),
        "rejected test probe crashed the default daemon"
    );
    assert!(
        std::fs::symlink_metadata(substitute_data.join("control.sock"))
            .expect("substitution remains")
            .file_type()
            .is_symlink(),
        "shipped control listener touched the swapped parent"
    );
    assert_eq!(
        std::fs::read(&victim).expect("victim readable"),
        b"untouched"
    );

    let membership_before = Arc::clone(&store)
        .list_prefix(peer_membership_prefix())
        .await
        .expect("membership before crash")
        .into_iter()
        .collect::<BTreeSet<_>>();
    let sessions_before = active_sessions(&store).await;
    assert_eq!(membership_before.len(), 3);
    assert_eq!(sessions_before.len(), 3);

    let mut owner = None;
    for volume in 100_u64..400 {
        for (index, node) in nodes.iter().enumerate().skip(1) {
            let response = control(
                &node.control(),
                &format!(
                    "{{\"operation\":\"create\",\"volume\":{volume},\"pages\":8,\"kind\":\"data\"}}"
                ),
            )
            .await;
            if response.get("created").and_then(serde_json::Value::as_u64) == Some(volume) {
                owner = Some((index, volume));
                break;
            }
        }
        if owner.is_some() {
            break;
        }
    }
    let (owner, volume) = owner.expect("volume owned by a non-swapped daemon");
    let value = 0x1122_3344_5566_7788_u64;
    assert_eq!(
        control(
            &nodes[owner].control(),
            &format!(
                "{{\"operation\":\"write-page\",\"volume\":{volume},\"page\":0,\"value\":{value}}}"
            ),
        )
        .await["written"],
        value
    );
    assert_eq!(
        control(
            &nodes[owner].control(),
            &format!("{{\"operation\":\"sync\",\"volume\":{volume}}}"),
        )
        .await["synced"],
        true
    );

    let crashed_pid = children[owner].id();
    assert!(
        tokio::process::Command::new("/bin/kill")
            .args(["-KILL", &crashed_pid.to_string()])
            .status()
            .await
            .expect("SIGKILL shipped daemon")
            .success()
    );
    let crashed = children[owner]
        .child
        .as_mut()
        .expect("crashed child")
        .wait()
        .await
        .expect("reap crashed daemon");
    children[owner].child.take();
    assert!(!crashed.success(), "SIGKILL daemon exited successfully");

    children[owner] = ChildGuard::spawn(&mut nodes[owner].command()).expect("restart daemon");
    wait_ready(nodes[owner].health).await;
    let inventory = control(&nodes[owner].control(), r#"{"operation":"inventory"}"#).await;
    assert!(
        inventory["volumes"]
            .as_array()
            .expect("volume inventory")
            .iter()
            .any(|entry| entry["volume"] == volume),
        "restarted daemon lost volume inventory: {inventory}"
    );
    assert_eq!(
        control(
            &nodes[owner].control(),
            &format!("{{\"operation\":\"read-page\",\"volume\":{volume},\"page\":0}}"),
        )
        .await["value"],
        value
    );

    let membership_after = Arc::clone(&store)
        .list_prefix(peer_membership_prefix())
        .await
        .expect("membership after restart")
        .into_iter()
        .collect::<BTreeSet<_>>();
    let sessions_after = active_sessions(&store).await;
    assert_eq!(membership_after, membership_before);
    assert_eq!(sessions_after, sessions_before);

    for child in &mut children {
        let _ = child.kill_and_wait().await;
    }

    let occupied = std::net::TcpListener::bind(nodes[owner].peer).expect("occupy peer port");
    let occupied_output = tokio::time::timeout(
        Duration::from_secs(20),
        ChildGuard::spawn(&mut nodes[owner].command())
            .expect("spawn occupied-port daemon")
            .wait_with_output(),
    )
    .await
    .expect("occupied-port daemon exited")
    .expect("occupied-port output");
    drop(occupied);
    let occupied_stderr = String::from_utf8_lossy(&occupied_output.stderr);
    assert!(
        !occupied_output.status.success(),
        "occupied port exited zero"
    );
    assert!(
        occupied_stderr.contains("runtime startup failed")
            && occupied_stderr.contains("peer listener startup failed")
            && occupied_stderr.contains("node_id=")
            && occupied_stderr.contains("cluster_id="),
        "{occupied_stderr}"
    );

    let mut unavailable = nodes[owner].clone();
    unavailable.endpoint = format!("http://{}", free_addr());
    unavailable.health = free_addr();
    let unavailable_output = tokio::time::timeout(
        Duration::from_secs(20),
        ChildGuard::spawn(&mut unavailable.command())
            .expect("spawn unavailable-store daemon")
            .wait_with_output(),
    )
    .await
    .expect("unavailable-store daemon exited")
    .expect("unavailable-store output");
    let unavailable_stderr = String::from_utf8_lossy(&unavailable_output.stderr);
    assert!(
        !unavailable_output.status.success(),
        "store failure exited zero"
    );
    assert!(
        unavailable_stderr.contains("cluster bootstrap failed"),
        "{unavailable_stderr}"
    );

    assert!(
        std::fs::symlink_metadata(substitute_data.join("control.sock"))
            .expect("substitution remains after cleanup")
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        std::fs::read(&victim).expect("victim readable"),
        b"untouched"
    );
    std::fs::remove_dir_all(&original_root).expect("remove substitute hierarchy");
    drop(mounts);
}
