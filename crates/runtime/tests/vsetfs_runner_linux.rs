#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn wait_for_path(child: &mut Child, path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "runner exited before binding"
        );
        assert!(
            Instant::now() < deadline,
            "runner did not bind control socket"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn command(path: &Path, request: &str) -> String {
    let mut stream = UnixStream::connect(path).unwrap();
    writeln!(stream, "{request}").unwrap();
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).unwrap();
    response.trim().to_owned()
}

#[test]
fn runner_hotplugs_multiple_databases_over_one_endpoint() {
    let root = tempfile::tempdir().unwrap();
    let control = root.path().join("control.sock");
    let vhost = root.path().join("vhost.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_blockd-vsetfs"))
        .args([
            "--vm",
            "7",
            "--vhost-socket",
            vhost.to_str().unwrap(),
            "--control-socket",
            control.to_str().unwrap(),
            "--blob-dir",
            root.path().join("blobs").to_str().unwrap(),
            "--store-dir",
            root.path().join("store").to_str().unwrap(),
            "--state-file",
            root.path().join("state").to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_path(&mut child, &control);

    let mut malformed = UnixStream::connect(&control).unwrap();
    malformed.write_all(&[0xff, b'\n']).unwrap();
    drop(malformed);
    assert_eq!(command(&control, "list"), "OK");
    assert!(
        child.try_wait().unwrap().is_none(),
        "runner terminated after a malformed control connection"
    );

    assert_eq!(command(&control, "create 11 256 backed"), "OK created 11");
    assert_eq!(command(&control, "create 12 256 backed"), "OK created 12");
    assert!(command(&control, "attach orders 11").starts_with("OK attached orders 11 "));
    assert_eq!(
        command(&control, "attach duplicate 11"),
        "ERR database is already attached"
    );
    assert!(
        child.try_wait().unwrap().is_none(),
        "runner terminated after attach conflict"
    );
    assert!(command(&control, "attach users 12").starts_with("OK attached users 12 "));
    assert_eq!(command(&control, "list"), "OK orders=11 users=12");
    assert_eq!(command(&control, "shutdown"), "OK shutdown");
    assert!(child.wait().unwrap().success());

    let mut restarted = Command::new(env!("CARGO_BIN_EXE_blockd-vsetfs"))
        .args([
            "--vm",
            "7",
            "--vhost-socket",
            vhost.to_str().unwrap(),
            "--control-socket",
            control.to_str().unwrap(),
            "--blob-dir",
            root.path().join("blobs").to_str().unwrap(),
            "--store-dir",
            root.path().join("store").to_str().unwrap(),
            "--state-file",
            root.path().join("state").to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_path(&mut restarted, &control);
    assert!(
        command(&control, "attach orders 11").starts_with("OK attached orders 11 "),
        "backed vset was not attachable after runner restart"
    );
    assert_eq!(command(&control, "shutdown"), "OK shutdown");
    assert!(restarted.wait().unwrap().success());
}
