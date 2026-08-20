//! Repository-level production policy regressions.

use std::path::{Path, PathBuf};
use std::process::Command;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}

fn workflow_source() -> String {
    let directory = repository_root().join(".github/workflows");
    let mut paths = std::fs::read_dir(directory)
        .expect("workflow directory")
        .map(|entry| entry.expect("workflow entry").path())
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .into_iter()
        .filter(|path| {
            matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("yml" | "yaml")
            )
        })
        .map(|path| std::fs::read_to_string(path).expect("workflow source"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn workflow(name: &str) -> String {
    std::fs::read_to_string(repository_root().join(".github/workflows").join(name))
        .expect("workflow source")
}

fn infrastructure_source(name: &str) -> String {
    std::fs::read_to_string(repository_root().join("infra").join(name))
        .expect("infrastructure source")
}

fn run(command: &mut Command) -> std::process::Output {
    command.output().expect("run policy fixture command")
}

fn git(directory: &Path, arguments: &[&str]) -> std::process::Output {
    run(Command::new("git").arg("-C").arg(directory).args(arguments))
}

fn terraform_resource<'a>(source: &'a str, kind: &str, name: &str) -> &'a str {
    let marker = format!("resource \"{kind}\" \"{name}\"");
    let start = source.find(&marker).expect("Terraform resource");
    let block = &source[start..];
    let open = block.find('{').expect("Terraform resource body");
    let mut depth = 0_u32;
    for (offset, byte) in block.as_bytes()[open..].iter().copied().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1).expect("balanced Terraform block");
                if depth == 0 {
                    return &block[..=open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("unterminated Terraform resource {kind}.{name}");
}

/// Regression PROD-014: required checks must gate every change, not live only in the
/// contributor documentation.
#[test]
fn ci_gates_lint_portable_tests_and_workspace_docs() {
    let workflows = workflow_source();
    assert!(workflows.contains("./lint.sh") || workflows.contains("run: lint.sh"));
    assert!(
        workflows.contains("cargo nextest run") || workflows.contains("cargo test --workspace")
    );
    assert!(workflows.contains("cargo test --doc --workspace"));
    assert!(
        workflow("linux-kernel.yml").contains("./lint.sh"),
        "Linux-only production code is not linted"
    );
}

/// Regression PROD-015: dependency advisory and license policy belong in a required
/// automated gate.
#[test]
fn ci_gates_dependency_advisories_and_licenses() {
    let workflows = workflow_source();
    assert!(
        workflows.contains("cargo audit") || workflows.contains("cargo deny check advisories"),
        "CI has no dependency advisory gate"
    );
    assert!(
        workflows.contains("cargo deny --offline --locked check advisories licenses bans sources")
    );
    assert!(workflows.contains("cargo fetch --locked"));
    assert!(workflows.contains("ADVISORY_DB_REVISION"));
    assert!(workflows.contains("cargo audit --db"));
    assert!(workflows.contains("--no-fetch"));
}

#[test]
fn collector_identity_is_separate_and_prefix_scoped() {
    let root = repository_root();
    let infrastructure = infrastructure_source("main.tf");
    let host_service = infrastructure_source("blockd.service");
    let collector_service = infrastructure_source("blockd-gc.service");
    let collector_provisioning = infrastructure_source("provision-collector.sh");

    assert!(infrastructure.contains("google_service_account\" \"collector"));
    let list_role = terraform_resource(
        &infrastructure,
        "google_project_iam_custom_role",
        "collector_list_role",
    );
    assert!(list_role.contains("\"storage.objects.list\""));
    assert_eq!(list_role.matches("storage.objects.").count(), 1);
    let gc_role = terraform_resource(
        &infrastructure,
        "google_project_iam_custom_role",
        "collector_gc_role",
    );
    assert!(gc_role.contains("\"storage.objects.get\""));
    assert!(gc_role.contains("\"storage.objects.delete\""));
    assert_eq!(gc_role.matches("storage.objects.").count(), 2);
    assert!(!gc_role.contains("storage.objects.create"));
    assert!(!gc_role.contains("storage.objects.update"));

    let list_binding = terraform_resource(
        &infrastructure,
        "google_storage_bucket_iam_member",
        "collector_list",
    );
    assert!(list_binding.contains("collector_list_role.name"));
    assert!(!list_binding.contains("condition"));
    let gc_binding = terraform_resource(
        &infrastructure,
        "google_storage_bucket_iam_member",
        "collector_archive_gc",
    );
    assert!(gc_binding.contains("collector_gc_role.name"));
    assert!(gc_binding.contains("condition"));
    assert!(gc_binding.contains("${local.object_prefix_path}v/"));
    assert!(gc_binding.contains("${local.object_prefix_path}b/"));
    assert!(!gc_binding.contains("cluster/"));

    let collector_workload = infrastructure
        .split_once("resource \"google_compute_instance\" \"collector\"")
        .and_then(|(_, rest)| rest.split_once("# Keep daemon blobs"))
        .map(|(collector, _)| collector)
        .expect("collector compute resource");
    assert!(
        collector_workload.contains("email  = google_service_account.collector.email"),
        "collector workload does not use the collector service account"
    );
    assert!(
        collector_workload
            .contains("metadata_startup_script = file(\"${path.module}/provision-collector.sh\")"),
        "collector workload is not provisioned"
    );
    assert!(infrastructure.contains("${local.object_prefix_path}v/"));
    assert!(infrastructure.contains("${local.object_prefix_path}b/"));
    assert!(
        infrastructure.contains("${local.object_prefix_path}cluster/tls/public-keys/"),
        "host membership withdrawal is not scoped to the configured cluster prefix"
    );
    assert!(!host_service.contains("blockd_gc"));
    assert!(
        collector_service.contains("ExecStart=/usr/local/bin/blockd_gc "),
        "collector service must invoke Cargo's shipped blockd_gc binary"
    );
    assert!(
        root.join("crates/runtime/src/bin/blockd_gc.rs").is_file(),
        "collector service target is not built by the runtime package"
    );
    assert!(collector_provisioning.contains("--bin blockd_gc"));
    assert!(collector_provisioning.contains("target/release/blockd_gc /usr/local/bin/blockd_gc"));
    assert!(
        collector_provisioning
            .contains("infra/blockd-gc.service /etc/systemd/system/blockd-gc.service")
    );
    assert!(collector_provisioning.contains("systemctl enable --now blockd-gc.service"));
    assert!(collector_provisioning.contains("BLOCKD_STORE=gs://$BUCKET/$PREFIX"));
}

#[test]
#[allow(clippy::too_many_lines)] // host and collector must share the full behavioral checkout matrix
fn provisioning_builds_only_a_clean_detached_full_commit() {
    let root = repository_root();
    let host_script = root.join("infra/provision.sh");
    let collector_script = root.join("infra/provision-collector.sh");
    let fixture = tempfile::tempdir().expect("provisioning checkout fixture");
    let source = fixture.path().join("source");
    std::fs::create_dir(&source).expect("source repository");
    assert!(git(&source, &["init", "--quiet"]).status.success());
    assert!(
        git(&source, &["config", "user.email", "policy@example.invalid"])
            .status
            .success()
    );
    assert!(
        git(&source, &["config", "user.name", "Policy Fixture"])
            .status
            .success()
    );
    std::fs::write(source.join("payload"), b"pinned\n").expect("initial payload");
    assert!(git(&source, &["add", "payload"]).status.success());
    assert!(
        git(&source, &["commit", "--quiet", "-m", "initial"])
            .status
            .success()
    );
    let pinned = String::from_utf8(git(&source, &["rev-parse", "HEAD"]).stdout)
        .expect("UTF-8 commit")
        .trim()
        .to_owned();
    std::fs::write(source.join("payload"), b"later\n").expect("later payload");
    assert!(git(&source, &["add", "payload"]).status.success());
    assert!(
        git(&source, &["commit", "--quiet", "-m", "later"])
            .status
            .success()
    );
    let later = String::from_utf8(git(&source, &["rev-parse", "HEAD"]).stdout)
        .expect("UTF-8 later commit")
        .trim()
        .to_owned();

    for script in [&host_script, &collector_script] {
        for invalid in ["main", &pinned[..12]] {
            let output = run(Command::new("bash")
                .arg(script)
                .args(["--validate-repo-commit", invalid]));
            assert!(
                !output.status.success(),
                "accepted mutable/short ref {invalid}"
            );
        }
        assert!(
            run(Command::new("bash")
                .arg(script)
                .args(["--validate-repo-commit", &pinned,]))
            .status
            .success()
        );

        let checkout = fixture.path().join(
            script
                .file_name()
                .and_then(|name| name.to_str())
                .expect("script file name"),
        );
        let first = run(Command::new("bash").arg(script).args([
            "--checkout-repo",
            source.to_str().expect("source path"),
            &pinned,
            checkout.to_str().expect("checkout path"),
        ]));
        assert!(
            first.status.success(),
            "{}",
            String::from_utf8_lossy(&first.stderr)
        );
        let wrong = run(Command::new("bash").arg(script).args([
            "--checkout-repo",
            source.to_str().expect("source path"),
            &later,
            checkout.to_str().expect("checkout path"),
        ]));
        assert!(
            wrong.status.success(),
            "{}",
            String::from_utf8_lossy(&wrong.stderr)
        );
        std::fs::write(checkout.join("payload"), b"dirty\n").expect("dirty tracked file");
        std::fs::write(checkout.join("untracked"), b"dirty\n").expect("dirty untracked file");
        let second = run(Command::new("bash").arg(script).args([
            "--checkout-repo",
            source.to_str().expect("source path"),
            &pinned,
            checkout.to_str().expect("checkout path"),
        ]));
        assert!(
            second.status.success(),
            "{}",
            String::from_utf8_lossy(&second.stderr)
        );
        assert_eq!(
            String::from_utf8(git(&checkout, &["rev-parse", "HEAD"]).stdout)
                .expect("UTF-8 checkout commit")
                .trim(),
            pinned
        );
        assert!(git(&checkout, &["status", "--porcelain"]).stdout.is_empty());
        assert!(
            !git(&checkout, &["symbolic-ref", "--quiet", "HEAD"])
                .status
                .success(),
            "provisioned checkout is attached to a mutable ref"
        );
    }

    let host = infrastructure_source("provision.sh");
    let collector = infrastructure_source("provision-collector.sh");
    let variables = infrastructure_source("variables.tf");
    assert!(host.contains("cargo build --locked --release -p blockd-runtime --bin blockd"));
    assert!(host.contains("cargo build --locked --release -p blockd-fc-guest"));
    assert!(host.contains("checkout_repo_at_commit \\"));
    assert!(host.contains("https://github.com/firecracker-microvm/firecracker"));
    assert!(!host.contains("if [ ! -f /opt/firecracker/Cargo.toml ]"));
    assert!(host.contains("git apply --check /opt/blockd/patches/firecracker-uffd-shmem.patch"));
    assert!(collector.contains("cargo build --locked --release -p blockd-runtime --bin blockd_gc"));
    assert!(!variables.contains("default     = \"main\""));
    assert!(variables.contains("^[0-9a-fA-F]{40}$"));
    assert!(!host.contains("LATEST="));
    assert!(host.contains("KERNEL_OBJECT=firecracker-ci/v1.13/x86_64/vmlinux-6.1.141"));
    assert!(host.contains("KERNEL_SHA256="));
    assert!(host.contains("sha256sum -c -"));

    let ready = fixture.path().join("ready");
    let installed = fixture.path().join("installed-deployment");
    let deployment_a = "a".repeat(64);
    let deployment_b = "b".repeat(64);
    std::fs::write(&ready, b"").expect("ready marker");
    std::fs::write(&installed, format!("{deployment_a}\n")).expect("installed deployment");
    assert!(
        run(Command::new("bash").arg(&host_script).args([
            "--ready-for-deployment",
            ready.to_str().expect("ready path"),
            installed.to_str().expect("deployment path"),
            &deployment_a,
        ]))
        .status
        .success(),
        "same deployment did not take the idempotent boot path"
    );
    assert!(
        !run(Command::new("bash").arg(&host_script).args([
            "--ready-for-deployment",
            ready.to_str().expect("ready path"),
            installed.to_str().expect("deployment path"),
            &deployment_b,
        ]))
        .status
        .success(),
        "changed deployment inputs incorrectly took the stale .ready fast path"
    );
    let requested = host
        .find("REQUESTED_DEPLOYMENT_ID=$(meta blockd-deployment-id)")
        .expect("requested deployment");
    let ready_check = host
        .find("installed_deployment_matches \"$READY\"")
        .expect("deployment-bound ready check");
    assert!(requested < ready_check);
    let persist = host
        .find("mv \"$INSTALLED_DEPLOYMENT.pending\" \"$INSTALLED_DEPLOYMENT\"")
        .expect("persist installed deployment");
    let publish_ready = host
        .rfind("touch \"$READY\"")
        .expect("publish ready marker");
    assert!(persist < publish_ready);
}

#[test]
fn infrastructure_inputs_are_immutable_and_verified() {
    let root = repository_root();
    let infrastructure = infrastructure_source("main.tf");
    let variables = infrastructure_source("variables.tf");
    let host = infrastructure_source("provision.sh");
    let collector = infrastructure_source("provision-collector.sh");
    let provider_lock = std::fs::read_to_string(root.join("infra/.terraform.lock.hcl"))
        .expect("committed provider lock");

    assert!(infrastructure.contains("version = \"= 6.50.0\""));
    assert!(infrastructure.contains("required_version = \"= 1.12.5\""));
    assert!(provider_lock.contains("version     = \"6.50.0\""));
    assert!(provider_lock.contains("constraints = \"6.50.0\""));
    assert!(provider_lock.contains("zh:"));
    assert!(!infrastructure.contains("google_compute_image"));
    assert!(!infrastructure.contains("family  ="));
    assert_eq!(infrastructure.matches("image = var.base_image").count(), 2);
    assert!(variables.contains(
        "default     = \"projects/ubuntu-os-cloud/global/images/ubuntu-2404-noble-amd64-v20260807\""
    ));
    assert!(!variables.contains("decommission_grant"));
    assert!(infrastructure.contains("blockd-deployment-id = sha256(join"));
    assert!(infrastructure.contains("filesha256(\"${path.module}/provision.sh\")"));
    assert!(infrastructure.contains("filesha256(\"${path.module}/blockd.service\")"));

    let tofu_installer = infrastructure_source("install-tofu.sh");
    assert!(tofu_installer.contains("VERSION=1.12.5"));
    assert!(tofu_installer.contains("SHA256="));
    assert!(tofu_installer.contains("sha256sum -c -"));

    for (provisioning, first_metadata) in [
        (&host, "REPO_REF=$(meta blockd-repo-ref)"),
        (&collector, "BUCKET=$(meta blockd-bucket)"),
    ] {
        assert!(!provisioning.contains("https://sh.rustup.rs"));
        assert!(
            provisioning.contains("rustup/archive/1.28.2/x86_64-unknown-linux-gnu/rustup-init")
        );
        assert!(provisioning.contains(
            "RUSTUP_INIT_SHA256=20a06e644b0d9bd2fbdbfd52d42540bdde820ea7df86e92e533c073da0cdd43c"
        ));
        assert!(provisioning.contains("sha256sum -c -"));
        assert!(!provisioning.contains("if [ ! -x /opt/cargo/bin/cargo ]"));
        assert!(provisioning.contains("APT_SNAPSHOT=20250818T000000Z"));
        assert!(provisioning.contains("snapshot.ubuntu.com/ubuntu/$APT_SNAPSHOT/"));
        assert!(provisioning.contains("Dir::Etc::sourceparts=-"));
        let curl_bootstrap = provisioning
            .find("apt_snapshot install -y curl")
            .expect("curl bootstrap from the pinned apt snapshot");
        let first_metadata_read = provisioning
            .find(first_metadata)
            .expect("first metadata read");
        assert!(
            curl_bootstrap < first_metadata_read,
            "metadata was read before the pinned curl bootstrap"
        );
    }
}

#[test]
fn production_storage_and_compute_defaults_are_non_destructive() {
    let infrastructure = infrastructure_source("main.tf");
    let bucket = terraform_resource(&infrastructure, "google_storage_bucket", "store");
    assert!(bucket.contains("force_destroy               = false"));

    let variables = infrastructure_source("variables.tf");
    let spot = variables
        .split_once("variable \"spot\"")
        .and_then(|(_, block)| block.split_once('}'))
        .map(|(block, _)| block)
        .expect("spot variable");
    assert!(spot.contains("default     = false"));
}

#[test]
fn collector_prefix_validation_rejects_iam_and_environment_metacharacters() {
    let root = repository_root();
    let variables =
        std::fs::read_to_string(root.join("infra/variables.tf")).expect("infrastructure variables");
    assert!(variables.contains("^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*/?$"));
    let validator = root.join("infra/provision-collector.sh");
    for prefix in ["", "cluster", "team-1/cluster_2.3", "team-1/cluster_2.3/"] {
        assert!(
            Command::new("bash")
                .arg(&validator)
                .arg("--validate-prefix")
                .arg(prefix)
                .output()
                .expect("execute collector prefix validator")
                .status
                .success(),
            "supported object prefix was rejected: {prefix:?}"
        );
    }
    for prefix in [
        "/absolute",
        "team//cluster",
        "team/../cluster",
        "team cluster",
        "team'cluster",
        "team\nBLOCKD_GC_GRACE_SECONDS=1",
        "team=cluster",
        ".hidden",
        "team\\cluster",
    ] {
        assert!(
            !Command::new("bash")
                .arg(&validator)
                .arg("--validate-prefix")
                .arg(prefix)
                .output()
                .expect("execute collector prefix validator")
                .status
                .success(),
            "unsafe object prefix was accepted: {prefix:?}"
        );
    }
}

#[test]
fn production_hosts_install_the_daemon_and_cannot_delete_permanent_claims() {
    let root = repository_root();
    let infrastructure =
        std::fs::read_to_string(root.join("infra/main.tf")).expect("infrastructure source");
    let provisioning =
        std::fs::read_to_string(root.join("infra/provision.sh")).expect("host provisioning");
    let unit = std::fs::read_to_string(root.join("infra/blockd.service")).expect("host unit");

    assert!(provisioning.contains("cargo build --locked --release -p blockd-runtime --bin blockd"));
    assert!(provisioning.contains("target/release/blockd /usr/local/bin/blockd"));
    assert!(provisioning.contains("infra/blockd.service /etc/systemd/system/blockd.service"));
    assert!(provisioning.contains("systemctl enable --now blockd"));
    assert!(!provisioning.contains("systemctl enable --now blockd-demod"));
    assert!(unit.contains("ExecStart=/usr/local/bin/blockd serve"));
    assert!(infrastructure.contains("2 = \"10.10.0.12\""));
    assert!(infrastructure.contains("blockd-prefix   = local.object_prefix_path"));
    let host_delete = terraform_resource(
        &infrastructure,
        "google_project_iam_custom_role",
        "host_delete",
    );
    assert_eq!(host_delete.matches("storage.objects.").count(), 1);
    assert!(host_delete.contains("storage.objects.delete"));
    let host_delete_binding = terraform_resource(
        &infrastructure,
        "google_storage_bucket_iam_member",
        "host_archive_delete",
    );
    assert!(host_delete_binding.contains("host_delete.name"));
    assert!(!host_delete_binding.contains("roles/storage.objectAdmin"));
    assert!(host_delete_binding.contains("${local.object_prefix_path}v/"));
    assert!(host_delete_binding.contains("${local.object_prefix_path}b/"));
    assert!(host_delete_binding.contains("${local.object_prefix_path}cluster/tls/public-keys/"));
    assert!(!host_delete_binding.contains("cluster/nodes/"));
    assert!(!host_delete_binding.contains("hosts/"));
    assert!(!infrastructure.contains("decommission"));
}
