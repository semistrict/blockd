#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(target_os = "linux")]
mod linux {

    use std::collections::BTreeMap;
    use std::fs::{File, OpenOptions};
    use std::io::{self, BufRead, BufReader, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use blockd_core::daemon::DaemonConfig;
    use blockd_core::journal::VsetConfig;
    use blockd_core::seam::DetachMode;
    use blockd_core::types::{HostId, VmId, VsetId, millis};
    use blockd_runtime::directory_store::DirectoryStore;
    use blockd_runtime::vsetfs::VsetFsEndpoint;
    use blockd_runtime::{ObjectStore, Runtime, RuntimeConfig};

    struct Args {
        host: HostId,
        vm: VmId,
        vhost_socket: PathBuf,
        control_socket: PathBuf,
        blob_dir: PathBuf,
        store_dir: PathBuf,
        state_file: PathBuf,
        tag: String,
    }

    impl Args {
        fn parse() -> io::Result<Self> {
            let mut values = std::env::args().skip(1);
            let mut args = BTreeMap::new();
            while let Some(flag) = values.next() {
                if !flag.starts_with("--") {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "expected --option",
                    ));
                }
                let value = values.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing value for {flag}"),
                    )
                })?;
                args.insert(flag, value);
            }
            let required = |name: &str| {
                args.get(name).cloned().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("missing {name}"))
                })
            };
            Ok(Self {
                host: HostId(
                    args.get("--host")
                        .map_or(Ok(0), |v| v.parse())
                        .map_err(invalid)?,
                ),
                vm: VmId(required("--vm")?.parse().map_err(invalid)?),
                vhost_socket: required("--vhost-socket")?.into(),
                control_socket: required("--control-socket")?.into(),
                blob_dir: required("--blob-dir")?.into(),
                store_dir: required("--store-dir")?.into(),
                state_file: required("--state-file")?.into(),
                tag: args
                    .get("--tag")
                    .cloned()
                    .unwrap_or_else(|| "vsets".to_owned()),
            })
        }
    }

    fn invalid(error: impl std::fmt::Display) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
    }

    fn remove_socket(path: &Path) -> io::Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn load_configs(path: &Path) -> io::Result<BTreeMap<VsetId, VsetConfig>> {
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(error) => return Err(error),
        };
        let mut configs = BTreeMap::new();
        for line in BufReader::new(file).lines() {
            let line = line?;
            let fields: Vec<_> = line.split_whitespace().collect();
            let [vset, pages, backed] = fields.as_slice() else {
                return Err(invalid("invalid runner state"));
            };
            let vset = VsetId(vset.parse().map_err(invalid)?);
            let pages = pages.parse().map_err(invalid)?;
            let backed = match *backed {
                "backed" => true,
                "local" => false,
                _ => return Err(invalid("invalid runner durability")),
            };
            if configs
                .insert(vset, VsetConfig::database(pages, backed))
                .is_some()
            {
                return Err(invalid("duplicate vset in runner state"));
            }
        }
        Ok(configs)
    }

    fn save_configs(path: &Path, configs: &BTreeMap<VsetId, VsetConfig>) -> io::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| invalid("state file has no parent"))?;
        std::fs::create_dir_all(parent)?;
        let temp = parent.join(format!(".blockd-vsetfs-state-{}", std::process::id()));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)?;
        for (vset, config) in configs {
            writeln!(
                file,
                "{} {} {}",
                vset.0,
                config.pages_per_volume,
                if config.durability.uses_store() {
                    "backed"
                } else {
                    "local"
                }
            )?;
        }
        file.sync_all()?;
        std::fs::rename(temp, path)?;
        File::open(parent)?.sync_all()
    }

    fn reply(mut stream: UnixStream, message: &str) -> io::Result<()> {
        writeln!(stream, "{message}")
    }

    #[allow(clippy::too_many_lines)]
    fn run() -> io::Result<()> {
        let args = Args::parse()?;
        let store: Arc<dyn ObjectStore> = Arc::new(DirectoryStore::new(args.store_dir)?);
        let config = RuntimeConfig {
            daemon: DaemonConfig {
                host: args.host,
                cache_pages: 4096,
                writeback_interval: millis(10),
                backup_retry: millis(100),
                disk_capacity: None,
                disk_headroom: 0,
                wedge_ticks: 25,
                replica_placement: None,
            },
            blob_dir: args.blob_dir,
            peer: None,
        };
        let mut configs = load_configs(&args.state_file)?;
        let runtime = if configs.is_empty() {
            Runtime::new(&config, store)
        } else {
            let (runtime, _) = Runtime::recover(&config, store, &configs);
            for (&vset, vset_config) in &configs {
                if vset_config.durability.uses_store() {
                    let _ = runtime.wait_recovered(vset);
                }
            }
            runtime
        };
        let runtime = Arc::new(runtime);
        let endpoint =
            VsetFsEndpoint::bind(Arc::clone(&runtime), args.vm, &args.vhost_socket, &args.tag)?;
        remove_socket(&args.control_socket)?;
        let listener = UnixListener::bind(&args.control_socket)?;
        let mut shutdown = false;
        while !shutdown {
            let (stream, _) = listener.accept()?;
            let mut line = String::new();
            let reader_stream = match stream.try_clone() {
                Ok(stream) => stream,
                Err(error) => {
                    eprintln!("control client clone failed: {error}");
                    continue;
                }
            };
            if let Err(error) = BufReader::new(reader_stream).read_line(&mut line) {
                eprintln!("control client read failed: {error}");
                continue;
            }
            let fields: Vec<_> = line.split_whitespace().collect();
            let response = match fields.as_slice() {
                ["create", vset, pages, durability] => {
                    let parsed = vset
                        .parse::<u64>()
                        .and_then(|vset| pages.parse::<u32>().map(|pages| (VsetId(vset), pages)));
                    match parsed {
                        Ok((vset, pages)) if !configs.contains_key(&vset) && pages > 0 => {
                            let backed = match *durability {
                                "backed" => Some(true),
                                "local" => Some(false),
                                _ => None,
                            };
                            if let Some(backed) = backed {
                                let vset_config = VsetConfig::database(pages, backed);
                                runtime.create_vset(vset, vset_config);
                                configs.insert(vset, vset_config);
                                match save_configs(&args.state_file, &configs) {
                                    Ok(()) => format!("OK created {}", vset.0),
                                    Err(error) => format!("ERR state persistence failed: {error}"),
                                }
                            } else {
                                "ERR durability must be backed or local".to_owned()
                            }
                        }
                        Ok(_) => "ERR vset already exists or page count is zero".to_owned(),
                        Err(_) => "ERR invalid create arguments".to_owned(),
                    }
                }
                ["attach", name, vset] => match vset.parse::<u64>() {
                    Ok(vset)
                        if !endpoint
                            .exports()?
                            .iter()
                            .any(|(export, _, _)| export == name) =>
                    {
                        let vset = VsetId(vset);
                        if configs.contains_key(&vset) {
                            match endpoint.attach(name, vset) {
                                Ok(export) => {
                                    format!(
                                        "OK attached {name} {} {}",
                                        vset.0, export.attachment.generation
                                    )
                                }
                                Err(error) => format!("ERR {error}"),
                            }
                        } else {
                            "ERR unknown vset".to_owned()
                        }
                    }
                    Ok(_) => "ERR export already exists".to_owned(),
                    Err(_) => "ERR invalid vset".to_owned(),
                },
                ["detach", name, mode] => {
                    let mode = match *mode {
                        "graceful" => Some(DetachMode::Graceful),
                        "forced" => Some(DetachMode::Forced),
                        _ => None,
                    };
                    let attachment =
                        endpoint
                            .exports()?
                            .into_iter()
                            .find_map(|(export, vset, attachment)| {
                                (export == *name).then_some((vset, attachment))
                            });
                    match (attachment, mode) {
                        (Some((vset, attachment)), Some(mode)) => {
                            let result = endpoint
                                .begin_detach(name, vset, attachment, mode)
                                .and_then(|mappings| {
                                    if mode == DetachMode::Forced {
                                        endpoint.revoke_dax_mappings(&mappings)?;
                                        let deadline = Instant::now() + Duration::from_secs(5);
                                        while !endpoint.finish_forced_detach(vset, attachment) {
                                            if Instant::now() >= deadline {
                                                return Err(io::Error::new(
                                                    io::ErrorKind::TimedOut,
                                                    "forced detach did not become durable",
                                                ));
                                            }
                                            std::thread::sleep(Duration::from_millis(2));
                                        }
                                        Ok(())
                                    } else {
                                        endpoint.finish_detach(name, vset, attachment)
                                    }
                                });
                            match result {
                                Ok(()) => format!("OK detached {name}"),
                                Err(error) => format!("ERR {error}"),
                            }
                        }
                        (None, _) => "ERR unknown export".to_owned(),
                        (_, None) => "ERR mode must be graceful or forced".to_owned(),
                    }
                }
                ["list"] => {
                    let exports = endpoint
                        .exports()?
                        .into_iter()
                        .map(|(name, vset, _)| format!("{name}={}", vset.0))
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!("OK{}{}", if exports.is_empty() { "" } else { " " }, exports)
                }
                ["shutdown"] => {
                    shutdown = true;
                    "OK shutdown".to_owned()
                }
                _ => "ERR expected create, attach, detach, list, or shutdown".to_owned(),
            };
            if let Err(error) = reply(stream, &response) {
                eprintln!("control client reply failed: {error}");
            }
        }
        drop(listener);
        remove_socket(&args.control_socket)?;
        endpoint.wait()
    }

    pub fn main() {
        if let Err(error) = run() {
            eprintln!("blockd-vsetfs: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("blockd-vsetfs is supported only on Linux");
    std::process::exit(2);
}
