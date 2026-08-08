#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[cfg(target_os = "linux")]
mod linux {

    use std::collections::BTreeMap;
    use std::fs::{File, OpenOptions};
    use std::io::{self, BufRead, BufReader, Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::{Component, Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use blockd_core::daemon::DaemonConfig;
    use blockd_core::journal::VsetConfig;
    use blockd_core::seam::{DetachMode, StoreFault};
    use blockd_core::types::{HostId, VmId, VsetId, millis};
    use blockd_runtime::vsetfs::VsetFsEndpoint;
    use blockd_runtime::{GetResult, ObjectStore, Runtime, RuntimeConfig};

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

    struct DirectoryStore {
        root: PathBuf,
        lock: Mutex<()>,
        next_temp: AtomicU64,
    }

    impl DirectoryStore {
        fn new(root: PathBuf) -> io::Result<Self> {
            std::fs::create_dir_all(&root)?;
            Ok(Self {
                root,
                lock: Mutex::new(()),
                next_temp: AtomicU64::new(1),
            })
        }

        fn path(&self, key: &str) -> io::Result<PathBuf> {
            let key = Path::new(key);
            if key.is_absolute()
                || key
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
            {
                return Err(invalid("invalid object key"));
            }
            Ok(self.root.join(key))
        }

        fn read(path: &Path) -> io::Result<Option<(u64, Vec<u8>)>> {
            let mut file = match File::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error),
            };
            let mut version = [0; 8];
            file.read_exact(&mut version)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            Ok(Some((u64::from_le_bytes(version), bytes)))
        }

        fn write(&self, path: &Path, version: u64, bytes: &[u8]) -> io::Result<()> {
            let parent = path
                .parent()
                .ok_or_else(|| invalid("object has no parent"))?;
            std::fs::create_dir_all(parent)?;
            let temp = parent.join(format!(
                ".blockd-store-{}-{}",
                std::process::id(),
                self.next_temp.fetch_add(1, Ordering::Relaxed)
            ));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            file.write_all(&version.to_le_bytes())?;
            file.write_all(bytes)?;
            file.sync_all()?;
            std::fs::rename(&temp, path)?;
            File::open(parent)?.sync_all()
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for DirectoryStore {
        async fn put(self: Arc<Self>, key: String, bytes: Vec<u8>) -> Result<u64, StoreFault> {
            let _guard = self.lock.lock().map_err(|_| StoreFault::Unavailable)?;
            let path = self.path(&key).map_err(|_| StoreFault::Unavailable)?;
            let version = Self::read(&path)
                .map_err(|_| StoreFault::Unavailable)?
                .map_or(1, |(version, _)| version.saturating_add(1));
            self.write(&path, version, &bytes)
                .map_err(|_| StoreFault::Unavailable)?;
            Ok(version)
        }

        async fn put_cas(
            self: Arc<Self>,
            key: String,
            expected: Option<u64>,
            bytes: Vec<u8>,
        ) -> Result<u64, StoreFault> {
            let _guard = self.lock.lock().map_err(|_| StoreFault::Unavailable)?;
            let path = self.path(&key).map_err(|_| StoreFault::Unavailable)?;
            let actual = Self::read(&path)
                .map_err(|_| StoreFault::Unavailable)?
                .map(|(version, _)| version);
            if actual != expected {
                return Err(StoreFault::CasConflict { actual });
            }
            let version = actual.map_or(1, |version| version.saturating_add(1));
            self.write(&path, version, &bytes)
                .map_err(|_| StoreFault::Unavailable)?;
            Ok(version)
        }

        async fn get(self: Arc<Self>, key: String) -> GetResult {
            let _guard = self.lock.lock().map_err(|_| StoreFault::Unavailable)?;
            let path = self.path(&key).map_err(|_| StoreFault::Unavailable)?;
            Self::read(&path).map_err(|_| StoreFault::Unavailable)
        }

        async fn get_range(self: Arc<Self>, key: String, offset: u64, len: u64) -> GetResult {
            let _guard = self.lock.lock().map_err(|_| StoreFault::Unavailable)?;
            let path = self.path(&key).map_err(|_| StoreFault::Unavailable)?;
            let Some((version, bytes)) = Self::read(&path).map_err(|_| StoreFault::Unavailable)?
            else {
                return Ok(None);
            };
            let start = usize::try_from(offset).map_err(|_| StoreFault::Unavailable)?;
            if start >= bytes.len() {
                return Ok(None);
            }
            let end = start
                .saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
                .min(bytes.len());
            Ok(Some((version, bytes[start..end].to_vec())))
        }

        async fn delete(self: Arc<Self>, key: String) {
            let Ok(_guard) = self.lock.lock() else { return };
            let Ok(path) = self.path(&key) else { return };
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        let _ = File::open(parent).and_then(|dir| dir.sync_all());
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(_) => {}
            }
        }
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
