//! The in-guest workload: PID 1 of a Firecracker microVM, speaking a tiny
//! line protocol over the serial console. It is the same seeded
//! write/verify pattern workload the simulations run — ported into a real
//! guest so snapshot, restore and fork scenarios can be verified from
//! INSIDE the VM: the guest itself checksums its memory, so carried state,
//! divergence and isolation are proven by the guest's own observations.
//!
//! Protocol (one command per line on ttyS0, one reply line each; replies
//! are uppercase so they never collide with the tty echo of commands):
//!   ping                 → PONG
//!   fill <seed> <pages>  → write patterns over N native guest pages → FILLED <fnv>
//!   sum <pages>          → checksum the first N native guest pages  → SUM <fnv>
//!   mark <page> <value>  → write one word (divergence)             → MARKED
//!   db-create <vset> <vm> <generation> <port> → WAL database       → DBCREATED ...
//!   db-open   <vset> <vm> <generation> <port> → reopen database    → DBOPEN ...
//!   db-check             → query rows + integrity                  → DBCHECK ...
//!   db-close             → sync and close                         → DBCLOSED
//!   fs-db-create <name>  → create through the stock Unix VFS      → FSDBCREATED ...
//!   fs-db-open <name>    → reopen through the stock Unix VFS      → FSDBOPEN ...
//!   fs-status            → report virtio-fs mount and root entries → FSSTATUS ...
//!   fs-open-storm <names> <ops> → concurrent virtio-fs open/stat profile
//!   off                  → reboot(RESTART) — Firecracker exits

#[cfg(target_os = "linux")]
fn main() {
    guest::run();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("blockd-fc-guest runs inside a Linux microVM");
}

#[cfg(target_os = "linux")]
mod guest {
    use blockd_platform::page_size;
    use std::fs::OpenOptions;
    use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;

    use blockd_core::database::AttachmentId;
    use blockd_core::types::{VmId, VsetId};
    use blockd_sqlite_vfs::{Registration, register_vsock};
    use rusqlite::{Connection, OpenFlags};

    const ARENA_PAGES: usize = 24 * 256;

    struct DatabaseSession {
        connection: Connection,
        _registration: Option<Registration>,
    }

    struct RetainedMapping {
        address: *mut u8,
        len: usize,
    }

    fn valid_export_name(name: &str) -> bool {
        !name.is_empty()
            && name.len() <= 128
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    }

    fn database_flags(create: bool) -> OpenFlags {
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | if create {
                OpenFlags::SQLITE_OPEN_CREATE
            } else {
                OpenFlags::empty()
            }
    }

    fn configure_database(
        connection: Connection,
        registration: Option<Registration>,
        filesystem: bool,
    ) -> Result<DatabaseSession, String> {
        let mode: String = connection
            .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
            .map_err(|error| error.to_string())?;
        if mode != "wal" {
            return Err(format!("journal-mode-{mode}"));
        }
        let pragmas = if filesystem {
            "PRAGMA synchronous=FULL;
             PRAGMA wal_autocheckpoint=1;
             PRAGMA mmap_size=67108864;"
        } else {
            "PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=1;"
        };
        connection
            .execute_batch(pragmas)
            .map_err(|error| error.to_string())?;
        Ok(DatabaseSession {
            connection,
            _registration: registration,
        })
    }

    fn database_reply(
        session: &DatabaseSession,
        create: bool,
        created_tag: &str,
        opened_tag: &str,
    ) -> Result<String, String> {
        let version: String = session
            .connection
            .query_row("SELECT sqlite_version()", [], |row| row.get(0))
            .unwrap_or_else(|_| "unknown".to_owned());
        if !create {
            return Ok(format!("{opened_tag} {version}"));
        }
        session
            .connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS firecracker_e2e(
                     id INTEGER PRIMARY KEY,
                     value INTEGER NOT NULL
                 );
                 DELETE FROM firecracker_e2e;
                 BEGIN IMMEDIATE;
                 INSERT INTO firecracker_e2e(value)
                 VALUES (101), (202), (303), (404);
                 COMMIT;",
            )
            .map_err(|error| error.to_string())?;
        Ok(format!("{created_tag} 4 1010 {version}"))
    }

    impl Drop for RetainedMapping {
        fn drop(&mut self) {
            unsafe {
                libc::munmap(self.address.cast(), self.len);
            }
        }
    }

    fn retain_filesystem_mapping(name: &str) -> Result<RetainedMapping, String> {
        if !valid_export_name(name) {
            return Err("invalid-export-name".to_owned());
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(format!("/vsets/{name}/database.sqlite"))
            .map_err(|error| error.to_string())?;
        let len = usize::try_from(file.metadata().map_err(|error| error.to_string())?.len())
            .map_err(|_| "mapping-too-large".to_owned())?;
        if len == 0 {
            return Err("empty-file".to_owned());
        }
        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if address == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().to_string());
        }
        drop(file);
        Ok(RetainedMapping {
            address: address.cast(),
            len,
        })
    }

    fn database_open(
        vset: u64,
        vm: u64,
        generation: u64,
        port: u32,
        create: bool,
    ) -> Result<DatabaseSession, String> {
        std::fs::create_dir_all("/run/blockd-sqlite").map_err(|error| error.to_string())?;
        let name = format!("blockd-vset-{vset}-generation-{generation}");
        let registration = register_vsock(
            Some(&name),
            port,
            std::path::Path::new("/run/blockd-sqlite"),
            VsetId(vset),
            AttachmentId {
                vm: VmId(vm),
                generation,
            },
        )
        .map_err(|code| format!("register-{code}"))?;
        let connection = Connection::open_with_flags_and_vfs(
            format!("vset-{vset}"),
            database_flags(create),
            name.as_str(),
        )
        .map_err(|error| error.to_string())?;
        configure_database(connection, Some(registration), false)
    }

    fn filesystem_database_open(name: &str, create: bool) -> Result<DatabaseSession, String> {
        if !valid_export_name(name) {
            return Err("invalid-export-name".to_owned());
        }
        let connection = Connection::open_with_flags(
            format!("/vsets/{name}/database.sqlite"),
            database_flags(create),
        )
        .map_err(|error| error.to_string())?;
        configure_database(connection, None, true)
    }

    #[allow(clippy::disallowed_methods, clippy::disallowed_types)]
    fn filesystem_open_storm(names: &str, operations: usize) -> String {
        let names = names
            .split(',')
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if names.is_empty() || operations == 0 {
            return "DBERR invalid-open-storm".to_owned();
        }
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(names.len()));
        let started = std::time::Instant::now();
        let workers = names
            .into_iter()
            .map(|name| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let path = format!("/vsets/{name}/database.sqlite");
                    barrier.wait();
                    (0..operations)
                        .filter(|_| {
                            std::fs::File::open(&path)
                                .and_then(|file| file.metadata())
                                .is_ok()
                        })
                        .count()
                })
            })
            .collect::<Vec<_>>();
        let completed = workers
            .into_iter()
            .map(|worker| worker.join().unwrap_or(0))
            .sum::<usize>();
        format!("FSOPEN {completed} {}", started.elapsed().as_micros())
    }

    fn parse_database_args<'a>(
        parts: &mut impl Iterator<Item = &'a str>,
    ) -> Result<(u64, u64, u64, u32), String> {
        let mut next_u64 = |label: &str| {
            parts
                .next()
                .ok_or_else(|| format!("missing-{label}"))?
                .parse()
                .map_err(|_| format!("invalid-{label}"))
        };
        let vset = next_u64("vset")?;
        let vm = next_u64("vm")?;
        let generation = next_u64("generation")?;
        let port = u32::try_from(next_u64("port")?).map_err(|_| "invalid-port".to_owned())?;
        Ok((vset, vm, generation, port))
    }

    fn fnv(h: &mut u64, v: u64) {
        *h ^= v;
        *h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }

    fn virtio_devices() -> String {
        let Ok(entries) = std::fs::read_dir("/sys/bus/virtio/devices") else {
            return "no-sysfs".to_owned();
        };
        let mut devices = entries
            .filter_map(Result::ok)
            .map(|entry| {
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().into_owned();
                let device = std::fs::read_to_string(path.join("device"))
                    .unwrap_or_default()
                    .trim()
                    .to_owned();
                let driver = std::fs::read_link(path.join("driver"))
                    .ok()
                    .and_then(|path| path.file_name().map(ToOwned::to_owned))
                    .map_or_else(
                        || "unbound".to_owned(),
                        |name| name.to_string_lossy().into_owned(),
                    );
                let tag = std::fs::read_to_string(path.join("tag"))
                    .unwrap_or_default()
                    .trim()
                    .to_owned();
                format!("{name}:{device}:{driver}:{tag}")
            })
            .collect::<Vec<_>>();
        devices.sort();
        devices.join(",")
    }

    fn virtio_drivers() -> String {
        let Ok(entries) = std::fs::read_dir("/sys/bus/virtio/drivers") else {
            return "no-drivers".to_owned();
        };
        let mut drivers = entries
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        drivers.sort();
        drivers.join(",")
    }

    fn kernel_diagnostics() -> String {
        let Ok(mut kmsg) = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open("/dev/kmsg")
        else {
            return "no-kmsg".to_owned();
        };
        let _ = kmsg.seek(SeekFrom::Start(0));
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            match kmsg.read(&mut buffer) {
                Ok(0) => break,
                Ok(size) => bytes.extend_from_slice(&buffer[..size]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
        let text = String::from_utf8_lossy(&bytes);
        text.lines()
            .filter(|line| {
                let lower = line.to_ascii_lowercase();
                lower.contains("virtio") || lower.contains("fuse")
            })
            .map(|line| line.split_once(';').map_or(line, |(_, message)| message))
            .collect::<Vec<_>>()
            .join("|")
    }

    /// Deterministic pattern for (seed, page, word).
    fn stamp(seed: u64, page: usize, word: usize) -> u64 {
        let mut h = 0xcbf2_9ce4_8422_2325;
        fnv(&mut h, seed);
        fnv(&mut h, page as u64);
        fnv(&mut h, word as u64);
        h
    }

    #[allow(clippy::disallowed_methods, clippy::too_many_lines)]
    pub fn run() {
        // PID 1 housekeeping: /dev so the console exists.
        std::fs::create_dir_all("/dev").ok();
        // SAFETY: plain mount syscall; failure just means devtmpfs is
        // already there (or CONFIG_DEVTMPFS_MOUNT did it).
        unsafe {
            libc::mount(
                c"devtmpfs".as_ptr(),
                c"/dev".as_ptr(),
                c"devtmpfs".as_ptr(),
                0,
                std::ptr::null(),
            );
        }
        std::fs::create_dir_all("/vsets").ok();
        std::fs::create_dir_all("/proc").ok();
        std::fs::create_dir_all("/sys").ok();
        unsafe {
            libc::mount(
                c"proc".as_ptr(),
                c"/proc".as_ptr(),
                c"proc".as_ptr(),
                0,
                std::ptr::null(),
            );
            libc::mount(
                c"sysfs".as_ptr(),
                c"/sys".as_ptr(),
                c"sysfs".as_ptr(),
                0,
                std::ptr::null(),
            );
        }
        // This is deliberately the ordinary Linux virtio-fs mount and the
        // ordinary SQLite Unix VFS. Failure is tolerated for memory-only and
        // prototype-transport tests that boot without the filesystem device.
        let mut virtiofs_mount_error = None;
        for _ in 0..100 {
            let mounted = unsafe {
                libc::mount(
                    c"vsets".as_ptr(),
                    c"/vsets".as_ptr(),
                    c"virtiofs".as_ptr(),
                    0,
                    c"dax=always".as_ptr().cast(),
                )
            } == 0;
            if mounted {
                virtiofs_mount_error = None;
                break;
            }
            let errno = std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
            virtiofs_mount_error = Some(errno);
            if !matches!(errno, libc::ENODEV | libc::ENOENT | libc::ENXIO) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let serial_in = OpenOptions::new()
            .read(true)
            .open("/dev/ttyS0")
            .expect("serial in");
        let mut serial_out = OpenOptions::new()
            .write(true)
            .open("/dev/ttyS0")
            .expect("serial out");

        let mut arena: Vec<u64> = vec![0; ARENA_PAGES * page_size() / 8];
        let words_per_page = page_size() / 8;
        let mut database: Option<DatabaseSession> = None;
        let mut retained_mapping: Option<RetainedMapping> = None;

        writeln!(serial_out, "READY {ARENA_PAGES}").expect("write");
        for line in BufReader::new(serial_in).lines() {
            let Ok(line) = line else { break };
            let mut parts = line.split_whitespace();
            let reply = match parts.next() {
                Some("ping") => "PONG".to_owned(),
                Some("fill") => {
                    let seed: u64 = parts.next().unwrap_or("0").parse().unwrap_or(0);
                    let pages: usize = parts.next().unwrap_or("0").parse().unwrap_or(0);
                    let pages = pages.min(ARENA_PAGES);
                    let mut h = 0xcbf2_9ce4_8422_2325u64;
                    for page in 0..pages {
                        for word in 0..words_per_page {
                            let v = stamp(seed, page, word);
                            arena[page * words_per_page + word] = v;
                            fnv(&mut h, v);
                        }
                    }
                    format!("FILLED {h:016x}")
                }
                Some("sum") => {
                    let pages: usize = parts.next().unwrap_or("0").parse().unwrap_or(0);
                    let pages = pages.min(ARENA_PAGES);
                    let mut h = 0xcbf2_9ce4_8422_2325u64;
                    for page in 0..pages {
                        for word in 0..words_per_page {
                            fnv(&mut h, arena[page * words_per_page + word]);
                        }
                    }
                    format!("SUM {h:016x}")
                }
                Some("mark") => {
                    let page: usize = parts.next().unwrap_or("0").parse().unwrap_or(0);
                    let value: u64 = parts.next().unwrap_or("0").parse().unwrap_or(0);
                    arena[page.min(ARENA_PAGES - 1) * words_per_page] = value;
                    "MARKED".to_owned()
                }
                Some(command @ ("db-create" | "db-open")) => {
                    let create = command == "db-create";
                    match parse_database_args(&mut parts)
                        .and_then(|args| database_open(args.0, args.1, args.2, args.3, create))
                    {
                        Ok(session) => {
                            match database_reply(&session, create, "DBCREATED", "DBOPEN") {
                                Ok(reply) => {
                                    database = Some(session);
                                    reply
                                }
                                Err(error) => format!("DBERR {error}"),
                            }
                        }
                        Err(error) => format!("DBERR {error}"),
                    }
                }
                Some(command @ ("fs-db-create" | "fs-db-open")) => {
                    let name = parts.next().unwrap_or_default();
                    let create = command == "fs-db-create";
                    match filesystem_database_open(name, create) {
                        Ok(session) => {
                            match database_reply(&session, create, "FSDBCREATED", "FSDBOPEN") {
                                Ok(reply) => {
                                    database = Some(session);
                                    reply
                                }
                                Err(error) => format!("DBERR {error}"),
                            }
                        }
                        Err(error) => format!("DBERR {error}"),
                    }
                }
                Some("fs-status") => {
                    let mut entries = std::fs::read_dir("/vsets")
                        .map(|entries| {
                            entries
                                .filter_map(Result::ok)
                                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    entries.sort();
                    let devices = virtio_devices();
                    let drivers = virtio_drivers();
                    let diagnostics = kernel_diagnostics();
                    match virtiofs_mount_error {
                        None => format!(
                            "FSSTATUS ok {} devices={devices} drivers={drivers} kmsg={diagnostics}",
                            entries.join(",")
                        ),
                        Some(errno) => format!(
                            "FSSTATUS errno-{errno} {} devices={devices} drivers={drivers} kmsg={diagnostics}",
                            entries.join(",")
                        ),
                    }
                }
                Some("fs-open-storm") => {
                    let names = parts.next().unwrap_or_default();
                    let operations = parts.next().unwrap_or("0").parse::<usize>().unwrap_or(0);
                    filesystem_open_storm(names, operations)
                }
                Some("db-check") => match database.as_ref() {
                    Some(session) => {
                        let values = session.connection.query_row(
                            "SELECT count(*), coalesce(sum(value), 0) FROM firecracker_e2e",
                            [],
                            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                        );
                        let integrity =
                            session
                                .connection
                                .query_row("PRAGMA integrity_check", [], |row| {
                                    row.get::<_, String>(0)
                                });
                        match (values, integrity) {
                            (Ok((count, sum)), Ok(integrity)) => {
                                format!("DBCHECK {count} {sum} {integrity}")
                            }
                            (Err(error), _) | (_, Err(error)) => format!("DBERR {error}"),
                        }
                    }
                    None => "DBERR not-open".to_owned(),
                },
                Some("db-close") => {
                    if let Some(session) = database.take() {
                        let _ = session
                            .connection
                            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize;");
                        drop(session);
                    }
                    "DBCLOSED".to_owned()
                }
                Some("fs-retain-map") => {
                    let name = parts.next().unwrap_or_default();
                    match retain_filesystem_mapping(name) {
                        Ok(mapping) => {
                            retained_mapping = Some(mapping);
                            "FSMAPPED".to_owned()
                        }
                        Err(error) => format!("DBERR {error}"),
                    }
                }
                Some("fs-stale-read") => match retained_mapping.as_ref() {
                    Some(mapping) => {
                        let byte = unsafe { std::ptr::read_volatile(mapping.address) };
                        format!("FSSTALE {byte}")
                    }
                    None => "DBERR no-retained-mapping".to_owned(),
                },
                Some("off") => {
                    let _ = writeln!(serial_out, "BYE");
                    // SAFETY: reboot as PID 1 shuts the microVM down.
                    unsafe {
                        libc::sync();
                        libc::reboot(libc::LINUX_REBOOT_CMD_RESTART);
                    }
                    unreachable!("rebooted");
                }
                _ => "ERR".to_owned(),
            };
            writeln!(serial_out, "{reply}").expect("write");
        }
    }
}
