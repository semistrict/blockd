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
    use std::io::{BufRead, BufReader, Write};

    const ARENA_PAGES: usize = 24 * 256;

    fn fnv(h: &mut u64, v: u64) {
        *h ^= v;
        *h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }

    /// Deterministic pattern for (seed, page, word).
    fn stamp(seed: u64, page: usize, word: usize) -> u64 {
        let mut h = 0xcbf2_9ce4_8422_2325;
        fnv(&mut h, seed);
        fnv(&mut h, page as u64);
        fnv(&mut h, word as u64);
        h
    }

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
