//! The demo control API: hand-rolled HTTP/1.1 over `std::net` (the house
//! style — this repo already speaks HTTP to Firecracker the same way).
//! Bound to an internal address only; reach it through an SSH tunnel.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::vm::Demod;

pub fn serve(state: &Arc<Demod>) {
    let listener = TcpListener::bind(state.cfg.api).expect("api listen");
    eprintln!(
        "demod host {} serving on http://{}",
        state.cfg.host.0, state.cfg.api
    );
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let state = state.clone();
        std::thread::spawn(move || handle(&state, stream));
    }
}

fn handle(state: &Arc<Demod>, mut stream: TcpStream) {
    let Some((method, path, query)) = read_request(&mut stream) else {
        return;
    };
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let reply = route(state, &method, &segments, &query);
    let (status, body) = match reply {
        Ok(body) => (200, body),
        Err(message) => (400, format!("{{\"error\":\"{message}\"}}\n")),
    };
    let _ = stream.write_all(
        format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .as_bytes(),
    );
}

fn route(
    state: &Arc<Demod>,
    method: &str,
    segments: &[&str],
    query: &BTreeMap<String, String>,
) -> Result<String, String> {
    let arg = |key: &str, default: u64| -> u64 {
        query
            .get(key)
            .map_or(default, |v| v.parse().unwrap_or(default))
    };
    match (method, segments) {
        ("GET", ["status"]) => Ok(status_json(state)),
        ("POST", ["base"]) => {
            let sum = state.bake_base();
            Ok(format!("{{\"baked\":true,\"sum\":\"{sum}\"}}\n"))
        }
        ("POST", ["vm"]) => {
            let id = state.start_vm(arg("backed", 0) == 1);
            Ok(format!("{{\"id\":{id}}}\n"))
        }
        ("POST", ["vm", id, "work"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let (burst, sum) = state.work(id, arg("bursts", 1));
            Ok(format!(
                "{{\"id\":{id},\"burst\":{burst},\"guest_sum\":\"{sum}\"}}\n"
            ))
        }
        ("POST", ["vm", id, "verify"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let (burst, mismatches) = state.verify(id);
            Ok(format!(
                "{{\"id\":{id},\"burst\":{burst},\"mismatches\":{mismatches},\"ok\":{}}}\n",
                mismatches == 0
            ))
        }
        ("POST", ["vm", id, "fork"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let n = u32::try_from(arg("n", 3)).map_err(|_| "bad n".to_owned())?;
            let (ids, rss, pss, resident) = state.fork(id, n);
            let ids: Vec<String> = ids.iter().map(u64::to_string).collect();
            Ok(format!(
                "{{\"forks\":[{}],\"rss_sum\":{rss},\"pss_sum\":{pss},\"base_resident\":{resident}}}\n",
                ids.join(",")
            ))
        }
        ("POST", ["vm", id, "expect"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            state.expect(id);
            Ok(format!("{{\"id\":{id},\"expecting\":true}}\n"))
        }
        ("POST", ["vm", id, "migrate"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let to = u16::try_from(arg("to", 1)).map_err(|_| "bad to".to_owned())?;
            let (snap_ms, handoff_ms) = state.migrate(id, to);
            Ok(format!(
                "{{\"id\":{id},\"to\":{to},\"snapshot_ms\":{snap_ms},\"handoff_ms\":{handoff_ms}}}\n"
            ))
        }
        ("POST", ["vm", id, "restore"]) => {
            let id: u64 = id.parse().map_err(|_| "bad id".to_owned())?;
            let verdict = state.restore(id);
            Ok(format!("{{\"id\":{id},\"verdict\":\"{verdict}\"}}\n"))
        }
        _ => Err(format!("no route for {method} /{}", segments.join("/"))),
    }
}

fn status_json(state: &Arc<Demod>) -> String {
    let mut vms = String::new();
    for (id, vm) in state.vms.lock().expect("lock").iter() {
        if !vms.is_empty() {
            vms.push(',');
        }
        write!(
            vms,
            "{{\"id\":{id},\"state\":\"{}\",\"backed\":{},\"prefix\":\"{}\"}}",
            vm.state, vm.backed, vm.prefix
        )
        .expect("string write");
    }
    let counters = state.rt.counters();
    let bill = &state.store.stats;
    format!(
        "{{\"host\":{},\"vms\":[{vms}],\
         \"counters\":{{\"fills\":{},\"pages_flushed\":{},\"records_written\":{},\
         \"syncs_acked\":{},\"manifests_published\":{},\"hydrate_fills\":{},\
         \"segs_compacted\":{}}},\
         \"store\":{{\"puts\":{},\"cas_puts\":{},\"gets\":{},\"ranged_gets\":{},\
         \"deletes\":{},\"unavailable\":{},\"bytes_up\":{},\"bytes_down\":{}}},\
         \"peer_dropped_sends\":{},\"incidents\":{}}}\n",
        state.cfg.host.0,
        counters.fills,
        counters.pages_flushed,
        counters.records_written,
        counters.syncs_acked,
        counters.manifests_published,
        counters.hydrate_fills,
        counters.segs_compacted,
        bill.puts.load(Ordering::SeqCst),
        bill.cas_puts.load(Ordering::SeqCst),
        bill.gets.load(Ordering::SeqCst),
        bill.ranged_gets.load(Ordering::SeqCst),
        bill.deletes.load(Ordering::SeqCst),
        bill.unavailable.load(Ordering::SeqCst),
        bill.bytes_up.load(Ordering::SeqCst),
        bill.bytes_down.load(Ordering::SeqCst),
        state.rt.peer_dropped_sends(),
        state.rt.incidents().len(),
    )
}

fn read_request(stream: &mut TcpStream) -> Option<(String, String, BTreeMap<String, String>)> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            _ => return None,
        }
    }
    let text = String::from_utf8_lossy(&buf);
    let request = text.lines().next()?;
    let mut parts = request.split_whitespace();
    let method = parts.next()?.to_owned();
    let target = parts.next()?;
    let (path, query_text) = target.split_once('?').unwrap_or((target, ""));
    let mut query = BTreeMap::new();
    for pair in query_text.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        query.insert(key.to_owned(), value.to_owned());
    }
    Some((method, path.to_owned(), query))
}
