//! The peer transport: plain TCP inside the cluster's private network,
//! carrying [`blockd_core::peer`] frames between daemons. The channel
//! contract is at-least-once with drops tolerated (the daemon's retry
//! timers re-drive everything), so this layer is deliberately dumb:
//! sends are fire-and-forget — a dead connection, a full queue, or an
//! unreachable peer just drops the frame; inbound frames that fail
//! verification close the connection and the peer reconnects.
//!
//! mTLS (R11) slots in at exactly two seams — wrap the stream after
//! `connect` and after `accept`, taking the sender identity from the
//! certificate instead of the envelope. The demo trusts the VPC.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread;
use std::time::Duration;

use blockd_core::format::{Dec, FRAME_HEADER};
use blockd_core::peer::{MAGIC_PEER, MAX_PEER_PAYLOAD, decode_peer, encode_peer};
use blockd_core::seam::PeerMsg;
use blockd_core::types::HostId;

/// What the transport does with a verified inbound message.
type Deliver = dyn Fn(HostId, PeerMsg) + Send + Sync;

#[derive(Clone, Debug)]
pub struct PeerConfig {
    /// Where this daemon listens — the VPC-internal address, never public.
    pub listen: SocketAddr,
    /// The cluster roster: peer identity → address (R6.5: the control
    /// plane's knowledge, carried here by static config).
    pub peers: BTreeMap<HostId, SocketAddr>,
}

/// Per-peer outbound queue depth. A full queue drops the newest frame —
/// bounded memory beats delivery the protocol never needed guaranteed.
const SEND_QUEUE: usize = 128;

pub struct PeerNet {
    senders: BTreeMap<HostId, SyncSender<Vec<u8>>>,
    /// Frames dropped on the floor (queue full or peer down) — the demo's
    /// visibility into how hard the retry timers are working.
    pub dropped_sends: AtomicU64,
}

impl PeerNet {
    /// Start the listener and one lazy sender per configured peer;
    /// verified inbound frames reach `deliver` (the runtime injects them
    /// as `Event::PeerDelivered`).
    pub fn start(
        config: &PeerConfig,
        self_id: HostId,
        deliver: impl Fn(HostId, PeerMsg) + Send + Sync + 'static,
    ) -> Arc<PeerNet> {
        let mut senders = BTreeMap::new();
        for (&peer, &addr) in &config.peers {
            if peer == self_id {
                continue;
            }
            let (tx, rx) = sync_channel::<Vec<u8>>(SEND_QUEUE);
            thread::spawn(move || sender_loop(&rx, addr));
            senders.insert(peer, tx);
        }
        let listener = TcpListener::bind(config.listen).expect("peer listen");
        {
            let deliver: Arc<Deliver> = Arc::new(deliver);
            thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { return };
                    let deliver = deliver.clone();
                    thread::spawn(move || reader_loop(stream, &deliver));
                }
            });
        }
        Arc::new(PeerNet {
            senders,
            dropped_sends: AtomicU64::new(0),
        })
    }

    /// Fire-and-forget: encode and queue. Never blocks the daemon thread;
    /// unknown peers and full queues drop the frame (retries re-drive).
    pub fn send(&self, self_id: HostId, to: HostId, msg: &PeerMsg) {
        let Some(sender) = self.senders.get(&to) else {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
            return;
        };
        let frame = encode_peer(self_id, msg);
        if let Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) = sender.try_send(frame) {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// One outbound connection, made lazily and remade on any error. A frame
/// that hits a connect or write failure is dropped with its connection —
/// the protocol's retry timers own recovery, not this loop.
fn sender_loop(rx: &Receiver<Vec<u8>>, addr: SocketAddr) {
    let mut conn: Option<TcpStream> = None;
    while let Ok(frame) = rx.recv() {
        if conn.is_none() {
            conn = TcpStream::connect_timeout(&addr, Duration::from_secs(1))
                .ok()
                .and_then(|stream| {
                    stream.set_nodelay(true).ok()?;
                    stream
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .ok()?;
                    Some(stream)
                });
        }
        let Some(stream) = conn.as_mut() else {
            continue; // peer unreachable: drop the frame
        };
        if stream.write_all(&frame).is_err() {
            conn = None;
        }
    }
}

/// One inbound connection: length-delimited by the frame header itself,
/// verified by `decode_peer` (magic, length cap, crc, strict layout). Any
/// violation closes the connection; the peer reconnects and retries.
fn reader_loop(mut stream: TcpStream, deliver: &Arc<Deliver>) {
    let _ = stream.set_nodelay(true);
    loop {
        let mut header = [0u8; FRAME_HEADER];
        if stream.read_exact(&mut header).is_err() {
            return;
        }
        let mut d = Dec::new(&header);
        let magic = d.u32().expect("12 bytes");
        let len = d.u32().expect("12 bytes");
        if magic != MAGIC_PEER || len > MAX_PEER_PAYLOAD {
            return; // desynced or hostile: drop the connection
        }
        let mut frame = header.to_vec();
        let start = frame.len();
        frame.resize(start + usize::try_from(len).expect("fits"), 0);
        if stream.read_exact(&mut frame[start..]).is_err() {
            return;
        }
        let Ok((from, msg)) = decode_peer(&frame) else {
            return; // damage: drop it and the connection (R8.1)
        };
        deliver(from, msg);
    }
}
