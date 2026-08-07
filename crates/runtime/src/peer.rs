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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use blockd_core::format::{Dec, FRAME_HEADER};
use blockd_core::peer::{MAGIC_PEER, MAX_PEER_PAYLOAD, decode_peer, encode_peer};
use blockd_core::seam::PeerMsg;
use blockd_core::types::HostId;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender, error::TrySendError};
use tokio::time::timeout;

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
    senders: BTreeMap<HostId, Sender<Vec<u8>>>,
    /// Frames dropped on the floor (queue full or peer down) — the demo's
    /// visibility into how hard the retry timers are working.
    pub dropped_sends: Arc<AtomicU64>,
    connected: BTreeMap<HostId, Arc<AtomicBool>>,
    shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    io_thread: Mutex<Option<thread::JoinHandle<()>>>,
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
        let dropped_sends = Arc::new(AtomicU64::new(0));
        let mut senders = BTreeMap::new();
        let mut connected = BTreeMap::new();
        let mut receivers = Vec::new();
        for (&peer, &addr) in &config.peers {
            if peer == self_id {
                continue;
            }
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(SEND_QUEUE);
            let peer_connected = Arc::new(AtomicBool::new(false));
            receivers.push((peer, addr, rx, peer_connected.clone()));
            senders.insert(peer, tx);
            connected.insert(peer, peer_connected);
        }
        let listen = config.listen;
        let deliver: Arc<Deliver> = Arc::new(deliver);
        let dropped = dropped_sends.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let io_thread = thread::Builder::new()
            .name("blockd-peer-io".to_owned())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("peer I/O runtime");
                runtime.block_on(async move {
                    let listener = match TcpListener::bind(listen).await {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready_tx.send(Err(error.to_string()));
                            return;
                        }
                    };
                    for (peer, addr, rx, connected) in receivers {
                        let dropped = dropped.clone();
                        tokio::spawn(async move {
                            sender_loop(rx, addr, peer, connected, dropped).await;
                        });
                    }
                    let _ = ready_tx.send(Ok(()));
                    let listener = tokio::spawn(listener_loop(listener, deliver));
                    let _ = shutdown_rx.await;
                    listener.abort();
                });
            })
            .expect("spawn peer I/O runtime");
        ready_rx
            .recv()
            .expect("peer I/O runtime stopped during startup")
            .unwrap_or_else(|error| panic!("peer listen: {error}"));
        Arc::new(PeerNet {
            senders,
            dropped_sends,
            connected,
            shutdown: Mutex::new(Some(shutdown)),
            io_thread: Mutex::new(Some(io_thread)),
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
        if let Err(TrySendError::Full(_) | TrySendError::Closed(_)) = sender.try_send(frame) {
            self.dropped_sends.fetch_add(1, Ordering::SeqCst);
        }
    }

    pub fn connections(&self) -> Vec<(HostId, bool)> {
        self.connected
            .iter()
            .map(|(&peer, connected)| (peer, connected.load(Ordering::Relaxed)))
            .collect()
    }
}

impl Drop for PeerNet {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.lock().expect("lock").take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.io_thread.lock().expect("lock").take() {
            let _ = thread.join();
        }
    }
}

/// One outbound connection, made lazily and remade on any error. A frame
/// that hits a connect or write failure is dropped with its connection —
/// the protocol's retry timers own recovery, not this loop.
async fn sender_loop(
    mut rx: Receiver<Vec<u8>>,
    addr: SocketAddr,
    peer: HostId,
    connected: Arc<AtomicBool>,
    dropped_sends: Arc<AtomicU64>,
) {
    let mut conn: Option<TcpStream> = None;
    while let Some(frame) = rx.recv().await {
        if conn.is_none() {
            conn = match timeout(Duration::from_secs(1), TcpStream::connect(addr)).await {
                Ok(Ok(stream)) if stream.set_nodelay(true).is_ok() => Some(stream),
                Ok(Ok(_) | Err(_)) | Err(_) => None,
            };
            connected.store(conn.is_some(), Ordering::Relaxed);
        }
        let Some(stream) = conn.as_mut() else {
            dropped_sends.fetch_add(1, Ordering::SeqCst);
            continue; // peer unreachable: drop the frame
        };
        if !matches!(
            timeout(Duration::from_secs(5), stream.write_all(&frame)).await,
            Ok(Ok(()))
        ) {
            dropped_sends.fetch_add(1, Ordering::SeqCst);
            connected.store(false, Ordering::Relaxed);
            tracing::warn!(peer_host = peer.0, %addr, "peer connection lost");
            conn = None;
        }
    }
    connected.store(false, Ordering::Relaxed);
}

/// One inbound connection: length-delimited by the frame header itself,
/// verified by `decode_peer` (magic, length cap, crc, strict layout). Any
/// violation closes the connection; the peer reconnects and retries.
async fn listener_loop(listener: TcpListener, deliver: Arc<Deliver>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let deliver = deliver.clone();
                tokio::spawn(async move { reader_loop(stream, deliver).await });
            }
            Err(error) => {
                tracing::error!(%error, "peer listener stopped");
                return;
            }
        }
    }
}

async fn reader_loop(mut stream: TcpStream, deliver: Arc<Deliver>) {
    let _ = stream.set_nodelay(true);
    loop {
        let mut header = [0u8; FRAME_HEADER];
        if stream.read_exact(&mut header).await.is_err() {
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
        if stream.read_exact(&mut frame[start..]).await.is_err() {
            return;
        }
        let Ok((from, msg)) = decode_peer(&frame) else {
            return; // damage: drop it and the connection (R8.1)
        };
        deliver(from, msg);
    }
}
