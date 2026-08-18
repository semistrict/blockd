//! Peer transport for one Turmoil host.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use blockd_core::peer::encode_peer;
use blockd_core::protocol::PeerMsg;
use blockd_core::types::HostId;
use blockd_exec::{now, random_u64};
use blockd_transport::{DecodePolicy, receive_loop, write_frame};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, mpsc};
use turmoil::net::{TcpListener, TcpStream};

const PEER_PORT: u16 = 1738;
const SEND_QUEUE: usize = 128;
const IO_TIMEOUT: Duration = Duration::from_secs(10_000);
const CONNECTION_LINGER: Duration = Duration::new(100, 1_000_000);
pub(crate) const MAX_IN_FLIGHT: usize = 64;

pub struct PeerTransport {
    self_id: HostId,
    senders: BTreeMap<HostId, Sender<Vec<u8>>>,
    incoming: Mutex<mpsc::UnboundedReceiver<(HostId, PeerMsg)>>,
    faults: PeerTransportFaults,
    stats: Rc<PeerTransportStats>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PeerTransportFaults {
    pub duplicate_odds: (u64, u64),
    pub targeted_drop: Option<(u8, u64, u64)>,
    pub max_frames_per_connection: usize,
}

#[derive(Debug, Default)]
pub struct PeerTransportStats {
    drops: Cell<u64>,
    duplicates: Cell<u64>,
    clogs: Cell<u64>,
    targeted_drops: Cell<u64>,
    released: Cell<u64>,
}

impl PeerTransportStats {
    pub fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.drops.get(),
            self.duplicates.get(),
            self.clogs.get(),
            self.targeted_drops.get(),
            self.released.get(),
        )
    }

    pub fn record_clog(&self) {
        self.clogs.set(self.clogs.get().saturating_add(1));
    }
}

impl PeerTransport {
    pub async fn start(
        self_id: HostId,
        peers: BTreeMap<HostId, String>,
        faults: PeerTransportFaults,
        stats: Rc<PeerTransportStats>,
    ) -> io::Result<Rc<Self>> {
        let listener = TcpListener::bind(("0.0.0.0", PEER_PORT)).await?;
        let (incoming_tx, incoming) = mpsc::unbounded_channel();
        tokio::task::spawn_local(listener_loop(listener, incoming_tx, Rc::clone(&stats)));

        let mut senders = BTreeMap::new();
        for (peer, host) in peers {
            if peer == self_id {
                continue;
            }
            let (sender, receiver) = mpsc::channel(SEND_QUEUE);
            tokio::task::spawn_local(sender_loop(
                host,
                receiver,
                faults.max_frames_per_connection.max(1),
                Rc::clone(&stats),
            ));
            senders.insert(peer, sender);
        }
        Ok(Rc::new(Self {
            self_id,
            senders,
            incoming: Mutex::new(incoming),
            faults,
            stats,
        }))
    }

    pub async fn send(&self, to: HostId, message: &PeerMsg) -> bool {
        let Some(sender) = self.senders.get(&to) else {
            return false;
        };
        if self.faults.targeted_drop.is_some_and(|(kind, begin, end)| {
            kind == peer_tag(message) && (begin..end).contains(&now())
        }) {
            self.stats
                .targeted_drops
                .set(self.stats.targeted_drops.get().saturating_add(1));
            return false;
        }
        let copies = if odds(self.faults.duplicate_odds) {
            self.stats
                .duplicates
                .set(self.stats.duplicates.get().saturating_add(1));
            2
        } else {
            1
        };
        let frame = encode_peer(self.self_id, message);
        let mut accepted = true;
        for _ in 0..copies {
            if sender.send(frame.clone()).await.is_err() {
                self.stats
                    .drops
                    .set(self.stats.drops.get().saturating_add(1));
                accepted = false;
            }
        }
        accepted
    }

    pub async fn recv(&self) -> Option<(HostId, PeerMsg)> {
        self.incoming.lock().await.recv().await
    }
}

async fn sender_loop(
    host: String,
    receiver: Receiver<Vec<u8>>,
    max_frames_per_connection: usize,
    stats: Rc<PeerTransportStats>,
) {
    if max_frames_per_connection == 1 {
        frame_scoped_sender_loop(host, receiver, stats).await;
    } else {
        persistent_sender_loop(host, receiver, max_frames_per_connection, stats).await;
    }
}

async fn frame_scoped_sender_loop(
    host: String,
    mut receiver: Receiver<Vec<u8>>,
    stats: Rc<PeerTransportStats>,
) {
    let in_flight = Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT));
    while let Some(frame) = receiver.recv().await {
        let permit = Arc::clone(&in_flight)
            .acquire_owned()
            .await
            .expect("peer transport semaphore is never closed");
        let host = host.clone();
        let stats = Rc::clone(&stats);
        tokio::task::spawn_local(async move {
            let _permit = permit;
            send_frame(&host, &frame, &stats).await;
        });
    }
}

async fn send_frame(host: &str, frame: &[u8], stats: &PeerTransportStats) {
    let Ok(Ok(mut connection)) =
        tokio::time::timeout(IO_TIMEOUT, TcpStream::connect((host, PEER_PORT))).await
    else {
        stats.drops.set(stats.drops.get().saturating_add(1));
        return;
    };
    if !matches!(
        tokio::time::timeout(IO_TIMEOUT, write_frame(&mut connection, frame)).await,
        Ok(Ok(()))
    ) {
        stats.drops.set(stats.drops.get().saturating_add(1));
        return;
    }
    tokio::time::sleep(CONNECTION_LINGER).await;
}

async fn persistent_sender_loop(
    host: String,
    mut receiver: Receiver<Vec<u8>>,
    max_frames_per_connection: usize,
    stats: Rc<PeerTransportStats>,
) {
    let mut connection = None;
    let mut frames_on_connection = 0;
    while let Some(frame) = receiver.recv().await {
        if connection.is_none() || frames_on_connection == max_frames_per_connection {
            connection = if let Ok(Ok(stream)) =
                tokio::time::timeout(IO_TIMEOUT, TcpStream::connect((host.as_str(), PEER_PORT)))
                    .await
            {
                Some(stream)
            } else {
                stats.drops.set(stats.drops.get().saturating_add(1));
                None
            };
            frames_on_connection = 0;
        }
        let Some(stream) = connection.as_mut() else {
            continue;
        };
        if let Ok(Ok(())) = tokio::time::timeout(IO_TIMEOUT, write_frame(stream, &frame)).await {
            frames_on_connection += 1;
        } else {
            stats.drops.set(stats.drops.get().saturating_add(1));
            connection = None;
        }
    }
}

async fn listener_loop(
    listener: TcpListener,
    incoming: mpsc::UnboundedSender<(HostId, PeerMsg)>,
    stats: Rc<PeerTransportStats>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let incoming = incoming.clone();
        let stats = Rc::clone(&stats);
        tokio::task::spawn_local(async move {
            let _ = receive_loop(stream, None, DecodePolicy::Inline, move |from, message| {
                if peer_tag(&message) == 6 {
                    stats.released.set(stats.released.get().saturating_add(1));
                }
                let _ = incoming.send((from, message));
            })
            .await;
        });
    }
}

fn odds((numerator, denominator): (u64, u64)) -> bool {
    numerator != 0 && denominator != 0 && random_u64() % denominator < numerator
}

fn peer_tag(message: &PeerMsg) -> u8 {
    match message {
        PeerMsg::MigrateOffer { .. } => 0,
        PeerMsg::MigrateAccept { .. } => 1,
        PeerMsg::FetchRange { .. } => 2,
        PeerMsg::Page { .. } => 3,
        PeerMsg::Released { .. } => 6,
        PeerMsg::ReleasedAck { .. } => 7,
        PeerMsg::ReplicaPut { .. } => 8,
        PeerMsg::ReplicaPutAck { .. } => 9,
        PeerMsg::ReplicaCommit { .. } => 10,
        PeerMsg::ReplicaCommitAck { .. } => 11,
        PeerMsg::ReplicaStatus { .. } => 13,
        PeerMsg::ReplicaStatusReply { .. } => 14,
        PeerMsg::ReplicaRelease { .. } => 16,
        PeerMsg::ReplicaReleaseAck { .. } => 17,
        PeerMsg::VnodeAdopt { .. } => 18,
        PeerMsg::VnodeAdoptAck { .. } => 19,
        PeerMsg::VnodeFetchClosure { .. } => 20,
        PeerMsg::VnodeClosure { .. } => 21,
        PeerMsg::VnodeCommit { .. } => 22,
        PeerMsg::VnodeCommitAck { .. } => 23,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use blockd_core::types::VsetId;

    use super::*;

    #[test]
    fn separate_turmoil_hosts_exchange_real_peer_frames() {
        let received = Arc::new(Mutex::new(None));
        let server_received = Arc::clone(&received);
        let roster = BTreeMap::from([
            (HostId(0), "host-0".to_owned()),
            (HostId(1), "host-1".to_owned()),
        ]);
        let server_roster = roster.clone();
        let client_roster = roster;
        let mut simulation = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(1))
            .build();
        simulation.client("controller", async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(())
        });
        simulation.host("host-1", move || {
            let received = Arc::clone(&server_received);
            let roster = server_roster.clone();
            async move {
                let transport = PeerTransport::start(
                    HostId(1),
                    roster,
                    PeerTransportFaults::default(),
                    Rc::new(PeerTransportStats::default()),
                )
                .await?;
                *received.lock().expect("received lock") = transport.recv().await;
                Ok(())
            }
        });
        simulation.host("host-0", move || {
            let roster = client_roster.clone();
            async move {
                let transport = PeerTransport::start(
                    HostId(0),
                    roster,
                    PeerTransportFaults::default(),
                    Rc::new(PeerTransportStats::default()),
                )
                .await?;
                let message = PeerMsg::Released {
                    vset: VsetId(7),
                    release_fence: 11,
                };
                for _ in 0..10 {
                    let _ = transport.send(HostId(1), &message).await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(())
            }
        });

        simulation.run().unwrap();
        assert_eq!(
            received.lock().expect("received lock").take(),
            Some((
                HostId(0),
                PeerMsg::Released {
                    vset: VsetId(7),
                    release_fence: 11,
                }
            ))
        );
    }
}
