//! Peer transport for one Turmoil host.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use blockd_core::peer::encode_peer_routed;
use blockd_core::protocol::PeerMsg;
use blockd_core::types::HostId;
use blockd_exec::{now, random_u64};
use blockd_transport::{decode_routed_frame, read_frame, write_frame};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, mpsc};
use turmoil::net::{TcpListener, TcpStream};

const PEER_PORT: u16 = 1738;
const SEND_QUEUE: usize = 128;
const IO_TIMEOUT: Duration = Duration::from_secs(10_000);
pub(crate) const MAX_IN_FLIGHT: usize = 64;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PeerAuthorization {
    pub identity: HostId,
    pub certificate_generation: u64,
}

impl PeerAuthorization {
    pub fn new(identity: HostId, certificate_generation: u64) -> Self {
        assert_ne!(certificate_generation, 0, "certificate generation");
        Self {
            identity,
            certificate_generation,
        }
    }
}

pub(crate) type PeerMembership = Rc<RefCell<BTreeMap<HostId, PeerAuthorization>>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CertificateBinding {
    sender: HostId,
    recipient: HostId,
    sender_generation: u64,
    recipient_generation: u64,
}

#[derive(Clone)]
struct OutboundFrame {
    bytes: Option<Vec<u8>>,
    binding: CertificateBinding,
}

struct HeldAuthenticationProbe {
    target: HostId,
    frame: OutboundFrame,
}

pub struct PeerTransport {
    self_id: HostId,
    senders: BTreeMap<HostId, Sender<OutboundFrame>>,
    incoming: Mutex<mpsc::UnboundedReceiver<(HostId, PeerMsg)>>,
    faults: PeerTransportFaults,
    stats: Rc<PeerTransportStats>,
    membership: PeerMembership,
    held_authentication_probe: RefCell<Option<HeldAuthenticationProbe>>,
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
    authorization_drops: Cell<u64>,
    certificate_authorization_drops: Cell<u64>,
    renewed_certificate_frames: Cell<u64>,
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

    pub fn authorization_drops(&self) -> u64 {
        self.authorization_drops.get()
    }

    pub fn certificate_authorization_drops(&self) -> u64 {
        self.certificate_authorization_drops.get()
    }

    pub fn renewed_certificate_frames(&self) -> u64 {
        self.renewed_certificate_frames.get()
    }
}

impl PeerTransport {
    pub async fn start(
        self_id: HostId,
        peers: BTreeMap<HostId, String>,
        faults: PeerTransportFaults,
        stats: Rc<PeerTransportStats>,
    ) -> io::Result<Rc<Self>> {
        let membership = Rc::new(RefCell::new(
            peers
                .keys()
                .copied()
                .chain([self_id])
                .map(|identity| (identity, PeerAuthorization::new(identity, 1)))
                .collect(),
        ));
        Self::start_with_membership(self_id, peers, faults, stats, membership).await
    }

    pub(crate) async fn start_with_membership(
        self_id: HostId,
        peers: BTreeMap<HostId, String>,
        faults: PeerTransportFaults,
        stats: Rc<PeerTransportStats>,
        membership: PeerMembership,
    ) -> io::Result<Rc<Self>> {
        let listener = TcpListener::bind(("0.0.0.0", PEER_PORT)).await?;
        let (incoming_tx, incoming) = mpsc::unbounded_channel();
        tokio::task::spawn_local(listener_loop(
            listener,
            incoming_tx,
            Rc::clone(&stats),
            Rc::clone(&membership),
            self_id,
        ));

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
            assert!(
                senders.insert(peer, sender).is_none(),
                "peer roster contains two live allocations for one numeric host"
            );
        }
        Ok(Rc::new(Self {
            self_id,
            senders,
            incoming: tokio::sync::Mutex::new(incoming),
            faults,
            stats,
            membership,
            held_authentication_probe: RefCell::new(None),
        }))
    }

    pub fn send(&self, to: HostId, message: &PeerMsg) -> bool {
        let Some(binding) = self.certificate_binding(to) else {
            return self.reject_unauthorized();
        };
        let Some(sender) = self.senders.get(&to) else {
            return false;
        };
        if self.faults.targeted_drop.is_some_and(|(kind, begin, end)| {
            kind == message.tag() && (begin..end).contains(&now())
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
        let frame = OutboundFrame {
            bytes: Some(encode_peer_routed(self.self_id, to, message)),
            binding,
        };
        let mut accepted = true;
        for _ in 0..copies {
            // Network backpressure is itself a modeled packet loss. Never let
            // a partition fill this bounded queue and suspend the core actor
            // before its protocol-level retry timeout can start.
            if sender.try_send(frame.clone()).is_err() {
                self.stats
                    .drops
                    .set(self.stats.drops.get().saturating_add(1));
                accepted = false;
            }
        }
        accepted
    }

    pub(crate) fn send_authentication_probe(&self, to: HostId) -> bool {
        let Some(binding) = self.certificate_binding(to) else {
            return self.reject_unauthorized();
        };
        let Some(sender) = self.senders.get(&to) else {
            return false;
        };
        if sender
            .try_send(OutboundFrame {
                bytes: None,
                binding,
            })
            .is_err()
        {
            self.stats
                .drops
                .set(self.stats.drops.get().saturating_add(1));
            return false;
        }
        true
    }

    pub(crate) fn hold_authentication_probe(&self, to: HostId) -> bool {
        let Some(binding) = self.certificate_binding(to) else {
            return self.reject_unauthorized();
        };
        let mut held = self.held_authentication_probe.borrow_mut();
        if held.is_some() {
            return false;
        }
        *held = Some(HeldAuthenticationProbe {
            target: to,
            frame: OutboundFrame {
                bytes: None,
                binding,
            },
        });
        true
    }

    pub(crate) fn release_authentication_probe(&self) -> bool {
        let Some(held) = self.held_authentication_probe.borrow_mut().take() else {
            return false;
        };
        let Some(sender) = self.senders.get(&held.target) else {
            return false;
        };
        if sender.try_send(held.frame).is_err() {
            self.stats
                .drops
                .set(self.stats.drops.get().saturating_add(1));
            return false;
        }
        true
    }

    fn certificate_binding(&self, to: HostId) -> Option<CertificateBinding> {
        let membership = self.membership.borrow();
        let sender = membership
            .get(&self.self_id)
            .filter(|authorization| authorization.identity == self.self_id)?;
        let recipient = membership
            .get(&to)
            .filter(|authorization| authorization.identity == to)?;
        Some(CertificateBinding {
            sender: sender.identity,
            recipient: recipient.identity,
            sender_generation: sender.certificate_generation,
            recipient_generation: recipient.certificate_generation,
        })
    }

    fn reject_unauthorized(&self) -> bool {
        self.stats
            .authorization_drops
            .set(self.stats.authorization_drops.get().saturating_add(1));
        self.stats
            .drops
            .set(self.stats.drops.get().saturating_add(1));
        false
    }

    pub async fn recv(&self) -> Option<(HostId, PeerMsg)> {
        self.incoming.lock().await.recv().await
    }
}

async fn sender_loop(
    host: String,
    receiver: Receiver<OutboundFrame>,
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
    mut receiver: Receiver<OutboundFrame>,
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

async fn send_frame(host: &str, frame: &OutboundFrame, stats: &PeerTransportStats) {
    let Ok(Ok(mut connection)) =
        tokio::time::timeout(IO_TIMEOUT, TcpStream::connect((host, PEER_PORT))).await
    else {
        stats.drops.set(stats.drops.get().saturating_add(1));
        return;
    };
    if !write_certificate_binding(&mut connection, frame.binding, frame.bytes.is_none()).await {
        stats.drops.set(stats.drops.get().saturating_add(1));
        return;
    }
    let Some(bytes) = &frame.bytes else {
        return;
    };
    if !matches!(
        tokio::time::timeout(IO_TIMEOUT, write_frame(&mut connection, bytes)).await,
        Ok(Ok(()))
    ) {
        stats.drops.set(stats.drops.get().saturating_add(1));
    }
}

async fn persistent_sender_loop(
    host: String,
    mut receiver: Receiver<OutboundFrame>,
    max_frames_per_connection: usize,
    stats: Rc<PeerTransportStats>,
) {
    let mut connection = None;
    let mut connection_binding = None;
    let mut frames_on_connection = 0;
    while let Some(frame) = receiver.recv().await {
        if connection.is_none()
            || connection_binding != Some(frame.binding)
            || frames_on_connection == max_frames_per_connection
            || frame.bytes.is_none()
        {
            connection = if let Ok(Ok(mut stream)) =
                tokio::time::timeout(IO_TIMEOUT, TcpStream::connect((host.as_str(), PEER_PORT)))
                    .await
            {
                if write_certificate_binding(&mut stream, frame.binding, frame.bytes.is_none())
                    .await
                {
                    Some(stream)
                } else {
                    None
                }
            } else {
                stats.drops.set(stats.drops.get().saturating_add(1));
                None
            };
            connection_binding = connection.as_ref().map(|_| frame.binding);
            frames_on_connection = 0;
        }
        let Some(stream) = connection.as_mut() else {
            continue;
        };
        let Some(bytes) = &frame.bytes else {
            connection = None;
            connection_binding = None;
            continue;
        };
        if let Ok(Ok(())) = tokio::time::timeout(IO_TIMEOUT, write_frame(stream, bytes)).await {
            frames_on_connection += 1;
        } else {
            stats.drops.set(stats.drops.get().saturating_add(1));
            connection = None;
            connection_binding = None;
        }
    }
}

async fn write_certificate_binding(
    stream: &mut TcpStream,
    binding: CertificateBinding,
    authentication_probe: bool,
) -> bool {
    let mut encoded = [0_u8; 25];
    encoded[..4].copy_from_slice(&binding.sender.get().to_le_bytes());
    encoded[4..8].copy_from_slice(&binding.recipient.get().to_le_bytes());
    encoded[8..16].copy_from_slice(&binding.sender_generation.to_le_bytes());
    encoded[16..24].copy_from_slice(&binding.recipient_generation.to_le_bytes());
    encoded[24] = u8::from(authentication_probe);
    matches!(
        tokio::time::timeout(IO_TIMEOUT, stream.write_all(&encoded)).await,
        Ok(Ok(()))
    )
}

async fn read_certificate_binding(stream: &mut TcpStream) -> Option<(CertificateBinding, bool)> {
    let mut encoded = [0_u8; 25];
    match tokio::time::timeout(IO_TIMEOUT, stream.read_exact(&mut encoded)).await {
        Ok(Ok(_)) => {}
        Ok(Err(_)) | Err(_) => return None,
    }
    let binding = CertificateBinding {
        sender: HostId::new(u32::from_le_bytes(encoded[..4].try_into().ok()?)),
        recipient: HostId::new(u32::from_le_bytes(encoded[4..8].try_into().ok()?)),
        sender_generation: u64::from_le_bytes(encoded[8..16].try_into().ok()?),
        recipient_generation: u64::from_le_bytes(encoded[16..24].try_into().ok()?),
    };
    (binding.sender_generation != 0 && binding.recipient_generation != 0)
        .then_some((binding, encoded[24] != 0))
}

async fn listener_loop(
    listener: TcpListener,
    incoming: mpsc::UnboundedSender<(HostId, PeerMsg)>,
    stats: Rc<PeerTransportStats>,
    membership: PeerMembership,
    recipient: HostId,
) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let incoming = incoming.clone();
        let stats = Rc::clone(&stats);
        let membership = Rc::clone(&membership);
        tokio::task::spawn_local(async move {
            let Some((binding, authentication_probe)) = read_certificate_binding(&mut stream).await
            else {
                stats.drops.set(stats.drops.get().saturating_add(1));
                return;
            };
            if !authorize_binding(&membership, binding, recipient, &stats) {
                return;
            }
            if authentication_probe {
                record_renewed_certificate(binding, &stats);
                return;
            }
            loop {
                let Ok(frame) = read_frame(&mut stream).await else {
                    return;
                };
                let Ok((from, message)) = decode_routed_frame(&frame, None, recipient) else {
                    stats.drops.set(stats.drops.get().saturating_add(1));
                    return;
                };
                if from != binding.sender
                    || recipient != binding.recipient
                    || !authorize_binding(&membership, binding, recipient, &stats)
                {
                    return;
                }
                record_renewed_certificate(binding, &stats);
                if matches!(message, PeerMsg::Released { .. }) {
                    stats.released.set(stats.released.get().saturating_add(1));
                }
                let _ = incoming.send((from, message));
            }
        });
    }
}

fn authorize_binding(
    membership: &PeerMembership,
    binding: CertificateBinding,
    listener: HostId,
    stats: &PeerTransportStats,
) -> bool {
    let authorization = membership.borrow();
    let sender = authorization.get(&binding.sender);
    let recipient = authorization.get(&binding.recipient);
    let identities_valid = binding.recipient == listener
        && sender.is_some_and(|entry| entry.identity == binding.sender)
        && recipient.is_some_and(|entry| entry.identity == binding.recipient);
    let certificates_valid = sender
        .is_some_and(|entry| entry.certificate_generation == binding.sender_generation)
        && recipient
            .is_some_and(|entry| entry.certificate_generation == binding.recipient_generation);
    drop(authorization);
    if !identities_valid || !certificates_valid {
        stats
            .authorization_drops
            .set(stats.authorization_drops.get().saturating_add(1));
        if identities_valid {
            stats.certificate_authorization_drops.set(
                stats
                    .certificate_authorization_drops
                    .get()
                    .saturating_add(1),
            );
        }
        stats.drops.set(stats.drops.get().saturating_add(1));
        return false;
    }
    true
}

fn record_renewed_certificate(binding: CertificateBinding, stats: &PeerTransportStats) {
    if binding.sender_generation > 1 || binding.recipient_generation > 1 {
        stats
            .renewed_certificate_frames
            .set(stats.renewed_certificate_frames.get().saturating_add(1));
    }
}

fn odds((numerator, denominator): (u64, u64)) -> bool {
    numerator != 0 && denominator != 0 && random_u64() % denominator < numerator
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use blockd_core::types::VolumeId;

    use super::*;

    #[test]
    fn separate_turmoil_hosts_exchange_real_peer_frames() {
        let received = Arc::new(Mutex::new(None));
        let server_received = Arc::clone(&received);
        let host_0 = HostId::new(0);
        let host_1 = HostId::new(1);
        let roster = BTreeMap::from([(host_0, "host-0".to_owned()), (host_1, "host-1".to_owned())]);
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
                    host_1,
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
                    host_0,
                    roster,
                    PeerTransportFaults::default(),
                    Rc::new(PeerTransportStats::default()),
                )
                .await?;
                let message = PeerMsg::Released {
                    volume: VolumeId(7),
                    release_fence: 11,
                };
                for _ in 0..10 {
                    let _ = transport.send(host_1, &message);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(())
            }
        });

        simulation.run().unwrap();
        assert_eq!(
            received.lock().expect("received lock").take(),
            Some((
                host_0,
                PeerMsg::Released {
                    volume: VolumeId(7),
                    release_fence: 11,
                }
            ))
        );
    }

    #[tokio::test]
    async fn bounded_send_queue_drops_instead_of_blocking_actor() {
        let sender = HostId::new(0);
        let receiver = HostId::new(1);
        let (outgoing, _queued) = mpsc::channel(SEND_QUEUE);
        let (_incoming_sender, incoming) = mpsc::unbounded_channel();
        let stats = Rc::new(PeerTransportStats::default());
        let transport = PeerTransport {
            self_id: sender,
            senders: BTreeMap::from([(receiver, outgoing)]),
            incoming: tokio::sync::Mutex::new(incoming),
            faults: PeerTransportFaults::default(),
            stats: Rc::clone(&stats),
            membership: Rc::new(RefCell::new(BTreeMap::from([
                (sender, PeerAuthorization::new(sender, 1)),
                (receiver, PeerAuthorization::new(receiver, 1)),
            ]))),
            held_authentication_probe: RefCell::new(None),
        };
        let message = PeerMsg::Released {
            volume: VolumeId(7),
            release_fence: 11,
        };

        for _ in 0..SEND_QUEUE {
            assert!(transport.send(receiver, &message));
        }
        assert!(!transport.send(receiver, &message));
        assert_eq!(stats.snapshot().0, 1);
    }

    #[test]
    #[allow(clippy::similar_names, clippy::too_many_lines)]
    fn delayed_departed_host_frame_is_not_aliased_to_a_distinct_member() {
        let retired = HostId::new(0);
        let replacement = HostId::new(2);
        let receiver = HostId::new(1);
        let membership: PeerMembership = Rc::new(RefCell::new(BTreeMap::from([
            (retired, PeerAuthorization::new(retired, 1)),
            (receiver, PeerAuthorization::new(receiver, 1)),
        ])));
        let stats = Rc::new(PeerTransportStats::default());
        let queued = Rc::new(Cell::new(false));
        let reused = Rc::new(tokio::sync::Notify::new());
        let received = Rc::new(RefCell::new(None));
        let complete = Rc::new(tokio::sync::Notify::new());

        let mut simulation = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(2))
            .tick_duration(Duration::from_millis(1))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(1))
            .build();
        let retired_membership = Rc::clone(&membership);
        let retired_stats = Rc::clone(&stats);
        let retired_queued = Rc::clone(&queued);
        simulation.host("retired", move || {
            let membership = Rc::clone(&retired_membership);
            let stats = Rc::clone(&retired_stats);
            let queued = Rc::clone(&retired_queued);
            async move {
                let transport = PeerTransport::start_with_membership(
                    retired,
                    BTreeMap::from([
                        (retired, "retired".to_owned()),
                        (receiver, "receiver".to_owned()),
                    ]),
                    PeerTransportFaults::default(),
                    stats,
                    membership,
                )
                .await?;
                let _probe = TcpStream::connect(("receiver", PEER_PORT)).await?;
                turmoil::hold("retired", "receiver");
                assert!(transport.send(
                    receiver,
                    &PeerMsg::Released {
                        volume: VolumeId(7),
                        release_fence: 11,
                    },
                ));
                queued.set(true);
                std::future::pending::<()>().await;
                Ok(())
            }
        });

        let receiver_membership = Rc::clone(&membership);
        let receiver_stats = Rc::clone(&stats);
        let receiver_received = Rc::clone(&received);
        let receiver_complete = Rc::clone(&complete);
        simulation.host("receiver", move || {
            let membership = Rc::clone(&receiver_membership);
            let stats = Rc::clone(&receiver_stats);
            let received = Rc::clone(&receiver_received);
            let complete = Rc::clone(&receiver_complete);
            async move {
                let transport = PeerTransport::start_with_membership(
                    receiver,
                    BTreeMap::from([(receiver, "receiver".to_owned())]),
                    PeerTransportFaults::default(),
                    stats,
                    membership,
                )
                .await?;
                *received.borrow_mut() = transport.recv().await;
                complete.notify_one();
                Ok(())
            }
        });

        let replacement_membership = Rc::clone(&membership);
        let replacement_stats = Rc::clone(&stats);
        let replacement_reused = Rc::clone(&reused);
        simulation.host("replacement", move || {
            let membership = Rc::clone(&replacement_membership);
            let stats = Rc::clone(&replacement_stats);
            let reused = Rc::clone(&replacement_reused);
            async move {
                reused.notified().await;
                let transport = PeerTransport::start_with_membership(
                    replacement,
                    BTreeMap::from([
                        (replacement, "replacement".to_owned()),
                        (receiver, "receiver".to_owned()),
                    ]),
                    PeerTransportFaults::default(),
                    stats,
                    membership,
                )
                .await?;
                assert!(transport.send(
                    receiver,
                    &PeerMsg::Released {
                        volume: VolumeId(7),
                        release_fence: 12,
                    },
                ));
                std::future::pending::<()>().await;
                Ok(())
            }
        });

        simulation.client("controller", async move {
            complete.notified().await;
            Ok(())
        });

        while !queued.get() {
            simulation.step().unwrap();
        }
        for _ in 0..10 {
            simulation.step().unwrap();
        }
        membership.borrow_mut().remove(&retired);
        membership
            .borrow_mut()
            .insert(replacement, PeerAuthorization::new(replacement, 1));
        simulation.release("retired", "receiver");
        while stats.authorization_drops() == 0 {
            simulation.step().unwrap();
        }
        reused.notify_one();
        simulation.run().unwrap();
        assert_eq!(
            received.borrow_mut().take(),
            Some((
                replacement,
                PeerMsg::Released {
                    volume: VolumeId(7),
                    release_fence: 12,
                },
            ))
        );
    }

    #[test]
    #[allow(clippy::similar_names, clippy::too_many_lines)]
    fn delayed_obsolete_certificate_frame_is_rejected_and_renewed_traffic_recovers() {
        let sender = HostId::new(0);
        let receiver = HostId::new(1);
        let membership: PeerMembership = Rc::new(RefCell::new(BTreeMap::from([
            (sender, PeerAuthorization::new(sender, 1)),
            (receiver, PeerAuthorization::new(receiver, 1)),
        ])));
        let stats = Rc::new(PeerTransportStats::default());
        let queued = Rc::new(Cell::new(false));
        let rotated = Rc::new(tokio::sync::Notify::new());
        let received = Rc::new(RefCell::new(None));
        let complete = Rc::new(tokio::sync::Notify::new());

        let mut simulation = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(2))
            .tick_duration(Duration::from_millis(1))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(1))
            .build();
        let sender_membership = Rc::clone(&membership);
        let sender_stats = Rc::clone(&stats);
        let sender_queued = Rc::clone(&queued);
        let sender_rotated = Rc::clone(&rotated);
        simulation.host("sender", move || {
            let membership = Rc::clone(&sender_membership);
            let stats = Rc::clone(&sender_stats);
            let queued = Rc::clone(&sender_queued);
            let rotated = Rc::clone(&sender_rotated);
            async move {
                let transport = PeerTransport::start_with_membership(
                    sender,
                    BTreeMap::from([
                        (sender, "sender".to_owned()),
                        (receiver, "receiver".to_owned()),
                    ]),
                    PeerTransportFaults::default(),
                    stats,
                    membership,
                )
                .await?;
                let _probe = TcpStream::connect(("receiver", PEER_PORT)).await?;
                turmoil::hold("sender", "receiver");
                assert!(transport.send(
                    receiver,
                    &PeerMsg::Released {
                        volume: VolumeId(7),
                        release_fence: 11,
                    },
                ));
                queued.set(true);
                rotated.notified().await;
                assert!(transport.send(
                    receiver,
                    &PeerMsg::Released {
                        volume: VolumeId(7),
                        release_fence: 12,
                    },
                ));
                std::future::pending::<()>().await;
                Ok(())
            }
        });

        let receiver_membership = Rc::clone(&membership);
        let receiver_stats = Rc::clone(&stats);
        let receiver_received = Rc::clone(&received);
        let receiver_complete = Rc::clone(&complete);
        simulation.host("receiver", move || {
            let membership = Rc::clone(&receiver_membership);
            let stats = Rc::clone(&receiver_stats);
            let received = Rc::clone(&receiver_received);
            let complete = Rc::clone(&receiver_complete);
            async move {
                let transport = PeerTransport::start_with_membership(
                    receiver,
                    BTreeMap::from([(receiver, "receiver".to_owned())]),
                    PeerTransportFaults::default(),
                    stats,
                    membership,
                )
                .await?;
                *received.borrow_mut() = transport.recv().await;
                complete.notify_one();
                Ok(())
            }
        });
        simulation.client("controller", async move {
            complete.notified().await;
            Ok(())
        });

        while !queued.get() {
            simulation.step().unwrap();
        }
        membership
            .borrow_mut()
            .insert(sender, PeerAuthorization::new(sender, 2));
        rotated.notify_one();
        for _ in 0..10 {
            simulation.step().unwrap();
        }
        simulation.release("sender", "receiver");
        simulation.run().unwrap();
        assert_eq!(stats.certificate_authorization_drops(), 1);
        assert_eq!(stats.renewed_certificate_frames(), 1);
        assert_eq!(
            received.borrow_mut().take(),
            Some((
                sender,
                PeerMsg::Released {
                    volume: VolumeId(7),
                    release_fence: 12,
                },
            ))
        );
    }
}
