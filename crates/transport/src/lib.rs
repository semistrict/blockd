#![forbid(unsafe_code)]

//! Peer wire framing shared by real Tokio sockets and Turmoil sockets.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use blockd_core::format::{Dec, FRAME_HEADER};
use blockd_core::peer::{MAGIC_PEER, MAX_PEER_PAYLOAD, decode_peer, decode_peer_routed};
use blockd_core::protocol::PeerMsg;
use blockd_core::types::HostId;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::sync::Semaphore;

#[derive(Clone, Default)]
pub struct ReceiveMetrics(Arc<ReceiveMetricCounters>);

#[derive(Default)]
struct ReceiveMetricCounters {
    payload_budget_waits: AtomicU64,
    frame_read_timeouts: AtomicU64,
    idle_timeouts: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReceiveMetricsSnapshot {
    pub payload_budget_waits: u64,
    pub frame_read_timeouts: u64,
    pub idle_timeouts: u64,
}

#[derive(Clone)]
pub struct ReceiveLimits {
    pub read_timeout: Duration,
    pub inflight_bytes: Arc<Semaphore>,
    pub metrics: ReceiveMetrics,
}

#[derive(Default)]
struct ReceiveConfig {
    read_timeout: Option<Duration>,
    inflight_bytes: Option<Arc<Semaphore>>,
    metrics: ReceiveMetrics,
}

impl ReceiveConfig {
    fn timeout(read_timeout: Duration) -> Self {
        Self {
            read_timeout: Some(read_timeout),
            inflight_bytes: None,
            metrics: ReceiveMetrics::default(),
        }
    }
}

impl From<ReceiveLimits> for ReceiveConfig {
    fn from(limits: ReceiveLimits) -> Self {
        Self {
            read_timeout: Some(limits.read_timeout),
            inflight_bytes: Some(limits.inflight_bytes),
            metrics: limits.metrics,
        }
    }
}

impl ReceiveMetrics {
    pub fn snapshot(&self) -> ReceiveMetricsSnapshot {
        ReceiveMetricsSnapshot {
            payload_budget_waits: self.0.payload_budget_waits.load(Ordering::Relaxed),
            frame_read_timeouts: self.0.frame_read_timeouts.load(Ordering::Relaxed),
            idle_timeouts: self.0.idle_timeouts.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodePolicy {
    /// Decode on the current deterministic host runtime.
    Inline,
    /// Move frames at or above this size to Tokio's blocking pool.
    BlockingAbove(usize),
}

/// Read one bounded peer frame from any Tokio-compatible byte stream.
pub async fn read_frame(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    read_frame_budgeted(stream, None)
        .await
        .map(|(frame, _)| frame)
}

async fn read_frame_budgeted(
    stream: &mut (impl AsyncRead + Unpin),
    budget: Option<&std::sync::Arc<Semaphore>>,
) -> io::Result<(Vec<u8>, Option<tokio::sync::OwnedSemaphorePermit>)> {
    read_frame_budgeted_with_metrics(stream, budget, None, &ReceiveMetrics::default()).await
}

async fn read_exact_with_deadline(
    stream: &mut (impl AsyncRead + Unpin),
    bytes: &mut [u8],
    deadline: Option<Duration>,
) -> Result<(), (io::Error, bool)> {
    let mut consumed = 0;
    let read = async {
        while consumed < bytes.len() {
            let count = stream.read(&mut bytes[consumed..]).await?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "peer stream ended",
                ));
            }
            consumed += count;
        }
        Ok(())
    };
    match deadline {
        Some(deadline) => match tokio::time::timeout(deadline, read).await {
            Ok(result) => result.map_err(|error| (error, consumed > 0)),
            Err(_) => Err((
                io::Error::new(io::ErrorKind::TimedOut, "peer frame read timed out"),
                consumed > 0,
            )),
        },
        None => read.await.map_err(|error| (error, consumed > 0)),
    }
}

async fn read_frame_budgeted_with_metrics(
    stream: &mut (impl AsyncRead + Unpin),
    budget: Option<&Arc<Semaphore>>,
    read_timeout: Option<Duration>,
    metrics: &ReceiveMetrics,
) -> io::Result<(Vec<u8>, Option<tokio::sync::OwnedSemaphorePermit>)> {
    let mut header = [0u8; FRAME_HEADER];
    if let Err((error, progressed)) =
        read_exact_with_deadline(stream, &mut header, read_timeout).await
    {
        if error.kind() == io::ErrorKind::TimedOut {
            if progressed {
                metrics
                    .0
                    .frame_read_timeouts
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                metrics.0.idle_timeouts.fetch_add(1, Ordering::Relaxed);
            }
        }
        return Err(error);
    }
    let mut decoder = Dec::new(&header);
    let magic = decoder.u32().expect("fixed peer header");
    let payload_len = decoder.u32().expect("fixed peer header");
    if magic != MAGIC_PEER || payload_len > MAX_PEER_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid peer frame header",
        ));
    }

    let permit = match budget {
        Some(budget) => match Arc::clone(budget).try_acquire_many_owned(payload_len.max(1)) {
            Ok(permit) => Some(permit),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                metrics
                    .0
                    .payload_budget_waits
                    .fetch_add(1, Ordering::Relaxed);
                Some(
                    Arc::clone(budget)
                        .acquire_many_owned(payload_len.max(1))
                        .await
                        .map_err(|_| io::Error::other("peer frame budget closed"))?,
                )
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(io::Error::other("peer frame budget closed"));
            }
        },
        None => None,
    };

    let mut frame = header.to_vec();
    let header_len = frame.len();
    frame.resize(
        header_len + usize::try_from(payload_len).expect("u32 fits usize"),
        0,
    );
    if let Err((error, _)) =
        read_exact_with_deadline(stream, &mut frame[header_len..], read_timeout).await
    {
        if error.kind() == io::ErrorKind::TimedOut {
            metrics
                .0
                .frame_read_timeouts
                .fetch_add(1, Ordering::Relaxed);
        }
        return Err(error);
    }
    Ok((frame, permit))
}

/// Decode a peer frame and enforce the identity established by the transport.
pub fn decode_frame(frame: &[u8], authenticated: Option<HostId>) -> io::Result<(HostId, PeerMsg)> {
    let (from, message) = decode_peer(frame)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid peer frame"))?;
    if authenticated.is_some_and(|identity| identity != from) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "peer frame identity does not match transport identity",
        ));
    }
    Ok((from, message))
}

pub fn decode_routed_frame(
    frame: &[u8],
    authenticated: Option<HostId>,
    recipient: HostId,
) -> io::Result<(HostId, PeerMsg)> {
    let (from, to, message) = decode_peer_routed(frame)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid peer frame"))?;
    if authenticated.is_some_and(|identity| identity != from) || to != recipient {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "peer frame route does not match authenticated connection",
        ));
    }
    Ok((from, message))
}

/// Receive verified messages until the byte stream closes or becomes invalid.
pub async fn receive_loop(
    mut stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    decode: DecodePolicy,
    deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    receive_loop_inner(
        &mut stream,
        authenticated,
        decode,
        ReceiveConfig::default(),
        |_| true,
        deliver,
    )
    .await
}

/// Receive verified messages, closing a connection whose next complete frame
/// does not arrive within `read_timeout`.
pub async fn receive_loop_with_timeout(
    mut stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    decode: DecodePolicy,
    read_timeout: Duration,
    deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    receive_loop_inner(
        &mut stream,
        authenticated,
        decode,
        ReceiveConfig::timeout(read_timeout),
        |_| true,
        deliver,
    )
    .await
}

/// Receive verified messages while the transport identity remains authorized.
/// Authorization is checked for every frame so a long-lived connection cannot
/// retain access after its identity is revoked.
pub async fn receive_loop_while_authorized(
    mut stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    decode: DecodePolicy,
    authorized: impl FnMut(HostId) -> bool,
    deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    receive_loop_inner(
        &mut stream,
        authenticated,
        decode,
        ReceiveConfig::default(),
        authorized,
        deliver,
    )
    .await
}

/// The authorized receive loop with a per-frame read deadline.
pub async fn receive_loop_while_authorized_with_timeout(
    mut stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    decode: DecodePolicy,
    read_timeout: Duration,
    authorized: impl FnMut(HostId) -> bool,
    deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    receive_loop_inner(
        &mut stream,
        authenticated,
        decode,
        ReceiveConfig::timeout(read_timeout),
        authorized,
        deliver,
    )
    .await
}

/// Authorized receive loop with both a deadline and a process-wide payload
/// allocation budget. The budget is acquired from the decoded header before
/// the payload buffer is allocated and held through decode and delivery.
pub async fn receive_loop_while_authorized_with_limits(
    stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    decode: DecodePolicy,
    read_timeout: Duration,
    inflight_bytes: std::sync::Arc<Semaphore>,
    authorized: impl FnMut(HostId) -> bool,
    deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    receive_loop_while_authorized_with_limits_and_metrics(
        stream,
        authenticated,
        decode,
        ReceiveLimits {
            read_timeout,
            inflight_bytes,
            metrics: ReceiveMetrics::default(),
        },
        authorized,
        deliver,
    )
    .await
}

pub async fn receive_loop_while_authorized_with_limits_and_metrics(
    mut stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    decode: DecodePolicy,
    limits: ReceiveLimits,
    authorized: impl FnMut(HostId) -> bool,
    deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    receive_loop_inner(
        &mut stream,
        authenticated,
        decode,
        limits.into(),
        authorized,
        deliver,
    )
    .await
}

/// Receive routed frames while the authenticated permanent `HostId` remains a
/// member of the latest object-store roster.
pub async fn receive_routed_loop_with_limits_and_metrics(
    mut stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    recipient: HostId,
    decode: DecodePolicy,
    limits: ReceiveLimits,
    mut authorized: impl FnMut(HostId) -> bool,
    mut deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    loop {
        let (frame, _permit) = read_frame_budgeted_with_metrics(
            &mut stream,
            Some(&limits.inflight_bytes),
            Some(limits.read_timeout),
            &limits.metrics,
        )
        .await?;
        let decoded = match decode {
            DecodePolicy::BlockingAbove(threshold) if frame.len() >= threshold => {
                tokio::task::spawn_blocking(move || {
                    decode_routed_frame(&frame, authenticated, recipient)
                })
                .await
                .map_err(|error| io::Error::other(format!("peer decode task: {error}")))??
            }
            DecodePolicy::Inline | DecodePolicy::BlockingAbove(_) => {
                decode_routed_frame(&frame, authenticated, recipient)?
            }
        };
        if !authorized(decoded.0) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer transport identity is no longer authorized",
            ));
        }
        deliver(decoded.0, decoded.1);
    }
}

async fn receive_loop_inner(
    mut stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    decode: DecodePolicy,
    limits: ReceiveConfig,
    mut authorized: impl FnMut(HostId) -> bool,
    mut deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    loop {
        let (frame, _permit) = read_frame_budgeted_with_metrics(
            &mut stream,
            limits.inflight_bytes.as_ref(),
            limits.read_timeout,
            &limits.metrics,
        )
        .await?;
        let decoded = match decode {
            DecodePolicy::BlockingAbove(threshold) if frame.len() >= threshold => {
                tokio::task::spawn_blocking(move || decode_frame(&frame, authenticated))
                    .await
                    .map_err(|error| io::Error::other(format!("peer decode task: {error}")))??
            }
            DecodePolicy::Inline | DecodePolicy::BlockingAbove(_) => {
                decode_frame(&frame, authenticated)?
            }
        };
        if !authorized(decoded.0) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer transport identity is no longer authorized",
            ));
        }
        deliver(decoded.0, decoded.1);
    }
}

/// Write one already-encoded peer frame to any Tokio-compatible byte stream.
pub async fn write_frame(stream: &mut (impl AsyncWrite + Unpin), frame: &[u8]) -> io::Result<()> {
    stream.write_all(frame).await
}

#[cfg(test)]
mod tests {
    use blockd_core::peer::encode_peer;
    use blockd_core::protocol::PeerMsg;
    use blockd_core::types::{HostId, VolumeId};

    use super::*;

    #[tokio::test]
    async fn framing_round_trips_and_enforces_transport_identity() {
        let message = PeerMsg::Released {
            volume: VolumeId(7),
            release_fence: 11,
        };
        let encoded = encode_peer(HostId::new(3), &message);
        let (mut writer, mut reader) = tokio::io::duplex(encoded.len());

        let write = tokio::spawn(async move { write_frame(&mut writer, &encoded).await });
        let frame = read_frame(&mut reader).await.unwrap();
        write.await.unwrap().unwrap();

        assert_eq!(
            decode_frame(&frame, Some(HostId::new(3))).unwrap(),
            (HostId::new(3), message)
        );
        assert_eq!(
            decode_frame(&frame, Some(HostId::new(4)))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[tokio::test]
    async fn oversized_payload_is_rejected_before_allocation() {
        let mut header = Vec::from(MAGIC_PEER.to_le_bytes());
        header.extend_from_slice(&(MAX_PEER_PAYLOAD + 1).to_le_bytes());
        header.extend_from_slice(&0_u32.to_le_bytes());
        let (mut writer, mut reader) = tokio::io::duplex(header.len());
        write_frame(&mut writer, &header).await.unwrap();
        assert_eq!(
            read_frame(&mut reader).await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[tokio::test]
    async fn payload_budget_applies_backpressure_before_body_allocation() {
        let mut encoded = Vec::from(MAGIC_PEER.to_le_bytes());
        encoded.extend_from_slice(&2_u32.to_le_bytes());
        encoded.extend_from_slice(&0_u32.to_le_bytes());
        encoded.extend_from_slice(&[1, 2]);
        let (mut writer, mut reader) = tokio::io::duplex(encoded.len());
        write_frame(&mut writer, &encoded).await.unwrap();
        let budget = std::sync::Arc::new(Semaphore::new(1));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                read_frame_budgeted(&mut reader, Some(&budget))
            )
            .await
            .is_err(),
            "reader allocated a body before obtaining its byte budget"
        );
    }

    #[tokio::test]
    async fn receive_metrics_classify_budget_waits_idle_and_partial_frame_timeouts() {
        let metrics = ReceiveMetrics::default();
        let budget = Arc::new(Semaphore::new(2));
        let held = Arc::clone(&budget)
            .acquire_many_owned(2)
            .await
            .expect("hold byte budget");
        let mut encoded = Vec::from(MAGIC_PEER.to_le_bytes());
        encoded.extend_from_slice(&2_u32.to_le_bytes());
        encoded.extend_from_slice(&0_u32.to_le_bytes());
        encoded.extend_from_slice(&[1, 2]);
        let (mut writer, mut reader) = tokio::io::duplex(encoded.len());
        write_frame(&mut writer, &encoded).await.unwrap();
        let read = tokio::spawn({
            let budget = Arc::clone(&budget);
            let metrics = metrics.clone();
            async move {
                read_frame_budgeted_with_metrics(
                    &mut reader,
                    Some(&budget),
                    Some(Duration::from_secs(1)),
                    &metrics,
                )
                .await
            }
        });
        tokio::task::yield_now().await;
        assert_eq!(metrics.snapshot().payload_budget_waits, 1);
        drop(held);
        read.await.unwrap().unwrap();

        let (_writer, mut reader) = tokio::io::duplex(16);
        assert_eq!(
            read_frame_budgeted_with_metrics(
                &mut reader,
                None,
                Some(Duration::from_millis(10)),
                &metrics,
            )
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::TimedOut
        );
        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(&[0]).await.unwrap();
        assert_eq!(
            read_frame_budgeted_with_metrics(
                &mut reader,
                None,
                Some(Duration::from_millis(10)),
                &metrics,
            )
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            metrics.snapshot(),
            ReceiveMetricsSnapshot {
                payload_budget_waits: 1,
                frame_read_timeouts: 1,
                idle_timeouts: 1,
            }
        );
    }

    #[tokio::test]
    async fn exact_maximum_payload_is_admitted_before_the_body_deadline() {
        let mut header = Vec::from(MAGIC_PEER.to_le_bytes());
        header.extend_from_slice(&MAX_PEER_PAYLOAD.to_le_bytes());
        header.extend_from_slice(&0_u32.to_le_bytes());
        let (mut writer, mut reader) = tokio::io::duplex(header.len());
        write_frame(&mut writer, &header).await.unwrap();
        let metrics = ReceiveMetrics::default();
        let budget = Arc::new(Semaphore::new(MAX_PEER_PAYLOAD as usize));

        assert_eq!(
            read_frame_budgeted_with_metrics(
                &mut reader,
                Some(&budget),
                Some(Duration::from_millis(10)),
                &metrics,
            )
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::TimedOut,
            "the exact maximum must pass header validation and wait for its body"
        );
        assert_eq!(metrics.snapshot().frame_read_timeouts, 1);
        assert_eq!(
            budget.available_permits(),
            MAX_PEER_PAYLOAD as usize,
            "a timed-out maximum frame must return its byte budget"
        );
    }

    #[tokio::test]
    async fn receive_loop_delivers_multiple_frames_through_the_shared_session() {
        let messages = [
            PeerMsg::Released {
                volume: VolumeId(1),
                release_fence: 2,
            },
            PeerMsg::ReleasedAck {
                volume: VolumeId(1),
                release_fence: 2,
            },
        ];
        let (mut writer, reader) = tokio::io::duplex(1024);
        let encoded = messages
            .iter()
            .map(|message| encode_peer(HostId::new(3), message))
            .collect::<Vec<_>>();
        tokio::spawn(async move {
            for frame in encoded {
                write_frame(&mut writer, &frame).await.unwrap();
            }
        });

        let mut delivered = Vec::new();
        let result = receive_loop(
            reader,
            Some(HostId::new(3)),
            DecodePolicy::Inline,
            |from, message| delivered.push((from, message)),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(
            delivered,
            messages
                .into_iter()
                .map(|message| (HostId::new(3), message))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn receive_loop_closes_when_a_live_identity_is_revoked() {
        let first = encode_peer(
            HostId::new(3),
            &PeerMsg::Released {
                volume: VolumeId(1),
                release_fence: 2,
            },
        );
        let second = encode_peer(
            HostId::new(3),
            &PeerMsg::Released {
                volume: VolumeId(1),
                release_fence: 3,
            },
        );
        let (mut writer, reader) = tokio::io::duplex(first.len() + second.len());
        tokio::spawn(async move {
            write_frame(&mut writer, &first).await.unwrap();
            write_frame(&mut writer, &second).await.unwrap();
        });

        let mut checks = 0;
        let mut delivered = Vec::new();
        let error = receive_loop_while_authorized(
            reader,
            Some(HostId::new(3)),
            DecodePolicy::Inline,
            |_| {
                checks += 1;
                checks == 1
            },
            |_, message| delivered.push(message),
        )
        .await
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(delivered.len(), 1);
    }
}
