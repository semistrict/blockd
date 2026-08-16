#![forbid(unsafe_code)]

//! Peer wire framing shared by real Tokio sockets and Turmoil sockets.

use std::io;

use blockd_core::format::{Dec, FRAME_HEADER};
use blockd_core::peer::{MAGIC_PEER, MAX_PEER_PAYLOAD, decode_peer};
use blockd_core::protocol::PeerMsg;
use blockd_core::types::HostId;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodePolicy {
    /// Decode on the current deterministic host runtime.
    Inline,
    /// Move frames at or above this size to Tokio's blocking pool.
    BlockingAbove(usize),
}

/// Read one bounded peer frame from any Tokio-compatible byte stream.
pub async fn read_frame(stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut header = [0u8; FRAME_HEADER];
    stream.read_exact(&mut header).await?;
    let mut decoder = Dec::new(&header);
    let magic = decoder.u32().expect("fixed peer header");
    let payload_len = decoder.u32().expect("fixed peer header");
    if magic != MAGIC_PEER || payload_len > MAX_PEER_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid peer frame header",
        ));
    }

    let mut frame = header.to_vec();
    let header_len = frame.len();
    frame.resize(
        header_len + usize::try_from(payload_len).expect("u32 fits usize"),
        0,
    );
    stream.read_exact(&mut frame[header_len..]).await?;
    Ok(frame)
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

/// Receive verified messages until the byte stream closes or becomes invalid.
pub async fn receive_loop(
    mut stream: impl AsyncRead + Unpin,
    authenticated: Option<HostId>,
    decode: DecodePolicy,
    mut deliver: impl FnMut(HostId, PeerMsg),
) -> io::Result<()> {
    loop {
        let frame = read_frame(&mut stream).await?;
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
    use blockd_core::types::{HostId, VsetId};

    use super::*;

    #[tokio::test]
    async fn framing_round_trips_and_enforces_transport_identity() {
        let message = PeerMsg::Released {
            vset: VsetId(7),
            release_fence: 11,
        };
        let encoded = encode_peer(HostId(3), &message);
        let (mut writer, mut reader) = tokio::io::duplex(encoded.len());

        let write = tokio::spawn(async move { write_frame(&mut writer, &encoded).await });
        let frame = read_frame(&mut reader).await.unwrap();
        write.await.unwrap().unwrap();

        assert_eq!(
            decode_frame(&frame, Some(HostId(3))).unwrap(),
            (HostId(3), message)
        );
        assert_eq!(
            decode_frame(&frame, Some(HostId(4))).unwrap_err().kind(),
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
    async fn receive_loop_delivers_multiple_frames_through_the_shared_session() {
        let messages = [
            PeerMsg::Released {
                vset: VsetId(1),
                release_fence: 2,
            },
            PeerMsg::ReleasedAck {
                vset: VsetId(1),
                release_fence: 2,
            },
        ];
        let (mut writer, reader) = tokio::io::duplex(1024);
        let encoded = messages
            .iter()
            .map(|message| encode_peer(HostId(3), message))
            .collect::<Vec<_>>();
        tokio::spawn(async move {
            for frame in encoded {
                write_frame(&mut writer, &frame).await.unwrap();
            }
        });

        let mut delivered = Vec::new();
        let result = receive_loop(
            reader,
            Some(HostId(3)),
            DecodePolicy::Inline,
            |from, message| delivered.push((from, message)),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(
            delivered,
            messages
                .into_iter()
                .map(|message| (HostId(3), message))
                .collect::<Vec<_>>()
        );
    }
}
