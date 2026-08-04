//! The TCP peer transport on loopback: framing, sender identity, silent
//! drops before the listener exists, reconnection, corruption handling,
//! and a segment-sized payload — the transport half of what the migration
//! e2e then exercises end to end.

#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;

use blockd_core::seam::{IoId, PeerMsg};
use blockd_core::types::{HostId, SegId, VsetId};
use blockd_runtime::{PeerConfig, PeerNet};

/// A bound-then-released ephemeral port: still free momentarily after.
fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
}

fn net(
    listen: SocketAddr,
    peers: BTreeMap<HostId, SocketAddr>,
) -> (Arc<PeerNet>, Receiver<(HostId, PeerMsg)>) {
    let (tx, rx) = channel();
    let config = PeerConfig { listen, peers };
    let host = PeerNet::start(&config, HostId(9), move |from, msg| {
        let _ = tx.send((from, msg));
    });
    (host, rx)
}

fn sample_msgs() -> Vec<PeerMsg> {
    vec![
        PeerMsg::MigrateOffer {
            vset: VsetId(7),
            record: vec![1, 2, 3],
        },
        PeerMsg::MigrateAccept { vset: VsetId(7) },
        PeerMsg::FetchRange {
            io: IoId(1),
            vset: VsetId(7),
            fence: 2,
            seg: SegId(3),
            offset: 4,
            len: 5,
        },
        PeerMsg::Page {
            io: IoId(1),
            bytes: Some(vec![9; 640]),
        },
        PeerMsg::FetchLeaf {
            io: IoId(2),
            vset: VsetId(7),
            base: 0,
            fence: 2,
            id: 1,
        },
        PeerMsg::Leaf {
            io: IoId(2),
            bytes: None,
        },
        PeerMsg::Released { vset: VsetId(7) },
        PeerMsg::ReleasedAck { vset: VsetId(7) },
    ]
}

#[test]
fn every_variant_crosses_the_wire_with_its_sender() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let roster: BTreeMap<HostId, SocketAddr> = [(HostId(0), addr_a), (HostId(1), addr_b)]
        .into_iter()
        .collect();
    let (a, _rx_a) = net(addr_a, roster.clone());
    let (_b, rx_b) = net(addr_b, roster);

    for msg in sample_msgs() {
        a.send(HostId(0), HostId(1), &msg);
        let (from, got) = rx_b
            .recv_timeout(Duration::from_secs(5))
            .expect("delivered");
        assert_eq!((from, got), (HostId(0), msg));
    }
}

/// Sends into the void drop silently; once the listener exists, later
/// sends connect fresh and arrive. (The daemon's retry timers are what
/// re-drive the dropped ones in real use.)
#[test]
fn sends_before_the_listener_drop_and_reconnect_works_after() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let roster: BTreeMap<HostId, SocketAddr> = [(HostId(0), addr_a), (HostId(1), addr_b)]
        .into_iter()
        .collect();
    let (a, _rx_a) = net(addr_a, roster.clone());

    // No listener at addr_b yet: this frame is dropped on the floor.
    a.send(HostId(0), HostId(1), &PeerMsg::Released { vset: VsetId(1) });
    std::thread::sleep(Duration::from_millis(50));

    let (_b, rx_b) = net(addr_b, roster);
    // The sender's dead connection is discovered on the next write; the
    // retry after that reconnects. Send a few — at least one must land.
    for _ in 0..5 {
        a.send(
            HostId(0),
            HostId(1),
            &PeerMsg::ReleasedAck { vset: VsetId(2) },
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let (from, got) = rx_b
        .recv_timeout(Duration::from_secs(5))
        .expect("reconnected and delivered");
    assert_eq!(from, HostId(0));
    assert_eq!(got, PeerMsg::ReleasedAck { vset: VsetId(2) });
}

/// A stream that turns to garbage is closed at the first bad frame and
/// never wedges the receiver: fresh connections keep delivering.
#[test]
fn a_corrupt_frame_closes_its_connection_without_wedging() {
    let addr_b = free_addr();
    let roster: BTreeMap<HostId, SocketAddr> = [(HostId(1), addr_b)].into_iter().collect();
    let (_b, rx_b) = net(addr_b, roster.clone());

    // A raw connection writing garbage: dropped without a delivery.
    let mut raw = TcpStream::connect(addr_b).expect("connect");
    raw.write_all(&[0xFF; 64]).expect("write garbage");
    drop(raw);
    assert!(
        rx_b.recv_timeout(Duration::from_millis(200)).is_err(),
        "garbage must deliver nothing"
    );

    // A healthy transport still gets through.
    let addr_a = free_addr();
    let mut full = roster;
    full.insert(HostId(0), addr_a);
    let (a, _rx_a) = net(addr_a, full);
    a.send(HostId(0), HostId(1), &PeerMsg::Released { vset: VsetId(3) });
    let (_, got) = rx_b
        .recv_timeout(Duration::from_secs(5))
        .expect("delivered");
    assert_eq!(got, PeerMsg::Released { vset: VsetId(3) });
}

/// A segment-sized page payload (8 MiB) crosses intact.
#[test]
fn a_segment_sized_payload_round_trips() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let roster: BTreeMap<HostId, SocketAddr> = [(HostId(0), addr_a), (HostId(1), addr_b)]
        .into_iter()
        .collect();
    let (a, _rx_a) = net(addr_a, roster.clone());
    let (_b, rx_b) = net(addr_b, roster);

    let payload: Vec<u8> = (0..8 * 1024 * 1024u32)
        .map(|i| u8::try_from((i * 31) % 256).expect("fits"))
        .collect();
    a.send(
        HostId(0),
        HostId(1),
        &PeerMsg::Page {
            io: IoId(77),
            bytes: Some(payload.clone()),
        },
    );
    let (_, got) = rx_b
        .recv_timeout(Duration::from_secs(10))
        .expect("delivered");
    assert_eq!(
        got,
        PeerMsg::Page {
            io: IoId(77),
            bytes: Some(payload),
        }
    );
}
