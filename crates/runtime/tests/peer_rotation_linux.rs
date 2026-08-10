#![cfg(target_os = "linux")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;

use blockd_core::protocol::PeerMsg;
use blockd_core::types::{HostId, VsetId};
use blockd_runtime::{PeerConfig, PeerNet, PeerTlsConfig};

mod support;

#[derive(Clone, Copy)]
enum IdentitySet {
    Old,
    New,
}

fn tls(host: usize, active: IdentitySet, trust_old: bool, trust_new: bool) -> PeerTlsConfig {
    support::rotating_peer_tls(
        host,
        2,
        matches!(active, IdentitySet::New),
        trust_old,
        trust_new,
    )
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
}

fn start(
    host: u16,
    addresses: [SocketAddr; 2],
    tls: PeerTlsConfig,
    versions: BTreeMap<HostId, u16>,
) -> (Arc<PeerNet>, Receiver<(HostId, PeerMsg)>) {
    let (tx, rx) = channel();
    let net = PeerNet::start(
        &PeerConfig {
            listen: addresses[usize::from(host)],
            peers: BTreeMap::from([(HostId(0), addresses[0]), (HostId(1), addresses[1])]),
            outbound_protocol_versions: versions,
            tls: Some(tls),
        },
        HostId(host),
        move |from, msg| {
            let _ = tx.send((from, msg));
        },
    );
    (net, rx)
}

fn delivered(
    stage: &str,
    sender: &PeerNet,
    from: HostId,
    to: HostId,
    receiver: &Receiver<(HostId, PeerMsg)>,
) {
    let msg = PeerMsg::Released {
        vset: VsetId(7),
        release_fence: 3,
    };
    for _ in 0..30 {
        sender.send(from, to, &msg);
        if receiver.recv_timeout(Duration::from_millis(100)) == Ok((from, msg.clone())) {
            return;
        }
    }
    panic!("{stage}: message was not delivered after transport retries");
}

fn settle_rebind() {
    std::thread::sleep(Duration::from_millis(50));
}

#[test]
fn rolling_certificate_rotation_requires_overlap_then_removes_old_identity() {
    let addresses = [free_addr(), free_addr()];
    let (mut a, mut a_rx) = start(
        0,
        addresses,
        tls(0, IdentitySet::Old, true, false),
        BTreeMap::new(),
    );
    let (mut b, mut b_rx) = start(
        1,
        addresses,
        tls(1, IdentitySet::Old, true, false),
        BTreeMap::new(),
    );
    delivered("old A to old B", &a, HostId(0), HostId(1), &b_rx);
    delivered("old B to old A", &b, HostId(1), HostId(0), &a_rx);

    drop(a);
    settle_rebind();
    (a, a_rx) = start(
        0,
        addresses,
        tls(0, IdentitySet::Old, true, true),
        BTreeMap::new(),
    );
    delivered("overlap A to old B", &a, HostId(0), HostId(1), &b_rx);
    delivered("old B to overlap A", &b, HostId(1), HostId(0), &a_rx);

    drop(b);
    settle_rebind();
    (b, b_rx) = start(
        1,
        addresses,
        tls(1, IdentitySet::New, true, true),
        BTreeMap::new(),
    );
    delivered(
        "old A to new B during overlap",
        &a,
        HostId(0),
        HostId(1),
        &b_rx,
    );
    delivered(
        "new B to old A during overlap",
        &b,
        HostId(1),
        HostId(0),
        &a_rx,
    );

    drop(a);
    settle_rebind();
    (a, a_rx) = start(
        0,
        addresses,
        tls(0, IdentitySet::New, true, true),
        BTreeMap::new(),
    );
    delivered("new A to overlap B", &a, HostId(0), HostId(1), &b_rx);
    delivered("overlap B to new A", &b, HostId(1), HostId(0), &a_rx);

    drop(b);
    settle_rebind();
    (b, b_rx) = start(
        1,
        addresses,
        tls(1, IdentitySet::New, false, true),
        BTreeMap::new(),
    );
    delivered("new A to new-only B", &a, HostId(0), HostId(1), &b_rx);
    delivered("new-only B to new A", &b, HostId(1), HostId(0), &a_rx);

    drop(a);
    settle_rebind();
    let (a, a_rx) = start(
        0,
        addresses,
        tls(0, IdentitySet::New, false, true),
        BTreeMap::new(),
    );
    delivered("new-only A to B", &a, HostId(0), HostId(1), &b_rx);
    delivered("new-only B to A", &b, HostId(1), HostId(0), &a_rx);

    drop(a);
    settle_rebind();
    let (old_a, _) = start(
        0,
        addresses,
        tls(0, IdentitySet::Old, false, true),
        BTreeMap::new(),
    );
    // `delivered` fires up to 30 copies and returns on the first arrival;
    // under load the stragglers land late. Only the DISTINCT payload below
    // can prove the old identity authenticated — drain the stale
    // duplicates and reject on that payload alone.
    while b_rx.try_recv().is_ok() {}
    old_a.send(
        HostId(0),
        HostId(1),
        &PeerMsg::Released {
            vset: VsetId(9),
            release_fence: 4,
        },
    );
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while let Some(wait) = deadline.checked_duration_since(std::time::Instant::now()) {
        match b_rx.recv_timeout(wait) {
            Ok((
                _,
                PeerMsg::Released {
                    vset: VsetId(9), ..
                },
            )) => {
                panic!("old leaf identity must be rejected after overlap removal");
            }
            Ok(_) => {} // a straggling duplicate of an earlier legitimate send
            Err(_) => break,
        }
    }
}

#[test]
fn rolling_wire_downgrade_preserves_v1_and_fails_peer_stash_closed() {
    let addresses = [free_addr(), free_addr()];
    let versions = BTreeMap::from([(HostId(1), 1)]);
    let (a, _) = start(
        0,
        addresses,
        tls(0, IdentitySet::Old, true, false),
        versions,
    );
    let (_b, b_rx) = start(
        1,
        addresses,
        tls(1, IdentitySet::Old, true, false),
        BTreeMap::new(),
    );
    delivered("v1 A to B", &a, HostId(0), HostId(1), &b_rx);

    let before = a.dropped_sends.load(std::sync::atomic::Ordering::SeqCst);
    a.send(
        HostId(0),
        HostId(1),
        &PeerMsg::ReplicaStatus {
            vset: VsetId(7),
            assignment_epoch: 1,
        },
    );
    assert_eq!(
        a.dropped_sends.load(std::sync::atomic::Ordering::SeqCst),
        before + 1
    );
    assert!(b_rx.recv_timeout(Duration::from_millis(150)).is_err());
}
