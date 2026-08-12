use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::OnceLock;

use blockd_core::hostmeta::{HostConfig, ReplicaPlacementConfig};
use blockd_core::placement::PeerCandidate;
use blockd_core::types::HostId;
use blockd_core::types::millis;
use blockd_runtime::{PeerConfig, PeerTlsConfig, RuntimeConfig};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;

const MAX_TEST_HOSTS: usize = 3;

#[allow(dead_code)]
pub(crate) fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("address")
}

#[allow(dead_code)]
pub(crate) fn temp_root(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("blockd-runtime-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

#[allow(dead_code)]
pub(crate) fn base_daemon_config(host: u16) -> HostConfig {
    HostConfig {
        archive: blockd_core::hostmeta::ArchivePolicy::default(),
        host: HostId(host),
        cache_pages: 64,
        writeback_interval: millis(5),
        backup_retry: millis(20),
        disk_capacity: None,
        disk_headroom: 0,
        wedge_ticks: 500,
        replica_placement: None,
    }
}

#[allow(dead_code)]
pub(crate) fn three_host_roster(
    addresses: [SocketAddr; MAX_TEST_HOSTS],
) -> BTreeMap<HostId, SocketAddr> {
    addresses
        .into_iter()
        .enumerate()
        .map(|(host, address)| (HostId(u16::try_from(host).expect("fits")), address))
        .collect()
}

#[allow(dead_code)]
pub(crate) fn three_host_runtime_config(
    host: u16,
    blob_dir: PathBuf,
    addresses: [SocketAddr; MAX_TEST_HOSTS],
) -> RuntimeConfig {
    let mut daemon = base_daemon_config(host);
    daemon.replica_placement = Some(ReplicaPlacementConfig {
        membership_epoch: 1,
        local_failure_domain: host + 1,
        roster: (0..MAX_TEST_HOSTS)
            .map(|candidate| {
                let candidate = u16::try_from(candidate).expect("fits");
                PeerCandidate {
                    host: HostId(candidate),
                    weight: 1,
                    failure_domain: candidate + 1,
                    drained: false,
                }
            })
            .collect(),
        authority: None,
    });
    RuntimeConfig {
        daemon,
        blob_dir,
        peer: Some(PeerConfig {
            listen: addresses[usize::from(host)],
            peers: three_host_roster(addresses),
            tls: Some(peer_tls(usize::from(host), MAX_TEST_HOSTS)),
        }),
    }
}

struct Identity {
    certificate: Vec<u8>,
    private_key: Vec<u8>,
}

fn generate_set() -> Vec<Identity> {
    (0..MAX_TEST_HOSTS)
        .map(|host| {
            let CertifiedKey { cert, signing_key } =
                generate_simple_self_signed(vec![format!("host{host}.test")])
                    .expect("generate test TLS identity");
            Identity {
                certificate: cert.der().to_vec(),
                private_key: signing_key.serialize_der(),
            }
        })
        .collect()
}

fn identity_sets() -> &'static [Vec<Identity>; 2] {
    static SETS: OnceLock<[Vec<Identity>; 2]> = OnceLock::new();
    SETS.get_or_init(|| [generate_set(), generate_set()])
}

#[allow(dead_code)] // Rotation-only test binaries use the configurable helper below.
pub(crate) fn peer_tls(host: usize, host_count: usize) -> PeerTlsConfig {
    rotating_peer_tls(host, host_count, false, true, false)
}

pub(crate) fn rotating_peer_tls(
    host: usize,
    host_count: usize,
    active_new: bool,
    trust_old: bool,
    trust_new: bool,
) -> PeerTlsConfig {
    assert!(host < host_count && host_count <= MAX_TEST_HOSTS);
    let sets = identity_sets();
    let mut roots = RootCertStore::empty();
    let mut identities = BTreeMap::new();
    for (set, trusted) in [(&sets[0], trust_old), (&sets[1], trust_new)] {
        if !trusted {
            continue;
        }
        for (id, identity) in set.iter().take(host_count).enumerate() {
            roots
                .add(CertificateDer::from(identity.certificate.clone()))
                .expect("test trust anchor");
            identities.insert(
                identity.certificate.clone(),
                HostId(u16::try_from(id).expect("fits")),
            );
        }
    }
    let active = &sets[usize::from(active_new)][host];
    PeerTlsConfig::from_der(
        roots,
        active.certificate.clone(),
        &active.private_key,
        (0..host_count)
            .map(|id| {
                (
                    HostId(u16::try_from(id).expect("fits")),
                    format!("host{id}.test"),
                )
            })
            .collect(),
        identities,
    )
}
