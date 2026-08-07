use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use blockd_core::types::HostId;
use blockd_runtime::PeerTlsConfig;
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

const MAX_TEST_HOSTS: usize = 3;

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
    let _ = rustls::crypto::ring::default_provider().install_default();
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
    let certificate = CertificateDer::from(active.certificate.clone());
    let key = || PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(active.private_key.clone()));
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
        .build()
        .expect("client verifier");
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![certificate.clone()], key())
        .expect("server identity");
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![certificate], key())
        .expect("client identity");
    PeerTlsConfig {
        server: Arc::new(server),
        client: Arc::new(client),
        server_names: (0..host_count)
            .map(|id| {
                (
                    HostId(u16::try_from(id).expect("fits")),
                    format!("host{id}.test"),
                )
            })
            .collect(),
        certificate_identities: identities,
    }
}
