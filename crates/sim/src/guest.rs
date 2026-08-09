//! Deterministic guest page contents used by workload actors and oracles.

use blockd_core::types::{PageId, page_size};

/// Contents after a volume's `vol_seq` write. The first word carries the
/// sequence; sparse mixed words bind the bytes to the exact page identity.
pub fn page_pattern(page: PageId, vol_seq: u64) -> Vec<u8> {
    let mut bytes = vec![0_u8; page_size()];
    if vol_seq == 0 {
        return bytes;
    }
    bytes[0..8].copy_from_slice(&vol_seq.to_le_bytes());
    for word in (1..page_size() / 8).step_by(4) {
        let mut mix = 0xcbf2_9ce4_8422_2325_u64;
        for value in [
            page.volume.vset.0,
            u64::from(page.volume.idx.0),
            u64::from(page.page.0),
            vol_seq,
            word as u64,
        ] {
            mix ^= value;
            mix = mix.wrapping_mul(0x0000_0100_0000_01b3);
        }
        bytes[word * 8..word * 8 + 8].copy_from_slice(&mix.to_le_bytes());
    }
    bytes
}

pub fn claimed_vol_seq(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[0..8].try_into().expect("page has 8 bytes"))
}

#[cfg(test)]
mod tests {
    use blockd_core::types::{PageNo, VolumeId, VolumeIdx, VsetId};

    use super::*;

    #[test]
    fn patterns_are_exact_page_bound_sequences() {
        let page = PageId {
            volume: VolumeId {
                vset: VsetId(2),
                idx: VolumeIdx(1),
            },
            page: PageNo(7),
        };
        let bytes = page_pattern(page, 41);
        assert_eq!(bytes.len(), page_size());
        assert_eq!(claimed_vol_seq(&bytes), 41);
        assert_ne!(bytes, page_pattern(PageId { page: PageNo(8), ..page }, 41));
        assert_eq!(page_pattern(page, 0), vec![0; page_size()]);
    }
}
