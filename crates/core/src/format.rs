//! Wire/storage primitives (R10.2): everything fixed-width, little-endian,
//! float-free. Every durable or transferred unit is a checksummed frame; the
//! daemon verifies every frame it reads, wherever it came from — disk, peer,
//! or object store (R8.1). Byte layouts are pinned by tests; two encoders of
//! the same state produce identical bytes.

/// CRC-32C (Castagnoli), reflected, as used by iSCSI/ext4/S3 checksums.
// The truncating casts are the algorithm's byte indexing.
#[allow(clippy::cast_possible_truncation)]
pub fn crc32c(bytes: &[u8]) -> u32 {
    const fn table() -> [u32; 256] {
        let poly: u32 = 0x82F6_3B78;
        let mut table = [0u32; 256];
        let mut n = 0;
        while n < 256 {
            let mut crc = n as u32;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 == 1 {
                    (crc >> 1) ^ poly
                } else {
                    crc >> 1
                };
                bit += 1;
            }
            table[n] = crc;
            n += 1;
        }
        table
    }
    const TABLE: [u32; 256] = table();
    let mut crc = !0u32;
    for &b in bytes {
        crc = (crc >> 8) ^ TABLE[usize::from((crc as u8) ^ b)];
    }
    !crc
}

/// Append-only little-endian encoder. All widths explicit at call sites.
#[derive(Default)]
pub struct Enc {
    buf: Vec<u8>,
}

impl Enc {
    pub fn new() -> Enc {
        Enc { buf: Vec::new() }
    }

    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn bytes(&mut self, v: &[u8]) {
        self.buf.extend_from_slice(v);
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// Strict little-endian decoder. Every method fails loudly on truncation;
/// callers translate `DecodeError` into "corrupt unit" handling (R8.1).
pub struct Dec<'a> {
    rest: &'a [u8],
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct DecodeError;

impl<'a> Dec<'a> {
    pub fn new(bytes: &'a [u8]) -> Dec<'a> {
        Dec { rest: bytes }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.rest.len() < n {
            return Err(DecodeError);
        }
        let (head, tail) = self.rest.split_at(n);
        self.rest = tail;
        Ok(head)
    }

    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("len 2")))
    }

    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("len 4")))
    }

    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("len 8")))
    }

    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        self.take(n)
    }

    pub fn remaining(&self) -> usize {
        self.rest.len()
    }

    /// Decoding must consume exactly the input.
    pub fn finish(self) -> Result<(), DecodeError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(DecodeError)
        }
    }
}

/// A checksummed frame: `magic u32 | payload_len u32 | crc32c u32 | payload`.
/// The unit of verification everywhere (R8.1).
pub const FRAME_HEADER: usize = 12;

pub fn seal_frame(magic: u32, payload: &[u8]) -> Vec<u8> {
    let mut e = Enc::new();
    e.u32(magic);
    e.u32(u32::try_from(payload.len()).expect("frame payload < 4 GiB"));
    e.u32(crc32c(payload));
    e.bytes(payload);
    e.finish()
}

/// Verify and open a frame, returning the payload. Any mismatch — magic,
/// length, checksum, trailing garbage — is one answer: corrupt.
pub fn open_frame(magic: u32, bytes: &[u8]) -> Result<&[u8], DecodeError> {
    let mut d = Dec::new(bytes);
    if d.u32()? != magic {
        return Err(DecodeError);
    }
    let len = d.u32()?;
    let crc = d.u32()?;
    let payload = d.bytes(usize::try_from(len).expect("u32 fits usize"))?;
    d.finish()?;
    if crc32c(payload) != crc {
        return Err(DecodeError);
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32c_matches_the_standard_check_value() {
        // The canonical CRC-32C test vector.
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn frames_round_trip() {
        let framed = seal_frame(0x1234_5678, b"hello blockd");
        assert_eq!(open_frame(0x1234_5678, &framed), Ok(&b"hello blockd"[..]));
    }

    #[test]
    fn any_single_bit_flip_breaks_a_frame() {
        let framed = seal_frame(0xABCD_EF01, b"payload under test");
        for bit in 0..framed.len() * 8 {
            let mut damaged = framed.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(
                open_frame(0xABCD_EF01, &damaged).is_err(),
                "flip of bit {bit} went undetected"
            );
        }
    }

    #[test]
    fn truncated_frames_are_rejected() {
        let framed = seal_frame(0x0F0F_0F0F, b"soon torn");
        for keep in 0..framed.len() {
            assert!(
                open_frame(0x0F0F_0F0F, &framed[..keep]).is_err(),
                "truncation to {keep} bytes went undetected"
            );
        }
    }

    #[test]
    fn enc_dec_round_trip_and_strictness() {
        let mut e = Enc::new();
        e.u8(0x11);
        e.u16(0x2233);
        e.u32(0x4455_6677);
        e.u64(0x8899_aabb_ccdd_eeff);
        e.bytes(b"xy");
        let bytes = e.finish();
        assert_eq!(
            bytes,
            [
                0x11, 0x33, 0x22, 0x77, 0x66, 0x55, 0x44, 0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99,
                0x88, b'x', b'y'
            ]
        );
        let mut d = Dec::new(&bytes);
        assert_eq!(d.u8(), Ok(0x11));
        assert_eq!(d.u16(), Ok(0x2233));
        assert_eq!(d.u32(), Ok(0x4455_6677));
        assert_eq!(d.u64(), Ok(0x8899_aabb_ccdd_eeff));
        assert_eq!(d.bytes(2), Ok(&b"xy"[..]));
        assert!(d.finish().is_ok());

        let mut short = Dec::new(&[0x01]);
        assert!(short.u16().is_err());
        let leftover = Dec::new(&[0x01, 0x02]);
        assert!(leftover.finish().is_err());
    }
}
