//! Bounded, checksummed guest protocol for durable `SQLite` file operations.
//!
//! The trusted VM identity is supplied to [`decode_request`] by the host
//! listener; it is intentionally absent from guest-controlled bytes.

use crate::database::{
    AttachmentId, DatabaseError, DatabaseFile, DatabaseOp, DatabaseReply, DatabaseRequest,
    MAX_DATABASE_IO,
};
use crate::format::{Dec, DecodeError, Enc, FRAME_HEADER, open_frame, seal_frame};
use crate::protocol::ReqId;
use crate::types::{VmId, VsetId};

pub const MAGIC_DATABASE_REQUEST: u32 = u32::from_le_bytes(*b"BDQ1");
pub const MAGIC_DATABASE_REPLY: u32 = u32::from_le_bytes(*b"BDP1");
pub const MAX_DATABASE_FRAME: usize = FRAME_HEADER + MAX_DATABASE_IO + 64;

fn encode_file(e: &mut Enc, file: DatabaseFile) {
    e.u8(match file {
        DatabaseFile::Main => 0,
        DatabaseFile::Wal => 1,
        DatabaseFile::Journal => 2,
    });
}

fn decode_file(d: &mut Dec<'_>) -> Result<DatabaseFile, DecodeError> {
    match d.u8()? {
        0 => Ok(DatabaseFile::Main),
        1 => Ok(DatabaseFile::Wal),
        2 => Ok(DatabaseFile::Journal),
        _ => Err(DecodeError),
    }
}

pub fn encode_request(request: &DatabaseRequest) -> Vec<u8> {
    let mut e = Enc::new();
    e.u16(1);
    e.u64(request.req.0);
    e.u64(request.vset.0);
    e.u64(request.attachment.generation);
    match &request.op {
        DatabaseOp::Open {
            handle,
            file,
            create,
        } => {
            e.u8(0);
            e.u64(*handle);
            encode_file(&mut e, *file);
            e.u8(u8::from(*create));
        }
        DatabaseOp::Close { handle } => {
            e.u8(1);
            e.u64(*handle);
        }
        DatabaseOp::Read {
            handle,
            offset,
            len,
        } => {
            e.u8(2);
            e.u64(*handle);
            e.u64(*offset);
            e.u32(*len);
        }
        DatabaseOp::Write {
            handle,
            offset,
            bytes,
        } => {
            assert!(
                bytes.len() <= MAX_DATABASE_IO,
                "database write exceeds frame cap"
            );
            e.u8(3);
            e.u64(*handle);
            e.u64(*offset);
            e.u32(u32::try_from(bytes.len()).expect("bounded"));
            e.bytes(bytes);
        }
        DatabaseOp::Truncate { handle, size } => {
            e.u8(4);
            e.u64(*handle);
            e.u64(*size);
        }
        DatabaseOp::FileSize { handle } => {
            e.u8(5);
            e.u64(*handle);
        }
        DatabaseOp::Access { file } => {
            e.u8(6);
            encode_file(&mut e, *file);
        }
        DatabaseOp::Delete { file } => {
            e.u8(7);
            encode_file(&mut e, *file);
        }
        DatabaseOp::Sync { handle } => {
            e.u8(8);
            e.u64(*handle);
        }
        DatabaseOp::Stat { file } => {
            e.u8(9);
            encode_file(&mut e, *file);
        }
    }
    seal_frame(MAGIC_DATABASE_REQUEST, &e.finish())
}

pub fn decode_request(vm: VmId, bytes: &[u8]) -> Result<DatabaseRequest, DecodeError> {
    if bytes.len() > MAX_DATABASE_FRAME {
        return Err(DecodeError);
    }
    let payload = open_frame(MAGIC_DATABASE_REQUEST, bytes)?;
    let mut d = Dec::new(payload);
    if d.u16()? != 1 {
        return Err(DecodeError);
    }
    let req = ReqId(d.u64()?);
    let vset = VsetId(d.u64()?);
    let attachment = AttachmentId {
        vm,
        generation: d.u64()?,
    };
    let op = match d.u8()? {
        0 => {
            let handle = d.u64()?;
            let file = decode_file(&mut d)?;
            let create = match d.u8()? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError),
            };
            DatabaseOp::Open {
                handle,
                file,
                create,
            }
        }
        1 => DatabaseOp::Close { handle: d.u64()? },
        2 => {
            let handle = d.u64()?;
            let offset = d.u64()?;
            let len = d.u32()?;
            if usize::try_from(len).expect("u32 fits") > MAX_DATABASE_IO {
                return Err(DecodeError);
            }
            DatabaseOp::Read {
                handle,
                offset,
                len,
            }
        }
        3 => {
            let handle = d.u64()?;
            let offset = d.u64()?;
            let len = usize::try_from(d.u32()?).expect("u32 fits");
            if len > MAX_DATABASE_IO {
                return Err(DecodeError);
            }
            DatabaseOp::Write {
                handle,
                offset,
                bytes: d.bytes(len)?.to_vec(),
            }
        }
        4 => DatabaseOp::Truncate {
            handle: d.u64()?,
            size: d.u64()?,
        },
        5 => DatabaseOp::FileSize { handle: d.u64()? },
        6 => DatabaseOp::Access {
            file: decode_file(&mut d)?,
        },
        7 => DatabaseOp::Delete {
            file: decode_file(&mut d)?,
        },
        8 => DatabaseOp::Sync { handle: d.u64()? },
        9 => DatabaseOp::Stat {
            file: decode_file(&mut d)?,
        },
        _ => return Err(DecodeError),
    };
    d.finish()?;
    Ok(DatabaseRequest {
        req,
        vset,
        attachment,
        op,
    })
}

fn encode_error(error: DatabaseError) -> u8 {
    match error {
        DatabaseError::NotAttached => 0,
        DatabaseError::StaleAttachment => 1,
        DatabaseError::Draining => 2,
        DatabaseError::InvalidHandle => 3,
        DatabaseError::AlreadyOpen => 4,
        DatabaseError::NotFound => 5,
        DatabaseError::InvalidRequest => 6,
        DatabaseError::TooLarge => 7,
        DatabaseError::Busy => 8,
        DatabaseError::Io => 9,
    }
}

fn decode_error(value: u8) -> Result<DatabaseError, DecodeError> {
    match value {
        0 => Ok(DatabaseError::NotAttached),
        1 => Ok(DatabaseError::StaleAttachment),
        2 => Ok(DatabaseError::Draining),
        3 => Ok(DatabaseError::InvalidHandle),
        4 => Ok(DatabaseError::AlreadyOpen),
        5 => Ok(DatabaseError::NotFound),
        6 => Ok(DatabaseError::InvalidRequest),
        7 => Ok(DatabaseError::TooLarge),
        8 => Ok(DatabaseError::Busy),
        9 => Ok(DatabaseError::Io),
        _ => Err(DecodeError),
    }
}

pub fn encode_reply(reply: &DatabaseReply) -> Vec<u8> {
    let mut e = Enc::new();
    e.u16(1);
    e.u64(reply.req().0);
    match reply {
        DatabaseReply::Opened { .. } => e.u8(0),
        DatabaseReply::Closed { .. } => e.u8(1),
        DatabaseReply::Read { bytes, eof, .. } => {
            assert!(
                bytes.len() <= MAX_DATABASE_IO,
                "database read exceeds frame cap"
            );
            e.u8(2);
            e.u8(u8::from(*eof));
            e.u32(u32::try_from(bytes.len()).expect("bounded"));
            e.bytes(bytes);
        }
        DatabaseReply::Written { sequence, .. } => {
            e.u8(3);
            e.u64(*sequence);
        }
        DatabaseReply::Truncated { sequence, .. } => {
            e.u8(4);
            e.u64(*sequence);
        }
        DatabaseReply::FileSize { size, .. } => {
            e.u8(5);
            e.u64(*size);
        }
        DatabaseReply::Access { exists, .. } => {
            e.u8(6);
            e.u8(u8::from(*exists));
        }
        DatabaseReply::Deleted { sequence, .. } => {
            e.u8(7);
            e.u64(*sequence);
        }
        DatabaseReply::Synced { sequence, .. } => {
            e.u8(8);
            e.u64(*sequence);
        }
        DatabaseReply::Failed { error, .. } => {
            e.u8(9);
            e.u8(encode_error(*error));
        }
        DatabaseReply::Stat { exists, size, .. } => {
            e.u8(10);
            e.u8(u8::from(*exists));
            e.u64(*size);
        }
    }
    seal_frame(MAGIC_DATABASE_REPLY, &e.finish())
}

pub fn decode_reply(bytes: &[u8]) -> Result<DatabaseReply, DecodeError> {
    if bytes.len() > MAX_DATABASE_FRAME {
        return Err(DecodeError);
    }
    let payload = open_frame(MAGIC_DATABASE_REPLY, bytes)?;
    let mut d = Dec::new(payload);
    if d.u16()? != 1 {
        return Err(DecodeError);
    }
    let req = ReqId(d.u64()?);
    let reply = match d.u8()? {
        0 => DatabaseReply::Opened { req },
        1 => DatabaseReply::Closed { req },
        2 => {
            let eof = match d.u8()? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError),
            };
            let len = usize::try_from(d.u32()?).expect("u32 fits");
            if len > MAX_DATABASE_IO {
                return Err(DecodeError);
            }
            DatabaseReply::Read {
                req,
                bytes: d.bytes(len)?.to_vec(),
                eof,
            }
        }
        3 => DatabaseReply::Written {
            req,
            sequence: d.u64()?,
        },
        4 => DatabaseReply::Truncated {
            req,
            sequence: d.u64()?,
        },
        5 => DatabaseReply::FileSize {
            req,
            size: d.u64()?,
        },
        6 => DatabaseReply::Access {
            req,
            exists: match d.u8()? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError),
            },
        },
        7 => DatabaseReply::Deleted {
            req,
            sequence: d.u64()?,
        },
        8 => DatabaseReply::Synced {
            req,
            sequence: d.u64()?,
        },
        9 => DatabaseReply::Failed {
            req,
            error: decode_error(d.u8()?)?,
        },
        10 => DatabaseReply::Stat {
            req,
            exists: match d.u8()? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError),
            },
            size: d.u64()?,
        },
        _ => return Err(DecodeError),
    };
    d.finish()?;
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::crc32c;

    fn attachment() -> AttachmentId {
        AttachmentId {
            vm: VmId(44),
            generation: 9,
        }
    }

    fn requests() -> Vec<DatabaseRequest> {
        let ops = vec![
            DatabaseOp::Open {
                handle: 1,
                file: DatabaseFile::Main,
                create: true,
            },
            DatabaseOp::Close { handle: 1 },
            DatabaseOp::Read {
                handle: 1,
                offset: 17,
                len: 31,
            },
            DatabaseOp::Write {
                handle: 1,
                offset: 23,
                bytes: vec![0xA5; 37],
            },
            DatabaseOp::Truncate {
                handle: 1,
                size: 4097,
            },
            DatabaseOp::FileSize { handle: 1 },
            DatabaseOp::Access {
                file: DatabaseFile::Wal,
            },
            DatabaseOp::Delete {
                file: DatabaseFile::Journal,
            },
            DatabaseOp::Sync { handle: 1 },
            DatabaseOp::Stat {
                file: DatabaseFile::Main,
            },
        ];
        ops.into_iter()
            .enumerate()
            .map(|(i, op)| DatabaseRequest {
                req: ReqId(i as u64),
                vset: VsetId(7),
                attachment: attachment(),
                op,
            })
            .collect()
    }

    fn replies() -> Vec<DatabaseReply> {
        vec![
            DatabaseReply::Opened { req: ReqId(0) },
            DatabaseReply::Closed { req: ReqId(1) },
            DatabaseReply::Read {
                req: ReqId(2),
                bytes: vec![0x5A; 29],
                eof: true,
            },
            DatabaseReply::Written {
                req: ReqId(3),
                sequence: 11,
            },
            DatabaseReply::Truncated {
                req: ReqId(4),
                sequence: 12,
            },
            DatabaseReply::FileSize {
                req: ReqId(5),
                size: 99,
            },
            DatabaseReply::Access {
                req: ReqId(6),
                exists: true,
            },
            DatabaseReply::Deleted {
                req: ReqId(7),
                sequence: 13,
            },
            DatabaseReply::Synced {
                req: ReqId(8),
                sequence: 13,
            },
            DatabaseReply::Failed {
                req: ReqId(9),
                error: DatabaseError::StaleAttachment,
            },
            DatabaseReply::Stat {
                req: ReqId(10),
                exists: true,
                size: 1234,
            },
        ]
    }

    #[test]
    fn every_variant_round_trips_and_vm_identity_is_out_of_band() {
        for request in requests() {
            let bytes = encode_request(&request);
            assert_eq!(decode_request(VmId(44), &bytes), Ok(request.clone()));
            let mut other = request;
            other.attachment.vm = VmId(45);
            assert_eq!(decode_request(VmId(45), &bytes), Ok(other));
        }
        for reply in replies() {
            assert_eq!(decode_reply(&encode_reply(&reply)), Ok(reply));
        }
    }

    #[test]
    fn layouts_are_byte_pinned() {
        let request_bytes: Vec<u8> = requests().iter().flat_map(encode_request).collect();
        let reply_bytes: Vec<u8> = replies().iter().flat_map(encode_reply).collect();
        assert_eq!(request_bytes.len(), 520);
        assert_eq!(crc32c(&request_bytes), 0x0523_3CD2);
        assert_eq!(reply_bytes.len(), 338);
        assert_eq!(crc32c(&reply_bytes), 0x998A_D168);
    }

    #[test]
    fn damage_truncation_and_excess_lengths_are_rejected() {
        let frame = encode_request(&requests()[3]);
        for bit in 0..frame.len() * 8 {
            let mut damaged = frame.clone();
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert!(decode_request(VmId(44), &damaged).is_err());
        }
        for keep in 0..frame.len() {
            assert!(decode_request(VmId(44), &frame[..keep]).is_err());
        }
        assert!(decode_request(VmId(44), &vec![0; MAX_DATABASE_FRAME + 1]).is_err());
    }
}
