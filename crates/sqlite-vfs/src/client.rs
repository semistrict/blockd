use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use blockd_core::database::{AttachmentId, DatabaseOp, DatabaseReply, DatabaseRequest};
use blockd_core::dbproto::{MAX_DATABASE_FRAME, decode_reply, encode_request};
use blockd_core::format::FRAME_HEADER;
use blockd_core::protocol::ReqId;
use blockd_core::types::VsetId;

static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
pub(crate) enum Endpoint<'a> {
    Unix(&'a Path),
    #[cfg(target_os = "linux")]
    Vsock {
        cid: u32,
        port: u32,
    },
}

enum Stream {
    Unix(UnixStream),
    #[cfg(target_os = "linux")]
    Vsock(std::fs::File),
}

impl Read for Stream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Unix(stream) => stream.read(bytes),
            #[cfg(target_os = "linux")]
            Stream::Vsock(stream) => stream.read(bytes),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Unix(stream) => stream.write(bytes),
            #[cfg(target_os = "linux")]
            Stream::Vsock(stream) => stream.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Unix(stream) => stream.flush(),
            #[cfg(target_os = "linux")]
            Stream::Vsock(stream) => stream.flush(),
        }
    }
}

#[cfg(target_os = "linux")]
fn connect_vsock(cid: u32, port: u32) -> io::Result<std::fs::File> {
    use std::os::fd::FromRawFd;

    // SAFETY: the returned descriptor is either closed on every error path or
    // transferred exactly once to `File`; `sockaddr_vm` has the Linux ABI.
    unsafe {
        let fd = libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let address = libc::sockaddr_vm {
            svm_family: libc::sa_family_t::try_from(libc::AF_VSOCK).expect("family fits"),
            svm_reserved1: 0,
            svm_port: port,
            svm_cid: cid,
            svm_zero: [0; 4],
        };
        let result = libc::connect(
            fd,
            (&raw const address).cast::<libc::sockaddr>(),
            libc::socklen_t::try_from(std::mem::size_of_val(&address)).expect("size fits"),
        );
        if result != 0 {
            let error = io::Error::last_os_error();
            libc::close(fd);
            return Err(error);
        }
        Ok(std::fs::File::from_raw_fd(fd))
    }
}

pub(crate) struct Client {
    stream: Mutex<Stream>,
    vset: VsetId,
    attachment: AttachmentId,
    pub(crate) handle: u64,
}

impl Client {
    pub(crate) fn connect(
        endpoint: Endpoint<'_>,
        vset: VsetId,
        attachment: AttachmentId,
    ) -> io::Result<Client> {
        let stream = match endpoint {
            Endpoint::Unix(path) => Stream::Unix(UnixStream::connect(path)?),
            #[cfg(target_os = "linux")]
            Endpoint::Vsock { cid, port } => Stream::Vsock(connect_vsock(cid, port)?),
        };
        Ok(Client {
            stream: Mutex::new(stream),
            vset,
            attachment,
            handle: NEXT_HANDLE.fetch_add(1, Ordering::Relaxed),
        })
    }

    pub(crate) fn call(&self, op: DatabaseOp) -> io::Result<DatabaseReply> {
        let req = ReqId(NEXT_REQUEST.fetch_add(1, Ordering::Relaxed));
        let request = DatabaseRequest {
            req,
            vset: self.vset,
            attachment: self.attachment,
            op,
        };
        let frame = encode_request(&request);
        let mut stream = self.stream.lock().expect("database client lock");
        stream.write_all(&frame)?;
        let mut header = [0u8; FRAME_HEADER];
        stream.read_exact(&mut header)?;
        let payload = usize::try_from(u32::from_le_bytes(
            header[4..8].try_into().expect("four-byte length"),
        ))
        .expect("u32 fits");
        let len = FRAME_HEADER
            .checked_add(payload)
            .filter(|len| *len <= MAX_DATABASE_FRAME)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "reply frame too large"))?;
        let mut frame = vec![0u8; len];
        frame[..FRAME_HEADER].copy_from_slice(&header);
        stream.read_exact(&mut frame[FRAME_HEADER..])?;
        let reply = decode_reply(&frame)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid reply frame"))?;
        if reply.req() != req {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mismatched database reply id",
            ));
        }
        Ok(reply)
    }
}
