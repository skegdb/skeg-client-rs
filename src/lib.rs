#![deny(unsafe_code)]

//! `skeg-client` - async TCP client for the skeg binary protocol.

use bytes::{Bytes, BytesMut};
use skeg_proto::{
    ErrCode, Flags, FrameParser, Op, ParseError, ServerStats, ShardStats, VindexInfo,
    bytes_to_f32_vec, decode_bool_response, decode_mget_response, decode_shards_response,
    decode_stats_response, decode_value_response, decode_vindex_list_response,
    decode_vsearch_response, encode_del, encode_get, encode_mget, encode_ping, encode_set,
    encode_shards, encode_stats, encode_vdel, encode_vget, encode_vindex_create,
    encode_vindex_drop, encode_vindex_list, encode_vsearch, encode_vset,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

/// Error returned by client operations.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] ParseError),

    #[error("server error ({code:?}): {msg}")]
    Server { code: ErrCode, msg: String },

    #[error("unexpected response op")]
    UnexpectedOp,

    #[error("connection closed by server")]
    ConnectionClosed,

    #[error("request/response id mismatch: sent {sent}, got {got}")]
    ReqIdMismatch { sent: u64, got: u64 },
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// Quantization kind for a vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorKind {
    /// Full-precision f32; the flat scan runs exact cosine.
    F32,
    /// Symmetric 8-bit integer quantization.
    Int8,
    /// 1-bit sign quantization.
    Binary,
}

impl VectorKind {
    fn wire(self) -> u8 {
        match self {
            VectorKind::F32 => 0,
            VectorKind::Int8 => 1,
            VectorKind::Binary => 2,
        }
    }
}

/// Storage backend for a vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorBackend {
    /// In-RAM flat index (M7): full f32 in RAM, exhaustive scan.
    Flat,
    /// On-disk Vamana graph (M8): f32 vectors on disk, graph + int8 tier in
    /// RAM. RAM-frugal, the right choice past a few thousand vectors.
    DiskVamana,
}

impl VectorBackend {
    fn wire(self) -> u8 {
        match self {
            VectorBackend::Flat => 0,
            VectorBackend::DiskVamana => 1,
        }
    }
}

/// Single-connection async client. Not `Sync` - one client per task.
pub struct SkegClient {
    stream: TcpStream,
    parser: FrameParser,
    read_buf: BytesMut,
    next_req_id: u64,
}

impl SkegClient {
    /// Connect to a skeg server at `addr`.
    ///
    /// # Errors
    ///
    /// Returns an IO error if the TCP connection cannot be established.
    pub async fn connect(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        tune_socket(&stream);
        Ok(Self {
            stream,
            parser: FrameParser::new(),
            read_buf: BytesMut::with_capacity(64 * 1024),
            next_req_id: 1,
        })
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_req_id;
        self.next_req_id += 1;
        id
    }

    async fn send(&mut self, frame: Bytes) -> Result<()> {
        self.stream.write_all(&frame).await?;
        Ok(())
    }

    /// Read the next frame and validate it carries the expected
    /// `req_id`. A mismatch surfaces as `ReqIdMismatch` so a server
    /// out-of-order reply or a desynchronised connection fails loud
    /// instead of silently returning the wrong call's result.
    async fn recv_frame(&mut self, expected_id: u64) -> Result<skeg_proto::Frame> {
        loop {
            if let Some(frame) = self.parser.feed(&mut self.read_buf)? {
                if frame.header.req_id != expected_id {
                    return Err(ClientError::ReqIdMismatch {
                        sent: expected_id,
                        got: frame.header.req_id,
                    });
                }
                return Ok(frame);
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(ClientError::ConnectionClosed);
            }
        }
    }

    fn server_err(payload: &Bytes) -> ClientError {
        if payload.len() < 2 {
            return ClientError::UnexpectedOp;
        }
        let code_byte = payload[0];
        let msg_len = payload[1] as usize;
        let msg_end = 2 + msg_len.min(payload.len().saturating_sub(2));
        let msg = String::from_utf8_lossy(&payload[2..msg_end]).into_owned();
        let code = match code_byte {
            0x01 => ErrCode::NotFound,
            0x02 => ErrCode::InvalidRequest,
            _ => ErrCode::Internal,
        };
        ClientError::Server { code, msg }
    }

    /// Send a PING and wait for the server's PONG.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn ping(&mut self) -> Result<()> {
        let id = self.next_id();
        self.send(encode_ping(id)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(()),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Query aggregate server statistics (cache bytes, evictions, key count).
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn stats(&mut self) -> Result<ServerStats> {
        let id = self.next_id();
        self.send(encode_stats(id)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => decode_stats_response(&frame.payload).ok_or(ClientError::UnexpectedOp),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Per-shard stats breakdown. The TUI (`skeg-top`) and any ops
    /// dashboard that wants to render hot-shard skew calls this; the
    /// aggregate `stats()` is the sum of the rows returned here.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn shards(&mut self) -> Result<Vec<ShardStats>> {
        let id = self.next_id();
        self.send(encode_shards(id)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(decode_shards_response(&frame.payload)),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// List every VINDEX. Returns name, dim, tier-1 kind byte, backend
    /// byte, live vector count per index. The TUI uses this to render
    /// the VINDEX view; any management tool can use it for discovery.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn vindex_list(&mut self) -> Result<Vec<VindexInfo>> {
        let id = self.next_id();
        self.send(encode_vindex_list(id)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(decode_vindex_list_response(&frame.payload)),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// GET a key. Returns `None` if the key does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or non-`NotFound` server error.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
        let id = self.next_id();
        self.send(encode_get(id, key)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(decode_value_response(&frame.payload)),
            Op::Err => {
                // NotFound is a valid "miss" - return None
                if frame.payload.first().copied() == Some(ErrCode::NotFound as u8) {
                    Ok(None)
                } else {
                    Err(Self::server_err(&frame.payload))
                }
            }
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// SET a key-value pair.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let id = self.next_id();
        self.send(encode_set(id, key, value, Flags::empty()))
            .await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(()),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// SET with `NO_REPLY` flag - fire-and-forget, returns immediately.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure.
    pub async fn set_no_reply(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let id = self.next_id();
        self.send(encode_set(id, key, value, Flags::NO_REPLY))
            .await?;
        Ok(())
    }

    /// DEL a key. Returns `true` if the key existed.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn del(&mut self, key: &[u8]) -> Result<bool> {
        let id = self.next_id();
        self.send(encode_del(id, key)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(decode_bool_response(&frame.payload)),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// MGET multiple keys. Returns a `Vec` parallel to `keys`.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn mget(&mut self, keys: &[&[u8]]) -> Result<Vec<Option<Bytes>>> {
        let id = self.next_id();
        self.send(encode_mget(id, keys)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(decode_mget_response(&frame.payload)),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Create a vector index `name` for `dim`-dimensional vectors, choosing
    /// the storage `backend` (in-RAM flat or on-disk Vamana graph).
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error
    /// (e.g. the index already exists).
    pub async fn vindex_create(
        &mut self,
        name: &str,
        dim: u32,
        kind: VectorKind,
        backend: VectorBackend,
    ) -> Result<()> {
        let id = self.next_id();
        self.send(encode_vindex_create(
            id,
            name,
            dim,
            kind.wire(),
            backend.wire(),
        ))
        .await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(()),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Drop the vector index `name`.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn vindex_drop(&mut self, name: &str) -> Result<()> {
        let id = self.next_id();
        self.send(encode_vindex_drop(id, name)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(()),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Insert `vector` under `id` into the index `name`.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn vset(&mut self, name: &str, id: u64, vector: &[f32]) -> Result<()> {
        let req_id = self.next_id();
        self.send(encode_vset(req_id, name, id, vector, Flags::empty()))
            .await?;
        let frame = self.recv_frame(req_id).await?;
        match frame.header.op {
            Op::Ok => Ok(()),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Fetch the stored vector for `id` in `name`. `None` if absent.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or non-`NotFound`
    /// server error.
    pub async fn vget(&mut self, name: &str, id: u64) -> Result<Option<Vec<f32>>> {
        let req_id = self.next_id();
        self.send(encode_vget(req_id, name, id)).await?;
        let frame = self.recv_frame(req_id).await?;
        match frame.header.op {
            Op::Ok => Ok(decode_value_response(&frame.payload).map(|b| bytes_to_f32_vec(&b))),
            Op::Err => {
                if frame.payload.first().copied() == Some(ErrCode::NotFound as u8) {
                    Ok(None)
                } else {
                    Err(Self::server_err(&frame.payload))
                }
            }
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Tombstone the vector for `id` in `name`. Returns `true` if it existed.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn vdel(&mut self, name: &str, id: u64) -> Result<bool> {
        let req_id = self.next_id();
        self.send(encode_vdel(req_id, name, id)).await?;
        let frame = self.recv_frame(req_id).await?;
        match frame.header.op {
            Op::Ok => Ok(decode_bool_response(&frame.payload)),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Search `name` for the `k` nearest vectors to `query`.
    ///
    /// Returns `(vec_id, cosine)` pairs, highest cosine first.
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn vsearch(&mut self, name: &str, query: &[f32], k: u32) -> Result<Vec<(u64, f32)>> {
        let req_id = self.next_id();
        self.send(encode_vsearch(req_id, name, k, query)).await?;
        let frame = self.recv_frame(req_id).await?;
        match frame.header.op {
            Op::Ok => Ok(decode_vsearch_response(&frame.payload)),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }
}

/// Per-connection socket tuning. `TCP_NODELAY` keeps the small KV
/// request/reply pairs from getting batched by Nagle's algorithm,
/// and a keepalive probe detects a peer that died without sending
/// FIN within roughly 90 seconds instead of the kernel default of
/// 2-3 hours. Failures are swallowed: a connection that ignores the
/// tuning still works, just with the default tail behaviour.
fn tune_socket(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
    let sock = socket2::SockRef::from(stream);
    let ka = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(60))
        .with_interval(std::time::Duration::from_secs(10));
    let _ = sock.set_tcp_keepalive(&ka);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_err_parses_code_and_message() {
        // payload: [code u8][msg_len u8][msg bytes...]
        let payload = Bytes::from_static(&[
            0x01, 9, b'n', b'o', b'-', b's', b'u', b'c', b'h', b'-', b'k',
        ]);
        let err = SkegClient::server_err(&payload);
        match err {
            ClientError::Server { code, msg } => {
                assert!(matches!(code, ErrCode::NotFound));
                assert_eq!(msg, "no-such-k");
            }
            other => panic!("expected Server error, got {other:?}"),
        }
    }

    #[test]
    fn server_err_unknown_code_falls_back_to_internal() {
        let payload = Bytes::from_static(&[0xff, 2, b'h', b'i']);
        match SkegClient::server_err(&payload) {
            ClientError::Server { code, msg } => {
                assert!(matches!(code, ErrCode::Internal));
                assert_eq!(msg, "hi");
            }
            other => panic!("expected Server error, got {other:?}"),
        }
    }

    #[test]
    fn server_err_short_payload_is_unexpected_op() {
        let payload = Bytes::from_static(&[0x01]); // only the code byte
        assert!(matches!(
            SkegClient::server_err(&payload),
            ClientError::UnexpectedOp
        ));
    }

    #[test]
    fn server_err_truncated_message_does_not_panic() {
        // Declared msg_len=10 but only 3 message bytes follow. The
        // parser should clamp rather than read out of bounds.
        let payload = Bytes::from_static(&[0x01, 10, b'h', b'i', b'!']);
        let err = SkegClient::server_err(&payload);
        match err {
            ClientError::Server { msg, .. } => assert_eq!(msg, "hi!"),
            other => panic!("expected Server error, got {other:?}"),
        }
    }
}
