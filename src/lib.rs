#![deny(unsafe_code)]

//! `skeg-client` - async TCP client for the skeg binary protocol.

use bytes::{Bytes, BytesMut};
pub use skeg_proto::NativeCapabilities;
use skeg_proto::{
    ErrCode, Flags, FrameParser, Op, ParseError, ServerStats, ShardStats, VERSION_V1, VERSION_V2,
    VindexInfo, bytes_to_f32_vec, decode_bool_response, decode_mget_response,
    decode_native_capabilities_response, decode_shards_response, decode_stats_response,
    decode_value_response, decode_vindex_list_response, decode_vsearch_response, encode_del,
    encode_get, encode_mget, encode_native_hello, encode_ping, encode_set, encode_shards,
    encode_stats, encode_vdel, encode_vget, encode_vindex_create, encode_vindex_drop,
    encode_vindex_list, encode_vsearch, encode_vset,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

// Re-exported so a caller does not need its own skeg-proto dependency just to
// name a protocol version or read a capability response.
pub use skeg_proto::{NativeVectorKindV2, VERSION_V1 as PROTOCOL_V1, VERSION_V2 as PROTOCOL_V2};

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

    #[error("reply came back in native v{got}, sent v{sent}")]
    VersionMismatch { sent: u8, got: u8 },

    #[error("{0} requires a native v2 connection; use SkegClient::connect_with_version")]
    RequiresV2(&'static str),

    #[error("malformed native hello response")]
    BadNativeHello,
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

    /// The typed kind for a native v1 wire byte, or `None` if v1 has no name
    /// for it. Byte 3 is deliberately `None`: it meant PQ, and today's server
    /// refuses it rather than build the TQ1 index the same byte means in v2.
    #[must_use]
    pub fn from_wire(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(VectorKind::F32),
            1 => Some(VectorKind::Int8),
            2 => Some(VectorKind::Binary),
            _ => None,
        }
    }
}

/// Quantization kind for a vector index on native protocol v2.
///
/// Separate from [`VectorKind`] on purpose. Codes 0..2 agree, but code 3 is
/// PQ in v1 and TQ1 here - one shared enum would hide exactly the collision
/// that protocol v2 exists to resolve. The TurboQuant tiers are reachable
/// only from a v2 connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorKindV2 {
    /// Full-precision f32.
    F32,
    /// Symmetric 8-bit integer quantization.
    Int8,
    /// 1-bit sign quantization.
    Binary,
    /// TurboQuant, 1 bit per dimension.
    Tq1,
    /// TurboQuant, 2 bits per dimension. The server's default tier.
    Tq2,
    /// TurboQuant, 4 bits per dimension.
    Tq4,
}

impl VectorKindV2 {
    fn wire(self) -> u8 {
        match self {
            VectorKindV2::F32 => 0,
            VectorKindV2::Int8 => 1,
            VectorKindV2::Binary => 2,
            VectorKindV2::Tq1 => 3,
            VectorKindV2::Tq2 => 4,
            VectorKindV2::Tq4 => 5,
        }
    }

    /// The typed kind for a native v2 wire byte, or `None` above 5.
    #[must_use]
    pub fn from_wire(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(VectorKindV2::F32),
            1 => Some(VectorKindV2::Int8),
            2 => Some(VectorKindV2::Binary),
            3 => Some(VectorKindV2::Tq1),
            4 => Some(VectorKindV2::Tq2),
            5 => Some(VectorKindV2::Tq4),
            _ => None,
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
    version: u8,
}

impl SkegClient {
    /// Connect to a skeg server at `addr`.
    ///
    /// # Errors
    ///
    /// Returns an IO error if the TCP connection cannot be established.
    pub async fn connect(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        Self::connect_with_version(addr, VERSION_V1).await
    }

    /// Connect and speak `version` on every frame.
    ///
    /// [`VERSION_V1`] is what [`connect`](Self::connect) uses, so existing
    /// callers keep sending the same bytes. [`VERSION_V2`] is required for
    /// the TurboQuant kinds and for [`native_hello`](Self::native_hello).
    ///
    /// # Errors
    ///
    /// Returns an IO error if the TCP connection cannot be established.
    pub async fn connect_with_version(
        addr: impl ToSocketAddrs,
        version: u8,
    ) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        tune_socket(&stream);
        Ok(Self {
            stream,
            parser: FrameParser::new(),
            read_buf: BytesMut::with_capacity(64 * 1024),
            next_req_id: 1,
            version,
        })
    }

    /// The native protocol version this client stamps on every frame.
    #[must_use]
    pub fn version(&self) -> u8 {
        self.version
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_req_id;
        self.next_req_id += 1;
        id
    }

    async fn send(&mut self, frame: Bytes) -> Result<()> {
        let frame = self.stamp_version(frame);
        self.stream.write_all(&frame).await?;
        Ok(())
    }

    /// Rewrite a v1-encoded frame to carry this connection's version.
    ///
    /// `skeg-proto`'s typed encoders emit v1 for legacy callers. The version
    /// is one byte at a fixed header offset, so stamping it here is cheaper
    /// and far less error-prone than duplicating every payload builder - the
    /// same move the server makes when it answers in the request's version.
    fn stamp_version(&self, frame: Bytes) -> Bytes {
        if self.version == VERSION_V1 {
            return frame;
        }
        let mut out = BytesMut::from(frame.as_ref());
        out[2] = self.version;
        out.freeze()
    }

    /// Read the next frame and validate it carries the expected
    /// `req_id`. A mismatch surfaces as `ReqIdMismatch` so a server
    /// out-of-order reply or a desynchronised connection fails loud
    /// instead of silently returning the wrong call's result.
    async fn recv_frame(&mut self, expected_id: u64) -> Result<skeg_proto::Frame> {
        loop {
            if let Some(frame) = self.parser.feed(&mut self.read_buf)? {
                if frame.header.version != self.version {
                    return Err(ClientError::VersionMismatch {
                        sent: self.version,
                        got: frame.header.version,
                    });
                }
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

    /// Create a vector index using a native v2 kind, TurboQuant tiers
    /// included.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::RequiresV2`] on a v1 connection, or an IO,
    /// protocol, or server error.
    pub async fn vindex_create_v2(
        &mut self,
        name: &str,
        dim: u32,
        kind: VectorKindV2,
        backend: VectorBackend,
    ) -> Result<()> {
        if self.version != VERSION_V2 {
            return Err(ClientError::RequiresV2("vindex_create_v2"));
        }
        self.vindex_create_raw_kind(name, dim, kind.wire(), backend)
            .await
    }

    /// Create a vector index from a raw kind byte.
    ///
    /// The escape hatch for a byte neither kind enum names - which is how the
    /// server's refusal of it can be exercised at all. Prefer
    /// [`vindex_create`](Self::vindex_create) or
    /// [`vindex_create_v2`](Self::vindex_create_v2).
    ///
    /// # Errors
    ///
    /// Returns an error on IO failure, protocol error, or server error.
    pub async fn vindex_create_raw_kind(
        &mut self,
        name: &str,
        dim: u32,
        kind: u8,
        backend: VectorBackend,
    ) -> Result<()> {
        let id = self.next_id();
        self.send(encode_vindex_create(id, name, dim, kind, backend.wire()))
            .await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => Ok(()),
            Op::Err => Err(Self::server_err(&frame.payload)),
            _ => Err(ClientError::UnexpectedOp),
        }
    }

    /// Ask a v2 server which protocol version and vector kinds it supports.
    ///
    /// Test a kind with
    /// `caps.supports(NativeVectorKindV2::Tq2)`. A v1 connection gets
    /// [`ClientError::RequiresV2`] rather than a frame the server would
    /// refuse anyway.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::RequiresV2`] on a v1 connection, or an IO,
    /// protocol, or server error.
    pub async fn native_hello(&mut self) -> Result<NativeCapabilities> {
        if self.version != VERSION_V2 {
            return Err(ClientError::RequiresV2("native_hello"));
        }
        let id = self.next_id();
        self.send(encode_native_hello(id)).await?;
        let frame = self.recv_frame(id).await?;
        match frame.header.op {
            Op::Ok => decode_native_capabilities_response(&frame.payload)
                .ok_or(ClientError::BadNativeHello),
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
