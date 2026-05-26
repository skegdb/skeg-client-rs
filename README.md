# skeg-client

Async Rust client for the [skeg](https://github.com/skegdb/skeg)
binary protocol. Single-connection, tokio-based, talks to the native
`skeg` server on port `7379` (not the RESP3 server on `6379`; see the
note at the bottom).

```toml
[dependencies]
skeg-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quickstart

```rust
use skeg_client::{SkegClient, VectorBackend, VectorKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = SkegClient::connect("127.0.0.1:7379").await?;

    // KV
    c.set(b"hello", b"world").await?;
    let v = c.get(b"hello").await?;
    assert_eq!(v.as_deref(), Some(&b"world"[..]));

    // Vectors
    c.vindex_create("docs", 1024, VectorKind::Int8, VectorBackend::DiskVamana).await?;
    c.vset("docs", 1, &vec![0.0; 1024]).await?;
    let hits = c.vsearch("docs", &vec![0.0; 1024], 10).await?;
    for (id, score) in hits {
        println!("{id} {score:.4}");
    }
    Ok(())
}
```

## Operations

Each call is one round-trip on the single connection. There is no
internal pooling: a `SkegClient` is `!Sync` so you hold one per task,
or wrap it in your own mutex.

| Group         | Methods                                                                 |
| ------------- | ----------------------------------------------------------------------- |
| Connection    | `connect(addr)`, `ping`                                                 |
| KV            | `get`, `set`, `set_no_reply`, `del`, `mget`                             |
| Vector index  | `vindex_create(name, dim, kind, backend)`, `vindex_drop`, `vindex_list` |
| Vector data   | `vset(name, id, vec)`, `vget`, `vdel`, `vsearch(name, query, k)`        |
| Introspection | `stats`, `shards`                                                       |

`VectorKind` is `F32 | Int8 | Binary`. `VectorBackend` is `Flat`
(in-RAM exhaustive scan, fine up to a few thousand vectors) or
`DiskVamana` (on-disk Vamana graph, the right choice past that
threshold).

## Errors

All operations return `Result<T, ClientError>`:

- `Io` for socket failures.
- `Protocol` for unparseable server responses.
- `Server { code, msg }` for `-ERR` frames from the server.
- `UnexpectedOp` for the wrong opcode in a response.
- `ConnectionClosed` for a half-closed read.

## RESP3 instead?

`skeg-client` speaks the native binary protocol on port `7379`. For
the RESP3 wire on `6379` use any Redis client (e.g. `redis-rs`); the
server speaks both protocols with the same data underneath.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
