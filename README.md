# skeg-client

Async Rust client for the [skeg](https://github.com/skegdb/skeg)
binary protocol. Single-connection, tokio-based, talks to the native
`skeg` server on port `7379` (not the RESP3 server on `6379`).

Speaks native protocol v1 and v2. Note that RESP3 carries a larger
command surface than the native wire does - see the note at the bottom
before choosing.

```toml
[dependencies]
skeg-client = "0.2"
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
| Connection    | `connect(addr)`, `connect_with_version(addr, PROTOCOL_V2)`, `version()`, `ping` |
| KV            | `get`, `set`, `set_no_reply`, `del`, `mget`                             |
| Vector index  | `vindex_create(name, dim, kind, backend)`, `vindex_create_v2`, `vindex_drop`, `vindex_list` |
| Vector data   | `vset(name, id, vec)`, `vget`, `vdel`, `vsearch(name, query, k)`        |
| Introspection | `stats`, `shards`, `native_hello`                                       |

`VectorBackend` is `Flat` (in-RAM exhaustive scan, fine up to a few
thousand vectors) or `DiskVamana` (on-disk Vamana graph, the right
choice past that threshold).

## Protocol versions

The native wire has two versions, and the client speaks one per
connection.

| | v1 (`connect`) | v2 (`connect_with_version`) |
| --- | --- | --- |
| Kinds | `VectorKind`: F32, Int8, Binary | `VectorKindV2`: F32, Int8, Binary, Tq1, Tq2, Tq4 |
| Capability negotiation | — | `native_hello()` |

```rust
use skeg_client::{NativeVectorKindV2, SkegClient, VectorBackend, VectorKindV2, PROTOCOL_V2};

let mut c = SkegClient::connect_with_version("127.0.0.1:7379", PROTOCOL_V2).await?;
let caps = c.native_hello().await?;
if caps.supports(NativeVectorKindV2::Tq2) {
    c.vindex_create_v2("notes", 1024, VectorKindV2::Tq2, VectorBackend::DiskVamana).await?;
}
```

**`VectorKindV2` is a separate enum from `VectorKind`, not an extension
of it.** Kind byte 3 means PQ in v1 and TQ1 in v2; a single enum would
hide exactly the collision that v2 exists to resolve. The server refuses
byte 3 in a v1 frame rather than build an index you did not ask for.

Every request carries the connection's version, and a reply that comes
back in a different one is rejected as `VersionMismatch` rather than
parsed anyway.

## Errors

All operations return `Result<T, ClientError>`:

- `Io` for socket failures.
- `Protocol` for unparseable server responses.
- `Server { code, msg }` for `-ERR` frames from the server.
- `UnexpectedOp` for the wrong opcode in a response.
- `ConnectionClosed` for a half-closed read.
- `VersionMismatch` when a reply's frame version is not the request's.
- `RequiresV2` for a v2-only call on a v1 connection, refused locally
  rather than sent for the server to reject.
- `BadNativeHello` for a malformed capability response.

## RESP3 instead?

**RESP3 carries more than this client can.** Vector payloads, search
filters, bulk `VMSET`, index consolidation, and every tenancy, quota and
QoS command exist only there; the native protocol has no frame for any
of them.

There is deliberately no second Rust client for it. Any Redis crate
sends arbitrary commands, so `redis-rs` reaches the whole `SKEG.*`
namespace already:

```rust
let mut conn = redis::Client::open("redis://127.0.0.1:6379/")?.get_connection()?;
let hits: Vec<redis::Value> = redis::cmd("SKEG.VSEARCH")
    .arg("notes").arg(10).arg(0).arg(query_bytes).arg("WITHPAYLOAD")
    .query(&mut conn)?;
```

Reach for `skeg-client` when you want typed calls over the native wire
and can live with its smaller surface. Reach for a Redis crate when you
want the full command set. Both talk to the same data.

## Conformance

`tests/conformance.rs` runs the case files shared by every skeg client,
driving this crate's public API:

```sh
SKEG_BIN=$(which skeg) \
SKEG_CONFORMANCE_DIR=<skeg-internal>/conformance cargo test
```

Without both env vars the test skips. Cases marked `wire_only` are
skipped by design: they are things a typed client cannot express, and
the case files ship standalone validators that cover them against the
raw wire.

## License

Apache-2.0. See [`LICENSE`](LICENSE).
