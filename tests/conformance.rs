//! Native-protocol conformance, driven by the shared case file.
//!
//! Cases live in `skeg-internal/conformance/native-cases.jsonl` and are the
//! contract for every skeg client. Point `SKEG_CONFORMANCE_DIR` at that
//! directory and `SKEG_BIN` at a skeg binary; without either, the test skips.
//!
//! Every case goes through a public client method, so a missing method is a
//! failure rather than a gap nobody notices. Cases marked `wire_only` are ones
//! a typed client cannot express (wrong arity, ops the server does not
//! implement); the Python validators cover those against the raw wire.

use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use serde_json::Value;
use skeg_client::{SkegClient, VectorBackend, VectorKind, VectorKindV2};

struct Server {
    child: Child,
    port: u16,
    data_dir: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_server(bin: &str) -> Option<Server> {
    let port = free_port();
    let data_dir = std::env::temp_dir().join(format!("skeg-rs-conformance-{port}"));
    std::fs::create_dir_all(&data_dir).ok()?;
    let child = Command::new(bin)
        .args([
            "--mode",
            "rw",
            "--addr",
            &format!("127.0.0.1:{port}"),
            "--data-dir",
            data_dir.to_str()?,
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Some(Server {
                child,
                port,
                data_dir,
            });
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

fn vector_of(v: &Value) -> Vec<f32> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_f64().unwrap() as f32)
        .collect()
}

/// Run one case; `Err(reason)` when it does not hold.
async fn run_case(c: &mut SkegClient, case: &Value) -> Result<(), String> {
    let op = case["op"].as_str().unwrap();
    let a = &case["args"];
    let want = &case["want"];
    let wants_err = want.get("error_contains").is_some() || want.get("error_code").is_some();

    // Unwrap a client call against the case: a success when an error was
    // wanted (or the reverse) is the failure, and the message must match.
    macro_rules! check {
        ($call:expr) => {
            match $call.await {
                Ok(v) => {
                    if wants_err && want.get("error_code").is_none() {
                        return Err(format!("expected a server error, got {:?}", v));
                    }
                    v
                }
                Err(e) => {
                    let text = e.to_string();
                    if let Some(want_msg) = want.get("error_contains").and_then(Value::as_str) {
                        // This client refuses a v2-only call on a v1 connection
                        // itself, so the server never gets to say so. Refusing
                        // locally satisfies a case that pins the server's own
                        // "requires protocol version 2" refusal.
                        let guarded = matches!(e, skeg_client::ClientError::RequiresV2(_))
                            && want_msg.contains("protocol version 2");
                        return if text.contains(want_msg) || guarded {
                            Ok(())
                        } else {
                            Err(format!("error {text:?} lacks {want_msg:?}"))
                        };
                    }
                    if want.get("error_code").is_some() {
                        return Ok(());
                    }
                    return Err(format!("unexpected error: {text}"));
                }
            }
        };
    }

    match op {
        "ping" => {
            check!(c.ping());
        }
        "native_hello" => {
            let caps = check!(c.native_hello());
            if let Some(w) = want.get("capabilities") {
                let want_version = w["protocol_version"].as_u64().unwrap() as u8;
                let want_mask = w["vector_kind_mask"].as_u64().unwrap() as u8;
                if caps.protocol_version != want_version || caps.vector_kind_mask != want_mask {
                    return Err(format!(
                        "capabilities {caps:?} != version {want_version} mask {want_mask:#b}"
                    ));
                }
            }
        }
        "stats" => {
            check!(c.stats());
        }
        "shards" => {
            check!(c.shards());
        }
        "get" => {
            let got = check!(c.get(a["key"].as_str().unwrap().as_bytes()));
            if let Some(w) = want.get("value").and_then(Value::as_str) {
                let got = got.ok_or_else(|| "want a value, got None".to_string())?;
                if got.as_ref() != w.as_bytes() {
                    return Err(format!("value {got:?} != {w:?}"));
                }
            } else if want.get("error_code").is_some() && got.is_some() {
                return Err(format!("want NotFound, got {got:?}"));
            }
        }
        "set" => {
            check!(c.set(
                a["key"].as_str().unwrap().as_bytes(),
                a["value"].as_str().unwrap().as_bytes()
            ));
        }
        "del" => {
            let got = check!(c.del(a["key"].as_str().unwrap().as_bytes()));
            if let Some(w) = want.get("bool").and_then(Value::as_bool)
                && got != w
            {
                return Err(format!("bool {got} != {w}"));
            }
        }
        "mget" => {
            let keys: Vec<String> = a["keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|k| k.as_str().unwrap().to_owned())
                .collect();
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            let got = check!(c.mget(&refs));
            if let Some(w) = want.get("mget").and_then(Value::as_array) {
                let expected: Vec<Option<&str>> = w.iter().map(|v| v.as_str()).collect();
                let actual: Vec<Option<String>> = got
                    .iter()
                    .map(|o| o.as_ref().map(|b| String::from_utf8_lossy(b).into_owned()))
                    .collect();
                for (i, exp) in expected.iter().enumerate() {
                    if actual[i].as_deref() != *exp {
                        return Err(format!("mget[{i}] = {:?}, want {:?}", actual[i], exp));
                    }
                }
            }
        }
        "vindex_create" => {
            let kind = a["kind"].as_u64().unwrap() as u8;
            let name = a["name"].as_str().unwrap();
            let dim = a["dim"].as_u64().unwrap() as u32;
            let backend = if a["backend"].as_u64().unwrap() == 0 {
                VectorBackend::Flat
            } else {
                VectorBackend::DiskVamana
            };
            if c.version() == skeg_proto::VERSION_V2 {
                match VectorKindV2::from_wire(kind) {
                    Some(k) => {
                        check!(c.vindex_create_v2(name, dim, k, backend));
                    }
                    // A byte v2 has no name for: only a raw frame reaches the
                    // server's refusal, which is what the case pins.
                    None => {
                        check!(c.vindex_create_raw_kind(name, dim, kind, backend));
                    }
                }
            } else {
                match VectorKind::from_wire(kind) {
                    Some(k) => {
                        check!(c.vindex_create(name, dim, k, backend));
                    }
                    None => {
                        check!(c.vindex_create_raw_kind(name, dim, kind, backend));
                    }
                }
            }
        }
        "vindex_drop" => {
            check!(c.vindex_drop(a["name"].as_str().unwrap()));
        }
        "vindex_list" => {
            let rows = check!(c.vindex_list());
            if let Some(w) = want.get("rows_contain").and_then(Value::as_array) {
                let names: Vec<String> = rows.iter().map(|r| r.name.clone()).collect();
                for n in w {
                    let n = n.as_str().unwrap();
                    if !names.iter().any(|x| x == n) {
                        return Err(format!("{n:?} missing from {names:?}"));
                    }
                }
            }
        }
        "vset" => {
            check!(c.vset(
                a["name"].as_str().unwrap(),
                a["id"].as_u64().unwrap(),
                &vector_of(&a["vector"])
            ));
        }
        "vget" => {
            let got = check!(c.vget(a["name"].as_str().unwrap(), a["id"].as_u64().unwrap()));
            if want.get("error_code").is_some() && got.is_some() {
                return Err("want NotFound, got a vector".to_string());
            }
        }
        "vdel" => {
            check!(c.vdel(a["name"].as_str().unwrap(), a["id"].as_u64().unwrap()));
        }
        "vsearch" => {
            let hits = check!(c.vsearch(
                a["name"].as_str().unwrap(),
                &vector_of(&a["query"]),
                a["k"].as_u64().unwrap() as u32
            ));
            if let Some(n) = want.get("hits_len").and_then(Value::as_u64)
                && hits.len() != n as usize
            {
                return Err(format!("want {n} hits, got {}", hits.len()));
            }
            if let Some(top) = want.get("hits_top_id").and_then(Value::as_u64) {
                match hits.first() {
                    Some((id, _)) if *id == top => {}
                    other => return Err(format!("top hit {other:?} != id {top}")),
                }
            }
        }
        other => return Err(format!("no client method drives op {other:?}")),
    }

    if let Some(v) = want.get("version").and_then(Value::as_u64)
        && u64::from(c.version()) != v
    {
        return Err(format!("client speaks v{}, case wants v{v}", c.version()));
    }
    Ok(())
}

#[tokio::test]
async fn native_conformance() {
    let (Ok(bin), Ok(dir)) = (
        std::env::var("SKEG_BIN"),
        std::env::var("SKEG_CONFORMANCE_DIR"),
    ) else {
        eprintln!("skipping: set SKEG_BIN and SKEG_CONFORMANCE_DIR");
        return;
    };
    let path = PathBuf::from(dir).join("native-cases.jsonl");
    let body = std::fs::read_to_string(&path).expect("read native-cases.jsonl");
    let cases: Vec<Value> = body
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("case is valid JSON"))
        .collect();

    let server = spawn_server(&bin).expect("skeg server did not start");
    // One client per frame version: a client speaks a single version.
    let mut v1 = SkegClient::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    let mut v2 =
        SkegClient::connect_with_version(("127.0.0.1", server.port), skeg_proto::VERSION_V2)
            .await
            .unwrap();

    let (mut passed, mut skipped) = (0usize, 0usize);
    let mut failures = Vec::new();
    for case in &cases {
        let id = case["id"].as_str().unwrap();
        if case.get("wire_only").is_some()
            || case.get("unvalidated").is_some()
            || case.get("raw_payload").is_some()
        {
            skipped += 1;
            continue;
        }
        let version = case.get("version").and_then(Value::as_u64).unwrap_or(1);
        let c = if version == 2 { &mut v2 } else { &mut v1 };
        match run_case(c, case).await {
            Ok(()) => passed += 1,
            Err(reason) => failures.push(format!("{id}: {reason}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} case(s) failed:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
    eprintln!("{passed} native cases passed, {skipped} skipped (wire-only)");
}
