//! Actual CLI + Kernel + publisher/StorageBridge over a bounded in-memory fake parent.
//! These are offline transport fixtures, not Azure/provider qualification.
#![cfg(unix)]

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

const CAMPAIGN: &str = "00000000-0000-4000-8000-000000000001";
const RUN: &str = "00000000-0000-4000-8000-000000000002";
const GENERATION: &str = "00000000-0000-4000-8000-000000000003";
const UNRELATED: &str = "00000000-0000-4000-8000-000000000009";
const TABLE: &str = "telemetry.sqlserver.sqlserver_availability_replica_states";
const STAMP: &str = "Tue, 17 Nov 2026 03:00:00 GMT";
const FRAME_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
enum Case {
    Fresh,
    Nonempty,
    Conflict,
    RotateBeforeCommit,
    RefusedPut,
    UnknownPut,
    LostPutResponse,
    UnknownAdmission,
    UnknownDrainWrite,
    UnknownDrainRelease,
    WrongRoot,
    WrongOwner,
    Expired,
    MissingBridge,
    UnpairedBridge,
    PublicInput,
    Nonready,
    WrongGeneration,
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn canonical(value: &Value) -> Result<Vec<u8>> {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let values: BTreeMap<_, _> = map.iter().map(|(key, value)| (key.clone(), sorted(value))).collect();
                Value::Object(values.into_iter().collect())
            }
            Value::Array(values) => values.iter().map(sorted).collect(),
            value => value.clone(),
        }
    }
    Ok(serde_json::to_vec(&sorted(value))?)
}

fn response(request: &Value, status: u16, headers: BTreeMap<String, String>, body: &str) -> Value {
    json!({
        "schema_version":1,"kind":"storage_http_response","request_id":request["request_id"],
        "request_sha256":request["request_sha256"],"outcome":"response","status":status,
        "headers":headers.into_iter().collect::<Vec<_>>(),
        "body":body,"body_sha256":digest(body.as_bytes()),"truncated":false
    })
}

fn uncertain(request: &Value) -> Value {
    json!({
        "schema_version":1,"kind":"storage_http_response","request_id":request["request_id"],
        "request_sha256":request["request_sha256"],"outcome":"unknown","error_code":"response_lost"
    })
}

struct Parent {
    case: Case,
    input: Value,
    metadata: BTreeMap<String, String>,
    objects: BTreeMap<String, String>,
    requests: Vec<Value>,
    receipt: Option<Value>,
    error: String,
    lease: Option<String>,
    etag: u64,
    releases: usize,
    puts: usize,
}

impl Parent {
    fn members(&self) -> Result<serde_json::Map<String, Value>> {
        Ok(serde_json::from_str(&self.metadata["publishers"])?)
    }

    fn exchange(&mut self, request: Value) -> Result<Value> {
        let mut unsigned = request.clone();
        let request_hash = unsigned
            .as_object_mut()
            .context("request object")?
            .remove("request_sha256")
            .context("request digest")?;
        ensure!(
            request_hash == digest(&canonical(&unsigned)?),
            "canonical request identity changed"
        );
        ensure!(request["schema_version"] == 1 && request["kind"] == "storage_http_request");
        ensure!(request["request_id"] == self.requests.len() as u64 + 1);
        ensure!(
            request["authority"] == self.input["authority"],
            "original authority changed"
        );
        ensure!(request["original_deadline_unix_ms"] == self.input["original_deadline_unix_ms"]);
        ensure!(request["remaining_timeout_ms"].as_u64().context("remaining budget")? > 0);
        let body = request["body"].as_str().context("request body")?;
        ensure!(request["body_sha256"] == digest(body.as_bytes()));
        let headers: BTreeMap<String, String> =
            serde_json::from_value::<Vec<(String, String)>>(request["headers"].clone())?
                .into_iter()
                .collect();
        ensure!(headers.keys().all(|key| !key.eq_ignore_ascii_case("authorization")));
        let url = url::Url::parse(request["url"].as_str().context("request URL")?)?;
        ensure!(url.username().is_empty() && url.password().is_none());
        ensure!(url.host_str() == Some("offlineproof.blob.core.windows.net"));
        ensure!(url.query_pairs().all(|(key, _)| !key.eq_ignore_ascii_case("sig")));
        let method = request["method"].as_str().context("method")?;
        let query: BTreeMap<_, _> = url.query_pairs().into_owned().collect();
        self.requests.push(request.clone());
        let mut output = BTreeMap::new();
        let mut status = 200;
        let mut response_body = String::new();
        if request["target"] == "control" {
            if method == "HEAD" {
                output.insert("etag".into(), format!("\"{}\"", self.etag));
                output.extend(
                    self.metadata
                        .iter()
                        .map(|(key, value)| (format!("x-ms-meta-{key}"), value.clone())),
                );
            } else if query.get("comp").map(String::as_str) == Some("lease") {
                match headers.get("x-ms-lease-action").map(String::as_str) {
                    Some("acquire") => {
                        ensure!(self.lease.is_none(), "overlapping control lease");
                        self.lease = Some(headers.get("x-ms-proposed-lease-id").context("lease ID")?.clone());
                        status = 201;
                    }
                    Some("release") => {
                        ensure!(headers.get("x-ms-lease-id") == self.lease.as_ref());
                        self.lease = None;
                        self.releases += 1;
                        if (matches!(self.case, Case::UnknownAdmission) && self.releases == 1)
                            || (matches!(self.case, Case::UnknownDrainRelease) && self.releases == 2)
                        {
                            return Ok(uncertain(&request));
                        }
                    }
                    _ => anyhow::bail!("unexpected lease action"),
                }
            } else {
                ensure!(method == "PUT" && query.get("comp").map(String::as_str) == Some("metadata"));
                ensure!(self.lease.is_some() && headers.get("x-ms-lease-id") == self.lease.as_ref());
                if matches!(self.case, Case::UnknownDrainWrite) && self.releases == 1 {
                    return Ok(uncertain(&request));
                }
                self.metadata = headers
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix("x-ms-meta-")
                            .map(|key| (key.to_owned(), value.clone()))
                    })
                    .collect();
                self.etag += 1;
            }
        } else {
            ensure!(request["target"] == "object_store");
            let key = if query.get("comp").map(String::as_str) == Some("list") {
                ensure!(url.path() == "/tables");
                String::new()
            } else {
                url.path()
                    .strip_prefix("/tables/")
                    .context("data container")?
                    .to_owned()
            };
            if query.get("comp").map(String::as_str) == Some("list") {
                ensure!(method == "GET");
                let blobs = self
                    .objects
                    .iter()
                    .filter(|(key, _)| {
                        query.get("prefix").is_some_and(|prefix| key.starts_with(prefix))
                            && query.get("startFrom").is_none_or(|cursor| *key >= cursor)
                    })
                    .map(|(key, body)| {
                        format!(
                            "<Blob><Name>{key}</Name><Properties><Last-Modified>{STAMP}</Last-Modified>\
                     <Etag>\"offline-object\"</Etag><Content-Length>{}</Content-Length>\
                     <BlobType>BlockBlob</BlobType></Properties></Blob>",
                            body.len()
                        )
                    })
                    .collect::<String>();
                response_body = format!(
                    "<EnumerationResults ServiceEndpoint=\"https://offlineproof.blob.core.windows.net\" \
                     ContainerName=\"tables\"><Blobs>{blobs}</Blobs><NextMarker /></EnumerationResults>"
                );
                if matches!(self.case, Case::RotateBeforeCommit) {
                    self.metadata.insert("resetgeneration".into(), RUN.into());
                }
            } else if method == "PUT" {
                self.puts += 1;
                ensure!(headers.get("if-none-match").map(String::as_str) == Some("*"));
                ensure!(key.ends_with("/_delta_log/00000000000000000000.json"));
                if matches!(self.case, Case::RefusedPut) {
                    return Ok(json!({
                        "schema_version":1,"kind":"storage_http_response","request_id":request["request_id"],
                        "request_sha256":request["request_sha256"],"outcome":"definitely_not_sent",
                        "error_code":"admission_refused"
                    }));
                }
                if matches!(self.case, Case::Conflict) {
                    status = 409;
                    output.insert("x-ms-error-code".into(), "BlobAlreadyExists".into());
                    response_body = "<Error><Code>BlobAlreadyExists</Code></Error>".into();
                } else {
                    ensure!(
                        self.objects.insert(key, body.to_owned()).is_none(),
                        "log creation replayed"
                    );
                    if matches!(self.case, Case::UnknownPut) {
                        return Ok(uncertain(&request));
                    }
                    status = 201;
                    output.insert("etag".into(), "\"offline-object\"".into());
                    output.insert("last-modified".into(), STAMP.into());
                }
            } else if let Some(object) = self.objects.get(&key) {
                ensure!(matches!(method, "HEAD" | "GET"));
                output.insert("etag".into(), "\"offline-object\"".into());
                output.insert("last-modified".into(), STAMP.into());
                output.insert("content-length".into(), object.len().to_string());
                if method == "GET" {
                    response_body = object.clone();
                    if let Some(range) = headers.get("range").or_else(|| headers.get("x-ms-range")) {
                        let (start, end) = range
                            .strip_prefix("bytes=")
                            .context("byte range")?
                            .split_once('-')
                            .context("range bounds")?;
                        let start = start.parse::<usize>()?;
                        let end = if end.is_empty() {
                            object.len()
                        } else {
                            (end.parse::<usize>()? + 1).min(object.len())
                        };
                        response_body = object.get(start..end).context("valid UTF-8 test range")?.to_owned();
                        status = 206;
                        output.insert(
                            "content-range".into(),
                            format!("bytes {start}-{}/{}", end - 1, object.len()),
                        );
                        output.insert("content-length".into(), response_body.len().to_string());
                    }
                }
            } else {
                status = 404;
                output.insert("x-ms-error-code".into(), "BlobNotFound".into());
            }
        }
        Ok(response(&request, status, output, &response_body))
    }
}

fn private_json(path: &Path, value: &Value) -> Result<()> {
    std::fs::write(path, serde_json::to_vec(value)?)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

async fn run(case: Case) -> Result<Parent> {
    let scratch = tempfile::tempdir()?;
    let plan = json!({
        "account":"offlineproof","container":"tables",
        "campaign_root":format!("abfss://tables@offlineproof.dfs.core.windows.net/fixture/campaigns/{CAMPAIGN}/"),
        "run_root":format!("abfss://tables@offlineproof.dfs.core.windows.net/fixture/campaigns/{CAMPAIGN}/runs/{RUN}/"),
        "campaign_id":CAMPAIGN,"run_id":RUN,"generation":GENERATION,"table":TABLE,
        "partition_by_ingestion_date":true,"owner_id":RUN,"fencing_token":7
    });
    let table_path = format!(
        "fixture/campaigns/{CAMPAIGN}/runs/{RUN}/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states"
    );
    let mut input = json!({
        "authority":{
            "campaign_id":CAMPAIGN,"cell_run_id":RUN,"owner_id":RUN,"fencing_token":7,
            "storage_account":"offlineproof",
            "table_root_url":format!("abfss://tables@offlineproof.dfs.core.windows.net/{table_path}/"),
            "control_container_url":"https://offlineproof.blob.core.windows.net/checkpoints",
            "sandbox_id":"offline-sandbox","reset_generation":GENERATION
        },
        "original_deadline_unix_ms":u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())? + 30_000
    });
    match case {
        Case::WrongRoot => input["authority"]["table_root_url"] = plan["run_root"].clone(),
        Case::WrongOwner => input["authority"]["owner_id"] = json!(UNRELATED),
        Case::Expired => input["original_deadline_unix_ms"] = json!(1),
        _ => {}
    }
    let mut parent = Parent {
        case,
        input: input.clone(),
        metadata: BTreeMap::from([
            ("sandboxid".into(), "offline-sandbox".into()),
            ("resetstate".into(), "ready-v1".into()),
            ("resetgeneration".into(), GENERATION.into()),
            ("publisherregistryversion".into(), "1".into()),
            (
                "publishers".into(),
                json!({UNRELATED:{"generation":GENERATION,"state":"active"}}).to_string(),
            ),
            ("occupancy".into(), "released".into()),
            ("unrelated".into(), "preserved".into()),
        ]),
        objects: BTreeMap::new(),
        requests: Vec::new(),
        receipt: None,
        error: String::new(),
        lease: None,
        etag: 1,
        releases: 0,
        puts: 0,
    };
    if matches!(case, Case::Nonempty) {
        parent
            .objects
            .insert(format!("{table_path}/foreign"), "preserve-original".into());
    }
    if matches!(case, Case::Nonready) {
        parent.metadata.insert("resetstate".into(), "fenced-v1".into());
    }
    if matches!(case, Case::WrongGeneration) {
        parent.metadata.insert("resetgeneration".into(), RUN.into());
    }
    let plan_path = scratch.path().join("plan.json");
    let input_path = scratch.path().join("bridge.json");
    private_json(&plan_path, &plan)?;
    private_json(&input_path, &input)?;
    if matches!(case, Case::PublicInput) {
        std::fs::set_permissions(&input_path, std::fs::Permissions::from_mode(0o644))?;
    }
    let marker = scratch.path().join("unexpected-auth");
    let az = scratch.path().join("az");
    std::fs::write(
        &az,
        "#!/bin/sh\nprintf attempted > \"$INITIALIZER_IO_MARKER\"\nexit 97\n",
    )?;
    std::fs::set_permissions(&az, std::fs::Permissions::from_mode(0o700))?;
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_arrowcol-loadgen"));
    command
        .env_clear()
        .env("PATH", scratch.path())
        .env("HOME", scratch.path())
        .env("TMPDIR", scratch.path())
        .env("AZURE_CONFIG_DIR", scratch.path())
        .env("INITIALIZER_IO_MARKER", &marker)
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("ALL_PROXY", "http://127.0.0.1:9")
        .env("NO_PROXY", "")
        .args(["initialize-owned-run-table", "--plan"])
        .arg(&plan_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if !matches!(case, Case::MissingBridge) {
        command.arg("--storage-bridge-input").arg(&input_path);
        if !matches!(case, Case::UnpairedBridge) {
            command.arg("--storage-bridge-stdio");
        }
    }
    let mut child = command.spawn()?;
    let mut input_pipe = Some(child.stdin.take().context("stdin")?);
    let mut output = BufReader::new(child.stdout.take().context("stdout")?);
    let stderr = child.stderr.take().context("stderr")?;
    let errors = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stderr.take(256 * 1024).read_to_end(&mut bytes).await?;
        Ok::<_, std::io::Error>(bytes)
    });
    let status = tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let mut line = String::new();
            let bytes = (&mut output).take(FRAME_LIMIT as u64 + 1).read_line(&mut line).await?;
            if bytes == 0 {
                break;
            }
            ensure!(
                bytes <= FRAME_LIMIT && line.ends_with('\n'),
                "unbounded/incomplete child frame"
            );
            let value: Value = serde_json::from_str(&line)?;
            if value["kind"] == "owned_run_table_receipt" {
                ensure!(parent.receipt.is_none(), "duplicate terminal receipt");
                ensure!(value["schema_version"] == 1 && value["authority"] == input["authority"]);
                ensure!(value["original_deadline_unix_ms"] == input["original_deadline_unix_ms"]);
                ensure!(parent.members()?.len() == 1, "success before normal drain");
                parent.receipt = Some(value);
            } else {
                ensure!(parent.receipt.is_none(), "effect after final receipt");
                let disconnect = matches!(case, Case::LostPutResponse)
                    && value["target"] == "object_store"
                    && value["method"] == "PUT";
                let reply = parent.exchange(value)?;
                if disconnect {
                    input_pipe.take();
                    continue;
                }
                let writer = input_pipe.as_mut().context("request after parent pipe closed")?;
                writer.write_all(&serde_json::to_vec(&reply)?).await?;
                writer.write_all(b"\n").await?;
                writer.flush().await?;
            }
        }
        child.wait().await.context("CLI exit")
    })
    .await
    .context("bounded CLI bridge completion")??;
    let errors = String::from_utf8(errors.await??)?;
    ensure!(!marker.exists(), "direct credential fallback");
    ensure!(parent.metadata["unrelated"] == "preserved");
    ensure!(parent.members()?.get(UNRELATED) == Some(&json!({"generation":GENERATION,"state":"active"})));
    ensure!(status.success() == matches!(case, Case::Fresh), "{case:?}: {errors}");
    ensure!(
        parent.receipt.is_some() == status.success(),
        "failure-shaped terminal receipt"
    );
    parent.error = errors;
    eprintln!(
        "OWNED_BRIDGE_OFFLINE case={case:?} success={} requests={} puts={} members={}",
        status.success(),
        parent.requests.len(),
        parent.puts,
        parent.members()?.len()
    );
    Ok(parent)
}

#[tokio::test]
async fn actual_cli_bridge_creates_one_real_zero_row_table_and_drains() -> Result<()> {
    let parent = run(Case::Fresh).await?;
    let receipt = &parent.receipt.context("terminal receipt")?["receipt"];
    assert_eq!(receipt["publisher_drained"], true);
    assert_eq!(receipt["delta"]["version"], 0);
    assert_eq!(receipt["delta"]["active_files"], 0);
    assert_eq!(receipt["delta"]["partition_columns"], json!(["__arrowcol_ingestion"]));
    assert_eq!(parent.objects.len(), 1);
    assert_eq!(parent.puts, 1);
    let log = parent.objects.values().next().context("real Kernel log")?;
    let actions = log
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert!(actions.iter().all(|action| action.get("add").is_none()));
    let metadata = actions
        .iter()
        .find_map(|action| action.get("metaData"))
        .context("Kernel metadata")?;
    assert_eq!(metadata["id"], receipt["delta"]["table_id"]);
    assert_eq!(metadata["schemaString"], receipt["delta"]["schema_json"]);
    assert!(
        metadata["schemaString"]
            .as_str()
            .context("schema")?
            .contains("sample_time_utc__unix_nanos")
    );
    Ok(())
}

#[tokio::test]
async fn actual_cli_bridge_fresh_refusals_never_adopt_or_replay() -> Result<()> {
    for case in [
        Case::Nonempty,
        Case::Conflict,
        Case::RotateBeforeCommit,
        Case::RefusedPut,
        Case::Nonready,
        Case::WrongGeneration,
    ] {
        let parent = run(case).await?;
        assert!(parent.receipt.is_none());
        assert!(parent.puts <= 1);
        if matches!(case, Case::Nonempty) {
            assert_eq!(
                parent.objects.values().next().map(String::as_str),
                Some("preserve-original")
            );
            assert_eq!(parent.puts, 0);
            assert!(
                parent.error.contains("fresh table target is not empty"),
                "{}",
                parent.error
            );
        }
        if matches!(case, Case::RotateBeforeCommit) {
            assert_eq!(parent.puts, 0);
            assert!(
                parent
                    .requests
                    .iter()
                    .any(|request| request["target"] == "object_store")
            );
        }
        if matches!(case, Case::Conflict | Case::RefusedPut) {
            assert_eq!(parent.puts, 1, "intended refusal was not reached: {}", parent.error);
        }
        if matches!(case, Case::Nonready | Case::WrongGeneration) {
            assert!(!parent.requests.is_empty());
            assert!(
                parent
                    .requests
                    .iter()
                    .all(|request| request["target"] == "control" && request["method"] == "HEAD")
            );
            assert_eq!(parent.members()?.len(), 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn actual_cli_bridge_unknown_effects_never_report_success_or_force_cleanup() -> Result<()> {
    for case in [
        Case::UnknownAdmission,
        Case::UnknownPut,
        Case::LostPutResponse,
        Case::UnknownDrainWrite,
        Case::UnknownDrainRelease,
    ] {
        let parent = run(case).await?;
        assert_eq!(
            parent.members()?.len(),
            if matches!(case, Case::UnknownDrainRelease) {
                1
            } else {
                2
            }
        );
        assert!(parent.receipt.is_none());
        assert_eq!(parent.puts, usize::from(!matches!(case, Case::UnknownAdmission)));
        assert_eq!(
            parent.objects.len(),
            usize::from(!matches!(case, Case::UnknownAdmission))
        );
    }
    Ok(())
}

#[tokio::test]
async fn actual_cli_missing_or_invalid_bridge_fails_before_first_exchange() -> Result<()> {
    for case in [
        Case::MissingBridge,
        Case::UnpairedBridge,
        Case::PublicInput,
        Case::WrongRoot,
        Case::WrongOwner,
        Case::Expired,
    ] {
        let parent = run(case).await?;
        assert!(parent.requests.is_empty());
        assert!(parent.objects.is_empty());
        assert_eq!(parent.members()?.len(), 1);
    }
    Ok(())
}
