use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{Client, Method, Response, StatusCode, Url};
use serde::Deserialize;
use serde_json::json;

use crate::{Samples, cpu_seconds};

const VERSION: &str = "2021-12-02";
const APPEND_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, clap::ValueEnum, Debug)]
pub enum Mode {
    Block,
    Append,
}

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    pub endpoint: String,
    #[arg(long, default_value = "spike")]
    pub container: String,
    #[arg(long, value_enum)]
    pub mode: Mode,
    #[arg(long, default_value_t = 1_048_576)]
    pub bytes: usize,
    #[arg(long, default_value_t = 30)]
    pub seconds: u64,
    #[arg(long, default_value_t = 10)]
    pub warmup: u64,
    #[arg(long, default_value_t = 1024)]
    pub rotate_mib: u64,
    #[arg(long)]
    pub fence: bool,
}

struct Blob {
    client: Client,
    token: String,
    url: Url,
    lease: String,
    etag: String,
    offset: usize,
    blocks: Vec<String>,
    last_renewal: Instant,
    requests: u64,
}

impl Blob {
    async fn create(args: &Args) -> Result<Self> {
        let endpoint = Url::parse(&args.endpoint)?;
        ensure!(
            endpoint.scheme() == "https"
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none(),
            "endpoint must be an HTTPS URL without credentials or query"
        );
        ensure!(
            args.container
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && args.container.len() >= 3,
            "invalid container name"
        );
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()?;
        #[derive(Deserialize)]
        struct Token {
            access_token: String,
        }
        let token = if let Ok(value) = std::env::var("STORAGE_BEARER_TOKEN") {
            value
        } else {
            let metadata = Client::builder().no_proxy().timeout(Duration::from_secs(10)).build()?;
            metadata
                .get("http://169.254.169.254/metadata/identity/oauth2/token")
                .header("Metadata", "true")
                .query(&[
                    ("api-version", "2018-02-01"),
                    ("resource", "https://storage.azure.com/"),
                ])
                .send()
                .await?
                .error_for_status()?
                .json::<Token>()
                .await?
                .access_token
        };
        let url = endpoint.join(&format!("{}/{}", args.container, uuid::Uuid::new_v4()))?;
        let mut blob = Self {
            client,
            token,
            url,
            lease: uuid::Uuid::new_v4().to_string(),
            etag: String::new(),
            offset: 0,
            blocks: Vec::new(),
            last_renewal: Instant::now(),
            requests: 0,
        };
        let kind = match args.mode {
            Mode::Block => "BlockBlob",
            Mode::Append => "AppendBlob",
        };
        let response = blob
            .request(Method::PUT, &[])
            .header("x-ms-blob-type", kind)
            .header("If-None-Match", "*")
            .body(Vec::new())
            .send()
            .await?;
        blob.etag = checked(response, StatusCode::CREATED)
            .await?
            .headers()
            .get("etag")
            .context("missing ETag")?
            .to_str()?
            .into();
        let response = blob
            .request(Method::PUT, &[("comp", "lease")])
            .header("x-ms-lease-action", "acquire")
            .header("x-ms-lease-duration", "60")
            .header("x-ms-proposed-lease-id", &blob.lease)
            .body(Vec::new())
            .send()
            .await?;
        checked(response, StatusCode::CREATED).await?;
        blob.requests = 2;
        Ok(blob)
    }

    fn request(&self, method: Method, query: &[(&str, &str)]) -> reqwest::RequestBuilder {
        self.client
            .request(method, self.url.clone())
            .query(query)
            .bearer_auth(&self.token)
            .header("x-ms-version", VERSION)
            .header("x-ms-date", httpdate::fmt_http_date(SystemTime::now()))
    }

    async fn renew(&mut self) -> Result<()> {
        if self.last_renewal.elapsed() >= Duration::from_secs(15) {
            let response = self
                .request(Method::PUT, &[("comp", "lease")])
                .header("x-ms-lease-id", &self.lease)
                .header("x-ms-lease-action", "renew")
                .body(Vec::new())
                .send()
                .await?;
            checked(response, StatusCode::OK).await?;
            self.last_renewal = Instant::now();
            self.requests += 1;
        }
        Ok(())
    }

    async fn commit(&mut self, mode: Mode, payload: &[u8]) -> Result<(Duration, usize)> {
        let mut service = Duration::ZERO;
        let mut list_bytes = 0;
        match mode {
            Mode::Append => {
                for chunk in payload.chunks(APPEND_LIMIT) {
                    let start = Instant::now();
                    let response = self
                        .request(Method::PUT, &[("comp", "appendblock")])
                        .header("x-ms-lease-id", &self.lease)
                        .header("x-ms-blob-condition-appendpos", self.offset.to_string())
                        .body(chunk.to_vec())
                        .send()
                        .await?;
                    let response = checked(response, StatusCode::CREATED).await?;
                    self.offset += chunk.len();
                    self.etag = response.headers().get("etag").context("missing ETag")?.to_str()?.into();
                    service += start.elapsed();
                    self.requests += 1;
                }
            }
            Mode::Block => {
                let id = STANDARD.encode((self.blocks.len() as u64).to_be_bytes());
                let response = self
                    .request(Method::PUT, &[("comp", "block"), ("blockid", &id)])
                    .header("x-ms-lease-id", &self.lease)
                    .body(payload.to_vec())
                    .send()
                    .await?;
                checked(response, StatusCode::CREATED).await?;
                self.blocks.push(id);
                let xml = block_list(&self.blocks);
                list_bytes = xml.len();
                let start = Instant::now();
                let response = self
                    .request(Method::PUT, &[("comp", "blocklist")])
                    .header("x-ms-lease-id", &self.lease)
                    .header("If-Match", &self.etag)
                    .header("Content-Type", "application/xml")
                    .body(xml)
                    .send()
                    .await?;
                let response = checked(response, StatusCode::CREATED).await?;
                service = start.elapsed();
                self.etag = response.headers().get("etag").context("missing ETag")?.to_str()?.into();
                self.offset += payload.len();
                self.requests += 2;
            }
        }
        Ok((service, list_bytes))
    }

    async fn release(&self) -> Result<()> {
        let response = self
            .request(Method::PUT, &[("comp", "lease")])
            .header("x-ms-lease-action", "release")
            .header("x-ms-lease-id", &self.lease)
            .body(Vec::new())
            .send()
            .await?;
        checked(response, StatusCode::OK).await?;
        Ok(())
    }
}

fn block_list(ids: &[String]) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
    for id in ids {
        xml.push_str(&format!("<Latest>{id}</Latest>"));
    }
    xml.push_str("</BlockList>");
    xml
}

async fn checked(response: Response, expected: StatusCode) -> Result<Response> {
    if response.status() != expected {
        let status = response.status();
        let body = response.text().await?;
        bail!(
            "storage request failed: HTTP {status}, {}",
            body.chars().take(1024).collect::<String>()
        );
    }
    Ok(response)
}

pub async fn run(args: Args) -> Result<()> {
    ensure!(
        args.bytes >= 8
            && args.bytes <= 16 * 1024 * 1024
            && args.seconds > 0
            && args.seconds <= 600
            && args.rotate_mib > 0,
        "invalid bounded benchmark settings"
    );
    let mut blob = Blob::create(&args).await?;
    if args.fence {
        return fence(&mut blob, args.mode).await;
    }
    let mut payload = vec![0x5a; args.bytes];
    let mut sequence = 0_u64;
    for _ in 0..args.warmup {
        payload[..8].copy_from_slice(&sequence.to_le_bytes());
        blob.commit(args.mode, &payload).await?;
        sequence += 1;
    }
    let mut commit = Samples::default();
    let mut backend = Samples::default();
    let mut max_list_bytes = 0;
    let mut max_blocks = 0;
    let mut commits = 0_u64;
    let mut rotations = 0;
    let start = Instant::now();
    let cpu_start = cpu_seconds();
    let initial_requests = blob.requests;
    let mut previous_requests = 0;
    while start.elapsed() < Duration::from_secs(args.seconds) {
        if blob.offset as u64 + args.bytes as u64 > args.rotate_mib * 1_048_576 || blob.blocks.len() >= 40_000 {
            previous_requests += blob.requests;
            blob.release().await?;
            blob = Blob::create(&args).await?;
            rotations += 1;
        }
        blob.renew().await?;
        payload[..8].copy_from_slice(&sequence.to_le_bytes());
        let before = Instant::now();
        let (service, list_bytes) = blob.commit(args.mode, &payload).await?;
        commit.record(before.elapsed());
        backend.record(service);
        max_list_bytes = max_list_bytes.max(list_bytes);
        max_blocks = max_blocks.max(blob.blocks.len());
        sequence += 1;
        commits += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let cpu = cpu_seconds().zip(cpu_start).map(|(a, b)| a - b);
    let response = blob
        .request(Method::GET, &[])
        .header(
            "Range",
            format!("bytes={}-{}", blob.offset - payload.len(), blob.offset - 1),
        )
        .send()
        .await?;
    let read = checked(response, StatusCode::PARTIAL_CONTENT).await?.bytes().await?;
    ensure!(read.as_ref() == payload, "read-after-commit mismatch");
    blob.release().await?;
    println!(
        "{}",
        json!({
            "kind":"summary", "backend":"blob-primitives", "mode":format!("{:?}", args.mode),
            "seconds":elapsed, "commit_count":commits, "bytes_per_commit":args.bytes,
            "mib_per_second":commits as f64 * args.bytes as f64 / 1_048_576.0 / elapsed,
            "commits_per_second":commits as f64 / elapsed, "commit":commit.summary(), "backend_durable_call":backend.summary(),
            "cpu_seconds":cpu, "cpu_seconds_per_gib":cpu.map(|c| c / (commits as f64 * args.bytes as f64 / 1_073_741_824.0)),
            "requests":previous_requests + blob.requests - initial_requests, "rotations":rotations,
            "max_block_list_bytes":max_list_bytes, "max_committed_blocks":max_blocks,
            "read_back_passed":true, "api_version":VERSION, "append_request_limit":APPEND_LIMIT
        })
    );
    Ok(())
}

async fn fence(blob: &mut Blob, mode: Mode) -> Result<()> {
    let contenders = [uuid::Uuid::new_v4().to_string(), uuid::Uuid::new_v4().to_string()];
    blob.release().await?;
    let acquire = |id: &str| {
        blob.request(Method::PUT, &[("comp", "lease")])
            .header("x-ms-lease-action", "acquire")
            .header("x-ms-lease-duration", "60")
            .header("x-ms-proposed-lease-id", id)
            .body(Vec::new())
            .send()
    };
    let (a, b) = tokio::join!(acquire(&contenders[0]), acquire(&contenders[1]));
    let responses = [a?, b?];
    let statuses = [responses[0].status().as_u16(), responses[1].status().as_u16()];
    ensure!(
        statuses.iter().filter(|s| **s == 201).count() == 1 && statuses.iter().filter(|s| **s == 409).count() == 1,
        "lease race must have exactly one winner: {statuses:?}"
    );
    let winner = usize::from(statuses[1] == 201);
    blob.lease = contenders[winner].clone();
    blob.commit(mode, b"accepted").await?;
    let request = match mode {
        Mode::Append => blob.request(Method::PUT, &[("comp", "appendblock")]),
        Mode::Block => blob.request(
            Method::PUT,
            &[
                ("comp", "block"),
                ("blockid", &STANDARD.encode((blob.blocks.len() as u64).to_be_bytes())),
            ],
        ),
    };
    let stale = request
        .header("x-ms-lease-id", &contenders[1 - winner])
        .body("rejected")
        .send()
        .await?;
    ensure!(
        stale.status() == StatusCode::PRECONDITION_FAILED,
        "losing writer unexpectedly accepted: {}",
        stale.status()
    );
    let read = checked(blob.request(Method::GET, &[]).send().await?, StatusCode::OK)
        .await?
        .bytes()
        .await?;
    ensure!(read.as_ref() == b"accepted", "losing writer changed committed data");
    blob.release().await?;
    println!(
        "{}",
        json!({"kind":"fence", "lease_acquire_statuses":statuses, "loser_write_status":412, "read_back_passed":true, "passed":true})
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cumulative_list_retains_old_blocks_and_has_uniform_ids() {
        let ids: Vec<_> = (0_u64..3).map(|id| STANDARD.encode(id.to_be_bytes())).collect();
        let xml = block_list(&ids);
        assert_eq!(xml.matches("<Latest>").count(), 3);
        assert!(ids.iter().all(|id| id.len() == ids[0].len() && xml.contains(id)));
        assert!(xml.ends_with("</BlockList>"));
    }
}
