use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt8Array,
};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementUpdate, ProstMessageExt};
use arrow_flight::{FlightData, FlightDescriptor};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};
use arrowcol_catalog::{DdlCommand, DdlParser};
use arrowcol_flight::{DeliveryGuarantee, PutResultMetadata, decode_ddl_completion_metadata};
const OAT_RUN_ID_ENV: &str = "ARROWCOL_OAT_RUN_ID";
const OAT_RUN_ID_HEADER: &str = "x-arrowcol-oat-run-id";
use arrowcol_protocol::{
    ClaimedSchemaFingerprint, ImplementationPool, IngestMetadata, ProtocolVersion, TableIdentity, UnixMillis,
    fingerprint_schema,
};
use arrowcol_router_rust::{EdgeRequestSigner, insert_edge_signature_headers, read_secret_file};
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message as _;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

#[path = "dmv_client.rs"]
pub mod client;

const SCHEMA_DDL: &str = include_str!("../../../projects/loadtest/schemas/sqlserver.sql");
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

pub struct DmvTable {
    ddl: String,
    identity: TableIdentity,
    schema: SchemaRef,
}

impl DmvTable {
    #[must_use]
    pub fn identity(&self) -> &TableIdentity {
        &self.identity
    }
}

/// Returns the names of the DMV tables in the embedded fixture.
///
/// # Errors
///
/// Returns an error if the fixture cannot be parsed into Arrow schemas.
pub fn table_names() -> Result<Vec<String>> {
    Ok(tables()?
        .into_iter()
        .map(|table| table.identity.table().as_str().to_owned())
        .collect())
}

/// Parses the embedded DMV DDL fixture into table schemas.
///
/// # Errors
///
/// Returns an error if a statement is invalid, is not `CREATE TABLE`, or contains an unsupported field type.
pub fn tables() -> Result<Vec<DmvTable>> {
    let ddl = SCHEMA_DDL
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let parser = DdlParser::default();
    let mut tables = Vec::new();
    for statement in ddl.split(';').map(str::trim).filter(|statement| !statement.is_empty()) {
        let DdlCommand::CreateTable(create) = parser.parse(statement)? else {
            bail!("DMV fixture contains a non-CREATE statement");
        };
        let fields = create
            .fields()
            .iter()
            .map(|field| field.to_arrow())
            .collect::<Result<Vec<_>, _>>()?;
        tables.push(DmvTable {
            ddl: statement.to_owned(),
            identity: create.identity().clone(),
            schema: Arc::new(Schema::new(fields)),
        });
    }
    Ok(tables)
}

/// Builds an Arrow batch matching one DMV table's schema.
///
/// # Errors
///
/// Returns an error if the schema contains an unsupported type or Arrow rejects the batch.
pub fn batch(table: &DmvTable, client_id: &str, rows: usize, now_micros: i64) -> Result<RecordBatch> {
    let columns = table
        .schema
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef> {
            let name = field.name();
            Ok(match field.data_type() {
                DataType::Utf8 if name == "customer_id" => {
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(client_id, rows)))
                }
                DataType::Utf8 => Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|row| format!("{name}-{row}")),
                )),
                DataType::Boolean => Arc::new(BooleanArray::from_iter((0..rows).map(|row| Some(row % 2 == 0)))),
                DataType::UInt8 => Arc::new(UInt8Array::from_iter_values((0..rows).map(|row| (row % 255) as u8))),
                DataType::Int16 => Arc::new(Int16Array::from_iter_values((0..rows).map(|row| (row % 32_767) as i16))),
                DataType::Int32 => Arc::new(Int32Array::from_iter_values((0..rows).map(|row| row as i32))),
                DataType::Int64 => Arc::new(Int64Array::from_iter_values((0..rows).map(|row| row as i64))),
                DataType::Float64 => Arc::new(Float64Array::from_iter_values((0..rows).map(|row| row as f64 / 10.0))),
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
                    TimestampMicrosecondArray::from_iter_values((0..rows).map(|row| now_micros + row as i64))
                        .with_timezone_opt(timezone.clone()),
                ),
                other => bail!("unsupported DMV type {other} for {}.{name}", table.identity),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(&table.schema), columns)?)
}

/// Registers every DMV table on the collector and puts one batch per table; returns the table count.
///
/// # Errors
///
/// Returns an error if the fixture is invalid or collector registration or upload fails.
pub async fn send(endpoint: &str, client_id: &str, rows: usize) -> Result<usize> {
    send_with_concurrency(endpoint, client_id, rows, 1).await
}

/// Registers DMV tables and uploads one Arrow batch per table with bounded concurrency.
///
/// # Errors
///
/// Returns an error for invalid row or concurrency counts, invalid fixture DDL, or collector connection or upload failures.
pub async fn send_with_concurrency(endpoint: &str, client_id: &str, rows: usize, concurrency: usize) -> Result<usize> {
    send_tables_with_concurrency(endpoint, client_id, rows, concurrency, tables()?, None).await
}

/// Registers and uploads one named DMV table for a replica-pinned OAT workload.
///
/// # Errors
///
/// Returns an error if the table name is unknown, arguments are invalid, or collector connection or upload fails.
pub async fn send_table_with_concurrency(
    endpoint: &str,
    client_id: &str,
    table_name: &str,
    rows: usize,
    concurrency: usize,
) -> Result<usize> {
    send_tables_with_concurrency(
        endpoint,
        client_id,
        rows,
        concurrency,
        vec![table_named(table_name)?],
        None,
    )
    .await
}

/// Uploads DMV data through a trusted-edge router, or directly when no key file is supplied.
///
/// # Errors
///
/// Rejects unreadable/short keys, invalid input, and unsuccessful registration or admission.
pub async fn send_authenticated(
    endpoint: &str,
    client_id: &str,
    table_name: Option<&str>,
    rows: usize,
    concurrency: usize,
    edge_secret_file: Option<&std::path::Path>,
) -> Result<usize> {
    Ok(
        send_authenticated_report(endpoint, client_id, table_name, rows, concurrency, edge_secret_file)
            .await?
            .tables,
    )
}

/// Observed admission completions, not proof of downstream Delta delivery.
#[derive(Clone, Copy, Debug, Default)]
pub struct UploadReport {
    /// Tables in this successfully completed upload call.
    pub tables: usize,
    /// Streams completed with a best-effort admission acknowledgement.
    pub best_effort_streams: usize,
    /// Streams completed with a local durable-buffer acknowledgement.
    pub durable_streams: usize,
    /// Rows whose streams completed with a validated admission response.
    pub completed_rows: u64,
}

/// Uploads DMV data and preserves the actual admission guarantees.
///
/// # Errors
///
/// Returns the same validation, transport and admission failures as `send_authenticated`.
pub async fn send_authenticated_report(
    endpoint: &str,
    client_id: &str,
    table_name: Option<&str>,
    rows: usize,
    concurrency: usize,
    edge_secret_file: Option<&std::path::Path>,
) -> Result<UploadReport> {
    let signer = edge_secret_file.map(edge_signer_from_file).transpose()?;
    let tables = match table_name {
        Some(name) => vec![table_named(name)?],
        None => tables()?,
    };
    send_tables_report(endpoint, client_id, rows, concurrency, tables, signer).await
}

fn edge_signer_from_file(path: &std::path::Path) -> Result<EdgeRequestSigner> {
    let secret = read_secret_file(path).context("read trusted-edge key file")?;
    EdgeRequestSigner::new(secret).map_err(anyhow::Error::from)
}

fn table_named(table_name: &str) -> Result<DmvTable> {
    tables()?
        .into_iter()
        .find(|table| table.identity.table().as_str().eq_ignore_ascii_case(table_name))
        .with_context(|| format!("unknown DMV table {table_name:?}"))
}

async fn send_tables_with_concurrency(
    endpoint: &str,
    client_id: &str,
    rows: usize,
    concurrency: usize,
    tables: Vec<DmvTable>,
    signer: Option<EdgeRequestSigner>,
) -> Result<usize> {
    Ok(
        send_tables_report(endpoint, client_id, rows, concurrency, tables, signer)
            .await?
            .tables,
    )
}

async fn send_tables_report(
    endpoint: &str,
    client_id: &str,
    rows: usize,
    concurrency: usize,
    tables: Vec<DmvTable>,
    signer: Option<EdgeRequestSigner>,
) -> Result<UploadReport> {
    if rows == 0 {
        bail!("rows must be positive");
    }
    if concurrency == 0 {
        bail!("concurrency must be positive");
    }
    let channel = Endpoint::from_shared(endpoint.to_owned())?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .connect()
        .await
        .with_context(|| format!("connect {endpoint}"))?;
    let mut client = FlightServiceClient::new(channel.clone());
    let now_micros = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    let count = tables.len();
    let mut uploads = Vec::with_capacity(count);
    for table in tables {
        let version = register(&mut client, &table, signer.as_ref())
            .await
            .with_context(|| format!("register {}", table.identity))?;
        let batch = batch(&table, client_id, rows, now_micros)?;
        let table = Arc::new(table);
        for (offset, length) in batch_slices(rows, concurrency) {
            uploads.push((Arc::clone(&table), version.clone(), batch.slice(offset, length)));
        }
    }

    let admissions = stream::iter(uploads.into_iter().map(|(table, version, batch)| {
        let signer = signer.clone();
        async move {
            // Separate connections let Linux SO_REUSEPORT distribute streams across DFE cores.
            let channel = Endpoint::from_shared(endpoint.to_owned())?
                .connect_timeout(RPC_TIMEOUT)
                .timeout(RPC_TIMEOUT)
                .connect()
                .await?;
            let mut client = FlightServiceClient::new(channel);
            put(&mut client, &table, version, batch, signer.as_ref())
                .await
                .with_context(|| format!("put {}", table.identity))
        }
    }))
    .buffer_unordered(concurrency)
    .try_collect::<Vec<_>>()
    .await?;
    Ok(UploadReport {
        tables: count,
        completed_rows: rows as u64 * count as u64,
        best_effort_streams: admissions
            .iter()
            .filter(|value| **value == DeliveryGuarantee::BestEffort)
            .count(),
        durable_streams: admissions
            .iter()
            .filter(|value| **value == DeliveryGuarantee::DurableAdmission)
            .count(),
    })
}
fn batch_slices(rows: usize, concurrency: usize) -> Vec<(usize, usize)> {
    let streams = concurrency.min(rows);
    let size = rows.div_ceil(streams);
    (0..rows)
        .step_by(size)
        .map(|offset| (offset, size.min(rows - offset)))
        .collect()
}

fn request_metadata<T>(request: &mut Request<T>, table: &DmvTable, signer: Option<&EdgeRequestSigner>) -> Result<()> {
    let metadata = IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Dataflow,
        table.identity.clone(),
        ClaimedSchemaFingerprint::new(fingerprint_schema(table.schema.as_ref())),
    );
    let mut headers = http::HeaderMap::new();
    for (key, value) in metadata.to_ascii_pairs() {
        headers.insert(
            http::header::HeaderName::from_static(key),
            http::HeaderValue::try_from(value)?,
        );
    }
    if let Some(run_id) = std::env::var_os(OAT_RUN_ID_ENV) {
        let run_id = run_id.to_str().context("ARROWCOL_OAT_RUN_ID must be valid Unicode")?;
        validate_oat_run_id(run_id)?;
        headers.insert(
            http::header::HeaderName::from_static(OAT_RUN_ID_HEADER),
            http::HeaderValue::try_from(run_id)?,
        );
    }
    // Tonic sets this exact content type on the wire, after constructing the request.
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/grpc"),
    );
    if let Some(signer) = signer {
        insert_edge_signature_headers(
            &mut headers,
            signer,
            &http::Method::POST,
            "/arrow.flight.protocol.FlightService/DoPut",
            UnixMillis::new(u64::try_from(
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            )?)?,
            &uuid::Uuid::new_v4().to_string(),
        )?;
    }
    *request.metadata_mut() = tonic::metadata::MetadataMap::from_headers(headers);
    Ok(())
}

fn validate_oat_run_id(run_id: &str) -> Result<()> {
    if run_id.is_empty()
        || run_id.len() > 64
        || !run_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("{OAT_RUN_ID_ENV} must contain 1..=64 ASCII letters, digits, hyphens, or underscores");
    }
    Ok(())
}

async fn register(
    client: &mut FlightServiceClient<Channel>,
    table: &DmvTable,
    signer: Option<&EdgeRequestSigner>,
) -> Result<String> {
    let command = CommandStatementUpdate {
        query: table.ddl.replacen("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1),
        transaction_id: None,
    };
    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        cmd: command.as_any().encode_to_vec().into(),
        path: Vec::new(),
    };
    let mut request = Request::new(stream::iter(vec![FlightData {
        flight_descriptor: Some(descriptor),
        ..FlightData::default()
    }]));
    request_metadata(&mut request, table, signer)?;
    let response = client.do_put(request).await?;
    let completion = decode_ddl_completion_metadata(response.metadata())?;
    let mut results = response.into_inner();
    while results.message().await?.is_some() {}
    Ok(format!("v{}", completion.schema_version()))
}

async fn put(
    client: &mut FlightServiceClient<Channel>,
    table: &DmvTable,
    version: String,
    batch: RecordBatch,
    signer: Option<&EdgeRequestSigner>,
) -> Result<DeliveryGuarantee> {
    let identity = &table.identity;
    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Path as i32,
        cmd: Default::default(),
        path: vec![
            identity.catalog().to_string(),
            identity.schema().to_string(),
            identity.table().to_string(),
            version,
        ],
    };
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(Arc::clone(&table.schema))
        .with_flight_descriptor(Some(descriptor))
        .build(stream::iter([Ok::<RecordBatch, FlightError>(batch)]))
        .try_collect::<Vec<_>>()
        .await?;
    let mut request = Request::new(stream::iter(encoded));
    request_metadata(&mut request, table, signer)?;
    completed_admission(client.do_put(request).await?.into_inner()).await
}

async fn completed_admission(
    mut results: impl futures::Stream<Item = std::result::Result<arrow_flight::PutResult, tonic::Status>> + Unpin,
) -> Result<DeliveryGuarantee> {
    let mut admitted = None;
    while let Some(result) = results.next().await {
        let result = result?;
        match PutResultMetadata::from_put_result(&result)? {
            PutResultMetadata::Progress { .. } => {}
            PutResultMetadata::BestEffortAdmission { .. } => admitted = Some(DeliveryGuarantee::BestEffort),
            PutResultMetadata::DurableAdmission { .. } => admitted = Some(DeliveryGuarantee::DurableAdmission),
        }
    }
    admitted.context("collector closed the put stream without admitting the batch")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn completion_stream_preserves_admission_kind_and_propagates_terminal_failure() -> Result<()> {
        use arrowcol_catalog::SchemaVersion;
        use arrowcol_flight::{StreamCompletion, StreamSummary};
        for (completion, expected) in [
            (StreamCompletion::best_effort(), DeliveryGuarantee::BestEffort),
            (
                StreamCompletion::durable_admission(),
                DeliveryGuarantee::DurableAdmission,
            ),
        ] {
            let metadata = PutResultMetadata::completion(
                completion,
                SchemaVersion::FIRST,
                SchemaVersion::FIRST,
                fingerprint_schema(&Schema::empty()),
                StreamSummary::new(1, 1, 1),
            )
            .context("completion missing")?
            .into_put_result()?;
            assert_eq!(
                completed_admission(stream::iter([Ok(metadata.clone())])).await?,
                expected
            );
            assert!(
                completed_admission(stream::iter([
                    Ok(metadata),
                    Err(tonic::Status::unavailable("terminal failure")),
                ]))
                .await
                .is_err()
            );
        }
        assert!(completed_admission(stream::empty()).await.is_err());
        Ok(())
    }

    #[test]
    fn splits_one_table_without_losing_or_duplicating_rows() {
        assert_eq!(batch_slices(7, 3), vec![(0, 3), (3, 3), (6, 1)]);
        assert_eq!(batch_slices(2, 8), vec![(0, 1), (1, 1)]);
    }

    #[test]
    fn router_verifies_ddl_and_ingest_metadata_and_rejects_replay() -> Result<()> {
        let table = table_named("sqlserver_wait_stats")?;
        let key = vec![42; 32];
        let signer = EdgeRequestSigner::new(key.clone())?;
        let verifier = arrowcol_router_rust::EdgeVerifier::new(key, 30_000)?;
        for _ in 0..2 {
            let mut request = Request::new(());
            request_metadata(&mut request, &table, Some(&signer))?;
            let headers = request.metadata().clone().into_headers();
            let now = UnixMillis::new(u64::try_from(
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            )?)?;
            let verify = || {
                verifier.verify(
                    &http::Method::POST,
                    "/arrow.flight.protocol.FlightService/DoPut",
                    &headers,
                    now,
                )
            };
            verify()?;
            assert!(verify().is_err());
            assert_eq!(headers["x-arrowcol-table"], "sqlserver_wait_stats");
        }
        Ok(())
    }

    #[test]
    fn client_signs_with_the_router_key_from_a_newline_terminated_file() -> Result<()> {
        let table = table_named("sqlserver_wait_stats")?;
        let directory = std::env::temp_dir().join(format!("arrowcol-dmv-edge-key-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory)?;
        let key_file = directory.join("edge-hmac");
        std::fs::write(&key_file, format!("{}\n", "k".repeat(64)))?;
        let signer = edge_signer_from_file(&key_file);
        let router_key = read_secret_file(&key_file);
        std::fs::remove_dir_all(&directory)?;
        let verifier = arrowcol_router_rust::EdgeVerifier::new(router_key?, 30_000)?;
        let mut request = Request::new(());
        request_metadata(&mut request, &table, Some(&signer?))?;
        let now = UnixMillis::new(u64::try_from(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        )?)?;
        verifier.verify(
            &http::Method::POST,
            "/arrow.flight.protocol.FlightService/DoPut",
            &request.metadata().clone().into_headers(),
            now,
        )?;
        Ok(())
    }

    #[test]
    fn builds_a_batch_for_every_dmv_table_tagged_with_the_client() -> Result<()> {
        let tables = tables()?;
        assert_eq!(tables.len(), 32);
        for table in &tables {
            let batch = batch(table, "client-a", 3, 1)?;
            let customers = batch
                .column_by_name("customer_id")
                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                .context("customer_id column")?;
            assert!(customers.iter().all(|value| value == Some("client-a")));
            assert_eq!(batch.num_rows(), 3);
        }
        Ok(())
    }

    #[test]
    fn selects_a_single_dmv_table_case_insensitively() -> Result<()> {
        let table = table_named("SQLSERVER_WAIT_STATS")?;
        assert_eq!(table.identity.table().as_str(), "sqlserver_wait_stats");
        assert!(table_named("not_a_dmv_table").is_err());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_zero_rows_and_concurrency_before_connecting() -> Result<()> {
        let rows_error = send_with_concurrency("not a url", "client-a", 0, 1)
            .await
            .err()
            .context("zero rows should fail")?;
        assert!(rows_error.to_string().contains("rows must be positive"));

        let concurrency_error = send_with_concurrency("not a url", "client-a", 1, 0)
            .await
            .err()
            .context("zero concurrency should fail")?;
        assert!(concurrency_error.to_string().contains("concurrency must be positive"));
        Ok(())
    }
}
