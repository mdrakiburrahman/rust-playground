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
use arrowcol_flight::{PutResultMetadata, decode_ddl_completion_metadata};
use arrowcol_protocol::{
    ClaimedSchemaFingerprint, ImplementationPool, IngestMetadata, ProtocolVersion, TableIdentity, fingerprint_schema,
};
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message as _;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};

const SCHEMA_DDL: &str = include_str!("../../../projects/loadtest/schemas/sqlserver.sql");
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

pub struct DmvTable {
    ddl: String,
    identity: TableIdentity,
    schema: SchemaRef,
}

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
pub async fn send(endpoint: &str, client_id: &str, rows: usize) -> Result<usize> {
    send_with_concurrency(endpoint, client_id, rows, 1).await
}

/// Registers DMV tables and uploads one Arrow batch per table with bounded concurrency.
pub async fn send_with_concurrency(endpoint: &str, client_id: &str, rows: usize, concurrency: usize) -> Result<usize> {
    send_tables_with_concurrency(endpoint, client_id, rows, concurrency, tables()?).await
}

/// Registers and uploads one named DMV table for a replica-pinned OAT workload.
pub async fn send_table_with_concurrency(
    endpoint: &str,
    client_id: &str,
    table_name: &str,
    rows: usize,
    concurrency: usize,
) -> Result<usize> {
    send_tables_with_concurrency(endpoint, client_id, rows, concurrency, vec![table_named(table_name)?]).await
}

fn table_named(table_name: &str) -> Result<DmvTable> {
    tables()?
        .into_iter()
        .find(|table| table.identity.table().eq_ignore_ascii_case(table_name))
        .with_context(|| format!("unknown DMV table {table_name:?}"))
}

async fn send_tables_with_concurrency(
    endpoint: &str,
    client_id: &str,
    rows: usize,
    concurrency: usize,
    tables: Vec<DmvTable>,
) -> Result<usize> {
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
        let version = register(&mut client, &table.ddl)
            .await
            .with_context(|| format!("register {}", table.identity))?;
        let batch = batch(&table, client_id, rows, now_micros)?;
        uploads.push((table, version, batch));
    }

    let upload_client = FlightServiceClient::new(channel);
    stream::iter(uploads.into_iter().map(|(table, version, batch)| {
        let mut client = upload_client.clone();
        async move {
            put(&mut client, &table, version, batch)
                .await
                .with_context(|| format!("put {}", table.identity))
        }
    }))
    .buffer_unordered(concurrency.min(count))
    .try_collect::<Vec<_>>()
    .await?;
    Ok(count)
}
async fn register(client: &mut FlightServiceClient<Channel>, ddl: &str) -> Result<String> {
    let command = CommandStatementUpdate {
        query: ddl.to_owned(),
        transaction_id: None,
    };
    let descriptor = FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        cmd: command.as_any().encode_to_vec().into(),
        path: Vec::new(),
    };
    let response = client
        .do_put(Request::new(stream::iter(vec![FlightData {
            flight_descriptor: Some(descriptor),
            ..FlightData::default()
        }])))
        .await?;
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
) -> Result<()> {
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
    let metadata = IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Dataflow,
        identity.clone(),
        ClaimedSchemaFingerprint::new(fingerprint_schema(table.schema.as_ref())),
    );
    for (key, value) in metadata.to_ascii_pairs() {
        request
            .metadata_mut()
            .insert(key, MetadataValue::try_from(value.as_str())?);
    }
    let mut results = client.do_put(request).await?.into_inner();
    let mut admitted = false;
    while let Some(result) = results.message().await? {
        match PutResultMetadata::from_put_result(&result)? {
            PutResultMetadata::Progress { .. } => {}
            PutResultMetadata::BestEffortAdmission { .. } | PutResultMetadata::DurableAdmission { .. } => {
                admitted = true;
            }
        }
    }
    if !admitted {
        bail!("collector closed the put stream without admitting the batch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_a_batch_for_every_dmv_table_tagged_with_the_client() -> Result<()> {
        let tables = tables()?;
        assert_eq!(tables.len(), 13);
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
        let table = table_named("DMV_WAIT_STATS")?;
        assert_eq!(table.identity.table(), "dmv_wait_stats");
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
