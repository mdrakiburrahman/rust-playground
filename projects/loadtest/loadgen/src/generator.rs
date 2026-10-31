use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampNanosecondArray, UInt8Array,
    },
    datatypes::{DataType, TimeUnit},
};
use arrow_flight::{FlightData, encode::FlightDataEncoderBuilder, error::FlightError};
use futures::{TryStreamExt, stream};
use sha2::{Digest, Sha256};

use crate::sdk::GeneratedTable;

const MAX_FLIGHT_DATA_BYTES: usize = 31 * 1024 * 1024;
/// Interleaved tenants deliberately provide an unsorted input for layout tests.
pub const CUSTOMER_COUNT: usize = 16;

/// One immutable logical batch reused by paired pool sends and retries.
#[derive(Clone)]
pub struct LogicalBatch {
    pub table: Arc<GeneratedTable>,
    pub record_batch: RecordBatch,
    pub encoded: Arc<Vec<FlightData>>,
    pub rows: usize,
    pub arrow_bytes: u64,
    pub payload_sha256: String,
    pub row_sha256: String,
}

/// Global deterministic table scheduler shared by all lanes.
pub struct Scheduler {
    tables: Arc<Vec<Arc<GeneratedTable>>>,
    next_ordinal: AtomicU64,
    first_table: usize,
}

impl Scheduler {
    /// Creates an equal round-robin scheduler whose first table is seed-stable.
    ///
    /// # Errors
    ///
    /// Rejects an empty table set.
    pub fn new(tables: Vec<Arc<GeneratedTable>>, seed: u64) -> Result<Self> {
        if tables.is_empty() {
            bail!("scheduler requires at least one table");
        }
        let first_table = usize::try_from(seed % tables.len() as u64).context("scheduler table index")?;
        Ok(Self {
            tables: Arc::new(tables),
            next_ordinal: AtomicU64::new(0),
            first_table,
        })
    }

    /// Allocates the next globally ordered table.
    #[must_use]
    pub fn next(&self) -> ScheduledTable {
        let ordinal = self.next_ordinal.fetch_add(1, Ordering::SeqCst);
        let table_index = (self.first_table + ordinal as usize % self.tables.len()) % self.tables.len();
        ScheduledTable {
            ordinal,
            table_batch_ordinal: ordinal / self.tables.len() as u64,
            table: Arc::clone(&self.tables[table_index]),
        }
    }

    #[must_use]
    pub fn scheduled(&self) -> u64 {
        self.next_ordinal.load(Ordering::SeqCst)
    }
}

/// Scheduler output before row construction.
pub struct ScheduledTable {
    pub ordinal: u64,
    pub table_batch_ordinal: u64,
    pub table: Arc<GeneratedTable>,
}

/// Builds one exact-schema batch from the generated SDK descriptor schema.
///
/// Customer keys interleave across rows; other fields retain the deterministic
/// seed-derived corpus. Retries and paired pools reuse the encoded batch.
///
/// # Errors
///
/// Rejects unsupported or nullable generated schema fields and Arrow/IPC errors.
pub async fn build_logical_batch(table: Arc<GeneratedTable>, rows: usize, seed: u64) -> Result<LogicalBatch> {
    if rows == 0 {
        bail!("batch row count must be positive");
    }
    let seed = i64::try_from(seed).context("seed exceeds signed deterministic corpus range")?;
    let mut columns = Vec::with_capacity(table.schema.fields().len());
    let mut row_hasher = Sha256::new();
    row_hasher.update(table.identity.to_string().as_bytes());
    row_hasher.update(seed.to_be_bytes());
    for (index, field) in table.schema.fields().iter().enumerate() {
        if field.is_nullable() {
            bail!(
                "generated synthetic schema field {} is unexpectedly nullable",
                field.name()
            );
        }
        let offset = i64::try_from(index + 1).context("field offset overflow")?;
        let column = deterministic_column(
            table.identity.table().as_str(),
            field.name(),
            field.data_type(),
            rows,
            seed,
            offset,
            &mut row_hasher,
        )?;
        columns.push(column);
    }
    let record_batch = RecordBatch::try_new(Arc::clone(&table.schema), columns)?;
    let arrow_bytes = u64::try_from(record_batch.get_array_memory_size()).context("Arrow byte count overflow")?;
    let (encoded, payload_sha256) = encode_record_batch(&table, &record_batch).await?;
    Ok(LogicalBatch {
        table,
        record_batch,
        encoded: Arc::new(encoded),
        rows,
        arrow_bytes,
        payload_sha256,
        row_sha256: format!("sha256:{}", hex::encode(row_hasher.finalize())),
    })
}

/// Encodes a batch with the generated SDK manifest's descriptor path.
///
/// # Errors
///
/// Returns Arrow Flight IPC encoding failures.
pub async fn encode_record_batch(
    table: &GeneratedTable,
    record_batch: &RecordBatch,
) -> Result<(Vec<FlightData>, String)> {
    let descriptor = arrow_flight::FlightDescriptor::new_path(table.descriptor_path.clone());
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(record_batch.schema())
        .with_flight_descriptor(Some(descriptor))
        .with_max_flight_data_size(MAX_FLIGHT_DATA_BYTES)
        .build(stream::iter(vec![Ok::<_, FlightError>(record_batch.clone())]))
        .try_collect::<Vec<_>>()
        .await?;
    let mut payload_hasher = Sha256::new();
    for message in &encoded {
        hash_bytes(&mut payload_hasher, &message.data_header);
        hash_bytes(&mut payload_hasher, &message.data_body);
        hash_bytes(&mut payload_hasher, &message.app_metadata);
    }
    Ok((encoded, format!("sha256:{}", hex::encode(payload_hasher.finalize()))))
}

#[allow(clippy::too_many_arguments)]
fn deterministic_column(
    table: &str,
    name: &str,
    data_type: &DataType,
    rows: usize,
    seed: i64,
    offset: i64,
    row_hasher: &mut Sha256,
) -> Result<ArrayRef> {
    row_hasher.update(name.as_bytes());
    row_hasher.update(data_type.to_string().as_bytes());
    let array: ArrayRef = match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, timezone)
            if timezone.as_deref().is_some_and(|timezone| timezone == "UTC") =>
        {
            let value = seed
                .checked_add(offset)
                .and_then(|seconds| seconds.checked_mul(1_000_000_000))
                .context("deterministic timestamp overflow")?;
            row_hasher.update(value.to_be_bytes());
            Arc::new(TimestampNanosecondArray::from_value(value, rows).with_timezone("UTC"))
        }
        DataType::Utf8 if name == "customer_id" => {
            let values = (0..rows)
                .map(|row| format!("customer-{:04}", row % CUSTOMER_COUNT))
                .collect::<Vec<_>>();
            for value in &values {
                hash_bytes(row_hasher, value.as_bytes());
            }
            Arc::new(StringArray::from_iter_values(values))
        }
        DataType::Utf8 => {
            let value = if is_guid(table, name) {
                deterministic_guid(table, name, seed)
            } else {
                format!("{table}:{name}:fallback:{seed}")
            };
            row_hasher.update(value.as_bytes());
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(value.as_str(), rows)))
        }
        DataType::Boolean => {
            let value = seed.checked_add(offset).context("deterministic boolean overflow")? & 1 == 0;
            row_hasher.update([u8::from(value)]);
            Arc::new(BooleanArray::from(vec![value; rows]))
        }
        DataType::UInt8 => {
            let value = u8::try_from(seed.checked_add(offset).context("deterministic uint8 overflow")? % 255)
                .context("deterministic uint8 conversion")?;
            row_hasher.update([value]);
            Arc::new(UInt8Array::from(vec![value; rows]))
        }
        DataType::Int16 => {
            let value = i16::try_from(seed.checked_add(offset).context("deterministic int16 overflow")? % 32_767)
                .context("deterministic int16 conversion")?;
            row_hasher.update(value.to_be_bytes());
            Arc::new(Int16Array::from(vec![value; rows]))
        }
        DataType::Int32 => {
            let value =
                i32::try_from(seed.checked_add(offset).context("deterministic int32 overflow")? % i64::from(i32::MAX))
                    .context("deterministic int32 conversion")?;
            row_hasher.update(value.to_be_bytes());
            Arc::new(Int32Array::from(vec![value; rows]))
        }
        DataType::Int64 => {
            let value = seed
                .checked_mul(1_000)
                .and_then(|value| value.checked_add(offset))
                .context("deterministic int64 overflow")?;
            row_hasher.update(value.to_be_bytes());
            Arc::new(Int64Array::from(vec![value; rows]))
        }
        DataType::Float64 => {
            let value = if name == "cntr_value" {
                (seed % 1_000) as f64 + offset as f64 + 0.5
            } else {
                ((seed % 1_000) as f64 + offset as f64) / 10.0
            };
            row_hasher.update(value.to_bits().to_be_bytes());
            Arc::new(Float64Array::from(vec![value; rows]))
        }
        other => bail!("unsupported synthetic generated type {other} for {table}.{name}"),
    };
    Ok(array)
}

fn is_guid(table: &str, field: &str) -> bool {
    matches!(
        (table, field),
        ("sqlserver_active_sessions", "connection_id")
            | ("sqlserver_availability_group_states", "availability_group_id")
            | ("sqlserver_availability_replica_states", "availability_group_id")
            | ("sqlserver_database_replica_states", "availability_group_id")
            | ("sqlserver_database_replica_states", "replica_id")
            | ("sqlserver_database_replica_states", "group_database_id")
    )
}

fn deterministic_guid(table: &str, field: &str, seed: i64) -> String {
    let digest = Sha256::digest(format!("{table}:{field}:{seed}").as_bytes());
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        digest[3],
        digest[2],
        digest[1],
        digest[0],
        digest[5],
        digest[4],
        digest[7],
        digest[6],
        digest[8],
        digest[9],
        digest[10],
        digest[11],
        digest[12],
        digest[13],
        digest[14],
        digest[15],
    )
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};
    use arrow_flight::decode::FlightRecordBatchStream;
    use arrowcol_catalog::{DdlCommand, DdlParser};
    use arrowcol_protocol::{CatalogName, SchemaName, TableIdentity, TableName};
    use prost::Message;

    use super::*;
    use crate::schema::schema_statements;
    use crate::sdk::GeneratedTable;

    fn generated_table() -> Result<Arc<GeneratedTable>> {
        Ok(Arc::new(GeneratedTable {
            identity: TableIdentity::new(
                CatalogName::new("telemetry")?,
                SchemaName::new("sqlserver")?,
                TableName::new("sqlserver_active_sessions")?,
            ),
            schema_version: 1,
            fingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            descriptor_path: vec![
                "telemetry".to_owned(),
                "sqlserver".to_owned(),
                "sqlserver_active_sessions".to_owned(),
                "v1".to_owned(),
            ],
            schema: Arc::new(Schema::new(vec![
                Field::new("customer_id", DataType::Utf8, false),
                Field::new(
                    "sample_time_utc",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    false,
                ),
                Field::new("database_id", DataType::Int16, false),
                Field::new("connection_id", DataType::Utf8, false),
            ])),
            generated_root: "target/generated".into(),
        }))
    }

    #[tokio::test]
    async fn identical_seed_and_schema_produce_identical_batches() -> Result<()> {
        let table = generated_table()?;
        let first = build_logical_batch(Arc::clone(&table), 4, 7).await?;
        let second = build_logical_batch(table, 4, 7).await?;
        assert_eq!(first.record_batch, second.record_batch);
        assert_eq!(first.payload_sha256, second.payload_sha256);
        assert_eq!(first.row_sha256, second.row_sha256);
        Ok(())
    }

    #[tokio::test]
    async fn customer_workload_is_interleaved_reproducible_and_covers_all_customers() -> Result<()> {
        let batch = build_logical_batch(generated_table()?, CUSTOMER_COUNT * 3 + 1, 7).await?;
        let keys = batch
            .record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("customer keys are not strings")?;
        for row in 0..keys.len() {
            assert_eq!(keys.value(row), format!("customer-{:04}", row % CUSTOMER_COUNT));
        }
        assert_eq!(keys.value(0), keys.value(CUSTOMER_COUNT));
        let shorter = build_logical_batch(generated_table()?, CUSTOMER_COUNT, 7).await?;
        assert_ne!(batch.payload_sha256, shorter.payload_sha256);
        assert_ne!(batch.row_sha256, shorter.row_sha256);
        Ok(())
    }

    #[tokio::test]
    async fn default_schema_batches_stay_one_flight_record_batch() -> Result<()> {
        const LOADTEST_MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

        let parser = DdlParser::default();
        for statement in schema_statements()? {
            let DdlCommand::CreateTable(create) = parser.parse(&statement.ddl)? else {
                unreachable!("schema fixture accepts only CREATE TABLE");
            };
            let schema = Arc::new(Schema::new(
                create
                    .fields()
                    .iter()
                    .map(arrowcol_catalog::CatalogField::to_arrow)
                    .collect::<Result<Vec<_>, _>>()?,
            ));
            let table = Arc::new(GeneratedTable {
                identity: statement.identity.clone(),
                schema_version: 1,
                fingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                descriptor_path: vec![
                    statement.identity.catalog().to_string(),
                    statement.identity.schema().to_string(),
                    statement.identity.table().to_string(),
                    "v1".to_owned(),
                ],
                schema,
                generated_root: "target/generated".into(),
            });
            let batch = build_logical_batch(table, 8_192, 7).await?;

            for offset in (CUSTOMER_COUNT..batch.rows).step_by(CUSTOMER_COUNT) {
                assert_eq!(
                    batch.record_batch.slice(offset, CUSTOMER_COUNT),
                    batch.record_batch.slice(0, CUSTOMER_COUNT),
                    "{} complete payload rows must repeat every 16 customers, not introduce 8192 distinct rows",
                    statement.identity
                );
            }
            assert_eq!(
                batch.encoded.len(),
                2,
                "{} was fragmented into multiple Flight record batches",
                statement.identity
            );
            assert!(
                batch
                    .encoded
                    .iter()
                    .all(|message| message.encoded_len() <= LOADTEST_MAX_MESSAGE_BYTES),
                "{} exceeded the collector FlightData message limit",
                statement.identity
            );
            assert!(
                batch.arrow_bytes <= LOADTEST_MAX_MESSAGE_BYTES as u64,
                "{} exceeded the collector decoded-batch limit",
                statement.identity
            );
            let encoded = batch.encoded.as_ref().clone();
            let decoded = FlightRecordBatchStream::new_from_flight_data(stream::iter(
                encoded.into_iter().map(Ok::<_, FlightError>),
            ))
            .try_collect::<Vec<_>>()
            .await?;
            assert_eq!(decoded.len(), 1);
            let decoded_bytes = arrow_deltalake::record_batch_logical_bytes(&decoded[0])?;
            assert!(
                decoded_bytes <= LOADTEST_MAX_MESSAGE_BYTES as u64,
                "{} decoded to {decoded_bytes} logical bytes",
                statement.identity
            );
        }
        Ok(())
    }

    #[test]
    fn scheduler_is_global_round_robin_for_every_table() -> Result<()> {
        let table = generated_table()?;
        let scheduler = Scheduler::new(vec![Arc::clone(&table), table], 1)?;
        let ordinals = (0..6)
            .map(|_| {
                let scheduled = scheduler.next();
                (scheduled.ordinal, scheduled.table_batch_ordinal)
            })
            .collect::<Vec<_>>();
        assert_eq!(ordinals, vec![(0, 0), (1, 0), (2, 1), (3, 1), (4, 2), (5, 2)]);
        Ok(())
    }
}
