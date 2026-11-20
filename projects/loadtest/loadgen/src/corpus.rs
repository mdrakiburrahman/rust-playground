use std::{collections::BTreeMap, sync::Arc, time::Instant};

use anyhow::{Context, Result, ensure};
use arrow::datatypes::DataType;
use serde_json::json;

use crate::{
    generator::{CUSTOMER_COUNT, LogicalBatch, build_logical_batch_with_tuning, scheduled_batch_seed},
    sdk::GeneratedTable,
    tuning::{ClientTuning, DataMode},
};

/// Cached payloads include the original row-hash multiset; lookup never changes the scheduler's seed cycle.
pub struct PreparedCorpus {
    batches: BTreeMap<String, Vec<Vec<Arc<LogicalBatch>>>>,
    pub evidence: serde_json::Value,
}

impl PreparedCorpus {
    /// Validates conservative peak allocation bounds before constructing any generated batch.
    ///
    /// # Errors
    ///
    /// Rejects memory/Arrow bounds and propagates encoding or proof-generation failures.
    pub async fn prepare(
        tables: &[Arc<GeneratedTable>],
        rows: usize,
        seed: u64,
        lanes: usize,
        tuning: &ClientTuning,
    ) -> Result<Self> {
        tuning.validate(lanes, rows)?;
        let copies = match tuning.data_mode {
            DataMode::Shared => 1,
            DataMode::Independent => lanes,
        };
        let mut total_batch_bytes = 0_u64;
        let mut largest_batch_bytes = 0_u64;
        for table in tables {
            let mut row_bytes = 0_u64;
            for field in table.schema.fields() {
                let width = match field.data_type() {
                    DataType::Utf8 if field.name() == "customer_id" => 16,
                    DataType::Utf8 => tuning
                        .string_width
                        .max(table.identity.table().as_str().len() + field.name().len() + 64)
                        .max(36),
                    _ => 16,
                };
                if field.data_type() == &DataType::Utf8 {
                    ensure!(
                        rows.checked_mul(width).is_some_and(|bytes| bytes <= i32::MAX as usize),
                        "batch-rows and string-width exceed Arrow Utf8 column bounds"
                    );
                }
                row_bytes = row_bytes
                    .checked_add(width as u64)
                    .context("corpus row byte overflow")?;
            }
            let bytes = row_bytes
                .checked_mul(rows as u64)
                .and_then(|bytes| {
                    bytes.checked_add(table.schema.fields().len() as u64 * 4096 + CUSTOMER_COUNT as u64 * 256)
                })
                .and_then(|bytes| bytes.checked_mul(4))
                .context("corpus batch memory estimate overflow")?;
            total_batch_bytes = total_batch_bytes
                .checked_add(bytes)
                .context("corpus table memory estimate overflow")?;
            largest_batch_bytes = largest_batch_bytes.max(bytes);
        }
        let reservation = total_batch_bytes
            .checked_mul(tuning.precompute_batches_per_table as u64)
            .and_then(|bytes| bytes.checked_mul(copies as u64))
            .and_then(|bytes| bytes.checked_add(largest_batch_bytes.checked_mul(lanes as u64)?))
            .context("corpus memory reservation overflow")?;
        ensure!(
            reservation <= tuning.max_memory_bytes,
            "corpus and in-flight generation reserve {reservation} bytes, exceeding max-memory-bytes={}",
            tuning.max_memory_bytes
        );
        let started = Instant::now();
        let cpu_started = thread_cpu_seconds()?;
        let mut batches = BTreeMap::new();
        let mut decoded_bytes = 0_u64;
        let mut encoded_bytes = 0_u64;
        let mut prepared_rows = 0_u64;
        for table in tables {
            let mut table_copies = Vec::new();
            for _ in 0..copies {
                let mut variants = Vec::new();
                for variant in 0..tuning.precompute_batches_per_table {
                    let batch = Arc::new(
                        build_logical_batch_with_tuning(
                            Arc::clone(table),
                            rows,
                            scheduled_batch_seed(seed, variant as u64)?,
                            tuning,
                        )
                        .await?,
                    );
                    decoded_bytes = decoded_bytes
                        .checked_add(batch.arrow_bytes)
                        .context("decoded cache size overflow")?;
                    encoded_bytes = encoded_bytes
                        .checked_add(
                            batch
                                .encoded
                                .iter()
                                .map(|message| prost::Message::encoded_len(message) as u64)
                                .sum(),
                        )
                        .context("encoded cache size overflow")?;
                    prepared_rows = prepared_rows
                        .checked_add(rows as u64)
                        .context("prepared row count overflow")?;
                    ensure!(
                        decoded_bytes.saturating_add(encoded_bytes) <= tuning.max_memory_bytes,
                        "actual retained corpus exceeded max-memory-bytes"
                    );
                    variants.push(batch);
                }
                table_copies.push(variants);
            }
            batches.insert(table.identity.to_string(), table_copies);
        }
        Ok(Self {
            batches,
            evidence: json!({
                "version": 1, "prepared_rows": prepared_rows, "decoded_bytes": decoded_bytes,
                "encoded_protobuf_bytes": encoded_bytes, "memory_reservation_bytes": reservation,
                "preparation_wall_seconds": started.elapsed().as_secs_f64(),
                "preparation_cpu_seconds": thread_cpu_seconds()?.zip(cpu_started).map(|(end, start)| (end - start).max(0.0)),
                "cpu_scope": "preparation_caller_thread_linux_schedstat_null_elsewhere",
                "cached_variants_per_table": tuning.precompute_batches_per_table,
                "seed_cycle_variants": delta_inspect::MAX_CHUNK_DISTINCT_ROWS / CUSTOMER_COUNT,
                "partial_cache_misses": "generate_the_original_seed_without_modifying_proof_or_distribution",
            }),
        })
    }

    pub(crate) fn get(&self, table: &str, lane: usize, ordinal: u64) -> Option<Arc<LogicalBatch>> {
        let copies = self.batches.get(table)?;
        let variants = &copies[if copies.len() == 1 { 0 } else { lane }];
        let variant = ordinal as usize % (delta_inspect::MAX_CHUNK_DISTINCT_ROWS / CUSTOMER_COUNT);
        variants.get(variant).map(Arc::clone)
    }
}

fn thread_cpu_seconds() -> Result<Option<f64>> {
    #[cfg(target_os = "linux")]
    {
        let contents =
            std::fs::read_to_string("/proc/thread-self/schedstat").context("read generator CPU accounting")?;
        let nanos: u64 = contents
            .split_whitespace()
            .next()
            .context("missing CPU accounting")?
            .parse()?;
        Ok(Some(nanos as f64 / 1e9))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(None)
    }
}
