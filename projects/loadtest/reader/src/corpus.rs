//! Exact chunked validation corpus for long retained windows.
//!
//! Every captured row is kept with its multiplicity; nothing is sampled or truncated. Rows are
//! grouped by their committed partition value and whole partitions are packed into disjoint
//! chunks of at most [`MAX_CHUNK_DISTINCT_ROWS`] distinct rows so each chunk can be checked by one
//! bounded Kusto multiset query filtered to exactly its partitions. Exact per-partition counts let
//! the consumer reject rows in partitions that no chunk covers.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{PartitionValues, TableSummary};

/// Canonical JSON encoding of a row and its `sha256:` digest, shared by every oracle path.
pub(crate) fn encode_row(value: &Value) -> Result<(String, String)> {
    let encoded = serde_json::to_string(value)?;
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(encoded.as_bytes())));
    Ok((encoded, digest))
}

/// Distinct complete rows checked by one bounded multiset query.
pub const MAX_CHUNK_DISTINCT_ROWS: usize = 256;
/// Global in-memory guard for one table's corpus; exceeding it fails instead of truncating.
pub const MAX_CORPUS_DISTINCT_ROWS: usize = 65_536;

/// Distinct complete rows for a disjoint set of whole partitions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ValidationChunk {
    /// Exact committed partition values covered by this chunk; empty for unpartitioned tables.
    pub partition_values: Vec<String>,
    /// Distinct complete rows (partition column included) with their exact multiplicities.
    pub rows: Vec<(Value, u64)>,
}

/// Complete exact row multiset of one captured table, split into bounded disjoint chunks.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ValidationCorpus {
    /// The single string partition column, or `None` for an unpartitioned table.
    pub partition_column: Option<String>,
    pub expected_rows: u64,
    /// Exact row count of every committed partition; empty for unpartitioned tables.
    pub partition_rows: BTreeMap<String, u64>,
    pub chunks: Vec<ValidationChunk>,
}

impl ValidationCorpus {
    /// Number of distinct complete rows across every chunk.
    #[must_use]
    pub fn distinct_rows(&self) -> usize {
        self.chunks.iter().map(|chunk| chunk.rows.len()).sum()
    }

    /// Checks the structural guarantees consumers rely on: bounded chunks, disjoint and exhaustive
    /// partitions, unique rows with positive multiplicities, rows inside their chunk partitions,
    /// and exact chunk, partition and total counts.
    ///
    /// # Errors
    ///
    /// Returns an error describing the first violated guarantee.
    pub fn verify(&self) -> Result<()> {
        ensure!(!self.chunks.is_empty(), "validation corpus has no chunks");
        ensure!(
            self.distinct_rows() <= MAX_CORPUS_DISTINCT_ROWS,
            "validation corpus exceeds {MAX_CORPUS_DISTINCT_ROWS} distinct rows"
        );
        match &self.partition_column {
            None => ensure!(
                self.partition_rows.is_empty() && self.chunks.len() == 1 && self.chunks[0].partition_values.is_empty(),
                "an unpartitioned corpus must be exactly one unfiltered chunk"
            ),
            Some(column) => ensure!(!column.is_empty(), "empty partition column"),
        }
        let mut covered = BTreeSet::new();
        let mut total = 0_u64;
        for (index, chunk) in self.chunks.iter().enumerate() {
            ensure!(!chunk.rows.is_empty(), "chunk {index} has no rows");
            ensure!(
                chunk.rows.len() <= MAX_CHUNK_DISTINCT_ROWS,
                "chunk {index} exceeds {MAX_CHUNK_DISTINCT_ROWS} distinct rows"
            );
            let mut expected = 0_u64;
            if self.partition_column.is_some() {
                ensure!(!chunk.partition_values.is_empty(), "chunk {index} covers no partitions");
            }
            for partition in &chunk.partition_values {
                ensure!(
                    covered.insert(partition.as_str()),
                    "partition {partition:?} appears in more than one chunk"
                );
                let rows = self
                    .partition_rows
                    .get(partition)
                    .with_context(|| format!("chunk partition {partition:?} has no expected row count"))?;
                expected = expected.checked_add(*rows).context("partition row count overflow")?;
            }
            let mut distinct = BTreeSet::new();
            let mut rows = 0_u64;
            for (row, multiplicity) in &chunk.rows {
                ensure!(*multiplicity > 0, "chunk {index} has a zero-multiplicity row");
                ensure!(
                    distinct.insert(serde_json::to_string(row)?),
                    "chunk {index} repeats a distinct row"
                );
                if let Some(column) = &self.partition_column {
                    let value = row.get(column).and_then(Value::as_str);
                    ensure!(
                        value.is_some_and(|value| chunk.partition_values.iter().any(|partition| partition == value)),
                        "chunk {index} has a row outside its partitions"
                    );
                }
                rows = rows.checked_add(*multiplicity).context("chunk row count overflow")?;
            }
            if self.partition_column.is_some() {
                ensure!(
                    rows == expected,
                    "chunk {index} has {rows} rows, its partitions expect {expected}"
                );
            }
            total = total.checked_add(rows).context("corpus row count overflow")?;
        }
        if self.partition_column.is_some() {
            ensure!(
                covered.len() == self.partition_rows.len(),
                "chunks do not cover every counted partition"
            );
            let counted = self
                .partition_rows
                .values()
                .try_fold(0_u64, |sum, rows| sum.checked_add(*rows))
                .context("partition row count overflow")?;
            ensure!(
                counted == self.expected_rows,
                "partition counts differ from the expected rows"
            );
        }
        ensure!(
            total == self.expected_rows,
            "chunks hold {total} rows, expected {}",
            self.expected_rows
        );
        Ok(())
    }
}

type Rows = BTreeMap<String, (Value, u64)>;

/// Accumulates captured rows and packs them into a verified [`ValidationCorpus`].
pub(crate) struct CorpusBuilder {
    partition_column: Option<String>,
    partitions: BTreeMap<String, Rows>,
    hashes: BTreeMap<String, u64>,
    rows: u64,
    distinct: usize,
}

impl CorpusBuilder {
    pub(crate) fn new(summary: &TableSummary) -> Result<Self> {
        let partition_column = match summary.partition_columns.as_slice() {
            [] => None,
            [column] => {
                let kind = summary.schema["fields"]
                    .as_array()
                    .context("missing Delta fields")?
                    .iter()
                    .find(|field| field["name"].as_str() == Some(column))
                    .map(|field| &field["type"])
                    .context("partition column absent from schema")?;
                ensure!(
                    kind == "string",
                    "exact chunked oracle requires a string partition column"
                );
                Some(column.clone())
            }
            _ => bail!("exact chunked oracle supports at most one partition column"),
        };
        Ok(Self {
            partition_column,
            partitions: BTreeMap::new(),
            hashes: BTreeMap::new(),
            rows: 0,
            distinct: 0,
        })
    }

    pub(crate) fn add(&mut self, value: Value, partitions: &PartitionValues) -> Result<()> {
        let (encoded, digest) = encode_row(&value)?;
        self.add_encoded(value, encoded, digest, partitions)
    }

    /// Adds a row whose canonical encoding and digest were computed by [`encode_row`], letting
    /// callers hash rows in parallel while still adding them in a deterministic order.
    pub(crate) fn add_encoded(
        &mut self,
        value: Value,
        encoded: String,
        digest: String,
        partitions: &PartitionValues,
    ) -> Result<()> {
        let partition = match &self.partition_column {
            None => String::new(),
            Some(column) => partitions
                .get(column)
                .cloned()
                .flatten()
                .filter(|value| !value.is_empty())
                .context("exact chunked oracle does not support null or empty partition values")?,
        };
        *self.hashes.entry(digest).or_default() += 1;
        let rows = self.partitions.entry(partition).or_default();
        let before = rows.len();
        let entry = rows.entry(encoded).or_insert((value, 0));
        entry.1 = entry.1.checked_add(1).context("row multiplicity overflow")?;
        if rows.len() > before {
            ensure!(
                rows.len() <= MAX_CHUNK_DISTINCT_ROWS,
                "a single partition exceeds {MAX_CHUNK_DISTINCT_ROWS} distinct rows"
            );
            self.distinct += 1;
            ensure!(
                self.distinct <= MAX_CORPUS_DISTINCT_ROWS,
                "validation corpus exceeds {MAX_CORPUS_DISTINCT_ROWS} distinct rows"
            );
        }
        self.rows = self.rows.checked_add(1).context("row count overflow")?;
        Ok(())
    }

    pub(crate) fn finish(self, summary: &TableSummary) -> Result<ValidationCorpus> {
        if self.rows != summary.rows || self.hashes != summary.row_hashes {
            bail!("captured validation corpus changed");
        }
        let partitioned = self.partition_column.is_some();
        let mut partition_rows = BTreeMap::new();
        let mut chunks: Vec<ValidationChunk> = Vec::new();
        for (partition, rows) in self.partitions {
            let count = rows
                .values()
                .try_fold(0_u64, |sum, (_, rows)| sum.checked_add(*rows))
                .context("partition row count overflow")?;
            let fits = chunks
                .last()
                .is_some_and(|chunk| chunk.rows.len() + rows.len() <= MAX_CHUNK_DISTINCT_ROWS);
            if !fits {
                chunks.push(ValidationChunk {
                    partition_values: Vec::new(),
                    rows: Vec::new(),
                });
            }
            let chunk = chunks.last_mut().context("chunk missing")?;
            chunk.rows.extend(rows.into_values());
            if partitioned {
                chunk.partition_values.push(partition.clone());
                partition_rows.insert(partition, count);
            }
        }
        let corpus = ValidationCorpus {
            partition_column: self.partition_column,
            expected_rows: self.rows,
            partition_rows,
            chunks,
        };
        corpus.verify()?;
        Ok(corpus)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const COLUMN: &str = "__arrowcol_ingestion";

    fn summary(rows: &[(Value, &str)], partitioned: bool) -> Result<TableSummary> {
        let mut row_hashes = BTreeMap::new();
        for (row, _) in rows {
            *row_hashes
                .entry(format!(
                    "sha256:{}",
                    hex::encode(Sha256::digest(serde_json::to_string(row)?.as_bytes()))
                ))
                .or_default() += 1;
        }
        Ok(TableSummary {
            schema: json!({"fields":[{"name":"id","type":"long"},{"name":COLUMN,"type":"string"}]}),
            version: 1,
            active_files: 1,
            rows: rows.len() as u64,
            bytes: 1,
            minimum_file_bytes: 1,
            maximum_file_bytes: 1,
            row_hashes,
            payload_row_hashes: BTreeMap::new(),
            files: Vec::new(),
            partition_columns: if partitioned {
                vec![COLUMN.to_owned()]
            } else {
                Vec::new()
            },
        })
    }

    fn build(rows: &[(Value, &str)], partitioned: bool) -> Result<ValidationCorpus> {
        let summary = summary(rows, partitioned)?;
        let mut builder = CorpusBuilder::new(&summary)?;
        for (row, partition) in rows {
            builder.add(
                row.clone(),
                &BTreeMap::from([(COLUMN.to_owned(), Some((*partition).to_owned()))]),
            )?;
        }
        builder.finish(&summary)
    }

    /// `minutes` partitions of `per` distinct rows each; row 0 of every partition appears twice.
    fn window(minutes: usize, per: usize) -> Vec<(Value, String)> {
        let mut rows = Vec::new();
        for minute in 0..minutes {
            let partition = format!("2026-10-27-02-{minute:03}");
            for id in 0..per {
                let row = json!({"id": id, COLUMN: partition});
                if id == 0 {
                    rows.push((row.clone(), partition.clone()));
                }
                rows.push((row, partition.clone()));
            }
        }
        rows
    }

    fn borrowed(rows: &[(Value, String)]) -> Vec<(Value, &str)> {
        rows.iter()
            .map(|(row, partition)| (row.clone(), partition.as_str()))
            .collect()
    }

    #[test]
    fn long_windows_pack_whole_partitions_into_exact_disjoint_chunks() -> Result<()> {
        // 40 retained minutes x 16 customers = 640 distinct rows: far past the old 256 limit.
        let rows = window(40, 16);
        let corpus = build(&borrowed(&rows), true)?;
        assert_eq!(corpus.expected_rows, 680);
        assert_eq!(corpus.distinct_rows(), 640);
        assert_eq!(corpus.partition_rows.len(), 40);
        assert!(corpus.partition_rows.values().all(|rows| *rows == 17));
        assert_eq!(corpus.chunks.len(), 3, "16 whole partitions of 16 rows per chunk");
        assert!(
            corpus
                .chunks
                .iter()
                .all(|chunk| chunk.rows.len() <= MAX_CHUNK_DISTINCT_ROWS)
        );
        let covered: Vec<&String> = corpus.chunks.iter().flat_map(|chunk| &chunk.partition_values).collect();
        assert_eq!(covered, corpus.partition_rows.keys().collect::<Vec<_>>());
        assert_eq!(corpus, build(&borrowed(&rows), true)?, "chunking is deterministic");
        Ok(())
    }

    #[test]
    fn oversized_partitions_and_unpartitioned_corpora_fail_instead_of_truncating() -> Result<()> {
        let rows = window(1, MAX_CHUNK_DISTINCT_ROWS + 1);
        let error = build(&borrowed(&rows), true).err().context("oversized partition")?;
        assert!(error.to_string().contains("single partition exceeds 256"), "{error}");
        let error = build(&borrowed(&rows), false)
            .err()
            .context("oversized unpartitioned")?;
        assert!(error.to_string().contains("exceeds 256"), "{error}");
        let small = window(2, 3);
        let corpus = build(&borrowed(&small), false)?;
        assert_eq!(corpus.chunks.len(), 1);
        assert!(corpus.partition_column.is_none() && corpus.chunks[0].partition_values.is_empty());
        assert_eq!(corpus.expected_rows, 8);
        Ok(())
    }

    #[test]
    fn changed_capture_and_unsupported_partitions_fail() -> Result<()> {
        let rows = window(2, 3);
        let rows = borrowed(&rows);
        let mut changed = summary(&rows, true)?;
        changed.rows += 1;
        let mut builder = CorpusBuilder::new(&changed)?;
        for (row, partition) in &rows {
            builder.add(
                row.clone(),
                &BTreeMap::from([(COLUMN.to_owned(), Some((*partition).to_owned()))]),
            )?;
        }
        assert!(builder.finish(&changed).is_err(), "row count drift must fail");
        let summary = summary(&rows, true)?;
        let mut builder = CorpusBuilder::new(&summary)?;
        assert!(
            builder
                .add(json!({"id":1}), &BTreeMap::from([(COLUMN.to_owned(), None)]))
                .is_err()
        );
        let mut two = summary.clone();
        two.partition_columns.push("id".to_owned());
        assert!(CorpusBuilder::new(&two).is_err());
        let mut typed = summary;
        typed.schema = json!({"fields":[{"name":COLUMN,"type":"date"}]});
        assert!(CorpusBuilder::new(&typed).is_err());
        Ok(())
    }

    #[test]
    fn verification_rejects_overlapping_missing_and_miscounted_chunks() -> Result<()> {
        let rows = window(40, 16);
        let corpus = build(&borrowed(&rows), true)?;
        let mut overlapping = corpus.clone();
        let shared = overlapping.chunks[0].partition_values[0].clone();
        overlapping.chunks[1].partition_values.push(shared);
        assert!(overlapping.verify().is_err());
        let mut missing = corpus.clone();
        missing.chunks.pop();
        assert!(missing.verify().is_err(), "a dropped final chunk must fail");
        let mut unknown = corpus.clone();
        unknown.chunks[0].partition_values.push("2026-10-27-03-000".to_owned());
        assert!(unknown.verify().is_err());
        let mut duplicated = corpus.clone();
        duplicated.chunks[0].rows[0].1 += 1;
        assert!(duplicated.verify().is_err(), "multiplicity drift must fail");
        let mut repeated = corpus.clone();
        let row = repeated.chunks[0].rows[0].clone();
        repeated.chunks[0].rows.push(row);
        assert!(repeated.verify().is_err());
        let mut misplaced = corpus.clone();
        let row = misplaced.chunks[1].rows.remove(0);
        misplaced.chunks[0].rows.push(row);
        assert!(misplaced.verify().is_err());
        let mut oversized = corpus;
        let extra = oversized.chunks[1].rows.clone();
        oversized.chunks[0].rows.extend(extra);
        assert!(oversized.verify().is_err());
        Ok(())
    }
}
