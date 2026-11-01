//! The benchmark table schema and the CSV rows that feed the datagen.

use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_csv::ReaderBuilder;
use arrowcol_catalog::{CatalogSchemaVersion, DdlCommand, DdlParser, SchemaVersion, deterministic_table_id};

/// DDL the loadtest registers, so the schema and fingerprint match the benchmark.
const BENCHMARK_DDL: &str = include_str!("../../../projects/loadtest/schemas/sqlserver.sql");
/// Real SQL Server wait statistics committed next to the CLI.
const BUILTIN_CSV: &[u8] = include_bytes!("../data/sqlserver_wait_stats.csv");
/// Repository path of [`BUILTIN_CSV`], for messages.
pub(crate) const BUILTIN_CSV_PATH: &str = "bin/quiver-cli/data/sqlserver_wait_stats.csv";
/// Table the built-in CSV belongs to.
pub(crate) const DEFAULT_TABLE: &str = "telemetry.sqlserver.sqlserver_wait_stats";

/// A loadtest table as the catalog registers it (schema version 1).
pub(crate) struct BenchmarkTable {
    pub(crate) schema: CatalogSchemaVersion,
    pub(crate) arrow: SchemaRef,
}

impl BenchmarkTable {
    /// Parses the loadtest DDL and returns `name` with the catalog's
    /// deterministic table id and canonical Arrow schema fingerprint.
    pub(crate) fn load(name: &str) -> Result<Self> {
        let parser = DdlParser::default();
        let ddl = BENCHMARK_DDL
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let mut known = Vec::new();
        for statement in ddl.split(';').map(str::trim).filter(|statement| !statement.is_empty()) {
            let DdlCommand::CreateTable(create) = parser.parse(statement).context("parse the loadtest DDL")? else {
                continue;
            };
            let identity = create.identity().to_string();
            if identity != name {
                known.push(identity);
                continue;
            }
            let (identity, fields, canonical_ddl) = create.into_parts();
            let schema = CatalogSchemaVersion::new(
                deterministic_table_id(&identity),
                identity,
                SchemaVersion::FIRST,
                fields,
                canonical_ddl,
            )?;
            let arrow = Arc::new(schema.arrow_schema()?);
            return Ok(Self { schema, arrow });
        }
        bail!("{name} is not a loadtest table; choose one of: {}", known.join(", "))
    }
}

/// Reads CSV rows (with a header) into record batches of `batch_rows` rows,
/// typed by the table's Arrow schema. `None` reads the built-in CSV.
pub(crate) fn read_csv(table: &BenchmarkTable, csv: Option<&Path>, batch_rows: usize) -> Result<Vec<RecordBatch>> {
    if batch_rows == 0 {
        bail!("--batch-rows must be positive");
    }
    let input: Box<dyn Read> = match csv {
        Some(path) => Box::new(File::open(path).with_context(|| format!("open {}", path.display()))?),
        None => Box::new(Cursor::new(BUILTIN_CSV)),
    };
    let reader = ReaderBuilder::new(Arc::clone(&table.arrow))
        .with_header(true)
        .with_batch_size(batch_rows)
        .build(input)?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .context("parse the CSV rows with the table's Arrow schema")?;
    if batches.is_empty() {
        bail!("the CSV has no data rows");
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};

    use super::*;

    #[test]
    fn builtin_csv_matches_the_benchmark_schema() -> Result<()> {
        let table = BenchmarkTable::load(DEFAULT_TABLE)?;
        assert_eq!(table.schema.identity().to_string(), DEFAULT_TABLE);
        assert_eq!(table.arrow.fields().len(), 11);
        assert_eq!(table.arrow.field(0), &Field::new("customer_id", DataType::Utf8, false));
        let header = std::str::from_utf8(BUILTIN_CSV)?
            .lines()
            .next()
            .context("built-in CSV header is missing")?;
        assert_eq!(
            table
                .arrow
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            header.split(',').collect::<Vec<_>>(),
        );
        let batches = read_csv(&table, None, 128)?;
        assert!(batches.len() > 1);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 128));
        assert!(batches.iter().all(|batch| batch.schema() == table.arrow));
        let mut row = 0;
        for batch in &batches {
            let customers = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("customer_id is not a string array")?;
            for customer in customers.iter() {
                assert_eq!(customer, Some(format!("customer-{:04}", row % 16).as_str()));
                row += 1;
            }
        }
        assert_eq!(row, 3_546);
        Ok(())
    }

    #[test]
    fn unknown_tables_list_the_loadtest_tables() {
        let Err(error) = BenchmarkTable::load("telemetry.sqlserver.nope") else {
            panic!("unknown table loaded");
        };
        assert!(
            error
                .to_string()
                .contains("telemetry.sqlserver.sqlserver_cpu_utilization")
        );
    }
}
