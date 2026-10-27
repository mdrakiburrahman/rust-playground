use std::{fs, path::PathBuf, process::Command, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

struct Fixture(PathBuf);

impl Fixture {
    fn new(partitions: usize, distinct: i32) -> Result<Self> {
        let root = std::env::temp_dir().join(format!(
            "evidence-chunks-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        let fixture = Self(root);
        let table = fixture.0.join("telemetry/sqlserver/fixture");
        fs::create_dir_all(table.join("_delta_log"))?;
        let arrow_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let schema = json!({"type":"struct","fields":[
            {"name":"id","type":"integer","nullable":false,"metadata":{}},
            {"name":"__arrowcol_ingestion","type":"string","nullable":false,"metadata":{}}
        ]});
        let mut actions = vec![
            json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
            json!({"metaData":{"id":"fixture","format":{"provider":"parquet","options":{}},
                "schemaString":schema.to_string(),"partitionColumns":["__arrowcol_ingestion"],"configuration":{}}}),
        ];
        for partition in 0..partitions {
            let name = format!("part-{partition}.parquet");
            let file = table.join(&name);
            let values = (0..distinct).chain(std::iter::once(0)).collect::<Vec<_>>();
            let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(Int32Array::from(values))])?;
            let mut writer = ArrowWriter::try_new(fs::File::create(&file)?, Arc::clone(&arrow_schema), None)?;
            writer.write(&batch)?;
            writer.close()?;
            actions.push(json!({"add":{"path":name,
                "partitionValues":{"__arrowcol_ingestion":format!("2026-10-27-02-{partition:02}")},
                "size":fs::metadata(&file)?.len(),"modificationTime":1,"dataChange":true,
                "stats":json!({"numRecords":distinct + 1}).to_string()}}));
        }
        fs::write(
            table.join("_delta_log/00000000000000000000.json"),
            actions.iter().map(Value::to_string).collect::<Vec<_>>().join("\n") + "\n",
        )?;
        Ok(fixture)
    }

    fn cli(&self, command: &str, table: &str) -> Result<std::process::Output> {
        Command::new(env!("CARGO_BIN_EXE_delta-inspect"))
            .arg(command)
            .arg(&self.0)
            .args(["--table", table])
            .output()
            .context("run actual reader CLI")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn actual_cli_keeps_complete_long_window_chunks_and_legacy_guard() -> Result<()> {
    let fixture = Fixture::new(2, 200)?;
    let output = fixture.cli("evidence-chunks", "telemetry.sqlserver.fixture")?;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let evidence: Value = serde_json::from_slice(&output.stdout)?;
    let corpus: delta_inspect::ValidationCorpus =
        serde_json::from_value(evidence["corpus"]["telemetry.sqlserver.fixture"].clone())?;
    corpus.verify()?;
    assert_eq!(corpus.expected_rows, 402);
    assert_eq!(corpus.distinct_rows(), 400);
    assert_eq!(corpus.chunks.len(), 2);
    assert_eq!(corpus.partition_rows.values().sum::<u64>(), 402);
    assert_eq!(
        evidence["snapshot"]["tables"]["telemetry.sqlserver.fixture"]["rows"],
        402
    );
    assert!(
        corpus
            .chunks
            .iter()
            .all(|chunk| chunk.rows.iter().any(|(_, count)| *count == 2))
    );
    let legacy = fixture.cli("evidence", "telemetry.sqlserver.fixture")?;
    assert!(!legacy.status.success(), "legacy guard must remain unchanged");
    assert!(legacy.stdout.is_empty(), "failed evidence must never look complete");
    Ok(())
}

#[test]
fn actual_cli_fails_instead_of_truncating_or_omitting_requested_tables() -> Result<()> {
    let fixture = Fixture::new(1, 257)?;
    for table in ["telemetry.sqlserver.fixture", "telemetry.sqlserver.absent"] {
        let output = fixture.cli("evidence-chunks", table)?;
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
    Ok(())
}
