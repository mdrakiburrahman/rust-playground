use anyhow::{Context, Result, ensure};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_deltalake::delta::{DeltaTable, ingestion_partition_value};
use arrow_deltalake::layout::LayoutPolicy;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrowcol_catalog::{DdlCommand, DdlParser};
use arrowcol_loadgen::generator::build_logical_batch;
use arrowcol_loadgen::schema::schema_statements;
use arrowcol_loadgen::sdk::GeneratedTable;
use futures::{TryStreamExt, stream};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

const STAMP: i64 = 1_793_067_960_123;
const CHILD: &str = "ARROWCOL_WIDE_LAYOUT_TEST_CHILD";

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!("wide-layout-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("wide layout fixture cleanup failed: {error}");
        }
    }
}

fn table() -> Result<Arc<GeneratedTable>> {
    let statement = schema_statements()?
        .into_iter()
        .find(|statement| statement.identity.table().as_str() == "sqlserver_database_properties")
        .context("real database_properties fixture")?;
    let DdlCommand::CreateTable(create) = DdlParser::default().parse(&statement.ddl)? else {
        anyhow::bail!("fixture must be CREATE TABLE");
    };
    let schema = Arc::new(Schema::new(
        create
            .fields()
            .iter()
            .map(arrowcol_catalog::CatalogField::to_arrow)
            .collect::<std::result::Result<Vec<_>, _>>()?,
    ));
    Ok(Arc::new(GeneratedTable {
        descriptor_path: vec![
            statement.identity.catalog().to_string(),
            statement.identity.schema().to_string(),
            statement.identity.table().to_string(),
            "v1".into(),
        ],
        identity: statement.identity,
        schema_version: 1,
        fingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        schema,
        generated_root: PathBuf::from("target/generated"),
    }))
}

fn url(root: &Path) -> Result<Url> {
    Url::from_directory_path(root).map_err(|()| anyhow::anyhow!("invalid fixture URL"))
}

fn open(root: &Path, schema: &Arc<Schema>) -> Result<Arc<DeltaTable>> {
    std::fs::create_dir(root)?;
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new("__arrowcol_ingestion", DataType::Utf8, false)));
    DeltaTable::open_url(
        url(root)?,
        Arc::new(Schema::new(fields)),
        vec!["__arrowcol_ingestion".into()],
    )
}

async fn publish(
    table: &Arc<DeltaTable>,
    batch: &RecordBatch,
    writes: usize,
    policy: Option<LayoutPolicy>,
    app: &str,
) -> Result<()> {
    let mut writer =
        table.new_file_with_publication_and_layout(batch.schema(), app.into(), 0, STAMP, policy.clone())?;
    // Keep each real incoming batch intact. The repeated writes must sort
    // together, not just concatenate individually sorted batches.
    for _ in 0..writes {
        writer.write(batch).await?;
    }
    let result = writer.finish().await?;
    let observation = result.file.observation();
    assert_eq!(observation.rows, batch.num_rows() * writes);
    if let Some(policy) = policy {
        assert!(observation.rows <= policy.max_rows);
        assert!(observation.input_bytes <= u64::try_from(policy.max_input_bytes)?);
    }
    Ok(())
}

#[tokio::test]
async fn real_database_properties_ipc_batch_preserves_bounded_sorted_publication_and_replay() -> Result<()> {
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "real_database_properties_ipc_batch_preserves_bounded_sorted_publication_and_replay",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
            .output()?;
        ensure!(
            output.status.success(),
            "isolated minute-partition fixture failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        return Ok(());
    }
    for (row_count, writes) in [(1_024, 1), (8_192, 1), (1_024, 2), (8_192, 2)] {
        let root = Fixture::new()?;
        let logical = build_logical_batch(table()?, row_count, 7).await?;
        let decoded = FlightRecordBatchStream::new_from_flight_data(stream::iter(
            logical.encoded.as_ref().clone().into_iter().map(Ok::<_, FlightError>),
        ))
        .try_collect::<Vec<_>>()
        .await?;
        assert_eq!(
            decoded.len(),
            1,
            "fixture must be the actual single incoming Flight batch"
        );
        let incoming = &decoded[0];
        assert_eq!(incoming, &logical.record_batch);
        let policy = LayoutPolicy {
            row_group_rows: 512,
            page_rows: 128,
            ..LayoutPolicy::default()
        };
        assert_eq!(policy.max_rows, 262_144);
        assert_eq!(policy.max_input_bytes, 64 * 1024 * 1024);
        eprintln!(
            "database_properties rows={row_count} built_bytes={} decoded_shared_charge={} cap={}",
            logical.record_batch.get_array_memory_size(),
            incoming.get_array_memory_size(),
            policy.max_input_bytes
        );
        let sorted_root = root.0.join("sorted");
        let control_root = root.0.join("control");
        let sorted = open(&sorted_root, &incoming.schema())?;
        let control = open(&control_root, &logical.record_batch.schema())?;
        let app = format!("arrowcol.publication.wide-layout-{row_count}-{writes}");
        publish(&control, &logical.record_batch, writes, None, "control-publication").await?;
        publish(&sorted, incoming, writes, Some(policy.clone()), &app).await?;
        assert_eq!(sorted.lookup_txn(&app)?, Some(0));
        let version = sorted.version()?;
        let before = delta_inspect::inspect(&root.0).await?;
        assert_eq!(before["active_files"]["sorted"], 1);
        assert_eq!(before["active_files"]["control"], 1);
        let actual = before["tables"]["sorted"].as_array().context("sorted full rows")?;
        assert_eq!(actual.len(), row_count * writes);
        let expected = before["tables"]["control"].as_array().context("control full rows")?;
        let mut actual_multiset = actual
            .iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut expected_multiset = expected
            .iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        actual_multiset.sort();
        expected_multiset.sort();
        assert_eq!(
            actual_multiset, expected_multiset,
            "all columns and duplicate multiplicities must survive"
        );
        let keys = actual
            .iter()
            .map(|row| row["customer_id"].as_str().context("customer key"))
            .collect::<Result<Vec<_>>>()?;
        assert!(
            keys.windows(2).all(|pair| pair[0] <= pair[1]),
            "one complete file must be globally sorted across writes"
        );
        let mut customers = BTreeMap::new();
        for key in keys {
            *customers.entry(key).or_insert(0_usize) += 1;
        }
        assert_eq!(customers.len(), 16);
        assert!(customers.values().all(|count| *count == row_count * writes / 16));
        let minute = ingestion_partition_value(STAMP)?;
        assert_eq!(minute, "2026-10-27-02-26");
        assert!(actual.iter().all(|row| row["__arrowcol_ingestion"] == minute));
        let commit = std::fs::read_to_string(sorted_root.join(format!("_delta_log/{version:020}.json")))?;
        let actions = commit
            .lines()
            .map(serde_json::from_str::<serde_json::Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let adds = actions
            .iter()
            .filter_map(|action| action.get("add"))
            .collect::<Vec<_>>();
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0]["partitionValues"]["__arrowcol_ingestion"], minute);
        let transactions = actions
            .iter()
            .filter_map(|action| action.get("txn"))
            .collect::<Vec<_>>();
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0]["appId"], app);
        assert_eq!(transactions[0]["version"], 0);
        publish(&sorted, incoming, writes, Some(policy), &app).await?;
        assert_eq!(sorted.version()?, version, "same receipt must not commit again");
        assert_eq!(sorted.lookup_txn(&app)?, Some(0));
        assert_eq!(
            delta_inspect::inspect(&root.0).await?,
            before,
            "replay must preserve the entire independent snapshot"
        );
    }
    Ok(())
}
