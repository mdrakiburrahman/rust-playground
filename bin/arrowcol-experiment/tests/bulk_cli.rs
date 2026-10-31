use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use arrow::array::{Array, Int64Array, StringArray};
use arrow_deltalake::delta::DeltaTable;
use arrow_deltalake::maintenance::{
    Granularity, MaintenanceControl, OptimizeConfig, Outcome, PurgeConfig, VacuumConfig,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use sha2::{Digest, Sha256};
use url::Url;

struct Fixture {
    root: PathBuf,
    anchor: i64,
}

impl Fixture {
    fn new() -> Result<Self> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::current_dir()?.join(format!(".data/bulk-tests/{}-{nanos}", std::process::id()));
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            anchor: i64::try_from(nanos / 1_000_000)? / 60_000 * 60_000,
        })
    }

    fn command(&self) -> Command {
        let mut command = Command::new(assert_cmd::cargo::cargo_bin!("arrowcol-experiment"));
        command
            .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
            .arg("--root")
            .arg(&self.root)
            .args(["--table", "experiment_bulk", "--allow-table", "experiment_bulk"]);
        command
    }

    fn seed(&self) -> Result<Value> {
        self.seed_with(&[])
    }

    fn seed_with(&self, extra: &[&str]) -> Result<Value> {
        success(
            self.command()
                .args([
                    "seed-scale",
                    "--anchor-ms",
                    &self.anchor.to_string(),
                    "--seed-mib",
                    "1",
                    "--files-per-partition",
                    "2",
                ])
                .args(extra)
                .output()?,
        )
    }

    fn inspect(&self) -> Result<Value> {
        success(self.command().arg("inspect-scale").output()?)
    }

    fn table(&self) -> PathBuf {
        self.root.join("experiment_bulk")
    }

    fn url(&self) -> Result<Url> {
        Url::from_directory_path(self.table()).map_err(|()| anyhow::anyhow!("table URL"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn success(output: Output) -> Result<Value> {
    ensure!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let output: Value = serde_json::from_slice(&output.stdout)?;
    ensure!(output["ok"] == true, "unsuccessful JSON");
    Ok(output)
}

fn full_groups(fixture: &Fixture, inspection: &Value) -> Result<Value> {
    let mut groups = BTreeMap::<(String, i64, i64, String), u64>::new();
    for file in inspection["files"].as_array().context("files")? {
        let path = fixture.table().join(file["path"].as_str().context("path")?);
        let builder = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path)?)?;
        ensure!(
            builder.schema().fields().len() == 3,
            "physical partition column present"
        );
        for batch in builder.build()? {
            let batch = batch?;
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().context("id")?;
            let values = batch.column(1).as_any().downcast_ref::<Int64Array>().context("value")?;
            let payloads = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("payload")?;
            ensure!(payloads.null_count() == 0, "null payload");
            for index in 0..batch.num_rows() {
                let key = (
                    file["partition"].as_str().context("partition")?.to_owned(),
                    ids.value(index),
                    values.value(index),
                    format!("{:x}", Sha256::digest(payloads.value(index).as_bytes())),
                );
                *groups.entry(key).or_default() += 1;
            }
        }
    }
    Ok(Value::Array(
        groups
            .into_iter()
            .map(|((partition, id, value, digest), count)| {
                serde_json::json!({
                    "partition":partition,"id":id,"value":value,"payloadSha256":digest,"multiplicity":count
                })
            })
            .collect(),
    ))
}

#[test]
fn copied_snapshot_matches_full_payload_oracle_and_resume_is_idempotent() -> Result<()> {
    let fixture = Fixture::new()?;
    let seed = fixture.seed()?;
    assert_eq!(seed["rows"], 96);
    assert_eq!(seed["files"].as_array().context("files")?.len(), 6);
    assert_eq!(seed["expectedGroups"].as_array().context("groups")?.len(), 64);
    assert_eq!(seed["publisherClosed"], true);
    assert!(seed["totalBytes"].as_u64().context("bytes")? >= 6 * 1024 * 1024);
    assert!(seed["projectedKeyBytes"].as_u64().context("projected bytes")? < 64 * 1024);
    assert_eq!(seed["expectedGroups"], full_groups(&fixture, &seed)?);
    let resumed = fixture.seed()?;
    assert_eq!(resumed["version"], seed["version"]);
    assert_eq!(resumed["fingerprint"], seed["fingerprint"]);
    assert_eq!(resumed["expectedGroups"], seed["expectedGroups"]);
    assert!(
        !fixture.command().arg("inspect").output()?.status.success(),
        "rapid caps/schema must remain enforced"
    );
    Ok(())
}

#[test]
fn production_optimize_and_purge_preserve_projected_oracle_truth() -> Result<()> {
    let fixture = Fixture::new()?;
    let before = fixture.seed()?;
    let table = DeltaTable::open_for_maintenance(&fixture.url()?)?;
    let control = MaintenanceControl::new(Arc::new(AtomicBool::new(false)), None);
    let config = OptimizeConfig {
        target_file_bytes: 8 * 1024 * 1024,
        small_file_bytes: 4 * 1024 * 1024,
        now_ms: Some(fixture.anchor),
        granularity: Some(Granularity::Minute),
        latest_partitions: Some(2),
        ..OptimizeConfig::default()
    };
    let plan = table.plan_optimize(&config)?;
    let report = table.apply_optimize(&plan, &config, &control)?;
    ensure!(!report.outputs.is_empty(), "OPTIMIZE did not rewrite fixture");
    let optimized = fixture.inspect()?;
    assert_eq!(optimized["fingerprint"], before["fingerprint"]);
    assert_eq!(optimized["expectedGroups"], full_groups(&fixture, &optimized)?);
    let config = PurgeConfig::new(86_400_000, fixture.anchor, Granularity::Minute);
    let plan = table.plan_purge(&config)?;
    table.apply_purge(&plan, &config, &control)?;
    let purged = fixture.inspect()?;
    assert_eq!(purged["rows"], 64);
    assert_eq!(purged["expectedGroups"], full_groups(&fixture, &purged)?);
    assert!(
        !fixture
            .command()
            .args([
                "seed-scale",
                "--anchor-ms",
                &fixture.anchor.to_string(),
                "--seed-mib",
                "1",
                "--files-per-partition",
                "2",
            ])
            .output()?
            .status
            .success(),
        "resume must not undo maintenance"
    );
    Ok(())
}

#[test]
fn bounds_and_source_mutation_are_rejected() -> Result<()> {
    let fixture = Fixture::new()?;
    let rejected = fixture
        .command()
        .args([
            "seed-scale",
            "--anchor-ms",
            &fixture.anchor.to_string(),
            "--seed-mib",
            "32",
            "--files-per-partition",
            "511",
            "--max-total-bytes",
            "8589934592",
        ])
        .output()?;
    assert!(!rejected.status.success());
    assert!(
        !fixture.table().exists(),
        "configuration bound must reject before any file write"
    );
    fixture.seed()?;
    let manifest: Value = serde_json::from_slice(&fs::read(fixture.table().join("_scale/manifest.json"))?)?;
    let source = fixture.table().join(manifest["source"].as_str().context("source")?);
    let mut bytes = fs::read(&source)?;
    bytes[10] ^= 1;
    fs::write(source, bytes)?;
    assert!(
        !fixture
            .command()
            .args([
                "seed-scale",
                "--anchor-ms",
                &fixture.anchor.to_string(),
                "--seed-mib",
                "1",
                "--files-per-partition",
                "2",
            ])
            .output()?
            .status
            .success()
    );
    Ok(())
}

#[test]
fn completed_orphan_copy_resumes_but_foreign_destination_is_not_overwritten() -> Result<()> {
    let fixture = Fixture::new()?;
    let seeded = fixture.seed()?;
    let manifest: Value = serde_json::from_slice(&fs::read(fixture.table().join("_scale/manifest.json"))?)?;
    let version = seeded["version"].as_u64().context("version")?;
    fs::remove_file(fixture.table().join(format!("_delta_log/{version:020}.json")))?;
    fs::remove_dir_all(fixture.table().join("_scale/copies"))?;
    let copy = fixture
        .table()
        .join(manifest["copies"][0]["path"].as_str().context("copy")?);
    let original = fs::read(&copy)?;
    // The local object_store transport uses hard links; detach the corrupt destination.
    fs::remove_file(&copy)?;
    fs::write(&copy, vec![b'x'; original.len()])?;
    let failed = fixture
        .command()
        .args([
            "seed-scale",
            "--anchor-ms",
            &fixture.anchor.to_string(),
            "--seed-mib",
            "1",
            "--files-per-partition",
            "2",
        ])
        .output()?;
    assert!(!failed.status.success());
    let failed: Value = serde_json::from_slice(&failed.stdout)?;
    assert!(failed["orphanInventory"]["candidatePaths"].is_array());
    assert_eq!(fs::read(&copy)?, vec![b'x'; original.len()]);
    fs::write(&copy, original)?;
    let resumed = fixture.seed()?;
    assert_eq!(resumed["rows"], 96);
    assert_eq!(resumed["expectedGroups"], full_groups(&fixture, &resumed)?);
    Ok(())
}

#[test]
fn tombstoned_seed_leaves_only_physical_vacuum_work() -> Result<()> {
    let fixture = Fixture::new()?;
    let seed = fixture.seed_with(&["--tombstones"])?;
    assert_eq!(seed["tombstones"], 2);
    assert_eq!(seed["rows"], 64);
    assert_eq!(seed["files"].as_array().context("files")?.len(), 4);
    assert_eq!(seed["expectedGroups"], full_groups(&fixture, &seed)?);
    let expired = seed["partitions"]
        .as_array()
        .context("partitions")?
        .iter()
        .find(|partition| partition["role"] == "expired")
        .context("expired role")?["partition"]
        .clone();
    assert!(
        seed["files"]
            .as_array()
            .context("files")?
            .iter()
            .all(|file| file["partition"] != expired)
    );
    let manifest: Value = serde_json::from_slice(&fs::read(fixture.table().join("_scale/manifest.json"))?)?;
    let tombstoned = manifest["copies"]
        .as_array()
        .context("copies")?
        .iter()
        .filter(|copy| copy["partition"] == expired)
        .map(|copy| Ok(fixture.table().join(copy["path"].as_str().context("copy path")?)))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(tombstoned.len(), 2);
    assert!(
        tombstoned.iter().all(|path| path.exists()),
        "PURGE must not delete files"
    );

    let resumed = fixture.seed_with(&["--tombstones"])?;
    assert_eq!(resumed["version"], seed["version"]);
    assert_eq!(resumed["tombstones"], 2);
    assert_eq!(resumed["fingerprint"], seed["fingerprint"]);
    assert!(
        fixture
            .command()
            .args(["seed-scale", "--anchor-ms", &fixture.anchor.to_string()])
            .output()
            .map(|output| !output.status.success())?,
        "a differently configured resume must be rejected"
    );

    std::thread::sleep(std::time::Duration::from_millis(5));
    let now_ms = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let mut config = VacuumConfig::new(0, now_ms);
    config.allow_short_vacuum_retention = true;
    let table = DeltaTable::open_for_maintenance(&fixture.url()?)?;
    let plan = table.plan_vacuum(&config)?;
    assert_eq!(plan.candidates.len(), 2);
    let control = MaintenanceControl::new(Arc::new(AtomicBool::new(false)), None);
    let report = table.apply_vacuum(&plan, &config, &control)?;
    assert_eq!(report.outcome, Outcome::Deleted);
    assert_eq!(report.files_deleted, 2);
    assert!(tombstoned.iter().all(|path| !path.exists()));
    let vacuumed = fixture.inspect()?;
    assert_eq!(vacuumed["version"], seed["version"], "VACUUM must not commit");
    assert_eq!(vacuumed["fingerprint"], seed["fingerprint"]);
    Ok(())
}
