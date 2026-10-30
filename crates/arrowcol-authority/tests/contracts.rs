use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrowcol_authority::flight::Directory;
use arrowcol_authority::*;
use std::{collections::BTreeMap, sync::Arc};

fn writer() -> Writer {
    Writer {
        collector: "collector1".into(),
        process: "process1".into(),
        generation: "generation1".into(),
    }
}

#[tokio::test]
async fn runtime_replacement_drains_activity_and_permanently_closes_old_admission() -> anyhow::Result<()> {
    let old = arrowcol_authority::local::RuntimeGate::default();
    let admitted = old.enter().await?;
    let encoding = Arc::clone(&admitted);
    let replacing = old.clone();
    let task = tokio::spawn(async move { replacing.replace().await });
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    drop(admitted);
    assert!(!task.is_finished(), "encoding must also drain");
    drop(encoding);
    let successor = task.await??;
    assert!(old.enter().await.is_err());
    let current = successor.enter().await?;
    assert!(
        old.enter().await.is_err(),
        "old runtime cannot publish while successor writes"
    );
    drop(current);
    assert!(old.replace().await.is_err(), "cannot fork two successor runtimes");
    Ok(())
}

#[tokio::test]
async fn collector_runtime_replacement_requests_controller_session_fence() -> anyhow::Result<()> {
    use axum::{Json, Router, routing::post};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new().route(
        "/v1/authority",
        post(|Json(operation): Json<Operation>| async move {
            let Operation::ReplaceSession { mut writer } = operation else {
                panic!("replacement must request the controller's exact-session fence");
            };
            assert_eq!(writer.collector, "collector1");
            assert!(!writer.process.is_empty());
            writer.generation = "controller-generated-successor".into();
            Json(serde_json::json!({"writer":writer,"revision":10}))
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let mut config = config(std::path::PathBuf::new(), "schema".into());
    config.controller = format!("http://{address}");
    config.collectors.insert("collector1".into(), "collector-secret".into());
    let old = arrowcol_authority::flight::Service::collector(config, "collector1")?;
    let successor = old.replace_runtime().await?;
    assert!(
        old.replace_runtime().await.is_err(),
        "retired runtime cannot fork another writer"
    );
    let _next = successor.replace_runtime().await?;
    server.abort();
    Ok(())
}

#[test]
fn placement_counts_durable_targets_and_releases_completed_or_failed_reservations() -> anyhow::Result<()> {
    use arrowcol_authority::authority::Placement;
    let mut moving = record(1);
    moving.lifecycle = Lifecycle::Draining;
    let mut target = writer();
    target.collector = "collector2".into();
    moving.migration = Some(Migration {
        id: "move".into(),
        source: moving.writer.clone(),
        target: target.clone(),
        started_ms: 0,
        deadline_ms: 100,
        phase: "draining".into(),
        cutover_ms: None,
        activated_ms: None,
    });
    let restored: Record = serde_json::from_str(&serde_json::to_string(&moving)?)?;
    let pressure = Placement::from_records(&[moving.clone(), restored.clone()]);
    assert_eq!(pressure.assignments["collector2"], 2);
    assert!(!pressure.can_reserve("collector2"));
    assert!(!Placement::from_records(&vec![moving.clone(); 4]).can_reserve("collector3"));
    moving.writer = target;
    moving.lifecycle = Lifecycle::Recovering;
    assert_eq!(Placement::from_records(&[moving.clone()]).assignments["collector2"], 1);
    moving.lifecycle = Lifecycle::Active;
    assert_eq!(Placement::from_records(&[moving]).migrations, 0);
    Ok(())
}
fn record(epoch: u64) -> Record {
    Record {
        key: "immutable1".into(),
        epoch,
        writer: writer(),
        endpoint: "http://collector1:50051".into(),
        lifecycle: Lifecycle::Active,
        head: None,
        pending: vec![],
        migration: None,
    }
}
fn config(storage: std::path::PathBuf, fingerprint: String) -> Config {
    Config {
        delta: None,
        tenant: "bound-tenant".into(),
        catalog_id: "catalog1".into(),
        tables: vec![Table {
            key: "immutable1".into(),
            path: vec!["c".into(), "s".into(), "t".into()],
            fingerprints: vec![fingerprint],
        }],
        storage,
        controller: String::new(),
        operator_token: String::new(),
        gateway_token: String::new(),
        edge_secret: "e".repeat(32),
        internal_secret: "i".repeat(32),
        collectors: BTreeMap::new(),
        etcd: vec![],
        etcd_user: String::new(),
        etcd_password: String::new(),
    }
}
#[test]
fn directory_rejects_old_events_and_requires_resnapshot_on_revision_regression() -> anyhow::Result<()> {
    let mut directory = Directory::default();
    directory.snapshot(
        9,
        vec![Assignment {
            revision: 9,
            record: record(2),
        }],
    )?;
    directory.updates(
        11,
        vec![Assignment {
            revision: 8,
            record: record(1),
        }],
    )?;
    assert_eq!(directory.entries["immutable1"].record.epoch, 2);
    assert!(directory.updates(7, vec![]).is_err());
    directory.snapshot(
        12,
        vec![Assignment {
            revision: 12,
            record: record(3),
        }],
    )?;
    directory.snapshot(
        10,
        vec![Assignment {
            revision: 10,
            record: record(2),
        }],
    )?;
    assert_eq!(directory.entries["immutable1"].record.epoch, 3);
    assert_eq!(directory.snapshots, 2);
    directory.invalidate("immutable1", &tonic::Status::unavailable("NOT_OWNER"));
    assert!(directory.entries.is_empty());
    assert_eq!(directory.stale_invalidations, 1);
    assert_eq!(directory.route_error_invalidations, 1);
    directory.invalidate("immutable1", &tonic::Status::unavailable("other transport failure"));
    assert_eq!(directory.stale_invalidations, 1);
    assert_eq!(directory.route_error_invalidations, 2);
    Ok(())
}
#[test]
fn immutable_manifest_chain_rejects_overwrite_path_escape_and_corruption() -> anyhow::Result<()> {
    let root = std::env::current_dir()?
        .join(std::env::var_os("CARGO_TARGET_DIR").unwrap_or_else(|| "target".into()))
        .join(format!("contract-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("marker", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let fingerprint = arrowcol_protocol::fingerprint_schema(&schema).to_string();
    let config = config(root.clone(), fingerprint.clone());
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["m/0"])),
            Arc::new(Int64Array::from(vec![7])),
        ],
    )?;
    let manifest = Manifest {
        table: "immutable1".into(),
        writer: writer(),
        epoch: 1,
        commit: "commit1".into(),
        unit: "unit1".into(),
        parent: None,
        parquet: String::new(),
        parquet_sha256: String::new(),
        rows: 1,
        arrow_bytes: batch.get_array_memory_size(),
        fingerprint,
    };
    let head = storage::stage(&config, manifest.clone(), &batch)?;
    assert_eq!(storage::validate(&config, &head)?.rows, 1);
    assert!(storage::stage(&config, manifest.clone(), &batch).is_err());
    let second = Manifest {
        commit: "commit2".into(),
        parent: Some(head.clone()),
        ..manifest
    };
    let next = storage::stage(&config, second, &batch)?;
    assert!(storage::contains(&config, Some(next.clone()), &head)?);
    let mut bad = next.clone();
    bad.manifest = "../outside".into();
    assert!(storage::load(&config, &bad).is_err());
    std::fs::write(root.join(&next.manifest), b"corrupted")?;
    assert!(storage::validate(&config, &next).is_err());
    std::fs::remove_dir_all(root)?;
    Ok(())
}
#[test]
fn table_identity_is_not_schema_identity_and_schema_evolution_stays_on_same_key() {
    let config = config("target/unused".into(), "shared".into());
    let first = config.tables[0].clone();
    let second = Table {
        key: "immutable2".into(),
        path: vec!["c".into(), "s".into(), "other".into()],
        fingerprints: first.fingerprints.clone(),
    };
    assert_ne!(first.key, second.key);
    let evolved = Table {
        fingerprints: vec!["shared".into(), "evolved".into()],
        ..first.clone()
    };
    assert_eq!(first.key, evolved.key);
    assert!(config.table("missing").is_err());
    assert!(storage::namespace("../escape", &writer(), "commit").is_err());
    let mut fake = writer();
    fake.process = "../process".into();
    assert!(storage::namespace("table", &fake, "commit").is_err());
}

#[tokio::test]
async fn observation_counts_owned_queue_bytes_and_releases_them_on_cancellation() -> anyhow::Result<()> {
    let gauge = observation::Gauge::default();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    tx.send(gauge.hold(7)).await?;
    let held = gauge.hold(11);
    let pending = tokio::spawn(async move { tx.send(held).await });
    assert_eq!(gauge.sample().current, 18);
    assert_eq!(gauge.sample().lifetime_high_water, 18);
    pending.abort();
    let _ = pending.await;
    assert_eq!(gauge.sample().current, 7);
    drop(rx.recv().await);
    assert_eq!(gauge.sample().current, 0);
    assert_eq!(gauge.sample().lifetime_high_water, 18);
    Ok(())
}
