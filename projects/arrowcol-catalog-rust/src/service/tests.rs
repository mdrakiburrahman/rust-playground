use std::{sync::atomic::Ordering, time::Duration};

use arrow_flight::{flight_service_server::FlightServiceServer, sql::client::FlightSqlServiceClient};
use arrowcol_catalog::{CatalogStore, SchemaAlignmentPlan, SchemaValidator};
use arrowcol_catalog_client::{CatalogClient, RemoteCatalogClient};
use arrowcol_protocol::{ClaimedSchemaFingerprint, fingerprint_schema};
use futures::TryStreamExt;
use rpc::schema_catalog_server::{SchemaCatalog, SchemaCatalogServer};

use super::*;

fn config(root: &Path) -> CatalogConfig {
    CatalogConfig {
        warehouse_root: root.join("warehouse").to_string_lossy().into_owned(),
        state_dir: root.join("catalog"),
        ddl_dir: None,
        retained_events: 4,
    }
}

const CREATE: &str = "CREATE TABLE arrowcol.default.events (id BIGINT NOT NULL, day STRING) USING DELTA PARTITIONED BY (day) COMMENT 'catalog test' TBLPROPERTIES ('vendor.setting' = 'original')";

#[tokio::test]
async fn committed_delta_restart_and_concurrent_evolution() -> Result<()> {
    let root = tempfile::tempdir()?;
    let service = CatalogService::open(config(root.path())).await?;
    service.apply_sql(CREATE).await?;
    let identity = name_identity(&["arrowcol".into(), "default".into(), "events".into()])?;
    let old = service.store.schema_history(&identity).await?[0].clone();
    let (first, second, props) = tokio::join!(
        service.apply_sql("ALTER TABLE arrowcol.default.events ADD COLUMNS (a STRING)"),
        service.apply_sql("ALTER TABLE arrowcol.default.events ADD COLUMNS (b INT)"),
        service.apply_sql("ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('vendor.setting'='changed')"),
    );
    first?;
    second?;
    props?;
    let restarted = CatalogService::open(config(root.path())).await?;
    let history = restarted.store.schema_history(&identity).await?;
    let head = history.last().context("missing head")?;
    assert_eq!(head.fields().len(), 4);
    assert_eq!(head.metadata().properties["vendor.setting"], "changed");
    assert_eq!(head.metadata().partition_columns, ["day"]);
    assert_eq!(head.metadata().comment.as_deref(), Some("catalog test"));
    assert_eq!(history[0].fields(), old.fields());
    assert_eq!(
        history[0].metadata().properties["vendor.setting"],
        "original",
        "property updates must not rewrite historical descriptor versions"
    );
    let old_arrow = old.arrow_schema()?;
    let validated = SchemaValidator::new(Arc::clone(&restarted.store) as Arc<dyn CatalogStore>)
        .validate(
            &identity,
            ClaimedSchemaFingerprint::new(fingerprint_schema(&old_arrow)),
            &old_arrow,
        )
        .await?;
    assert!(matches!(
        validated.alignment(),
        SchemaAlignmentPlan::NullableAppend { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn commit_publication_crash_and_old_seed_do_not_rollback_delta() -> Result<()> {
    let root = tempfile::tempdir()?;
    let service = CatalogService::open(config(root.path())).await?;
    service.apply_sql(CREATE).await?;
    service.fail_after_commit.store(true, Ordering::SeqCst);
    assert!(
        service
            .apply_sql("ALTER TABLE arrowcol.default.events ADD COLUMNS (after_crash STRING)")
            .await
            .is_err()
    );
    assert_eq!(
        service.state.lock().await.event_id,
        1,
        "failed publication must emit no event"
    );
    let seeds = root.path().join("seeds");
    std::fs::create_dir_all(&seeds)?;
    std::fs::write(seeds.join("events.sql"), CREATE)?;
    let mut cfg = config(root.path());
    cfg.ddl_dir = Some(seeds);
    let restarted = CatalogService::open(cfg).await?;
    let snapshot = restarted.get_snapshot(Request::new(rpc::Empty {})).await?.into_inner();
    let table = arrowcol_catalog_client::decode_table(snapshot.tables[0].clone())?;
    assert_eq!(table.versions.last().context("head")?.fields().len(), 3);
    assert_eq!(snapshot.position.as_ref().context("position")?.event_id, 0);
    let old_position = service
        .get_current_event_id(Request::new(rpc::Empty {}))
        .await?
        .into_inner();
    assert_eq!(
        restarted
            .get_events(Request::new(rpc::EventsRequest {
                from: Some(old_position)
            }))
            .await
            .err()
            .context("expected epoch error")?
            .code(),
        tonic::Code::OutOfRange
    );
    Ok(())
}

#[tokio::test]
async fn flight_sql_and_warm_cache_count_actual_catalog_calls() -> Result<()> {
    let root = tempfile::tempdir()?;
    let service = CatalogService::open(config(root.path())).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server_service = service.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(server_service.clone()))
            .add_service(SchemaCatalogServer::new(server_service))
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
    });
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())?
        .connect()
        .await?;
    let mut sql = FlightSqlServiceClient::new(channel);
    assert_eq!(sql.execute_update(CREATE.to_owned(), None).await?, 0);
    let prepared = sql.execute("SHOW TABLES IN arrowcol.default".to_owned(), None).await?;
    let ticket = prepared.endpoint[0].ticket.clone().context("query ticket")?;
    let batches = sql.do_get(ticket).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(batches.iter().map(arrow_array::RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(
        sql.execute_update(CREATE.to_owned(), Some(vec![1].into()))
            .await
            .is_err()
    );
    let client = RemoteCatalogClient::connect(&endpoint, Duration::from_millis(250)).await?;
    let identity = name_identity(&["arrowcol".into(), "default".into(), "events".into()])?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let baseline = client.metrics().catalog_calls;
    let original = client.table(&identity).await?;
    let schema = original.versions[0].arrow_schema()?;
    let validator = SchemaValidator::new(Arc::clone(&client) as Arc<dyn CatalogStore>);
    for _ in 0..100 {
        validator
            .validate(
                &identity,
                ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
                &schema,
            )
            .await?;
    }
    assert_eq!(
        client.metrics().catalog_calls,
        baseline,
        "100 warm streams must perform zero catalog RPCs"
    );
    sql.execute_update(
        "ALTER TABLE arrowcol.default.events ADD COLUMNS (new_value STRING)".to_owned(),
        None,
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if client.metrics().invalidations > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let evolved = client.table(&identity).await?;
    assert_eq!(evolved.versions.last().context("head")?.fields().len(), 3);
    let validated = validator
        .validate(
            &identity,
            ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
            &schema,
        )
        .await?;
    assert!(matches!(
        validated.alignment(),
        SchemaAlignmentPlan::NullableAppend { .. }
    ));
    let wrong = arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("day", arrow_schema::DataType::Utf8, true),
    ]);
    assert!(
        validator
            .validate(
                &identity,
                ClaimedSchemaFingerprint::new(fingerprint_schema(&wrong)),
                &wrong
            )
            .await
            .is_err()
    );
    shutdown
        .send(())
        .map_err(|()| anyhow::anyhow!("server already stopped"))?;
    // Close CES before waiting for tonic's graceful streaming shutdown.
    drop(client);
    server.abort();
    let _ = server.await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        validator
            .validate(
                &identity,
                ClaimedSchemaFingerprint::new(fingerprint_schema(&schema)),
                &schema
            )
            .await
            .is_err(),
        "catalog outage must not silently accept an expired cached schema"
    );
    Ok(())
}

#[tokio::test]
async fn reserved_properties_failed_ddl_and_retained_gap() -> Result<()> {
    let root = tempfile::tempdir()?;
    let service = CatalogService::open(config(root.path())).await?;
    service.apply_sql(CREATE).await?;
    let from = service
        .get_current_event_id(Request::new(rpc::Empty {}))
        .await?
        .into_inner();
    for sql in [
        "ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('catalog.schemaHistory'='forged')",
        "ALTER TABLE arrowcol.default.events UNSET TBLPROPERTIES ('catalog.dropped')",
        "ALTER TABLE arrowcol.default.events ADD COLUMNS (required BIGINT NOT NULL)",
        "ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('arrowcol.unknown'='oops')",
        "ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('delta.columnMapping.mode'='name')",
        "ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('delta.enableDeletionVectors'='true')",
        "ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('delta.enableRowTracking'='true')",
        "ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('delta.appendOnly'='not-a-boolean')",
    ] {
        assert!(service.apply_sql(sql).await.is_err(), "invalid DDL was accepted: {sql}");
        assert_eq!(service.state.lock().await.event_id, from.event_id);
    }
    for index in 0..6 {
        service
            .apply_sql(&format!(
                "ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('vendor.setting'='{index}')"
            ))
            .await?;
    }
    let event_id = service.state.lock().await.event_id;
    service
        .apply_sql("ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('vendor.setting'='5')")
        .await?;
    assert_eq!(
        service.state.lock().await.event_id,
        event_id,
        "identical properties are a no-op"
    );
    let (tables, position) = service
        .query("SHOW TABLES IN arrowcol.default LIKE 'ev*'", None)
        .await?;
    assert_eq!(tables.num_rows(), 1);
    let (properties, _) = service
        .query("SHOW TBLPROPERTIES arrowcol.default.events", None)
        .await?;
    assert_eq!(properties.num_rows(), 1);
    let (description, _) = service.query("DESCRIBE TABLE arrowcol.default.events", None).await?;
    assert_eq!(description.num_rows(), 2);
    service
        .apply_sql("ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('vendor.setting'='latest')")
        .await?;
    assert!(
        service.query("SHOW TABLES", Some(position)).await.is_err(),
        "prepared query schema must not race catalog changes"
    );
    assert_eq!(
        service
            .get_events(Request::new(rpc::EventsRequest { from: Some(from) }))
            .await
            .err()
            .context("expected retained-gap error")?
            .code(),
        tonic::Code::OutOfRange
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_watch_replays_racing_ddl_and_drop_retry_retains_delta() -> Result<()> {
    let root = tempfile::tempdir()?;
    let service = CatalogService::open(config(root.path())).await?;
    service.apply_sql(CREATE).await?;
    assert!(
        service
            .apply_sql(&CREATE.replace("catalog test", "different comment"))
            .await
            .is_err()
    );
    let from = service
        .get_snapshot(Request::new(rpc::Empty {}))
        .await?
        .into_inner()
        .position
        .context("snapshot watermark")?;
    service
        .apply_sql("ALTER TABLE arrowcol.default.events ADD COLUMNS (racing STRING)")
        .await?;
    let mut stream = service
        .watch(Request::new(rpc::EventsRequest {
            from: Some(from.clone()),
        }))
        .await?
        .into_inner();
    let replay = stream.next().await.context("replayed event")??;
    assert_eq!(replay.position.context("position")?.event_id, from.event_id + 1);
    service
        .apply_sql("ALTER TABLE arrowcol.default.events SET TBLPROPERTIES ('vendor.setting'='subscribed')")
        .await?;
    let live = stream.next().await.context("subscribed event")??;
    assert_eq!(live.position.context("position")?.event_id, from.event_id + 2);
    let identity = name_identity(&["arrowcol".into(), "default".into(), "events".into()])?;
    let before = service.state.lock().await.event_id;
    service.fail_after_commit.store(true, Ordering::SeqCst);
    assert!(service.apply_sql("DROP TABLE arrowcol.default.events").await.is_err());
    assert_eq!(
        service.state.lock().await.event_id,
        before,
        "failed publication cannot publish a DROP event"
    );
    service.apply_sql("DROP TABLE arrowcol.default.events").await?;
    assert_eq!(
        service.state.lock().await.event_id,
        before + 1,
        "unknown-success DROP retry publishes once"
    );
    assert!(service.store.current_schema(&identity).await?.is_none());
    assert!(service.store.schema_history(&identity).await?.is_empty());
    let restarted = CatalogService::open(config(root.path())).await?;
    assert!(
        restarted
            .get_snapshot(Request::new(rpc::Empty {}))
            .await?
            .into_inner()
            .tables
            .is_empty()
    );
    let delta = read_delta(&service.default_location(&identity))
        .await?
        .context("retained Delta table")?;
    assert_eq!(delta.properties.get(DROPPED_KEY).map(String::as_str), Some("true"));
    Ok(())
}
