use std::{sync::atomic::Ordering, time::Duration};

use crate::{DdlIdentityPolicy, DdlIdentityVerifier};
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
        ingestion_layout: false,
    }
}

#[tokio::test]
async fn ingestion_catalog_adopts_bootstrap_without_exposing_physical_columns() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut config = config(root.path());
    config.ingestion_layout = true;
    let location = root.path().join("warehouse/telemetry/sqlserver/events");
    std::fs::create_dir_all(&location)?;
    let url = Url::from_directory_path(&location).map_err(|()| anyhow::anyhow!("invalid test path"))?;
    let sql = "CREATE TABLE telemetry.sqlserver.events (id BIGINT NOT NULL, observed TIMESTAMP) USING DELTA";
    let DdlCommand::CreateTable(create) = DdlParser::default().parse(sql)? else {
        bail!("expected CREATE");
    };
    let mut fields = create
        .fields()
        .iter()
        .map(|field| field.to_arrow())
        .collect::<Result<Vec<_>, _>>()?;
    fields.push(arrow_schema::Field::new(INGESTION, arrow_schema::DataType::Utf8, false));
    let physical = Arc::new(arrow_schema::Schema::new(fields));
    let table = DeltaTable::open_url(url, physical, vec![INGESTION.into()])?;
    let version = table.version()?;
    let service = CatalogService::open(config.clone()).await?;
    service.apply_sql(sql).await?;
    assert!(table.version()? > version);
    let identity = name_identity(&["telemetry".into(), "sqlserver".into(), "events".into()])?;
    let history = service.store.schema_history(&identity).await?;
    assert_eq!(history[0].fields().len(), 2);
    assert_eq!(
        table.catalog_snapshot()?.context("head")?.partition_columns,
        [INGESTION]
    );
    let restarted = CatalogService::open(config.clone()).await?;
    restarted.apply_sql(sql).await?;
    assert_eq!(restarted.store.schema_history(&identity).await?[0].fields().len(), 2);
    assert!(
        restarted
            .apply_sql("CREATE TABLE telemetry.sqlserver.events (wrong STRING) USING DELTA")
            .await
            .is_err()
    );
    restarted
        .apply_sql("ALTER TABLE telemetry.sqlserver.events ADD COLUMNS (next_observed TIMESTAMP)")
        .await?;
    let snapshot = table.catalog_snapshot()?.context("head after ALTER")?;
    assert_eq!(snapshot.schema.fields()[3].name(), INGESTION);
    assert_eq!(snapshot.schema.fields()[4].name(), "next_observed");
    let reopened = CatalogService::open(config).await?;
    assert_eq!(
        reopened
            .store
            .schema_history(&identity)
            .await?
            .last()
            .context("head")?
            .fields()
            .len(),
        3
    );
    Ok(())
}

const CREATE: &str = "CREATE TABLE arrowcol.default.events (id BIGINT NOT NULL, day STRING) USING DELTA PARTITIONED BY (day) COMMENT 'catalog test' TBLPROPERTIES ('vendor.setting' = 'original')";

#[tokio::test]
async fn committed_delta_restart_and_concurrent_evolution() -> Result<()> {
    const CHILD: &str = "ARROWCOL_CATALOG_PUBLICATION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "service::tests::committed_delta_restart_and_concurrent_evolution",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("ARROWCOL_PARTITION_GRANULARITY", "minute")
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "catalog CREATE-to-publication subprocess failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }
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
    restarted
        .apply_sql("CREATE TABLE arrowcol.default.plain (id BIGINT NOT NULL) USING DELTA")
        .await?;
    let plain = name_identity(&["arrowcol".into(), "default".into(), "plain".into()])?;
    let location = restarted.state.lock().await.tables[&plain].location.clone();
    let path = PathBuf::from(location);
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
    )?;
    let table = DeltaTable::open(path)?;
    for _ in 0..2 {
        let mut files = table.new_partitioned_publication_files(batch.clone(), "catalog-plain".into(), 0, 1)?;
        assert_eq!(files.len(), 1);
        let (mut writer, batch) = files.pop().context("publication file")?;
        writer.write(&batch).await?;
        let committed = writer.finish().await?;
        assert_eq!(
            committed.version, 1,
            "CREATE and replay must not add implicit partitions or duplicate rows"
        );
        assert_eq!(committed.file.rows, 3);
    }
    assert!(
        restarted
            .store
            .current_schema(&plain)
            .await?
            .context("plain schema")?
            .metadata()
            .partition_columns
            .is_empty()
    );
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
async fn flight_sql_ddl_requires_signed_identity_before_mutating_catalog() -> Result<()> {
    let root = tempfile::tempdir()?;
    let service = CatalogService::open(config(root.path())).await?;
    let verifier = DdlIdentityVerifier::from_secret(b"catalog-integration-key-at-least-32-bytes".to_vec())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server_service = service.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(
                server_service.flight_service_with_ddl_identity(DdlIdentityPolicy::require_signed(verifier)),
            ))
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
    });
    let channel = tonic::transport::Endpoint::from_shared(endpoint)?.connect().await?;
    let mut flight = arrow_flight::flight_service_client::FlightServiceClient::new(channel);
    let command = CommandStatementUpdate {
        query: CREATE.to_owned(),
        transaction_id: None,
    };
    let error = flight
        .do_put(futures::stream::iter([arrow_flight::FlightData {
            flight_descriptor: Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
                cmd: command.as_any().encode_to_vec().into(),
                path: Vec::new(),
            }),
            ..arrow_flight::FlightData::default()
        }]))
        .await
        .err()
        .context("unsigned DDL must be rejected")?;
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
    let identity = name_identity(&["arrowcol".into(), "default".into(), "events".into()])?;
    assert!(service.store.current_schema(&identity).await?.is_none());
    use arrowcol_protocol::{
        AuthContext, AuthMethod, AuthenticatedPrincipal, PrincipalId, PrincipalIssuer, PrincipalScope, UnixMillis,
    };
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::{
        collections::BTreeSet,
        time::{SystemTime, UNIX_EPOCH},
    };

    let now_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let principal = AuthenticatedPrincipal::new(
        PrincipalId::new("loadtest-catalog")?,
        PrincipalIssuer::new("arrowcol-loadgen")?,
        BTreeSet::from([PrincipalScope::new("arrowcol/catalog")?]),
    )?;
    let context = AuthContext::new(principal, AuthMethod::WorkloadIdentity, UnixMillis::new(now_ms)?, None)?;
    let encoded_context = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&context)?);
    let expiry = now_ms.checked_add(30_000).context("identity expiry overflow")?;
    let mut mac = Hmac::<Sha256>::new_from_slice(b"catalog-integration-key-at-least-32-bytes")?;
    mac.update(format!("{encoded_context}\n{expiry}").as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    let command = CommandStatementUpdate {
        query: CREATE.to_owned(),
        transaction_id: None,
    };
    let mut request = tonic::Request::new(futures::stream::iter([arrow_flight::FlightData {
        flight_descriptor: Some(FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
            cmd: command.as_any().encode_to_vec().into(),
            path: Vec::new(),
        }),
        ..arrow_flight::FlightData::default()
    }]));
    request
        .metadata_mut()
        .insert("x-arrowcol-internal-auth-context", encoded_context.parse()?);
    request
        .metadata_mut()
        .insert("x-arrowcol-internal-auth-expires", expiry.to_string().parse()?);
    request
        .metadata_mut()
        .insert("x-arrowcol-internal-auth-signature", signature.parse()?);
    let response = flight.do_put(request).await?;
    assert_eq!(
        arrowcol_flight::decode_ddl_completion_metadata(response.metadata())?.table(),
        "events"
    );
    let mut results = response.into_inner();
    assert!(results.message().await?.is_some());
    assert!(service.store.current_schema(&identity).await?.is_some());
    let _ = shutdown.send(());
    server.await??;
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
            .add_service(FlightServiceServer::new(
                server_service.flight_service_with_ddl_identity(DdlIdentityPolicy::dev_only_allow_unsigned()),
            ))
            .add_service(SchemaCatalogServer::new(server_service))
            .serve_with_incoming_shutdown(tokio_stream::wrappers::TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            })
            .await
    });
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())?
        .connect()
        .await?;
    let mut flight = arrow_flight::flight_service_client::FlightServiceClient::new(channel.clone());
    for disposition in ["created", "already_applied"] {
        let command = CommandStatementUpdate {
            query: CREATE.to_owned(),
            transaction_id: None,
        };
        let response = flight
            .do_put(futures::stream::iter([FlightData {
                flight_descriptor: Some(FlightDescriptor {
                    r#type: arrow_flight::flight_descriptor::DescriptorType::Cmd as i32,
                    cmd: command.as_any().encode_to_vec().into(),
                    path: Vec::new(),
                }),
                ..FlightData::default()
            }]))
            .await?;
        let completion = arrowcol_flight::decode_ddl_completion_metadata(response.metadata())?;
        assert_eq!(completion.catalog(), "arrowcol");
        assert_eq!(completion.schema(), "default");
        assert_eq!(completion.table(), "events");
        assert_eq!(completion.schema_version().get(), 1);
        assert_eq!(completion.disposition(), disposition);
        let identity = name_identity(&["arrowcol".into(), "default".into(), "events".into()])?;
        assert_eq!(
            completion.schema_fingerprint(),
            service
                .store
                .current_schema(&identity)
                .await?
                .context("committed schema")?
                .fingerprint()
        );
        let mut results = response.into_inner();
        let result = results.message().await?.context("DDL result")?;
        assert_eq!(
            arrow_flight::sql::DoPutUpdateResult::decode(result.app_metadata)?.record_count,
            0
        );
        assert!(results.message().await?.is_none());
    }
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
    let restarted = CatalogService::open(config(root.path())).await?;
    restarted
        .apply_sql("ALTER TABLE arrowcol.default.events ADD COLUMNS (while_disconnected STRING)")
        .await?;
    let listener = tokio::net::TcpListener::bind(
        url::Url::parse(&endpoint)?
            .socket_addrs(|| None)?
            .first()
            .context("endpoint address")?,
    )
    .await?;
    let server_service = restarted.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(
                server_service.flight_service_with_ddl_identity(DdlIdentityPolicy::dev_only_allow_unsigned()),
            ))
            .add_service(SchemaCatalogServer::new(server_service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
    let outcome = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(table) = client.table(&identity).await
                && table.position.epoch != evolved.position.epoch
                && table.versions.last().is_some_and(|version| version.fields().len() == 4)
            {
                break table;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    server.abort();
    let _ = server.await;
    let recovered = outcome.context("same client did not reconcile missed ALTER after catalog restart")?;
    assert!(
        client.metrics().catalog_calls > baseline,
        "reconnect must start real reconciliation RPCs"
    );
    assert_ne!(
        recovered.position.epoch, evolved.position.epoch,
        "restart must replace epoch"
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
