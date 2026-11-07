use std::sync::Arc;

use arrowcol_catalog::{
    CatalogControlPlane, CatalogErrorCode, CatalogStore, DdlCompletionDisposition, DdlParser, FailurePoint,
    FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore, MemoryMetastore,
};

fn create_ddl() -> FlightSqlDdlRequest {
    FlightSqlDdlRequest::new("CREATE TABLE telemetry.prod.events (id BIGINT NOT NULL, body STRING) USING DELTA")
}

#[tokio::test]
async fn create_append_and_retry_preserve_identity_and_numbers() -> anyhow::Result<()> {
    let store = Arc::new(MemoryCatalogStore::new());
    let metastore = Arc::new(MemoryMetastore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        metastore,
    );

    let created = service.execute_ddl(create_ddl()).await?;
    assert_eq!(created.disposition(), DdlCompletionDisposition::Created);
    assert_eq!(created.schema_version().get(), 1);

    let retry = service.execute_ddl(create_ddl()).await?;
    assert_eq!(retry.disposition(), DdlCompletionDisposition::AlreadyApplied);
    assert_eq!(retry.fingerprint(), created.fingerprint());

    let alter_request = || {
        FlightSqlDdlRequest::new(
            "ALTER TABLE telemetry.prod.events ADD COLUMNS \
             (severity SMALLINT, attributes MAP<STRING, STRING>)",
        )
    };
    let altered = service.execute_ddl(alter_request()).await?;
    assert_eq!(altered.disposition(), DdlCompletionDisposition::Altered);
    assert_eq!(altered.schema_version().get(), 2);
    let alter_retry = service.execute_ddl(alter_request()).await?;
    assert_eq!(alter_retry.disposition(), DdlCompletionDisposition::AlreadyApplied);
    assert_eq!(alter_retry.schema_version(), altered.schema_version());

    let history = store.schema_history(altered.identity()).await?;
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].fields()[0].id(), history[1].fields()[0].id());
    assert_eq!(
        history[0].fields()[0].field_number(),
        history[1].fields()[0].field_number()
    );
    assert_eq!(history[1].fields()[2].position(), 2);
    assert_eq!(history[1].fields()[2].field_number(), 3);
    assert_eq!(history[1].fields()[3].position(), 3);
    assert_eq!(history[1].fields()[3].field_number(), 4);
    Ok(())
}

#[tokio::test]
async fn concurrent_same_ddl_coalesces_to_one_version() -> anyhow::Result<()> {
    let store = Arc::new(MemoryCatalogStore::new());
    let metastore = Arc::new(MemoryMetastore::new());
    let service = Arc::new(CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        metastore,
    ));

    let (left, right) = tokio::join!(service.execute_ddl(create_ddl()), service.execute_ddl(create_ddl()));
    let left = left?;
    let right = right?;
    assert_eq!(left.fingerprint(), right.fingerprint());
    let dispositions = [left.disposition(), right.disposition()];
    assert!(dispositions.contains(&DdlCompletionDisposition::Created));
    assert!(dispositions.contains(&DdlCompletionDisposition::AlreadyApplied));
    assert_eq!(store.schema_history(left.identity()).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn hive_failure_is_visible_and_reconciles() -> anyhow::Result<()> {
    let store = Arc::new(MemoryCatalogStore::new());
    let metastore = Arc::new(MemoryMetastore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::<MemoryMetastore>::clone(&metastore),
    );
    service.execute_ddl(create_ddl()).await?;

    metastore.fail_next(FailurePoint::HiveApply).await;
    let result = service
        .execute_ddl(FlightSqlDdlRequest::new(
            "ALTER TABLE telemetry.prod.events ADD COLUMNS (retryable STRING)",
        ))
        .await;
    let Err(error) = result else {
        anyhow::bail!("injected Hive failure must be visible");
    };
    assert_eq!(error.code(), CatalogErrorCode::HiveMetastore);
    let incomplete = store.incomplete_sagas().await?;
    assert_eq!(incomplete.len(), 1);
    assert_eq!(incomplete[0].attempt_count(), 1);
    assert!(incomplete[0].last_error().is_some());

    let report = service.reconcile_incomplete().await?;
    assert_eq!(report.examined, 1);
    assert_eq!(report.completed, 1);
    assert!(report.failures.is_empty());
    let current = store
        .current_schema(incomplete[0].target_schema().identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing current schema after reconciliation"))?;
    assert_eq!(current.version().get(), 2);
    Ok(())
}

#[tokio::test]
async fn failure_after_hive_apply_reconciles_idempotently() -> anyhow::Result<()> {
    let store = Arc::new(MemoryCatalogStore::new());
    let metastore = Arc::new(MemoryMetastore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        metastore,
    );
    store.fail_next(FailurePoint::MarkHiveApplied).await;
    let result = service.execute_ddl(create_ddl()).await;
    let Err(error) = result else {
        anyhow::bail!("application-store failure must be visible");
    };
    assert_eq!(error.code(), CatalogErrorCode::ApplicationDatabase);

    let report = service.reconcile_incomplete().await?;
    assert_eq!(report.completed, 1);
    assert!(report.failures.is_empty());
    let identity = create_identity()?;
    assert_eq!(store.schema_history(&identity).await?.len(), 1);
    Ok(())
}

fn create_identity() -> anyhow::Result<arrowcol_protocol::TableIdentity> {
    Ok(arrowcol_protocol::TableIdentity::new(
        arrowcol_protocol::CatalogName::new("telemetry")?,
        arrowcol_protocol::SchemaName::new("prod")?,
        arrowcol_protocol::TableName::new("events")?,
    ))
}
