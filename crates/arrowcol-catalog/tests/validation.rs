use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use arrowcol_catalog::{
    AlignmentAction, CatalogControlPlane, CatalogErrorCode, CatalogStore, DdlParser, FlightSqlDdlHandler,
    FlightSqlDdlRequest, MemoryCatalogStore, MemoryMetastore, SchemaAlignmentPlan, SchemaValidator,
};
use arrowcol_protocol::{ClaimedSchemaFingerprint, fingerprint_schema};

#[tokio::test]
async fn exact_and_registered_historical_schemas_are_accepted() -> anyhow::Result<()> {
    let store = Arc::new(MemoryCatalogStore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    let created = service
        .execute_ddl(FlightSqlDdlRequest::new(
            "CREATE TABLE telemetry.prod.validate_me (id BIGINT NOT NULL, body STRING) USING DELTA",
        ))
        .await?;
    let historical = store
        .current_schema(created.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing created schema"))?;
    service
        .execute_ddl(FlightSqlDdlRequest::new(
            "ALTER TABLE telemetry.prod.validate_me ADD COLUMNS \
             (severity SMALLINT, attributes MAP<STRING, STRING>)",
        ))
        .await?;
    let current = store
        .current_schema(created.identity())
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing current schema"))?;
    let validator = SchemaValidator::new(store);

    let current_arrow = current.arrow_schema()?;
    let current_validated = validator
        .validate(
            created.identity(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(&current_arrow)),
            &current_arrow,
        )
        .await?;
    assert!(matches!(
        current_validated.alignment(),
        SchemaAlignmentPlan::Exact { .. }
    ));

    let historical_arrow = historical.arrow_schema()?;
    let historical_validated = validator
        .validate(
            created.identity(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(&historical_arrow)),
            &historical_arrow,
        )
        .await?;
    let SchemaAlignmentPlan::NullableAppend { actions, .. } = historical_validated.alignment() else {
        anyhow::bail!("expected nullable append plan");
    };
    assert_eq!(actions.len(), 4);
    assert!(matches!(
        actions.as_slice(),
        [
            AlignmentAction::Source { .. },
            AlignmentAction::Source { .. },
            AlignmentAction::NullFill { .. },
            AlignmentAction::NullFill { .. }
        ]
    ));
    Ok(())
}

#[tokio::test]
async fn unknown_and_arbitrary_missing_schemas_are_rejected() -> anyhow::Result<()> {
    let store = Arc::new(MemoryCatalogStore::new());
    let service = CatalogControlPlane::new(
        DdlParser::default(),
        Arc::<MemoryCatalogStore>::clone(&store),
        Arc::new(MemoryMetastore::new()),
    );
    let completion = service
        .execute_ddl(FlightSqlDdlRequest::new(
            "CREATE TABLE telemetry.prod.strict_schema \
             (id BIGINT NOT NULL, body STRING, severity SMALLINT) USING DELTA",
        ))
        .await?;
    let validator = SchemaValidator::new(store);
    let arbitrary_prefix = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]);
    let result = validator
        .validate(
            completion.identity(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(&arbitrary_prefix)),
            &arbitrary_prefix,
        )
        .await;
    let Err(error) = result else {
        anyhow::bail!("unregistered prefix must be rejected");
    };
    assert_eq!(error.code(), CatalogErrorCode::UnknownSchemaFingerprint);

    let wrong_claim = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let result = validator
        .validate(
            completion.identity(),
            ClaimedSchemaFingerprint::new(completion.fingerprint()),
            &wrong_claim,
        )
        .await;
    let Err(error) = result else {
        anyhow::bail!("claim mismatch must be rejected");
    };
    assert_eq!(error.code(), CatalogErrorCode::UnknownSchemaFingerprint);
    Ok(())
}
