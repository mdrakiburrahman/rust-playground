use arrow_schema::{DataType, TimeUnit};
use arrowcol_catalog::{CatalogErrorCode, CatalogType, DdlCommand, DdlParser, ListKind};

fn synthetic_create() -> &'static str {
    r#"
        CREATE TABLE telemetry.prod.synthetic (
            sequence BIGINT NOT NULL,
            service STRING,
            observed_at TIMESTAMP_NTZ NOT NULL,
            trace_id BINARY,
            summary ARRAY<STRUCT<quantile: DOUBLE, value: DOUBLE>>,
            event_date DATE NOT NULL
        )
        USING DELTA PARTITIONED BY (event_date)
    "#
}

#[test]
fn parses_synthetic_create_with_exact_cross_system_types() -> anyhow::Result<()> {
    let DdlCommand::CreateTable(create) = DdlParser::default().parse(synthetic_create())? else {
        anyhow::bail!("expected create table");
    };
    assert_eq!(create.identity().to_string(), "telemetry.prod.synthetic");
    assert_eq!(create.fields().len(), 6);
    assert_eq!(create.fields()[0].mapping().arrow(), &CatalogType::Int64);
    assert_eq!(create.fields()[0].mapping().hive(), "bigint");
    assert_eq!(
        create.fields()[2].mapping().arrow(),
        &CatalogType::Timestamp {
            unit: arrowcol_catalog::TimeUnit::Microsecond,
            timezone: None,
        }
    );
    assert_eq!(create.fields()[3].mapping().arrow(), &CatalogType::Binary);
    assert!(matches!(
        create.fields()[4].mapping().arrow(),
        CatalogType::List {
            list_kind: ListKind::List,
            ..
        }
    ));
    assert_eq!(create.fields()[5].partition().map(|p| p.ordinal()), Some(0));

    let schema = arrow_schema::Schema::new(
        create
            .fields()
            .iter()
            .map(|field| field.to_arrow())
            .collect::<Result<Vec<_>, _>>()?,
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(
        schema.field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    Ok(())
}

#[test]
fn parses_plural_nullable_add_columns() -> anyhow::Result<()> {
    let command = DdlParser::default().parse(
        "ALTER TABLE telemetry.prod.synthetic ADD COLUMNS \
         (attributes MAP<STRING, STRING>, status SMALLINT)",
    )?;
    let DdlCommand::AddColumns(add) = command else {
        anyhow::bail!("expected add columns");
    };
    assert_eq!(add.fields().len(), 2);
    assert!(add.fields().iter().all(|field| field.is_nullable()));
    assert_eq!(
        add.canonical_ddl(),
        "ALTER TABLE `telemetry`.`prod`.`synthetic` ADD COLUMNS \
         (`attributes` MAP<STRING, STRING>, `status` SMALLINT)"
    );
    Ok(())
}

#[test]
fn rejects_every_breaking_or_unsupported_mutation() {
    let cases = [
        (
            "DROP TABLE telemetry.prod.synthetic",
            CatalogErrorCode::DdlDestructiveStatement,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic DROP COLUMN service",
            CatalogErrorCode::DdlParse,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic RENAME COLUMN service TO service_name",
            CatalogErrorCode::DdlParse,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ALTER COLUMN service TYPE BIGINT",
            CatalogErrorCode::DdlParse,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ALTER COLUMN service SET NOT NULL",
            CatalogErrorCode::DdlParse,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ADD COLUMN status STRING FIRST",
            CatalogErrorCode::DdlParse,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ADD PARTITION (event_date = '2026-01-01')",
            CatalogErrorCode::DdlParse,
        ),
        (
            "ALTER TABLE telemetry.prod.synthetic ADD COLUMN status STRING NOT NULL",
            CatalogErrorCode::DdlParse,
        ),
        (
            "CREATE TABLE telemetry.prod.lossy (half FLOAT16) USING DELTA",
            CatalogErrorCode::DdlParse,
        ),
        ("SELECT 1", CatalogErrorCode::DdlParse),
    ];
    for (ddl, expected) in cases {
        let Err(error) = DdlParser::default().parse(ddl) else {
            panic!("mutation must be rejected: {ddl}");
        };
        assert_eq!(error.code(), expected, "DDL: {ddl}; error: {error}");
    }
}

#[test]
fn rejects_partition_changes_and_nonprimitive_partition_types() {
    let Err(error) = DdlParser::default().parse(
        "CREATE TABLE telemetry.prod.bad_partition (id INT, labels ARRAY<STRING>) \
         USING DELTA PARTITIONED BY (labels)",
    ) else {
        panic!("nested partition must fail");
    };
    assert_eq!(error.code(), CatalogErrorCode::DdlParse);
}

#[test]
fn rejects_lossy_lengths_and_decimal_defaults() {
    for ddl in [
        "CREATE TABLE telemetry.prod.lossy (name VARCHAR(10)) USING DELTA",
        "CREATE TABLE telemetry.prod.lossy (amount DECIMAL) USING DELTA",
        "CREATE TABLE telemetry.prod.lossy (amount DECIMAL(20)) USING DELTA",
        "CREATE TABLE telemetry.prod.lossy (bits BINARY(16)) USING DELTA",
    ] {
        let Err(error) = DdlParser::default().parse(ddl) else {
            panic!("lossy mapping must fail closed: {ddl}");
        };
        assert_eq!(error.code(), CatalogErrorCode::DdlParse);
    }
}
