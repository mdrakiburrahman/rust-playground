use arrowcol_sparksql::{Statement, parse};
use proptest::prelude::*;

#[test]
fn every_statement_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    for sql in [
        "CREATE TABLE IF NOT EXISTS cat.db.tbl (id BIGINT NOT NULL COMMENT 'identity', info STRUCT<a: STRING, b: ARRAY<MAP<STRING, DECIMAL(20,4)>>>, ts TIMESTAMP, wall TIMESTAMP_NTZ) TBLPROPERTIES ('b'='2','a'='1') COMMENT 'table' PARTITIONED BY (id) LOCATION 'abfss://data/path' USING DELTA",
        "ALTER TABLE cat.db.tbl ADD COLUMNS (x STRING, y ARRAY<INT>)",
        "ALTER TABLE cat.db.tbl ADD COLUMN x STRING COMMENT 'new'",
        "ALTER TABLE cat.db.tbl SET TBLPROPERTIES ('a'='2')",
        "ALTER TABLE cat.db.tbl UNSET TBLPROPERTIES IF EXISTS ('a','b')",
        "DROP TABLE IF EXISTS cat.db.tbl",
        "CREATE SCHEMA IF NOT EXISTS cat.db LOCATION '/tmp/db' WITH DBPROPERTIES ('a'='1') COMMENT 'db'",
        "SHOW TABLES FROM cat.db LIKE 'events*'",
        "SHOW TABLES",
        "SHOW TBLPROPERTIES cat.db.tbl ('a')",
        "SHOW TBLPROPERTIES cat.db.tbl",
        "DESC cat.db.tbl",
    ] {
        let first = parse(sql).map_err(|errors| format!("{errors:?}"))?;
        let rendered = first.to_spark_sql();
        let second = parse(&rendered).map_err(|errors| format!("{rendered}: {errors:?}"))?;
        assert_eq!(first, second, "{sql}");
    }
    Ok(())
}

#[test]
fn unsupported_or_lossy_syntax_is_rejected() {
    for sql in [
        "CREATE TABLE c.d.t (x BIGINT UNSIGNED) USING DELTA",
        "CREATE TABLE c.d.t (x TIMESTAMP(9)) USING DELTA",
        "CREATE TABLE c.d.t (x BYTEA) USING DELTA",
        "CREATE TABLE c.d.t (x INT) USING PARQUET",
        "CREATE TABLE c.d.t (x INT)",
        "CREATE TABLE c.d.t (x INT DEFAULT 1) USING DELTA",
        "CREATE TABLE c.d.t (x INT) USING DELTA AS SELECT 1",
        "CREATE TABLE c.d.t (x INT) USING DELTA PARTITIONED BY (y)",
        "CREATE TABLE c.d.t (x INT) USING DELTA PARTITIONED BY (x INT)",
        "CREATE TABLE c.d.t (x INT) USING DELTA LOCATION 'a' LOCATION 'b'",
        "CREATE TABLE c.d.t (x INT) USING DELTA TBLPROPERTIES ('a'='1','a'='2')",
        "CREATE TABLE c.d.t (x MAP<ARRAY<INT>,STRING>) USING DELTA",
        "CREATE TABLE c.d.t (x STRUCT<a: INT,a: STRING>) USING DELTA",
        "CREATE TABLE c.d.t (x DECIMAL(39,1)) USING DELTA",
        "CREATE TABLE c.d.t (x DECIMAL(2,3)) USING DELTA",
        "ALTER TABLE c.d.t ADD COLUMN x INT NOT NULL",
        "ALTER TABLE c.d.t DROP COLUMN x",
        "SHOW TABLES; DROP TABLE c.d.t",
        "CREATE TABLE c.d.t (x INT) USING DELTA /* unterminated",
    ] {
        assert!(parse(sql).is_err(), "accepted unsupported SQL: {sql}");
    }
}

#[test]
fn deeply_nested_input_is_bounded_before_recursive_parsing() {
    let sql = format!(
        "CREATE TABLE c.d.t (x {}INT{}) USING DELTA",
        "ARRAY<".repeat(65),
        ">".repeat(65)
    );
    let Err(errors) = parse(&sql) else {
        panic!("unbounded nesting accepted");
    };
    assert!(errors.iter().any(|error| error.message.contains("nesting exceeds")));
    let sql = format!(
        "CREATE TABLE c.d.t (x STRING COMMENT '{}') USING DELTA",
        "<".repeat(100)
    );
    assert!(parse(&sql).is_ok(), "literal contents must not count as nesting");
}

#[test]
fn utf8_spans_and_escaped_literals_are_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let sql = "CREATE TABLE `Cat`.`db`.`t` (`a``b` STRING COMMENT 'it''s \\\\ fine') USING DELTA COMMENT '雪' TBLPROPERTIES ('x'='line\\nnext'); -- end";
    let first = parse(sql).map_err(|errors| format!("{errors:?}"))?;
    assert_eq!(
        parse(&first.to_spark_sql()).map_err(|errors| format!("{errors:?}"))?,
        first
    );
    let invalid = "CREATE TABLE c.d.t (x STRING COMMENT '雪', bad UNSIGNED) USING DELTA";
    let errors = match parse(invalid) {
        Err(errors) => errors,
        Ok(_) => return Err("accepted unsupported type".into()),
    };
    assert!(
        errors
            .iter()
            .all(|error| invalid.is_char_boundary(error.span.start) && invalid.is_char_boundary(error.span.end))
    );
    assert!(errors.iter().any(|error| &invalid[error.span.clone()] == "UNSIGNED"));
    Ok(())
}

proptest! {
    #[test]
    fn generated_ir_round_trips(
        suffix in "[a-z][a-z0-9_]{0,15}",
        precision in 1_u8..=38,
        scale in 0_u8..=38,
        nullable in any::<bool>(),
        comment in "[a-zA-Z0-9 ']{0,32}",
        value in "[a-zA-Z0-9 ']{0,32}",
    ) {
        let sql = format!("CREATE TABLE c.d.{suffix} (id BIGINT{}, payload STRUCT<label: STRING COMMENT {}, values: ARRAY<MAP<STRING,DECIMAL({precision},{})>>>) USING DELTA TBLPROPERTIES ('value'={})",
            if nullable { "" } else { " NOT NULL" }, arrowcol_sparksql::quote_string(&comment), scale.min(precision), arrowcol_sparksql::quote_string(&value));
        let ir = parse(&sql).map_err(|errors| TestCaseError::fail(format!("{errors:?}")))?;
        let rendered = ir.to_spark_sql();
        prop_assert_eq!(parse(&rendered).map_err(|errors| TestCaseError::fail(format!("{errors:?}")))?, ir);
    }

    #[test]
    fn arbitrary_text_never_panics(sql in ".{0,512}") {
        let _ = parse(&sql);
    }
}

#[test]
fn partition_clause_does_not_reorder_columns() -> Result<(), Box<dyn std::error::Error>> {
    let ir = parse("CREATE TABLE c.d.t (a INT, b STRING, c DATE) USING DELTA PARTITIONED BY (c,a)")
        .map_err(|errors| format!("{errors:?}"))?;
    let Statement::CreateTable(table) = ir else {
        return Err("wrong command".into());
    };
    assert_eq!(
        table
            .columns
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    assert_eq!(table.metadata.partition_columns, ["c", "a"]);
    Ok(())
}
