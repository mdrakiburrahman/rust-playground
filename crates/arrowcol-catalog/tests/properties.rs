use arrowcol_catalog::{CatalogSchemaVersion, DdlCommand, DdlParser, SchemaVersion, deterministic_table_id};
use proptest::prelude::*;

proptest! {
    #[test]
    fn canonical_create_is_whitespace_and_keyword_case_independent(
        table in "[a-z][a-z0-9_]{0,12}",
        field in "[a-z][a-z0-9_]{0,12}",
    ) {
        let compact = format!("create table telemetry.prod.{table} ({field} bigint) using delta");
        let spaced = format!(
            "  CREATE   TABLE telemetry.prod.{table} ( {field}   BIGINT  ) USING DELTA ; "
        );
        let DdlCommand::CreateTable(left) = DdlParser::default().parse(&compact)? else {
            return Err(TestCaseError::fail("expected create"));
        };
        let DdlCommand::CreateTable(right) = DdlParser::default().parse(&spaced)? else {
            return Err(TestCaseError::fail("expected create"));
        };
        prop_assert_eq!(left.canonical_ddl(), right.canonical_ddl());
        prop_assert_eq!(left.fields(), right.fields());
    }

    #[test]
    fn catalog_schema_ir_round_trips_losslessly(
        table in "[a-z][a-z0-9_]{0,12}",
        comment in "[a-zA-Z0-9 ']{0,24}",
        partitioned in any::<bool>(),
        nullable in any::<bool>(),
        timezone in any::<bool>(),
        precision in 1_u8..=38,
        scale in 0_u8..=38,
    ) {
        let parser = DdlParser::default();
        let sql = format!(
            "CREATE TABLE c.d.{table} (id BIGINT{}, info STRUCT<label: STRING COMMENT {}, values: ARRAY<MAP<STRING,DECIMAL({precision},{})>>>, ts {}) USING DELTA{} COMMENT {} TBLPROPERTIES ('a'='1')",
            if nullable { "" } else { " NOT NULL" },
            arrowcol_sparksql::quote_string(&comment), scale.min(precision),
            if timezone { "TIMESTAMP" } else { "TIMESTAMP_NTZ" },
            if partitioned { " PARTITIONED BY (id)" } else { "" },
            arrowcol_sparksql::quote_string(&comment),
        );
        let DdlCommand::CreateTable(first) = parser.parse(&sql)? else {
            return Err(TestCaseError::fail("expected create"));
        };
        let schema = CatalogSchemaVersion::new(
            deterministic_table_id(first.identity()), first.identity().clone(),
            SchemaVersion::FIRST, first.fields().to_vec(), first.canonical_ddl(),
        )?.with_metadata(first.metadata().clone())?;
        let DdlCommand::CreateTable(second) = parser.parse(&schema.to_spark_sql()?)? else {
            return Err(TestCaseError::fail("expected rendered create"));
        };
        let reparsed = CatalogSchemaVersion::new(
            deterministic_table_id(second.identity()), second.identity().clone(),
            SchemaVersion::FIRST, second.fields().to_vec(), second.canonical_ddl(),
        )?.with_metadata(second.metadata().clone())?;
        prop_assert_eq!(schema, reparsed);
    }

    #[test]
    fn create_and_append_share_one_legal_monotonic_numbering_contract(position in 0_u32..1_000_000) {
        let number = arrowcol_catalog::protobuf_field_number(position)?;
        let next = arrowcol_catalog::protobuf_field_number(position + 1)?;
        prop_assert!(number > 0 && !(19_000..=19_999).contains(&number));
        prop_assert!(next > number);
        prop_assert_eq!(arrowcol_catalog::next_protobuf_field_number(number)?, next);
    }
}
