use anyhow::{Context, Result, bail};
use arrowcol_catalog::{DdlCommand, DdlParser};
use arrowcol_protocol::TableIdentity;
use sha2::{Digest, Sha256};

const SCHEMA_DDL: &str = include_str!("../../schemas/sqlserver.sql");

/// Exact number of public synthetic SQL Server tables.
pub const TABLE_COUNT: usize = 13;

/// One strict Flight SQL schema registration request.
#[derive(Clone, Debug)]
pub struct SchemaStatement {
    pub ddl: String,
    pub identity: TableIdentity,
    pub field_count: usize,
}

/// Returns the checked-in public DDL fixture.
#[must_use]
pub const fn schema_ddl() -> &'static str {
    SCHEMA_DDL
}

/// Parses and validates every exact table registration statement.
///
/// # Errors
///
/// Rejects malformed DDL, non-create statements, wrong table prefixes, or an
/// incomplete/duplicated table set.
pub fn schema_statements() -> Result<Vec<SchemaStatement>> {
    let without_comments = SCHEMA_DDL
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    let parser = DdlParser::default();
    let mut statements = Vec::new();
    for raw in without_comments.split(';') {
        let ddl = raw.trim();
        if ddl.is_empty() {
            continue;
        }
        let DdlCommand::CreateTable(create) = parser
            .parse(ddl)
            .with_context(|| format!("parse schema fixture statement {}", statements.len() + 1))?
        else {
            bail!("schema fixture contains a non-CREATE statement");
        };
        if create.identity().catalog().as_str() != "telemetry"
            || create.identity().schema().as_str() != "sqlserver"
            || !create.identity().table().as_str().starts_with("sqlserver_")
        {
            bail!("schema fixture identity is outside telemetry.sqlserver");
        }
        if create.fields().iter().any(|field| field.is_nullable()) {
            bail!("synthetic SQL Server fields must all be required");
        }
        let customer = create
            .fields()
            .first()
            .context("synthetic table has no fields")?
            .to_arrow()?;
        if customer.name() != "customer_id" || customer.data_type() != &arrow::datatypes::DataType::Utf8 {
            bail!("every synthetic table must start with required customer_id VARCHAR");
        }
        statements.push(SchemaStatement {
            ddl: ddl.to_owned(),
            identity: create.identity().clone(),
            field_count: create.fields().len(),
        });
    }
    statements.sort_by(|left, right| left.identity.cmp(&right.identity));
    if statements.len() != TABLE_COUNT {
        bail!(
            "schema fixture contains {} tables; expected {TABLE_COUNT}",
            statements.len()
        );
    }
    if statements.windows(2).any(|pair| pair[0].identity == pair[1].identity) {
        bail!("schema fixture contains duplicate table identities");
    }
    Ok(statements)
}

/// Stable digest over canonical parsed DDL, used by tests and run metadata.
///
/// # Errors
///
/// Returns an error when the checked-in fixture no longer parses or validates.
pub fn schema_contract_sha256() -> Result<String> {
    let parser = DdlParser::default();
    let mut canonical = Vec::new();
    for statement in schema_statements()? {
        canonical.push(parser.parse(&statement.ddl)?.canonical_ddl().to_owned());
    }
    Ok(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(canonical.join("\n").as_bytes()))
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_has_all_exact_required_tables_and_ordered_column_counts() -> Result<()> {
        let statements = schema_statements()?;
        let summary = statements
            .iter()
            .map(|statement| (statement.identity.table().to_string(), statement.field_count))
            .collect::<Vec<_>>();
        assert_eq!(
            summary,
            vec![
                ("sqlserver_active_sessions".to_owned(), 9),
                ("sqlserver_availability_group_states".to_owned(), 13),
                ("sqlserver_availability_replica_states".to_owned(), 22),
                ("sqlserver_client_connections".to_owned(), 14),
                ("sqlserver_cpu_utilization".to_owned(), 8),
                ("sqlserver_database_properties".to_owned(), 44),
                ("sqlserver_database_replica_states".to_owned(), 37),
                ("sqlserver_database_storage_utilization".to_owned(), 16),
                ("sqlserver_memory_utilization".to_owned(), 8),
                ("sqlserver_performance_counters_common".to_owned(), 11),
                ("sqlserver_performance_counters_detailed".to_owned(), 11),
                ("sqlserver_storage_io".to_owned(), 19),
                ("sqlserver_wait_stats".to_owned(), 11),
            ]
        );
        assert_eq!(
            format!("sha256:{}", hex::encode(Sha256::digest(schema_ddl().as_bytes()))),
            "sha256:83ea68f2a9ba1c34d907f6364b0a1b1b75b2a33676ebab857de446ab677f9e6e"
        );
        Ok(())
    }
}
