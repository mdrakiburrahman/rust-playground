//! Strict Spark SQL catalog DDL syntax, parsed entirely in Rust.
//!
//! The rule-shaped AST follows Spark's `SqlBaseParser.g4` naming. This is a
//! deliberately bounded grammar, not a Spark query engine or CTAS parser.

use std::{collections::BTreeMap, ops::Range};

use serde::{Deserialize, Serialize};

mod parser;
pub use parser::parse;

/// A syntax or semantic error with a UTF-8 byte range in the original SQL.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{message} at bytes {span:?}")]
pub struct ParseError {
    /// Original input byte range.
    pub span: Range<usize>,
    /// Human-readable diagnostic.
    pub message: String,
}

/// Canonical Spark primitive or nested data type.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DataType {
    /// Boolean.
    Boolean,
    /// Signed eight-bit integer.
    TinyInt,
    /// Signed sixteen-bit integer.
    SmallInt,
    /// Signed thirty-two-bit integer.
    Int,
    /// Signed sixty-four-bit integer.
    BigInt,
    /// IEEE single precision.
    Float,
    /// IEEE double precision.
    Double,
    /// UTF-8 string.
    String,
    /// Variable length bytes.
    Binary,
    /// Day-resolution date.
    Date,
    /// Microsecond timestamp in the configured session timezone.
    Timestamp,
    /// Microsecond timestamp without timezone.
    TimestampNtz,
    /// Precision and nonnegative scale, bounded by Spark's precision of 38.
    Decimal(u8, u8),
    /// Ordered nested fields.
    Struct(Vec<Column>),
    /// Nullable array elements.
    Array(Box<DataType>),
    /// Non-null keys and nullable values.
    Map(Box<DataType>, Box<DataType>),
}

/// Spark's `colType` / `complexColType` rule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Column {
    /// Identifier, with quoted casing preserved.
    pub name: String,
    /// Canonical logical type.
    pub data_type: DataType,
    /// Whether values may be null.
    pub nullable: bool,
    /// Optional field comment.
    pub comment: Option<String>,
}

/// HMS-shaped table metadata independent of its Arrow schema.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TableMetadata {
    /// Storage provider; the supported DDL provider is `delta`.
    pub provider: String,
    /// Ordered partition column names.
    pub partition_columns: Vec<String>,
    /// Explicit storage location, when supplied.
    pub location: Option<String>,
    /// Optional table comment.
    pub comment: Option<String>,
    /// Sorted, case-sensitive table properties.
    pub properties: BTreeMap<String, String>,
}

impl Default for TableMetadata {
    fn default() -> Self {
        Self {
            provider: "delta".to_owned(),
            partition_columns: Vec::new(),
            location: None,
            comment: None,
            properties: BTreeMap::new(),
        }
    }
}

/// Spark's `createTable` rule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreateTable {
    /// Multipart table name.
    pub name: Vec<String>,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Ordered columns.
    pub columns: Vec<Column>,
    /// Provider and `createTableClauses`.
    pub metadata: TableMetadata,
}

/// Spark's `createNamespace` rule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreateNamespace {
    /// Multipart namespace name.
    pub name: Vec<String>,
    /// `IF NOT EXISTS` flag.
    pub if_not_exists: bool,
    /// Optional namespace comment.
    pub comment: Option<String>,
    /// Optional namespace storage location.
    pub location: Option<String>,
    /// Sorted namespace properties.
    pub properties: BTreeMap<String, String>,
}

/// Supported Spark `statement` alternatives.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Statement {
    /// Create a Delta table.
    CreateTable(CreateTable),
    /// Append nullable columns.
    AddTableColumns {
        /// Multipart table name.
        name: Vec<String>,
        /// Ordered additions.
        columns: Vec<Column>,
    },
    /// Replace or add property values.
    SetTableProperties {
        /// Multipart table name.
        name: Vec<String>,
        /// Sorted property assignments.
        properties: BTreeMap<String, String>,
    },
    /// Remove property keys.
    UnsetTableProperties {
        /// Multipart table name.
        name: Vec<String>,
        /// `IF EXISTS` flag.
        if_exists: bool,
        /// Ordered property keys.
        keys: Vec<String>,
    },
    /// Drop a table.
    DropTable {
        /// Multipart table name.
        name: Vec<String>,
        /// `IF EXISTS` flag.
        if_exists: bool,
    },
    /// Create a database or schema.
    CreateNamespace(CreateNamespace),
    /// List tables in an optional namespace.
    ShowTables {
        /// Optional multipart namespace.
        namespace: Option<Vec<String>>,
        /// Optional Spark pattern.
        pattern: Option<String>,
    },
    /// Read table properties.
    ShowTableProperties {
        /// Multipart table name.
        name: Vec<String>,
        /// Optional single key.
        key: Option<String>,
    },
    /// Describe a table schema.
    DescribeTable {
        /// Multipart table name.
        name: Vec<String>,
    },
}

/// Quotes an identifier without changing its spelling.
#[must_use]
pub fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Quotes a Spark string using doubled quotes and escaped backslashes.
#[must_use]
pub fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn multipart(name: &[String]) -> String {
    name.iter()
        .map(|part| quote_identifier(part))
        .collect::<Vec<_>>()
        .join(".")
}

fn properties(values: &BTreeMap<String, String>) -> String {
    values
        .iter()
        .map(|(k, v)| format!("{}={}", quote_string(k), quote_string(v)))
        .collect::<Vec<_>>()
        .join(", ")
}

impl DataType {
    /// Renders a canonical Spark SQL data type.
    #[must_use]
    pub fn to_spark_sql(&self) -> String {
        match self {
            Self::Boolean => "BOOLEAN".to_owned(),
            Self::TinyInt => "TINYINT".to_owned(),
            Self::SmallInt => "SMALLINT".to_owned(),
            Self::Int => "INT".to_owned(),
            Self::BigInt => "BIGINT".to_owned(),
            Self::Float => "FLOAT".to_owned(),
            Self::Double => "DOUBLE".to_owned(),
            Self::String => "STRING".to_owned(),
            Self::Binary => "BINARY".to_owned(),
            Self::Date => "DATE".to_owned(),
            Self::Timestamp => "TIMESTAMP".to_owned(),
            Self::TimestampNtz => "TIMESTAMP_NTZ".to_owned(),
            Self::Decimal(p, s) => format!("DECIMAL({p},{s})"),
            Self::Struct(fields) => format!(
                "STRUCT<{}>",
                fields
                    .iter()
                    .map(|field| field.render(": "))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Array(element) => format!("ARRAY<{}>", element.to_spark_sql()),
            Self::Map(key, value) => format!("MAP<{}, {}>", key.to_spark_sql(), value.to_spark_sql()),
        }
    }
}

impl Column {
    fn render(&self, separator: &str) -> String {
        let mut sql = format!(
            "{}{separator}{}",
            quote_identifier(&self.name),
            self.data_type.to_spark_sql()
        );
        if !self.nullable {
            sql.push_str(" NOT NULL");
        }
        if let Some(comment) = &self.comment {
            sql.push_str(&format!(" COMMENT {}", quote_string(comment)));
        }
        sql
    }

    /// Renders a canonical column declaration.
    #[must_use]
    pub fn to_spark_sql(&self) -> String {
        self.render(" ")
    }
}

impl Statement {
    /// Renders canonical Spark SQL with deterministic property ordering.
    #[must_use]
    pub fn to_spark_sql(&self) -> String {
        match self {
            Self::CreateTable(table) => {
                let mut sql = format!(
                    "CREATE TABLE {}{} ({}) USING {}",
                    if table.if_not_exists { "IF NOT EXISTS " } else { "" },
                    multipart(&table.name),
                    table
                        .columns
                        .iter()
                        .map(Column::to_spark_sql)
                        .collect::<Vec<_>>()
                        .join(", "),
                    table.metadata.provider.to_ascii_uppercase()
                );
                if !table.metadata.partition_columns.is_empty() {
                    sql.push_str(&format!(
                        " PARTITIONED BY ({})",
                        table
                            .metadata
                            .partition_columns
                            .iter()
                            .map(|name| quote_identifier(name))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if let Some(location) = &table.metadata.location {
                    sql.push_str(&format!(" LOCATION {}", quote_string(location)));
                }
                if let Some(comment) = &table.metadata.comment {
                    sql.push_str(&format!(" COMMENT {}", quote_string(comment)));
                }
                if !table.metadata.properties.is_empty() {
                    sql.push_str(&format!(" TBLPROPERTIES ({})", properties(&table.metadata.properties)));
                }
                sql
            }
            Self::AddTableColumns { name, columns } => format!(
                "ALTER TABLE {} ADD COLUMNS ({})",
                multipart(name),
                columns.iter().map(Column::to_spark_sql).collect::<Vec<_>>().join(", ")
            ),
            Self::SetTableProperties {
                name,
                properties: props,
            } => format!(
                "ALTER TABLE {} SET TBLPROPERTIES ({})",
                multipart(name),
                properties(props)
            ),
            Self::UnsetTableProperties { name, if_exists, keys } => format!(
                "ALTER TABLE {} UNSET TBLPROPERTIES {}({})",
                multipart(name),
                if *if_exists { "IF EXISTS " } else { "" },
                keys.iter().map(|k| quote_string(k)).collect::<Vec<_>>().join(", ")
            ),
            Self::DropTable { name, if_exists } => format!(
                "DROP TABLE {}{}",
                if *if_exists { "IF EXISTS " } else { "" },
                multipart(name)
            ),
            Self::CreateNamespace(ns) => {
                let mut sql = format!(
                    "CREATE DATABASE {}{}",
                    if ns.if_not_exists { "IF NOT EXISTS " } else { "" },
                    multipart(&ns.name)
                );
                if let Some(comment) = &ns.comment {
                    sql.push_str(&format!(" COMMENT {}", quote_string(comment)));
                }
                if let Some(location) = &ns.location {
                    sql.push_str(&format!(" LOCATION {}", quote_string(location)));
                }
                if !ns.properties.is_empty() {
                    sql.push_str(&format!(" WITH DBPROPERTIES ({})", properties(&ns.properties)));
                }
                sql
            }
            Self::ShowTables { namespace, pattern } => {
                let mut sql = "SHOW TABLES".to_owned();
                if let Some(namespace) = namespace {
                    sql.push_str(&format!(" IN {}", multipart(namespace)));
                }
                if let Some(pattern) = pattern {
                    sql.push_str(&format!(" LIKE {}", quote_string(pattern)));
                }
                sql
            }
            Self::ShowTableProperties { name, key } => {
                let mut sql = format!("SHOW TBLPROPERTIES {}", multipart(name));
                if let Some(key) = key {
                    sql.push_str(&format!(" ({})", quote_string(key)));
                }
                sql
            }
            Self::DescribeTable { name } => format!("DESCRIBE TABLE {}", multipart(name)),
        }
    }
}
