use arrowcol_protocol::{CatalogName, SchemaName, TableIdentity, TableName};
use arrowcol_sparksql::{Column, DataType, Statement, TableMetadata};
use uuid::Uuid;

use crate::{
    CatalogError, CatalogErrorCode, CatalogField, CatalogResult, CatalogType, FieldPath, ListKind, PartitionMetadata,
    TimeUnit, TypeMapping, deterministic_field_id, deterministic_table_id, protobuf_field_number,
};

/// Schema-mutating catalog DDL commands.
///
/// Namespace, property, inspection and drop commands are available through
/// [`DdlParser::parse_statement`] for the service's separate operation dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DdlCommand {
    /// Create a table and its first immutable schema version.
    CreateTable(ParsedCreateTable),
    /// Append nullable fields to an existing table.
    AddColumns(ParsedAddColumns),
}

impl DdlCommand {
    /// Returns the target table.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        match self {
            Self::CreateTable(command) => command.identity(),
            Self::AddColumns(command) => command.identity(),
        }
    }

    /// Returns canonical Spark SQL used as the idempotency input.
    #[must_use]
    pub fn canonical_ddl(&self) -> &str {
        match self {
            Self::CreateTable(command) => command.canonical_ddl(),
            Self::AddColumns(command) => command.canonical_ddl(),
        }
    }

    /// Renders canonical Spark SQL.
    #[must_use]
    pub fn to_spark_sql(&self) -> String {
        self.canonical_ddl().to_owned()
    }
}

/// Parsed Spark `CREATE TABLE`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedCreateTable {
    identity: TableIdentity,
    fields: Vec<CatalogField>,
    canonical_ddl: String,
    metadata: TableMetadata,
    if_not_exists: bool,
}

impl ParsedCreateTable {
    /// Returns the target identity.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        &self.identity
    }
    /// Returns all columns in their declared order.
    #[must_use]
    pub fn fields(&self) -> &[CatalogField] {
        &self.fields
    }
    /// Returns canonical Spark SQL.
    #[must_use]
    pub fn canonical_ddl(&self) -> &str {
        &self.canonical_ddl
    }
    /// Returns provider, partition columns, properties, location and comment.
    #[must_use]
    pub const fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
    /// Returns whether an existing table is permitted.
    #[must_use]
    pub const fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
    /// Consumes the command into its legacy schema-saga parts.
    ///
    /// Read [`Self::metadata`] before consuming to retain table metadata.
    #[must_use]
    pub fn into_parts(self) -> (TableIdentity, Vec<CatalogField>, String) {
        (self.identity, self.fields, self.canonical_ddl)
    }
}

/// Parsed Spark `ALTER TABLE ... ADD COLUMNS`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedAddColumns {
    identity: TableIdentity,
    fields: Vec<FieldBlueprint>,
    canonical_ddl: String,
}

impl ParsedAddColumns {
    /// Returns the target identity.
    #[must_use]
    pub const fn identity(&self) -> &TableIdentity {
        &self.identity
    }
    /// Returns nullable fields to append.
    #[must_use]
    pub fn fields(&self) -> &[FieldBlueprint] {
        &self.fields
    }
    /// Returns canonical Spark SQL.
    #[must_use]
    pub fn canonical_ddl(&self) -> &str {
        &self.canonical_ddl
    }
}

/// An addition before its never-reused sibling position and protobuf number
/// are allocated against the existing schema head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldBlueprint {
    id: Uuid,
    name: String,
    nullable: bool,
    mapping: TypeMapping,
    comment: Option<String>,
}

impl FieldBlueprint {
    /// Returns the deterministic field identifier.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }
    /// Returns the field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Returns whether the field is nullable.
    #[must_use]
    pub const fn is_nullable(&self) -> bool {
        self.nullable
    }
    /// Returns its cross-system type mapping.
    #[must_use]
    pub const fn mapping(&self) -> &TypeMapping {
        &self.mapping
    }
    /// Returns its optional comment.
    #[must_use]
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
    /// Allocates the field at a caller-supplied legal position and number.
    ///
    /// # Errors
    ///
    /// Rejects invalid protobuf numbers or catalog field invariants.
    pub fn materialize(&self, position: u32, field_number: u32) -> CatalogResult<CatalogField> {
        CatalogField::new(
            self.id,
            self.name.clone(),
            position,
            field_number,
            self.nullable,
            None,
            self.mapping.clone(),
        )
        .map(|field| field.with_comment(self.comment.clone()))
    }
}

/// Rust/chumsky Spark DDL front end for the schema catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlParser {
    timestamp_timezone: String,
}

impl Default for DdlParser {
    fn default() -> Self {
        Self {
            timestamp_timezone: "UTC".to_owned(),
        }
    }
}

impl DdlParser {
    /// Configures the catalog's Spark `TIMESTAMP` session timezone.
    ///
    /// The lossless canonical contract currently supports UTC only; other
    /// timezone representations are explicitly rejected when materialized.
    #[must_use]
    pub fn new(timestamp_timezone: impl Into<String>) -> Self {
        Self {
            timestamp_timezone: timestamp_timezone.into(),
        }
    }

    /// Parses any supported Spark catalog statement.
    ///
    /// # Errors
    ///
    /// Returns original-input byte spans for malformed or unsupported syntax.
    pub fn parse_statement(&self, sql: &str) -> CatalogResult<Statement> {
        arrowcol_sparksql::parse(sql).map_err(|errors| {
            CatalogError::new(
                CatalogErrorCode::DdlParse,
                errors.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "),
            )
        })
    }

    /// Parses and materializes a schema mutation.
    ///
    /// # Errors
    ///
    /// Rejects unsupported statements, malformed identities, non-null
    /// additions, or types without a lossless Spark/Arrow mapping.
    pub fn parse(&self, sql: &str) -> CatalogResult<DdlCommand> {
        let statement = self.parse_statement(sql)?;
        let canonical_ddl = statement.to_spark_sql();
        match statement {
            Statement::CreateTable(create) => {
                let identity = parse_identity(&create.name)?;
                let table_id = deterministic_table_id(&identity);
                let fields = create
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        let position = u32::try_from(index).map_err(|_| too_many_fields())?;
                        let partition = create
                            .metadata
                            .partition_columns
                            .iter()
                            .position(|name| name == &column.name)
                            .map(|ordinal| {
                                u32::try_from(ordinal)
                                    .map(PartitionMetadata::new)
                                    .map_err(|_| too_many_fields())
                            })
                            .transpose()?;
                        self.materialize_column(table_id, &FieldPath::root(&column.name), column, position, partition)
                    })
                    .collect::<CatalogResult<Vec<_>>>()?;
                Ok(DdlCommand::CreateTable(ParsedCreateTable {
                    identity,
                    fields,
                    canonical_ddl,
                    metadata: create.metadata,
                    if_not_exists: create.if_not_exists,
                }))
            }
            Statement::AddTableColumns { name, columns } => {
                let identity = parse_identity(&name)?;
                let table_id = deterministic_table_id(&identity);
                let fields = columns
                    .iter()
                    .map(|column| {
                        validate_field_name(&column.name)?;
                        let path = FieldPath::root(&column.name);
                        let mapping = TypeMapping::new(self.materialize_type(table_id, &path, &column.data_type)?);
                        mapping.spark()?;
                        Ok(FieldBlueprint {
                            id: deterministic_field_id(table_id, &path),
                            name: column.name.clone(),
                            nullable: true,
                            mapping,
                            comment: column.comment.clone(),
                        })
                    })
                    .collect::<CatalogResult<Vec<_>>>()?;
                Ok(DdlCommand::AddColumns(ParsedAddColumns {
                    identity,
                    fields,
                    canonical_ddl,
                }))
            }
            Statement::DropTable { .. } => Err(CatalogError::new(
                CatalogErrorCode::DdlDestructiveStatement,
                "DROP TABLE must use the catalog service's authorized operation dispatch",
            )),
            _ => Err(CatalogError::new(
                CatalogErrorCode::DdlUnsupportedStatement,
                "use parse_statement for namespace, property and inspection commands",
            )),
        }
    }

    fn materialize_column(
        &self,
        table_id: Uuid,
        path: &FieldPath,
        column: &Column,
        position: u32,
        partition: Option<PartitionMetadata>,
    ) -> CatalogResult<CatalogField> {
        validate_field_name(&column.name)?;
        let mapping = TypeMapping::new(self.materialize_type(table_id, path, &column.data_type)?);
        mapping.spark()?;
        CatalogField::new(
            deterministic_field_id(table_id, path),
            column.name.clone(),
            position,
            protobuf_field_number(position)?,
            column.nullable,
            partition,
            mapping,
        )
        .map(|field| field.with_comment(column.comment.clone()))
    }

    fn nested_field(
        &self,
        table_id: Uuid,
        path: &FieldPath,
        name: &str,
        ty: &DataType,
        position: u32,
        nullable: bool,
    ) -> CatalogResult<CatalogField> {
        let column = Column {
            name: name.to_owned(),
            data_type: ty.clone(),
            nullable,
            comment: None,
        };
        self.materialize_column(table_id, path, &column, position, None)
    }

    fn materialize_type(&self, table_id: Uuid, path: &FieldPath, ty: &DataType) -> CatalogResult<CatalogType> {
        Ok(match ty {
            DataType::Boolean => CatalogType::Boolean,
            DataType::TinyInt => CatalogType::Int8,
            DataType::SmallInt => CatalogType::Int16,
            DataType::Int => CatalogType::Int32,
            DataType::BigInt => CatalogType::Int64,
            DataType::Float => CatalogType::Float32,
            DataType::Double => CatalogType::Float64,
            DataType::String => CatalogType::Utf8,
            DataType::Binary => CatalogType::Binary,
            DataType::Date => CatalogType::Date32,
            DataType::Timestamp => CatalogType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some(self.timestamp_timezone.clone()),
            },
            DataType::TimestampNtz => CatalogType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
            DataType::Decimal(precision, scale) => CatalogType::Decimal128 {
                precision: *precision,
                scale: i8::try_from(*scale).map_err(|_| too_many_fields())?,
            },
            DataType::Struct(columns) => CatalogType::Struct {
                fields: columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        self.materialize_column(
                            table_id,
                            &path.child(&column.name),
                            column,
                            u32::try_from(index).map_err(|_| too_many_fields())?,
                            None,
                        )
                    })
                    .collect::<CatalogResult<Vec<_>>>()?,
            },
            DataType::Array(element) => CatalogType::List {
                list_kind: ListKind::List,
                element: Box::new(self.nested_field(table_id, &path.child("item"), "item", element, 0, true)?),
            },
            DataType::Map(key, value) => CatalogType::Map {
                key: Box::new(self.nested_field(table_id, &path.child("key"), "key", key, 0, false)?),
                value: Box::new(self.nested_field(table_id, &path.child("value"), "value", value, 1, true)?),
                keys_sorted: false,
            },
        })
    }
}

fn parse_identity(parts: &[String]) -> CatalogResult<TableIdentity> {
    let [catalog, schema, table] = parts else {
        return Err(CatalogError::new(
            CatalogErrorCode::InvalidIdentity,
            "table identity requires catalog.schema.table",
        ));
    };
    let invalid = |error: arrowcol_protocol::IdentityError| {
        CatalogError::new(CatalogErrorCode::InvalidIdentity, error.to_string())
    };
    Ok(TableIdentity::new(
        CatalogName::new(catalog.clone()).map_err(invalid)?,
        SchemaName::new(schema.clone()).map_err(invalid)?,
        TableName::new(table.clone()).map_err(invalid)?,
    ))
}

fn validate_field_name(name: &str) -> CatalogResult<()> {
    TableName::new(name.to_owned())
        .map(|_| ())
        .map_err(|error| CatalogError::new(CatalogErrorCode::InvalidIdentity, error.to_string()))
}

fn too_many_fields() -> CatalogError {
    CatalogError::new(
        CatalogErrorCode::CorruptCatalogState,
        "field allocation space exhausted",
    )
}
