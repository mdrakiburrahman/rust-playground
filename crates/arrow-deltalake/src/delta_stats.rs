use anyhow::{Context, Result, ensure};
#[cfg(feature = "delta")]
use arrow::datatypes::{DataType, Schema};
#[cfg(feature = "delta")]
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub const ENVIRONMENT: &str = "ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS";
pub const COLUMNS_PROPERTY: &str = "delta.dataSkippingStatsColumns";
pub const COUNT_PROPERTY: &str = "delta.dataSkippingNumIndexedCols";
const FIELD: &str = "collector.commit.delta_stats_columns";
const COUNT_FIELD: &str = "collector.commit.delta_stats_num_indexed_cols";

/// Creation-only Delta statistics selection, projected through [`ENVIRONMENT`] as a JSON
/// array of column paths or a JSON integer column count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeltaStatsPolicy {
    /// `delta.dataSkippingStatsColumns`; an empty list disables indexed columns.
    Columns(Vec<String>),
    /// `delta.dataSkippingNumIndexedCols`: the first N leaf columns, or -1 for all of them.
    NumIndexedCols(i32),
}

impl DeltaStatsPolicy {
    /// Validate bounded, unambiguous dotted column paths or an indexed-column count.
    ///
    /// # Errors
    /// Rejects duplicate, malformed, or oversized selectors, and counts other than -1 or positive.
    pub fn validate(&self) -> Result<()> {
        let columns = match self {
            Self::Columns(columns) => columns,
            Self::NumIndexedCols(count) => {
                ensure!(
                    *count == -1 || *count > 0,
                    "{COUNT_FIELD} must be -1 (all columns) or a positive column count; \
                     use {FIELD}=[] to disable indexed columns"
                );
                return Ok(());
            }
        };
        ensure!(columns.len() <= 256, "{FIELD} permits at most 256 column paths");
        let mut unique = BTreeSet::new();
        for path in columns {
            ensure!(
                path.len() <= 1024
                    && path.split('.').all(|part| {
                        !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    }),
                "{FIELD} contains invalid column path {path:?}"
            );
            ensure!(unique.insert(path), "{FIELD} contains duplicate column path {path:?}");
        }
        Ok(())
    }

    /// Parse the [`ENVIRONMENT`] projection.
    ///
    /// # Errors
    /// Rejects anything other than an array of column paths or an in-range integer count.
    pub fn from_json(raw: &str) -> Result<Self> {
        let policy = match serde_json::from_str::<serde_json::Value>(raw).with_context(|| format!("parse {FIELD}"))? {
            serde_json::Value::Number(count) => Self::NumIndexedCols(
                count
                    .as_i64()
                    .and_then(|count| i32::try_from(count).ok())
                    .with_context(|| format!("{COUNT_FIELD} must be an integer between -1 and {}", i32::MAX))?,
            ),
            value @ serde_json::Value::Array(_) => {
                Self::Columns(serde_json::from_value(value).with_context(|| format!("parse {FIELD}"))?)
            }
            _ => anyhow::bail!("{FIELD} must be an array of column paths, or {COUNT_FIELD} an integer"),
        };
        policy.validate()?;
        Ok(policy)
    }

    #[cfg(feature = "delta")]
    pub(crate) fn from_env() -> Result<Option<Self>> {
        match std::env::var(ENVIRONMENT) {
            Ok(raw) => Self::from_json(&raw).map(Some),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read {FIELD}")),
        }
    }

    #[cfg(feature = "delta")]
    pub(crate) fn validate_schema(&self, schema: &Schema) -> Result<()> {
        self.validate()?;
        let Self::Columns(columns) = self else {
            return Ok(());
        };
        for path in columns {
            let mut parts = path.split('.');
            let first = parts.next().context("missing stats path")?;
            let mut field = schema
                .field_with_name(first)
                .with_context(|| format!("{FIELD}: unknown column {path}"))?;
            for part in parts {
                let DataType::Struct(fields) = field.data_type() else {
                    anyhow::bail!("{FIELD}: {path} does not resolve through a struct");
                };
                field = fields
                    .iter()
                    .find(|field| field.name() == part)
                    .with_context(|| format!("{FIELD}: unknown column {path}"))?;
            }
            ensure!(
                !matches!(
                    field.data_type(),
                    DataType::Struct(_)
                        | DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::Map(_, _)
                        | DataType::Union(_, _)
                ),
                "{FIELD}: {path} must select a primitive leaf"
            );
        }
        Ok(())
    }

    #[cfg(feature = "delta")]
    pub(crate) fn properties(&self) -> BTreeMap<String, String> {
        match self {
            Self::Columns(columns) if columns.is_empty() => {
                BTreeMap::from([(COUNT_PROPERTY.to_owned(), "0".to_owned())])
            }
            Self::Columns(columns) => BTreeMap::from([(COLUMNS_PROPERTY.to_owned(), columns.join(","))]),
            Self::NumIndexedCols(count) => BTreeMap::from([(COUNT_PROPERTY.to_owned(), count.to_string())]),
        }
    }

    #[cfg(feature = "delta")]
    pub(crate) fn verify_metadata(&self, metadata: &BTreeMap<String, String>) -> Result<()> {
        let field = match self {
            Self::Columns(_) => FIELD,
            Self::NumIndexedCols(_) => COUNT_FIELD,
        };
        for (key, value) in self.properties() {
            ensure!(
                metadata.get(&key) == Some(&value),
                "{field}: committed table metadata {key} differs; pre-existing properties are never changed"
            );
        }
        if !matches!(self, Self::Columns(columns) if !columns.is_empty()) {
            ensure!(
                !metadata.contains_key(COLUMNS_PROPERTY),
                "{field}: pre-existing {COLUMNS_PROPERTY} overrides the requested indexed-column count"
            );
        }
        Ok(())
    }
}
