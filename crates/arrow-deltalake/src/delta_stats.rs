#[cfg(feature = "delta")]
use anyhow::Context;
use anyhow::{Result, ensure};
#[cfg(feature = "delta")]
use arrow::datatypes::{DataType, Schema};
#[cfg(feature = "delta")]
use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub const ENVIRONMENT: &str = "ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS";
pub const COLUMNS_PROPERTY: &str = "delta.dataSkippingStatsColumns";
pub const COUNT_PROPERTY: &str = "delta.dataSkippingNumIndexedCols";
const FIELD: &str = "collector.commit.delta_stats_columns";

/// Creation-only Delta statistics selection. An empty list disables indexed columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaStatsPolicy {
    pub columns: Vec<String>,
}

impl DeltaStatsPolicy {
    /// Validate bounded, unambiguous dotted column paths.
    ///
    /// # Errors
    /// Rejects duplicate, malformed, or oversized selectors.
    pub fn validate(&self) -> Result<()> {
        ensure!(self.columns.len() <= 256, "{FIELD} permits at most 256 column paths");
        let mut unique = BTreeSet::new();
        for path in &self.columns {
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

    #[cfg(feature = "delta")]
    pub(crate) fn from_env() -> Result<Option<Self>> {
        match std::env::var(ENVIRONMENT) {
            Ok(raw) => {
                let columns = serde_json::from_str(&raw).with_context(|| format!("parse {FIELD}"))?;
                let policy = Self { columns };
                policy.validate()?;
                Ok(Some(policy))
            }
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read {FIELD}")),
        }
    }

    #[cfg(feature = "delta")]
    pub(crate) fn validate_schema(&self, schema: &Schema) -> Result<()> {
        self.validate()?;
        for path in &self.columns {
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
        if self.columns.is_empty() {
            BTreeMap::from([(COUNT_PROPERTY.to_owned(), "0".to_owned())])
        } else {
            BTreeMap::from([(COLUMNS_PROPERTY.to_owned(), self.columns.join(","))])
        }
    }

    #[cfg(feature = "delta")]
    pub(crate) fn verify_metadata(&self, metadata: &BTreeMap<String, String>) -> Result<()> {
        for (key, value) in self.properties() {
            ensure!(
                metadata.get(&key) == Some(&value),
                "{FIELD}: committed table metadata {key} differs; pre-existing properties are never changed"
            );
        }
        if self.columns.is_empty() {
            ensure!(
                !metadata.contains_key(COLUMNS_PROPERTY),
                "{FIELD}: pre-existing {COLUMNS_PROPERTY} overrides the requested empty list"
            );
        }
        Ok(())
    }
}
