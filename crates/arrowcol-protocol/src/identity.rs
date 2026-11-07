use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

/// Maximum UTF-8 byte length of one catalog, schema, or table component.
pub const MAX_IDENTITY_COMPONENT_BYTES: usize = 128;

macro_rules! identity_component {
    ($name:ident, $label:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Creates a validated ", $label, " name.")]
            ///
            /// # Errors
            ///
            /// Returns [`IdentityError`] when the value is empty, too long, or
            /// contains characters outside the portable identity grammar.
            pub fn new(value: impl Into<String>) -> Result<Self, IdentityError> {
                let value = value.into();
                validate_component($label, &value)?;
                Ok(Self(value))
            }

            #[doc = concat!("Returns the ", $label, " name as text.")]
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[doc = concat!("Consumes the value and returns the owned ", $label, " name.")]
            #[must_use]
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = IdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

identity_component!(
    CatalogName,
    "catalog",
    "A validated, portable Arrow collector catalog name."
);
identity_component!(
    SchemaName,
    "schema",
    "A validated, portable Arrow collector schema name."
);
identity_component!(TableName, "table", "A validated, portable Arrow collector table name.");

/// A fully qualified table identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TableIdentity {
    catalog: CatalogName,
    schema: SchemaName,
    table: TableName,
}

impl TableIdentity {
    /// Creates a fully qualified table identity from validated components.
    #[must_use]
    pub const fn new(catalog: CatalogName, schema: SchemaName, table: TableName) -> Self {
        Self { catalog, schema, table }
    }

    /// Returns the catalog component.
    #[must_use]
    pub const fn catalog(&self) -> &CatalogName {
        &self.catalog
    }

    /// Returns the schema component.
    #[must_use]
    pub const fn schema(&self) -> &SchemaName {
        &self.schema
    }

    /// Returns the table component.
    #[must_use]
    pub const fn table(&self) -> &TableName {
        &self.table
    }
}

impl fmt::Display for TableIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// The collector implementation pool selected for a delivery.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImplementationPool {
    /// The collector implemented with the OTAP dataflow engine.
    Dataflow,
}

impl fmt::Display for ImplementationPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Dataflow => "dataflow",
        })
    }
}

impl FromStr for ImplementationPool {
    type Err = PoolParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "dataflow" => Ok(Self::Dataflow),
            unsupported => Err(PoolParseError(unsupported.to_owned())),
        }
    }
}

/// Failure to parse an implementation pool.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("unsupported implementation pool {0:?}")]
pub struct PoolParseError(pub String);

/// Failure to validate a catalog, schema, or table component.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IdentityError {
    /// The component was empty.
    #[error("{component} name must not be empty")]
    Empty {
        /// The identity component being validated.
        component: &'static str,
    },
    /// The component exceeded its byte limit.
    #[error("{component} name is {actual} bytes; maximum is {maximum}")]
    TooLong {
        /// The identity component being validated.
        component: &'static str,
        /// Actual UTF-8 byte length.
        actual: usize,
        /// Maximum permitted UTF-8 byte length.
        maximum: usize,
    },
    /// The component used a character outside the portable grammar.
    #[error("{component} name contains invalid character {character:?} at byte {index}")]
    InvalidCharacter {
        /// The identity component being validated.
        component: &'static str,
        /// Byte offset of the invalid character.
        index: usize,
        /// The invalid character.
        character: char,
    },
}

fn validate_component(component: &'static str, value: &str) -> Result<(), IdentityError> {
    if value.is_empty() {
        return Err(IdentityError::Empty { component });
    }
    if value.len() > MAX_IDENTITY_COMPONENT_BYTES {
        return Err(IdentityError::TooLong {
            component,
            actual: value.len(),
            maximum: MAX_IDENTITY_COMPONENT_BYTES,
        });
    }

    for (index, character) in value.char_indices() {
        let valid = if index == 0 {
            character.is_ascii_alphanumeric() || character == '_'
        } else {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
        };
        if !valid {
            return Err(IdentityError::InvalidCharacter {
                component,
                index,
                character,
            });
        }
    }
    Ok(())
}
