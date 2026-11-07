use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;
use uuid::Uuid;

macro_rules! uuid_id {
    ($name:ident, $doc:literal, $new_doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        #[allow(clippy::new_without_default)]
        impl $name {
            #[doc = $new_doc]
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            #[doc = concat!("Returns the UUID backing this ", stringify!($name), ".")]
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.hyphenated().fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let id = Uuid::parse_str(value).map_err(|_| IdParseError::Malformed)?;
                if id.is_nil() {
                    return Err(IdParseError::Nil);
                }
                Ok(Self(id))
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(de::Error::custom)
            }
        }
    };
}

uuid_id!(
    CollectorInstanceId,
    "A generated identifier for one collector process lifetime.",
    "Generates an identifier for a newly starting collector instance."
);
uuid_id!(
    LeaseId,
    "A generated identifier for one table lease lineage.",
    "Generates an identifier for a newly assigned table lease."
);
uuid_id!(
    PublicationId,
    "An internal identifier used to deduplicate one DFE publication; it is not a client delivery receipt.",
    "Generates an identifier for a new DFE publication operation."
);

/// Failure to parse a UUID-backed contract identifier.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum IdParseError {
    /// The text was not a UUID.
    #[error("identifier is not a valid UUID")]
    Malformed,
    /// Nil UUIDs are reserved and never valid contract identifiers.
    #[error("nil UUID is not a valid identifier")]
    Nil,
}
