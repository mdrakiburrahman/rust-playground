use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

/// Last collector stage exercised by a Flight isolation run.
/// Only [`Self::L5`] runs the normal publication path.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IsolationLevel {
    /// Drain Flight payloads without decoding them.
    L0,
    /// Decode Arrow batches, then discard them.
    L1,
    /// Encode decoded batches as Parquet.
    L2,
    /// Write and fsync the encoded files locally.
    L3,
    /// Upload the files without publishing a Delta commit.
    L4,
    /// Run the normal collector and Delta publication path.
    #[default]
    L5,
}

impl IsolationLevel {
    /// Returns the exact token accepted by config files and environment variables.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::L0 => "l0",
            Self::L1 => "l1",
            Self::L2 => "l2",
            Self::L3 => "l3",
            Self::L4 => "l4",
            Self::L5 => "l5",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "l0" => Ok(Self::L0),
            "l1" => Ok(Self::L1),
            "l2" => Ok(Self::L2),
            "l3" => Ok(Self::L3),
            "l4" => Ok(Self::L4),
            "l5" => Ok(Self::L5),
            _ => Err("expected one of l0, l1, l2, l3, l4, l5"),
        }
    }
}

impl<'de> Deserialize<'de> for IsolationLevel {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?.parse().map_err(D::Error::custom)
    }
}
