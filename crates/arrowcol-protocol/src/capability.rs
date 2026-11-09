use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{CollectorInstanceId, ImplementationPool, PositiveMeasurement, ProtocolVersion};

/// Arrow schema compatibility line used by this contract crate.
pub const ARROW_SCHEMA_CONTRACT_VERSION: &str = "58.3";

/// A collector behavior advertised to routers and operators.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// The instance can publish Arrow deliveries to Delta tables.
    DeltaPublication,
    /// The instance enforces table lease fencing.
    TableLeaseFencing,
    /// The instance emits bounded pressure snapshots.
    PressureReporting,
    /// Accepted payloads are persisted to local durable storage before they
    /// are acknowledged and are replayed after a restart.
    DurablePayloadStorage,
}

/// Versioned capability document for one collector instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectorCapabilities {
    protocol_version: ProtocolVersion,
    instance_id: CollectorInstanceId,
    implementation_pool: ImplementationPool,
    supported_protocol_versions: Vec<ProtocolVersion>,
    max_request_bytes: PositiveMeasurement,
    max_concurrent_streams: PositiveMeasurement,
    capabilities: BTreeSet<Capability>,
}

impl CollectorCapabilities {
    /// Creates a capability document for the current protocol.
    #[must_use]
    pub fn new(
        instance_id: CollectorInstanceId,
        implementation_pool: ImplementationPool,
        max_request_bytes: PositiveMeasurement,
        max_concurrent_streams: PositiveMeasurement,
        capabilities: BTreeSet<Capability>,
    ) -> Self {
        Self {
            protocol_version: ProtocolVersion::CURRENT,
            instance_id,
            implementation_pool,
            supported_protocol_versions: vec![ProtocolVersion::CURRENT],
            max_request_bytes,
            max_concurrent_streams,
            capabilities,
        }
    }

    /// Returns the capability document version.
    #[must_use]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    /// Returns the collector instance identifier.
    #[must_use]
    pub const fn instance_id(&self) -> CollectorInstanceId {
        self.instance_id
    }

    /// Returns the implementation pool served by this instance.
    #[must_use]
    pub const fn implementation_pool(&self) -> ImplementationPool {
        self.implementation_pool
    }

    /// Returns protocol versions accepted by this instance.
    #[must_use]
    pub fn supported_protocol_versions(&self) -> &[ProtocolVersion] {
        &self.supported_protocol_versions
    }

    /// Returns the maximum accepted request size in bytes.
    #[must_use]
    pub const fn max_request_bytes(&self) -> PositiveMeasurement {
        self.max_request_bytes
    }

    /// Returns the maximum concurrent inbound stream count.
    #[must_use]
    pub const fn max_concurrent_streams(&self) -> PositiveMeasurement {
        self.max_concurrent_streams
    }

    /// Returns advertised behaviors.
    #[must_use]
    pub const fn capabilities(&self) -> &BTreeSet<Capability> {
        &self.capabilities
    }

    /// Returns whether the instance advertises a behavior.
    #[must_use]
    pub fn supports(&self, capability: Capability) -> bool {
        self.capabilities.contains(&capability)
    }
}

impl Serialize for CollectorCapabilities {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            protocol_version: ProtocolVersion,
            instance_id: CollectorInstanceId,
            implementation_pool: ImplementationPool,
            supported_protocol_versions: &'a [ProtocolVersion],
            arrow_schema_contract_version: &'static str,
            max_request_bytes: PositiveMeasurement,
            max_concurrent_streams: PositiveMeasurement,
            capabilities: &'a BTreeSet<Capability>,
        }

        Wire {
            protocol_version: self.protocol_version,
            instance_id: self.instance_id,
            implementation_pool: self.implementation_pool,
            supported_protocol_versions: &self.supported_protocol_versions,
            arrow_schema_contract_version: ARROW_SCHEMA_CONTRACT_VERSION,
            max_request_bytes: self.max_request_bytes,
            max_concurrent_streams: self.max_concurrent_streams,
            capabilities: &self.capabilities,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CollectorCapabilities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            protocol_version: ProtocolVersion,
            instance_id: CollectorInstanceId,
            implementation_pool: ImplementationPool,
            supported_protocol_versions: Vec<ProtocolVersion>,
            arrow_schema_contract_version: String,
            max_request_bytes: PositiveMeasurement,
            max_concurrent_streams: PositiveMeasurement,
            capabilities: Vec<Capability>,
        }

        let wire = Wire::deserialize(deserializer)?;
        if wire.arrow_schema_contract_version != ARROW_SCHEMA_CONTRACT_VERSION {
            return Err(de::Error::custom(CapabilityError::ArrowSchemaVersionMismatch));
        }
        if wire.supported_protocol_versions.is_empty() {
            return Err(de::Error::custom(CapabilityError::NoSupportedProtocolVersions));
        }
        let unique: BTreeSet<_> = wire.supported_protocol_versions.iter().copied().collect();
        if unique.len() != wire.supported_protocol_versions.len() {
            return Err(de::Error::custom(CapabilityError::DuplicateProtocolVersion));
        }
        if !unique.contains(&wire.protocol_version) {
            return Err(de::Error::custom(CapabilityError::DocumentVersionNotSupported));
        }
        let capability_count = wire.capabilities.len();
        let capabilities: BTreeSet<_> = wire.capabilities.into_iter().collect();
        if capabilities.len() != capability_count {
            return Err(de::Error::custom(CapabilityError::DuplicateCapability));
        }

        Ok(Self {
            protocol_version: wire.protocol_version,
            instance_id: wire.instance_id,
            implementation_pool: wire.implementation_pool,
            supported_protocol_versions: wire.supported_protocol_versions,
            max_request_bytes: wire.max_request_bytes,
            max_concurrent_streams: wire.max_concurrent_streams,
            capabilities,
        })
    }
}

/// Failure to validate a collector capability document.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CapabilityError {
    /// The document advertised a different Arrow compatibility line.
    #[error("unsupported Arrow schema contract version")]
    ArrowSchemaVersionMismatch,
    /// At least one supported protocol is required.
    #[error("supported protocol versions must not be empty")]
    NoSupportedProtocolVersions,
    /// A supported protocol appeared more than once.
    #[error("supported protocol versions contain a duplicate")]
    DuplicateProtocolVersion,
    /// The document's own version was not in its supported set.
    #[error("document protocol version is not advertised as supported")]
    DocumentVersionNotSupported,
    /// An advertised capability appeared more than once.
    #[error("capability document contains a duplicate capability")]
    DuplicateCapability,
}
