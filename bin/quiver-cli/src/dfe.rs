//! DFE pluggable-Arrow bundle metadata and a Quiver bundle writer.

use std::time::SystemTime;

use anyhow::{Context, Result, bail};
use arrow::record_batch::RecordBatch;
use arrowcol_catalog::{CatalogSchemaVersion, SchemaVersion};
use arrowcol_protocol::TableIdentity;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use otel_arrow_dfe_quiver::record_bundle::{BundleDescriptor, PayloadRef, RecordBundle, SlotDescriptor, SlotId};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

const MAGIC: [u8; 4] = *b"QPA\x01";
const REPRESENTATION: &str = "arrowcol.flight";
const REPRESENTATION_VERSION: (u16, u16) = (1, 1);
const SIGNAL_LOGS: u8 = 0;
const ROOT_SLOT: u16 = 46;
const ROOT_TABLE_ID: u32 = 0;
const SCHEMA_ID_PREFIX: &str = "arrowcol.flight.v1:";

#[derive(Clone, Debug)]
pub(crate) struct ManifestTable {
    pub(crate) slot: u16,
    pub(crate) table_id: u32,
    pub(crate) fingerprint: [u8; 32],
    pub(crate) schema_id: String,
}

#[derive(Clone, Debug)]
pub(crate) struct DfeManifest {
    pub(crate) representation: String,
    pub(crate) major: u16,
    pub(crate) minor: u16,
    pub(crate) signal: u8,
    pub(crate) root_items: u64,
    pub(crate) tables: Vec<ManifestTable>,
}

impl DfeManifest {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let id = self.representation.as_bytes();
        let id_len = u16::try_from(id.len()).context("representation ID is too long")?;
        let table_count = u8::try_from(self.tables.len()).context("too many tables in DFE manifest")?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&id_len.to_le_bytes());
        bytes.extend_from_slice(id);
        bytes.extend_from_slice(&self.major.to_le_bytes());
        bytes.extend_from_slice(&self.minor.to_le_bytes());
        bytes.push(self.signal);
        bytes.extend_from_slice(&self.root_items.to_le_bytes());
        bytes.push(table_count);
        for table in &self.tables {
            bytes.extend_from_slice(&table.slot.to_le_bytes());
            bytes.extend_from_slice(&table.table_id.to_le_bytes());
            bytes.extend_from_slice(&table.fingerprint);
            let schema_id = table.schema_id.as_bytes();
            let len = u32::try_from(schema_id.len()).context("schema ID is too long")?;
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(schema_id);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader { bytes, position: 0 };
        if reader.take(MAGIC.len())? != MAGIC {
            bail!("unsupported Quiver bundle metadata format");
        }
        let id_len = usize::from(reader.u16()?);
        let representation = std::str::from_utf8(reader.take(id_len)?)?.to_owned();
        let major = reader.u16()?;
        let minor = reader.u16()?;
        let signal = reader.u8()?;
        let root_items = reader.u64()?;
        let table_count = usize::from(reader.u8()?);
        if table_count > 12 {
            bail!("DFE manifest has {table_count} tables; maximum is 12");
        }
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            let slot = reader.u16()?;
            let table_id = reader.u32()?;
            let mut fingerprint = [0; 32];
            fingerprint.copy_from_slice(reader.take(32)?);
            let schema_len = usize::try_from(reader.u32()?).context("schema ID length overflow")?;
            let schema_id = std::str::from_utf8(reader.take(schema_len)?)?.to_owned();
            tables.push(ManifestTable {
                slot,
                table_id,
                fingerprint,
                schema_id,
            });
        }
        if reader.position != bytes.len() {
            bail!("DFE manifest has trailing bytes");
        }
        Ok(Self {
            representation,
            major,
            minor,
            signal,
            root_items,
            tables,
        })
    }

    pub(crate) fn table_summary(&self) -> String {
        self.tables
            .iter()
            .map(|table| {
                decode_envelope(&table.schema_id).map_or_else(
                    || format!("table {}", table.table_id),
                    |envelope| format!("{} v{}", envelope.table, envelope.target_schema_version.get()),
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Deserialize)]
struct EnvelopeSummary {
    table: TableIdentity,
    target_schema_version: SchemaVersion,
}

fn decode_envelope(schema_id: &str) -> Option<EnvelopeSummary> {
    let encoded = schema_id.strip_prefix(SCHEMA_ID_PREFIX)?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub(crate) fn schema_description(schema_id: &str) -> Option<(String, u64)> {
    decode_envelope(schema_id).map(|envelope| (envelope.table.to_string(), envelope.target_schema_version.get()))
}

pub(crate) struct DfeRecordBundle {
    batch: RecordBatch,
    descriptor: BundleDescriptor,
    metadata: Vec<u8>,
    fingerprint: [u8; 32],
    ingestion_time: SystemTime,
}

impl DfeRecordBundle {
    pub(crate) fn new(schema: &CatalogSchemaVersion, batch: RecordBatch) -> Result<Self> {
        let fingerprint = *schema.fingerprint().as_bytes();
        let version = schema.version().get();
        let envelope = json!({
            "publication_id": null,
            "ingested_at_ms": null,
            "table": schema.identity(),
            "source_schema_version": version,
            "target_schema_version": version,
            "lease_id": Uuid::new_v4().to_string(),
            "holder_instance_id": Uuid::new_v4().to_string(),
            "fencing_token": 1,
            "instance_key": "quiver-cli"
        });
        let schema_id = format!(
            "{SCHEMA_ID_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&envelope)?)
        );
        let metadata = DfeManifest {
            representation: REPRESENTATION.to_owned(),
            major: REPRESENTATION_VERSION.0,
            minor: REPRESENTATION_VERSION.1,
            signal: SIGNAL_LOGS,
            root_items: u64::try_from(batch.num_rows())?,
            tables: vec![ManifestTable {
                slot: ROOT_SLOT,
                table_id: ROOT_TABLE_ID,
                fingerprint,
                schema_id,
            }],
        }
        .encode()?;
        let descriptor = BundleDescriptor::new(vec![SlotDescriptor::new(SlotId::new(ROOT_SLOT), "PluggableArrow")]);
        Ok(Self {
            batch,
            descriptor,
            metadata,
            fingerprint,
            ingestion_time: SystemTime::now(),
        })
    }
}

impl RecordBundle for DfeRecordBundle {
    fn descriptor(&self) -> &BundleDescriptor {
        &self.descriptor
    }

    fn ingestion_time(&self) -> SystemTime {
        self.ingestion_time
    }

    fn payload(&self, slot: SlotId) -> Option<PayloadRef<'_>> {
        (slot.raw() == ROOT_SLOT).then_some(PayloadRef {
            schema_fingerprint: self.fingerprint,
            batch: &self.batch,
        })
    }

    fn item_count(&self) -> u64 {
        u64::try_from(self.batch.num_rows()).unwrap_or(u64::MAX)
    }

    fn byte_count(&self) -> Option<u64> {
        u64::try_from(self.batch.get_array_memory_size()).ok()
    }

    fn metadata(&self) -> Option<&[u8]> {
        Some(&self.metadata)
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl Reader<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8]> {
        let end = self.position.checked_add(len).context("manifest length overflow")?;
        let value = self.bytes.get(self.position..end).context("truncated DFE manifest")?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    #[test]
    fn manifest_matches_the_dfe_pluggable_arrow_wire_format() -> Result<()> {
        let manifest = DfeManifest {
            representation: REPRESENTATION.to_owned(),
            major: 1,
            minor: 1,
            signal: SIGNAL_LOGS,
            root_items: 3,
            tables: vec![ManifestTable {
                slot: ROOT_SLOT,
                table_id: ROOT_TABLE_ID,
                fingerprint: [7; 32],
                schema_id: "arrowcol.flight.v1:abc".to_owned(),
            }],
        };
        let encoded = manifest.encode()?;
        assert_eq!(&encoded[..4], b"QPA\x01");
        assert_eq!(DfeManifest::decode(&encoded)?.tables[0].fingerprint, [7; 32]);
        assert!(DfeManifest::decode(&encoded[..encoded.len() - 1]).is_err());
        Ok(())
    }
}
