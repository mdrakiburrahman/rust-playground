use std::sync::Arc;

use anyhow::{Result, ensure};
use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::{FlightDescriptor, encode::FlightDataEncoderBuilder, flight_service_client::FlightServiceClient};
use arrow_schema::{DataType, Field, Schema};
use arrowcol_protocol::{
    CatalogName, ClaimedSchemaFingerprint, ImplementationPool, IngestMetadata, ProtocolVersion, SchemaName,
    TableIdentity, TableName, fingerprint_schema,
};
use futures::{TryStreamExt, stream};
use tonic::{Request, metadata::MetadataValue};

#[tokio::main]
async fn main() -> Result<()> {
    let identity = TableIdentity::new(
        CatalogName::new("arrowcol")?,
        SchemaName::new("fixture")?,
        TableName::new("windows_probe")?,
    );
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![41, 42]))])?;
    let descriptor = FlightDescriptor::new_path(vec![
        identity.catalog().to_string(),
        identity.schema().to_string(),
        identity.table().to_string(),
        "v1".into(),
    ]);
    let frames: Vec<_> = FlightDataEncoderBuilder::new()
        .with_schema(Arc::clone(&schema))
        .with_flight_descriptor(Some(descriptor))
        .build(stream::iter([Ok(batch)]))
        .try_collect()
        .await?;
    let mut request = Request::new(stream::iter(frames));
    for (key, value) in IngestMetadata::new(
        ProtocolVersion::CURRENT,
        ImplementationPool::Dataflow,
        identity,
        ClaimedSchemaFingerprint::new(fingerprint_schema(schema.as_ref())),
    )
    .to_ascii_pairs()
    {
        request.metadata_mut().insert(key, MetadataValue::try_from(value.as_str())?);
    }
    let mut client = FlightServiceClient::connect("http://127.0.0.1:32310").await?;
    let mut response = client.do_put(request).await?.into_inner();
    ensure!(response.message().await?.is_some(), "Flight returned no ingestion acknowledgment");
    while response.message().await?.is_some() {}
    Ok(())
}
