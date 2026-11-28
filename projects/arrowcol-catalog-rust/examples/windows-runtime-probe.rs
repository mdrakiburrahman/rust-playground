use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::{
    FlightData, FlightDescriptor,
    encode::FlightDataEncoderBuilder,
    flight_service_client::FlightServiceClient,
    sql::{CommandStatementUpdate, ProstMessageExt},
};
use arrow_schema::{DataType, Field, Schema};
use arrowcol_catalog_client::InternalRequestSigner;
use arrowcol_protocol::{
    CatalogName, ClaimedSchemaFingerprint, ImplementationPool, IngestMetadata, ProtocolVersion, SchemaName,
    TableIdentity, TableName, fingerprint_schema,
};
use futures::{TryStreamExt, stream};
use prost::Message;
use tonic::{Code, Request, metadata::MetadataValue, transport::Endpoint};

async fn verify_ddl() -> Result<()> {
    let channel = Endpoint::from_static("http://127.0.0.1:8817").connect().await?;
    let mut client = FlightServiceClient::new(channel);
    let payload = CommandStatementUpdate {
        query: "CREATE TABLE arrowcol.fixture.windows_signed_probe (id BIGINT NOT NULL) USING DELTA".into(),
        transaction_id: None,
    }
    .as_any()
    .encode_to_vec();
    let request = || {
        Request::new(stream::iter([FlightData {
            flight_descriptor: Some(FlightDescriptor::new_cmd(payload.clone())),
            ..FlightData::default()
        }]))
    };
    let status = client
        .do_put(request())
        .await
        .err()
        .context("unsigned DDL was accepted")?;
    ensure!(
        status.code() == Code::Unauthenticated,
        "unexpected unsigned DDL response: {status}"
    );

    let key = std::env::var_os("ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE").context("missing identity key path")?;
    let mut request = request();
    InternalRequestSigner::from_file(key)?.sign_payload(
        &mut request,
        "/arrow.flight.protocol.FlightService/DoPut",
        &payload,
    )?;
    let mut response = client.do_put(request).await?.into_inner();
    ensure!(
        response.message().await?.is_some(),
        "signed DDL returned no acknowledgment"
    );
    while response.message().await?.is_some() {}
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    verify_ddl().await?;
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
        request
            .metadata_mut()
            .insert(key, MetadataValue::try_from(value.as_str())?);
    }
    let channel = Endpoint::from_static("http://127.0.0.1:32310").connect().await?;
    let mut client = FlightServiceClient::new(channel);
    let mut response = client.do_put(request).await?.into_inner();
    ensure!(
        response.message().await?.is_some(),
        "Flight returned no ingestion acknowledgment"
    );
    while response.message().await?.is_some() {}
    Ok(())
}
