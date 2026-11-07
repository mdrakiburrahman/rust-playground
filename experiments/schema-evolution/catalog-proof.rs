use arrowcol_catalog_client::rpc::{
    Empty, EventPosition, EventsRequest, TableRequest, schema_catalog_client::SchemaCatalogClient,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 1 && args.len() != 3 {
        return Err("usage: catalog-proof ENDPOINT [EPOCH EVENT_ID]".into());
    }
    let mut client = SchemaCatalogClient::connect(args[0].clone()).await?;
    let current = client.get_current_event_id(Empty {}).await?.into_inner();
    let from = if args.len() == 3 {
        EventPosition {
            epoch: args[1].clone(),
            event_id: args[2].parse()?,
        }
    } else {
        EventPosition {
            epoch: current.epoch.clone(),
            event_id: 0,
        }
    };
    let events = client
        .get_events(EventsRequest { from: Some(from) })
        .await?
        .into_inner();
    let snapshot = client
        .get_table(TableRequest {
            catalog: "schema_evolution".to_owned(),
            database: "public".to_owned(),
            table: "foo".to_owned(),
        })
        .await?
        .into_inner();
    let history: serde_json::Value = serde_json::from_slice(&snapshot.schema_history_json)?;
    println!(
        "{}",
        json!({
            "position": {"epoch": current.epoch, "eventId": current.event_id},
            "events": events.events.iter().map(|event| json!({
                "position": event.position.as_ref().map(|position| json!({"epoch": position.epoch, "eventId": position.event_id})),
                "type": event.event_type, "version": event.schema_version,
                "table": event.table.as_ref().map(|table| json!({"catalog": table.catalog, "database": table.database, "table": table.table}))
            })).collect::<Vec<_>>(),
            "snapshot": {
                "provider": snapshot.provider, "location": snapshot.location,
                "partitions": snapshot.partition_columns, "properties": snapshot.properties,
                "history": history,
                "arrowSchemaBytes": snapshot.arrow_schema_ipc.len(),
                "manifestBytes": snapshot.manifest.len(),
                "protobufDescriptorBytes": snapshot.protobuf_descriptor.len()
            }
        })
    );
    Ok(())
}
