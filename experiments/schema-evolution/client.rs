#[cfg(not(feature = "v2"))]
use std::io::BufRead;
use std::io::{self, Write};

use arrow_flight::flight_service_client::FlightServiceClient;
use futures::TryStreamExt;
use serde_json::json;

#[cfg(feature = "v2")]
const CLIENT: &str = "new";
#[cfg(not(feature = "v2"))]
const CLIENT: &str = "old";

async fn append(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
    phase: &str,
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = (0..count)
        .map(|value| {
            Ok(sdk::FooRow {
                client: CLIENT.to_owned(),
                phase: phase.to_owned(),
                value: i64::try_from(value)?,
                tenant: "evo".to_owned(),
                #[cfg(feature = "v2")]
                bar: Some(format!("bar-{value}")),
            })
        })
        .collect::<Result<Vec<_>, std::num::TryFromIntError>>()?;
    let batch = sdk::record_batch(&rows)?;
    let mut response = sdk::do_put(client, [batch], sdk::ImplementationPool::Dataflow)
        .await?
        .into_inner();
    while response.message().await?.is_some() {}
    println!(
        "{}",
        json!({
            "event": "accepted", "client": CLIENT, "phase": phase, "rows": count,
            "pid": std::process::id(), "version": sdk::SCHEMA_VERSION,
            "fingerprint": sdk::SCHEMA_FINGERPRINT
        })
    );
    io::stdout().flush()?;
    Ok(())
}

async fn wrong_schema(
    client: &mut FlightServiceClient<tonic::transport::Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch = arrow_array::RecordBatch::try_new(
        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "wrong_column",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![std::sync::Arc::new(arrow_array::Int64Array::from(vec![1]))],
    )?;
    let encoded = arrow_flight::encode::FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(sdk::flight_descriptor()))
        .build(futures::stream::iter([Ok(batch)]))
        .try_collect::<Vec<_>>()
        .await?;
    let request = sdk::decorate_do_put_request(
        tonic::Request::new(futures::stream::iter(encoded)),
        sdk::ImplementationPool::Dataflow,
    )?;
    let status = match client.do_put(request).await {
        Err(status) => status,
        Ok(response) => {
            let mut stream = response.into_inner();
            loop {
                match stream.message().await {
                    Err(status) => break status,
                    Ok(Some(_)) => {}
                    Ok(None) => return Err("DFE accepted a wrong-schema client".into()),
                }
            }
        }
    };
    if status.code() != tonic::Code::InvalidArgument {
        return Err(format!("wrong schema failed for a non-schema reason: {status}").into());
    }
    println!(
        "{}",
        json!({"event": "rejected", "code": "InvalidArgument", "message": status.message()})
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.as_slice() == ["describe"] {
        println!(
            "{}",
            json!({
                "client": CLIENT, "version": sdk::SCHEMA_VERSION, "fingerprint": sdk::SCHEMA_FINGERPRINT,
                "fields": sdk::FIELD_METADATA.iter().map(|field| field.name).collect::<Vec<_>>()
            })
        );
        return Ok(());
    }
    if args.len() != 3 {
        return Err("usage: schema-evolution-client ENDPOINT ROWS write|wrong-schema".into());
    }
    let count = args[1].parse::<usize>()?;
    if !(1..=10_000).contains(&count) {
        return Err("rows must be 1..10000".into());
    }
    let channel = tonic::transport::Endpoint::from_shared(args[0].clone())?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);
    match args[2].as_str() {
        "wrong-schema" => wrong_schema(&mut client).await?,
        "write" => {
            #[cfg(not(feature = "v2"))]
            {
                append(&mut client, "before", count).await?;
                let line = io::stdin().lock().lines().next().transpose()?;
                if line.as_deref() != Some("after") {
                    return Err(
                        "old client requires an 'after' line after ALTER; refusing restart-shaped proof".into(),
                    );
                }
            }
            append(&mut client, "after", count).await?;
        }
        _ => return Err("mode must be write or wrong-schema".into()),
    }
    Ok(())
}
