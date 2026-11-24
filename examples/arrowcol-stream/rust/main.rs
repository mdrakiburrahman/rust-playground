use std::time::{Duration, Instant};
use sdk::{EventsStream, proto::Events};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut endpoint = "http://127.0.0.1:18830".to_owned();
    let mut seconds = 300u64;
    let mut interval_ms = 1000u64;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or("each option requires a value")?;
        match flag.as_str() {
            "--endpoint" => endpoint = value,
            "--duration-seconds" => seconds = value.parse()?,
            "--interval-ms" => interval_ms = value.parse()?,
            _ => return Err(format!("unknown option: {flag}").into()),
        }
    }
    if seconds == 0 || seconds > 86_400 || interval_ms == 0 || interval_ms > 60_000 {
        return Err("duration must be 1..86400 seconds and interval 1..60000 ms".into());
    }
    let mut stream = EventsStream::connect(&endpoint, Default::default()).await?;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut id = 0i64;
    while Instant::now() < deadline {
        // Edit these values to experiment. Protobuf presence is validated against SQL nullability.
        stream.push(Events {
            event_id: Some(id),
            observed_at: Some(1_700_000_000_000_000 + id),
            payload: Some(format!("rust-event-{id}")),
        }).await?;
        id += 1;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => { signal?; break; },
            _ = tokio::time::sleep(Duration::from_millis(interval_ms).min(deadline.saturating_duration_since(Instant::now()))) => {}
        }
    }
    stream.flush().await?;
    stream.close().await?;
    println!("{{\"language\":\"rust\",\"admitted_rows\":{id},\"sink_commit_verified\":false}}");
    Ok(())
}
