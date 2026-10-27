//! Local-only scheduling/cancellation proof: no emulator or collector is contacted.
use anyhow::{Context, Result, ensure};
use arrowcol_loadgen::observe_qap;
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

struct State {
    stop: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
    requests: Mutex<Vec<String>>,
}

struct Fixture {
    endpoint: String,
    state: Arc<State>,
    task: Option<JoinHandle<Result<()>>>,
    directory: PathBuf,
}

impl Fixture {
    fn new(delay: Duration) -> Result<Self> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let directory = PathBuf::from(format!(
            "target/observer-scheduling/{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&directory)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(State {
            stop: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        });
        let owned = Arc::clone(&state);
        let task = thread::spawn(move || {
            let mut handlers = Vec::new();
            while !owned.stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((socket, _)) => {
                        let state = Arc::clone(&owned);
                        handlers.push(thread::spawn(move || handle(socket, &state, delay)));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            for handler in handlers {
                handler.join().map_err(|_| anyhow::anyhow!("fixture handler panic"))??;
            }
            Ok(())
        });
        Ok(Self {
            endpoint,
            state,
            task: Some(task),
            directory,
        })
    }

    fn finish(&mut self) -> Result<()> {
        self.state.stop.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !task.is_finished() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            ensure!(task.is_finished(), "fixture cleanup exceeded two seconds");
            task.join().map_err(|_| anyhow::anyhow!("fixture listener panic"))??;
        }
        ensure!(
            self.state.active.load(Ordering::Acquire) == 0,
            "active fixture handlers remain"
        );
        Ok(())
    }

    fn requests(&self) -> Result<Vec<String>> {
        Ok(self
            .state
            .requests
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture mutex"))?
            .clone())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let cleanup = self.finish();
        let removed = fs::remove_dir_all(&self.directory);
        if !thread::panicking() {
            assert!(cleanup.is_ok(), "{cleanup:?}");
            assert!(removed.is_ok(), "{removed:?}");
        }
    }
}

struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn handle(mut socket: TcpStream, state: &State, delay: Duration) -> Result<()> {
    let active = state.active.fetch_add(1, Ordering::AcqRel) + 1;
    state.peak.fetch_max(active, Ordering::AcqRel);
    let _active = Active(&state.active);
    socket.set_read_timeout(Some(Duration::from_secs(1)))?;
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        socket.read_exact(&mut byte)?;
        header.push(byte[0]);
    }
    let header = String::from_utf8(header)?;
    let length: usize = header
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(str::to_owned)
        })
        .context("fixture content length")?
        .trim()
        .parse()?;
    let mut body = vec![0; length];
    socket.read_exact(&mut body)?;
    let request: Value = serde_json::from_slice(&body)?;
    let command = request["csl"].as_str().context("fixture command")?.to_owned();
    state
        .requests
        .lock()
        .map_err(|_| anyhow::anyhow!("fixture mutex"))?
        .push(command.clone());
    socket.set_nonblocking(true)?;
    let deadline = Instant::now() + delay;
    while Instant::now() < deadline && !state.stop.load(Ordering::Acquire) {
        match socket.peek(&mut [0]) {
            Ok(0) => return Ok(()),
            Ok(_) => anyhow::bail!("unexpected pipelined fixture request"),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        thread::sleep(Duration::from_millis(5));
    }
    if state.stop.load(Ordering::Acquire) {
        return Ok(());
    }
    socket.set_nonblocking(false)?;
    socket.set_write_timeout(Some(Duration::from_secs(1)))?;
    let response = if command.ends_with("policy query_acceleration") {
        response(
            &["Policy"],
            vec![json!(["{\"IsEnabled\":true,\"MaxAge\":\"00:01:00\"}"])],
        )
    } else if command.ends_with(" details") {
        response(&["QueryAccelerationState"], vec![json!(["{\"IsHealthy\":true}"])])
    } else if command == ".show capacity" {
        response(&["Resource", "Total"], vec![json!(["Ingestions", 2])])
    } else {
        response(&[], vec![])
    };
    let body = response.to_string();
    match write!(
        socket,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    ) {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn response(columns: &[&str], rows: Vec<Value>) -> Value {
    json!({"Tables":[{"TableName":"PrimaryResult",
        "Columns":columns.iter().map(|name| json!({"ColumnName":name,"DataType":"String"})).collect::<Vec<_>>(),
        "Rows":rows}]})
}

#[tokio::test]
async fn all26_delayed_observations_complete_inside_unchanged_30s_with_two_request_slots() -> Result<()> {
    let mut fixture = Fixture::new(Duration::from_millis(450))?;
    let tables = ["direct", "dataflow"]
        .into_iter()
        .flat_map(|pool| (0..13).map(move |index| format!("{pool}_fixture_{index}")))
        .collect::<Vec<_>>();
    let started = Instant::now();
    let outcome = observe_qap(
        &fixture.endpoint,
        &fixture.directory,
        Some("loadtest"),
        &tables,
        Duration::from_secs(30),
    )
    .await;
    let elapsed = started.elapsed();
    fixture.finish()?;
    eprintln!(
        "observer scheduling receipt: elapsed_ms={} peak={} requests={} success={}",
        elapsed.as_millis(),
        fixture.state.peak.load(Ordering::Acquire),
        fixture.requests()?.len(),
        outcome.is_ok()
    );
    let observation = outcome?;
    ensure!(
        elapsed < Duration::from_secs(30),
        "observation exceeded unchanged deadline"
    );
    ensure!(
        fixture.state.peak.load(Ordering::Acquire) == 2,
        "must overlap exactly two requests"
    );
    let requests = fixture.requests()?;
    ensure!(requests.len() == 79 && requests.last().is_some_and(|command| command == ".show capacity"));
    let observed = observation["tables"].as_object().context("observed table map")?;
    ensure!(observed.len() == 26 && tables.iter().all(|table| observed.contains_key(table)));
    for table in &tables {
        let identifier = format!("['{table}']");
        let literal = format!("ExternalTableName == \"{table}\"");
        let commands = requests
            .iter()
            .filter(|command| command.contains(&identifier) || command.contains(&literal))
            .collect::<Vec<_>>();
        ensure!(commands.len() == 3, "one serial triple per table");
        ensure!(commands[0].ends_with("policy query_acceleration"));
        ensure!(commands[1].contains("query_acceleration statistics"));
        ensure!(commands[2].ends_with(" details"));
    }
    let lines = fs::read_to_string(fixture.directory.join("kusto-queries.jsonl"))?;
    let artifacts = lines
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(artifacts.len() == 79, "every response must have a whole JSONL artifact");
    let diagnostic: Value = serde_json::from_slice(&fs::read(fixture.directory.join("observation-diagnostic.json"))?)?;
    ensure!(diagnostic["queries_completed"] == 79 && diagnostic["code"] == "completed");
    Ok(())
}

#[tokio::test]
async fn deadline_cancels_both_slots_and_joins_handlers_without_late_artifacts() -> Result<()> {
    let mut fixture = Fixture::new(Duration::from_secs(60))?;
    let outcome = observe_qap(
        &fixture.endpoint,
        &fixture.directory,
        Some("loadtest"),
        &["direct_a".to_owned(), "dataflow_b".to_owned()],
        Duration::from_millis(250),
    )
    .await;
    ensure!(outcome.is_err(), "held responses must fail closed");
    ensure!(fixture.state.peak.load(Ordering::Acquire) == 2);
    let diagnostic_path = fixture.directory.join("observation-diagnostic.json");
    let diagnostic = fs::read(&diagnostic_path)?;
    let parsed: Value = serde_json::from_slice(&diagnostic)?;
    ensure!(parsed["code"] == "deadline_exceeded" && parsed["queries_completed"] == 0);
    let until = Instant::now() + Duration::from_secs(2);
    while fixture.state.active.load(Ordering::Acquire) != 0 && Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    ensure!(
        fixture.state.active.load(Ordering::Acquire) == 0,
        "cancelled sockets must close"
    );
    fixture.finish()?;
    let requests = fixture.requests()?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    ensure!(fixture.requests()? == requests && requests.len() == 2);
    ensure!(fs::read(diagnostic_path)? == diagnostic, "no late counter growth");
    ensure!(
        !fixture.directory.join("kusto-queries.jsonl").exists(),
        "no post-return appends"
    );
    eprintln!("observer cancellation receipt: active=0 joined=true requests=2 late_artifacts=0");
    Ok(())
}
