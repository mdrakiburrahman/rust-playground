use assert_cmd::Command;
use predicates::prelude::*;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

fn hello_world() -> Command {
    Command::new(env!("CARGO_BIN_EXE_hello-world"))
}

#[test]
fn greets_the_default_name() {
    hello_world().assert().success().stdout("Hello, World!\n");
}

#[test]
fn greets_the_requested_name() {
    hello_world()
        .args(["--name", "Ferris"])
        .assert()
        .success()
        .stdout("Hello, Ferris!\n");
}

#[test]
fn rejects_a_blank_name() {
    hello_world()
        .args(["--name", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("name must not be empty"));
}

#[test]
fn greets_a_finite_number_of_times() {
    hello_world()
        .args(["--name", "Ferris", "--count", "3", "--interval-ms", "1"])
        .assert()
        .success()
        .stdout("Hello, Ferris!\nHello, Ferris!\nHello, Ferris!\n")
        .stderr(predicate::str::contains("greeting generated"));
}

#[test]
fn rejects_invalid_options() {
    for args in [
        vec!["--count", "0"],
        vec!["--count", "-1"],
        vec!["--count", "no"],
        vec!["--interval-ms", "0"],
        vec!["--interval-ms", "-1"],
        vec!["--interval-ms", "no"],
        vec!["--run-id", " "],
        vec!["--repeat", "--count", "3"],
    ] {
        hello_world().args(args).assert().failure();
    }
}

#[test]
fn telemetry_is_opt_in() {
    hello_world()
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
        .assert()
        .success()
        .stdout("Hello, World!\n");
}

#[test]
fn rejects_an_invalid_telemetry_endpoint() {
    telemetry_command("not an endpoint")
        .arg("--telemetry")
        .assert()
        .failure();
}

#[test]
fn fails_when_collector_is_unavailable() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    drop(listener);
    telemetry_command(&endpoint)
        .arg("--telemetry")
        .assert()
        .failure()
        .stdout("Hello, World!\n")
        .stderr(predicate::str::contains("telemetry export failed"));
    Ok(())
}

#[test]
fn exports_all_signals_as_protobuf() -> Result<(), Box<dyn std::error::Error>> {
    let collector = Collector::start(None)?;
    telemetry_command(&collector.endpoint)
        .args([
            "--telemetry",
            "--count",
            "3",
            "--interval-ms",
            "1",
            "--run-id",
            "cli-marker",
        ])
        .assert()
        .success()
        .stdout("Hello, World!\nHello, World!\nHello, World!\n");
    let requests = collector.finish()?;
    for (path, signal) in [
        ("/v1/logs", "greeting generated"),
        ("/v1/metrics", "hello_world.greetings"),
        ("/v1/traces", "greeting"),
    ] {
        let matching: Vec<_> = requests.iter().filter(|request| request.path == path).collect();
        assert!(!matching.is_empty(), "missing {path}");
        for request in matching {
            assert!(request.headers.to_lowercase().contains("application/x-protobuf"));
            for marker in [
                signal,
                "run.id",
                "cli-marker",
                "name",
                "World",
                "service.name",
                "hello-world",
            ] {
                assert!(
                    request
                        .body
                        .windows(marker.len())
                        .any(|bytes| bytes == marker.as_bytes()),
                    "missing {marker} in {path}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn fails_for_each_signal_export_failure() -> Result<(), Box<dyn std::error::Error>> {
    for path in ["/v1/logs", "/v1/metrics", "/v1/traces"] {
        let collector = Collector::start(Some(path))?;
        telemetry_command(&collector.endpoint)
            .arg("--telemetry")
            .assert()
            .failure()
            .stderr(predicate::str::contains("telemetry export failed"));
        collector.finish()?;
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn repeat_stops_gracefully_on_sigterm() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::BufRead;
    use std::process::{Command as ProcessCommand, Stdio};
    use std::time::Instant;

    let mut child = ProcessCommand::new(env!("CARGO_BIN_EXE_hello-world"))
        .args(["--repeat", "--interval-ms", "60000"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let stdout = child.stdout.take().ok_or("missing stdout")?;
    let mut greeting = String::new();
    std::io::BufReader::new(stdout).read_line(&mut greeting)?;
    assert_eq!(greeting, "Hello, World!\n");
    let signal = ProcessCommand::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()?;
    assert!(signal.success());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait()? {
            assert!(status.success());
            return Ok(());
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err("interrupt did not wake the interval wait".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn telemetry_command(endpoint: &str) -> Command {
    let mut command = hello_world();
    for (key, _) in std::env::vars().filter(|(key, _)| key.starts_with("OTEL_")) {
        command.env_remove(key);
    }
    command
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
        .env("OTEL_EXPORTER_OTLP_TIMEOUT", "100")
        .timeout(Duration::from_secs(10));
    command
}

struct Request {
    path: String,
    headers: String,
    body: Vec<u8>,
}

struct Collector {
    endpoint: String,
    stop: Arc<AtomicBool>,
    worker: thread::JoinHandle<std::io::Result<Vec<Request>>>,
}

impl Collector {
    fn start(failing_path: Option<&'static str>) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            let mut requests = Vec::new();
            while !stopping.load(Ordering::Relaxed) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut bytes = Vec::new();
                let mut buffer = [0; 4096];
                let (headers, header_length, body_length) = loop {
                    let size = stream.read(&mut buffer)?;
                    if size == 0 {
                        return Err(std::io::ErrorKind::UnexpectedEof.into());
                    }
                    bytes.extend_from_slice(&buffer[..size]);
                    if let Some(index) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..index]).into_owned();
                        let body_length = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                            .ok_or(std::io::ErrorKind::InvalidData)?;
                        break (headers, index + 4, body_length);
                    }
                };
                while bytes.len() < header_length + body_length {
                    let size = stream.read(&mut buffer)?;
                    if size == 0 {
                        return Err(std::io::ErrorKind::UnexpectedEof.into());
                    }
                    bytes.extend_from_slice(&buffer[..size]);
                }
                let path = headers
                    .split_whitespace()
                    .nth(1)
                    .ok_or(std::io::ErrorKind::InvalidData)?
                    .to_owned();
                let status = if failing_path == Some(path.as_str()) {
                    "503 Service Unavailable"
                } else {
                    "200 OK"
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )?;
                requests.push(Request {
                    path,
                    headers,
                    body: bytes[header_length..].to_vec(),
                });
            }
            Ok(requests)
        });
        Ok(Self { endpoint, stop, worker })
    }

    fn finish(self) -> std::io::Result<Vec<Request>> {
        self.stop.store(true, Ordering::Relaxed);
        self.worker
            .join()
            .map_err(|_| std::io::Error::other("collector thread panicked"))?
    }
}
