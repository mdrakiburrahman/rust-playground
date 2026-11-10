use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start(listen: &str, allow_remote: bool) -> std::io::Result<Server> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_arrowcol-oat-rust"));
    command
        .args(["--listen", listen])
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if allow_remote {
        command.arg("--allow-insecure-non-loopback");
    }
    Ok(Server(command.spawn()?))
}

#[test]
fn non_loopback_api_requires_explicit_opt_in_and_loopback_still_works() -> std::io::Result<()> {
    for listen in ["0.0.0.0:0", "[::]:0"] {
        let mut server = start(listen, false)?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let status = loop {
            if let Some(status) = server.0.try_wait()? {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "unauthenticated listener started without opt-in: {listen}"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(!status.success());
        let mut error = String::new();
        if let Some(mut stderr) = server.0.stderr.take() {
            stderr.read_to_string(&mut error)?;
        }
        assert!(
            error.contains("non-loopback OAT listeners require --allow-insecure-non-loopback"),
            "{error}"
        );
    }
    for (host, allow_remote) in [("127.0.0.1", false), ("0.0.0.0", true)] {
        let port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let mut server = start(&format!("{host}:{port}"), allow_remote)?;
        let deadline = Instant::now() + Duration::from_secs(3);
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let mut stream = loop {
            if let Ok(stream) = TcpStream::connect_timeout(&address, Duration::from_millis(50)) {
                break stream;
            }
            assert!(server.0.try_wait()?.is_none(), "permitted listener exited");
            assert!(Instant::now() < deadline, "permitted listener did not start");
            std::thread::sleep(Duration::from_millis(10));
        };
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        stream.set_write_timeout(Some(Duration::from_secs(3)))?;
        stream.write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("\"status\":\"ok\""), "{response}");
    }
    Ok(())
}
