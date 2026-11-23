async fn local_router_state(config: &OatConfig, platform: &LocalPlatform) -> Result<Option<String>> {
    let compose_file = config
        .platform
        .compose_file
        .as_deref()
        .context("local compose file is missing")?;
    let base_project = config
        .platform
        .compose_project
        .as_deref()
        .context("local compose project name is missing")?;
    let project = scoped_compose_project(base_project, &platform.run_id);
    let mut ps = compose_command(
        compose_file,
        config.platform.compose_override_file.as_deref(),
        &project,
        "benchmark",
    );
    platform.add_compose_environment(&mut ps);
    ps.args(["ps", "--all", "--quiet", "router"]);
    let ids = run_command(ps, "inspect local router container").await?;
    let ids = String::from_utf8(ids.stdout).context("router container ID is not UTF-8")?;
    let ids: Vec<_> = ids.lines().filter(|line| !line.is_empty()).collect();
    ensure!(ids.len() == 1, "expected exactly one local router container");
    let id = ids[0];
    ensure!(
        id.len() >= 12 && id.len() <= 64 && id.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid router container ID"
    );
    let mut inspect = Command::new("docker");
    inspect.envs(&platform.environment);
    inspect.args(["inspect", "--format", "{{.State.Status}}|{{.State.ExitCode}}", id]);
    let state = run_command(inspect, "inspect local router state").await?;
    let state = String::from_utf8(state.stdout).context("router state is not UTF-8")?;
    let (status, exit) = state.trim().split_once('|').context("invalid router container state")?;
    let exit: i32 = exit.parse().context("invalid router container exit code")?;
    if matches!(status, "exited" | "dead" | "restarting") {
        let read = async {
            let mut logs = Command::new("docker");
            logs.envs(&platform.environment);
            logs.args(["logs", "--tail", "20", "--timestamps", id]);
            logs.stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
            let mut child = logs.spawn().context("start local router log read")?;
            let stdout = child.stdout.take().context("router log stdout missing")?;
            let stderr = child.stderr.take().context("router log stderr missing")?;
            let (out, err, status) = tokio::try_join!(bounded_router_log(stdout), bounded_router_log(stderr), async {
                child.wait().await.context("wait for local router logs")
            })?;
            ensure!(
                status.success(),
                "docker logs failed with {status}: {}",
                String::from_utf8_lossy(&err)
            );
            Ok::<_, anyhow::Error>(format!(
                "{}{}",
                String::from_utf8_lossy(&out),
                String::from_utf8_lossy(&err)
            ))
        };
        let log = match tokio::time::timeout(Duration::from_secs(3), read).await {
            Ok(Ok(log)) => log,
            Ok(Err(error)) => format!("unavailable: {error:#}"),
            Err(_) => "unavailable: router log read timed out".to_owned(),
        };
        return Ok(Some(format!(
            "local router container state={status} exit_code={exit}; bounded log tail: {log}"
        )));
    }
    ensure!(
        matches!(status, "running" | "created"),
        "unexpected local router container state={status} exit_code={exit}"
    );
    Ok(None)
}

async fn bounded_router_log(mut reader: impl tokio::io::AsyncRead + Unpin) -> Result<Vec<u8>> {
    let mut tail = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        let n = reader.read(&mut chunk).await.context("read local router logs")?;
        if n == 0 {
            return Ok(tail);
        }
        tail.extend_from_slice(&chunk[..n]);
        if tail.len() > 8192 {
            tail.drain(..tail.len() - 8192);
        }
    }
}

async fn wait_for_local_router<F, Fut>(endpoint: &str, deadline: Duration, mut inspect: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<String>>>,
{
    let mut ready_url = Url::parse(endpoint)?;
    ready_url.set_path("/readyz");
    ready_url.set_query(None);
    let client = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(1))
        .build()?;
    let until = tokio::time::Instant::now() + deadline;
    let mut backoff = Duration::from_millis(100);
    let explanation = format!(
        "local router endpoint {endpoint} did not return HTTP 200 from /readyz; configure platform.local.bind_host/endpoint_host to the Docker bridge gateway IP, not a loopback fallback"
    );
    loop {
        match tokio::time::timeout_at(until, inspect()).await {
            Ok(Ok(Some(failure))) => bail!("{failure}"),
            Ok(Ok(None)) => {}
            Ok(Err(error)) => return Err(error.context("inspect router while waiting for endpoint")),
            Err(_) => bail!("{explanation}"),
        }
        let attempt_deadline = (tokio::time::Instant::now() + Duration::from_secs(1)).min(until);
        if let Ok(Ok(response)) = tokio::time::timeout_at(attempt_deadline, client.get(ready_url.clone()).send()).await
            && response.status() == reqwest::StatusCode::OK
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= until {
            bail!("{explanation}");
        }
        tokio::time::sleep_until((tokio::time::Instant::now() + backoff).min(until)).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod local_router_readiness_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn delayed_router_bind_succeeds() -> Result<()> {
        let reservation = tokio::net::TcpSocket::new_v4()?;
        reservation.bind("127.0.0.1:0".parse()?)?;
        let port = reservation.local_addr()?.port();
        let server = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            let listener = reservation.listen(16)?;
            for status in ["503 Service Unavailable", "200 OK"] {
                let (mut stream, _) = listener.accept().await?;
                let mut request = [0; 512];
                let mut request_len = 0;
                loop {
                    let count = stream.read(&mut request[request_len..]).await?;
                    if count == 0 {
                        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "empty HTTP request"));
                    }
                    request_len += count;
                    if request[..request_len].windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                    if request_len == request.len() {
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "HTTP request too large"));
                    }
                }
                if !String::from_utf8_lossy(&request[..request_len]).starts_with("GET /readyz ") {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected readiness path"));
                }
                let response = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                stream.write_all(response.as_bytes()).await?;
            }
            Ok::<_, std::io::Error>(())
        });
        wait_for_local_router(
            &format!("http://127.0.0.1:{port}/metrics"),
            Duration::from_secs(2),
            || async { Ok(None) },
        )
        .await?;
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn absent_router_times_out_without_loopback_fallback() -> Result<()> {
        let reservation = tokio::net::TcpSocket::new_v4()?;
        reservation.bind("127.0.0.1:0".parse()?)?;
        let port = reservation.local_addr()?.port();
        let error = wait_for_local_router(
            &format!("http://127.0.0.1:{port}/metrics"),
            Duration::from_millis(80),
            || async { Ok(None) },
        )
        .await
        .err()
        .context("unexpectedly connected")?;
        assert!(error.to_string().contains("not a loopback fallback"));
        Ok(())
    }

    #[tokio::test]
    async fn exited_router_fails_without_waiting_for_timeout() -> Result<()> {
        let reservation = tokio::net::TcpSocket::new_v4()?;
        reservation.bind("127.0.0.1:0".parse()?)?;
        let port = reservation.local_addr()?.port();
        let inspections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let inspected = std::sync::Arc::clone(&inspections);
        let error = wait_for_local_router(
            &format!("http://127.0.0.1:{port}/metrics"),
            Duration::from_secs(5),
            || {
                inspected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async {
                    Ok(Some(
                        "local router container state=exited exit_code=17; bounded log tail: failed".to_owned(),
                    ))
                }
            },
        )
        .await
        .err()
        .context("unexpectedly connected")?;
        assert!(error.to_string().contains("state=exited exit_code=17"));
        assert_eq!(inspections.load(std::sync::atomic::Ordering::Relaxed), 1);
        Ok(())
    }
}
