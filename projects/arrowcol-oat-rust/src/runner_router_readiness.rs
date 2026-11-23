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
    let url = Url::parse(endpoint)?;
    let host = url
        .host_str()
        .context("local endpoint has no host")?
        .trim_matches(['[', ']']);
    let port = url.port_or_known_default().context("local endpoint has no port")?;
    let until = tokio::time::Instant::now() + deadline;
    let mut backoff = Duration::from_millis(100);
    let explanation = format!(
        "local router endpoint {endpoint} is unreachable; configure platform.local.bind_host/endpoint_host, not a loopback fallback"
    );
    loop {
        match tokio::time::timeout_at(until, inspect()).await {
            Ok(Ok(Some(failure))) => bail!("{failure}"),
            Ok(Ok(None)) => {}
            Ok(Err(error)) => return Err(error.context("inspect router while waiting for endpoint")),
            Err(_) => bail!("{explanation}"),
        }
        let attempt_deadline = (tokio::time::Instant::now() + Duration::from_secs(1)).min(until);
        if let Ok(Ok(_)) = tokio::time::timeout_at(attempt_deadline, tokio::net::TcpStream::connect((host, port))).await
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

    #[tokio::test]
    async fn delayed_router_bind_succeeds() -> Result<()> {
        let reservation = tokio::net::TcpSocket::new_v4()?;
        reservation.bind("127.0.0.1:0".parse()?)?;
        let port = reservation.local_addr()?.port();
        let server = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(75)).await;
            let listener = reservation.listen(16)?;
            let _connection = listener.accept().await?;
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
        let started = tokio::time::Instant::now();
        let error = wait_for_local_router(
            &format!("http://127.0.0.1:{port}/metrics"),
            Duration::from_secs(5),
            || async {
                Ok(Some(
                    "local router container state=exited exit_code=17; bounded log tail: failed".to_owned(),
                ))
            },
        )
        .await
        .err()
        .context("unexpectedly connected")?;
        assert!(error.to_string().contains("state=exited exit_code=17"));
        assert!(started.elapsed() < Duration::from_secs(2));
        Ok(())
    }
}
