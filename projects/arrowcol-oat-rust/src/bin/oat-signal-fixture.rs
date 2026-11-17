#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod fixture {
    use std::fs::{self, File};
    use std::io::{Read, Write};
    use std::os::unix::fs::MetadataExt;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    use anyhow::{Context, Result, bail, ensure};
    use nix::sys::signal::{SigSet, Signal};
    use serde_json::json;
    use uuid::Uuid;

    const TERM_CAP: Duration = Duration::from_secs(540);
    const LIFETIME_CAP: Duration = Duration::from_secs(1200);
    const CAP_EXIT: i32 = 20;
    const INTERNAL_EXIT: i32 = 21;
    const FIXTURE_USER: u32 = 10001;

    #[derive(Debug, PartialEq)]
    enum Event {
        TermSeen,
        EarlyReleaseInvalid,
        ReleaseSeen,
        NoChange,
    }

    #[derive(Default)]
    struct Gate {
        first_term: Option<Duration>,
        early_reported: bool,
    }

    impl Gate {
        fn deadline(&self) -> Duration {
            self.first_term
                .map_or(LIFETIME_CAP, |first| (first + TERM_CAP).min(LIFETIME_CAP))
        }

        fn receive(&mut self, signal: Signal, elapsed: Duration) -> Result<Event> {
            ensure!(elapsed < self.deadline(), "signal fixture cap expired");
            match signal {
                Signal::SIGTERM => {
                    if self.first_term.is_none() {
                        self.first_term = Some(elapsed);
                        Ok(Event::TermSeen)
                    } else {
                        Ok(Event::NoChange)
                    }
                }
                Signal::SIGUSR1 if self.first_term.is_some() => Ok(Event::ReleaseSeen),
                Signal::SIGUSR1 if !self.early_reported => {
                    self.early_reported = true;
                    Ok(Event::EarlyReleaseInvalid)
                }
                Signal::SIGUSR1 => Ok(Event::NoChange),
                _ => bail!("SIGINT is not a successful release"),
            }
        }
    }

    fn canonical_uid(value: &str) -> Result<&str> {
        let parsed = Uuid::parse_str(value).context("invalid pod UID")?;
        ensure!(
            parsed.to_string() == value && !parsed.is_nil(),
            "pod UID must be canonical and nonnil"
        );
        Ok(value)
    }

    fn pod_uid() -> Result<String> {
        let value = std::env::var("OAT_FIXTURE_POD_UID").context("missing Downward-API pod UID")?;
        canonical_uid(&value)?;
        Ok(value)
    }

    fn read_bounded(path: &str, limit: u64) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
        ensure!(bytes.len() as u64 <= limit, "oversized process identity");
        Ok(bytes)
    }

    fn require_user(status: &[u8]) -> Result<()> {
        let status = std::str::from_utf8(status)?;
        for field in ["Uid:", "Gid:"] {
            let line = status
                .lines()
                .find(|line| line.starts_with(field))
                .context("missing process user identity")?;
            let values = line
                .split_whitespace()
                .skip(1)
                .map(str::parse::<u32>)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            ensure!(
                values == [FIXTURE_USER; 4],
                "fixture must run exclusively as UID/GID10001"
            );
        }
        Ok(())
    }

    fn validate_target(command: &[u8], environment: &[u8], expected: &str) -> Result<()> {
        let command = command.strip_suffix(&[0]).context("unterminated PID1 command")?;
        let args = command.split(|byte| *byte == 0).collect::<Vec<_>>();
        ensure!(
            args == [b"/oat-signal-fixture".as_slice(), b"wait".as_slice()],
            "PID1 is not the exact wait command"
        );
        let values = environment
            .split(|byte| *byte == 0)
            .filter_map(|entry| entry.strip_prefix(b"OAT_FIXTURE_POD_UID="))
            .collect::<Vec<_>>();
        ensure!(
            values == [expected.as_bytes()],
            "PID1 Downward-API pod UID does not match"
        );
        Ok(())
    }

    fn emit(event: &str, uid: &str, elapsed: Duration) -> Result<()> {
        let mut out = std::io::stdout().lock();
        writeln!(
            out,
            "{}",
            json!({"version":1,"event":event,"pod_uid":uid,"elapsed_ms":elapsed.as_millis()})
        )?;
        out.flush()?;
        Ok(())
    }

    fn wait(started: Instant) -> Result<()> {
        ensure!(
            std::process::id() == 1,
            "wait must be PID1 in an owned fixture PID namespace"
        );
        let uid = pod_uid()?;
        require_user(&read_bounded("/proc/self/status", 8192)?)?;
        let mut signals = SigSet::empty();
        signals.add(Signal::SIGINT);
        signals.add(Signal::SIGUSR1);
        signals.add(Signal::SIGTERM);
        // Block before creating any thread. This fixture supports Linux x86_64
        // only; pending USR1 precedes TERM there, not portably across Unix targets.
        signals.thread_block()?;
        let runtime = tokio::runtime::Builder::new_current_thread().enable_io().build()?;
        let _entered = runtime.enter();
        // PID1 needs installed dispositions. Sigwait, not these unpolled streams,
        // consumes the blocked signals; no async stream ordering is assumed.
        let _term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let _release = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;
        let _interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let state = Arc::new((Mutex::new(Gate::default()), Condvar::new()));
        let watchdog = Arc::clone(&state);
        std::thread::Builder::new().name("fixture-cap".into()).spawn(move || {
            let (gate, changed) = &*watchdog;
            let mut gate = match gate.lock() {
                Ok(gate) => gate,
                Err(_) => std::process::exit(INTERNAL_EXIT),
            };
            loop {
                let Some(remaining) = gate
                    .deadline()
                    .checked_sub(started.elapsed())
                    .filter(|time| !time.is_zero())
                else {
                    std::process::exit(CAP_EXIT);
                };
                gate = match changed.wait_timeout(gate, remaining) {
                    Ok((gate, _)) => gate,
                    Err(_) => std::process::exit(INTERNAL_EXIT),
                };
            }
        })?;
        emit("ready", &uid, started.elapsed())?;
        loop {
            let signal = signals.wait()?;
            let (event, elapsed) = {
                let mut gate = state.0.lock().map_err(|_| anyhow::anyhow!("fixture state poisoned"))?;
                let elapsed = started.elapsed();
                (gate.receive(signal, elapsed)?, elapsed)
            };
            match event {
                Event::TermSeen => {
                    state.1.notify_one();
                    emit("term_seen", &uid, elapsed)?;
                }
                Event::EarlyReleaseInvalid => emit("early_release_invalid", &uid, elapsed)?,
                Event::ReleaseSeen => {
                    emit("release_seen", &uid, elapsed)?;
                    let gate = state.0.lock().map_err(|_| anyhow::anyhow!("fixture state poisoned"))?;
                    ensure!(started.elapsed() < gate.deadline(), "signal fixture cap expired");
                    return Ok(());
                }
                Event::NoChange => {}
            }
        }
    }

    fn release(expected: &str) -> Result<()> {
        canonical_uid(expected)?;
        ensure!(std::process::id() != 1, "release cannot be PID1");
        ensure!(pod_uid()? == expected, "release Downward-API pod UID does not match");
        require_user(&read_bounded("/proc/self/status", 8192)?)?;
        let own = fs::metadata("/proc/self/exe")?;
        let target = fs::metadata("/proc/1/exe")?;
        ensure!(
            own.dev() == target.dev() && own.ino() == target.ino(),
            "PID1 is not this fixture binary"
        );
        require_user(&read_bounded("/proc/1/status", 8192)?)?;
        validate_target(
            &read_bounded("/proc/1/cmdline", 4096)?,
            &read_bounded("/proc/1/environ", 65536)?,
            expected,
        )?;
        // PID1 cannot be reused while another process survives in its PID namespace.
        rustix::process::kill_process(rustix::process::Pid::INIT, rustix::process::Signal::USR1)?;
        Ok(())
    }

    pub fn run() -> Result<()> {
        let started = Instant::now();
        let args = std::env::args_os().skip(1).collect::<Vec<_>>();
        match args.as_slice() {
            [mode] if mode == "wait" => wait(started),
            [mode, uid] if mode == "release" => release(uid.to_str().context("non-UTF8 pod UID")?),
            _ => bail!("expected exactly: wait | release <expected-pod-uid>"),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        const UID: &str = "00000000-0000-4000-8000-000000000001";

        #[test]
        fn early_release_is_not_latched_and_repeated_term_cannot_extend_cap() -> Result<()> {
            let mut gate = Gate::default();
            assert_eq!(
                gate.receive(Signal::SIGUSR1, Duration::from_secs(1))?,
                Event::EarlyReleaseInvalid
            );
            assert_eq!(gate.receive(Signal::SIGTERM, Duration::from_secs(10))?, Event::TermSeen);
            assert_eq!(
                gate.receive(Signal::SIGTERM, Duration::from_secs(549))?,
                Event::NoChange
            );
            assert!(gate.receive(Signal::SIGUSR1, Duration::from_secs(550)).is_err());
            assert!(gate.receive(Signal::SIGINT, Duration::from_secs(20)).is_err());
            let mut gate = Gate::default();
            gate.receive(Signal::SIGTERM, Duration::from_secs(1190))?;
            assert!(gate.receive(Signal::SIGUSR1, Duration::from_secs(1200)).is_err());
            Ok(())
        }

        #[test]
        fn release_requires_post_term_and_exact_process_identity() -> Result<()> {
            let mut gate = Gate::default();
            gate.receive(Signal::SIGUSR1, Duration::from_secs(1))?;
            gate.receive(Signal::SIGTERM, Duration::from_secs(2))?;
            assert_eq!(
                gate.receive(Signal::SIGUSR1, Duration::from_secs(3))?,
                Event::ReleaseSeen
            );
            let command = b"/oat-signal-fixture\0wait\0";
            let environment = format!("OAT_FIXTURE_POD_UID={UID}\0");
            validate_target(command, environment.as_bytes(), UID)?;
            assert!(validate_target(b"/oat-signal-fixture\0release\0", environment.as_bytes(), UID).is_err());
            assert!(validate_target(command, environment.as_bytes(), "00000000-0000-4000-8000-000000000002").is_err());
            assert!(validate_target(command, format!("{environment}{environment}").as_bytes(), UID).is_err());
            assert!(canonical_uid("00000000-0000-0000-0000-000000000000").is_err());
            assert!(require_user(b"Uid:\t0\t0\t0\t0\nGid:\t10001\t10001\t10001\t10001\n").is_err());
            Ok(())
        }
    }
}

fn main() -> std::process::ExitCode {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    match fixture::run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("signal fixture refused: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    {
        eprintln!("signal fixture requires Linux x86_64 and an owned PID namespace");
        std::process::ExitCode::FAILURE
    }
}
