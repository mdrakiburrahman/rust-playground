//! Inert bootstrap for the bounded identity reader; scanning is not enabled.

#[cfg(target_os = "linux")]
mod linux {
    use anyhow::{Context, Result, bail, ensure};
    use rustix::process::{
        Resource, Rlimit, Signal, getppid, getrlimit, parent_process_death_signal, set_parent_process_death_signal,
        setrlimit,
    };

    #[derive(Debug)]
    pub struct Limits {
        pub parent: i32,
        pub address_bytes: u64,
        pub cpu_seconds: u64,
        pub descriptors: u64,
    }

    impl Limits {
        pub fn parse(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<Self> {
            let mut args = args.into_iter();
            let mut next = || -> Result<String> {
                let value = args.next().context("missing worker limit")?;
                ensure!(value.as_encoded_bytes().len() <= 20, "worker limit is too long");
                value.into_string().map_err(|_| anyhow::anyhow!("worker limit must be UTF-8"))
            };
            let limits = Self {
                parent: next()?.parse().context("invalid expected parent")?,
                address_bytes: next()?.parse().context("invalid address-space limit")?,
                cpu_seconds: next()?.parse().context("invalid CPU limit")?,
                descriptors: next()?.parse().context("invalid descriptor limit")?,
            };
            ensure!(args.next().is_none(), "unexpected worker limit argument");
            ensure!(
                limits.parent > 1
                    && (1..u64::MAX).contains(&limits.address_bytes)
                    && (1..u64::MAX).contains(&limits.cpu_seconds)
                    && (3..u64::MAX).contains(&limits.descriptors),
                "worker limits must be finite and positive"
            );
            Ok(limits)
        }

        pub fn install(&self) -> Result<()> {
            let parent = || getppid().map(|pid| pid.as_raw_pid());
            ensure!(parent() == Some(self.parent), "worker parent changed before setup");
            set_parent_process_death_signal(Some(Signal::KILL))?;
            ensure!(
                parent_process_death_signal()? == Some(Signal::KILL) && parent() == Some(self.parent),
                "worker parent-death control is unavailable or parent changed"
            );
            for (resource, maximum) in [
                (Resource::Core, 0),
                (Resource::As, self.address_bytes),
                (Resource::Cpu, self.cpu_seconds),
                (Resource::Nofile, self.descriptors),
            ] {
                let expected = Rlimit {
                    current: Some(maximum),
                    maximum: Some(maximum),
                };
                setrlimit(resource, expected).context("install worker resource limit")?;
                ensure!(getrlimit(resource) == expected, "worker resource limit readback differs");
            }
            ensure!(parent() == Some(self.parent), "worker parent changed during setup");
            Ok(())
        }
    }

    pub fn run() -> Result<()> {
        let limits = Limits::parse(std::env::args_os().skip(1))?;
        limits.install()?;
        bail!("identity decoding is disabled until the worker protocol and supervisor are complete")
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::process::Command;

        #[test]
        fn limits_reject_unbounded_or_malformed_input() {
            for values in [
                ["1", "1024", "1", "3"],
                ["2", "0", "1", "3"],
                ["2", "18446744073709551615", "1", "3"],
                ["2", "1024", "0", "3"],
                ["2", "1024", "1", "2"],
                ["2", "bad", "1", "3"],
            ] {
                assert!(Limits::parse(values.map(std::ffi::OsString::from)).is_err());
            }
            assert!(Limits::parse(["2", "999999999999999999999", "1", "3"].map(std::ffi::OsString::from)).is_err());
            assert!(Limits::parse(["2", "1024", "1", "3", "extra"].map(std::ffi::OsString::from)).is_err());
            use std::os::unix::ffi::OsStringExt;
            let invalid = std::ffi::OsString::from_vec(vec![0xff]);
            assert!(Limits::parse([invalid, "1024".into(), "1".into(), "3".into()]).is_err());
        }

        #[test]
        fn changed_parent_fails_before_resource_changes() -> Result<()> {
            let parent = getppid().context("missing parent")?.as_raw_pid();
            let limits = Limits {
                parent: parent.checked_add(1).context("parent PID overflow")?,
                address_bytes: 1024 * 1024 * 1024,
                cpu_seconds: 30,
                descriptors: 64,
            };
            let core = getrlimit(Resource::Core);
            assert!(limits.install().is_err());
            assert_eq!(getrlimit(Resource::Core), core);
            Ok(())
        }

        #[test]
        fn limits_install_and_read_back_in_owned_child() -> Result<()> {
            const CHILD: &str = "OAT_LIMIT_TEST_CHILD";
            if std::env::var_os(CHILD).is_some() {
                let parent = getppid().context("missing parent")?.as_raw_pid();
                let limits = Limits::parse([
                    parent.to_string(),
                    (1024_u64 * 1024 * 1024).to_string(),
                    "30".to_owned(),
                    "64".to_owned(),
                ].map(std::ffi::OsString::from))?;
                limits.install()?;
                return Ok(());
            }
            let status = Command::new(std::env::current_exe()?)
                .args(["--exact", "linux::tests::limits_install_and_read_back_in_owned_child"])
                .env(CHILD, "1")
                .status()?;
            ensure!(status.success(), "owned limit-test child failed");
            Ok(())
        }
    }
}

fn main() -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    {
        linux::run()
    }
    #[cfg(not(target_os = "linux"))]
    {
        anyhow::bail!("bounded identity worker requires Linux")
    }
}
