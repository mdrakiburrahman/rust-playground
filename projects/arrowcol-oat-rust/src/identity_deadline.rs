//! Suspend-inclusive deadline for one identity worker attempt.

use std::io::{self, ErrorKind};
use std::time::Duration;

use rustix::fd::OwnedFd;
use rustix::time::{
    DynamicClockId, Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, Timespec, clock_gettime_dynamic,
    timerfd_create, timerfd_settime,
};
use tokio::io::unix::AsyncFd;

pub struct IdentityDeadline {
    end: Timespec,
    timer: AsyncFd<OwnedFd>,
}

impl IdentityDeadline {
    /// Create and arm one absolute CLOCK_BOOTTIME deadline inside a Tokio runtime.
    ///
    /// # Errors
    /// Rejects zero or overflowing budgets and unavailable clock or timer controls.
    pub fn new(budget: Duration) -> io::Result<Self> {
        if budget.is_zero() {
            return Err(io::Error::new(ErrorKind::InvalidInput, "identity deadline must be positive"));
        }
        let duration = Timespec::try_from(budget)
            .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "identity deadline budget is too large"))?;
        let end = now()?
            .checked_add(duration)
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "identity deadline overflow"))?;
        let fd = timerfd_create(
            TimerfdClockId::Boottime,
            TimerfdFlags::CLOEXEC | TimerfdFlags::NONBLOCK,
        )?;
        timerfd_settime(
            &fd,
            TimerfdTimerFlags::ABSTIME,
            &Itimerspec {
                it_interval: Timespec::default(),
                it_value: end,
            },
        )?;
        Ok(Self {
            end,
            timer: AsyncFd::new(fd)?,
        })
    }

    /// Refuse a result after the original deadline, including suspend time.
    ///
    /// # Errors
    /// Returns timeout or unavailable clock errors.
    pub fn check(&self) -> io::Result<()> {
        if now()? >= self.end {
            return Err(io::Error::new(ErrorKind::TimedOut, "identity deadline expired"));
        }
        Ok(())
    }

    /// Wait for the same absolute deadline; this never extends it.
    ///
    /// # Errors
    /// Returns timer, clock or readiness failures.
    pub async fn wait(&self) -> io::Result<()> {
        match self.check() {
            Err(error) if error.kind() == ErrorKind::TimedOut => return Ok(()),
            Err(error) => return Err(error),
            Ok(()) => {}
        }
        loop {
            let mut ready = self.timer.readable().await?;
            let mut expirations = [0_u8; 8];
            match ready.try_io(|inner| rustix::io::read(inner.get_ref(), &mut expirations).map_err(io::Error::from)) {
                Ok(result) => {
                    if result? != expirations.len() {
                        return Err(io::Error::new(ErrorKind::UnexpectedEof, "short identity timer read"));
                    }
                    if u64::from_ne_bytes(expirations) == 0 {
                        return Err(io::Error::other("identity timer reported no expirations"));
                    }
                    return match self.check() {
                        Err(error) if error.kind() == ErrorKind::TimedOut => Ok(()),
                        Err(error) => Err(error),
                        Ok(()) => Err(io::Error::other("identity timer fired before its deadline")),
                    };
                }
                Err(_) => continue,
            }
        }
    }
}

fn now() -> io::Result<Timespec> {
    clock_gettime_dynamic(DynamicClockId::Boottime).map_err(io::Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timer_expires_without_extending_its_deadline() -> io::Result<()> {
        let deadline = IdentityDeadline::new(Duration::from_millis(10))?;
        deadline.wait().await?;
        deadline.wait().await?;
        assert_eq!(deadline.check().expect_err("deadline must expire").kind(), ErrorKind::TimedOut);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_deadline_budgets_are_refused() {
        assert!(IdentityDeadline::new(Duration::ZERO).is_err());
        assert!(IdentityDeadline::new(Duration::MAX).is_err());
    }
}
