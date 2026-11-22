use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const LARGE_QUANTUM_BYTES: u64 = 1024;
const MAX_UNITS: u64 = if Semaphore::MAX_PERMITS as u64 >= u32::MAX as u64 {
    u32::MAX as u64
} else {
    Semaphore::MAX_PERMITS as u64
};
// MAX_UNITS is at most u32::MAX, so this product fits u64.
const MAX_LIMIT_BYTES: u64 = MAX_UNITS * LARGE_QUANTUM_BYTES;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ByteCapacityError {
    #[error("byte capacity must be positive")]
    ZeroLimit,
    #[error("byte capacity {limit_bytes} exceeds the representable limit of {max_bytes} bytes")]
    UnsupportedLimit { limit_bytes: u64, max_bytes: u64 },
    #[error("byte request {requested_bytes} exceeds the effective capacity of {effective_limit_bytes} bytes")]
    Oversized {
        requested_bytes: u64,
        effective_limit_bytes: u64,
    },
    #[error("byte capacity semaphore is closed")]
    Closed,
}

/// Weighted FIFO byte admission backed by one Tokio semaphore. Permits account
/// for bytes but do not allocate them. Cancelling an acquire returns any
/// reservation Tokio made for it; dropping the owned permit returns its charge.
#[derive(Debug)]
pub(crate) struct ByteCapacity {
    semaphore: Arc<Semaphore>,
    effective_limit_bytes: u64,
    quantum_bytes: u64,
}

/// Owned (and `Send`) charge; dropping it returns capacity to the same budget.
#[derive(Debug)]
#[must_use = "dropping the permit releases its byte charge"]
pub(crate) struct BytePermit {
    _permit: OwnedSemaphorePermit,
}

impl ByteCapacity {
    /// Use one permit per byte where representable. For larger budgets, floor
    /// the configured limit to a KiB boundary and round each request up to a KiB.
    ///
    /// # Errors
    /// Rejects zero or a limit exceeding the platform's representable capacity.
    pub(crate) fn new(limit_bytes: u64) -> Result<Self, ByteCapacityError> {
        if limit_bytes == 0 {
            return Err(ByteCapacityError::ZeroLimit);
        }
        let unsupported = || ByteCapacityError::UnsupportedLimit {
            limit_bytes,
            max_bytes: MAX_LIMIT_BYTES,
        };
        if limit_bytes > MAX_LIMIT_BYTES {
            return Err(unsupported());
        }
        let quantum_bytes = if limit_bytes <= MAX_UNITS {
            1
        } else {
            LARGE_QUANTUM_BYTES
        };
        let units = limit_bytes / quantum_bytes;
        let permits = usize::try_from(units).map_err(|_| unsupported())?;
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            effective_limit_bytes: limit_bytes - limit_bytes % quantum_bytes,
            quantum_bytes,
        })
    }

    /// Largest single request that can fit; may be smaller than the configured limit.
    pub(crate) const fn effective_limit_bytes(&self) -> u64 {
        self.effective_limit_bytes
    }

    /// Number of bytes charged per semaphore permit.
    pub(crate) const fn quantum_bytes(&self) -> u64 {
        self.quantum_bytes
    }

    #[cfg(test)]
    pub(crate) fn available_bytes(&self) -> u64 {
        self.semaphore.available_permits() as u64 * self.quantum_bytes
    }

    /// Request the full rounded charge in one acquisition. Positive requests
    /// cannot bypass earlier queued requests; a zero-byte request charges zero.
    ///
    /// # Errors
    /// Rejects requests above the effective limit before waiting, or a closed semaphore.
    pub(crate) async fn acquire(&self, bytes: u64) -> Result<BytePermit, ByteCapacityError> {
        let oversize = || ByteCapacityError::Oversized {
            requested_bytes: bytes,
            effective_limit_bytes: self.effective_limit_bytes(),
        };
        if bytes > self.effective_limit_bytes() {
            return Err(oversize());
        }
        let units = u32::try_from(bytes.div_ceil(self.quantum_bytes())).map_err(|_| oversize())?;
        let permit = Arc::clone(&self.semaphore)
            .acquire_many_owned(units)
            .await
            .map_err(|_| ByteCapacityError::Closed)?;
        Ok(BytePermit { _permit: permit })
    }
}

#[cfg(test)]
mod tests {
    use std::{pin::pin, task::Poll};

    use futures::poll;

    use super::*;

    const GIB: u64 = 1 << 30;

    #[tokio::test]
    async fn large_requests_wait_in_fifo_order_and_owned_drop_releases_capacity() -> anyhow::Result<()> {
        for limit in [32 * GIB, 64 * GIB, 128 * GIB] {
            let budget = ByteCapacity::new(limit)?;
            let held = budget.acquire(limit - 4 * GIB).await?;
            let mut large = pin!(budget.acquire(limit));
            let mut small = pin!(budget.acquire(1));
            assert!(poll!(&mut large).is_pending());
            assert!(poll!(&mut small).is_pending());

            // Moving the permit across tasks also verifies it is Send and owned.
            tokio::spawn(async move { drop(held) }).await?;
            assert!(poll!(&mut small).is_pending());
            let Poll::Ready(full) = poll!(&mut large) else {
                anyhow::bail!("the oldest request did not receive the released budget");
            };
            let full = full?;
            assert!(poll!(&mut small).is_pending());
            drop(full);
            assert!(matches!(poll!(&mut small), Poll::Ready(Ok(_))));
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_waiters_return_partial_and_fully_assigned_reservations() -> anyhow::Result<()> {
        let limit = 32 * GIB;
        let budget = ByteCapacity::new(limit)?;
        for release_before_cancel in [false, true] {
            let mut held = Some(budget.acquire(8 * GIB).await?);
            {
                let mut cancelled = pin!(budget.acquire(limit));
                assert!(poll!(&mut cancelled).is_pending());
                if release_before_cancel {
                    drop(held.take());
                }
            }
            let mut remaining = pin!(budget.acquire(if release_before_cancel { limit } else { 24 * GIB }));
            assert!(matches!(poll!(&mut remaining), Poll::Ready(Ok(_))));
            drop(held);
        }
        Ok(())
    }

    #[tokio::test]
    async fn small_budgets_are_exact_and_invalid_inputs_fail_without_waiting() -> anyhow::Result<()> {
        assert_eq!(ByteCapacity::new(0).err(), Some(ByteCapacityError::ZeroLimit));
        for limit_bytes in [MAX_LIMIT_BYTES + 1, u64::MAX] {
            assert_eq!(
                ByteCapacity::new(limit_bytes).err(),
                Some(ByteCapacityError::UnsupportedLimit {
                    limit_bytes,
                    max_bytes: MAX_LIMIT_BYTES,
                }),
            );
        }
        let budget = ByteCapacity::new(3)?;
        let two = budget.acquire(2).await?;
        let one = budget.acquire(1).await?;
        let zero = budget.acquire(0).await?;
        for requested_bytes in [4, u64::MAX] {
            let mut request = pin!(budget.acquire(requested_bytes));
            assert!(matches!(
                poll!(&mut request),
                Poll::Ready(Err(ByteCapacityError::Oversized {
                    requested_bytes: n,
                    effective_limit_bytes: 3,
                })) if n == requested_bytes
            ));
        }
        let mut waiting = pin!(budget.acquire(1));
        assert!(poll!(&mut waiting).is_pending());
        drop(zero);
        assert!(poll!(&mut waiting).is_pending());
        drop(one);
        assert!(matches!(poll!(&mut waiting), Poll::Ready(Ok(_))));
        drop(two);
        Ok(())
    }

    #[tokio::test]
    async fn quantized_budget_floors_limit_and_ceils_each_request() -> anyhow::Result<()> {
        let effective = 32 * GIB;
        let budget = ByteCapacity::new(effective + 1023)?;
        assert_eq!(budget.effective_limit_bytes(), effective);
        assert_eq!(budget.quantum_bytes(), 1024);
        let mut rounded_out = pin!(budget.acquire(effective + 1));
        assert!(matches!(
            poll!(&mut rounded_out),
            Poll::Ready(Err(ByteCapacityError::Oversized { .. }))
        ));

        let almost_full = budget.acquire(effective - 1024).await?;
        let one = budget.acquire(1).await?;
        let mut another = pin!(budget.acquire(1));
        assert!(poll!(&mut another).is_pending());
        drop(one);
        assert!(matches!(poll!(&mut another), Poll::Ready(Ok(_))));
        drop(almost_full);
        Ok(())
    }
}
