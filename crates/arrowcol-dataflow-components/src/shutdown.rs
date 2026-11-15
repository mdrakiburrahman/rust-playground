use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use anyhow::{Context, Result, bail};
use arrowcol_protocol::{CollectorInstanceId, PublicationId};
use arrowcol_routing::{CollectorState, RoutingStore};

use crate::config::now_unix_millis;

pub(crate) static PROCESS_ROUTING_SHUTDOWN: LazyLock<RoutingShutdown> = LazyLock::new(RoutingShutdown::default);

/// Routing identities retained until the embedding process finishes all publication.
#[derive(Default)]
pub struct RoutingShutdown {
    registrations: Mutex<Vec<(Arc<dyn RoutingStore>, CollectorInstanceId)>>,
    failed: AtomicBool,
    pending: Mutex<HashMap<PublicationId, PublicationState>>,
    workers: Mutex<HashMap<PathBuf, Arc<AtomicBool>>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PublicationState {
    NeverStaged,
    Staged,
    Nacked,
}

#[derive(Default, Debug, PartialEq, Eq)]
struct PendingCounts {
    never_staged: usize,
    staged: usize,
    nacked: usize,
}

impl RoutingShutdown {
    /// Retains a successfully registered receiver's store and process identity.
    ///
    /// # Errors
    /// Returns an error if the process identity registry is poisoned.
    pub fn register(&self, store: Arc<dyn RoutingStore>, instance_id: CollectorInstanceId) -> Result<()> {
        self.registrations
            .lock()
            .map_err(|_| anyhow::anyhow!("routing shutdown registry poisoned"))?
            .push((store, instance_id));
        Ok(())
    }

    /// Records a publication or drain failure that a clean engine exit must not hide.
    pub fn record_failure(&self) {
        self.failed.store(true, Ordering::Release);
    }

    /// Tracks an admitted or NACKed publication until that exact identity publishes successfully.
    ///
    /// # Errors
    /// Reports poisoned publication accounting rather than assuming the batch was published.
    pub fn publication_finished(&self, publication_id: PublicationId, published: bool) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?;
        if published {
            pending.remove(&publication_id);
        } else {
            pending.insert(publication_id, PublicationState::Nacked);
        }
        Ok(())
    }

    pub(crate) fn publication_staged(&self, publication_id: PublicationId) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?;
        let state = pending.entry(publication_id).or_insert(PublicationState::Staged);
        if *state != PublicationState::Nacked {
            *state = PublicationState::Staged;
        }
        Ok(())
    }

    /// Tracks a fresh identity across its downstream send, forgetting it only when the send returned it.
    ///
    /// The identity is tracked before sending so a fast publication outcome cannot precede it. An
    /// explicit send error hands the message back, so that identity never reached the exporter. A
    /// cancelled send keeps the identity: a dropped flume send can already have been delivered.
    ///
    /// # Errors
    /// Reports poisoned publication accounting; an unforgettable identity stays tracked.
    pub(crate) async fn admit_publication<E>(
        &self,
        publication_id: PublicationId,
        send: impl Future<Output = Result<(), E>>,
    ) -> Result<Result<(), E>> {
        let created_entry = {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?;
            match pending.entry(publication_id) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(PublicationState::NeverStaged);
                    true
                }
                std::collections::hash_map::Entry::Occupied(_) => false,
            }
        };
        let result = send.await;
        if created_entry && result.is_err() {
            self.pending
                .lock()
                .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?
                .remove(&publication_id);
        }
        Ok(result)
    }

    pub(crate) fn track_worker(&self, root: PathBuf, retired: Arc<AtomicBool>) -> Result<()> {
        self.workers
            .lock()
            .map_err(|_| anyhow::anyhow!("worker accounting poisoned"))?
            .insert(root, retired);
        Ok(())
    }

    /// Persists terminal state after the controller and publisher-close hook have finished.
    ///
    /// # Errors
    /// Reports incomplete publication/drain or any terminal write failure, attempting every identity.
    pub async fn finish(&self, clean: bool) -> Result<()> {
        let registrations = self
            .registrations
            .lock()
            .map_err(|_| anyhow::anyhow!("routing shutdown registry poisoned"))?
            .clone();
        let (pending_total, counts) = {
            let pending = self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?;
            let mut counts = PendingCounts::default();
            for state in pending.values() {
                match state {
                    PublicationState::NeverStaged => counts.never_staged += 1,
                    PublicationState::Staged => counts.staged += 1,
                    PublicationState::Nacked => counts.nacked += 1,
                }
            }
            (pending.len(), counts)
        };
        let retired = self
            .workers
            .lock()
            .map_err(|_| anyhow::anyhow!("worker accounting poisoned"))?
            .values()
            .filter(|worker| worker.load(Ordering::Acquire))
            .count();
        let failed = self.failed.load(Ordering::Acquire) || pending_total != 0 || retired != 0;
        let state = terminal_state(clean, failed);
        let now = now_unix_millis().map_err(anyhow::Error::msg)?;
        let mut errors = Vec::new();
        if failed {
            tracing::error!(
                never_staged = counts.never_staged,
                staged_unpublished = counts.staged,
                nacked = counts.nacked,
                "dataflow shutdown publication accounting"
            );
            errors.push(format!("dataflow drain incomplete: {pending_total} unpublished identities (never_staged={}, staged_unpublished={}, nacked={}), {retired} retired workers, ingress failure={}", counts.never_staged, counts.staged, counts.nacked, self.failed.load(Ordering::Acquire)));
        }
        for (store, instance_id) in registrations {
            if let Err(error) = store.set_collector_state(instance_id, state, now).await {
                errors.push(format!("terminal routing state for {instance_id}: {error}"));
            }
        }
        if !errors.is_empty() {
            bail!("{}", errors.join("; "));
        }
        Ok(())
    }
}

fn terminal_state(clean: bool, failed: bool) -> CollectorState {
    if clean && !failed {
        CollectorState::Stopped
    } else {
        CollectorState::Failed
    }
}

/// Finalizes this process's receivers from the post-controller, post-publisher main-thread hook.
///
/// # Errors
/// Reports runtime creation, incomplete drain/publication, or terminal routing-write failures.
pub fn finish_routing_shutdown(clean: bool) -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        bail!("terminal routing shutdown must run after the controller runtime is dropped");
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create terminal routing shutdown runtime")?
        .block_on(PROCESS_ROUTING_SHUTDOWN.finish(clean))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_clean_publication_and_drain_can_stop() {
        assert_eq!(terminal_state(true, false), CollectorState::Stopped);
        for (clean, failed) in [(false, false), (true, true), (false, true)] {
            assert_eq!(terminal_state(clean, failed), CollectorState::Failed);
        }
    }

    #[tokio::test]
    async fn validation_without_registered_receivers_needs_no_terminal_row() -> Result<()> {
        RoutingShutdown::default().finish(true).await
    }

    #[tokio::test]
    async fn retired_worker_must_be_replaced_before_clean_shutdown() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        let root = PathBuf::from("table");
        shutdown.track_worker(root.clone(), Arc::new(AtomicBool::new(true)))?;
        assert!(shutdown.finish(true).await.is_err());
        shutdown.track_worker(root, Arc::new(AtomicBool::new(false)))?;
        shutdown.finish(true).await
    }

    #[tokio::test]
    async fn returned_send_is_not_unpublished_work() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        for error in ["route closed", "route full"] {
            let sent = shutdown
                .admit_publication(PublicationId::new(), async { Err(error) })
                .await?;
            assert_eq!(sent, Err(error));
        }
        shutdown.finish(true).await
    }

    #[tokio::test]
    async fn returned_retry_preserves_preexisting_nacked_identity() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        let publication = PublicationId::new();
        shutdown.publication_finished(publication, false)?;

        let sent = shutdown
            .admit_publication(publication, async { Err("route full") })
            .await?;
        assert_eq!(sent, Err("route full"));
        let error = shutdown
            .finish(true)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("the preexisting NACK must remain unpublished"))?
            .to_string();
        assert!(
            error.contains("1 unpublished identities (never_staged=0, staged_unpublished=0, nacked=1)"),
            "{error}"
        );

        shutdown.publication_finished(publication, true)?;
        shutdown.finish(true).await
    }

    #[tokio::test]
    async fn cancelled_send_after_delivery_and_nack_stays_unpublished() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        let publication = PublicationId::new();
        let delivered_then_nacked = async {
            shutdown.publication_staged(publication)?;
            shutdown.publication_finished(publication, false)?;
            std::future::pending::<Result<()>>().await
        };
        let cancelled = shutdown.admit_publication(publication, delivered_then_nacked);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), cancelled)
                .await
                .is_err()
        );
        assert!(shutdown.finish(true).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn pending_categories_reflect_exporter_progress_and_nacks() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        let never_staged = PublicationId::new();
        let staged = PublicationId::new();
        let nacked = PublicationId::new();
        for id in [never_staged, staged, nacked] {
            shutdown
                .admit_publication(id, async { Ok::<_, ()>(()) })
                .await?
                .map_err(|()| anyhow::anyhow!("send failed"))?;
        }
        shutdown.publication_staged(staged)?;
        shutdown.publication_staged(nacked)?;
        shutdown.publication_finished(nacked, false)?;
        shutdown.publication_staged(nacked)?;
        let error = shutdown
            .finish(true)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("unpublished work must fail"))?
            .to_string();
        assert!(
            error.contains("3 unpublished identities (never_staged=1, staged_unpublished=1, nacked=1)"),
            "{error}"
        );
        for id in [never_staged, staged, nacked] {
            shutdown.publication_finished(id, true)?;
        }
        shutdown.finish(true).await
    }

    #[tokio::test]
    async fn admitted_send_stays_unpublished_until_that_identity_publishes() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        let publication = PublicationId::new();
        shutdown
            .admit_publication(publication, async { Ok::<_, ()>(()) })
            .await?
            .map_err(|()| anyhow::anyhow!("send failed"))?;
        assert!(shutdown.finish(true).await.is_err());
        shutdown.publication_finished(publication, true)?;
        shutdown.finish(true).await
    }

    #[test]
    fn synchronous_finalizer_rejects_live_runtime_without_panicking() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let _entered = runtime.enter();
        assert!(finish_routing_shutdown(true).is_err());
        Ok(())
    }
}
