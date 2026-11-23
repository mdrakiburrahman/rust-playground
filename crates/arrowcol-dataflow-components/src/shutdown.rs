use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use arrowcol_protocol::{CollectorInstanceId, PublicationId, UnixMillis};
use arrowcol_routing::{CollectorState, RoutingErrorKind, RoutingStore};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::config::{checked_add_millis, now_unix_millis};

pub(crate) static PROCESS_ROUTING_SHUTDOWN: LazyLock<RoutingShutdown> = LazyLock::new(RoutingShutdown::default);

/// Routing identities retained until the embedding process finishes all publication.
#[derive(Default)]
pub struct RoutingShutdown {
    registrations: Mutex<Vec<(Arc<dyn RoutingStore>, CollectorInstanceId)>>,
    drain_heartbeats: Mutex<HashMap<CollectorInstanceId, DropGuard>>,
    failed: AtomicBool,
    next_publication_generation: AtomicU64,
    pending: Mutex<HashMap<PublicationId, PendingPublication>>,
    workers: Mutex<HashMap<PathBuf, Arc<AtomicBool>>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PublicationState {
    NeverStaged,
    Staged,
    Nacked,
}

struct PendingPublication {
    state: PublicationState,
    generation: u64,
    active_admissions: usize,
    owner_failed: bool,
    send_succeeded: bool,
    cancelled: bool,
}

#[derive(Default, Debug, PartialEq, Eq)]
struct PendingCounts {
    never_staged: usize,
    staged: usize,
    nacked: usize,
    cancelled_ambiguous: usize,
}

struct AdmissionGuard<'a> {
    shutdown: &'a RoutingShutdown,
    publication_id: PublicationId,
    generation: u64,
    armed: bool,
}

impl Drop for AdmissionGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(mut pending) = self.shutdown.pending.lock() else {
            self.shutdown.record_failure();
            return;
        };
        if let Some(entry) = pending.get_mut(&self.publication_id)
            && entry.generation == self.generation
        {
            if let Some(active) = entry.active_admissions.checked_sub(1) {
                entry.active_admissions = active;
                entry.cancelled = true;
            } else {
                self.shutdown.record_failure();
            }
        }
    }
}

impl RoutingShutdown {
    /// Retains a successfully registered receiver's store and process identity.
    ///
    /// # Errors
    /// Returns an error if the process identity registry is poisoned.
    pub fn register(&self, store: Arc<dyn RoutingStore>, instance_id: CollectorInstanceId) -> Result<()> {
        let mut registrations = self
            .registrations
            .lock()
            .map_err(|_| anyhow::anyhow!("routing shutdown registry poisoned"))?;
        if !registrations.iter().any(|(_, id)| *id == instance_id) {
            registrations.push((store, instance_id));
        }
        Ok(())
    }

    pub(crate) async fn begin_drain(
        &self,
        store: Arc<dyn RoutingStore>,
        instance_id: CollectorInstanceId,
        interval: Duration,
        ttl: Duration,
    ) -> Result<()> {
        let now = now_unix_millis().map_err(anyhow::Error::msg)?;
        store
            .set_collector_state(instance_id, CollectorState::Draining, now)
            .await?;
        let mut heartbeats = self
            .drain_heartbeats
            .lock()
            .map_err(|_| anyhow::anyhow!("drain heartbeat registry poisoned"))?;
        if let std::collections::hash_map::Entry::Vacant(slot) = heartbeats.entry(instance_id) {
            let cancel = CancellationToken::new();
            // The controller drops its runtime before the publisher close and terminal hook.
            arrow_deltalake::delta::spawn_shared_io(renew_drain_registration(
                store,
                instance_id,
                interval,
                ttl,
                cancel.clone(),
                now_unix_millis,
            ))?;
            slot.insert(cancel.drop_guard());
        }
        Ok(())
    }

    /// Records a publication or drain failure that a clean engine exit must not hide.
    pub fn record_failure(&self) {
        self.failed.store(true, Ordering::Release);
    }

    fn next_publication_generation(&self) -> Result<u64> {
        self.next_publication_generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |generation| {
                generation.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| anyhow::anyhow!("publication generation exhausted"))
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
            match pending.entry(publication_id) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().state = PublicationState::Nacked;
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(PendingPublication {
                        state: PublicationState::Nacked,
                        generation: self.next_publication_generation()?,
                        active_admissions: 0,
                        owner_failed: false,
                        send_succeeded: false,
                        cancelled: false,
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn publication_staged(&self, publication_id: PublicationId) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?;
        match pending.entry(publication_id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let state = &mut entry.get_mut().state;
                if *state != PublicationState::Nacked {
                    *state = PublicationState::Staged;
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(PendingPublication {
                    state: PublicationState::Staged,
                    generation: self.next_publication_generation()?,
                    active_admissions: 0,
                    owner_failed: false,
                    send_succeeded: false,
                    cancelled: false,
                });
            }
        }
        Ok(())
    }

    /// Tracks a fresh identity across its downstream send, forgetting it only when the send returned it.
    ///
    /// The identity is tracked before sending so a fast publication outcome cannot precede it. An
    /// explicit send error hands the message back, so that identity never reached the exporter. A
    /// cancelled send remains tracked because a dropped flume send can already have been delivered.
    /// At terminal shutdown, an unstaged cancelled send is classified as ambiguous rather than
    /// mistaken for an acknowledged publication that failed to drain.
    ///
    /// # Errors
    /// Reports poisoned publication accounting; an unforgettable identity stays tracked.
    pub(crate) async fn admit_publication<E>(
        &self,
        publication_id: PublicationId,
        send: impl Future<Output = Result<(), E>>,
    ) -> Result<Result<(), E>> {
        let (generation, is_owner) = {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?;
            match pending.entry(publication_id) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let generation = self.next_publication_generation()?;
                    entry.insert(PendingPublication {
                        state: PublicationState::NeverStaged,
                        generation,
                        active_admissions: 1,
                        owner_failed: false,
                        send_succeeded: false,
                        cancelled: false,
                    });
                    (generation, true)
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let pending = entry.get_mut();
                    pending.active_admissions = pending
                        .active_admissions
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("publication admission count exhausted"))?;
                    (pending.generation, false)
                }
            }
        };
        let mut guard = AdmissionGuard {
            shutdown: self,
            publication_id,
            generation,
            armed: true,
        };
        let result = send.await;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?;
        let remove = match pending.get_mut(&publication_id) {
            Some(entry) if entry.generation == generation => {
                if is_owner && result.is_err() {
                    entry.owner_failed = true;
                }
                if result.is_ok() {
                    entry.send_succeeded = true;
                }
                entry.active_admissions = entry
                    .active_admissions
                    .checked_sub(1)
                    .ok_or_else(|| anyhow::anyhow!("publication admission accounting underflow"))?;
                entry.owner_failed
                    && entry.active_admissions == 0
                    && !entry.send_succeeded
                    && !entry.cancelled
                    && entry.state == PublicationState::NeverStaged
            }
            _ => false,
        };
        if remove {
            pending.remove(&publication_id);
        }
        guard.armed = false;
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
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("publication accounting poisoned"))?;
            let mut counts = PendingCounts::default();
            pending.retain(|_, publication| {
                match publication.state {
                    PublicationState::NeverStaged
                        if publication.cancelled
                            && !publication.send_succeeded
                            && publication.active_admissions == 0 =>
                    {
                        counts.cancelled_ambiguous += 1;
                        return false;
                    }
                    PublicationState::NeverStaged => counts.never_staged += 1,
                    PublicationState::Staged => counts.staged += 1,
                    PublicationState::Nacked => counts.nacked += 1,
                }
                true
            });
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
        if counts.cancelled_ambiguous != 0 {
            tracing::warn!(
                cancelled_ambiguous = counts.cancelled_ambiguous,
                "cancelled admissions had no observed publication outcome; delivery remains ambiguous"
            );
        }
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
            match self.drain_heartbeats.lock() {
                Ok(mut heartbeats) => {
                    heartbeats.remove(&instance_id);
                }
                Err(_) => errors.push("drain heartbeat registry poisoned".to_owned()),
            }
        }
        if !errors.is_empty() {
            bail!("{}", errors.join("; "));
        }
        Ok(())
    }
}

async fn renew_drain_registration(
    store: Arc<dyn RoutingStore>,
    instance_id: CollectorInstanceId,
    interval: Duration,
    ttl: Duration,
    cancel: CancellationToken,
    now: impl Fn() -> Result<UnixMillis, String>,
) {
    let mut heartbeat = tokio::time::interval(interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = heartbeat.tick() => {}
        }
        match now().and_then(|now| checked_add_millis(now, ttl).map(|expiry| (now, expiry))) {
            Ok((now, expiry)) => match store.heartbeat_collector(instance_id, now, expiry).await {
                Ok(()) => {}
                Err(error) => {
                    tracing::warn!(%instance_id, %error, "dataflow drain registration heartbeat failed");
                    if error.kind() == RoutingErrorKind::Conflict {
                        return;
                    }
                }
            },
            Err(error) => tracing::warn!(%instance_id, %error, "dataflow drain registration clock failed"),
        }
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
    async fn failed_creator_preserves_overlapping_successful_admission() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        let publication = PublicationId::new();
        let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
        let (release_first_tx, release_first_rx) = tokio::sync::oneshot::channel();
        let first = shutdown.admit_publication(publication, async move {
            first_started_tx.send(()).map_err(|_| "first admission dropped")?;
            release_first_rx.await.map_err(|_| "release dropped")?;
            Err("route full")
        });
        let second = async {
            first_started_rx.await?;
            let result = shutdown
                .admit_publication(publication, async { Ok::<(), &str>(()) })
                .await?;
            release_first_tx
                .send(())
                .map_err(|_| anyhow::anyhow!("first admission stopped"))?;
            Ok::<_, anyhow::Error>(result)
        };
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first?, Err("route full"));
        assert_eq!(second?, Ok(()));
        let error = shutdown
            .finish(true)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("overlapping admission must remain pending"))?
            .to_string();
        assert!(error.contains("never_staged=1"), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn stale_creator_cannot_remove_replacement_generation() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        let publication = PublicationId::new();
        let (first_started_tx, first_started_rx) = tokio::sync::oneshot::channel();
        let (release_first_tx, release_first_rx) = tokio::sync::oneshot::channel();
        let first = shutdown.admit_publication(publication, async move {
            first_started_tx.send(()).map_err(|_| "first admission dropped")?;
            release_first_rx.await.map_err(|_| "release dropped")?;
            Err("route full")
        });
        let replacement = async {
            first_started_rx.await?;
            shutdown.publication_finished(publication, true)?;
            let result = shutdown
                .admit_publication(publication, async { Ok::<(), &str>(()) })
                .await?;
            release_first_tx
                .send(())
                .map_err(|_| anyhow::anyhow!("first admission stopped"))?;
            Ok::<_, anyhow::Error>(result)
        };
        let (first, replacement) = tokio::join!(first, replacement);
        assert_eq!(first?, Err("route full"));
        assert_eq!(replacement?, Ok(()));
        let error = shutdown
            .finish(true)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("replacement generation must remain pending"))?
            .to_string();
        assert!(error.contains("never_staged=1"), "{error}");
        Ok(())
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
    async fn cancelled_full_channel_send_is_ambiguous_until_staged() -> Result<()> {
        let shutdown = RoutingShutdown::default();
        let publication = PublicationId::new();
        let (sender, receiver) = flume::bounded(1);
        sender.send(1)?;
        let send = shutdown.admit_publication(publication, sender.send_async(2));
        assert!(tokio::time::timeout(Duration::from_millis(10), send).await.is_err());
        assert_eq!(receiver.try_recv()?, 1);
        assert!(receiver.try_recv().is_err());
        shutdown.finish(true).await?;
        assert!(
            !shutdown
                .pending
                .lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))?
                .contains_key(&publication)
        );

        shutdown.publication_staged(publication)?;
        assert!(shutdown.finish(true).await.is_err());
        shutdown.publication_finished(publication, true)?;
        shutdown.finish(true).await
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

    #[tokio::test(start_paused = true)]
    async fn long_drain_keeps_registration_live_until_terminal_write() -> Result<()> {
        use arrowcol_protocol::{ImplementationPool, PositiveMeasurement};
        use arrowcol_routing::{CollectorRegistration, MemoryRoutingStore};
        use tokio::time::{Instant, sleep, timeout};

        let shutdown = RoutingShutdown::default();
        let store = Arc::new(MemoryRoutingStore::default());
        let id = CollectorInstanceId::new();
        let origin = Instant::now();
        let initial = now_unix_millis().map_err(anyhow::Error::msg)?;
        let clock = move || checked_add_millis(initial, origin.elapsed());
        let interval = Duration::from_secs(2);
        let ttl = Duration::from_secs(5);
        let initial_expiry = checked_add_millis(initial, ttl).map_err(anyhow::Error::msg)?;
        store
            .register_collector(&CollectorRegistration {
                instance_id: id,
                instance_key: "long-drain".to_owned(),
                implementation_pool: ImplementationPool::Dataflow,
                flight_endpoint: "127.0.0.1:32010".parse()?,
                catalog_capable: true,
                max_concurrent_streams: PositiveMeasurement::new(1)?,
                started_at_unix_ms: initial,
                heartbeat_at_unix_ms: initial,
                lease_expires_at_unix_ms: initial_expiry,
            })
            .await?;
        shutdown.register(Arc::clone(&store) as Arc<dyn RoutingStore>, id)?;
        store.set_collector_state(id, CollectorState::Draining, initial).await?;
        let cancel = CancellationToken::new();
        let task = tokio::spawn(renew_drain_registration(
            Arc::clone(&store) as Arc<dyn RoutingStore>,
            id,
            interval,
            ttl,
            cancel.clone(),
            clock,
        ));
        shutdown
            .drain_heartbeats
            .lock()
            .map_err(|_| anyhow::anyhow!("drain heartbeat registry poisoned"))?
            .insert(id, cancel.drop_guard());

        sleep(ttl * 3).await;
        let now = clock().map_err(anyhow::Error::msg)?;
        let view = store.load_view(now).await?;
        let draining = &view.collectors[&id];
        assert_eq!(draining.state, CollectorState::Draining);
        assert!(now >= initial_expiry);
        assert!(
            !draining.is_terminated_at(now),
            "live drain must not qualify for crash succession"
        );

        shutdown.finish(true).await?;
        timeout(interval, task).await??;
        let view = store.load_view(now).await?;
        let stopped = &view.collectors[&id];
        assert_eq!(stopped.state, CollectorState::Stopped);
        assert!(
            stopped.is_terminated_at(now),
            "terminal predecessor can be succeeded before expiry"
        );
        assert_eq!(
            stopped.registration.lease_expires_at_unix_ms,
            draining.registration.lease_expires_at_unix_ms
        );
        Ok(())
    }

    #[test]
    fn synchronous_finalizer_rejects_live_runtime_without_panicking() -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let _entered = runtime.enter();
        assert!(finish_routing_shutdown(true).is_err());
        Ok(())
    }
}
