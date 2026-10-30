//! Bounded observational spans. Trace metadata never authenticates or grants work.
use serde::Serialize;
use std::{
    future::Future,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

tokio::task_local! { static CONTEXT: Context; }
#[cfg(test)]
tokio::task_local! { static TEST_OUTCOMES: std::cell::RefCell<Vec<(&'static str, &'static str)>>; }
static ORIGIN: OnceLock<Instant> = OnceLock::new();
static PROCESS: OnceLock<String> = OnceLock::new();
static EVENTS: AtomicU64 = AtomicU64::new(0);
const LIMIT: u64 = 100_000;

#[derive(Clone)]
pub struct Context {
    pub trace: String,
    pub role: &'static str,
    pub parent: Option<String>,
}

impl Context {
    pub fn new(trace: &str, role: &'static str) -> Self {
        Self {
            trace: if crate::safe_id(trace) {
                trace.to_owned()
            } else {
                uuid::Uuid::new_v4().to_string()
            },
            role,
            parent: None,
        }
    }
    pub fn from_parent(trace: &str, role: &'static str, parent: &str) -> Self {
        let mut context = Self::new(trace, role);
        if enabled() && crate::safe_id(parent) {
            context.parent = Some(parent.to_owned());
        }
        context
    }
    pub fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        CONTEXT.scope(self, Box::pin(future))
    }
}

pub fn current() -> Option<Context> {
    CONTEXT.try_with(Clone::clone).ok()
}

pub fn enabled() -> bool {
    std::env::var("ARROWCOL_TRACE").as_deref() == Ok("1")
}

fn emit(value: impl Serialize) {
    if !enabled() {
        return;
    }
    let n = EVENTS.fetch_add(1, Ordering::Relaxed);
    if n < LIMIT {
        match serde_json::to_string(&value) {
            Ok(line) => eprintln!("{line}"),
            Err(error) => eprintln!("trace serialization failed: {error}"),
        }
    } else if n == LIMIT {
        eprintln!("{{\"event\":\"trace_overflow\",\"complete\":false}}");
    }
}

pub struct Span {
    capture: Option<Capture>,
    stage: &'static str,
    category: &'static str,
    status: &'static str,
}

struct Capture {
    id: String,
    context: Context,
    start: Instant,
    offset: u128,
}

impl Span {
    pub fn new(stage: &'static str, category: &'static str) -> Self {
        let capture = if enabled() {
            current().map(|context| {
                let origin = ORIGIN.get_or_init(Instant::now);
                let start = Instant::now();
                Capture {
                    id: uuid::Uuid::new_v4().to_string(),
                    context,
                    start,
                    offset: start.duration_since(*origin).as_nanos(),
                }
            })
        } else {
            None
        };
        Self {
            capture,
            stage,
            category,
            status: "cancelled",
        }
    }
    pub fn completed(&mut self) {
        self.status = "completed";
    }
    pub fn id(&self) -> Option<&str> {
        self.capture.as_ref().map(|capture| capture.id.as_str())
    }
    pub fn failed(&mut self) {
        self.status = "failed";
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        #[cfg(test)]
        let _ = TEST_OUTCOMES.try_with(|outcomes| outcomes.borrow_mut().push((self.stage, self.status)));
        if let Some(capture) = &self.capture {
            let c = &capture.context;
            let process = PROCESS.get_or_init(|| uuid::Uuid::new_v4().to_string());
            emit(serde_json::json!({
                "event":"service_span","id":capture.id,"parent_id":c.parent,
                "trace_id":c.trace,"role":c.role,"process_id":process,"clock_id":process,
                "stage":self.stage,"category":self.category,"status":self.status,
                "offset_ns":capture.offset.to_string(),"duration_ns":capture.start.elapsed().as_nanos().to_string()
            }));
        }
    }
}

/// # Errors
/// Preserves the wrapped operation's error without changing its outcome.
pub fn measure<T, E, F: Future<Output = Result<T, E>>>(
    stage: &'static str,
    category: &'static str,
    future: F,
) -> impl Future<Output = Result<T, E>> {
    // Box before constructing the async state, including its initial arguments.
    let mut future = Box::pin(future);
    async move {
        let mut span = Span::new(stage, category);
        let result = if let Some(capture) = &span.capture {
            let mut child_context = capture.context.clone();
            child_context.parent = Some(capture.id.clone());
            CONTEXT.scope(child_context, future.as_mut()).await
        } else {
            future.as_mut().await
        };
        span.status = if result.is_ok() { "completed" } else { "failed" };
        result
    }
}

pub fn unit(unit: &str) {
    if !enabled() {
        return;
    }
    if let Some(c) = current() {
        emit(serde_json::json!({"event":"trace_unit","trace_id":c.trace,"unit":unit}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_wrappers_do_not_multiply_wrapped_future_storage() {
        struct LargeFuture([u8; 65_536]);
        impl Future for LargeFuture {
            type Output = Result<u8, ()>;

            fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                std::task::Poll::Ready(Ok(self.0[0]))
            }
        }

        let scope = Context::new("storage-check", "collector").scope(LargeFuture([0; 65_536]));
        let measured = measure("storage-check", "processing", LargeFuture([0; 65_536]));
        let nested = measure("outer", "lifecycle", measured);
        let limit = std::mem::size_of::<LargeFuture>() + 4_096;
        assert!(std::mem::size_of_val(&scope) <= limit);
        assert!(
            std::mem::size_of_val(&nested) <= limit,
            "nested trace future uses {} bytes, limit {limit}",
            std::mem::size_of_val(&nested),
        );
    }

    #[tokio::test]
    async fn context_is_explicit_and_task_local() {
        let a = Context::new("attempt-a", "collector");
        a.scope(async {
            assert_eq!(current().map(|c| c.trace), Some("attempt-a".into()));
            assert!(matches!(tokio::spawn(async { current() }).await, Ok(None)));
            assert_eq!(measure("test", "processing", async { Ok::<_, ()>(42) }).await, Ok(42));
        })
        .await;
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn returned_errors_and_dropped_futures_have_distinct_outcomes() {
        TEST_OUTCOMES
            .scope(std::cell::RefCell::new(Vec::new()), async {
                drop(measure("unpolled", "lifecycle", async { Ok::<_, ()>(1) }));
                TEST_OUTCOMES.with(|outcomes| assert!(outcomes.borrow().is_empty()));
                assert_eq!(measure("success", "lifecycle", async { Ok::<_, ()>(1) }).await, Ok(1));
                assert_eq!(
                    measure("rejected", "downstream_wait", async { Err::<(), _>("domain-error") }).await,
                    Err("domain-error")
                );
                let cancelled = tokio::time::timeout(
                    std::time::Duration::from_millis(1),
                    measure("cancelled", "lifecycle", std::future::pending::<Result<(), ()>>()),
                )
                .await;
                assert!(cancelled.is_err());
                TEST_OUTCOMES.with(|outcomes| {
                    assert_eq!(
                        *outcomes.borrow(),
                        [
                            ("success", "completed"),
                            ("rejected", "failed"),
                            ("cancelled", "cancelled"),
                        ]
                    )
                });
            })
            .await;
    }
}
