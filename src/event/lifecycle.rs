//! The request-scoped record lifecycle: the request-scoped event handle,
//! dedupe state, idempotency-key stamping, and the auto-record entry point a
//! transport adapter drives.
//!
//! A transport adapter (axum today; actix-web or tonic later) supplies only
//! transport-specific bindings: an `Event` template (actor, origin), an
//! optional [`OutcomeCapture`], and a call to [`end`] after its handler
//! completes. Everything else lives here so every adapter gets it
//! identically instead of re-deriving it: the `recorded` dedupe flag, the
//! idempotency-key stamp, and the rule for when the "no response written"
//! sentinel may be applied. This mirrors `pkg/event/lifecycle.go` in
//! `sdk-go` and `src/event/event.ts`'s `begin`/`current`/`prepareEvent` in
//! `sdk-node` - both hard-won designs after real bugs, carried over rather
//! than re-derived.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use super::context::prepare;
use super::{Event, Outcome};

/// Reports the adapter-derived outcome for an in-flight call, so
/// [`prepare_event`] (mid-handler) and [`end`] (after the handler has
/// genuinely finished) can auto-populate an event's `outcome` when the
/// handler hasn't set one itself.
///
/// `None` means the call has not produced an outcome yet. This replaces an
/// integer-status sentinel: an HTTP status of `0` can mean "nothing
/// written", but gRPC's OK status IS code 0, so a transport-neutral capture
/// cannot signal "no outcome" with an integer. [`Outcome`] itself implements
/// this trait (always returning `Some(self.clone())`), so an adapter that
/// already knows the final outcome by the time it calls `end` - axum, whose
/// tower `Service` only sees a finished `Response` after the whole handler
/// future resolves - can hand one over directly. An adapter that exposes a
/// live, queryable response state instead (a `ResponseWriter`-style
/// wrapper, the way `sdk-go`'s stdlib adapter does) implements this trait
/// properly, returning `None` until something is actually written.
pub trait OutcomeCapture: Send + Sync {
    fn outcome(&self) -> Option<Outcome>;
}

impl OutcomeCapture for Outcome {
    fn outcome(&self) -> Option<Outcome> {
        Some(self.clone())
    }
}

/// Object-safe recording sink [`end`] needs.
///
/// Redeclared here rather than depending on [`crate::recorder::Recorder`]
/// directly - not to avoid an import cycle (Rust modules in one crate may
/// reference each other freely) but because that trait returns `impl
/// Future` (return-position `impl Trait` in a trait), which is not
/// dyn-compatible. `end` must accept whichever concrete recorder an adapter
/// is configured with through one non-generic signature, so it needs a
/// trait object; this is the dyn-compatible shape of the same capability.
/// Blanket-implemented for anything implementing `crate::recorder::Recorder`,
/// so adapters never construct this type themselves - passing `&HttpRecorder`
/// or `&BufferedRecorder` where `&dyn Recorder` is expected just works.
pub trait Recorder: Send + Sync {
    fn record<'a>(
        &'a self,
        event: Event,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::recorder::RecordError>> + Send + 'a>>;
}

impl<T> Recorder for T
where
    T: crate::recorder::Recorder + Send + Sync,
{
    fn record<'a>(
        &'a self,
        event: Event,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::recorder::RecordError>> + Send + 'a>> {
        Box::pin(crate::recorder::Recorder::record(self, event))
    }
}

/// The event plus the dedupe flag both submission paths (the adapter's
/// auto-record, and a handler's own manual record call) check and set. They
/// live together deliberately: the existing axum adapter already guarded
/// the shared event with `Arc<Mutex<Event>>`, so `recorded` gets a natural
/// home right next to the data it dedupes, both reachable through the same
/// `Arc` clone.
struct Shared {
    event: Mutex<Event>,
    recorded: AtomicBool,
}

/// A handle to the request-scoped event, shared between a transport
/// adapter's auto-record path and any manual recording a handler does
/// itself. Cloning is cheap (`Arc`); every clone refers to the same
/// underlying event.
///
/// This is the framework-neutral primitive invariant (2) asks for: a
/// transport adapter's "current event" extractor - axum's [`CurrentEvent`]
/// today, and any future actix-web/tonic equivalent - is a thin wrapper
/// that pulls one of these out of its own request storage and hands it to
/// the handler. See [`current`] for how a handler reaches one without an
/// extractor at all.
///
/// [`CurrentEvent`]: crate::axum::CurrentEvent
#[derive(Clone)]
pub struct EventHandle(Arc<Shared>);

impl EventHandle {
    fn wrap(e: Event) -> Self {
        EventHandle(Arc::new(Shared {
            event: Mutex::new(e),
            recorded: AtomicBool::new(false),
        }))
    }

    /// A handle that is never auto-recorded: what an adapter's extractor
    /// returns when no lifecycle has been installed on the request (the
    /// middleware/layer isn't mounted, or the call is happening outside a
    /// request at all).
    pub fn detached() -> Self {
        EventHandle::wrap(Event::default())
    }

    /// Mutate the shared event. Locks for the duration of `f`; keep `f`
    /// short and non-blocking.
    pub fn with<T>(&self, f: impl FnOnce(&mut Event) -> T) -> T {
        let mut guard = self.0.event.lock().expect("event mutex poisoned");
        f(&mut guard)
    }

    /// A snapshot (clone) of the shared event's current state.
    pub fn snapshot(&self) -> Event {
        self.0.event.lock().expect("event mutex poisoned").clone()
    }

    /// Whether this event has already been handed to a recorder, by either
    /// the manual path ([`EventHandle::prepare_for_record`]) or the
    /// auto-record path ([`end`]).
    pub fn is_recorded(&self) -> bool {
        self.0.recorded.load(Ordering::SeqCst)
    }

    /// Atomically claims the dedupe flag. Returns `true` for exactly one
    /// caller across however many times this is invoked on clones of the
    /// same handle; every other caller gets `false` and must not record.
    /// This is state, not inference: it does not look at `action` or
    /// anything else about the event, which is what keeps a handler that
    /// both sets `action` and records explicitly from submitting the same
    /// event id twice.
    fn try_claim_recorded(&self) -> bool {
        self.0
            .recorded
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Fills defaults (`id`, `occurred_at`) and, if the event has no outcome
    /// yet, fills it from `capture` (non-final: see [`prepare_event`]'s
    /// doc), then claims the dedupe flag and returns a snapshot.
    ///
    /// Call this immediately before a handler's own manual
    /// `recorder.record(...)` of the request-scoped event - it is a
    /// commitment to record, exactly like [`end`]'s claim, and shares the
    /// same flag: whichever of the two calls this event's `try_claim_recorded`
    /// first wins, and the loser's caller must not submit `Some` again.
    /// Returns `None` if this event was already claimed (already recorded,
    /// or already committed to being recorded) - the caller's cue to skip
    /// its own record call.
    pub fn prepare_for_record(&self, capture: Option<&dyn OutcomeCapture>) -> Option<Event> {
        if !self.try_claim_recorded() {
            return None;
        }
        let mut guard = self.0.event.lock().expect("event mutex poisoned");
        prepare(&mut guard);
        apply_outcome(capture, &mut guard, false);
        Some(guard.clone())
    }
}

/// Per-request state threaded through the task by [`scope`]: the template
/// (for [`new_from_context`]), the shared current event (for [`current`]),
/// and the optional outcome capture (for [`prepare_event`] and
/// [`EventHandle::prepare_for_record`]).
#[derive(Clone)]
struct Ctx {
    template: Event,
    current: EventHandle,
    capture: Option<Arc<dyn OutcomeCapture>>,
}

tokio::task_local! {
    static CTX: Ctx;
}

/// Builds the request-scoped current event from `template` (a clone, so the
/// template itself stays unstamped - see [`new_from_context`]), stamps
/// `idempotency_key = id` on it, then runs `fut` with it (and `capture`)
/// installed as this task's ambient lifecycle state.
///
/// Returns `fut`'s output alongside the [`EventHandle`], so the caller (a
/// transport adapter) can act on the same shared event after the handler
/// has returned - typically by calling [`end`].
///
/// Stamping happens here, unconditionally, and only on the derived current
/// event, never on `template`. Both the manual path
/// ([`EventHandle::prepare_for_record`]) and the auto-record path ([`end`])
/// must submit the same key so the server's `ON CONFLICT` arbiter absorbs a
/// duplicate instead of colliding on the events primary key; stamping later
/// (e.g. inside `end`) would key only the second submission, which dedupes
/// nothing. Stamping `template` instead would be worse: [`new_from_context`]
/// clones it for every mid-handler event, so every clone in a
/// multiple-events-per-handler flow would share one key and the server
/// would silently discard all but the first. A handler that sets its own
/// `idempotency_key` on the current event simply overwrites this default,
/// since the handler runs after `scope` has already stamped it.
///
/// # Why a task-local, not a passed-in parameter
///
/// Go threads the lifecycle through `context.Context`; Node threads it
/// through `AsyncLocalStorage`. Rust has no language-level equivalent, so
/// this SDK picks the closest analog available: a `tokio::task_local!`,
/// scoped around the handler's future. Like Go's context and Node's ALS, it
/// propagates automatically through nested `.await` points within the same
/// task without threading a parameter through every function signature -
/// which is what lets [`current`] and [`prepare_event`] be called with no
/// argument at all from deep inside a handler, matching the ergonomics
/// `event.Current(ctx)` / `current()` already have in `sdk-go` and
/// `sdk-node`. The tradeoff is the same as those two: state installed here
/// does not cross a `tokio::spawn` boundary, exactly as a Go goroutine
/// started without passing `ctx`, or a Node callback invoked outside
/// AsyncLocalStorage's tracked async operations, would also lose it. axum
/// handlers run to completion inside the same task the connection is
/// driven from by default, so this covers the common case; a handler that
/// explicitly spawns work and wants to record from it should call
/// [`current`] (or [`new_from_context`]) before spawning and move the
/// resulting value in.
///
/// The alternative considered was an explicit handle threaded as a function
/// parameter (or an extractor-only value, never reachable except through
/// axum's own extraction machinery). That would be more explicit, but it
/// would mean a bare free function, a background helper, or anything not
/// wired directly into the transport's own extraction system could never
/// reach the current event - unlike Go and Node, where any function
/// holding the ambient context/no-argument-at-all can. The task-local
/// keeps that ergonomic intact and is the one place in this port where
/// Rust cannot copy the other two languages verbatim, only their intent.
pub async fn scope<F: Future>(
    template: Event,
    capture: Option<Arc<dyn OutcomeCapture>>,
    fut: F,
) -> (F::Output, EventHandle) {
    let mut current_event = template.clone();
    current_event.idempotency_key = current_event.id.clone();
    let current = EventHandle::wrap(current_event);

    let ctx = Ctx {
        template,
        current: current.clone(),
        capture,
    };
    let output = CTX.scope(ctx, fut).await;
    (output, current)
}

/// Returns the request-scoped mutable event installed by [`scope`] - the
/// event a transport adapter will auto-record via [`end`]. Handlers
/// recording several events per request should use [`new_from_context`]
/// instead, which returns an independent clone with a fresh id.
///
/// Framework-neutral: callable with no arguments from anywhere inside the
/// handler's async call tree, the same as `event.Current(ctx)` in Go and
/// `current()` in Node. Outside a `scope` (no adapter mounted, or called
/// from a task spawned off the request without carrying the handle along),
/// returns a detached handle - harmless to call, but nothing recorded
/// through it is ever actually recorded, since no adapter owns it.
pub fn current() -> EventHandle {
    CTX.try_with(|c| c.current.clone())
        .unwrap_or_else(|_| EventHandle::detached())
}

/// Returns a fresh [`Event`] derived from the request-scoped template
/// installed by [`scope`]: same actor/action/target/origin/tenant_id seed,
/// a fresh `id` and `occurred_at`, empty `metadata`, and - deliberately -
/// no `idempotency_key`. Outside a `scope`, returns `Event::new("")`.
///
/// Handlers recording multiple events per request call this once per event
/// and record each themselves; [`current`] returns the one event the
/// adapter owns and auto-records.
pub fn new_from_context() -> Event {
    CTX.try_with(|c| clone_template(&c.template))
        .unwrap_or_else(|_| Event::new(""))
}

/// Derives a fresh event from `tmpl` for [`new_from_context`]: keeps
/// actor/action/target/origin/tenant_id, assigns a new id and timestamp,
/// starts with empty metadata (each event owns its own map), and clears
/// `idempotency_key`.
///
/// [`scope`] stamps `idempotency_key = id` exactly once, on the single
/// request-scoped current event it returns - never on the template this
/// clones from. A clone that inherited the key would let the server's
/// duplicate-absorbing `ON CONFLICT` arbiter dedupe unrelated events
/// against each other: every event a multi-event handler emits would share
/// one key, and the server would silently discard all but the first.
fn clone_template(tmpl: &Event) -> Event {
    let mut clone = tmpl.clone();
    clone.id = Uuid::new_v4().to_string();
    clone.occurred_at = chrono::Utc::now();
    clone.metadata.clear();
    clone.idempotency_key.clear();
    clone
}

/// Fills defaults on `e` (`id`, `occurred_at`, via [`prepare`]) and, if `e`
/// has no outcome yet, fills it from the ambient capture installed by
/// [`scope`] - only if that capture actually reports one.
///
/// This population is never final: `prepare_event` can run mid-handler,
/// when a handler records an extra event before the response is written.
/// At that moment the capture legitimately reports no outcome yet, which
/// does not mean no outcome ever - so unlike [`end`], this never stamps the
/// "no response written" sentinel. Only `end`, which runs after the
/// handler has genuinely finished, may do that.
///
/// Meant for a standalone event (typically a [`new_from_context`] clone, or
/// a non-HTTP `Event::new(...)`), not the request-scoped one `scope`
/// installed, since there is no dedupe flag to touch here at all. That's
/// unlike [`EventHandle::prepare_for_record`], which is a method on the
/// handle precisely because only the handle carries that flag.
pub fn prepare_event(e: &mut Event) {
    prepare(e);
    let capture = CTX.try_with(|c| c.capture.clone()).ok().flatten();
    apply_outcome(capture.as_deref(), e, false);
}

/// Fills `e.outcome` from `capture` when `e` doesn't already have one.
///
/// `is_final` distinguishes [`end`] from [`prepare_event`] and
/// [`EventHandle::prepare_for_record`], both of which can run mid-handler.
/// `end` is guaranteed to be the last word on the event's outcome, since it
/// runs after the handler has genuinely completed, so only a final caller
/// may stamp the "no response written" sentinel when the capture reports
/// no outcome. From a non-final caller, no outcome yet just means "nothing
/// written yet", not "nothing ever will be"; stamping the sentinel there
/// would bake a false error into an event recorded before the response.
fn apply_outcome(capture: Option<&dyn OutcomeCapture>, e: &mut Event, is_final: bool) {
    if !e.outcome.is_empty() {
        return;
    }
    let Some(capture) = capture else {
        return;
    };
    if let Some(outcome) = capture.outcome() {
        e.outcome = outcome;
        return;
    }
    if !is_final {
        return;
    }
    // No outcome was produced, and this call is final: keeps the
    // diagnostic explicit in core so no adapter can silently drop it.
    e.outcome = Outcome {
        status: "error".to_string(),
        code: 0,
        message: Some(serde_json::Value::String(
            "no response written".to_string(),
        )),
    };
}

/// Records `handle`'s event once, if `recorder` is configured and the
/// handler named it (`action` non-empty). Safe to call even when a handler
/// already recorded the same event manually via
/// [`EventHandle::prepare_for_record`] - both paths share `handle`'s dedupe
/// flag, so only the first submission wins; this call is then simply a
/// no-op.
///
/// `capture` is applied with `is_final = true`: calling `end` is the
/// adapter's guarantee that the handler has genuinely finished, so an
/// event that still has no outcome may get the "no response written"
/// sentinel. Pass the same capture given to [`scope`] (or `None`, if none
/// was, as axum's adapter does - it only learns the outcome after its
/// handler future resolves, via [`crate::event::outcome_from_http_status`],
/// so there is nothing live to install ahead of time).
pub async fn end(
    handle: &EventHandle,
    capture: Option<&dyn OutcomeCapture>,
    recorder: Option<&dyn Recorder>,
) {
    let Some(recorder) = recorder else { return };

    let mut ev = handle.snapshot();
    if ev.action.is_empty() {
        return;
    }
    if !handle.try_claim_recorded() {
        return; // the manual path already won it
    }

    prepare(&mut ev);
    apply_outcome(capture, &mut ev, true);

    if let Err(err) = recorder.record(ev).await {
        log::error!("everscribe: auto-record failed: {err}");
    }
}
