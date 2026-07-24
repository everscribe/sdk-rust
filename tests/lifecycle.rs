//! Tests for the framework-neutral record lifecycle in `everscribe::event`
//! (`scope`, `current`, `new_from_context`, `prepare_event`, `end`).
//!
//! These pin four invariants carried over from `sdk-go` and `sdk-node`,
//! each a real bug found by testing there, not by review:
//!
//!   - dedupe is state (a flag both submission paths check and set), never
//!     inferred from whether `action` is non-empty
//!   - the idempotency key is stamped once, at `scope`, on the current
//!     event only - never on the template
//!   - the "no response written" sentinel may only be applied by `end`
//!     (final); `prepare_event`, which can run mid-handler, must leave a
//!     still-unknown outcome alone
//!   - `new_from_context` clones get a fresh id and never inherit the
//!     idempotency key
//!
//! Exercised directly against the core, not through the axum adapter,
//! since these are lifecycle invariants any future transport adapter
//! (actix-web, tonic, ...) must inherit for free.

use std::sync::{Arc, Mutex};

use everscribe::event::{self, Event, Outcome, OutcomeCapture};
use everscribe::recorder::{RecordError, Recorder};

#[derive(Clone, Default)]
struct FakeRecorder(Arc<Mutex<Vec<Event>>>);

impl FakeRecorder {
    fn events(&self) -> Vec<Event> {
        self.0.lock().unwrap().clone()
    }
}

impl Recorder for FakeRecorder {
    async fn record(&self, e: Event) -> Result<(), RecordError> {
        self.0.lock().unwrap().push(e);
        Ok(())
    }
}

/// An [`OutcomeCapture`] that never produces an outcome - stands in for a
/// handler that panics, or a streaming response that never finishes,
/// distinguishing "nothing yet" (legitimate mid-handler) from "nothing
/// ever" (legitimate only once `end` runs).
struct NeverCapture;

impl OutcomeCapture for NeverCapture {
    fn outcome(&self) -> Option<Outcome> {
        None
    }
}

// --- invariant 4: idempotency key stamped on current, at scope, never on
// the template ---------------------------------------------------------

#[tokio::test]
async fn idempotency_key_stamped_on_current_not_template() {
    let tmpl = Event::new("");
    let ((current_key, current_id, clone_key, clone_id), _handle) =
        event::scope(tmpl, None, async {
            let current = event::current().snapshot();
            let clone = event::new_from_context();
            (
                current.idempotency_key.clone(),
                current.id.clone(),
                clone.idempotency_key.clone(),
                clone.id.clone(),
            )
        })
        .await;

    assert!(
        !current_key.is_empty(),
        "scope must stamp an idempotency key on the current event"
    );
    assert_eq!(
        current_key, current_id,
        "the stamped idempotency key must equal the current event's own id"
    );
    assert!(
        clone_key.is_empty(),
        "new_from_context must not inherit the idempotency key from the template"
    );
    assert_ne!(
        clone_id, current_id,
        "new_from_context must return a distinct event, not the current one"
    );
}

#[tokio::test]
async fn handler_supplied_idempotency_key_overrides_default() {
    let tmpl = Event::new("");
    let (custom_key, _handle) = event::scope(tmpl, None, async {
        event::current().with(|e| e.idempotency_key = "caller-supplied".to_string());
        event::current().snapshot().idempotency_key
    })
    .await;
    assert_eq!(custom_key, "caller-supplied");
}

// --- invariant 6: new_from_context clones get fresh ids and stay keyless ---

#[tokio::test]
async fn new_from_context_clones_are_distinct_and_keyless() {
    let mut tmpl = Event::new("");
    tmpl.actor.r#type = "user".to_string();
    tmpl.action = "seed.action".to_string();

    let ((a_id, a_key, a_action), (b_id, b_key, b_action)) =
        event::scope(tmpl, None, async {
            let a = event::new_from_context();
            let b = event::new_from_context();
            (
                (a.id.clone(), a.idempotency_key.clone(), a.action.clone()),
                (b.id.clone(), b.idempotency_key.clone(), b.action.clone()),
            )
        })
        .await
        .0;

    assert_ne!(a_id, b_id, "each clone must get its own fresh id");
    assert!(a_key.is_empty(), "clones must never carry an idempotency key");
    assert!(b_key.is_empty(), "clones must never carry an idempotency key");
    // Non-key fields still seed from the template.
    assert_eq!(a_action, "seed.action");
    assert_eq!(b_action, "seed.action");
}

#[tokio::test]
async fn new_from_context_outside_scope_is_a_minimal_event() {
    let e = event::new_from_context();
    assert!(e.action.is_empty());
    assert!(e.idempotency_key.is_empty());
}

// --- invariant 3: dedupe is state, not inferred from action ------------

#[tokio::test]
async fn end_alone_records_once_when_handler_only_sets_action() {
    let rec = FakeRecorder::default();
    let tmpl = Event::new("");

    let (_, handle) = event::scope(tmpl, None, async {
        event::current().with(|e| e.action = "user.login".to_string());
    })
    .await;

    event::end(&handle, None, Some(&rec)).await;
    event::end(&handle, None, Some(&rec)).await; // adapters may call end more than once

    assert_eq!(rec.events().len(), 1);
    assert!(handle.is_recorded());
}

#[tokio::test]
async fn manual_record_then_auto_end_records_only_once() {
    let rec = FakeRecorder::default();
    let tmpl = Event::new("");

    // This is exactly the case invariant 3 warns about: the handler both
    // names the event (sets `action`) AND records it explicitly. Inferring
    // dedupe from "action is non-empty" would let `end`'s auto-record fire
    // a second time for the same event id.
    let (_, handle) = event::scope(tmpl, None, async {
        event::current().with(|e| e.action = "user.login".to_string());
        let ready = event::current()
            .prepare_for_record(None)
            .expect("first claim on a fresh event must win");
        rec.record(ready).await.unwrap();
    })
    .await;

    event::end(&handle, None, Some(&rec)).await;

    let events = rec.events();
    assert_eq!(
        events.len(),
        1,
        "a handler that sets action and records manually must not also get an auto-recorded duplicate"
    );
    assert!(handle.is_recorded());
}

#[tokio::test]
async fn second_manual_claim_is_refused() {
    let tmpl = Event::new("");
    let ((first, second), _handle) = event::scope(tmpl, None, async {
        event::current().with(|e| e.action = "user.login".to_string());
        let first = event::current().prepare_for_record(None);
        let second = event::current().prepare_for_record(None);
        (first, second)
    })
    .await;

    assert!(first.is_some(), "the first claim must win");
    assert!(second.is_none(), "a second claim on the same event must be refused");
}

// --- invariant 5: the "no response written" sentinel is end's alone,
// never prepare_event's --------------------------------------------------

#[tokio::test]
async fn prepare_event_never_stamps_sentinel_mid_handler() {
    let capture: Arc<dyn OutcomeCapture> = Arc::new(NeverCapture);
    let tmpl = Event::new("");

    let (mid_handler_outcome_is_empty, handle) = event::scope(tmpl, Some(capture), async {
        event::current().with(|e| e.action = "user.login".to_string());

        // Mid-handler: the handler records a secondary event before the
        // response is written. NeverCapture reports no outcome, which at
        // this point means "nothing written YET", not "nothing ever will
        // be" - prepare_event must leave outcome untouched, not stamp the
        // sentinel.
        let mut extra = event::new_from_context();
        event::prepare_event(&mut extra);
        extra.outcome.is_empty()
    })
    .await;

    assert!(
        mid_handler_outcome_is_empty,
        "prepare_event (non-final) must not apply the \"no response written\" sentinel \
         just because the capture has not produced an outcome yet"
    );

    // Now the handler has genuinely finished and the adapter calls end,
    // which IS final. The capture still reports nothing (the response
    // truly never got written, e.g. a panic), so applying the sentinel
    // here is the correct diagnosis.
    let rec = FakeRecorder::default();
    let final_capture: Arc<dyn OutcomeCapture> = Arc::new(NeverCapture);
    event::end(&handle, Some(final_capture.as_ref()), Some(&rec)).await;

    let events = rec.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome.status, "error");
    assert_eq!(events[0].outcome.code, 0);
    assert_eq!(
        events[0].outcome.message,
        Some(serde_json::json!("no response written"))
    );
}

#[tokio::test]
async fn prepare_for_record_never_stamps_sentinel_when_not_final() {
    let capture: Arc<dyn OutcomeCapture> = Arc::new(NeverCapture);
    let tmpl = Event::new("");

    let (ready, _handle) = event::scope(tmpl, Some(capture.clone()), async move {
        event::current().with(|e| e.action = "user.login".to_string());
        // A handler manually recording the *current* event mid-flight
        // (before the response is written) via prepare_for_record - also
        // non-final, must not stamp the sentinel either.
        event::current().prepare_for_record(Some(capture.as_ref()))
    })
    .await;

    let ready = ready.expect("first claim must win");
    assert!(
        ready.outcome.is_empty(),
        "prepare_for_record (non-final) must not apply the sentinel"
    );
}

#[tokio::test]
async fn explicit_outcome_set_by_handler_is_never_overwritten() {
    let capture: Arc<dyn OutcomeCapture> = Arc::new(NeverCapture);
    let rec = FakeRecorder::default();
    let tmpl = Event::new("");

    let (_, handle) = event::scope(tmpl, Some(capture.clone()), async {
        event::current().with(|e| {
            e.action = "password.reset".to_string();
            e.outcome = Outcome {
                status: "denied".to_string(),
                code: 418,
                message: None,
            };
        });
    })
    .await;

    event::end(&handle, Some(capture.as_ref()), Some(&rec)).await;

    let events = rec.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome.status, "denied");
    assert_eq!(events[0].outcome.code, 418);
}
