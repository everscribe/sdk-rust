//! Tests for the recorder: HTTP request shape (via wiremock) and the buffered
//! recorder's batching, overflow policies, flush/close/stats (via a fake inner).

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use everscribe::event::Event;
use everscribe::recorder::{
    self, BatchRecorder, BufferedRecorder, HttpError, HttpRecorder, OverflowPolicy, RecordError,
    RecorderOptions,
};
use tokio::sync::Notify;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn opts(base_url: String) -> RecorderOptions {
    RecorderOptions {
        base_url,
        ..Default::default()
    }
}

// --- HttpRecorder (wiremock) ----------------------------------------------

#[tokio::test]
async fn http_record_posts_single_event() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/projects/proj1/events"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;

    let rec = HttpRecorder::new("proj1", "key1", &opts(server.uri()));
    rec.record(Event::new("user.login")).await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    assert_eq!(reqs[0].headers.get("authorization").unwrap(), "Bearer key1");
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body["action"], "user.login");
}

#[tokio::test]
async fn http_empty_action_is_noop() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    let rec = HttpRecorder::new("p", "k", &opts(server.uri()));
    rec.record(Event::new("")).await.unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn http_batch_filters_empty_and_wraps() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/projects/p/events/batch"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    let rec = HttpRecorder::new("p", "k", &opts(server.uri()));
    rec.record_batch(vec![Event::new("a"), Event::new(""), Event::new("b")])
        .await
        .unwrap();

    let reqs = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    let actions: Vec<&str> = body["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert_eq!(actions, ["a", "b"]);
}

#[tokio::test]
async fn http_non_2xx_is_http_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("  bad request  "))
        .mount(&server)
        .await;
    let rec = HttpRecorder::new("p", "k", &opts(server.uri()));
    let err = rec.record(Event::new("a")).await.unwrap_err();
    match err {
        RecordError::Http(HttpError { status, body }) => {
            assert_eq!(status, 400);
            assert_eq!(body, "bad request");
            assert!(!HttpError { status, body }.transient());
        }
        other => panic!("expected Http error, got {other:?}"),
    }
}

#[tokio::test]
async fn http_auto_idempotency_key_copies_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    let o = RecorderOptions {
        auto_idempotency_key: true,
        ..opts(server.uri())
    };
    let rec = HttpRecorder::new("p", "k", &o);
    rec.record(Event::new("a")).await.unwrap();
    let reqs = server.received_requests().await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body["idempotency_key"], body["id"]);
}

// --- BufferedRecorder (fake inner) ----------------------------------------

#[derive(Clone)]
struct FakeInner {
    batches: Arc<Mutex<Vec<Vec<Event>>>>,
    entered: Arc<Notify>,
    gate: Arc<tokio::sync::Mutex<()>>,
    fail: Arc<AtomicBool>,
}

impl FakeInner {
    fn new() -> Self {
        FakeInner {
            batches: Arc::new(Mutex::new(Vec::new())),
            entered: Arc::new(Notify::new()),
            gate: Arc::new(tokio::sync::Mutex::new(())),
            fail: Arc::new(AtomicBool::new(false)),
        }
    }
    fn collected(&self) -> Vec<Event> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .cloned()
            .collect()
    }
}

impl BatchRecorder for FakeInner {
    async fn record_batch(&self, events: Vec<Event>) -> Result<(), RecordError> {
        self.entered.notify_one();
        let _g = self.gate.lock().await; // uncontended unless a test holds it
        self.batches.lock().unwrap().push(events);
        if self.fail.load(Relaxed) {
            return Err(RecordError::Http(HttpError {
                status: 500,
                body: "boom".into(),
            }));
        }
        Ok(())
    }
}

fn quiet() -> RecorderOptions {
    // Large size/interval so the worker only flushes on explicit flush/close.
    RecorderOptions {
        flush_size: 10_000,
        flush_interval: Duration::from_secs(3600),
        ..Default::default()
    }
}

#[tokio::test]
async fn buffered_manual_flush_collects_all() {
    let inner = FakeInner::new();
    let rec = BufferedRecorder::new(inner.clone(), &quiet());
    rec.record(Event::new("a")).await.unwrap();
    rec.record(Event::new("b")).await.unwrap();
    rec.flush().await.unwrap();
    let actions: Vec<String> = inner.collected().iter().map(|e| e.action.clone()).collect();
    assert_eq!(actions, ["a", "b"]);
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_flush_on_size() {
    let inner = FakeInner::new();
    let o = RecorderOptions {
        flush_size: 2,
        flush_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let rec = BufferedRecorder::new(inner.clone(), &o);
    rec.record(Event::new("a")).await.unwrap();
    rec.record(Event::new("b")).await.unwrap();
    rec.flush().await.unwrap(); // sync point
    assert_eq!(inner.collected().len(), 2);
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_flush_on_interval() {
    let inner = FakeInner::new();
    let o = RecorderOptions {
        flush_size: 10_000,
        flush_interval: Duration::from_millis(50),
        ..Default::default()
    };
    let rec = BufferedRecorder::new(inner.clone(), &o);
    rec.record(Event::new("a")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(inner.collected().len(), 1);
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_empty_action_not_enqueued() {
    let inner = FakeInner::new();
    let rec = BufferedRecorder::new(inner.clone(), &quiet());
    rec.record(Event::new("")).await.unwrap();
    rec.flush().await.unwrap();
    assert!(inner.collected().is_empty());
    assert_eq!(rec.stats().dropped, 0);
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_close_drains() {
    let inner = FakeInner::new();
    let rec = BufferedRecorder::new(inner.clone(), &quiet());
    rec.record(Event::new("a")).await.unwrap();
    rec.record(Event::new("b")).await.unwrap();
    rec.close().await.unwrap();
    assert_eq!(inner.collected().len(), 2);
}

#[tokio::test]
async fn buffered_close_idempotent_and_record_after_close_noop() {
    let inner = FakeInner::new();
    let rec = BufferedRecorder::new(inner.clone(), &quiet());
    rec.close().await.unwrap();
    rec.close().await.unwrap();
    rec.record(Event::new("a")).await.unwrap();
    assert!(inner.collected().is_empty());
}

#[tokio::test]
async fn buffered_stats_flushed() {
    let inner = FakeInner::new();
    let rec = BufferedRecorder::new(inner.clone(), &quiet());
    rec.record(Event::new("a")).await.unwrap();
    rec.record(Event::new("b")).await.unwrap();
    rec.flush().await.unwrap();
    let s = rec.stats();
    assert_eq!(s.flushed, 2);
    assert_eq!(s.pending, 0);
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_flush_error_propagates_and_counts() {
    let inner = FakeInner::new();
    inner.fail.store(true, Relaxed);
    let rec = BufferedRecorder::new(inner.clone(), &quiet());
    rec.record(Event::new("a")).await.unwrap();
    let err = rec.flush().await.unwrap_err();
    assert!(matches!(err, RecordError::Http(_)));
    assert_eq!(rec.stats().flush_errs, 1);
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_overflow_drop_newest() {
    let inner = FakeInner::new();
    let o = RecorderOptions {
        buffer_size: 1,
        flush_size: 1,
        flush_interval: Duration::from_secs(3600),
        overflow: OverflowPolicy::DropNewest,
        ..Default::default()
    };
    let rec = BufferedRecorder::new(inner.clone(), &o);
    let guard = inner.gate.clone().lock_owned().await; // block the flush

    rec.record(Event::new("a")).await.unwrap(); // pulled -> flush -> blocks on gate
    inner.entered.notified().await; // task is now inside record_batch, channel drained
    rec.record(Event::new("b")).await.unwrap(); // fills the 1-slot channel
    rec.record(Event::new("c")).await.unwrap(); // full -> dropped
    assert_eq!(rec.stats().dropped, 1);

    drop(guard);
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_overflow_error() {
    let inner = FakeInner::new();
    let o = RecorderOptions {
        buffer_size: 1,
        flush_size: 1,
        flush_interval: Duration::from_secs(3600),
        overflow: OverflowPolicy::Error,
        ..Default::default()
    };
    let rec = BufferedRecorder::new(inner.clone(), &o);
    let guard = inner.gate.clone().lock_owned().await;

    rec.record(Event::new("a")).await.unwrap();
    inner.entered.notified().await;
    rec.record(Event::new("b")).await.unwrap();
    let err = rec.record(Event::new("c")).await.unwrap_err();
    assert!(matches!(err, RecordError::BufferFull));

    drop(guard);
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_overflow_block_unblocks_on_space() {
    let inner = FakeInner::new();
    let o = RecorderOptions {
        buffer_size: 1,
        flush_size: 1,
        flush_interval: Duration::from_secs(3600),
        overflow: OverflowPolicy::Block,
        ..Default::default()
    };
    let rec = Arc::new(BufferedRecorder::new(inner.clone(), &o));
    let guard = inner.gate.clone().lock_owned().await;

    rec.record(Event::new("a")).await.unwrap();
    inner.entered.notified().await;
    rec.record(Event::new("b")).await.unwrap(); // fills channel

    let rec2 = rec.clone();
    let blocked = tokio::spawn(async move { rec2.record(Event::new("c")).await });
    // Still blocked: no space and the flush is gated.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!blocked.is_finished());

    drop(guard); // flush completes -> space frees -> blocked send proceeds
    blocked.await.unwrap().unwrap();
    rec.close().await.unwrap();
}

#[tokio::test]
async fn buffered_close_drain_timeout() {
    let inner = FakeInner::new();
    let o = RecorderOptions {
        flush_size: 1,
        flush_interval: Duration::from_secs(3600),
        drain_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    let rec = BufferedRecorder::new(inner.clone(), &o);
    let guard = inner.gate.clone().lock_owned().await; // hang the flush

    rec.record(Event::new("a")).await.unwrap();
    inner.entered.notified().await;
    let err = rec.close().await.unwrap_err();
    assert!(matches!(err, everscribe::recorder::DrainTimeout));

    drop(guard);
}

#[tokio::test]
async fn factory_new_returns_buffered() {
    // Just confirm the factory wires an HttpRecorder + BufferedRecorder.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    let rec = recorder::new("p", "k", opts(server.uri()));
    rec.record(Event::new("a")).await.unwrap();
    rec.flush().await.unwrap();
    rec.close().await.unwrap();
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
}
