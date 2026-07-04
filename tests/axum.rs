//! Tests for the axum adapter (run with `--features axum`).
#![cfg(feature = "axum")]

use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::routing::{get, post};
use axum::Router;
use everscribe::axum::{CurrentEvent, EverscribeLayer};
use everscribe::event::{Actor, Event, Outcome, Target};
use everscribe::recorder::{RecordError, Recorder};
use http::request::Parts;
use http::{Request, StatusCode};
use tower::ServiceExt; // for `oneshot`

#[derive(Clone)]
struct FakeRec(Arc<Mutex<Vec<Event>>>);

impl FakeRec {
    fn new() -> Self {
        FakeRec(Arc::new(Mutex::new(Vec::new())))
    }
    fn events(&self) -> Vec<Event> {
        self.0.lock().unwrap().clone()
    }
}

impl Recorder for FakeRec {
    async fn record(&self, e: Event) -> Result<(), RecordError> {
        self.0.lock().unwrap().push(e);
        Ok(())
    }
}

fn resolver(parts: &Parts) -> Actor {
    match parts
        .headers
        .get("x-demo-actor")
        .and_then(|v| v.to_str().ok())
    {
        Some(id) if !id.is_empty() => Actor {
            r#type: "user".into(),
            id: id.into(),
            ..Default::default()
        },
        _ => Actor::new("anonymous"),
    }
}

async fn login(ev: CurrentEvent) -> &'static str {
    ev.with(|e| {
        e.action = "user.login".into();
        e.target = Target::new("user", "u1");
    });
    "ok"
}

async fn noop() -> &'static str {
    "x"
}

async fn denied(ev: CurrentEvent) -> StatusCode {
    ev.with(|e| e.action = "secret.reveal".into());
    StatusCode::FORBIDDEN
}

async fn boom(ev: CurrentEvent) -> StatusCode {
    ev.with(|e| e.action = "job.run".into());
    StatusCode::INTERNAL_SERVER_ERROR
}

async fn explicit(ev: CurrentEvent) -> &'static str {
    ev.with(|e| {
        e.action = "password.reset".into();
        e.outcome = Outcome {
            status: "denied".into(),
            code: 418,
            message: None,
        };
    });
    "ok"
}

fn app(rec: FakeRec) -> Router {
    Router::new()
        .route("/login", post(login))
        .route("/noop", get(noop))
        .route("/denied", get(denied))
        .route("/boom", get(boom))
        .route("/explicit", get(explicit))
        .layer(EverscribeLayer::new(rec, resolver))
}

async fn send(rec: &FakeRec, req: Request<Body>) -> StatusCode {
    app(rec.clone()).oneshot(req).await.unwrap().status()
}

fn req(method: &str, uri: &str, headers: &[(&str, &str)]) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    b.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn auto_records_on_finish() {
    let rec = FakeRec::new();
    let status = send(&rec, req("POST", "/login", &[("x-demo-actor", "u1")])).await;
    assert_eq!(status, StatusCode::OK);

    let events = rec.events();
    assert_eq!(events.len(), 1);
    let e = &events[0];
    assert_eq!(e.action, "user.login");
    assert_eq!(
        e.actor,
        Actor {
            r#type: "user".into(),
            id: "u1".into(),
            ..Default::default()
        }
    );
    assert_eq!(e.target, Target::new("user", "u1"));
    assert_eq!(e.outcome.status, "ok");
    assert_eq!(e.outcome.code, 200);
    assert!(!e.id.is_empty());
}

#[tokio::test]
async fn empty_action_not_recorded() {
    let rec = FakeRec::new();
    send(&rec, req("GET", "/noop", &[])).await;
    assert!(rec.events().is_empty());
}

#[tokio::test]
async fn anonymous_actor_without_header() {
    let rec = FakeRec::new();
    send(&rec, req("POST", "/login", &[])).await;
    assert_eq!(rec.events()[0].actor.r#type, "anonymous");
}

#[tokio::test]
async fn origin_from_headers() {
    let rec = FakeRec::new();
    send(
        &rec,
        req(
            "POST",
            "/login",
            &[("x-request-id", "req-1"), ("x-forwarded-for", "1.2.3.4")],
        ),
    )
    .await;
    let origin = &rec.events()[0].origin;
    assert_eq!(origin.request_id, "req-1");
    assert_eq!(origin.ip, "1.2.3.4");
}

#[tokio::test]
async fn status_403_maps_to_denied() {
    let rec = FakeRec::new();
    let status = send(&rec, req("GET", "/denied", &[])).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(rec.events()[0].outcome.status, "denied");
    assert_eq!(rec.events()[0].outcome.code, 403);
}

#[tokio::test]
async fn status_500_maps_to_error() {
    let rec = FakeRec::new();
    send(&rec, req("GET", "/boom", &[])).await;
    assert_eq!(rec.events()[0].outcome.status, "error");
    assert_eq!(rec.events()[0].outcome.code, 500);
}

#[tokio::test]
async fn explicit_outcome_wins_over_status() {
    let rec = FakeRec::new();
    let status = send(&rec, req("GET", "/explicit", &[])).await;
    assert_eq!(status, StatusCode::OK);
    let o = &rec.events()[0].outcome;
    assert_eq!(o.status, "denied");
    assert_eq!(o.code, 418);
}
