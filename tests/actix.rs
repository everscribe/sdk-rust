//! Tests for the actix-web adapter (run with `--features actix`).
#![cfg(feature = "actix")]

use std::sync::{Arc, Mutex};

use actix_web::dev::ServiceRequest;
use actix_web::http::StatusCode;
use actix_web::test::{call_service, init_service, TestRequest};
use actix_web::{web, App, HttpResponse};
use everscribe::actix::EverscribeLayer;
use everscribe::event::{self, Actor, Event};
use everscribe::recorder::{RecordError, Recorder};

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

fn resolver(req: &ServiceRequest) -> Actor {
    match req
        .headers()
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

async fn login() -> HttpResponse {
    event::current().with(|e| e.action = "user.login".into());
    HttpResponse::Ok().finish()
}

async fn noop() -> HttpResponse {
    HttpResponse::Ok().finish()
}

async fn denied() -> HttpResponse {
    event::current().with(|e| e.action = "secret.reveal".into());
    HttpResponse::Forbidden().finish()
}

async fn boom() -> HttpResponse {
    event::current().with(|e| e.action = "job.run".into());
    HttpResponse::InternalServerError().finish()
}

/// The closest actix-web has to "handler returned having written nothing":
/// `Result<(), E>` is the one type actix special-cases (see the `actix`
/// module docs and actix-web's own `response/responder.rs`), and it answers
/// `Ok(())` with 204 No Content - a real, deliberate status distinct from
/// 200, not a false default. There is no way to write a handler that
/// returns bare `()` at all: `Responder` is not implemented for it.
async fn wrote_nothing() -> Result<(), actix_web::Error> {
    event::current().with(|e| e.action = "silent.op".into());
    Ok(())
}

#[actix_web::test]
async fn auto_records_on_finish() {
    let rec = FakeRec::new();
    let app = init_service(
        App::new()
            .wrap(EverscribeLayer::new(rec.clone(), resolver))
            .route("/login", web::post().to(login)),
    )
    .await;
    let req = TestRequest::post()
        .uri("/login")
        .insert_header(("x-demo-actor", "u1"))
        .to_request();
    let resp = call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let events = rec.events();
    assert_eq!(events.len(), 1);
    let e = &events[0];
    assert_eq!(e.action, "user.login");
    assert_eq!(e.actor.r#type, "user");
    assert_eq!(e.actor.id, "u1");
    assert_eq!(e.outcome.status, "ok");
    assert_eq!(e.outcome.code, 200);
    assert!(!e.id.is_empty());
}

#[actix_web::test]
async fn unnamed_event_not_recorded() {
    let rec = FakeRec::new();
    let app = init_service(
        App::new()
            .wrap(EverscribeLayer::new(rec.clone(), resolver))
            .route("/noop", web::get().to(noop)),
    )
    .await;
    let req = TestRequest::get().uri("/noop").to_request();
    call_service(&app, req).await;
    assert!(rec.events().is_empty());
}

#[actix_web::test]
async fn anonymous_actor_without_header() {
    let rec = FakeRec::new();
    let app = init_service(
        App::new()
            .wrap(EverscribeLayer::new(rec.clone(), resolver))
            .route("/login", web::post().to(login)),
    )
    .await;
    let req = TestRequest::post().uri("/login").to_request();
    call_service(&app, req).await;
    assert_eq!(rec.events()[0].actor.r#type, "anonymous");
}

#[actix_web::test]
async fn denied_status_maps_to_denied() {
    let rec = FakeRec::new();
    let app = init_service(
        App::new()
            .wrap(EverscribeLayer::new(rec.clone(), resolver))
            .route("/denied", web::get().to(denied)),
    )
    .await;
    let req = TestRequest::get().uri("/denied").to_request();
    let resp = call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(rec.events()[0].outcome.status, "denied");
    assert_eq!(rec.events()[0].outcome.code, 403);
}

#[actix_web::test]
async fn error_status_maps_to_error() {
    let rec = FakeRec::new();
    let app = init_service(
        App::new()
            .wrap(EverscribeLayer::new(rec.clone(), resolver))
            .route("/boom", web::get().to(boom)),
    )
    .await;
    let req = TestRequest::get().uri("/boom").to_request();
    call_service(&app, req).await;
    assert_eq!(rec.events()[0].outcome.status, "error");
    assert_eq!(rec.events()[0].outcome.code, 500);
}

/// The verified "no-write" behavior: actix-web maps `Ok(())` to 204, a real
/// and distinct status, not a false 200. See the `actix` module docs and
/// `wrote_nothing` above.
#[actix_web::test]
async fn no_write_case_records_204_not_200() {
    let rec = FakeRec::new();
    let app = init_service(
        App::new()
            .wrap(EverscribeLayer::new(rec.clone(), resolver))
            .route("/silent", web::get().to(wrote_nothing)),
    )
    .await;
    let req = TestRequest::get().uri("/silent").to_request();
    let resp = call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let events = rec.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].outcome.code, 204);
    assert_eq!(events[0].outcome.status, "ok");
    assert_ne!(events[0].outcome.code, 200);
}
