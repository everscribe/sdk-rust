//! actix-web adapter (behind the `actix` cargo feature).
//!
//! [`EverscribeLayer`] installs a per-request event (actor via your resolver,
//! origin from headers) and auto-records it on response finish when an
//! `action` was set. Handlers reach the event with
//! [`everscribe::event::current()`](crate::event::current) - actix-web
//! handlers are plain async functions/methods, not [`FromRequestParts`](axum::extract::FromRequestParts)-style
//! extractors, so unlike the `axum` feature this adapter needs no
//! `CurrentEvent` wrapper type at all: the task-local [`crate::event::current`]
//! already is the thin binding.
//!
//! Like the `axum` feature, this adapter supplies only transport bindings on
//! top of the framework-neutral record lifecycle in [`crate::event`]: the
//! dedupe flag, idempotency-key stamp, and "no response written" sentinel
//! rule all live in core, not here.
//!
//! # The written-vs-default question
//!
//! `sdk-go`'s gin and echo adapters both need a "was anything written" flag
//! alongside the final status, because both frameworks hand handlers a
//! mutable response writer that is pre-initialized to status 200 before the
//! handler does anything; a handler that returns without calling `Write` at
//! all is indistinguishable, by reading status alone, from one that
//! deliberately wrote a 200. Investigating actix-web's own types shows this
//! ambiguity cannot arise here, for a structural reason rather than an
//! incidental one: an actix-web handler does not mutate a shared writer, it
//! *constructs and returns* one complete [`actix_web::HttpResponse`] value
//! (via the [`actix_web::Responder`] trait), so there is no ambient default
//! for a no-op handler to accidentally inherit. actix-web does not implement
//! `Responder` for bare `()` at all - seen directly in its source
//! (`response/responder.rs`), citing
//! <https://github.com/actix/actix-web/issues/1108> for the reasoning - so a
//! handler that "returns having written nothing" does not compile. The
//! closest legal analogue, a handler returning `Result<(), E>`, is special
//! cased to answer with **`204 No Content`** on `Ok(())`
//! (<https://github.com/actix/actix-web/pull/3560>), a genuinely different,
//! deliberately-chosen status from `200`. Extractor failures and 404s are
//! likewise converted to a real `HttpResponse` before this middleware's
//! `call` future ever resolves (see `handler_service` in actix-web's
//! `handler.rs`): every [`ServiceResponse`] this middleware observes on the
//! success path already carries a genuine, deliberately-produced status. So
//! this adapter's [`crate::event::outcome_from_http_status`] call needs no
//! "written" flag of its own the way `stdlibResponseWriter` or gin's
//! `ginCapture` do in `sdk-go` - the ambiguous state those exist to guard
//! against is not reachable through actix-web's own API shape. (A panic
//! mid-handler is a different case, not this one: it unwinds the whole
//! future including this middleware's own code after `.await`, so nothing
//! after that point runs at all - the same as it would for the axum
//! adapter's tower `Service`, with or without a written flag.)
//!
//! ```no_run
//! use actix_web::{post, App};
//! use everscribe::actix::EverscribeLayer;
//! use everscribe::event::{self, Actor};
//!
//! #[post("/login")]
//! async fn login() -> &'static str {
//!     event::current().with(|e| e.action = "user.login".into());
//!     "ok"
//! }
//!
//! // A real multi-worker HttpServer::new(move || ...) factory is called
//! // once per worker and so needs an owned recorder per call - wrap it in
//! // an Arc-backed type of your own (or one recorder per worker, if it's
//! // cheap to construct) rather than moving the same value in repeatedly.
//! # fn build(rec: everscribe::recorder::BufferedRecorder) {
//! let _app = App::new()
//!     .wrap(EverscribeLayer::new(rec, |req: &actix_web::dev::ServiceRequest| {
//!         match req.headers().get("x-demo-actor").and_then(|v| v.to_str().ok()) {
//!             Some(id) => Actor { r#type: "user".into(), id: id.into(), ..Default::default() },
//!             None => Actor::new("anonymous"),
//!         }
//!     }))
//!     .service(login);
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::Error;

use crate::event::{self, origin_from_headers, Actor, Event};

/// A boxed, non-`Send` future: actix-web runs each worker on its own
/// single-threaded `LocalSet`, so unlike axum's tower `Service` (which must
/// be `Send` to move across a multi-threaded runtime), this middleware's
/// future never needs to cross a thread once it starts.
type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Derives the [`Actor`] for a request from the pre-handler
/// [`ServiceRequest`] (headers, extensions such as a session set by an
/// earlier middleware, etc.).
pub trait ActorResolver: Fn(&ServiceRequest) -> Actor + 'static {}
impl<F: Fn(&ServiceRequest) -> Actor + 'static> ActorResolver for F {}

/// actix-web middleware factory that installs the per-request event and
/// auto-records it. Add it with `.wrap(EverscribeLayer::new(recorder,
/// resolve_actor))`.
///
/// `recorder` and `resolve` live behind `Arc` (not `Rc`): [`crate::recorder::Recorder`]
/// requires `Send + Sync` since [`crate::event::end`] awaits it from inside
/// this middleware's boxed future, and actix workers run on their own
/// threads (one per CPU by default), so the same `EverscribeLayer` value is
/// shared across worker threads even though any one request is handled on a
/// single worker's `LocalSet`.
pub struct EverscribeLayer<R, F> {
    recorder: Arc<R>,
    resolve: Arc<F>,
}

impl<R, F> EverscribeLayer<R, F> {
    pub fn new(recorder: R, resolve: F) -> Self {
        EverscribeLayer {
            recorder: Arc::new(recorder),
            resolve: Arc::new(resolve),
        }
    }
}

// Manual Clone for the same reason as axum's EverscribeLayer: derive would
// wrongly require R: Clone + F: Clone.
impl<R, F> Clone for EverscribeLayer<R, F> {
    fn clone(&self) -> Self {
        EverscribeLayer {
            recorder: self.recorder.clone(),
            resolve: self.resolve.clone(),
        }
    }
}

impl<S, B, R, F> Transform<S, ServiceRequest> for EverscribeLayer<R, F>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
    R: crate::recorder::Recorder + Send + Sync + 'static,
    F: ActorResolver,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = EverscribeMiddleware<S, R, F>;
    type InitError = ();
    type Future = std::future::Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        std::future::ready(Ok(EverscribeMiddleware {
            // actix's own Service trait takes `&self` (not `&mut self`,
            // unlike tower's), specifically so implementations do not need
            // the ready-clone dance axum's tower Service uses; Rc is enough
            // to share the inner service across calls on one worker.
            service: Rc::new(service),
            recorder: self.recorder.clone(),
            resolve: self.resolve.clone(),
        }))
    }
}

/// The [`Service`] produced by [`EverscribeLayer`].
pub struct EverscribeMiddleware<S, R, F> {
    service: Rc<S>,
    recorder: Arc<R>,
    resolve: Arc<F>,
}

impl<S, B, R, F> Service<ServiceRequest> for EverscribeMiddleware<S, R, F>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    R: crate::recorder::Recorder + Send + Sync + 'static,
    F: ActorResolver,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let service = Rc::clone(&self.service);
        let recorder = self.recorder.clone();
        let resolve = self.resolve.clone();

        Box::pin(async move {
            let actor = (resolve)(&req);
            // connection_info() borrows req through a RefCell guard; copy the
            // one field needed before req moves into service.call below.
            let remote_addr = req
                .connection_info()
                .peer_addr()
                .unwrap_or_default()
                .to_string();
            let origin = origin_from_headers(
                |name| {
                    req.headers()
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned)
                },
                &remote_addr,
            );
            let tmpl = Event {
                actor,
                origin,
                ..Event::default()
            };

            // Mirrors axum: actix-web only hands back a finished
            // ServiceResponse after the whole handler chain resolves, so
            // there is no live outcome to install ahead of time - capture is
            // None going into scope. Handlers reach the installed event via
            // event::current(), no extractor needed (see module docs).
            let (call_result, current) = event::scope(tmpl, None, service.call(req)).await;

            let resp = call_result?;
            let outcome = event::outcome_from_http_status(resp.status().as_u16());
            let recorder: &dyn event::Recorder = recorder.as_ref();
            event::end(&current, Some(&outcome), Some(recorder)).await;
            Ok(resp)
        })
    }
}
