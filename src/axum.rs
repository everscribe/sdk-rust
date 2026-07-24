//! axum / tower middleware adapter (behind the `axum` cargo feature).
//!
//! [`EverscribeLayer`] installs a per-request event
//! (actor via your resolver, origin from headers) reachable in handlers via
//! the [`CurrentEvent`] extractor, and auto-records it on response finish when
//! an `action` was set.
//!
//! This adapter supplies only transport bindings on top of the
//! framework-neutral record lifecycle in [`crate::event`]
//! ([`crate::event::scope`], [`crate::event::current`],
//! [`crate::event::end`]): [`CurrentEvent`] is a thin wrapper over
//! [`crate::event::EventHandle`], and the dedupe flag, idempotency-key
//! stamp, and "no response written" sentinel rule all live in core, not
//! here. A future actix-web or tonic adapter reuses the same core and wires
//! up only its own actor/origin extraction and its own final-outcome call.
//!
//! ```no_run
//! use axum::{routing::post, Router};
//! use everscribe::axum::{CurrentEvent, EverscribeLayer};
//! use everscribe::event::Actor;
//! use http::request::Parts;
//!
//! # async fn build(rec: everscribe::recorder::BufferedRecorder) -> Router {
//! async fn login(ev: CurrentEvent) -> &'static str {
//!     ev.with(|e| e.action = "user.login".into());
//!     "ok"
//! }
//!
//! Router::new().route("/login", post(login)).layer(EverscribeLayer::new(
//!     rec,
//!     |parts: &Parts| match parts.headers.get("x-demo-actor").and_then(|v| v.to_str().ok()) {
//!         Some(id) => Actor { r#type: "user".into(), id: id.into(), ..Default::default() },
//!         None => Actor::new("anonymous"),
//!     },
//! ))
//! # }
//! ```

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::extract::{FromRequestParts, Request};
use axum::response::Response;
use http::request::Parts;
use tower::{Layer, Service};

use crate::event::{self, outcome_from_http_status, origin_from_headers, Actor, Event, EventHandle};

/// Derives the [`Actor`] for a request from its parts (headers, extensions such
/// as a session set by an earlier layer, etc.).
pub trait ActorResolver: Fn(&Parts) -> Actor + Send + Sync + 'static {}
impl<F: Fn(&Parts) -> Actor + Send + Sync + 'static> ActorResolver for F {}

/// A handle to the per-request event, installed by [`EverscribeLayer`] and
/// pulled into handlers as an extractor. Mutate it via [`CurrentEvent::with`];
/// the middleware records it on response finish when `action` is set.
///
/// A thin wrapper over [`crate::event::EventHandle`] - the framework-neutral
/// primitive - so this extractor is not where the record lifecycle lives,
/// only where axum reaches it from.
#[derive(Clone)]
pub struct CurrentEvent(EventHandle);

impl CurrentEvent {
    /// Mutate the per-request event.
    pub fn with<T>(&self, f: impl FnOnce(&mut Event) -> T) -> T {
        self.0.with(f)
    }
}

#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for CurrentEvent {
    type Rejection = Infallible;

    async fn from_request_parts(_parts: &mut Parts, _state: &S) -> Result<Self, Infallible> {
        // event::current() reads the ambient task-local lifecycle state
        // EverscribeService installed via event::scope around the whole
        // inner.call - the same primitive any future actix-web/tonic
        // adapter's equivalent extractor would call. It already falls back
        // to a detached handle when no lifecycle is installed, so no
        // request-extension lookup is needed here at all.
        Ok(CurrentEvent(event::current()))
    }
}

/// tower [`Layer`] that installs the per-request event and auto-records it.
/// Add it with `.layer(EverscribeLayer::new(recorder, resolve_actor))`.
pub struct EverscribeLayer<R, F> {
    recorder: Arc<R>,
    resolve: Arc<F>,
}

// Manual Clone: the recorder/resolver live behind `Arc`, so the layer is
// always cloneable regardless of whether `R`/`F` are (derive would wrongly
// require `R: Clone` + `F: Clone`, which e.g. BufferedRecorder is not).
impl<R, F> Clone for EverscribeLayer<R, F> {
    fn clone(&self) -> Self {
        EverscribeLayer {
            recorder: self.recorder.clone(),
            resolve: self.resolve.clone(),
        }
    }
}

impl<R, F> EverscribeLayer<R, F> {
    pub fn new(recorder: R, resolve: F) -> Self {
        EverscribeLayer {
            recorder: Arc::new(recorder),
            resolve: Arc::new(resolve),
        }
    }
}

impl<S, R, F> Layer<S> for EverscribeLayer<R, F> {
    type Service = EverscribeService<S, R, F>;

    fn layer(&self, inner: S) -> Self::Service {
        EverscribeService {
            inner,
            recorder: self.recorder.clone(),
            resolve: self.resolve.clone(),
        }
    }
}

/// The [`Service`] produced by [`EverscribeLayer`].
pub struct EverscribeService<S, R, F> {
    inner: S,
    recorder: Arc<R>,
    resolve: Arc<F>,
}

// Only the inner service needs to be Clone; the recorder/resolver are `Arc`.
impl<S: Clone, R, F> Clone for EverscribeService<S, R, F> {
    fn clone(&self) -> Self {
        EverscribeService {
            inner: self.inner.clone(),
            recorder: self.recorder.clone(),
            resolve: self.resolve.clone(),
        }
    }
}

impl<S, R, F> Service<Request> for EverscribeService<S, R, F>
where
    S: Service<Request, Response = Response> + Clone + Send + 'static,
    S::Future: Send + 'static,
    R: crate::recorder::Recorder + Send + Sync + 'static,
    F: ActorResolver,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // Ready-clone pattern: poll_ready was called on self.inner, so swap it
        // out to drive this request and leave a fresh clone in place.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let recorder = self.recorder.clone();
        let resolve = self.resolve.clone();

        Box::pin(async move {
            let (parts, body) = req.into_parts();

            let actor = (resolve)(&parts);
            let origin = origin_from_headers(
                |name| {
                    parts
                        .headers
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned)
                },
                "",
            );
            let tmpl = Event {
                actor,
                origin,
                ..Event::default()
            };

            // axum's tower Service only sees a finished Response after the
            // whole handler future resolves, so there is no live outcome to
            // install ahead of time - capture is None going into scope.
            // event::current() (what CurrentEvent's extractor calls) reads
            // the current-event handle this installs for the duration of
            // `fut`, so handlers reach it with no argument at all.
            let (call_result, current) = event::scope(tmpl, None, async move {
                inner.call(Request::from_parts(parts, body)).await
            })
            .await;

            let resp = call_result?;
            let outcome = outcome_from_http_status(resp.status().as_u16());
            let recorder: &dyn event::Recorder = recorder.as_ref();
            event::end(&current, Some(&outcome), Some(recorder)).await;
            Ok(resp)
        })
    }
}
