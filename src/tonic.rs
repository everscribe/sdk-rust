//! tonic / tower gRPC adapter (behind the `tonic` cargo feature).
//!
//! [`EverscribeGrpcLayer`] wraps a tonic-generated `<Service>Server<T>` (or
//! anything else shaped like one - a `tower::Service<http::Request<ReqBody>,
//! Response = http::Response<RespBody>>`), installs a per-request event, and
//! auto-records it once the RPC's real gRPC status is known. Handlers reach
//! the event with [`crate::event::current`], exactly like the `actix`
//! feature: tonic-generated service methods are plain async functions/trait
//! methods, not extractors, so there is no `CurrentEvent`-equivalent type
//! here either.
//!
//! # Why not `tonic::service::Interceptor`
//!
//! `tonic::service::Interceptor` only sees the request - it has no way to
//! observe the response, so it cannot derive an outcome at all. This adapter
//! is a `tower::Layer`, the same shape as the `axum` feature's, precisely so
//! it can wrap the *response*.
//!
//! # Reading the trailer
//!
//! tonic (and gRPC generally) returns HTTP 200 for RPC-level errors too; the
//! real status lives in the `grpc-status` **trailer**, sent after the
//! response body, not in the response head. So the outcome cannot be read
//! off `resp.status()` the way the `axum` and `actix` adapters read theirs -
//! this adapter wraps the response *body* ([`TrailerCaptureBody`]) and
//! inspects the frames as they stream past for the trailers frame, which is
//! where [`tonic::Status::from_header_map`] finds `grpc-status` (and
//! `grpc-message`) for a normal response.
//!
//! A **trailers-only** response (no messages at all - typically an RPC
//! rejected before the handler produced anything, e.g. an interceptor
//! denial) is the other shape gRPC-over-HTTP/2 allows: the initial HEADERS
//! frame itself carries `END_STREAM`, so `grpc-status` arrives in the
//! response *head*, and the body never produces a trailers frame at all.
//! This adapter checks the head first, before ever wrapping the body, and
//! records immediately when it finds one there - see `call` below.
//!
//! # Result.code is the canonical HTTP equivalent, never the native gRPC code
//!
//! [`crate::event::Outcome::code`] carries the HTTP-equivalent status
//! (`http_status_for_grpc`, ported from `sdk-go`'s
//! `pkg/event/adapter_codes.go`), not the raw gRPC code. This is
//! deliberate, not cosmetic: gRPC's OK is code 0, which every wire encoder in
//! every one of this SDK's sibling implementations drops as empty
//! (`omitempty`/`skip_serializing_if`), so a native code would make a
//! *successful* RPC unmatchable by a `result.code` query - the exact
//! collision [`crate::event::outcome_from_http_status`]'s own "no response
//! written" sentinel already has to work around for HTTP. Routing gRPC
//! status through the same HTTP-status table core already understands
//! (rather than reimplementing the ok/denied/error split here) means OK maps
//! to code `200`, never `0`.
//!
//! # action defaults to the full method name
//!
//! Unlike the `axum` and `actix` adapters - which record nothing until a
//! handler explicitly names an event - every RPC here defaults `action` to
//! the request path (e.g. `/billing.v1.Billing/RefundInvoice`), so every RPC
//! records unless a handler clears it. `sdk-go` and `sdk-node` both do this
//! for the same reason: an unnamed gRPC call is far more likely to be a
//! forgotten audit point than an intentionally-unaudited one, the reverse of
//! the HTTP case where most routes (health checks, static assets) are
//! rightly never audited.
//!
//! The default is stamped on the *request-scoped event*
//! ([`crate::event::current`]), never on the template passed to
//! [`crate::event::scope`]. Stamping the template was a real bug caught in
//! `sdk-go`'s review: every [`crate::event::new_from_context`] clone a
//! handler makes inherits the template, so a secondary event the handler
//! never named would inherit the RPC method name too, instead of being
//! dropped by `end`'s empty-action guard. A handler that sets its own
//! `action` afterward simply overwrites this default, since the handler runs
//! after the default is stamped.
//!
//! # Streaming
//!
//! tonic dispatches all four RPC shapes (unary, server streaming, client
//! streaming, bidirectional) through the exact same generated
//! `tower::Service::call` this layer wraps - confirmed by reading tonic's
//! own `server::Grpc::{unary, server_streaming, client_streaming,
//! streaming}` (`src/server/grpc.rs`), all four of which just `.await` the
//! user's handler inline, with no `tokio::spawn` of their own. So this
//! layer's setup (actor/origin resolution, the request-scoped event, the
//! default `action`) and its outcome capture (watching body frames for the
//! trailer, which does not care how many messages preceded it) both apply
//! to every RPC shape without modification - implemented and exercised here
//! for unary and server streaming.
//!
//! What is *not* fully transparent, and was only found by testing it rather
//! than by reading the dispatch code above, is `event::current()`'s reach
//! during streaming. `tests/tonic.rs`'s `ListSay` handler calls
//! `event::current()` three times: once before returning its response
//! stream, and once from inside the stream body on two separate yielded
//! items. Only the first one sticks - `streaming_current_not_reachable_during_item_production`
//! asserts this directly. The reason is not a `tokio::spawn` boundary (there
//! is none here - `async-stream`'s generator is driven inline by whatever
//! polls the response body, on the very same task); it is that
//! [`crate::event::scope`]'s task-local is only installed for the dynamic
//! extent of the `.await` this layer drives, which ends the moment the
//! handler *returns* its `Response` - for a streaming RPC, that is before a
//! single item has been produced, since the stream is only polled by the
//! transport afterward, once this layer's own call to `event::scope` has
//! already returned. So `event::current()` called later, during item
//! production, resolves to a detached handle no matter what runs it or
//! where, spawned or not.
//!
//! The supported pattern for a streaming handler that needs to touch the
//! event while producing items is the same one `crate::event::scope`'s own
//! docs already prescribe for the `tokio::spawn` case, and it works for the
//! same reason: capture the [`crate::event::EventHandle`] itself
//! (`event::current()`, called - and stored - before returning the stream)
//! and call `.with(...)` on that captured handle from inside the stream
//! body, rather than re-fetching it via `event::current()` each time. A
//! handle carries its own `Arc`-shared state and needs no task-local access
//! at all once obtained, so it works from anywhere, at any point, regardless
//! of when or on what task it is used - `tests/tonic.rs`'s
//! `streaming_records_once_with_captured_handle_action` exercises exactly
//! this and passes.
//!
//! Client-streaming and bidirectional RPCs were not exercised here (no test
//! RPC of either shape was written), so are not claimed as verified, though
//! nothing above suggests they would behave differently: a client-streaming
//! handler's whole body (reading the inbound `Streaming<T>`, then returning
//! one response) runs before this layer's `event::scope` call returns, the
//! same as unary; a bidi handler that returns a response stream would have
//! the exact same `event::current()`-during-production caveat server
//! streaming does, for the exact same reason.
//!
//! ```no_run
//! use everscribe::event::{self, Actor};
//! use everscribe::tonic::EverscribeGrpcLayer;
//! use http::request::Parts;
//! use tower::Layer;
//!
//! // Any tonic-generated `<Service>Server<T>` (or anything else shaped like
//! // one) is a valid `grpc_server` here - `T` is left generic since this is
//! // a doc example, not tied to one particular generated service.
//! # fn build<S>(rec: everscribe::recorder::BufferedRecorder, grpc_server: S)
//! # where
//! #     S: tower::Service<http::Request<tonic::body::Body>, Response = http::Response<tonic::body::Body>>
//! #         + Clone + Send + 'static,
//! #     S::Future: Send + 'static,
//! #     S::Error: Send + 'static,
//! # {
//! let _wrapped = EverscribeGrpcLayer::new(rec, |parts: &Parts| {
//!     match parts.headers.get("x-demo-actor").and_then(|v| v.to_str().ok()) {
//!         Some(id) => Actor { r#type: "user".into(), id: id.into(), ..Default::default() },
//!         None => Actor::new("anonymous"),
//!     }
//! })
//! .layer(grpc_server);
//! // Inside an RPC handler: event::current().with(|e| e.action = "...".into());
//! # let _ = event::current();
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::request::Parts;
use http::{HeaderMap, Request, Response};
use http_body::{Body, Frame, SizeHint};
use serde_json::Value;
use tower::{Layer, Service};

use crate::event::{self, origin_from_headers, Actor, Event, EventHandle, Outcome};

/// Derives the [`Actor`] for an RPC from its request parts (gRPC metadata
/// arrives as ordinary HTTP/2 headers, so this is the same shape as the
/// `axum` adapter's resolver).
pub trait ActorResolver: Fn(&Parts) -> Actor + Send + Sync + 'static {}
impl<F: Fn(&Parts) -> Actor + Send + Sync + 'static> ActorResolver for F {}

/// tower [`Layer`] that installs the per-request event and auto-records it
/// once the RPC's gRPC status is known. Add it with
/// `.layer(EverscribeGrpcLayer::new(recorder, resolve_actor))` around a
/// tonic-generated `<Service>Server<T>`.
pub struct EverscribeGrpcLayer<R, F> {
    recorder: Arc<R>,
    resolve: Arc<F>,
}

// Manual Clone for the same reason as the axum/actix layers: derive would
// wrongly require R: Clone + F: Clone.
impl<R, F> Clone for EverscribeGrpcLayer<R, F> {
    fn clone(&self) -> Self {
        EverscribeGrpcLayer {
            recorder: self.recorder.clone(),
            resolve: self.resolve.clone(),
        }
    }
}

impl<R, F> EverscribeGrpcLayer<R, F> {
    pub fn new(recorder: R, resolve: F) -> Self {
        EverscribeGrpcLayer {
            recorder: Arc::new(recorder),
            resolve: Arc::new(resolve),
        }
    }
}

impl<S, R, F> Layer<S> for EverscribeGrpcLayer<R, F> {
    type Service = EverscribeGrpcService<S, R, F>;

    fn layer(&self, inner: S) -> Self::Service {
        EverscribeGrpcService {
            inner,
            recorder: self.recorder.clone(),
            resolve: self.resolve.clone(),
        }
    }
}

/// The [`Service`] produced by [`EverscribeGrpcLayer`].
pub struct EverscribeGrpcService<S, R, F> {
    inner: S,
    recorder: Arc<R>,
    resolve: Arc<F>,
}

impl<S: Clone, R, F> Clone for EverscribeGrpcService<S, R, F> {
    fn clone(&self) -> Self {
        EverscribeGrpcService {
            inner: self.inner.clone(),
            recorder: self.recorder.clone(),
            resolve: self.resolve.clone(),
        }
    }
}

impl<S, R, F, ReqBody, RespBody> Service<Request<ReqBody>> for EverscribeGrpcService<S, R, F>
where
    S: Service<Request<ReqBody>, Response = Response<RespBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    R: crate::recorder::Recorder + Send + Sync + 'static,
    F: ActorResolver,
    ReqBody: Send + 'static,
    RespBody: Body + Unpin + Send + 'static,
    RespBody::Data: Send,
    RespBody::Error: std::fmt::Display + Send,
{
    type Response = Response<TrailerCaptureBody<RespBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        // Ready-clone pattern, same as the axum adapter: poll_ready was
        // called on self.inner, so swap it out to drive this request and
        // leave a fresh clone in place.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let recorder = self.recorder.clone();
        let resolve = self.resolve.clone();

        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let full_method = parts.uri.path().to_string();
            let actor = (resolve)(&parts);
            let origin = origin_from_headers(
                |name| {
                    parts
                        .headers
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned)
                },
                // No generic way to reach a peer address at this layer: it
                // would come from a transport-specific request extension
                // (e.g. tonic::transport::server::TcpConnectInfo), and this
                // adapter deliberately depends on none of tonic's transport
                // feature set (see the module docs' dependency note). A
                // proxied deployment still gets a real client IP via
                // x-forwarded-for/x-real-ip, same as the other adapters.
                "",
            );
            let tmpl = Event {
                actor,
                origin,
                ..Event::default()
            };

            let (call_result, current) = event::scope(tmpl, None, async move {
                // Stamped on the request-scoped event, not `tmpl`: see the
                // module docs' "action defaults to the full method name"
                // section for why the template would be the wrong place.
                event::current().with(|e| e.action = full_method);
                inner.call(Request::from_parts(parts, body)).await
            })
            .await;

            let resp = call_result?;
            let (parts, body) = resp.into_parts();
            let capture = Capture {
                current,
                recorder: recorder as Arc<dyn event::Recorder>,
            };

            // Trailers-only response: grpc-status is already in the head,
            // and the body below will never produce a trailers frame at
            // all - see the module docs. Record now rather than waiting on
            // a frame that isn't coming.
            if let Some(outcome) = outcome_from_headers(&parts.headers) {
                spawn_end(capture, outcome);
                return Ok(Response::from_parts(parts, TrailerCaptureBody::done(body)));
            }

            Ok(Response::from_parts(
                parts,
                TrailerCaptureBody::watch(body, capture),
            ))
        })
    }
}

/// Delegates to the wrapped service's own name, so
/// `tonic::transport::Server::add_service` (which requires `NamedService` to
/// route by path) accepts a `Router<L>`-wrapped `EverscribeGrpcService`
/// exactly as it would the bare `<Service>Server<T>` this layer wraps.
impl<S, R, F> tonic::server::NamedService for EverscribeGrpcService<S, R, F>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

/// Everything [`TrailerCaptureBody`] needs to record once, moved into the
/// tokio task [`spawn_end`] starts. `recorder` is the dyn-compatible
/// [`crate::event::Recorder`] (not [`crate::recorder::Recorder`] directly),
/// the same non-generic sink [`crate::event::end`] itself takes - it lets
/// this body type stay free of the service's `R` type parameter entirely.
struct Capture {
    current: EventHandle,
    recorder: Arc<dyn event::Recorder>,
}

/// Runs [`crate::event::end`] on a detached task: [`http_body::Body::poll_frame`]
/// is synchronous and cannot itself await the recorder, and this can also
/// fire from [`TrailerCaptureBody`]'s `Drop` impl, which is sync by
/// construction. `event::end` only ever actually submits once no matter how
/// many times this runs for the same handle (the dedupe flag), so a
/// redundant call here (there shouldn't be one - see `finish`) is harmless.
fn spawn_end(capture: Capture, outcome: Outcome) {
    tokio::spawn(async move {
        event::end(
            &capture.current,
            Some(&outcome),
            Some(capture.recorder.as_ref()),
        )
        .await;
    });
}

/// Extracts a gRPC status from a header/trailer map and maps it onto the
/// canonical [`Outcome`] this adapter promises (HTTP-equivalent code, never
/// the native gRPC one). Returns `None` when no `grpc-status` is present at
/// all - the normal case for a response's initial headers, where the status
/// is expected to arrive later, in the trailer.
fn outcome_from_headers(headers: &HeaderMap) -> Option<Outcome> {
    let status = tonic::Status::from_header_map(headers)?;
    let mut outcome = event::outcome_from_http_status(http_status_for_grpc(status.code()));
    if !status.message().is_empty() {
        outcome.message = Some(Value::String(status.message().to_string()));
    }
    Some(outcome)
}

/// Maps a gRPC status code to its canonical HTTP equivalent, following the
/// grpc-gateway / Google API design guide mapping. Ported from `sdk-go`'s
/// `pkg/event/adapter_codes.go` (`HTTPStatusFor`) - see that file for the
/// full rationale (native codes collapse OK to 0, which every wire encoder
/// in this SDK family drops as empty, and native error codes don't overlap
/// HTTP's >=400 range at all).
///
/// The cost is the same one Go accepts: `InvalidArgument`, `FailedPrecondition`,
/// and `OutOfRange` all collapse to 400, so the exact gRPC code is not
/// recoverable from `Outcome.code` alone - the full status message survives
/// in `Outcome.message`.
fn http_status_for_grpc(code: tonic::Code) -> u16 {
    use tonic::Code;
    match code {
        Code::Ok => 200,
        Code::Cancelled => 499, // nginx's client-closed-request; no http crate constant
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::AlreadyExists | Code::Aborted => 409,
        Code::ResourceExhausted => 429,
        Code::Unimplemented => 501,
        Code::Unavailable => 503,
        Code::DeadlineExceeded => 504,
        // Unknown, Internal, DataLoss, and any future/unrecognized code.
        _ => 500,
    }
}

/// Wraps a gRPC response body to watch for the `grpc-status` trailer as
/// frames stream past, and records the request-scoped event via
/// [`crate::event::end`] the moment the RPC's real outcome is known -
/// whether that is a trailers frame ([`Body::poll_frame`]), a stream error
/// (also `poll_frame`, the `Err` arm), or the body simply being dropped
/// before either happens ([`Drop`]), e.g. the connection closing mid-stream.
/// Every path funnels through `finish`, so exactly one of them wins per
/// response.
///
/// `B: Unpin` keeps every part of this safe: no part of `poll_frame` needs
/// to project a pin over a `!Unpin` inner body, which real gRPC body types
/// (tonic's own included, and any body built from `UnsyncBoxBody`) already
/// are.
pub struct TrailerCaptureBody<B> {
    inner: B,
    capture: Option<Capture>,
}

impl<B> TrailerCaptureBody<B> {
    fn watch(inner: B, capture: Capture) -> Self {
        TrailerCaptureBody {
            inner,
            capture: Some(capture),
        }
    }

    /// For a trailers-only response, already recorded by the time this is
    /// constructed - see `call` above. The body is passed through
    /// untouched; `finish` is a no-op forever since `capture` starts `None`.
    fn done(inner: B) -> Self {
        TrailerCaptureBody {
            inner,
            capture: None,
        }
    }

    /// Records once, if this body hasn't already (`capture.take()` is the
    /// dedupe here, ahead of - and independent from - the `recorded` flag
    /// [`crate::event::end`] itself also checks).
    fn finish(&mut self, outcome: Outcome) {
        if let Some(capture) = self.capture.take() {
            spawn_end(capture, outcome);
        }
    }
}

impl<B> Body for TrailerCaptureBody<B>
where
    B: Body + Unpin,
    B::Error: std::fmt::Display,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<B::Data>, B::Error>>> {
        // Self is Unpin whenever B is (Capture holds no self-references), so
        // this is a plain reborrow, not a pin projection.
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_frame(cx);

        if let Poll::Ready(frame_opt) = &poll {
            match frame_opt {
                Some(Ok(frame)) => {
                    if let Some(trailers) = frame.trailers_ref() {
                        let outcome = outcome_from_headers(trailers)
                            .unwrap_or_else(|| event::outcome_from_http_status(0));
                        this.finish(outcome);
                    }
                }
                Some(Err(err)) => {
                    // A transport-level stream error, not an application
                    // grpc-status - there is no trailer to read at all.
                    this.finish(Outcome {
                        status: "error".to_string(),
                        code: 500,
                        message: Some(Value::String(err.to_string())),
                    });
                }
                None => {
                    // End of stream with no trailers frame ever seen: a
                    // protocol violation for a real gRPC body (grpc-status
                    // MUST be present per the spec), guarded rather than
                    // assumed away so the RPC still records instead of
                    // silently going unrecorded.
                    this.finish(event::outcome_from_http_status(0));
                }
            }
        }

        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<B> Drop for TrailerCaptureBody<B> {
    /// Covers the connection-dropped-mid-stream case: if neither a trailers
    /// frame nor a stream error ever reached `poll_frame` before this body
    /// was dropped, the RPC still records rather than silently vanishing -
    /// consistent with "action defaults to the full method name, so every
    /// RPC records."
    fn drop(&mut self) {
        self.finish(event::outcome_from_http_status(0));
    }
}
