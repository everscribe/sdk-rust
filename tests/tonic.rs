//! Tests for the tonic adapter (run with `--features tonic`).
//!
//! These exercise a real generated `EchoServer` over a real in-process TCP
//! channel (tonic's `Endpoint::connect`), not mocks: `serve` and `client`
//! below bind an actual `tokio::net::TcpListener`, so every assertion here
//! observes the same `grpc-status` trailer / head a real deployment would
//! produce.
#![cfg(feature = "tonic")]

mod pb {
    tonic::include_proto!("everscribe.testpb");
}

use std::sync::{Arc, Mutex};

use everscribe::event::{self, Event};
use everscribe::recorder::{RecordError, Recorder};
use everscribe::tonic::EverscribeGrpcLayer;
use pb::echo_client::EchoClient;
use pb::echo_server::{Echo, EchoServer};
use pb::{FailRequest, SayReply, SayRequest};
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};
use tonic::{Code, Request, Response, Status};
use tower::ServiceBuilder;

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

#[derive(Default)]
struct EchoSvc;

#[tonic::async_trait]
impl Echo for EchoSvc {
    async fn say(&self, request: Request<SayRequest>) -> Result<Response<SayReply>, Status> {
        let msg = request.get_ref().message.clone();
        if request.get_ref().set_action {
            event::current().with(|e| e.action = "custom.say".into());
        }
        Ok(Response::new(SayReply { message: msg }))
    }

    async fn fail(&self, request: Request<FailRequest>) -> Result<Response<SayReply>, Status> {
        let code = Code::from_i32(request.get_ref().code);
        if code == Code::Ok {
            return Ok(Response::new(SayReply {
                message: "ok".into(),
            }));
        }
        Err(Status::new(code, format!("requested {code:?}")))
    }

    type ListSayStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<SayReply, Status>> + Send>>;

    async fn list_say(
        &self,
        request: Request<SayRequest>,
    ) -> Result<Response<Self::ListSayStream>, Status> {
        // Reachable here: this runs inside the layer's event::scope, before
        // the handler returns its Response value.
        let ev = event::current();
        ev.with(|e| e.action = "before".into());
        let base = request.into_inner().message;
        let stream = async_stream::stream! {
            // Item 0: the *ambient* event::current() - called from inside the
            // stream body, which is driven by whatever polls the response
            // body, after the layer's event::scope(...).await has already
            // returned the Response value. Empirically a no-op: see
            // streaming_current_not_reachable_during_item_production, and the
            // src/tonic.rs module docs' streaming section for why (it is not
            // a tokio::spawn boundary - scope's task-local window simply ends
            // when the handler returns the stream, not when the stream
            // finishes).
            event::current().with(|e| e.action = "ambient-0".into());
            yield Ok(SayReply { message: format!("{base}-0") });

            // Item 1: the *captured* handle from before the handler
            // returned - moved into this generator, not re-fetched. This is
            // the supported workaround, and does take effect.
            ev.with(|e| e.action = "captured-1".into());
            yield Ok(SayReply { message: format!("{base}-1") });

            // Item 2: ambient current() again, proving item 0's no-op
            // wasn't a fluke of ordering - if the ambient call worked at
            // all, this would clobber "captured-1".
            event::current().with(|e| e.action = "ambient-2".into());
            yield Ok(SayReply { message: format!("{base}-2") });
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Starts a real server on an ephemeral TCP port with the layer installed,
/// returning a connected client channel and the fake recorder it feeds.
async fn serve() -> (EchoClient<Channel>, FakeRec) {
    let rec = FakeRec::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let layer = EverscribeGrpcLayer::new(rec.clone(), |_parts: &http::request::Parts| {
        everscribe::event::Actor::new("system")
    });
    let svc = ServiceBuilder::new()
        .layer(layer)
        .service(EchoServer::new(EchoSvc));

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let channel = Channel::builder(format!("http://{addr}").parse().unwrap())
        .connect()
        .await
        .unwrap();
    (EchoClient::new(channel), rec)
}

async fn wait_for<F: Fn() -> bool>(pred: F) {
    for _ in 0..200 {
        if pred() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for condition");
}

#[tokio::test]
async fn unary_action_defaults_to_full_method_name() {
    let (mut client, rec) = serve().await;
    client
        .say(SayRequest {
            message: "hi".into(),
            set_action: false,
        })
        .await
        .unwrap();

    wait_for(|| !rec.events().is_empty()).await;
    let events = rec.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "/everscribe.testpb.Echo/Say");
    assert_eq!(events[0].outcome.status, "ok");
    assert_eq!(events[0].outcome.code, 200);
    assert_ne!(events[0].outcome.code, 0, "OK must never record as code 0");
}

#[tokio::test]
async fn handler_set_action_wins_over_default() {
    let (mut client, rec) = serve().await;
    client
        .say(SayRequest {
            message: "hi".into(),
            set_action: true,
        })
        .await
        .unwrap();

    wait_for(|| !rec.events().is_empty()).await;
    let events = rec.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "custom.say");
}

#[tokio::test]
async fn current_reachable_inside_handler() {
    // handler_set_action_wins_over_default already proves current() resolves
    // to the same event the layer records: the handler's write is only
    // observable in the recorded event if it landed on the live task-local
    // handle, not a detached one. This test name documents that as the
    // explicit claim for the unary case; the streaming case is covered by
    // streaming_current_reachable_during_item_production below.
    let (mut client, rec) = serve().await;
    client
        .say(SayRequest {
            message: "hi".into(),
            set_action: true,
        })
        .await
        .unwrap();
    wait_for(|| !rec.events().is_empty()).await;
    assert_eq!(rec.events()[0].action, "custom.say");
}

#[tokio::test]
async fn permission_denied_records_denied_403() {
    let (mut client, rec) = serve().await;
    let err = client
        .fail(FailRequest {
            code: Code::PermissionDenied as i32,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);

    wait_for(|| !rec.events().is_empty()).await;
    let events = rec.events();
    assert_eq!(events[0].outcome.status, "denied");
    assert_eq!(events[0].outcome.code, 403);
}

#[tokio::test]
async fn ok_records_as_200_never_native_zero() {
    let (mut client, rec) = serve().await;
    client
        .fail(FailRequest {
            code: Code::Ok as i32,
        })
        .await
        .unwrap();

    wait_for(|| !rec.events().is_empty()).await;
    let events = rec.events();
    assert_eq!(events[0].outcome.status, "ok");
    assert_eq!(events[0].outcome.code, 200);
    assert_ne!(events[0].outcome.code, 0);
}

/// Exhaustive over all 17 gRPC status codes: asserts Outcome.code is always
/// the canonical HTTP equivalent, ported from sdk-go's adapter_codes.go, and
/// never the native gRPC code (which would collide with 0..=16, entirely
/// different numbers from the HTTP status space).
#[tokio::test]
async fn exhaustive_17_code_mapping() {
    let cases: &[(Code, i32, &str)] = &[
        (Code::Ok, 200, "ok"),
        (Code::Cancelled, 499, "error"),
        (Code::Unknown, 500, "error"),
        (Code::InvalidArgument, 400, "error"),
        (Code::DeadlineExceeded, 504, "error"),
        (Code::NotFound, 404, "error"),
        (Code::AlreadyExists, 409, "error"),
        (Code::PermissionDenied, 403, "denied"),
        (Code::ResourceExhausted, 429, "error"),
        (Code::FailedPrecondition, 400, "error"),
        (Code::Aborted, 409, "error"),
        (Code::OutOfRange, 400, "error"),
        (Code::Unimplemented, 501, "error"),
        (Code::Internal, 500, "error"),
        (Code::Unavailable, 503, "error"),
        (Code::DataLoss, 500, "error"),
        (Code::Unauthenticated, 401, "denied"),
    ];
    assert_eq!(cases.len(), 17, "must cover all 17 gRPC status codes");

    let (mut client, rec) = serve().await;
    for (code, want_http, want_status) in cases {
        let before = rec.events().len();
        let _ = client.fail(FailRequest { code: *code as i32 }).await;
        wait_for(|| rec.events().len() > before).await;
        let events = rec.events();
        let e = events.last().unwrap();
        assert_eq!(e.outcome.code, *want_http, "code mismatch for {code:?}");
        assert_eq!(
            e.outcome.status, *want_status,
            "status mismatch for {code:?}"
        );
        // The native gRPC code (0..=16) must never leak into Outcome.code
        // except where it is coincidentally also the right HTTP status
        // (there is no such overlap in this table).
        assert_ne!(
            i64::from(e.outcome.code),
            *code as i64,
            "native gRPC code leaked for {code:?}"
        );
    }
}

/// Streaming records once at stream close, with the right outcome, and the
/// action set from the *captured* handle wins over both the pre-stream
/// default and the ambient (and, per the next test, ineffective) calls made
/// from inside the stream body. See EchoSvc::list_say for exactly what each
/// item does and why.
#[tokio::test]
async fn streaming_records_once_with_captured_handle_action() {
    let (mut client, rec) = serve().await;
    let mut stream = client
        .list_say(SayRequest {
            message: "s".into(),
            set_action: false,
        })
        .await
        .unwrap()
        .into_inner();

    let mut items = Vec::new();
    while let Some(item) = tokio_stream::StreamExt::next(&mut stream).await {
        items.push(item.unwrap().message);
    }
    assert_eq!(items, vec!["s-0", "s-1", "s-2"]);

    wait_for(|| !rec.events().is_empty()).await;
    let events = rec.events();
    assert_eq!(events.len(), 1, "one event per RPC, not per message");
    assert_eq!(events[0].action, "captured-1");
    assert_eq!(events[0].outcome.status, "ok");
    assert_eq!(events[0].outcome.code, 200);
}

/// The verified streaming limitation: event::current(), called from inside
/// a streaming handler's item-producing body (as opposed to its synchronous
/// prelude, before the Response is returned), does not resolve to the event
/// this layer records. Proven here, not assumed - see the previous test's
/// "captured-1" surviving both the "ambient-0" write before it and the
/// "ambient-2" write after it: if the ambient event::current() call worked
/// at all inside the stream body, "ambient-2" (the last one) would have won
/// instead.
#[tokio::test]
async fn streaming_current_not_reachable_during_item_production() {
    let (mut client, rec) = serve().await;
    let mut stream = client
        .list_say(SayRequest {
            message: "t".into(),
            set_action: false,
        })
        .await
        .unwrap()
        .into_inner();
    while tokio_stream::StreamExt::next(&mut stream).await.is_some() {}

    wait_for(|| !rec.events().is_empty()).await;
    let action = rec.events()[0].action.clone();
    assert_ne!(action, "ambient-0");
    assert_ne!(action, "ambient-2");
    assert_eq!(action, "captured-1");
}
