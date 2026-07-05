<p align="center">
  <img src="assets/everscribe.svg" alt="Everscribe" height="64" align="middle">
  &nbsp;&nbsp;<b>+</b>&nbsp;&nbsp;
  <img src="assets/rust.svg" alt="Rust" height="56" align="middle">
</p>

# sdk-rust

Rust SDK for the [Everscribe](https://everscribe.io) audit-log API. Two
coordinated surfaces:

- **Recorder** - append-only event ingest. Records who did what, when, on what
  resource, and, for mutation events, how the resource changed.
- **Minter** - mints short-lived embed tokens that let a customer's frontend
  mount the Everscribe embeddable component to display events without exposing
  the project API key to the browser.

Async on [tokio](https://tokio.rs); HTTP via [reqwest](https://docs.rs/reqwest).
An optional [axum](https://docs.rs/axum) `tower` middleware (behind the `axum`
cargo feature) installs a per-request event and auto-records it on response
finish. Requires Rust 1.85+.

---

## Table of contents

- [Install](#install)
- [Quickstart](#quickstart)
- [Three key behaviors](#three-key-behaviors)
- [The Event shape](#the-event-shape)
- [BufferedRecorder](#bufferedrecorder)
- [Idempotency](#idempotency)
- [Embedded views](#embedded-views)

---

## Install

```sh
cargo add everscribe                    # core
cargo add everscribe --features axum    # + the axum middleware
```

or add it to `Cargo.toml` by hand:

```toml
[dependencies]
everscribe = { version = "0.0.0", features = ["axum"] }
```

The crate is organized as: `everscribe::event`,
`everscribe::recorder`, `everscribe::minter`, and (with the feature)
`everscribe::axum`. The root of the crate binds credentials once and hands out
per-surface clients.

---

## Quickstart

### 1. Bind credentials and construct subclients

```rust,no_run
# async fn f() -> Result<(), Box<dyn std::error::Error>> {
let es = everscribe::new(project_id, api_key)?;   // errors if either is empty
let rec = es.new_recorder(Default::default());
// shutdown: rec.close().await?;
# Ok(()) }
```

For 12-factor / containerized deployments, read credentials from the
environment - `new_from_env` reads `EVERSCRIBE_PROJECT_ID` and
`EVERSCRIBE_API_KEY` and errors naming the missing variable:

```rust,no_run
let es = everscribe::new_from_env()?;
```

Override defaults with `RecorderOptions`:

```rust,no_run
use everscribe::recorder::{OverflowPolicy, RecorderOptions};
use std::time::Duration;

let rec = es.new_recorder(RecorderOptions {
    buffer_size: 2000,
    flush_interval: Duration::from_secs(2),
    overflow: OverflowPolicy::Block,
    ..Default::default()
});
```

Customers who only need one surface can skip the root client:
`everscribe::recorder::new(project_id, api_key, opts)` and
`everscribe::minter::Client::new(project_id, api_key, opts)` both work.

> **Async note.** `record(...).await` only enqueues the event (a background
> tokio task does the HTTP flush), so it never blocks on the network - it only
> `.await`s under `OverflowPolicy::Block` when the buffer is full.

### 2. Define your actor resolver

With the axum feature, the middleware's resolver receives the request
`Parts`, so identity can come off a session set by an earlier layer, a JWT, or
(in the demo below) a header:

```rust,no_run
use everscribe::event::Actor;
use http::request::Parts;

fn resolve_actor(parts: &Parts) -> Actor {
    match parts.headers.get("x-demo-actor").and_then(|v| v.to_str().ok()) {
        Some(id) => Actor { r#type: "user".into(), id: id.into(), ..Default::default() },
        None => Actor::new("anonymous"),
    }
}
```

### 3. Wire up the middleware

Add the layer to your router. It installs the per-request event and records it
on response finish:

```rust,no_run
use axum::{routing::post, Router};
use everscribe::axum::EverscribeLayer;
# fn resolve_actor(_: &http::request::Parts) -> everscribe::event::Actor { todo!() }
# async fn create_key() {}
# fn build(rec: everscribe::recorder::BufferedRecorder) -> Router {
Router::new()
    .route("/api-keys", post(create_key))
    .layer(EverscribeLayer::new(rec, resolve_actor))
# }
```

> **Ordering.** If your resolver reads a session, that session layer must run
> *before* (i.e. be the outer layer). In axum, layers added later wrap the
> outside, so add the session layer *after* `EverscribeLayer`.

### 4. Record events in handlers

Pull the per-request event with the `CurrentEvent` extractor and enrich it; the
middleware records on response finish when `action` is set:

```rust,no_run
use axum::http::StatusCode;
use everscribe::axum::CurrentEvent;
use everscribe::event::Target;

async fn create_key(event: CurrentEvent) -> StatusCode {
    event.with(|e| {
        e.action = "api_key.create".into();
        e.target = Target::new("api_key", "k_123");
    });
    StatusCode::CREATED
    // result auto-captures as { status: "ok", code: 201 }
}
```

`event.with(|e| ...)` gives you `&mut Event`. The middleware auto-fills `result`
from the response status, so error and success paths both record correctly
without you setting `result` by hand.

#### Recording state changes

For mutation events, attach before/after state with `diff`. The audit-log API
computes the JSON Patch on ingest.

```rust,no_run
# use everscribe::axum::CurrentEvent;
# use everscribe::event::Target;
# #[derive(serde::Serialize)] struct User { email: String }
# async fn load(_: &str) -> User { todo!() }
# async fn save(_: User) -> User { todo!() }
async fn update_user(event: CurrentEvent) {
    let before = load("u1").await;
    let after = save(User { email: "new@x.io".into() }).await;
    event.with(|e| {
        e.action = "user.update".into();
        e.target = Target::new("user", "u1");
        // redact sensitive JSON Pointer paths from the diff:
        e.diff_redacted(&before, &after, &["/password_hash"]);
    });
}
```

Use `diff(&before, &after)` when there's nothing to redact. Paths are
[JSON Pointers](https://datatracker.ietf.org/doc/html/rfc6901) (RFC 6901).

#### Recording multiple events per request

Some handlers fan out. Build extra events yourself and record them on the
recorder directly (in addition to, or instead of, the middleware's primary
event):

```rust,no_run
# use everscribe::event::{Event, Target};
# use everscribe::recorder::BufferedRecorder;
# async fn revoke(_: &str) {}
async fn revoke_all(rec: &BufferedRecorder, actor: everscribe::event::Actor, session_ids: Vec<String>) {
    for id in session_ids {
        revoke(&id).await;
        let mut e = Event::new("session.revoke");
        e.actor = actor.clone();
        e.target = Target::new("session", &id);
        rec.record(e).await.ok();
    }
}
```

The buffered recorder coalesces these (and events from other concurrent
requests) into a single batch call on each flush.

---

## Three key behaviors

**Empty `action` is a no-op.** The middleware skips auto-record when the
event's `action` is empty, and both recorders drop empty-action events at send
time, so handlers that bail out before setting an action produce no event.

**Overriding the resolver's actor.** When there's no session yet (login,
signup) or the actor isn't a session user (webhooks, system tasks), set
`e.actor` in the handler. Login is the canonical case: at handler entry the
resolver returns `anonymous`, and the handler overrides it once authentication
succeeds.

**Explicit `result` wins over auto-capture.** The middleware fills `result`
from the HTTP status only when you haven't set one. Set `e.outcome` yourself
for anti-enumeration handlers that return the same status for different audit
outcomes (e.g. a 404 that should record as `denied`).

---

## The Event shape

```rust,ignore
pub struct Event {
    pub id: String,                       // uuid v4; auto-generated
    pub tenant_id: String,                // optional within-project dimension
    pub occurred_at: DateTime<Utc>,       // auto-populated
    pub actor: Actor,                     // who caused the event
    pub action: String,                   // dotted verb, e.g. "user.lock"
    pub target: Target,                   // what was acted on
    pub metadata: BTreeMap<String, Value>,// freeform context
    pub origin: Origin,                   // IP, user-agent, request ID
    pub outcome: Outcome,                 // serialized as "result"
    pub change: Option<Change>,           // before/after for mutations
    pub idempotency_key: String,          // optional dedup key
}
```

Fields use plain values with empty defaults; empty means "not set" and the
serializer omits them. The JSON wire format is **snake_case** with empty fields
omitted.

> The outcome type is named **`Outcome`** (not `Result`) to avoid clashing with
> [`std::result::Result`]; it still serializes to the wire field `result`.

Metadata helpers:

```rust,no_run
# let mut e = everscribe::event::Event::new("x");
e.with_field("reason", "policy_violation");
e.with_fields([("severity".into(), serde_json::json!("high"))]);
```

For non-HTTP callers (background jobs, cron), build events directly and record
them on a recorder you constructed at boot:

```rust,no_run
# use everscribe::event::{Event, Actor, Target};
# async fn f(rec: &everscribe::recorder::BufferedRecorder) {
let mut e = Event::new("subscription.trial_expired");
e.actor = Actor { r#type: "system".into(), id: "trial_expirer".into(), ..Default::default() };
e.target = Target::new("subscription", "sub_1");
rec.record(e).await.ok();
# }
```

---

## BufferedRecorder

`recorder::new` (and `Client::new_recorder`) returns a `BufferedRecorder` -
events enqueue on a bounded channel and a background tokio task flushes batches
when the size threshold or interval is reached.

### Options (`RecorderOptions`)

| Field | Default | Purpose |
|---|---|---|
| `buffer_size` | `1000` | Capacity of the in-memory event buffer. |
| `flush_size` | `100` | Event count that triggers an immediate flush. |
| `flush_interval` | `5s` | Max time between flushes when the size threshold isn't reached. |
| `flush_timeout` | `30s` | Per-flush timeout for the batch HTTP call. |
| `overflow` | `DropNewest` | Behavior when the buffer is full. |
| `drain_timeout` | `30s` | Max time `close()` waits for in-flight events. |
| `request_timeout` | `10s` | Per-request HTTP timeout. |
| `base_url` | production | Override the ingestion endpoint (tests, staging). |
| `auto_idempotency_key` | `false` | Copy `event.id` into `idempotency_key` when empty. |

Durations are `std::time::Duration`.

### Overflow policies

```text
OverflowPolicy::DropNewest  // drop the incoming event, log a warning (default)
OverflowPolicy::Block       // await until space frees (the only awaiting case)
OverflowPolicy::Error       // return RecordError::BufferFull
```

### `flush`, `close`, and `stats`

- `close().await` flushes pending events and waits up to `drain_timeout`,
  returning `DrainTimeoutError` if it can't finish. Call once on shutdown.
- `flush().await` drains everything buffered at call time - useful in tests and
  graceful-shutdown sync points.
- `stats()` returns `dropped` / `flushed` / `flush_errs` / `pending` /
  `buffer_size` for observability.

### Errors

- `HttpError` - non-2xx from the ingestion endpoint; `.transient()` flags 5xx +
  429 for retry.
- `RecordError` - `Http` / `Transport` / `Timeout` / `BufferFull`, with a
  `.transient()` convenience.
- `DrainTimeoutError` - `close()` exceeded `drain_timeout`.

---

## Idempotency

Set `event.idempotency_key` for caller-supplied stable keys - webhook event
IDs, upstream request IDs, anything that identifies "the same logical event"
across retries:

```rust,no_run
# let mut e = everscribe::event::Event::new("subscription.created");
e.idempotency_key = stripe_event_id;
```

For SDK-internal safety against double-sends of the same event, set
`auto_idempotency_key: true` in `RecorderOptions`; it copies `event.id` into
`idempotency_key` at send time when the latter is empty. Caller-supplied keys
always win.

---

## Embedded views

The `minter` module mints short-lived JWTs that let a customer's frontend mount
the Everscribe embed component without exposing your API key.

### Minting a token

```rust,no_run
use everscribe::minter::TokenOptions;
use std::time::Duration;

# async fn f(es: everscribe::Client) -> Result<(), Box<dyn std::error::Error>> {
let m = es.new_minter(Default::default());

// Single-tenant (or admin) - no tenant_id, sees the whole project:
let token = m.mint_token(&TokenOptions {
    expires_in: Duration::from_secs(60 * 60),
    allowed_columns: Some(vec!["occurred_at".into(), "action".into(), "actor".into()]),
    ..Default::default()
}).await?;

// Multi-tenant - scoped to the signed-in user's tenant:
let token = m.mint_token(&TokenOptions {
    tenant_id: "acme".into(),
    expires_in: Duration::from_secs(60 * 60),
    allowed_actions: Some(vec!["user.*".into(), "billing.invoice.created".into()]),
    ..Default::default()
}).await?;
# let _ = token; Ok(()) }
```

Multi-tenant products typically expose two endpoints: `/embed-token/customer`
(with `tenant_id`) and `/embed-token/admin` (without). Same minter, different
scoping.

### `TokenOptions`

| Field | Type | Notes |
|---|---|---|
| `tenant_id` | `String` | Scopes reads to a tenant. Trimmed; ≤ 256 chars. Empty = unscoped. |
| `expires_in` | `Duration` | Server clamps to `[MIN_EXPIRES_IN, MAX_EXPIRES_IN]` (60s-24h). `ZERO` = server default (1h). |
| `allowed_columns` | `Option<Vec<String>>` | Whitelist of Event field names. `None` = no restriction; empty rejected. |
| `allowed_actions` | `Option<Vec<String>>` | Exact or suffix wildcard (`user.*`). `None` = no restriction; empty rejected. |
| `allowed_fields` | `Option<Vec<String>>` | Restricts catalog fields for DSL/NLP. Validated server-side. |
| `allow_dsl_input` / `allow_nlp` | `bool` | Unlock the Query / AI tabs. |

The crate exports `MIN_EXPIRES_IN`, `MAX_EXPIRES_IN`, and `ALLOWED_COLUMNS`.

### Errors

`mint_token` returns `MinterError`: `Validation` (client-side, no HTTP call),
`Http { status, body }` (400/401/404), `Transport` (network), or `Decode`
(unexpected response body).
