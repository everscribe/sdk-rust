# everscribe (Rust SDK)

Rust SDK for the [Everscribe](https://everscribe.io) audit-log API.

> **Status: work in progress.** Built to parity with the
> [Go](https://github.com/everscribe/sdk-go),
> [Node](https://github.com/everscribe/sdk-node), and
> [Python](https://github.com/everscribe/sdk-python) SDKs. The API is not yet
> stable.

Two coordinated surfaces:

- **Recorder** — append-only event ingest: who did what, when, on what
  resource, and how state changed.
- **Minter** — short-lived embed tokens that let your frontend render audit-log
  views without exposing your API key.

Async on [tokio](https://tokio.rs), HTTP via [reqwest](https://docs.rs/reqwest).
An optional [axum](https://docs.rs/axum) `tower` middleware (behind the `axum`
cargo feature) installs a per-request event and auto-records on response finish.

## Install

```toml
[dependencies]
everscribe = "0.0.0"
# with the axum middleware:
everscribe = { version = "0.0.0", features = ["axum"] }
```

## Quickstart

_Documentation is being written as the SDK lands. See the Go, Node, and Python
SDK READMEs for the shape of the API in the meantime._
