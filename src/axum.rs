//! axum / tower middleware adapter (behind the `axum` cargo feature).
//!
//! Mirrors the Go `net/http` middleware, the Node Express adapter, and the
//! Python ASGI middleware: installs a per-request event (actor + origin) and
//! auto-records it on response finish. Populated in a later build step.
