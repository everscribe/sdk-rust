//! Everscribe Rust SDK.
//!
//! Rust SDK for the Everscribe audit-log API, built to parity with the Go,
//! Node, and Python SDKs. Two coordinated surfaces:
//!
//! - [`recorder`] — append-only event ingest.
//! - [`minter`] — short-lived embed tokens for frontend audit views.
//!
//! Bind credentials once with `new` (or `new_from_env`) and create per-surface
//! subclients from the returned `Client`. The root client and top-level
//! exports are assembled in a later build step.

pub mod event;
pub mod minter;
pub mod recorder;

#[cfg(feature = "axum")]
pub mod axum;
