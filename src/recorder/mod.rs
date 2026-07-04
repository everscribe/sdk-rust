//! Recorder: append-only event ingest over HTTP with async buffering.
//!
//! Mirrors `sdk-go/pkg/recorder`, `sdk-node/src/recorder`, and
//! `sdk-python/src/everscribe/recorder`. HTTP via reqwest; the buffered
//! recorder flushes batches on a background tokio task. Populated in a later
//! build step.
