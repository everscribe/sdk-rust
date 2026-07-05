//! Recorder: append-only event ingest over HTTP with async buffering.
//!
//! HTTP via reqwest; the buffered
//! recorder flushes batches on a background tokio task. [`new`] is the
//! recommended entry point.

mod buffered;
mod error;
mod http;

pub use buffered::{BufferedRecorder, BufferedStats, OverflowPolicy};
pub use error::{DrainTimeout, HttpError, RecordError};
pub use http::{HttpRecorder, DEFAULT_REQUEST_TIMEOUT};

use std::future::Future;
use std::time::Duration;

use crate::event::Event;

/// The production ingestion endpoint.
pub const DEFAULT_BASE_URL: &str = "https://api.everscribe.io";

/// Records audit events. Implemented by both [`HttpRecorder`] and
/// [`BufferedRecorder`]. An empty `action` is a no-op.
pub trait Recorder {
    fn record(&self, event: Event) -> impl Future<Output = Result<(), RecordError>> + Send;
}

/// Optional capability for recorders that persist multiple events in one call.
/// [`BufferedRecorder`] uses it to flush batches.
pub trait BatchRecorder: Send + Sync + 'static {
    fn record_batch(
        &self,
        events: Vec<Event>,
    ) -> impl Future<Output = Result<(), RecordError>> + Send;
}

/// Configuration for [`new`]. Construct with `..Default::default()`:
///
/// ```no_run
/// use everscribe::recorder::{RecorderOptions, OverflowPolicy};
/// let opts = RecorderOptions { buffer_size: 2000, overflow: OverflowPolicy::Block, ..Default::default() };
/// ```
#[derive(Clone, Debug)]
pub struct RecorderOptions {
    /// Ingestion endpoint. Override for tests and staging.
    pub base_url: String,
    /// Per-request HTTP timeout.
    pub request_timeout: Duration,
    /// Copy `event.id` into `event.idempotency_key` at send time when empty.
    pub auto_idempotency_key: bool,
    /// Capacity of the in-memory event buffer.
    pub buffer_size: usize,
    /// Event count that triggers an immediate flush.
    pub flush_size: usize,
    /// Max time between flushes when the size threshold isn't reached.
    pub flush_interval: Duration,
    /// Per-flush timeout for the batch call to the inner recorder.
    pub flush_timeout: Duration,
    /// Behavior when the buffer is full.
    pub overflow: OverflowPolicy,
    /// Max time `close()` waits for in-flight events to flush.
    pub drain_timeout: Duration,
}

impl Default for RecorderOptions {
    fn default() -> Self {
        RecorderOptions {
            base_url: DEFAULT_BASE_URL.to_string(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            auto_idempotency_key: false,
            buffer_size: 1000,
            flush_size: 100,
            flush_interval: Duration::from_secs(5),
            flush_timeout: Duration::from_secs(30),
            overflow: OverflowPolicy::DropNewest,
            drain_timeout: Duration::from_secs(30),
        }
    }
}

/// Return a [`BufferedRecorder`] wrapping an [`HttpRecorder`] - the recommended
/// way to construct a recorder. Must be called within a tokio runtime.
pub fn new(
    project_id: impl Into<String>,
    api_key: impl Into<String>,
    opts: RecorderOptions,
) -> BufferedRecorder {
    let inner = HttpRecorder::new(project_id, api_key, &opts);
    BufferedRecorder::new(inner, &opts)
}
