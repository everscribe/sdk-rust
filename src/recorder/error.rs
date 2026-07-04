//! Recorder error types.

/// Returned when the ingestion endpoint responds with a non-2xx status.
/// Inspect [`HttpError::transient`] to distinguish retryable failures (5xx,
/// 429) from permanent ones (4xx).
#[derive(Debug, thiserror::Error)]
#[error("recorder: http {status}: {body}")]
pub struct HttpError {
    pub status: u16,
    pub body: String,
}

impl HttpError {
    /// Whether the error is likely to resolve on retry (5xx server errors and
    /// 429 rate limits).
    pub fn transient(&self) -> bool {
        self.status >= 500 || self.status == 429
    }
}

/// The error type returned by recording operations.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    /// A non-2xx response from the ingestion endpoint.
    #[error(transparent)]
    Http(#[from] HttpError),
    /// A transport-level failure (connection refused, TLS, etc.).
    #[error("recorder: request failed: {0}")]
    Transport(#[from] reqwest::Error),
    /// A flush exceeded its timeout.
    #[error("recorder: flush timed out")]
    Timeout,
    /// The buffer was full and the overflow policy is
    /// [`OverflowPolicy::Error`](crate::recorder::OverflowPolicy::Error).
    #[error("recorder: buffer full")]
    BufferFull,
}

impl RecordError {
    /// Whether the error is likely to resolve on retry.
    pub fn transient(&self) -> bool {
        match self {
            RecordError::Http(e) => e.transient(),
            RecordError::Transport(_) | RecordError::Timeout => true,
            RecordError::BufferFull => false,
        }
    }
}

/// Returned by [`BufferedRecorder::close`](crate::recorder::BufferedRecorder::close)
/// when the drain timeout elapses with events still pending.
#[derive(Debug, thiserror::Error)]
#[error("recorder: drain timed out with events pending")]
pub struct DrainTimeout;
