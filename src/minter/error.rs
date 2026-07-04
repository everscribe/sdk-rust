//! Minter error type.

/// Errors from minting an embed token.
#[derive(Debug, thiserror::Error)]
pub enum MinterError {
    /// Client-side validation failed before any HTTP call was made.
    #[error("minter: {0}")]
    Validation(String),
    /// A non-2xx response from the mint endpoint. Typical codes: 400 (invalid
    /// options), 401 (bad auth), 404 (missing/soft-deleted project).
    #[error("minter: http {status}: {body}")]
    Http { status: u16, body: String },
    /// A transport-level failure (connection refused, TLS, etc.).
    #[error("minter: request failed: {0}")]
    Transport(#[from] reqwest::Error),
    /// The 2xx response body was not the expected `{"token": "..."}` shape.
    #[error("minter: invalid response: {0}")]
    Decode(String),
}
