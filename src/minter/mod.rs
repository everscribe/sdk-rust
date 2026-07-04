//! Minter: short-lived embed tokens for frontend audit-log views.
//!
//! Mirrors `sdk-go/pkg/minter`, `sdk-node/src/minter`, and
//! `sdk-python/src/everscribe/minter`. An embed token is a signed, short-lived
//! JWT your backend mints (with the project API key) and forwards to your
//! frontend, which passes it to the Everscribe embed component.

mod columns;
mod error;
mod options;

pub use columns::ALLOWED_COLUMNS;
pub use error::MinterError;
pub use options::{TokenOptions, MAX_EXPIRES_IN, MIN_EXPIRES_IN};

use std::time::Duration;

use serde::Deserialize;

/// The production API host.
pub const DEFAULT_BASE_URL: &str = "https://api.everscribe.io";
/// Default per-request HTTP timeout.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration for [`Client::new`].
#[derive(Clone, Debug)]
pub struct MinterOptions {
    pub base_url: String,
    pub request_timeout: Duration,
}

impl Default for MinterOptions {
    fn default() -> Self {
        MinterOptions {
            base_url: DEFAULT_BASE_URL.to_string(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }
}

/// Mints embed tokens for a single project. Reuse for the lifetime of the
/// process - cheap to clone and safe for concurrent use.
#[derive(Clone)]
pub struct Client {
    base_url: String,
    project_id: String,
    api_key: String,
    http: reqwest::Client,
}

impl Client {
    /// Construct a minter client bound to `project_id`.
    pub fn new(
        project_id: impl Into<String>,
        api_key: impl Into<String>,
        opts: MinterOptions,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(opts.request_timeout)
            .build()
            .expect("failed to build reqwest client");
        Client {
            base_url: opts.base_url.trim_end_matches('/').to_string(),
            project_id: project_id.into(),
            api_key: api_key.into(),
            http,
        }
    }

    /// Request a new embed token and return the JWT string. Validates `opts`
    /// client-side first (returning [`MinterError::Validation`] with no HTTP
    /// call); non-2xx responses return [`MinterError::Http`].
    pub async fn mint_token(&self, opts: &TokenOptions) -> Result<String, MinterError> {
        let body = opts.to_wire()?;
        let url = format!(
            "{}/v1/projects/{}/embed-tokens",
            self.base_url, self.project_id
        );
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if status.is_success() {
            let parsed: TokenResponse = resp
                .json()
                .await
                .map_err(|e| MinterError::Decode(e.to_string()))?;
            return Ok(parsed.token);
        }

        let text = resp.text().await.unwrap_or_default();
        Err(MinterError::Http {
            status: status.as_u16(),
            body: text.trim().to_string(),
        })
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    token: String,
}
