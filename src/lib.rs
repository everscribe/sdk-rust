//! Everscribe Rust SDK.
//!
//! Rust SDK for the Everscribe audit-log API. Two coordinated surfaces:
//!
//! - [`recorder`] - append-only event ingest.
//! - [`minter`] - short-lived embed tokens for frontend audit views.
//!
//! Bind credentials once with [`new`] (or [`new_from_env`]) and create
//! per-surface subclients from the returned [`Client`]:
//!
//! ```no_run
//! # async fn f() -> Result<(), Box<dyn std::error::Error>> {
//! let es = everscribe::new("proj_...", "es_live_...")?;
//! let rec = es.new_recorder(Default::default());
//! rec.record(everscribe::Event::new("user.login")).await.ok();
//! # Ok(()) }
//! ```
//!
//! Customers who only need one surface can call its constructor directly -
//! [`recorder::new`] and [`minter::Client::new`] both work and skip the client
//! step. An optional axum middleware lives in [`axum`] (enable the `axum`
//! feature).

pub mod event;
pub mod minter;
pub mod recorder;

#[cfg(feature = "axum")]
pub mod axum;

pub use event::Event;

use minter::MinterOptions;
use recorder::{BufferedRecorder, RecorderOptions};

/// Environment variable read by [`new_from_env`] for the project ID.
pub const EVERSCRIBE_PROJECT_ID: &str = "EVERSCRIBE_PROJECT_ID";
/// Environment variable read by [`new_from_env`] for the API key.
pub const EVERSCRIBE_API_KEY: &str = "EVERSCRIBE_API_KEY";

/// Errors constructing a [`Client`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("everscribe: project_id is empty")]
    EmptyProjectId,
    #[error("everscribe: api_key is empty")]
    EmptyApiKey,
    #[error("everscribe: {0} is not set or empty")]
    MissingEnv(&'static str),
}

/// A credential-bearing handle to an Everscribe project. Holds no network
/// state itself; the subclient constructors build per-surface clients. Reuse a
/// single client for the lifetime of the process.
#[derive(Clone, Debug)]
pub struct Client {
    project_id: String,
    api_key: String,
}

impl Client {
    /// Construct a client, trimming credentials. Returns [`ConfigError`] if
    /// either is empty or whitespace-only, so configuration bugs surface at
    /// construction rather than at the first network call.
    pub fn new(
        project_id: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Result<Self, ConfigError> {
        let project_id = project_id.into().trim().to_string();
        if project_id.is_empty() {
            return Err(ConfigError::EmptyProjectId);
        }
        let api_key = api_key.into().trim().to_string();
        if api_key.is_empty() {
            return Err(ConfigError::EmptyApiKey);
        }
        Ok(Client {
            project_id,
            api_key,
        })
    }

    /// Construct a client from `EVERSCRIBE_PROJECT_ID` and `EVERSCRIBE_API_KEY`.
    /// Returns [`ConfigError::MissingEnv`] naming the missing variable if
    /// either is unset or empty after trimming.
    pub fn from_env() -> Result<Self, ConfigError> {
        let project_id = std::env::var(EVERSCRIBE_PROJECT_ID).unwrap_or_default();
        if project_id.trim().is_empty() {
            return Err(ConfigError::MissingEnv(EVERSCRIBE_PROJECT_ID));
        }
        let api_key = std::env::var(EVERSCRIBE_API_KEY).unwrap_or_default();
        if api_key.trim().is_empty() {
            return Err(ConfigError::MissingEnv(EVERSCRIBE_API_KEY));
        }
        Client::new(project_id, api_key)
    }

    /// The bound project ID.
    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    /// A buffered recorder for the bound project. Must be called within a tokio
    /// runtime.
    pub fn new_recorder(&self, opts: RecorderOptions) -> BufferedRecorder {
        recorder::new(self.project_id.clone(), self.api_key.clone(), opts)
    }

    /// A minter client for the bound project.
    pub fn new_minter(&self, opts: MinterOptions) -> minter::Client {
        minter::Client::new(self.project_id.clone(), self.api_key.clone(), opts)
    }
}

/// Construct a [`Client`]. See [`Client::new`].
pub fn new(
    project_id: impl Into<String>,
    api_key: impl Into<String>,
) -> Result<Client, ConfigError> {
    Client::new(project_id, api_key)
}

/// Construct a [`Client`] from environment variables. See [`Client::from_env`].
pub fn new_from_env() -> Result<Client, ConfigError> {
    Client::from_env()
}
