//! HTTP recorder: posts events to the audit-log ingestion API via reqwest.
//!
//! Wire format:
//!
//! ```text
//! POST {base_url}/v1/projects/{project_id}/events
//!     body: a single Event JSON object
//! POST {base_url}/v1/projects/{project_id}/events/batch
//!     body: {"events": [Event, Event, ...]}
//! Authorization: Bearer {api_key}
//! ```

use std::time::Duration;

use serde::Serialize;

use super::error::{HttpError, RecordError};
use super::{BatchRecorder, Recorder, RecorderOptions};
use crate::event::{prepare, Event};

pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

const MAX_ERROR_BODY: usize = 4096;

/// Posts events to the audit-log ingestion API for one project, authenticating
/// with `api_key` as a bearer token.
#[derive(Clone)]
pub struct HttpRecorder {
    base_url: String,
    project_id: String,
    api_key: String,
    client: reqwest::Client,
    auto_idempotency_key: bool,
}

impl HttpRecorder {
    /// Construct from [`RecorderOptions`] (uses only the HTTP-related fields).
    pub fn new(
        project_id: impl Into<String>,
        api_key: impl Into<String>,
        opts: &RecorderOptions,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(opts.request_timeout)
            .build()
            .expect("failed to build reqwest client");
        HttpRecorder {
            base_url: opts.base_url.trim_end_matches('/').to_string(),
            project_id: project_id.into(),
            api_key: api_key.into(),
            client,
            auto_idempotency_key: opts.auto_idempotency_key,
        }
    }

    /// POST a single event. An empty `action` is a no-op.
    pub async fn record(&self, event: Event) -> Result<(), RecordError> {
        if event.action.is_empty() {
            return Ok(());
        }
        let mut event = event;
        prepare(&mut event);
        self.finalize(&mut event);
        let url = format!("{}/v1/projects/{}/events", self.base_url, self.project_id);
        self.post(&url, &event).await
    }

    /// POST multiple events in one request. Empty-`action` events are filtered
    /// out; an empty batch is a no-op.
    pub async fn record_batch(&self, events: Vec<Event>) -> Result<(), RecordError> {
        let prepared: Vec<Event> = events
            .into_iter()
            .filter(|e| !e.action.is_empty())
            .map(|mut e| {
                prepare(&mut e);
                self.finalize(&mut e);
                e
            })
            .collect();
        if prepared.is_empty() {
            return Ok(());
        }
        let url = format!(
            "{}/v1/projects/{}/events/batch",
            self.base_url, self.project_id
        );
        self.post(&url, &BatchBody { events: &prepared }).await
    }

    fn finalize(&self, e: &mut Event) {
        if self.auto_idempotency_key && e.idempotency_key.is_empty() {
            e.idempotency_key = e.id.clone();
        }
    }

    async fn post<T: Serialize>(&self, url: &str, body: &T) -> Result<(), RecordError> {
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let text = resp.text().await.unwrap_or_default();
        let body: String = text.chars().take(MAX_ERROR_BODY).collect();
        Err(RecordError::Http(HttpError {
            status: status.as_u16(),
            body: body.trim().to_string(),
        }))
    }
}

#[derive(Serialize)]
struct BatchBody<'a> {
    events: &'a [Event],
}

impl Recorder for HttpRecorder {
    async fn record(&self, event: Event) -> Result<(), RecordError> {
        HttpRecorder::record(self, event).await
    }
}

impl BatchRecorder for HttpRecorder {
    async fn record_batch(&self, events: Vec<Event>) -> Result<(), RecordError> {
        HttpRecorder::record_batch(self, events).await
    }
}
