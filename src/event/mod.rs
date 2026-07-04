//! Event domain: the on-the-wire event model, redaction, and request-context
//! helpers.
//!
//! Mirrors `sdk-go/pkg/event`, `sdk-node/src/event`, and
//! `sdk-python/src/everscribe/event`. The wire format (snake_case keys,
//! empty fields omitted) is byte-compatible with the sibling SDKs.

mod context;
mod redact;

pub use context::{client_ip, origin_from_headers, prepare, result_from_status};
pub use redact::apply_redaction;

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

/// Identifies who caused the event. `type` values are conventional, not
/// enforced: "user", "admin", "system", "api_key", "anonymous".
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Actor {
    // `type` is always present (no skip); other fields use omitempty. serde
    // serializes the raw identifier `r#type` as the JSON key "type".
    pub r#type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub display_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub email: String,
}

impl Actor {
    /// An actor with just a `type` set.
    pub fn new(r#type: impl Into<String>) -> Self {
        Actor {
            r#type: r#type.into(),
            ..Default::default()
        }
    }
}

/// What the event acted on. Both fields empty means "no target".
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Target {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub r#type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub id: String,
}

impl Target {
    pub fn new(r#type: impl Into<String>, id: impl Into<String>) -> Self {
        Target {
            r#type: r#type.into(),
            id: id.into(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.r#type.is_empty() && self.id.is_empty()
    }
}

/// Network/request context where the event was emitted.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Origin {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub ip: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user_agent: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub request_id: String,
}

impl Origin {
    pub fn is_empty(&self) -> bool {
        self.ip.is_empty() && self.user_agent.is_empty() && self.request_id.is_empty()
    }
}

/// Outcome of the audited action. The Rust analog of the other SDKs'
/// `Result`, renamed to avoid clashing with [`std::result::Result`]; it still
/// serializes to the wire field `result`.
///
/// `status`: "ok" | "error" | "denied" - empty means unrecorded.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Outcome {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub code: i32,
    #[serde(skip_serializing_if = "message_is_empty")]
    pub message: Option<Value>,
}

impl Outcome {
    pub fn is_empty(&self) -> bool {
        self.status.is_empty() && self.code == 0 && message_is_empty(&self.message)
    }
}

/// State transition for mutation events. `before`/`after` hold the (already
/// redacted, if applicable) wire-shape state on either side; the audit-log API
/// computes the JSON Patch on ingest. `patch` is optional and only set when
/// the caller supplies a precomputed RFC 6902 patch via [`Event::raw_diff`].
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Change {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<Value>,
}

/// The canonical audit record.
///
/// Construct via [`Event::new`] for non-HTTP callers; HTTP handlers get one
/// from the request context (see the `axum` feature). An empty `action` is a
/// no-op at record time.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Event {
    pub id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub tenant_id: String,
    #[serde(serialize_with = "serialize_rfc3339")]
    pub occurred_at: DateTime<Utc>,
    pub actor: Actor,
    pub action: String,
    #[serde(skip_serializing_if = "Target::is_empty")]
    pub target: Target,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "Origin::is_empty")]
    pub origin: Origin,
    #[serde(rename = "result", skip_serializing_if = "Outcome::is_empty")]
    pub outcome: Outcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change: Option<Change>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub idempotency_key: String,
}

impl Default for Event {
    fn default() -> Self {
        Event::new("")
    }
}

impl Event {
    /// A new event with `id` (uuid v4), `occurred_at` (now, UTC), and `action`
    /// populated; everything else empty.
    pub fn new(action: impl Into<String>) -> Self {
        Event {
            id: Uuid::new_v4().to_string(),
            tenant_id: String::new(),
            occurred_at: Utc::now(),
            actor: Actor::default(),
            action: action.into(),
            target: Target::default(),
            metadata: BTreeMap::new(),
            origin: Origin::default(),
            outcome: Outcome::default(),
            change: None,
            idempotency_key: String::new(),
        }
    }

    /// Set a single metadata key/value. Values that fail to serialize are
    /// silently skipped. Chainable.
    pub fn with_field(&mut self, key: impl Into<String>, value: impl Serialize) -> &mut Self {
        if let Ok(v) = serde_json::to_value(value) {
            self.metadata.insert(key.into(), v);
        }
        self
    }

    /// Set several metadata entries at once. Chainable.
    pub fn with_fields(&mut self, fields: impl IntoIterator<Item = (String, Value)>) -> &mut Self {
        self.metadata.extend(fields);
        self
    }

    /// Record a state transition. `before`/`after` are JSON-normalized and
    /// stored on the event; the audit-log API computes the patch on ingest.
    /// Serialization failures leave `change` unset. Chainable.
    pub fn diff(&mut self, before: &impl Serialize, after: &impl Serialize) -> &mut Self {
        self.diff_redacted(before, after, &[])
    }

    /// Like [`Event::diff`], but scrubs the values at the given JSON Pointer
    /// (RFC 6901) paths with `"[REDACTED]"` before storage. Use for fields
    /// that must not appear in audit logs. Chainable.
    pub fn diff_redacted(
        &mut self,
        before: &impl Serialize,
        after: &impl Serialize,
        redact_paths: &[&str],
    ) -> &mut Self {
        let (Ok(b), Ok(a)) = (serde_json::to_value(before), serde_json::to_value(after)) else {
            return self;
        };
        self.change = Some(Change {
            before: Some(apply_redaction(b, redact_paths)),
            after: Some(apply_redaction(a, redact_paths)),
            patch: None,
        });
        self
    }

    /// Escape hatch for callers that already have JSON-shaped before/after
    /// state, or a precomputed RFC 6902 patch. If all three are `None`, this
    /// is a no-op. Chainable.
    pub fn raw_diff(
        &mut self,
        before: Option<Value>,
        after: Option<Value>,
        patch: Option<Value>,
    ) -> &mut Self {
        if before.is_none() && after.is_none() && patch.is_none() {
            return self;
        }
        self.change = Some(Change {
            before,
            after,
            patch,
        });
        self
    }
}

// --- serde helpers ---------------------------------------------------------

fn is_zero(n: &i32) -> bool {
    *n == 0
}

fn message_is_empty(m: &Option<Value>) -> bool {
    match m {
        None => true,
        Some(Value::String(s)) => s.is_empty(),
        _ => false,
    }
}

/// Serialize a UTC timestamp as RFC 3339 with a `Z` suffix, matching the
/// sibling SDKs (e.g. `2026-07-01T12:00:00Z`).
fn serialize_rfc3339<S: Serializer>(dt: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}
