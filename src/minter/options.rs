//! Token mint options and their client-side validation.
//!
//! Validation runs before any HTTP call; failures
//! return [`MinterError::Validation`].

use std::time::Duration;

use serde_json::{json, Map, Value};

use super::columns::ALLOWED_COLUMNS;
use super::error::MinterError;

/// Lower bound the server enforces on `expires_in`.
pub const MIN_EXPIRES_IN: Duration = Duration::from_secs(60);
/// Upper bound the server enforces on `expires_in`.
pub const MAX_EXPIRES_IN: Duration = Duration::from_secs(24 * 60 * 60);

/// Configures a token mint request. The default mints a 1-hour, full-project,
/// read-only token.
///
/// The `allowed_*` fields use `None` for "no restriction" and reject an empty
/// `Vec`, so building a list from filtered user input can never silently widen
/// scope to "everything".
#[derive(Clone, Debug, Default)]
pub struct TokenOptions {
    /// Scopes reads to events with a matching `tenant_id`. Trimmed; rejected if
    /// empty after trim or longer than 256 characters. Empty string = unscoped.
    pub tenant_id: String,
    /// Token lifetime. The server clamps to [`MIN_EXPIRES_IN`, `MAX_EXPIRES_IN`].
    /// `Duration::ZERO` uses the server default (1 hour).
    pub expires_in: Duration,
    /// Whitelist of Event field names. `None` = no restriction; empty rejected.
    pub allowed_columns: Option<Vec<String>>,
    /// Allowed actions - exact ("user.login") or suffix wildcard ("user.*").
    /// `None` = no restriction; empty rejected.
    pub allowed_actions: Option<Vec<String>>,
    /// Restricts catalog fields the token's DSL/NLP queries may reference.
    /// `None` = no restriction; empty rejected. Validated server-side.
    pub allowed_fields: Option<Vec<String>>,
    /// Unlock the Query (advanced DSL) tab and `?q=` on the read API.
    pub allow_dsl_input: bool,
    /// Unlock the AI ("Ask in plain English") tab and the NLP endpoint.
    pub allow_nlp: bool,
}

impl TokenOptions {
    /// Validate the options and return the wire-shape request body.
    pub fn to_wire(&self) -> Result<Value, MinterError> {
        let mut w = Map::new();

        if !self.tenant_id.is_empty() {
            let t = self.tenant_id.trim();
            if t.is_empty() {
                return Err(invalid("tenant_id is empty after trim"));
            }
            if t.chars().count() > 256 {
                return Err(invalid("tenant_id exceeds 256 chars"));
            }
            w.insert("tenant_id".into(), json!(t));
        }

        if !self.expires_in.is_zero() {
            if self.expires_in < MIN_EXPIRES_IN {
                return Err(invalid(format!(
                    "expires_in {:?} is below minimum {:?}",
                    self.expires_in, MIN_EXPIRES_IN
                )));
            }
            if self.expires_in > MAX_EXPIRES_IN {
                return Err(invalid(format!(
                    "expires_in {:?} is above maximum {:?}",
                    self.expires_in, MAX_EXPIRES_IN
                )));
            }
            w.insert("expires_in".into(), json!(self.expires_in.as_secs()));
        }

        if let Some(cols) = &self.allowed_columns {
            if cols.is_empty() {
                return Err(invalid(
                    "allowed_columns is empty; use None for no restriction",
                ));
            }
            for c in cols {
                if !ALLOWED_COLUMNS.contains(&c.as_str()) {
                    return Err(invalid(format!("unknown column name {c:?}")));
                }
            }
            w.insert("columns".into(), json!(cols));
        }

        if let Some(actions) = &self.allowed_actions {
            if actions.is_empty() {
                return Err(invalid(
                    "allowed_actions is empty; use None for no restriction",
                ));
            }
            for a in actions {
                if !valid_action(a) {
                    return Err(invalid(format!(
                        "action entry {a:?} does not match grammar \
                         [a-zA-Z0-9_]+(\\.[a-zA-Z0-9_]+)*(\\.\\*)?"
                    )));
                }
            }
            w.insert("actions".into(), json!(actions));
        }

        if let Some(fields) = &self.allowed_fields {
            if fields.is_empty() {
                return Err(invalid(
                    "allowed_fields is empty; use None for no restriction",
                ));
            }
            // Field validation lives on the server; ship entries verbatim.
            w.insert("allowed_fields".into(), json!(fields));
        }

        if self.allow_dsl_input {
            w.insert("allow_dsl_input".into(), json!(true));
        }
        if self.allow_nlp {
            w.insert("allow_nlp".into(), json!(true));
        }

        Ok(Value::Object(w))
    }
}

fn invalid(msg: impl Into<String>) -> MinterError {
    MinterError::Validation(msg.into())
}

/// Matches `^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*(\.\*)?$`: dot-separated
/// alphanumeric/underscore segments with an optional trailing `.*`.
fn valid_action(a: &str) -> bool {
    if a.is_empty() {
        return false;
    }
    let segs: Vec<&str> = a.split('.').collect();
    let last = segs.len() - 1;
    for (i, seg) in segs.iter().enumerate() {
        if i == last && *seg == "*" {
            // A bare "*" (no preceding segment) is rejected.
            return segs.len() >= 2;
        }
        if seg.is_empty() || !seg.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return false;
        }
    }
    true
}
