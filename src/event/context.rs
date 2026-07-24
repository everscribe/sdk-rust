//! Runtime-agnostic request-context helpers: default population, the
//! HTTP-status -> [`Outcome`] mapping, and header-based origin extraction.
//!
//! These are the pieces a framework adapter (e.g. the `axum` feature) builds
//! on. Header access is passed as a closure so the core stays free of any HTTP
//! crate dependency; the closure should perform case-insensitive lookups.

use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use super::{Event, Origin, Outcome};

/// Fill defaults on `e`: `id` if empty, and `occurred_at` if it's the Unix
/// epoch (the zero value). Recorder implementations call this before sending.
pub fn prepare(e: &mut Event) {
    if e.id.is_empty() {
        e.id = Uuid::new_v4().to_string();
    }
    if e.occurred_at == DateTime::<Utc>::from_timestamp(0, 0).expect("epoch is valid") {
        e.occurred_at = Utc::now();
    }
}

/// Derive an [`Outcome`] from an HTTP status code. Opt-in: core never calls
/// this implicitly, since a bare status number cannot serve every transport
/// (gRPC's OK status is code 0, colliding with the "nothing written yet"
/// sentinel an integer would otherwise need). An HTTP-shaped adapter calls
/// this explicitly - typically to build the [`Outcome`] it hands to
/// [`crate::event::end`] once its response is final - rather than each
/// adapter carrying its own copy of the table. Named to match `sdk-go`'s
/// `ResultFromHTTPStatus` and `sdk-node`'s `resultFromHttpStatus`; the
/// `outcome_` prefix (not `result_`) matches this SDK's own type name,
/// [`Outcome`], chosen so as not to clash with [`std::result::Result`].
///
/// Status `0` (no response written) maps to an error - typically an early
/// return or panic before any response.
pub fn outcome_from_http_status(status: u16) -> Outcome {
    if status == 0 {
        return Outcome {
            status: "error".to_string(),
            code: 0,
            message: Some(Value::String("no response written".to_string())),
        };
    }
    let s = match status {
        200..=399 => "ok",
        401 | 403 => "denied",
        _ => "error",
    };
    Outcome {
        status: s.to_string(),
        code: i32::from(status),
        message: None,
    }
}

/// Extract network context from request headers and a remote address.
/// Respects `X-Forwarded-For` (first entry) and `X-Real-IP` before falling
/// back to `remote_addr`. `get` should look up a header case-insensitively.
pub fn origin_from_headers<F>(get: F, remote_addr: &str) -> Origin
where
    F: Fn(&str) -> Option<String>,
{
    Origin {
        ip: client_ip(&get, remote_addr),
        user_agent: header(&get, "user-agent"),
        request_id: header(&get, "x-request-id"),
    }
}

/// Extract the client IP from `X-Forwarded-For` (first entry), then
/// `X-Real-IP`, then `remote_addr` (with a trailing `:port` stripped).
pub fn client_ip<F>(get: F, remote_addr: &str) -> String
where
    F: Fn(&str) -> Option<String>,
{
    if let Some(xff) = header_nonempty(&get, "x-forwarded-for") {
        let first = xff.split(',').next().unwrap_or("").trim();
        return first.to_string();
    }
    if let Some(xri) = header_nonempty(&get, "x-real-ip") {
        return xri;
    }
    if remote_addr.is_empty() {
        return String::new();
    }
    match remote_addr.rfind(':') {
        Some(i) => remote_addr[..i].to_string(),
        None => remote_addr.to_string(),
    }
}

fn header<F: Fn(&str) -> Option<String>>(get: &F, name: &str) -> String {
    get(name).unwrap_or_default()
}

fn header_nonempty<F: Fn(&str) -> Option<String>>(get: &F, name: &str) -> Option<String> {
    get(name).filter(|s| !s.is_empty())
}
