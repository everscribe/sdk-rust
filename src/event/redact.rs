//! JSON Pointer (RFC 6901) redaction for event diffs.
//!
//! Values at the given pointer paths are replaced with the string
//! `"[REDACTED]"`.

use serde_json::Value;

const REDACTED: &str = "[REDACTED]";

/// Redact the given JSON Pointer paths in `value`, returning the result.
/// Paths that don't exist in the document are silently skipped.
pub fn apply_redaction(mut value: Value, paths: &[&str]) -> Value {
    for p in paths {
        redact_path(&mut value, p);
    }
    value
}

fn redact_path(doc: &mut Value, pointer: &str) {
    if pointer.is_empty() {
        *doc = Value::String(REDACTED.to_string());
        return;
    }
    if !pointer.starts_with('/') {
        return;
    }
    let tokens = split_pointer(&pointer[1..]);
    redact_tokens(doc, &tokens);
}

fn redact_tokens(node: &mut Value, tokens: &[String]) {
    let Some((head, rest)) = tokens.split_first() else {
        *node = Value::String(REDACTED.to_string());
        return;
    };
    match node {
        Value::Object(map) => {
            if let Some(child) = map.get_mut(head) {
                redact_tokens(child, rest);
            }
        }
        Value::Array(arr) => {
            if let Some(idx) = parse_uint(head) {
                if idx < arr.len() {
                    redact_tokens(&mut arr[idx], rest);
                }
            }
        }
        _ => {}
    }
}

/// Split an RFC 6901 reference body by '/' and unescape the standard
/// "~1" -> "/" and "~0" -> "~" sequences.
fn split_pointer(body: &str) -> Vec<String> {
    body.split('/')
        .map(|p| p.replace("~1", "/").replace("~0", "~"))
        .collect()
}

/// Parse a base-10 unsigned integer for a JSON Pointer array index. Returns
/// `None` on empty input or any non-ASCII-digit character.
fn parse_uint(s: &str) -> Option<usize> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}
