//! Tests for the runtime-agnostic context helpers.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use everscribe::event::{
    client_ip, origin_from_headers, prepare, result_from_status, Event, Origin,
};

/// Build a case-insensitive header lookup closure from lowercase-keyed pairs.
fn headers(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.to_string()))
        .collect();
    move |name: &str| map.get(&name.to_lowercase()).cloned()
}

#[test]
fn prepare_fills_missing_id() {
    let mut e = Event::new("x");
    e.id = String::new();
    prepare(&mut e);
    assert!(!e.id.is_empty());
}

#[test]
fn prepare_fills_epoch_occurred_at() {
    let mut e = Event::new("x");
    e.occurred_at = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
    prepare(&mut e);
    assert!(e.occurred_at.timestamp() > 0);
}

#[test]
fn prepare_leaves_set_fields() {
    let mut e = Event::new("x");
    let id = e.id.clone();
    let ts = e.occurred_at;
    prepare(&mut e);
    assert_eq!(e.id, id);
    assert_eq!(e.occurred_at, ts);
}

#[test]
fn status_ok_range() {
    for code in [200u16, 201, 302, 399] {
        let o = result_from_status(code);
        assert_eq!(o.status, "ok");
        assert_eq!(o.code, i32::from(code));
    }
}

#[test]
fn status_denied() {
    for code in [401u16, 403] {
        let o = result_from_status(code);
        assert_eq!(o.status, "denied");
        assert_eq!(o.code, i32::from(code));
    }
}

#[test]
fn status_error() {
    let o = result_from_status(500);
    assert_eq!(o.status, "error");
    assert_eq!(o.code, 500);
}

#[test]
fn status_zero_no_response() {
    let o = result_from_status(0);
    assert_eq!(o.status, "error");
    assert_eq!(o.code, 0);
    assert_eq!(o.message, Some(serde_json::json!("no response written")));
}

#[test]
fn origin_from_headers_and_remote() {
    let o = origin_from_headers(
        headers(&[("User-Agent", "curl/8"), ("X-Request-ID", "req-1")]),
        "9.9.9.9",
    );
    assert_eq!(
        o,
        Origin {
            ip: "9.9.9.9".into(),
            user_agent: "curl/8".into(),
            request_id: "req-1".into(),
        }
    );
}

#[test]
fn origin_empty_when_no_data() {
    assert_eq!(origin_from_headers(headers(&[]), ""), Origin::default());
}

#[test]
fn client_ip_prefers_xff_first_entry() {
    let ip = client_ip(
        headers(&[("X-Forwarded-For", "1.1.1.1, 2.2.2.2")]),
        "9.9.9.9",
    );
    assert_eq!(ip, "1.1.1.1");
}

#[test]
fn client_ip_falls_back_to_x_real_ip() {
    assert_eq!(
        client_ip(headers(&[("X-Real-IP", "5.5.5.5")]), "9.9.9.9"),
        "5.5.5.5"
    );
}

#[test]
fn client_ip_falls_back_to_remote_addr_stripping_port() {
    assert_eq!(client_ip(headers(&[]), "6.6.6.6:54321"), "6.6.6.6");
    assert_eq!(client_ip(headers(&[]), "6.6.6.6"), "6.6.6.6");
}

#[test]
fn client_ip_empty_when_nothing() {
    assert_eq!(client_ip(headers(&[]), ""), "");
}

#[test]
fn header_lookup_is_case_insensitive() {
    assert_eq!(
        client_ip(headers(&[("X-REAL-IP", "7.7.7.7")]), ""),
        "7.7.7.7"
    );
}
