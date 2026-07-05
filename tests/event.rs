//! Tests for the event domain: construction, builders, redaction, and the
//! on-the-wire serialization format (snake_case keys, empty fields omitted).

use chrono::{TimeZone, Utc};
use everscribe::event::{apply_redaction, Actor, Change, Event, Origin, Outcome, Target};
use serde_json::json;

/// Pin id/occurred_at so wire output is deterministic.
fn fixed(mut e: Event) -> Event {
    e.id = "11111111-1111-4111-8111-111111111111".to_string();
    e.occurred_at = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    e
}

fn wire(e: &Event) -> serde_json::Value {
    serde_json::to_value(e).unwrap()
}

#[test]
fn new_event_populates_id_action_defaults() {
    let e = Event::new("user.login");
    assert_eq!(e.action, "user.login");
    assert_eq!(uuid::Uuid::parse_str(&e.id).unwrap().get_version_num(), 4);
    assert_eq!(e.actor, Actor::default());
    assert!(e.target.is_empty());
    assert!(e.origin.is_empty());
    assert!(e.outcome.is_empty());
    assert!(e.metadata.is_empty());
    assert!(e.change.is_none());
}

#[test]
fn with_field_and_fields() {
    let mut e = Event::new("x");
    e.with_field("a", 1).with_field("b", "two");
    e.with_fields([("c".to_string(), json!(true))]);
    assert_eq!(e.metadata["a"], json!(1));
    assert_eq!(e.metadata["b"], json!("two"));
    assert_eq!(e.metadata["c"], json!(true));
}

#[test]
fn wire_minimal_event() {
    let e = fixed(Event::new("user.login"));
    assert_eq!(
        wire(&e),
        json!({
            "id": "11111111-1111-4111-8111-111111111111",
            "occurred_at": "2026-07-01T12:00:00Z",
            "actor": { "type": "" },
            "action": "user.login",
        })
    );
}

#[test]
fn wire_occurred_at_has_z_and_subseconds() {
    let mut e = fixed(Event::new("x"));
    e.occurred_at = Utc
        .with_ymd_and_hms(2026, 7, 1, 12, 0, 0)
        .unwrap()
        .with_timezone(&Utc)
        + chrono::Duration::milliseconds(123);
    assert_eq!(wire(&e)["occurred_at"], json!("2026-07-01T12:00:00.123Z"));
}

#[test]
fn wire_full_event_snake_case_and_omitempty() {
    let mut e = fixed(Event::new("api_key.revoke"));
    e.tenant_id = "tenant-42".into();
    e.actor = Actor {
        r#type: "user".into(),
        id: "u1".into(),
        display_name: "Ada".into(),
        email: "ada@x.io".into(),
    };
    e.target = Target::new("api_key", "k1");
    e.with_field("reason", "leaked");
    e.origin = Origin {
        ip: "1.2.3.4".into(),
        user_agent: "curl/8".into(),
        request_id: "req-9".into(),
    };
    e.outcome = Outcome {
        status: "ok".into(),
        code: 200,
        message: None,
    };
    e.idempotency_key = "evt-1".into();

    assert_eq!(
        wire(&e),
        json!({
            "id": "11111111-1111-4111-8111-111111111111",
            "tenant_id": "tenant-42",
            "occurred_at": "2026-07-01T12:00:00Z",
            "actor": { "type": "user", "id": "u1", "display_name": "Ada", "email": "ada@x.io" },
            "action": "api_key.revoke",
            "target": { "type": "api_key", "id": "k1" },
            "metadata": { "reason": "leaked" },
            "origin": { "ip": "1.2.3.4", "user_agent": "curl/8", "request_id": "req-9" },
            "result": { "status": "ok", "code": 200 },
            "idempotency_key": "evt-1",
        })
    );
}

#[test]
fn wire_omits_empty_nested_objects() {
    let w = wire(&fixed(Event::new("x")));
    let obj = w.as_object().unwrap();
    for k in [
        "tenant_id",
        "target",
        "metadata",
        "origin",
        "result",
        "change",
        "idempotency_key",
    ] {
        assert!(!obj.contains_key(k), "{k} should be omitted");
    }
}

#[test]
fn outcome_code_zero_omitted_but_status_kept() {
    let mut e = fixed(Event::new("x"));
    e.outcome = Outcome {
        status: "ok".into(),
        code: 0,
        message: None,
    };
    assert_eq!(wire(&e)["result"], json!({ "status": "ok" }));
}

#[test]
fn outcome_empty_string_message_omitted() {
    let mut e = fixed(Event::new("x"));
    e.outcome = Outcome {
        status: "ok".into(),
        code: 0,
        message: Some(json!("")),
    };
    assert_eq!(wire(&e)["result"], json!({ "status": "ok" }));
}

#[test]
fn outcome_message_preserved() {
    let mut e = fixed(Event::new("x"));
    e.outcome = Outcome {
        status: "error".into(),
        code: 500,
        message: Some(json!("boom")),
    };
    assert_eq!(
        wire(&e)["result"],
        json!({ "status": "error", "code": 500, "message": "boom" })
    );
}

#[test]
fn diff_sets_change() {
    let mut e = fixed(Event::new("user.update"));
    e.diff(&json!({ "name": "old" }), &json!({ "name": "new" }));
    assert_eq!(
        wire(&e)["change"],
        json!({ "before": { "name": "old" }, "after": { "name": "new" } })
    );
}

#[test]
fn diff_redacts_fields() {
    let mut e = fixed(Event::new("user.update"));
    e.diff_redacted(
        &json!({ "email": "a@x.io", "password": "hunter2" }),
        &json!({ "email": "b@x.io", "password": "hunter3" }),
        &["/password"],
    );
    let change = &wire(&e)["change"];
    assert_eq!(
        change["before"],
        json!({ "email": "a@x.io", "password": "[REDACTED]" })
    );
    assert_eq!(
        change["after"],
        json!({ "email": "b@x.io", "password": "[REDACTED]" })
    );
}

#[test]
fn diff_of_null_records_explicit_null() {
    let mut e = fixed(Event::new("x"));
    e.diff(&serde_json::Value::Null, &json!({ "a": 1 }));
    assert_eq!(
        wire(&e)["change"],
        json!({ "before": null, "after": { "a": 1 } })
    );
}

#[test]
fn raw_diff_all_none_is_noop() {
    let mut e = Event::new("x");
    e.raw_diff(None, None, None);
    assert!(e.change.is_none());
}

#[test]
fn raw_diff_patch_only() {
    let mut e = fixed(Event::new("x"));
    let patch = json!([{ "op": "replace", "path": "/a", "value": 2 }]);
    e.raw_diff(None, None, Some(patch.clone()));
    assert_eq!(wire(&e)["change"], json!({ "patch": patch }));
    // change is a struct; assert only patch present
    assert_eq!(
        e.change,
        Some(Change {
            before: None,
            after: None,
            patch: Some(patch)
        })
    );
}

// --- apply_redaction directly ---------------------------------------------

#[test]
fn redaction_nested_and_array_index() {
    let doc = json!({ "user": { "password": "p" }, "keys": ["a", "b"] });
    let out = apply_redaction(doc, &["/user/password", "/keys/0"]);
    assert_eq!(
        out,
        json!({ "user": { "password": "[REDACTED]" }, "keys": ["[REDACTED]", "b"] })
    );
}

#[test]
fn redaction_missing_and_out_of_range_skipped() {
    assert_eq!(
        apply_redaction(json!({ "a": 1 }), &["/nope", "/a/deep"]),
        json!({ "a": 1 })
    );
    assert_eq!(
        apply_redaction(json!({ "k": ["a"] }), &["/k/5"]),
        json!({ "k": ["a"] })
    );
}

#[test]
fn redaction_empty_pointer_redacts_whole_doc() {
    assert_eq!(
        apply_redaction(json!({ "a": 1 }), &[""]),
        json!("[REDACTED]")
    );
}

#[test]
fn redaction_escaped_tokens() {
    let doc = json!({ "a/b": { "c~d": "secret" } });
    let out = apply_redaction(doc, &["/a~1b/c~0d"]);
    assert_eq!(out, json!({ "a/b": { "c~d": "[REDACTED]" } }));
}
