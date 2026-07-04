//! Tests for the minter: option validation, wire shape, and the mint flow.

use std::time::Duration;

use everscribe::minter::{
    Client, MinterError, MinterOptions, TokenOptions, MAX_EXPIRES_IN, MIN_EXPIRES_IN,
};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn wire(opts: &TokenOptions) -> serde_json::Value {
    opts.to_wire().unwrap()
}

// --- validation / wire -----------------------------------------------------

#[test]
fn default_options_empty_wire() {
    assert_eq!(wire(&TokenOptions::default()), json!({}));
}

#[test]
fn tenant_id_trimmed() {
    let o = TokenOptions {
        tenant_id: "  t1 ".into(),
        ..Default::default()
    };
    assert_eq!(wire(&o), json!({ "tenant_id": "t1" }));
}

#[test]
fn tenant_id_empty_after_trim_rejected() {
    let o = TokenOptions {
        tenant_id: "   ".into(),
        ..Default::default()
    };
    assert!(matches!(o.to_wire(), Err(MinterError::Validation(m)) if m.contains("after trim")));
}

#[test]
fn tenant_id_too_long_rejected() {
    let o = TokenOptions {
        tenant_id: "x".repeat(257),
        ..Default::default()
    };
    assert!(matches!(o.to_wire(), Err(MinterError::Validation(m)) if m.contains("256")));
}

#[test]
fn tenant_id_max_length_ok() {
    let o = TokenOptions {
        tenant_id: "x".repeat(256),
        ..Default::default()
    };
    assert_eq!(wire(&o)["tenant_id"], json!("x".repeat(256)));
}

#[test]
fn expires_in_to_seconds() {
    let o = TokenOptions {
        expires_in: Duration::from_secs(3600),
        ..Default::default()
    };
    assert_eq!(wire(&o)["expires_in"], json!(3600));
}

#[test]
fn expires_in_zero_omitted() {
    assert!(wire(&TokenOptions::default()).get("expires_in").is_none());
}

#[test]
fn expires_in_below_min_rejected() {
    let o = TokenOptions {
        expires_in: MIN_EXPIRES_IN - Duration::from_secs(1),
        ..Default::default()
    };
    assert!(matches!(o.to_wire(), Err(MinterError::Validation(m)) if m.contains("below minimum")));
}

#[test]
fn expires_in_above_max_rejected() {
    let o = TokenOptions {
        expires_in: MAX_EXPIRES_IN + Duration::from_secs(1),
        ..Default::default()
    };
    assert!(matches!(o.to_wire(), Err(MinterError::Validation(m)) if m.contains("above maximum")));
}

#[test]
fn expires_in_bounds_inclusive() {
    assert!(TokenOptions {
        expires_in: MIN_EXPIRES_IN,
        ..Default::default()
    }
    .to_wire()
    .is_ok());
    assert!(TokenOptions {
        expires_in: MAX_EXPIRES_IN,
        ..Default::default()
    }
    .to_wire()
    .is_ok());
}

#[test]
fn allowed_columns_valid() {
    let o = TokenOptions {
        allowed_columns: Some(vec!["actor".into(), "action".into()]),
        ..Default::default()
    };
    assert_eq!(wire(&o)["columns"], json!(["actor", "action"]));
}

#[test]
fn allowed_columns_empty_rejected() {
    let o = TokenOptions {
        allowed_columns: Some(vec![]),
        ..Default::default()
    };
    assert!(
        matches!(o.to_wire(), Err(MinterError::Validation(m)) if m.contains("empty; use None"))
    );
}

#[test]
fn allowed_columns_unknown_rejected() {
    let o = TokenOptions {
        allowed_columns: Some(vec!["bogus".into()]),
        ..Default::default()
    };
    assert!(matches!(o.to_wire(), Err(MinterError::Validation(m)) if m.contains("unknown column")));
}

#[test]
fn allowed_actions_valid_grammar() {
    for a in [
        "user.login",
        "user.*",
        "a",
        "a.b.c",
        "api_key.revoke",
        "a1_b2.c3",
    ] {
        let o = TokenOptions {
            allowed_actions: Some(vec![a.into()]),
            ..Default::default()
        };
        assert!(o.to_wire().is_ok(), "{a} should be valid");
    }
}

#[test]
fn allowed_actions_invalid_grammar_rejected() {
    for a in [
        "*",
        "*.create",
        "user.*.create",
        "user*",
        "user.",
        ".user",
        "user.log-in",
        "",
    ] {
        let o = TokenOptions {
            allowed_actions: Some(vec![a.into()]),
            ..Default::default()
        };
        assert!(
            matches!(o.to_wire(), Err(MinterError::Validation(_))),
            "{a:?} should be rejected"
        );
    }
}

#[test]
fn allowed_actions_empty_rejected() {
    let o = TokenOptions {
        allowed_actions: Some(vec![]),
        ..Default::default()
    };
    assert!(
        matches!(o.to_wire(), Err(MinterError::Validation(m)) if m.contains("empty; use None"))
    );
}

#[test]
fn allowed_fields_passthrough_and_empty_rejected() {
    let o = TokenOptions {
        allowed_fields: Some(vec!["metadata.plan".into()]),
        ..Default::default()
    };
    assert_eq!(wire(&o)["allowed_fields"], json!(["metadata.plan"]));
    let empty = TokenOptions {
        allowed_fields: Some(vec![]),
        ..Default::default()
    };
    assert!(matches!(empty.to_wire(), Err(MinterError::Validation(_))));
}

#[test]
fn allow_flags_only_when_true() {
    assert_eq!(wire(&TokenOptions::default()), json!({}));
    let o = TokenOptions {
        allow_dsl_input: true,
        allow_nlp: true,
        ..Default::default()
    };
    assert_eq!(
        wire(&o),
        json!({ "allow_dsl_input": true, "allow_nlp": true })
    );
}

#[test]
fn full_wire_shape() {
    let o = TokenOptions {
        tenant_id: "acme".into(),
        expires_in: Duration::from_secs(1800),
        allowed_columns: Some(vec!["actor".into(), "action".into()]),
        allowed_actions: Some(vec!["user.*".into(), "billing.charge".into()]),
        allowed_fields: Some(vec!["actor.email".into()]),
        allow_dsl_input: true,
        allow_nlp: true,
    };
    assert_eq!(
        wire(&o),
        json!({
            "tenant_id": "acme",
            "expires_in": 1800,
            "columns": ["actor", "action"],
            "actions": ["user.*", "billing.charge"],
            "allowed_fields": ["actor.email"],
            "allow_dsl_input": true,
            "allow_nlp": true,
        })
    );
}

// --- client ----------------------------------------------------------------

#[tokio::test]
async fn mint_token_success() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/projects/proj1/embed-tokens"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({ "token": "jwt.abc" })))
        .mount(&server)
        .await;

    let c = Client::new(
        "proj1",
        "key1",
        MinterOptions {
            base_url: server.uri(),
            ..Default::default()
        },
    );
    let token = c
        .mint_token(&TokenOptions {
            tenant_id: "acme".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(token, "jwt.abc");

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs[0].headers.get("authorization").unwrap(), "Bearer key1");
    let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body, json!({ "tenant_id": "acme" }));
}

#[tokio::test]
async fn mint_token_validation_error_makes_no_http_call() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({ "token": "x" })))
        .mount(&server)
        .await;
    let c = Client::new(
        "p",
        "k",
        MinterOptions {
            base_url: server.uri(),
            ..Default::default()
        },
    );
    let err = c
        .mint_token(&TokenOptions {
            allowed_columns: Some(vec![]),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, MinterError::Validation(_)));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn mint_token_non_201_is_http_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_string("  bad options  "))
        .mount(&server)
        .await;
    let c = Client::new(
        "p",
        "k",
        MinterOptions {
            base_url: server.uri(),
            ..Default::default()
        },
    );
    let err = c.mint_token(&TokenOptions::default()).await.unwrap_err();
    match err {
        MinterError::Http { status, body } => {
            assert_eq!(status, 400);
            assert_eq!(body, "bad options");
        }
        other => panic!("expected Http error, got {other:?}"),
    }
}
