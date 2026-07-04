//! Tests for the top-level client: construction, env loading, subclients.

use everscribe::minter::{MinterOptions, TokenOptions};
use everscribe::recorder::RecorderOptions;
use everscribe::{Client, ConfigError};
use serde_json::json;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn new_trims_and_validates() {
    let es = everscribe::new("  proj ", "  key ").unwrap();
    assert_eq!(es.project_id(), "proj");

    assert!(matches!(
        everscribe::new("   ", "key"),
        Err(ConfigError::EmptyProjectId)
    ));
    assert!(matches!(
        everscribe::new("proj", "   "),
        Err(ConfigError::EmptyApiKey)
    ));
}

#[test]
fn client_new_matches_module_fn() {
    assert_eq!(Client::new("p", "k").unwrap().project_id(), "p");
}

#[test]
fn from_env_success_and_missing() {
    // Run sequentially in one test to avoid races on process-global env.
    std::env::set_var("EVERSCRIBE_PROJECT_ID", "  penv ");
    std::env::set_var("EVERSCRIBE_API_KEY", "kenv");
    assert_eq!(everscribe::new_from_env().unwrap().project_id(), "penv");

    std::env::remove_var("EVERSCRIBE_PROJECT_ID");
    assert!(matches!(
        everscribe::new_from_env(),
        Err(ConfigError::MissingEnv("EVERSCRIBE_PROJECT_ID"))
    ));

    std::env::set_var("EVERSCRIBE_PROJECT_ID", "p");
    std::env::remove_var("EVERSCRIBE_API_KEY");
    assert!(matches!(
        everscribe::new_from_env(),
        Err(ConfigError::MissingEnv("EVERSCRIBE_API_KEY"))
    ));

    std::env::remove_var("EVERSCRIBE_PROJECT_ID");
}

#[tokio::test]
async fn new_recorder_returns_working_recorder() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;

    let es = everscribe::new("proj", "key").unwrap();
    let rec = es.new_recorder(RecorderOptions {
        base_url: server.uri(),
        ..Default::default()
    });
    rec.record(everscribe::Event::new("user.login"))
        .await
        .unwrap();
    rec.flush().await.unwrap();
    rec.close().await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn new_minter_returns_working_minter() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({ "token": "jwt" })))
        .mount(&server)
        .await;

    let es = everscribe::new("proj", "key").unwrap();
    let m = es.new_minter(MinterOptions {
        base_url: server.uri(),
        ..Default::default()
    });
    let token = m.mint_token(&TokenOptions::default()).await.unwrap();
    assert_eq!(token, "jwt");
}
