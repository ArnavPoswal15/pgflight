//! Docker-backed shadow tests (`postgres-shadow` on :5434).

use pg_shadow::{assert_select_only, ShadowClient, DEFAULT_SHADOW_URL};
use std::env;

fn shadow_enabled() -> bool {
    env::var("PGFLIGHT_SHADOW_IT").ok().as_deref() == Some("1")
}

#[tokio::test]
#[ignore = "requires docker postgres-shadow; set PGFLIGHT_SHADOW_IT=1 to run"]
async fn explain_select_on_shop() {
    if !shadow_enabled() {
        return;
    }
    assert_select_only("SELECT count(*) FROM users").unwrap();
    let client = ShadowClient::new(DEFAULT_SHADOW_URL);
    let out = client
        .explain_select("SELECT count(*) FROM users", &[])
        .await
        .expect("shadow explain");
    let text = out.plan_json.to_string();
    assert!(text.contains("Aggregate") || text.contains("Seq Scan") || text.contains("Index"));
}

#[tokio::test]
#[ignore = "requires docker postgres-shadow; set PGFLIGHT_SHADOW_IT=1 to run"]
async fn explain_parameterized_user_lookup() {
    if !shadow_enabled() {
        return;
    }
    let client = ShadowClient::new(DEFAULT_SHADOW_URL);
    let id: i32 = 1;
    let out = client
        .explain_select("SELECT * FROM users WHERE id = $1", &[&id])
        .await
        .expect("shadow explain with bind");
    assert!(!out.hints_slice().is_empty() || out.plan_json.is_array());
}
