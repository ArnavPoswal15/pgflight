//! Docker-backed integration tests (`cargo test -p pg-proxy -- --ignored`).

fn proxy_url() -> String {
    std::env::var("PGFLIGHT_PROXY")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@127.0.0.1:5433/shop".into())
}

#[tokio::test]
#[ignore]
async fn phase1_select_one() {
    let url = proxy_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect through proxy");
    let row: (i32,) = sqlx::query_as("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("query");
    assert_eq!(row.0, 1);
}

#[tokio::test]
#[ignore]
async fn phase2_pipelined_sqlx() {
    let url = proxy_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    let mut conn = pool.acquire().await.expect("acquire");
    for i in 0..3 {
        let v: (i32,) = sqlx::query_as("SELECT $1::int4")
            .bind(i)
            .fetch_one(&mut *conn)
            .await
            .expect("pipelined");
        assert_eq!(v.0, i);
    }
}

#[tokio::test]
#[ignore]
async fn phase2_error_mid_pipeline() {
    let url = proxy_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    let bad: Result<(i32,), _> = sqlx::query_as("SELECT definitely_not_a_column")
        .fetch_one(&pool)
        .await;
    assert!(bad.is_err());
    let ok: (i32,) = sqlx::query_as("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("after error");
    assert_eq!(ok.0, 1);
}

#[tokio::test]
#[ignore]
async fn phase2_pg_dump_schema() {
    let upstream = std::env::var("PGFLIGHT_UPSTREAM").unwrap_or_else(|_| "127.0.0.1:5432".into());
    let listen = std::env::var("PGFLIGHT_LISTEN").unwrap_or_else(|_| "127.0.0.1:5433".into());
    let output = std::process::Command::new("pg_dump")
        .args([
            "-h",
            listen.split(':').next().unwrap_or("127.0.0.1"),
            "-p",
            listen.split(':').nth(1).unwrap_or("5433"),
            "-U",
            "postgres",
            "-d",
            "shop",
            "--schema-only",
            "--no-owner",
        ])
        .env("PGPASSWORD", "postgres")
        .output()
        .expect("spawn pg_dump");
    assert!(
        output.status.success(),
        "pg_dump failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let _ = upstream;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("CREATE TABLE"));
}
