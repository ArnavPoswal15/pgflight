//! pgflightd: long-running proxy. Control socket: ~/.pgflight/control.sock (mode 0600).

mod config;
mod control;

use clap::Parser;
use config::Config;
use pg_proxy::cancel::{CancelRegistry, SharedCancelRegistry};
use pg_proxy::relay::handle_connection;
use pg_proxy::telemetry::{DroppedEventCounter, TelemetryEvent, EVENT_CHANNEL_CAP};
use pg_proxy::upstream::UpstreamConfig;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("pgflightd=info".parse()?))
        .init();

    let cfg = Config::parse();
    let control_path = cfg.control_socket_path();
    control::serve_control_socket(&control_path).await?;
    info!(path = %control_path.display(), "control socket listening (mode 0600)");

    let ca_pem = match &cfg.upstream_ca {
        Some(p) => Some(std::fs::read(p)?),
        None => None,
    };
    let upstream_cfg = UpstreamConfig {
        addr: cfg.upstream.clone(),
        tls: cfg.upstream_tls,
        ca_pem,
        tls_verify: cfg.upstream_tls_verify,
    };

    let registry: SharedCancelRegistry = Arc::new(CancelRegistry::new());
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel::<TelemetryEvent>(EVENT_CHANNEL_CAP);
    let drop_counter = Arc::new(DroppedEventCounter::default());

    let listener = TcpListener::bind(&cfg.listen).await?;
    info!(addr = %cfg.listen, upstream = %cfg.upstream, "pgflightd listening");

    loop {
        let (client, peer) = listener.accept().await?;
        let upstream_cfg = upstream_cfg.clone();
        let registry = registry.clone();
        let event_tx = event_tx.clone();
        let drop_counter = drop_counter.clone();
        tokio::spawn(async move {
            info!(%peer, "client connected");
            if let Err(e) =
                handle_connection(client, upstream_cfg, registry, Some(event_tx), drop_counter)
                    .await
            {
                error!(%peer, error = %e, "connection ended");
            }
        });
    }
}
