//! pgflightd: long-running proxy. Control socket: ~/.pgflight/control.sock (mode 0600).

mod analysis;
mod config;
mod control;
mod event_hub;
mod recorder;

use analysis::AnalysisState;
use clap::Parser;
use config::Config;
use control::{serve_control_socket, ControlContext};
use event_hub::EventHub;
use pg_proxy::cancel::{CancelRegistry, SharedCancelRegistry};
use pg_proxy::relay::handle_connection;
use pg_proxy::telemetry::{DroppedEventCounter, TelemetryEvent, EVENT_CHANNEL_CAP};
use pg_proxy::upstream::UpstreamConfig;
use pg_storage::{FlightRecorder, FlightRecorderConfig};
use recorder::{spawn_recorder_worker, RecorderState};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    cfg.validate()?;
    if !cfg.upstream_tls_verify {
        error!(
            "upstream TLS certificate verification is disabled; upstream traffic is vulnerable to MITM"
        );
    }
    let control_path = cfg.control_socket_path();

    let recorder_cfg = FlightRecorderConfig {
        data_dir: cfg.data_dir_path(),
        redact_params: cfg.redact_params,
        ..Default::default()
    };
    let flight_recorder = FlightRecorder::open(recorder_cfg)?;
    let analysis = AnalysisState::new();
    let recording = Arc::new(AtomicBool::new(cfg.record));
    let drop_counter = Arc::new(DroppedEventCounter::default());

    let hub = EventHub::new();
    hub.spawn_flush_task(flight_recorder.clone(), analysis.clone(), recording.clone());

    let recorder_state = RecorderState {
        recording: recording.clone(),
        flight_recorder: flight_recorder.clone(),
        analysis: analysis.clone(),
        dropped_proxy: drop_counter.clone(),
        hub: hub.clone(),
    };

    let (event_tx, event_rx) = tokio::sync::mpsc::channel::<TelemetryEvent>(EVENT_CHANNEL_CAP);
    spawn_recorder_worker(event_rx, recorder_state.clone());

    let ctx = Arc::new(ControlContext {
        listen: cfg.listen.clone(),
        upstream: cfg.upstream.clone(),
        recorder: recorder_state,
        hub,
        analysis,
    });
    serve_control_socket(&control_path, ctx).await?;
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
    let next_connection_id = Arc::new(AtomicU64::new(1));

    let listener = TcpListener::bind(&cfg.listen).await?;
    info!(addr = %cfg.listen, upstream = %cfg.upstream, "pgflightd listening");

    loop {
        let (client, peer) = listener.accept().await?;
        let upstream_cfg = upstream_cfg.clone();
        let registry = registry.clone();
        let event_tx = event_tx.clone();
        let drop_counter = drop_counter.clone();
        let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            info!(%peer, connection_id, "client connected");
            if let Err(e) = handle_connection(
                client,
                upstream_cfg,
                registry,
                Some(event_tx),
                drop_counter,
                connection_id,
            )
            .await
            {
                error!(%peer, connection_id, error = %e, "connection ended");
            }
        });
    }
}
