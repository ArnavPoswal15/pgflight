//! Thin client of pgflightd — Query Flight Recorder cockpit (CLI).

use clap::{Parser, Subcommand};
use pgflight_control::protocol::{ControlRequest, ControlResponse};
use pgflight_control::ControlClient;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "pgflight",
    about = "Query Flight Recorder — CLI cockpit for pgflightd"
)]
struct Cli {
    #[arg(long, default_value = "~/.pgflight/control.sock")]
    socket: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Daemon reachability
    Ping,
    /// Proxy + recorder health
    Status,
    /// Live flight log (subscribe → events)
    Tail {
        #[arg(long)]
        patterns: bool,
    },
    /// JSONL export of recent query_events
    Export {
        #[arg(long, default_value_t = 300)]
        since_secs: u64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let socket = expand_home(&cli.socket);
    let client = ControlClient::new(socket);

    match cli.command {
        Commands::Ping => {
            client.ping().await?;
            println!("pong");
        }
        Commands::Status => {
            let res = client.request(&ControlRequest::Status).await?;
            for r in res {
                if let ControlResponse::Status(s) = r {
                    println!("Query Flight Recorder status (v{})", s.version);
                    println!("  listen:   {}", s.listen);
                    println!("  upstream: {}", s.upstream);
                    println!("  recorder:");
                    println!("    flight_id:       {}", s.recorder.flight_id);
                    println!("    recording:       {}", s.recorder.recording);
                    println!("    events_recorded: {}", s.recorder.events_recorded);
                    println!("    db_path:         {}", s.recorder.db_path);
                    println!("    redact_params:   {}", s.recorder.redact_params);
                    println!("    dropped_events:          {}", s.recorder.dropped_events);
                    println!(
                        "    dropped_storage_events:  {}",
                        s.recorder.dropped_storage_events
                    );
                    println!("  anomalies: {}", s.anomaly_count);
                }
            }
        }
        Commands::Tail { patterns } => {
            let topics = if patterns {
                vec!["events".into(), "anomalies".into(), "leaderboard".into()]
            } else {
                vec!["events".into()]
            };
            let res = client
                .request(&ControlRequest::Subscribe { topics })
                .await?;
            for r in res {
                match r {
                    ControlResponse::FlightLogBatch(b) => {
                        for e in b.events {
                            let ms = e.exec_us as f64 / 1000.0;
                            println!(
                                "[{}] fp={} {} {:.2}ms rows={}",
                                e.created_at_ms,
                                e.fingerprint_id,
                                e.canonical_sql,
                                ms,
                                e.rows_returned
                            );
                        }
                    }
                    ControlResponse::PatternSnapshot(p) => {
                        for row in p.patterns {
                            println!(
                                "pattern fp={} count={} p95={}us {}",
                                row.fingerprint_id, row.exec_count, row.p95_us, row.canonical_sql
                            );
                        }
                    }
                    ControlResponse::AnomalyNotification(a) => {
                        println!(
                            "ANOMALY {} repeat={} fp={}",
                            a.anomaly.kind, a.anomaly.repeat_count, a.anomaly.fingerprint_id
                        );
                    }
                    _ => {}
                }
            }
        }
        Commands::Export { since_secs } => {
            let res = client
                .request(&ControlRequest::Export {
                    since_secs: Some(since_secs),
                })
                .await?;
            for r in res {
                if let ControlResponse::ExportLine { line } = r {
                    println!("{line}");
                }
            }
        }
    }
    Ok(())
}

fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}
