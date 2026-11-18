//! Thin client of pgflightd — Query Flight Recorder cockpit (CLI).

use clap::{Parser, Subcommand};
use pgflight_control::protocol::{
    ControlRequest, ControlResponse, ExplainResultBody, FingerprintPlan, PlanCompareBody,
    ReplayResultBody, ReplayResultId,
};
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
    /// EXPLAIN (ANALYZE, BUFFERS) on shadow for a fingerprint or event
    Explain {
        #[arg(long, group = "target")]
        fingerprint_id: Option<i64>,
        #[arg(long, group = "target")]
        event_id: Option<u64>,
        #[arg(long)]
        flight_id: Option<u64>,
        #[arg(long)]
        json: bool,
    },
    /// Replay queries from a `.pgrec` on shadow (timing + optional plans)
    Replay {
        #[arg(long)]
        pgrec: PathBuf,
        #[arg(long)]
        label: String,
        #[arg(long, default_value_t = 0)]
        warmup: u32,
        #[arg(long)]
        explain: bool,
        #[arg(long)]
        json: bool,
    },
    /// Compare EXPLAIN plans (replay IDs or inline JSON plan sets)
    Compare {
        #[arg(long)]
        baseline_replay_id: Option<String>,
        #[arg(long)]
        baseline_label: Option<String>,
        #[arg(long)]
        variant_replay_id: Option<String>,
        #[arg(long)]
        variant_label: Option<String>,
        /// JSON file: array of `{ "fingerprint_id", "plan_json" }` (MVP inline compare)
        #[arg(long)]
        baseline_plans: Option<PathBuf>,
        #[arg(long)]
        variant_plans: Option<PathBuf>,
        #[arg(long)]
        json: bool,
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
                    if let Some(shadow) = &s.shadow {
                        println!("  shadow:");
                        println!("    enabled:   {}", shadow.enabled);
                        println!("    connected: {}", shadow.connected);
                        println!("    url:       {}", shadow.url);
                        println!("    queued:    {}", shadow.queued_jobs);
                    }
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
        Commands::Explain {
            fingerprint_id,
            event_id,
            flight_id,
            json,
        } => {
            if fingerprint_id.is_none() && event_id.is_none() {
                return Err("explain requires --fingerprint-id or --event-id".into());
            }
            let resp = client
                .request_one(&ControlRequest::Explain {
                    fingerprint_id,
                    flight_id,
                    event_id,
                })
                .await?;
            let ControlResponse::ExplainResult(body) = resp else {
                return Err("unexpected response from daemon".into());
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                print_explain(&body);
            }
        }
        Commands::Replay {
            pgrec,
            label,
            warmup,
            explain,
            json,
        } => {
            let path = pgrec.to_string_lossy().into_owned();
            let resp = client
                .request_one(&ControlRequest::ReplayRun {
                    pgrec_path: path,
                    label,
                    warmup_iters: warmup,
                    explain,
                })
                .await?;
            let ControlResponse::ReplayResult(body) = resp else {
                return Err("unexpected response from daemon".into());
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                print_replay(&body);
            }
        }
        Commands::Compare {
            baseline_replay_id,
            baseline_label,
            variant_replay_id,
            variant_label,
            baseline_plans,
            variant_plans,
            json,
        } => {
            let req = build_compare_request(
                baseline_replay_id,
                baseline_label,
                variant_replay_id,
                variant_label,
                baseline_plans,
                variant_plans,
            )?;
            let resp = client.request_one(&req).await?;
            let ControlResponse::PlanCompare(body) = resp else {
                return Err("unexpected response from daemon".into());
            };
            if json {
                println!("{}", serde_json::to_string_pretty(&body)?);
            } else {
                print_compare(&body);
            }
        }
    }
    Ok(())
}

fn build_compare_request(
    baseline_replay_id: Option<String>,
    baseline_label: Option<String>,
    variant_replay_id: Option<String>,
    variant_label: Option<String>,
    baseline_plans: Option<PathBuf>,
    variant_plans: Option<PathBuf>,
) -> Result<ControlRequest, Box<dyn std::error::Error>> {
    let by_id = baseline_replay_id.is_some() || variant_replay_id.is_some();
    let by_file = baseline_plans.is_some() || variant_plans.is_some();
    if by_id && by_file {
        return Err("use either replay IDs or plan JSON files, not both".into());
    }
    if by_id {
        let baseline = baseline_replay_id.ok_or("compare requires --baseline-replay-id")?;
        let variant = variant_replay_id.ok_or("compare requires --variant-replay-id")?;
        return Ok(ControlRequest::CompareReplay {
            baseline: Some(ReplayResultId {
                replay_id: baseline,
                label: baseline_label.unwrap_or_default(),
            }),
            variant: Some(ReplayResultId {
                replay_id: variant,
                label: variant_label.unwrap_or_default(),
            }),
            baseline_plans: None,
            variant_plans: None,
        });
    }
    if by_file {
        let bp = baseline_plans.ok_or("compare requires --baseline-plans")?;
        let vp = variant_plans.ok_or("compare requires --variant-plans")?;
        let baseline_plans: Vec<FingerprintPlan> = load_plan_file(&bp)?;
        let variant_plans: Vec<FingerprintPlan> = load_plan_file(&vp)?;
        return Ok(ControlRequest::CompareReplay {
            baseline: None,
            variant: None,
            baseline_plans: Some(baseline_plans),
            variant_plans: Some(variant_plans),
        });
    }
    Err(
        "compare requires --baseline-replay-id and --variant-replay-id, \
         or --baseline-plans and --variant-plans JSON files"
            .into(),
    )
}

fn load_plan_file(path: &PathBuf) -> Result<Vec<FingerprintPlan>, Box<dyn std::error::Error>> {
    let raw = std::fs::read_to_string(path)?;
    serde_json::from_str(&raw)
        .map_err(|e| format!("{}: invalid plan JSON: {e}", path.display()).into())
}

fn print_explain(body: &ExplainResultBody) {
    if body.hints.is_empty() {
        println!("No plan hints.");
    } else {
        println!("Plan hints:");
        for h in &body.hints {
            let path = h
                .node_path
                .as_deref()
                .map(|p| format!(" @ {p}"))
                .unwrap_or_default();
            println!("  [{}] {}{}", h.code, h.message, path);
        }
    }
    println!();
    match serde_json::to_string_pretty(&body.plan_json) {
        Ok(p) => println!("{p}"),
        Err(e) => eprintln!("plan_json (unformatted): {e}"),
    }
}

fn print_replay(body: &ReplayResultBody) {
    println!(
        "Replay {} (label={})",
        body.replay.replay_id, body.replay.label
    );
    if body.per_fingerprint.is_empty() {
        println!("  (no query timings recorded)");
    } else {
        println!("Per-fingerprint latency (µs):");
        for row in &body.per_fingerprint {
            println!(
                "  fp={} samples={} p50={} p95={} p99={}",
                row.fingerprint_id, row.samples, row.exec_us_p50, row.exec_us_p95, row.exec_us_p99
            );
        }
    }
    if !body.plans.is_empty() {
        println!(
            "Captured {} EXPLAIN plan(s) (use --json for full plans).",
            body.plans.len()
        );
    }
}

fn print_compare(body: &PlanCompareBody) {
    println!("Plan compare summary");
    if body.improved.is_empty() && body.regressed.is_empty() && body.diff_nodes.is_empty() {
        println!("  (no differences)");
        return;
    }
    if !body.improved.is_empty() {
        println!("\nImproved:");
        for e in &body.improved {
            println!("  fp={}: {}", e.fingerprint_id, e.summary);
        }
    }
    if !body.regressed.is_empty() {
        println!("\nRegressed:");
        for e in &body.regressed {
            println!("  fp={}: {}", e.fingerprint_id, e.summary);
        }
    }
    if !body.diff_nodes.is_empty() {
        println!("\nStructural diffs:");
        for n in &body.diff_nodes {
            let base = n.baseline.as_deref().unwrap_or("—");
            let var = n.variant.as_deref().unwrap_or("—");
            println!("  {} baseline={base} variant={var}", n.path);
        }
    }
}

fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}
