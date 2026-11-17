use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "pgflightd",
    about = "Query Flight Recorder daemon (PostgreSQL wire proxy)"
)]
pub struct Config {
    #[arg(long, default_value = "127.0.0.1:5433")]
    pub listen: String,

    #[arg(long, default_value = "127.0.0.1:5432")]
    pub upstream: String,

    #[arg(long, default_value_t = false)]
    pub upstream_tls: bool,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub upstream_tls_verify: bool,

    #[arg(long)]
    pub upstream_ca: Option<PathBuf>,

    #[arg(long, default_value = "~/.pgflight/control.sock")]
    pub control_socket: String,

    #[arg(long, default_value = "~/.pgflight")]
    pub data_dir: String,

    #[arg(long, default_value_t = true)]
    pub record: bool,

    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub redact_params: bool,

    /// Allow binding the proxy to non-loopback addresses (e.g. `0.0.0.0`).
    #[arg(long, default_value_t = false)]
    pub allow_network_bind: bool,

    /// Required with `--no-upstream-tls-verify` (disables upstream certificate validation).
    #[arg(long, default_value_t = false)]
    pub insecure_upstream_tls: bool,
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if !self.allow_network_bind && !is_loopback_bind(&self.listen) {
            return Err(format!(
                "listen address {:?} is not loopback; pass --allow-network-bind to expose the proxy",
                self.listen
            ));
        }
        if !self.upstream_tls_verify && !self.insecure_upstream_tls {
            return Err(
                "refusing --no-upstream-tls-verify without --insecure-upstream-tls (MITM risk)"
                    .into(),
            );
        }
        Ok(())
    }

    pub fn control_socket_path(&self) -> PathBuf {
        expand_home(&self.control_socket)
    }

    pub fn data_dir_path(&self) -> PathBuf {
        expand_home(&self.data_dir)
    }
}

fn is_loopback_bind(listen: &str) -> bool {
    let host = listen
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(listen)
        .trim_start_matches('[')
        .trim_end_matches(']');
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}
