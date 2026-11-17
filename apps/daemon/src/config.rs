use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "pgflightd", about = "PostgreSQL wire proxy daemon")]
pub struct Config {
    #[arg(long, default_value = "127.0.0.1:5433")]
    pub listen: String,

    #[arg(long, default_value = "127.0.0.1:5432")]
    pub upstream: String,

    #[arg(long, default_value_t = false)]
    pub upstream_tls: bool,

    #[arg(long, default_value_t = true)]
    pub upstream_tls_verify: bool,

    #[arg(long)]
    pub upstream_ca: Option<PathBuf>,

    #[arg(long, default_value = "~/.pgflight/control.sock")]
    pub control_socket: String,
}

impl Config {
    pub fn control_socket_path(&self) -> PathBuf {
        if self.control_socket.starts_with("~/") {
            if let Ok(home) = std::env::var("HOME") {
                return PathBuf::from(home).join(&self.control_socket[2..]);
            }
        }
        PathBuf::from(&self.control_socket)
    }
}
