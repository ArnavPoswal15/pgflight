//! JSON-lines control protocol for pgflightd (CLI + Tauri cockpit).

pub mod client;
pub mod protocol;

pub use client::{control_socket_path, ControlClient, ControlError};
pub use protocol::*;
