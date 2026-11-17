//! Ring buffer, SQLite WAL writer, `.pgrec` recorder (20-byte header, u64 start micros).

pub mod pgrec;
pub mod recorder;
pub mod ring;
pub mod sqlite;

pub use pgrec::{NoopPgrecWriter, PgrecWriter};
pub use recorder::{FlightRecorder, FlightRecorderConfig, RecordInput, RecorderError};
pub use ring::{RingBuffer, RingEntry};
pub use sqlite::{count_events, insert_anomaly, open_db, SqliteError};
