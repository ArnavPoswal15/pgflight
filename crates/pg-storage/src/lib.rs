//! Ring buffer, SQLite WAL writer, `.pgrec` recorder (20-byte header, u64 start micros).

pub mod pgrec;
pub mod recorder;
pub mod ring;
pub mod sqlite;

pub use pgrec::{
    pgrec_path_for_flight, unix_now_us, NoopPgrecWriter, PgrecError, PgrecReader, PgrecRecord,
    PgrecRecordKind, PgrecWriter, ZstdPgrecWriter, PGREC_MAGIC,
};
pub use recorder::{FlightRecorder, FlightRecorderConfig, RecordInput, RecorderError};
pub use ring::{RingBuffer, RingEntry};
pub use sqlite::{count_events, insert_anomaly, open_db, SqliteError};
