#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use pg_wire::startup::{encode_cancel, parse_startup_pdu};

fuzz_target!(|data: &[u8]| {
    let _ = parse_startup_pdu(Bytes::copy_from_slice(data));
    if data.len() >= 8 {
        let pid = i32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4]));
        let secret = i32::from_le_bytes(data[4..8].try_into().unwrap_or([0; 4]));
        let encoded = encode_cancel(pid, secret);
        let _ = parse_startup_pdu(encoded.freeze());
    }
});