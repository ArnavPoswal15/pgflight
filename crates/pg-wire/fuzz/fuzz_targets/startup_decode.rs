#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use pg_wire::startup::parse_startup_pdu;

fuzz_target!(|data: &[u8]| {
    let _ = parse_startup_pdu(Bytes::copy_from_slice(data));
});