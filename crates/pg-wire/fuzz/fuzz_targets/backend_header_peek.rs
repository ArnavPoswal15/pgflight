#![no_main]

use libfuzzer_sys::fuzz_target;
use pg_wire::peek_header;

fuzz_target!(|data: &[u8]| {
    let _ = peek_header(data);
});