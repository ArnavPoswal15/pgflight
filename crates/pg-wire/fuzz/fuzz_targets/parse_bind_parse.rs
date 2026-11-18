#![no_main]

use libfuzzer_sys::fuzz_target;
use pg_wire::frontend::{parse_bind, parse_parse};

fuzz_target!(|data: &[u8]| {
    let _ = parse_parse(data);
    let _ = parse_bind(data);
});