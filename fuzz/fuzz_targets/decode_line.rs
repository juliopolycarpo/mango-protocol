//! `mango_protocol::decode_line` over arbitrary bytes: must never panic,
//! whatever the bytes and whatever ceiling they are decoded against.
#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use mango_protocol::codec::ndjson::decode_line;

fuzz_target!(|data: &[u8]| {
    let mut source = Unstructured::new(data);
    // A ceiling bounded well below the 16 MiB protocol default keeps the
    // "too large" refusal reachable within the time budget; at the default,
    // no fuzzer-sized input ever crosses it.
    let max_frame_bytes = source.int_in_range(16..=8192usize).unwrap_or(4096);
    let line = source.take_rest();
    let _ = decode_line(line, max_frame_bytes);
});
