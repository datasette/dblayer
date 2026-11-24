//! Seeded fuzz of everything that reads bytes from the parent: the frame
//! reader and both decoders. Mutated valid messages plus random bytes; the
//! property is "no panic" (an error is fine) and a stable re-encoding of
//! whatever decoded; plus timing checks for quadratic decoding (the DuckDB
//! Rust worker had a quadratic map decoder). `cargo test` runs it;
//! `DBLAYER_RS_FUZZ_ITERS` raises the count.

use std::time::Instant;

use crate::codec::{Codec, Value, W, decode_msg};
use crate::worker::frame_for_test;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

fn s(x: &str) -> Value {
    Value::Str(x.as_bytes().to_vec())
}

fn obj(items: Vec<(&str, Value)>) -> Value {
    Value::Map(items.into_iter().map(|(k, v)| (s(k), v)).collect())
}

fn seeds() -> Vec<Value> {
    let ext = |code: i8, v: Value| Value::Ext(code, Box::new(v));
    vec![
        obj(vec![("type", s("open")), ("path", s("/tmp/x.db")), ("mode", s("write")), ("id", Value::Int(1))]),
        obj(vec![
            ("type", s("execute")),
            ("sql", s("select ?, ?, ?")),
            ("params", Value::Arr(vec![Value::Int(1), s("é"), Value::Float(2.5), Value::Bin(vec![0, 1, 255]), Value::Nil, Value::Bool(true)])),
            ("id", Value::Int(2)),
            ("conn", Value::Int(1)),
        ]),
        obj(vec![
            ("type", s("execute")),
            ("sql", s("select :a")),
            ("params", Value::Map(vec![(s("a"), Value::Arr(vec![Value::Int(i64::MIN), Value::BigInt(b"340282366920938463463374607431768211455".to_vec())]))])),
        ]),
        obj(vec![(
            "params",
            Value::Arr(vec![
                ext(16, Value::Arr(vec![s("2024-01-02T03:04:05.123456+05:30"), s("Europe/London")])),
                ext(17, s("2024-02-29")),
                ext(19, s("-123.4500")),
                ext(22, Value::Arr(vec![Value::Arr(vec![Value::Int(1), s("a")])])),
            ]),
        )]),
        obj(vec![("type", s("stream")), ("sql", s("select 1")), ("chunk_rows", Value::Int(1)), ("credit", Value::Int(2))]),
        obj(vec![("type", s("more")), ("stream", Value::Int(5)), ("credit", Value::Int(2))]),
        obj(vec![("type", s("execute_many")), ("sql", s("insert into t values (?)")), ("params_seq", Value::Arr(vec![Value::Arr(vec![Value::Int(1)]); 5]))]),
    ]
}

fn encode(codec: Codec, v: &Value) -> Vec<u8> {
    let mut w = W::new(codec, 1 << 26);
    w.val(v);
    w.finish().to_vec()
}

fn mutate(rng: &mut Rng, b: &mut Vec<u8>) {
    for _ in 0..1 + rng.below(4) {
        if b.is_empty() {
            b.push(rng.next() as u8);
            continue;
        }
        let pos = rng.below(b.len());
        match rng.below(5) {
            0 | 1 => b[pos] = rng.next() as u8,
            2 => {
                let end = (pos + 1 + rng.below(4)).min(b.len());
                b.drain(pos..end);
            }
            3 => {
                for _ in 0..1 + rng.below(4) {
                    b.insert(pos, rng.next() as u8);
                }
            }
            // Interesting bytes: length prefixes, ext markers, quotes.
            _ => b[pos] = [0xc7, 0xc9, 0xd4, 0xd8, 0xdc, 0xdd, 0xde, 0xdf, 0xff, b'"', b'\\', b'{', b'[', b'$'][rng.below(14)],
        }
    }
}

fn iters() -> usize {
    std::env::var("DBLAYER_RS_FUZZ_ITERS").ok().and_then(|v| v.parse().ok()).unwrap_or(30_000)
}

#[test]
fn fuzz_decoders_and_frame_reader() {
    let mut rng = Rng(0x5eed_1234_abcd_0001);
    let seeds = seeds();
    let t0 = Instant::now();
    let (mut ok, mut err) = (0usize, 0usize);
    for i in 0..iters() {
        let codec = if i % 2 == 0 { Codec::Json } else { Codec::Msgpack };
        let mut frame = if rng.below(10) == 0 {
            (0..rng.below(64)).map(|_| rng.next() as u8).collect()
        } else {
            encode(codec, &seeds[rng.below(seeds.len())])
        };
        mutate(&mut rng, &mut frame);
        // The frame reader: whatever the header says.
        if let Ok(Some(payload)) = frame_for_test(&frame, 1 << 20) {
            match decode_msg(codec, &payload) {
                Ok(v) => {
                    ok += 1;
                    // Whatever decoded re-encodes and decodes to itself
                    // (values that never echo, ext, become nil once).
                    let again = encode(codec, &v);
                    let v2 = decode_msg(codec, &again[4..]);
                    if !not_echoed(&v) {
                        assert_eq!(v2.as_ref().ok(), Some(&v), "re-encoding changed {v:?}");
                    }
                }
                Err(_) => err += 1,
            }
        }
        // And the payload as such, with no frame header.
        let _ = decode_msg(codec, &frame);
    }
    eprintln!("fuzz: {} iterations, {ok} decoded, {err} refused, {:?}", iters(), t0.elapsed());
}

fn not_echoed(v: &Value) -> bool {
    match v {
        Value::Ext(..) => true,
        Value::Float(f) => f.is_nan(),
        // JSON: a one-key map that looks like a wrapper does not survive
        Value::Map(m) => m.len() == 1 || m.iter().any(|(k, x)| not_echoed(k) || not_echoed(x)),
        Value::Arr(a) => a.iter().any(not_echoed),
        // msgpack keys may be bytes or numbers, which JSON cannot echo
        Value::BigInt(_) | Value::Bin(_) => true,
        // invalid UTF-8 is echoed with U+FFFD, on purpose
        Value::Str(b) => std::str::from_utf8(b).is_err(),
        _ => false,
    }
}

/// No decoder is quadratic: maps of distinct and of repeated keys, long
/// arrays and long strings with escapes, both codecs. Each shape is decoded
/// at size n and 10n; linear decoding takes about 10 times as long,
/// quadratic about 100 times. The ratio is asserted (below 40), not a time,
/// so sanitizer and Miri builds pass too. `DBLAYER_RS_FUZZ_SCALE` sets n.
#[test]
fn decoders_are_linear() {
    let n: usize = std::env::var("DBLAYER_RS_FUZZ_SCALE").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000);
    let shapes: Vec<(&str, fn(usize) -> Value)> = vec![
        ("map", |n| Value::Map((0..n).map(|i| (Value::Str(format!("k{i}").into_bytes()), Value::Int(i as i64))).collect())),
        ("dup keys", |n| Value::Map((0..n).map(|_| (s("k"), Value::Int(1))).collect())),
        ("array", |n| Value::Arr((0..n * 10).map(|i| Value::Int(i as i64)).collect())),
        ("string", |n| Value::Str("a\\\"é\n".repeat(n * 20).into_bytes())),
    ];
    for codec in [Codec::Json, Codec::Msgpack] {
        for (name, make) in &shapes {
            let mut times = Vec::new();
            for size in [n, n * 10] {
                let b = encode(codec, &obj(vec![("x", make(size))]));
                let t0 = Instant::now();
                let d = decode_msg(codec, &b[4..]).unwrap();
                times.push(t0.elapsed().as_secs_f64().max(1e-6));
                assert!(matches!(d, Value::Map(_)));
            }
            let ratio = times[1] / times[0];
            eprintln!("decode {name} ({codec:?}): {:.2} ms at n, {:.2} ms at 10n, ratio {ratio:.1}", times[0] * 1e3, times[1] * 1e3);
            assert!(ratio < 40.0 || times[1] < 0.05, "{name} {codec:?} looks quadratic: ratio {ratio:.1}");
        }
    }
}

#[test]
fn frames_over_the_cap_and_truncated_frames_are_refused() {
    assert!(frame_for_test(&[0, 0, 0, 10, 1, 2], 1 << 20).is_err());
    assert!(frame_for_test(&[0x80, 0, 0, 0], 1 << 20).unwrap_err().contains("exceeds"));
    assert_eq!(frame_for_test(&[], 10).unwrap(), None);
    assert!(frame_for_test(&[0, 0], 10).is_err());
    assert_eq!(frame_for_test(&[0, 0, 0, 2, b'{', b'}'], 10).unwrap(), Some(b"{}".to_vec()));
}
