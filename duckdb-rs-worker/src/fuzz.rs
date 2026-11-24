//! Seeded fuzz of everything that reads bytes from the parent: the frame
//! reader, both codecs' decoders and the parameter conversion that follows
//! them. Mutated valid messages plus random bytes; the property is "no
//! panic" (an error is fine), plus a stable re-encoding of whatever decoded.
//! `cargo test` runs it; `DBLAYER_RS_FUZZ_ITERS` raises the count.

use std::io::Cursor;
use std::time::Instant;

use crate::value::Value;
use crate::{convert, json, msgpack, worker};

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

fn seeds() -> Vec<Value> {
    let ext = |code: i8, v: Value| Value::Ext(code, Box::new(v));
    vec![
        Value::obj(vec![("type", Value::str("open")), ("path", Value::str("/tmp/x.duckdb")), ("mode", Value::str("write")), ("id", Value::Int(1))]),
        Value::obj(vec![
            ("type", Value::str("execute")),
            ("sql", Value::str("select ?, ?, ?")),
            ("params", Value::List(vec![Value::Int(1), Value::str("é"), Value::Float(2.5), Value::Bytes(vec![0, 1, 255]), Value::Null, Value::Bool(true)])),
            ("id", Value::Int(2)),
        ]),
        Value::obj(vec![
            ("type", Value::str("execute")),
            ("sql", Value::str("select $a")),
            ("params", Value::Map(vec![(Value::str("a"), Value::List(vec![Value::Int(1 << 70), Value::BigInt("340282366920938463463374607431768211455".into())]))])),
        ]),
        Value::obj(vec![(
            "params",
            Value::List(vec![
                ext(16, Value::List(vec![Value::str("2024-01-02T03:04:05.123456+05:30")])),
                ext(17, Value::str("2024-02-29")),
                ext(18, Value::str("23:59:59.999999")),
                ext(19, Value::str("-123.4500")),
                ext(20, Value::str("12345678-1234-5678-1234-567812345678")),
                ext(21, Value::List(vec![Value::Int(-1), Value::Int(86399), Value::Int(999999)])),
                ext(22, Value::List(vec![Value::List(vec![Value::Int(1), Value::str("a")])])),
                ext(23, Value::List(vec![Value::Int(1), Value::Null])),
            ]),
        )]),
        Value::obj(vec![("type", Value::str("stream")), ("sql", Value::str("select 1")), ("chunk_rows", Value::Int(1)), ("credit", Value::Int(2))]),
        Value::obj(vec![("type", Value::str("op")), ("name", Value::str("append")), ("args", Value::obj(vec![("table", Value::str("t")), ("rows", Value::List(vec![Value::List(vec![Value::Int(1)])]))]))]),
    ]
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

/// Whatever decodes must convert to parameters without a panic and
/// re-encode to bytes that decode to the same encoding again.
fn exercise(v: &Value) {
    let params = match v {
        Value::Map(items) => items.iter().find(|(k, _)| *k == Value::str("params")).map(|(_, p)| p.clone()),
        _ => None,
    };
    match params {
        Some(Value::List(ps)) => ps.iter().for_each(|p| drop(convert::param(p))),
        Some(Value::Map(ps)) => ps.iter().for_each(|(_, p)| drop(convert::param(p))),
        Some(p) => drop(convert::param(&p)),
        None => drop(convert::param(v)),
    }
    let m = msgpack::encode(v);
    assert_eq!(msgpack::encode(&msgpack::decode(&m).expect("re-decode msgpack")), m);
    let j = json::encode(v);
    if let Ok(again) = json::decode(&j) {
        assert_eq!(json::encode(&again), j);
    }
}

#[test]
fn decoders_never_panic() {
    let iters: usize = std::env::var("DBLAYER_RS_FUZZ_ITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let mut rng = Rng(0x5eed_1234_abcd_0001);
    let encoded: Vec<(bool, Vec<u8>)> = seeds()
        .iter()
        .flat_map(|v| [(true, msgpack::encode(v)), (false, json::encode(v))])
        .collect();
    let mut decoded = 0usize;
    for i in 0..iters {
        let (is_mp, mut b) = if i % 10 == 0 {
            // Pure noise, sometimes long.
            let n = rng.below(if i % 100 == 0 { 4096 } else { 64 });
            (i % 20 == 0, (0..n).map(|_| rng.next() as u8).collect())
        } else {
            let (m, b) = &encoded[rng.below(encoded.len())];
            (*m, b.clone())
        };
        if i % 10 != 0 {
            mutate(&mut rng, &mut b);
        }
        let r = if is_mp { msgpack::decode(&b) } else { json::decode(&b) };
        if let Ok(v) = r {
            decoded += 1;
            exercise(&v);
        }
        // The same bytes as a frame stream: header, payload, more frames.
        let mut cur = Cursor::new(&b);
        let mut frames = 0;
        while let Ok(Some(f)) = worker::read_frame(&mut cur, 1 << 16) {
            assert!(f.len() <= 1 << 16);
            frames += 1;
            if frames > 1000 {
                break;
            }
        }
    }
    assert!(decoded > iters / 50, "the mutations are too destructive: {decoded} of {iters} decoded");
}

/// A large map decodes in time linear in its size, not quadratic (the
/// decoder keeps the last value of a repeated key, as Python's dict does).
#[test]
fn large_maps_decode_quickly() {
    let n = 100_000;
    let v = Value::Map((0..n).map(|i| (Value::Int(i), Value::Null)).collect());
    let m = msgpack::encode(&v);
    let t = Instant::now();
    assert_eq!(msgpack::decode(&m).unwrap(), v);
    let j = json::encode(&Value::Map((0..n).map(|i| (Value::str(format!("k{i}")), Value::Null)).collect()));
    json::decode(&j).unwrap();
    let took = t.elapsed();
    assert!(took.as_secs_f64() < 5.0, "decoding two {n}-key maps took {took:?}");
}
