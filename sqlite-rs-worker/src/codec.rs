//! Wire codecs (docs/worker-protocol.md, "Codecs"): a decoded value tree,
//! the MessagePack and JSON decoders, and a streaming writer that emits
//! either codec straight into a frame buffer, so rows are encoded as they
//! are stepped, with no intermediate value tree (as the C worker's codec.c).
//!
//! Strings are bytes, not `String`: JSON from Python may carry a lone
//! surrogate (kept as its 3-byte form, like Python's "surrogatepass"), and
//! MessagePack strings are not validated, as in the C worker; SQLite gets
//! exactly the bytes the parent sent.

use std::fmt::Write as _;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Json,
    Msgpack,
}

pub const EXT_BIGINT: i8 = 1;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Nil,
    Bool(bool),
    Int(i64),
    /// An integer outside 64 bits: its decimal text.
    BigInt(Vec<u8>),
    Float(f64),
    Str(Vec<u8>),
    Bin(Vec<u8>),
    Arr(Vec<Value>),
    /// In order; keys are usually strings. Duplicates are kept (lookups
    /// find the first), as in the C worker.
    Map(Vec<(Value, Value)>),
    /// A registered value type (ext code 16..127) and its to_wire() form.
    Ext(i8, Box<Value>),
}

const VALUE_TYPE_TAGS: [&str; 8] = ["datetime", "date", "time", "decimal", "uuid", "timedelta", "map", "tuple"];

pub fn value_type_code(tag: &[u8]) -> i8 {
    VALUE_TYPE_TAGS.iter().position(|t| t.as_bytes() == tag).map(|i| 16 + i as i8).unwrap_or(-1)
}

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(items) => {
                items.iter().find(|(k, _)| matches!(k, Value::Str(s) if s == key.as_bytes())).map(|(_, v)| v)
            }
            _ => None,
        }
    }

    pub fn is_str(&self, s: &str) -> bool {
        matches!(self, Value::Str(b) if b == s.as_bytes())
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// An integer, bool or (truncated) float field; `None` when absent or
    /// null. Other types give `Some(dflt)`, as the C worker's get_int.
    pub fn int_field(&self, key: &str, dflt: i64) -> Option<i64> {
        match self.get(key) {
            None | Some(Value::Nil) => None,
            Some(Value::Int(i)) => Some(*i),
            Some(Value::Bool(b)) => Some(*b as i64),
            Some(Value::Float(f)) => Some(*f as i64),
            Some(_) => Some(dflt),
        }
    }

    pub fn int_or(&self, key: &str, dflt: i64) -> i64 {
        self.int_field(key, dflt).unwrap_or(dflt)
    }
}

pub fn field_is_str(m: &Value, key: &str, s: &str) -> bool {
    m.get(key).is_some_and(|v| v.is_str(s))
}

// -- UTF-8 --------------------------------------------------------------------

/// Number of code points if `p` is valid UTF-8.
pub fn utf8_chars(p: &[u8]) -> Option<usize> {
    if p.is_ascii() {
        return Some(p.len());
    }
    std::str::from_utf8(p).ok().map(|s| s.chars().count())
}

/// Invalid sequences replaced by U+FFFD one byte at a time (the C
/// worker's utf8_lossy, which differs from `String::from_utf8_lossy` on
/// truncated sequences).
pub fn utf8_lossy(p: &[u8]) -> Vec<u8> {
    if std::str::from_utf8(p).is_ok() {
        return p.to_vec();
    }
    let mut out = Vec::with_capacity(p.len() + 8);
    let mut i = 0;
    while i < p.len() {
        let b = p[i];
        let len = match b {
            0..=0x7f => 1,
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => 0,
        };
        if len > 0 && i + len <= p.len() && std::str::from_utf8(&p[i..i + len]).is_ok() {
            out.extend_from_slice(&p[i..i + len]);
            i += len;
        } else {
            out.extend_from_slice("\u{fffd}".as_bytes());
            i += 1;
        }
    }
    out
}

// -- base64 -----------------------------------------------------------------

pub const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64val(c: u8) -> Option<u32> {
    Some(match c {
        b'A'..=b'Z' => c - b'A',
        b'a'..=b'z' => c - b'a' + 26,
        b'0'..=b'9' => c - b'0' + 52,
        b'+' => 62,
        b'/' => 63,
        _ => return None,
    } as u32)
}

/// Lenient like Python's b64decode(validate=False): skips non-alphabet bytes.
fn b64decode(s: &[u8]) -> Vec<u8> {
    let mut o = Vec::with_capacity(s.len() / 4 * 3 + 3);
    let mut acc: u32 = 0;
    let mut bits = 0;
    for &c in s {
        match b64val(c) {
            Some(v) => {
                acc = (acc << 6) | v;
                bits += 6;
                if bits >= 8 {
                    bits -= 8;
                    o.push((acc >> bits) as u8);
                }
            }
            None if c == b'=' => break,
            None => {}
        }
    }
    o
}

pub fn b64encode(out: &mut Vec<u8>, s: &[u8]) {
    out.reserve(s.len().div_ceil(3) * 4);
    let mut chunks = s.chunks_exact(3);
    for c in &mut chunks {
        let v = (c[0] as u32) << 16 | (c[1] as u32) << 8 | c[2] as u32;
        out.extend_from_slice(&[B64[(v >> 18) as usize], B64[(v >> 12 & 63) as usize], B64[(v >> 6 & 63) as usize], B64[(v & 63) as usize]]);
    }
    match *chunks.remainder() {
        [a] => {
            let v = (a as u32) << 16;
            out.extend_from_slice(&[B64[(v >> 18) as usize], B64[(v >> 12 & 63) as usize], b'=', b'=']);
        }
        [a, b] => {
            let v = (a as u32) << 16 | (b as u32) << 8;
            out.extend_from_slice(&[B64[(v >> 18) as usize], B64[(v >> 12 & 63) as usize], B64[(v >> 6 & 63) as usize], b'=']);
        }
        _ => {}
    }
}

// -- decoders -----------------------------------------------------------------

const MAX_DEPTH: usize = 200;

/// Decode one frame payload into a message (a map).
pub fn decode_msg(codec: Codec, p: &[u8]) -> Result<Value, String> {
    let mut d = Dec { p, i: 0, depth: 0 };
    let v = match codec {
        Codec::Msgpack => d.mp_value()?,
        Codec::Json => {
            let v = d.j_value()?;
            d.j_ws();
            v
        }
    };
    if d.i != p.len() {
        return Err("extra data after the message".into());
    }
    if !matches!(v, Value::Map(_)) {
        return Err("frame is not a message".into());
    }
    Ok(v)
}

struct Dec<'a> {
    p: &'a [u8],
    i: usize,
    depth: usize,
}

impl<'a> Dec<'a> {
    fn left(&self) -> usize {
        self.p.len() - self.i
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.left() < n {
            return Err("truncated MessagePack data".into());
        }
        let s = &self.p[self.i..self.i + n];
        self.i += n;
        Ok(s)
    }

    fn be(&mut self, n: usize) -> Result<u64, String> {
        Ok(self.take(n)?.iter().fold(0u64, |a, &b| (a << 8) | b as u64))
    }

    // MessagePack

    fn mp_container(&mut self, n: usize, is_map: bool) -> Result<Value, String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err("nesting too deep".into());
        }
        let items = if is_map { n.saturating_mul(2) } else { n };
        // Every item takes at least one byte: refuse absurd counts early.
        if items > self.left() {
            return Err("truncated MessagePack data".into());
        }
        let v = if is_map {
            let mut m = Vec::with_capacity(n);
            for _ in 0..n {
                let k = self.mp_value()?;
                let v = self.mp_value()?;
                m.push((k, v));
            }
            Value::Map(m)
        } else {
            let mut a = Vec::with_capacity(n);
            for _ in 0..n {
                a.push(self.mp_value()?);
            }
            Value::Arr(a)
        };
        self.depth -= 1;
        Ok(v)
    }

    fn mp_ext(&mut self, n: usize) -> Result<Value, String> {
        let body = self.take(n.checked_add(1).ok_or("truncated MessagePack data")?)?;
        let ty = body[0] as i8;
        let data = &body[1..];
        if ty >= 16 {
            let mut sub = Dec { p: data, i: 0, depth: self.depth };
            let inner = sub.mp_value()?;
            return Ok(Value::Ext(ty, Box::new(inner)));
        }
        if ty != EXT_BIGINT {
            return Err(format!("unsupported MessagePack ext type {ty}"));
        }
        Ok(Value::BigInt(data.to_vec()))
    }

    fn mp_value(&mut self) -> Result<Value, String> {
        let b = self.take(1)?[0];
        Ok(match b {
            0x00..=0x7f => Value::Int(b as i64),
            0xe0..=0xff => Value::Int(b as i8 as i64),
            0x80..=0x8f => return self.mp_container((b & 0x0f) as usize, true),
            0x90..=0x9f => return self.mp_container((b & 0x0f) as usize, false),
            0xa0..=0xbf => Value::Str(self.take((b & 0x1f) as usize)?.to_vec()),
            0xc0 => Value::Nil,
            0xc2 => Value::Bool(false),
            0xc3 => Value::Bool(true),
            0xc4 | 0xc5 | 0xc6 | 0xd9 | 0xda | 0xdb => {
                let w = match b {
                    0xc4 | 0xd9 => 1,
                    0xc5 | 0xda => 2,
                    _ => 4,
                };
                let n = self.be(w)? as usize;
                let s = self.take(n)?.to_vec();
                if b >= 0xd9 { Value::Str(s) } else { Value::Bin(s) }
            }
            0xc7 | 0xc8 | 0xc9 => {
                let w = match b {
                    0xc7 => 1,
                    0xc8 => 2,
                    _ => 4,
                };
                let n = self.be(w)? as usize;
                return self.mp_ext(n);
            }
            0xd4 => return self.mp_ext(1),
            0xd5 => return self.mp_ext(2),
            0xd6 => return self.mp_ext(4),
            0xd7 => return self.mp_ext(8),
            0xd8 => return self.mp_ext(16),
            0xca => Value::Float(f32::from_bits(self.be(4)? as u32) as f64),
            0xcb => Value::Float(f64::from_bits(self.be(8)?)),
            0xcc..=0xcf => {
                let u = self.be(1 << (b - 0xcc))?;
                if u > i64::MAX as u64 { Value::BigInt(u.to_string().into_bytes()) } else { Value::Int(u as i64) }
            }
            0xd0 => Value::Int(self.be(1)? as u8 as i8 as i64),
            0xd1 => Value::Int(self.be(2)? as u16 as i16 as i64),
            0xd2 => Value::Int(self.be(4)? as u32 as i32 as i64),
            0xd3 => Value::Int(self.be(8)? as i64),
            0xdc | 0xdd | 0xde | 0xdf => {
                let w = if b == 0xdc || b == 0xde { 2 } else { 4 };
                let n = self.be(w)? as usize;
                return self.mp_container(n, b >= 0xde);
            }
            _ => return Err(format!("invalid MessagePack byte 0x{b:02x}")),
        })
    }

    // JSON

    fn j_ws(&mut self) {
        while self.i < self.p.len() && matches!(self.p[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn j_lit(&mut self, lit: &[u8]) -> bool {
        if self.p[self.i..].starts_with(lit) {
            self.i += lit.len();
            return true;
        }
        false
    }

    fn j_value(&mut self) -> Result<Value, String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err("nesting too deep".into());
        }
        self.j_ws();
        if self.i >= self.p.len() {
            return Err("unexpected end of JSON".into());
        }
        let c = self.p[self.i];
        let v = if c == b'{' {
            self.i += 1;
            self.j_object()?
        } else if c == b'[' {
            self.i += 1;
            self.j_array()?
        } else if c == b'"' {
            self.i += 1;
            Value::Str(self.j_string()?)
        } else if self.j_lit(b"true") {
            Value::Bool(true)
        } else if self.j_lit(b"false") {
            Value::Bool(false)
        } else if self.j_lit(b"null") {
            Value::Nil
        } else if self.j_lit(b"NaN") {
            Value::Float(f64::NAN)
        } else if c == b'-' || c.is_ascii_digit() || c == b'I' {
            self.j_number()?
        } else {
            return Err("invalid JSON".into());
        };
        self.depth -= 1;
        Ok(v)
    }

    fn j_string(&mut self) -> Result<Vec<u8>, String> {
        let s = self.i;
        let mut escaped = false;
        while self.i < self.p.len() && self.p[self.i] != b'"' {
            if self.p[self.i] == b'\\' {
                escaped = true;
                self.i += 1;
            }
            self.i += 1;
        }
        if self.i >= self.p.len() {
            return Err("unterminated JSON string".into());
        }
        let raw = &self.p[s..self.i];
        self.i += 1;
        if !escaped {
            return Ok(raw.to_vec());
        }
        let mut o = Vec::with_capacity(raw.len());
        let mut i = 0;
        while i < raw.len() {
            let c = raw[i];
            if c != b'\\' {
                o.push(c);
                i += 1;
                continue;
            }
            i += 1;
            let c = raw.get(i).copied().unwrap_or(0);
            match c {
                b'"' => o.push(b'"'),
                b'\\' => o.push(b'\\'),
                b'/' => o.push(b'/'),
                b'b' => o.push(8),
                b'f' => o.push(12),
                b'n' => o.push(b'\n'),
                b'r' => o.push(b'\r'),
                b't' => o.push(b'\t'),
                b'u' => {
                    let mut cp = hex4(raw.get(i + 1..i + 5)).ok_or("invalid \\u escape")?;
                    i += 4;
                    if (0xd800..=0xdbff).contains(&cp)
                        && raw.get(i + 1) == Some(&b'\\')
                        && raw.get(i + 2) == Some(&b'u')
                        && let Some(lo) = hex4(raw.get(i + 3..i + 7))
                        && (0xdc00..=0xdfff).contains(&lo)
                    {
                        cp = 0x10000 + ((cp - 0xd800) << 10) + (lo - 0xdc00);
                        i += 6;
                    }
                    // A lone surrogate is kept as its 3-byte form.
                    put_utf8(&mut o, cp);
                }
                _ => return Err("invalid JSON escape".into()),
            }
            i += 1;
        }
        Ok(o)
    }

    fn j_number(&mut self) -> Result<Value, String> {
        let s = self.i;
        if self.i < self.p.len() && self.p[self.i] == b'-' {
            self.i += 1;
        }
        if self.j_lit(b"Infinity") {
            return Ok(Value::Float(if self.p[s] == b'-' { f64::NEG_INFINITY } else { f64::INFINITY }));
        }
        let mut is_float = false;
        while self.i < self.p.len() {
            match self.p[self.i] {
                b'0'..=b'9' => {}
                b'.' | b'e' | b'E' | b'+' | b'-' => is_float = true,
                _ => break,
            }
            self.i += 1;
        }
        let t = &self.p[s..self.i];
        if t.is_empty() || t == b"-" {
            return Err("invalid JSON number".into());
        }
        // ASCII only, by construction.
        let txt = std::str::from_utf8(t).map_err(|_| "invalid JSON number")?;
        if !is_float {
            return Ok(match txt.parse::<i64>() {
                Ok(v) => Value::Int(v),
                Err(_) => Value::BigInt(t.to_vec()),
            });
        }
        // strtod accepts what Rust's parser accepts here (digits, '.',
        // exponent), except that neither reads "inf"/"nan" from this set.
        txt.parse::<f64>().map(Value::Float).map_err(|_| "invalid JSON number".into())
    }

    fn j_object(&mut self) -> Result<Value, String> {
        let mut items: Vec<(Value, Value)> = Vec::new();
        self.j_ws();
        if self.i < self.p.len() && self.p[self.i] == b'}' {
            self.i += 1;
        } else {
            loop {
                self.j_ws();
                if self.i >= self.p.len() || self.p[self.i] != b'"' {
                    return Err("expected a JSON object key".into());
                }
                self.i += 1;
                let k = self.j_string()?;
                self.j_ws();
                if self.i >= self.p.len() || self.p[self.i] != b':' {
                    return Err("expected ':'".into());
                }
                self.i += 1;
                let v = self.j_value()?;
                items.push((Value::Str(k), v));
                self.j_ws();
                if self.i < self.p.len() && self.p[self.i] == b',' {
                    self.i += 1;
                    continue;
                }
                if self.i < self.p.len() && self.p[self.i] == b'}' {
                    self.i += 1;
                    break;
                }
                return Err("expected ',' or '}'".into());
            }
        }
        if items.len() == 1 {
            let (k, v) = &items[0];
            if k.is_str("$b") {
                let Value::Str(s) = v else { return Err("invalid $b value".into()) };
                return Ok(Value::Bin(b64decode(s)));
            }
            if k.is_str("$d") {
                let Value::Arr(a) = v else { return Err("invalid $d value".into()) };
                if a.len() != 2 {
                    return Err("invalid $d value".into());
                }
                let mut a = a.clone();
                let val = a.pop().unwrap();
                let key = a.pop().unwrap();
                return Ok(Value::Map(vec![(key, val)]));
            }
            if k.is_str("$x") {
                let ok = matches!(v, Value::Arr(a) if a.len() == 2 && matches!(a[0], Value::Str(_)));
                if !ok {
                    return Err("invalid $x value".into());
                }
                let Some((_, Value::Arr(mut a))) = items.pop() else { unreachable!() };
                let inner = a.pop().unwrap();
                let Value::Str(tag) = a.pop().unwrap() else { unreachable!() };
                return Ok(Value::Ext(value_type_code(&tag), Box::new(inner)));
            }
        }
        Ok(Value::Map(items))
    }

    fn j_array(&mut self) -> Result<Value, String> {
        let mut items = Vec::new();
        self.j_ws();
        if self.i < self.p.len() && self.p[self.i] == b']' {
            self.i += 1;
        } else {
            loop {
                items.push(self.j_value()?);
                self.j_ws();
                if self.i < self.p.len() && self.p[self.i] == b',' {
                    self.i += 1;
                    continue;
                }
                if self.i < self.p.len() && self.p[self.i] == b']' {
                    self.i += 1;
                    break;
                }
                return Err("expected ',' or ']'".into());
            }
        }
        Ok(Value::Arr(items))
    }
}

fn hex4(s: Option<&[u8]>) -> Option<u32> {
    let s = s?;
    if s.len() != 4 {
        return None;
    }
    let mut v = 0u32;
    for &c in s {
        v = (v << 4) | (c as char).to_digit(16)?;
    }
    Some(v)
}

fn put_utf8(o: &mut Vec<u8>, cp: u32) {
    if cp < 0x80 {
        o.push(cp as u8);
    } else if cp < 0x800 {
        o.extend_from_slice(&[0xc0 | (cp >> 6) as u8, 0x80 | (cp & 0x3f) as u8]);
    } else if cp < 0x10000 {
        o.extend_from_slice(&[0xe0 | (cp >> 12) as u8, 0x80 | ((cp >> 6) & 0x3f) as u8, 0x80 | (cp & 0x3f) as u8]);
    } else {
        o.extend_from_slice(&[
            0xf0 | (cp >> 18) as u8,
            0x80 | ((cp >> 12) & 0x3f) as u8,
            0x80 | ((cp >> 6) & 0x3f) as u8,
            0x80 | (cp & 0x3f) as u8,
        ]);
    }
}

// -- writer -----------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fail {
    Ok,
    NoMem,
    TooLarge,
}

#[derive(Clone, Copy)]
struct Level {
    map: bool,
    /// msgpack: a 32-bit count to patch at `patch` when the level ends.
    open: bool,
    n: u32,
    patch: usize,
}

const W_MAX_DEPTH: usize = 32;

/// A frame being written: a 4-byte length header, then one message in
/// either codec. Fails (`failed`) rather than growing past `limit` (the
/// frame size cap) or when memory runs out: the caller turns that into a
/// ResourceLimitExceeded reply.
pub struct W {
    pub buf: Vec<u8>,
    pub codec: Codec,
    pub limit: usize,
    pub failed: Fail,
    st: Vec<Level>,
}

#[derive(Clone, Copy)]
pub struct Mark {
    len: usize,
    depth: usize,
    top: Option<Level>,
}

impl W {
    pub fn new(codec: Codec, limit: usize) -> W {
        W { buf: vec![0; 4], codec, limit, failed: Fail::Ok, st: Vec::with_capacity(8) }
    }

    /// Empty again (header reserved), keeping the buffer's memory.
    pub fn start(&mut self, codec: Codec, limit: usize) {
        self.codec = codec;
        self.limit = limit;
        self.buf.truncate(4);
        if self.buf.len() < 4 {
            self.buf.resize(4, 0);
        }
        self.failed = Fail::Ok;
        self.st.clear();
    }

    pub fn shrink(&mut self) {
        self.buf = vec![0; 4];
        self.buf.shrink_to_fit();
    }

    pub fn ok(&self) -> bool {
        self.failed == Fail::Ok
    }

    pub fn mark(&self) -> Mark {
        Mark { len: self.buf.len(), depth: self.st.len(), top: self.st.last().copied() }
    }

    pub fn rewind(&mut self, m: Mark) {
        self.buf.truncate(m.len);
        self.st.truncate(m.depth);
        if let (Some(t), Some(l)) = (m.top, self.st.last_mut()) {
            *l = t;
        }
        self.failed = Fail::Ok;
    }

    /// The frame: header filled in.
    pub fn finish(&mut self) -> &[u8] {
        let n = (self.buf.len() - 4) as u32;
        self.buf[..4].copy_from_slice(&n.to_be_bytes());
        &self.buf
    }

    #[inline]
    fn put(&mut self, p: &[u8]) {
        if self.failed != Fail::Ok {
            return;
        }
        if self.buf.len() - 4 + p.len() > self.limit {
            self.failed = Fail::TooLarge;
            return;
        }
        if self.buf.capacity() - self.buf.len() < p.len() && self.buf.try_reserve(p.len()).is_err() {
            self.failed = Fail::NoMem;
            return;
        }
        self.buf.extend_from_slice(p);
    }

    #[inline]
    fn put1(&mut self, b: u8) {
        self.put(&[b]);
    }

    fn put_be(&mut self, tag: u8, v: u64, n: usize) {
        let mut b = [0u8; 9];
        b[0] = tag;
        b[1..=n].copy_from_slice(&v.to_be_bytes()[8 - n..]);
        self.put(&b[..=n]);
    }

    /// Separators (JSON) and item counts (both codecs).
    #[inline]
    fn pre(&mut self) {
        let json = self.codec == Codec::Json;
        let Some(l) = self.st.last_mut() else { return };
        let n = l.n;
        l.n += 1;
        if json {
            if !l.map {
                if n > 0 {
                    self.put1(b',');
                }
            } else if n % 2 == 0 {
                if n > 0 {
                    self.put1(b',');
                }
            } else {
                self.put1(b':');
            }
        }
    }

    fn push(&mut self, map: bool, open: bool, patch: usize) {
        if self.st.len() >= W_MAX_DEPTH {
            self.failed = Fail::NoMem;
            return;
        }
        self.st.push(Level { map, open, n: 0, patch });
    }

    pub fn nil(&mut self) {
        self.pre();
        match self.codec {
            Codec::Msgpack => self.put1(0xc0),
            Codec::Json => self.put(b"null"),
        }
    }

    pub fn bool(&mut self, b: bool) {
        self.pre();
        match self.codec {
            Codec::Msgpack => self.put1(if b { 0xc3 } else { 0xc2 }),
            Codec::Json => self.put(if b { b"true" } else { b"false" }),
        }
    }

    pub fn int(&mut self, i: i64) {
        self.pre();
        if self.codec == Codec::Json {
            let mut b = [0u8; 24];
            let n = i64_text(i, &mut b);
            self.put(&b[..n]);
            return;
        }
        if i >= 0 {
            let u = i as u64;
            if u < 128 {
                self.put1(u as u8)
            } else if u <= 0xff {
                self.put_be(0xcc, u, 1)
            } else if u <= 0xffff {
                self.put_be(0xcd, u, 2)
            } else if u <= 0xffff_ffff {
                self.put_be(0xce, u, 4)
            } else {
                self.put_be(0xcf, u, 8)
            }
        } else if i >= -32 {
            self.put1(i as i8 as u8)
        } else if i >= i8::MIN as i64 {
            self.put_be(0xd0, i as u64 & 0xff, 1)
        } else if i >= i16::MIN as i64 {
            self.put_be(0xd1, i as u64 & 0xffff, 2)
        } else if i >= i32::MIN as i64 {
            self.put_be(0xd2, i as u64 & 0xffff_ffff, 4)
        } else {
            self.put_be(0xd3, i as u64, 8)
        }
    }

    pub fn double(&mut self, d: f64) {
        self.pre();
        if self.codec == Codec::Msgpack {
            self.put_be(0xcb, d.to_bits(), 8);
            return;
        }
        if d.is_nan() {
            self.put(b"NaN");
        } else if d.is_infinite() {
            self.put(if d > 0.0 { b"Infinity" as &[u8] } else { b"-Infinity" });
        } else {
            // Shortest text that reads back as the same double; Debug always
            // has a '.' or an exponent, so Python decodes a float.
            let mut s = String::with_capacity(24);
            let _ = write!(s, "{d:?}");
            self.put(s.as_bytes());
        }
    }

    fn json_str(&mut self, p: &[u8]) {
        self.put1(b'"');
        let mut start = 0;
        for (i, &c) in p.iter().enumerate() {
            if c >= 0x20 && c != b'"' && c != b'\\' {
                continue;
            }
            self.put(&p[start..i]);
            start = i + 1;
            match c {
                b'"' => self.put(b"\\\""),
                b'\\' => self.put(b"\\\\"),
                b'\n' => self.put(b"\\n"),
                b'\r' => self.put(b"\\r"),
                b'\t' => self.put(b"\\t"),
                8 => self.put(b"\\b"),
                12 => self.put(b"\\f"),
                _ => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    self.put(&[b'\\', b'u', b'0', b'0', HEX[(c >> 4) as usize], HEX[(c & 15) as usize]]);
                }
            }
        }
        self.put(&p[start..]);
        self.put1(b'"');
    }

    /// A string; `p` must be valid UTF-8 (see `str_lossy`).
    pub fn str(&mut self, p: &[u8]) {
        self.pre();
        if self.codec == Codec::Json {
            self.json_str(p);
            return;
        }
        let n = p.len();
        if n < 32 {
            self.put1(0xa0 | n as u8)
        } else if n <= 0xff {
            self.put_be(0xd9, n as u64, 1)
        } else if n <= 0xffff {
            self.put_be(0xda, n as u64, 2)
        } else {
            self.put_be(0xdb, n as u64, 4)
        }
        self.put(p);
    }

    pub fn cstr(&mut self, s: &str) {
        self.str(s.as_bytes());
    }

    pub fn str_lossy(&mut self, p: &[u8]) {
        if std::str::from_utf8(p).is_ok() {
            self.str(p);
        } else {
            let s = utf8_lossy(p);
            self.str(&s);
        }
    }

    pub fn bin(&mut self, p: &[u8]) {
        self.pre();
        if self.codec == Codec::Msgpack {
            let n = p.len();
            if n <= 0xff {
                self.put_be(0xc4, n as u64, 1)
            } else if n <= 0xffff {
                self.put_be(0xc5, n as u64, 2)
            } else {
                self.put_be(0xc6, n as u64, 4)
            }
            self.put(p);
            return;
        }
        self.put(b"{\"$b\":\"");
        if self.failed != Fail::Ok {
            return;
        }
        let need = p.len().div_ceil(3) * 4;
        if self.buf.len() - 4 + need > self.limit {
            self.failed = Fail::TooLarge;
            return;
        }
        if self.buf.try_reserve(need).is_err() {
            self.failed = Fail::NoMem;
            return;
        }
        b64encode(&mut self.buf, p);
        self.put(b"\"}");
    }

    pub fn array(&mut self, n: u32) {
        self.pre();
        if self.codec == Codec::Json {
            self.put1(b'[');
        } else if n < 16 {
            self.put1(0x90 | n as u8)
        } else if n <= 0xffff {
            self.put_be(0xdc, n as u64, 2)
        } else {
            self.put_be(0xdd, n as u64, 4)
        }
        self.push(false, false, 0);
    }

    pub fn map(&mut self, pairs: u32) {
        self.pre();
        if self.codec == Codec::Json {
            self.put1(b'{');
        } else if pairs < 16 {
            self.put1(0x80 | pairs as u8)
        } else if pairs <= 0xffff {
            self.put_be(0xde, pairs as u64, 2)
        } else {
            self.put_be(0xdf, pairs as u64, 4)
        }
        self.push(true, false, 0);
    }

    /// An array whose length is known only at its end.
    pub fn array_open(&mut self) {
        self.pre();
        if self.codec == Codec::Json {
            self.put1(b'[');
            self.push(false, false, 0);
        } else {
            self.put_be(0xdd, 0, 4);
            let at = self.buf.len() - 4;
            self.push(false, true, at);
        }
    }

    pub fn map_open(&mut self) {
        self.pre();
        if self.codec == Codec::Json {
            self.put1(b'{');
            self.push(true, false, 0);
        } else {
            self.put_be(0xdf, 0, 4);
            let at = self.buf.len() - 4;
            self.push(true, true, at);
        }
    }

    pub fn end(&mut self) {
        let Some(l) = self.st.pop() else { return };
        if self.codec == Codec::Json {
            self.put1(if l.map { b'}' } else { b']' });
        } else if l.open && self.failed == Fail::Ok {
            let n = if l.map { l.n / 2 } else { l.n };
            self.buf[l.patch..l.patch + 4].copy_from_slice(&n.to_be_bytes());
        }
    }

    pub fn raw(&mut self, p: &[u8]) {
        self.pre();
        self.put(p);
    }

    /// Re-encode a decoded value (the `id` of `hello`).
    pub fn val(&mut self, v: &Value) {
        match v {
            Value::Nil => self.nil(),
            Value::Bool(b) => self.bool(*b),
            Value::Int(i) => self.int(*i),
            Value::BigInt(t) => {
                if self.codec == Codec::Json {
                    self.raw(t)
                } else {
                    self.str(t)
                }
            }
            Value::Float(d) => self.double(*d),
            Value::Str(s) => self.str_lossy(s),
            Value::Bin(b) => self.bin(b),
            Value::Arr(a) => {
                self.array(a.len() as u32);
                for x in a {
                    self.val(x);
                }
                self.end();
            }
            Value::Map(m) => {
                self.map(m.len() as u32);
                for (k, x) in m {
                    self.val(k);
                    self.val(x);
                }
                self.end();
            }
            Value::Ext(..) => self.nil(), // never echoed
        }
    }
}

pub fn i64_text(v: i64, out: &mut [u8; 24]) -> usize {
    let mut t = [0u8; 20];
    let mut n = 0;
    let mut u = v.unsigned_abs();
    loop {
        t[n] = b'0' + (u % 10) as u8;
        n += 1;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    let mut k = 0;
    if v < 0 {
        out[0] = b'-';
        k = 1;
    }
    while n > 0 {
        n -= 1;
        out[k] = t[n];
        k += 1;
    }
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(codec: Codec, v: &Value) -> Value {
        let mut w = W::new(codec, 1 << 20);
        w.val(v);
        let b = w.finish().to_vec();
        decode_msg(codec, &b[4..]).unwrap()
    }

    #[test]
    fn both_codecs_round_trip_a_message() {
        let v = Value::Map(vec![
            (Value::Str(b"id".to_vec()), Value::Int(-1234567890123)),
            (Value::Str(b"s".to_vec()), Value::Str("é\"\\\n\u{1}x".as_bytes().to_vec())),
            (Value::Str(b"f".to_vec()), Value::Float(1.0)),
            (Value::Str(b"g".to_vec()), Value::Float(1e300)),
            (Value::Str(b"b".to_vec()), Value::Bin(vec![0, 1, 2, 255])),
            (Value::Str(b"a".to_vec()), Value::Arr(vec![Value::Nil, Value::Bool(true), Value::Int(i64::MIN)])),
        ]);
        for c in [Codec::Json, Codec::Msgpack] {
            assert_eq!(roundtrip(c, &v), v, "{c:?}");
        }
    }

    #[test]
    fn json_numbers_and_escapes() {
        let txt = format!("{{\"a\": 1.0, \"b\": 12345678901234567890123, \"c\": -Infinity, \"d\": \"{0}ud83d{0}ude00{0}ud800\"}}", '\\');
        let m = decode_msg(Codec::Json, txt.as_bytes()).unwrap();
        assert_eq!(m.get("a"), Some(&Value::Float(1.0)));
        assert_eq!(m.get("b"), Some(&Value::BigInt(b"12345678901234567890123".to_vec())));
        assert_eq!(m.get("c"), Some(&Value::Float(f64::NEG_INFINITY)));
        assert_eq!(m.get("d"), Some(&Value::Str(vec![0xf0, 0x9f, 0x98, 0x80, 0xed, 0xa0, 0x80])));
    }

    #[test]
    fn json_wrappers() {
        let m = decode_msg(Codec::Json, br#"{"a": {"$b": "AAEC"}, "b": {"$d": ["$b", 1]}, "c": {"$x": ["date", "2024-01-02"]}}"#).unwrap();
        assert_eq!(m.get("a"), Some(&Value::Bin(vec![0, 1, 2])));
        assert_eq!(m.get("b"), Some(&Value::Map(vec![(Value::Str(b"$b".to_vec()), Value::Int(1))])));
        assert_eq!(m.get("c"), Some(&Value::Ext(17, Box::new(Value::Str(b"2024-01-02".to_vec())))));
    }

    #[test]
    fn lossy_matches_the_c_worker() {
        assert_eq!(utf8_lossy(b"a\xe2\x82b"), "a\u{fffd}\u{fffd}b".as_bytes());
        assert_eq!(utf8_chars("é😀".as_bytes()), Some(2));
        assert_eq!(utf8_chars(b"\xff"), None);
    }

    #[test]
    fn frame_cap() {
        let mut w = W::new(Codec::Msgpack, 10);
        w.str(b"0123456789abc");
        assert_eq!(w.failed, Fail::TooLarge);
    }
}
