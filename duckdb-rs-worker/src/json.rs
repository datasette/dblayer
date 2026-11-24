//! The `json` codec, as Python's `json` module with the protocol's hooks
//! writes and reads it: `NaN`/`Infinity` allowed, integers of any size,
//! `{"$b": base64}` for bytes, `{"$d": [k, v]}` for an escaped one-key dict,
//! `{"$x": [tag, payload]}` for registered value types.

use crate::value::{Value, code_for, tag_for};

const MAX_DEPTH: usize = 200;

pub fn decode(data: &[u8]) -> Result<Value, String> {
    let text = String::from_utf8_lossy(data);
    let mut p = Parser { s: text.as_bytes(), i: 0, depth: 0 };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.s.len() {
        return Err(format!("extra data at byte {}", p.i));
    }
    Ok(v)
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    depth: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while let Some(&c) = self.s.get(self.i) {
            if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                self.i += 1;
            } else {
                break;
            }
        }
    }

    fn eat(&mut self, lit: &str) -> bool {
        if self.s[self.i..].starts_with(lit.as_bytes()) {
            self.i += lit.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Result<Value, String> {
        let Some(&c) = self.s.get(self.i) else {
            return Err("unexpected end of input".into());
        };
        match c {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => Ok(Value::Str(self.string()?)),
            b't' if self.eat("true") => Ok(Value::Bool(true)),
            b'f' if self.eat("false") => Ok(Value::Bool(false)),
            b'n' if self.eat("null") => Ok(Value::Null),
            b'N' if self.eat("NaN") => Ok(Value::Float(f64::NAN)),
            b'I' if self.eat("Infinity") => Ok(Value::Float(f64::INFINITY)),
            b'-' if self.eat("-Infinity") => Ok(Value::Float(f64::NEG_INFINITY)),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err(format!("unexpected character at byte {}", self.i)),
        }
    }

    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err("nested too deeply".into());
        }
        Ok(())
    }

    fn array(&mut self) -> Result<Value, String> {
        self.enter()?;
        self.i += 1;
        let mut items = Vec::new();
        self.ws();
        if self.s.get(self.i) == Some(&b']') {
            self.i += 1;
            self.depth -= 1;
            return Ok(Value::List(items));
        }
        loop {
            self.ws();
            items.push(self.value()?);
            self.ws();
            match self.s.get(self.i) {
                Some(b',') => self.i += 1,
                Some(b']') => {
                    self.i += 1;
                    break;
                }
                _ => return Err(format!("expected , or ] at byte {}", self.i)),
            }
        }
        self.depth -= 1;
        Ok(Value::List(items))
    }

    fn object(&mut self) -> Result<Value, String> {
        self.enter()?;
        self.i += 1;
        let mut items: Vec<(Value, Value)> = Vec::new();
        self.ws();
        if self.s.get(self.i) == Some(&b'}') {
            self.i += 1;
        } else {
            loop {
                self.ws();
                if self.s.get(self.i) != Some(&b'"') {
                    return Err(format!("expected a key at byte {}", self.i));
                }
                let k = self.string()?;
                self.ws();
                if self.s.get(self.i) != Some(&b':') {
                    return Err(format!("expected : at byte {}", self.i));
                }
                self.i += 1;
                self.ws();
                let v = self.value()?;
                // Python dicts: a repeated key keeps its first position and
                // its last value.
                if let Some(slot) = items.iter_mut().find(|(ek, _)| matches!(ek, Value::Str(s) if *s == k)) {
                    slot.1 = v;
                } else {
                    items.push((Value::Str(k), v));
                }
                self.ws();
                match self.s.get(self.i) {
                    Some(b',') => self.i += 1,
                    Some(b'}') => {
                        self.i += 1;
                        break;
                    }
                    _ => return Err(format!("expected , or }} at byte {}", self.i)),
                }
            }
        }
        self.depth -= 1;
        hook(items)
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let h = self.s.get(self.i..self.i + 4).ok_or("truncated \\u escape")?;
        let h = std::str::from_utf8(h).map_err(|_| "bad \\u escape")?;
        let n = u32::from_str_radix(h, 16).map_err(|_| "bad \\u escape")?;
        if !h.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("bad \\u escape".into());
        }
        self.i += 4;
        Ok(n)
    }

    fn string(&mut self) -> Result<String, String> {
        self.i += 1; // opening quote
        let mut out: Vec<u8> = Vec::new();
        loop {
            let Some(&c) = self.s.get(self.i) else {
                return Err("unterminated string".into());
            };
            self.i += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    let Some(&e) = self.s.get(self.i) else {
                        return Err("unterminated string".into());
                    };
                    self.i += 1;
                    let ch = match e {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let hi = self.hex4()?;
                            let cp = if (0xD800..0xDC00).contains(&hi)
                                && self.s.get(self.i..self.i + 2) == Some(b"\\u")
                            {
                                let save = self.i;
                                self.i += 2;
                                let lo = self.hex4()?;
                                if (0xDC00..0xE000).contains(&lo) {
                                    0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                                } else {
                                    self.i = save;
                                    0xFFFD
                                }
                            } else {
                                hi
                            };
                            // A lone surrogate (Python keeps it; Rust strings
                            // cannot hold one): the replacement character.
                            char::from_u32(cp).unwrap_or('\u{FFFD}')
                        }
                        _ => return Err(format!("bad escape at byte {}", self.i)),
                    };
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                }
                0..=0x1f => return Err(format!("control character in string at byte {}", self.i)),
                _ => out.push(c),
            }
        }
        String::from_utf8(out).map_err(|_| "invalid UTF-8 in string".to_string())
    }

    fn number(&mut self) -> Result<Value, String> {
        let start = self.i;
        if self.s.get(self.i) == Some(&b'-') {
            self.i += 1;
        }
        let digits = |p: &mut Self| {
            let s = p.i;
            while matches!(p.s.get(p.i), Some(b'0'..=b'9')) {
                p.i += 1;
            }
            p.i - s
        };
        let int_start = self.i;
        if digits(self) == 0 {
            return Err(format!("bad number at byte {start}"));
        }
        if self.s[int_start] == b'0' && self.i - int_start > 1 {
            // Python's json reads "01" as 0 followed by extra data.
            return Err(format!("bad number at byte {start}"));
        }
        let mut float = false;
        if self.s.get(self.i) == Some(&b'.') {
            self.i += 1;
            if digits(self) == 0 {
                return Err(format!("bad number at byte {start}"));
            }
            float = true;
        }
        if matches!(self.s.get(self.i), Some(b'e' | b'E')) {
            self.i += 1;
            if matches!(self.s.get(self.i), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if digits(self) == 0 {
                return Err(format!("bad number at byte {start}"));
            }
            float = true;
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).map_err(|_| "bad number")?;
        if float {
            text.parse::<f64>().map(Value::Float).map_err(|_| format!("bad number {text}"))
        } else {
            Ok(match text.parse::<i128>() {
                Ok(i) => Value::Int(i),
                Err(_) => Value::BigInt(text.trim_start_matches('+').to_string()),
            })
        }
    }
}

/// `protocol._json_hook`: one-key dicts that are codec wrappers.
fn hook(mut items: Vec<(Value, Value)>) -> Result<Value, String> {
    if items.len() == 1 {
        let is = |k: &str| matches!(&items[0].0, Value::Str(s) if s == k);
        if is("$b") {
            let Value::Str(b) = &items[0].1 else {
                return Err("$b payload is not a string".into());
            };
            return b64decode(b).map(Value::Bytes);
        }
        if is("$d") {
            let Value::List(kv) = &items[0].1 else {
                return Err("$d payload is not a pair".into());
            };
            if kv.len() != 2 {
                return Err("$d payload is not a pair".into());
            }
            return Ok(Value::Map(vec![(kv[0].clone(), kv[1].clone())]));
        }
        if is("$x") {
            let (_, payload) = items.pop().expect("one item");
            let Value::List(mut tp) = payload else {
                return Err("$x payload is not a pair".into());
            };
            if tp.len() != 2 {
                return Err("$x payload is not a pair".into());
            }
            let inner = tp.pop().expect("two items");
            let Value::Str(tag) = tp.pop().expect("two items") else {
                return Err("$x tag is not a string".into());
            };
            let code = code_for(&tag).ok_or_else(|| format!("unknown value type {tag:?}"))?;
            return Ok(Value::Ext(code, Box::new(inner)));
        }
    }
    Ok(Value::Map(items))
}

// -- encoding --------------------------------------------------------------------

pub fn encode(v: &Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    write(&mut out, v);
    out
}

pub fn write(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Int(i) => out.extend_from_slice(i.to_string().as_bytes()),
        Value::BigInt(s) => out.extend_from_slice(s.as_bytes()),
        Value::Float(f) => write_float(out, *f),
        Value::Str(s) => write_str(out, s),
        Value::Bytes(b) => {
            out.extend_from_slice(b"{\"$b\":\"");
            b64encode(out, b);
            out.extend_from_slice(b"\"}");
        }
        Value::List(items) => {
            out.push(b'[');
            for (n, x) in items.iter().enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                write(out, x);
            }
            out.push(b']');
        }
        Value::Map(items) => {
            out.push(b'{');
            for (n, (k, x)) in items.iter().enumerate() {
                if n > 0 {
                    out.push(b',');
                }
                match k {
                    Value::Str(s) => write_str(out, s),
                    // Python's json writes int/float/bool/None keys as
                    // strings; nothing else reaches here.
                    Value::Int(i) => write_str(out, &i.to_string()),
                    Value::Bool(b) => write_str(out, if *b { "true" } else { "false" }),
                    Value::Null => write_str(out, "null"),
                    other => write_str(out, &format!("{other:?}")),
                }
                out.push(b':');
                write(out, x);
            }
            out.push(b'}');
        }
        Value::Ext(code, payload) => {
            out.extend_from_slice(b"{\"$x\":[");
            write_str(out, tag_for(*code).unwrap_or("?"));
            out.push(b',');
            write(out, payload);
            out.extend_from_slice(b"]}");
        }
    }
}

pub fn write_float(out: &mut Vec<u8>, f: f64) {
    if f.is_nan() {
        out.extend_from_slice(b"NaN");
    } else if f.is_infinite() {
        out.extend_from_slice(if f > 0.0 { b"Infinity" } else { b"-Infinity" });
    } else {
        // Debug formatting is the shortest round-trip form and always has
        // a '.' or an exponent, so Python reads it back as a float.
        out.extend_from_slice(format!("{f:?}").as_bytes());
    }
}

pub fn write_str(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let esc: Option<&[u8]> = match b {
            b'"' => Some(b"\\\""),
            b'\\' => Some(b"\\\\"),
            b'\n' => Some(b"\\n"),
            b'\r' => Some(b"\\r"),
            b'\t' => Some(b"\\t"),
            0..=0x1f => None,
            _ => continue,
        };
        out.extend_from_slice(&bytes[start..i]);
        match esc {
            Some(e) => out.extend_from_slice(e),
            None => out.extend_from_slice(format!("\\u{b:04x}").as_bytes()),
        }
        start = i + 1;
    }
    out.extend_from_slice(&bytes[start..]);
    out.push(b'"');
}

// -- base64 ----------------------------------------------------------------------

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn b64encode(out: &mut Vec<u8>, data: &[u8]) {
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(B64[(n >> 18) as usize & 63]);
        out.push(B64[(n >> 12) as usize & 63]);
        out.push(if chunk.len() > 1 { B64[(n >> 6) as usize & 63] } else { b'=' });
        out.push(if chunk.len() > 2 { B64[n as usize & 63] } else { b'=' });
    }
}

/// Python's `base64.b64decode` (non-alphabet characters are discarded;
/// padding must be right).
pub fn b64decode(s: &str) -> Result<Vec<u8>, String> {
    let mut vals = Vec::with_capacity(s.len());
    let mut pad = 0;
    for &c in s.as_bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => {
                pad += 1;
                continue;
            }
            _ => continue,
        };
        if pad > 0 {
            return Err("base64: data after padding".into());
        }
        vals.push(v);
    }
    if vals.len() % 4 == 1 || (pad > 0 && (vals.len() + pad) % 4 != 0) {
        return Err("base64: incorrect padding".into());
    }
    if pad == 0 && vals.len() % 4 != 0 {
        return Err("base64: incorrect padding".into());
    }
    let mut out = Vec::with_capacity(vals.len() * 3 / 4);
    for chunk in vals.chunks(4) {
        let mut n = 0u32;
        for (i, &v) in chunk.iter().enumerate() {
            n |= (v as u32) << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let v = Value::obj(vec![
            ("a", Value::Int(-5)),
            ("b", Value::Float(1.5)),
            ("c", Value::Float(f64::NAN)),
            ("d", Value::Bytes(vec![0, 255, 7])),
            ("e", Value::str("é\"\n\u{1}")),
            ("f", Value::Ext(17, Box::new(Value::str("2024-01-02")))),
            ("g", Value::BigInt("123456789012345678901234567890123456789012".into())),
        ]);
        let enc = encode(&v);
        let back = decode(&enc).unwrap();
        assert_eq!(format!("{back:?}"), format!("{v:?}"));
    }

    #[test]
    fn floats_stay_floats() {
        for f in [1.0, 1e16, 1.5e-7, -0.0, 123456789.0] {
            let mut out = Vec::new();
            write_float(&mut out, f);
            let s = String::from_utf8(out).unwrap();
            assert!(s.contains('.') || s.contains('e'), "{s}");
            assert_eq!(s.parse::<f64>().unwrap(), f);
        }
    }

    #[test]
    fn base64() {
        for data in [&b""[..], b"a", b"ab", b"abc", b"abcd", b"\x00\xff"] {
            let mut out = Vec::new();
            b64encode(&mut out, data);
            assert_eq!(b64decode(std::str::from_utf8(&out).unwrap()).unwrap(), data);
        }
        assert!(b64decode("abc").is_err());
    }

    #[test]
    fn rejects_bad_input() {
        for bad in [&b"{"[..], b"[1,", b"{\"a\" 1}", b"01", b"\"\\u12\"", b"{\"$b\": 5}", b"1 2", b"\"a\x01\""] {
            assert!(decode(bad).is_err(), "{:?}", String::from_utf8_lossy(bad));
        }
        let deep = "[".repeat(5000) + &"]".repeat(5000);
        assert!(decode(deep.as_bytes()).is_err());
    }
}
