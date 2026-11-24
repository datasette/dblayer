//! The `msgpack` codec, as `msgpack.Packer(use_bin_type=True)` and
//! `unpackb(raw=False, strict_map_key=False)` with the protocol's ext hook:
//! ext 1 is an integer outside 64 bits as decimal text; ext 16 to 23 are the
//! registered value types, their payload packed with this same codec.

use crate::value::{EXT_BIGINT, Value, tag_for};

const MAX_DEPTH: usize = 200;

pub fn decode(data: &[u8]) -> Result<Value, String> {
    let mut r = Reader { s: data, i: 0, depth: 0 };
    let v = r.value()?;
    if r.i != data.len() {
        return Err(format!("extra data after the message ({} bytes)", data.len() - r.i));
    }
    Ok(v)
}

struct Reader<'a> {
    s: &'a [u8],
    i: usize,
    depth: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.i.checked_add(n).filter(|&e| e <= self.s.len()).ok_or("truncated message")?;
        let b = &self.s[self.i..end];
        self.i = end;
        Ok(b)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn be(&mut self, n: usize) -> Result<u64, String> {
        let b = self.take(n)?;
        Ok(b.iter().fold(0u64, |acc, &x| (acc << 8) | x as u64))
    }

    /// A count of items still to come: each needs at least one byte, so a
    /// count over what is left is refused before anything is allocated.
    fn count(&mut self, n: usize) -> Result<usize, String> {
        let c = self.be(n)? as usize;
        if c > self.s.len() - self.i {
            return Err("length exceeds the message".into());
        }
        Ok(c)
    }

    fn len(&mut self, n: usize) -> Result<usize, String> {
        self.count(n)
    }

    fn value(&mut self) -> Result<Value, String> {
        let b = self.u8()?;
        Ok(match b {
            0x00..=0x7f => Value::Int(b as i128),
            0xe0..=0xff => Value::Int((b as i8) as i128),
            0x80..=0x8f => self.map((b & 0x0f) as usize)?,
            0x90..=0x9f => self.array((b & 0x0f) as usize)?,
            0xa0..=0xbf => self.str((b & 0x1f) as usize)?,
            0xc0 => Value::Null,
            0xc2 => Value::Bool(false),
            0xc3 => Value::Bool(true),
            0xc4 => {
                let n = self.len(1)?;
                Value::Bytes(self.take(n)?.to_vec())
            }
            0xc5 => {
                let n = self.len(2)?;
                Value::Bytes(self.take(n)?.to_vec())
            }
            0xc6 => {
                let n = self.len(4)?;
                Value::Bytes(self.take(n)?.to_vec())
            }
            0xc7 => {
                let n = self.len(1)?;
                self.ext(n)?
            }
            0xc8 => {
                let n = self.len(2)?;
                self.ext(n)?
            }
            0xc9 => {
                let n = self.len(4)?;
                self.ext(n)?
            }
            0xca => Value::Float(f32::from_bits(self.be(4)? as u32) as f64),
            0xcb => Value::Float(f64::from_bits(self.be(8)?)),
            0xcc => Value::Int(self.be(1)? as i128),
            0xcd => Value::Int(self.be(2)? as i128),
            0xce => Value::Int(self.be(4)? as i128),
            0xcf => Value::Int(self.be(8)? as i128),
            0xd0 => Value::Int((self.be(1)? as u8 as i8) as i128),
            0xd1 => Value::Int((self.be(2)? as u16 as i16) as i128),
            0xd2 => Value::Int((self.be(4)? as u32 as i32) as i128),
            0xd3 => Value::Int((self.be(8)? as i64) as i128),
            0xd4 => self.ext(1)?,
            0xd5 => self.ext(2)?,
            0xd6 => self.ext(4)?,
            0xd7 => self.ext(8)?,
            0xd8 => self.ext(16)?,
            0xd9 => {
                let n = self.len(1)?;
                self.str(n)?
            }
            0xda => {
                let n = self.len(2)?;
                self.str(n)?
            }
            0xdb => {
                let n = self.len(4)?;
                self.str(n)?
            }
            0xdc => {
                let n = self.count(2)?;
                self.array(n)?
            }
            0xdd => {
                let n = self.count(4)?;
                self.array(n)?
            }
            0xde => {
                let n = self.count(2)?;
                self.map(n)?
            }
            0xdf => {
                let n = self.count(4)?;
                self.map(n)?
            }
            0xc1 => return Err("invalid byte 0xc1".into()),
        })
    }

    fn str(&mut self, n: usize) -> Result<Value, String> {
        let b = self.take(n)?;
        // msgpack raises on invalid UTF-8 with raw=False, except that the
        // parent's surrogatepass never applies to msgpack: refuse.
        String::from_utf8(b.to_vec()).map(Value::Str).map_err(|_| "invalid UTF-8 in string".into())
    }

    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err("nested too deeply".into());
        }
        Ok(())
    }

    fn array(&mut self, n: usize) -> Result<Value, String> {
        self.enter()?;
        let mut items = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            items.push(self.value()?);
        }
        self.depth -= 1;
        Ok(Value::List(items))
    }

    fn map(&mut self, n: usize) -> Result<Value, String> {
        self.enter()?;
        let mut items: Vec<(Value, Value)> = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let k = self.value()?;
            if matches!(k, Value::List(_) | Value::Map(_)) {
                return Err("unhashable map key".into());
            }
            let v = self.value()?;
            if let Some(slot) = items.iter_mut().find(|(ek, _)| *ek == k) {
                slot.1 = v;
            } else {
                items.push((k, v));
            }
        }
        self.depth -= 1;
        Ok(Value::Map(items))
    }

    fn ext(&mut self, n: usize) -> Result<Value, String> {
        let code = self.u8()? as i8;
        let data = self.take(n)?;
        if code == EXT_BIGINT {
            let text = std::str::from_utf8(data).map_err(|_| "bad big integer")?;
            let digits = text.strip_prefix('-').unwrap_or(text);
            if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
                return Err("bad big integer".into());
            }
            return Ok(match text.parse::<i128>() {
                Ok(i) => Value::Int(i),
                Err(_) => Value::BigInt(text.to_string()),
            });
        }
        if tag_for(code).is_none() {
            return Err(format!("unknown ext type {code}"));
        }
        let mut inner = Reader { s: data, i: 0, depth: self.depth + 1 };
        if inner.depth > MAX_DEPTH {
            return Err("nested too deeply".into());
        }
        let payload = inner.value()?;
        if inner.i != data.len() {
            return Err("extra data in ext payload".into());
        }
        Ok(Value::Ext(code, Box::new(payload)))
    }
}

// -- encoding --------------------------------------------------------------------

pub fn encode(v: &Value) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    write(&mut out, v);
    out
}

pub fn write(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.push(0xc0),
        Value::Bool(b) => out.push(if *b { 0xc3 } else { 0xc2 }),
        Value::Int(i) => write_int(out, *i),
        Value::BigInt(s) => write_ext(out, EXT_BIGINT, s.as_bytes()),
        Value::Float(f) => {
            out.push(0xcb);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Value::Str(s) => write_str(out, s),
        Value::Bytes(b) => {
            let n = b.len();
            if n < 256 {
                out.extend_from_slice(&[0xc4, n as u8]);
            } else if n < 65536 {
                out.push(0xc5);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            } else {
                out.push(0xc6);
                out.extend_from_slice(&(n as u32).to_be_bytes());
            }
            out.extend_from_slice(b);
        }
        Value::List(items) => {
            write_array_header(out, items.len());
            for x in items {
                write(out, x);
            }
        }
        Value::Map(items) => {
            let n = items.len();
            if n < 16 {
                out.push(0x80 | n as u8);
            } else if n < 65536 {
                out.push(0xde);
                out.extend_from_slice(&(n as u16).to_be_bytes());
            } else {
                out.push(0xdf);
                out.extend_from_slice(&(n as u32).to_be_bytes());
            }
            for (k, x) in items {
                write(out, k);
                write(out, x);
            }
        }
        Value::Ext(code, payload) => {
            let mut inner = Vec::with_capacity(32);
            write(&mut inner, payload);
            write_ext(out, *code, &inner);
        }
    }
}

pub fn write_array_header(out: &mut Vec<u8>, n: usize) {
    if n < 16 {
        out.push(0x90 | n as u8);
    } else if n < 65536 {
        out.push(0xdc);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
}

pub fn write_int(out: &mut Vec<u8>, i: i128) {
    if (0..128).contains(&i) {
        out.push(i as u8);
    } else if (-32..0).contains(&i) {
        out.push(i as i8 as u8);
    } else if i >= 0 {
        if i < 256 {
            out.extend_from_slice(&[0xcc, i as u8]);
        } else if i < 65536 {
            out.push(0xcd);
            out.extend_from_slice(&(i as u16).to_be_bytes());
        } else if i <= u32::MAX as i128 {
            out.push(0xce);
            out.extend_from_slice(&(i as u32).to_be_bytes());
        } else if i <= u64::MAX as i128 {
            out.push(0xcf);
            out.extend_from_slice(&(i as u64).to_be_bytes());
        } else {
            write_ext(out, EXT_BIGINT, i.to_string().as_bytes());
        }
    } else if i >= i8::MIN as i128 {
        out.extend_from_slice(&[0xd0, i as i8 as u8]);
    } else if i >= i16::MIN as i128 {
        out.push(0xd1);
        out.extend_from_slice(&(i as i16).to_be_bytes());
    } else if i >= i32::MIN as i128 {
        out.push(0xd2);
        out.extend_from_slice(&(i as i32).to_be_bytes());
    } else if i >= i64::MIN as i128 {
        out.push(0xd3);
        out.extend_from_slice(&(i as i64).to_be_bytes());
    } else {
        write_ext(out, EXT_BIGINT, i.to_string().as_bytes());
    }
}

pub fn write_str(out: &mut Vec<u8>, s: &str) {
    let n = s.len();
    if n < 32 {
        out.push(0xa0 | n as u8);
    } else if n < 256 {
        out.extend_from_slice(&[0xd9, n as u8]);
    } else if n < 65536 {
        out.push(0xda);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
    out.extend_from_slice(s.as_bytes());
}

pub fn write_ext(out: &mut Vec<u8>, code: i8, data: &[u8]) {
    let n = data.len();
    match n {
        1 => out.push(0xd4),
        2 => out.push(0xd5),
        4 => out.push(0xd6),
        8 => out.push(0xd7),
        16 => out.push(0xd8),
        _ if n < 256 => out.extend_from_slice(&[0xc7, n as u8]),
        _ if n < 65536 => {
            out.push(0xc8);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        _ => {
            out.push(0xc9);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        }
    }
    out.push(code as u8);
    out.extend_from_slice(data);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let v = Value::obj(vec![
            ("ints", Value::List([0i128, 127, 128, -1, -33, 300, -300, 1 << 40, -(1 << 40), u64::MAX as i128, i64::MIN as i128, 1 << 100].into_iter().map(Value::Int).collect())),
            ("f", Value::Float(-2.5)),
            ("b", Value::Bytes(vec![1; 300])),
            ("s", Value::str("x".repeat(70000))),
            ("e", Value::Ext(22, Box::new(Value::List(vec![Value::List(vec![Value::Int(1), Value::str("a")])])))),
            ("m", Value::Map(vec![(Value::Int(1), Value::Null)])),
        ]);
        assert_eq!(decode(&encode(&v)).unwrap(), v);
    }

    #[test]
    fn rejects_bad_input() {
        let deep = [vec![0x91u8; 5000], vec![0xc0]].concat();
        for bad in [&b""[..], b"\xc1", b"\x81", b"\xdf\xff\xff\xff\xff", b"\xdd\x7f\xff\xff\xff", b"\x81\xa4type\xdb\xff\xff\xff\xff", b"\x81\xa4type\xc7\x05\x02abc", b"\x81\xa4type\xd4\x05x", &deep, b"\x80\x80"] {
            assert!(decode(bad).is_err(), "{bad:?}");
        }
    }
}
