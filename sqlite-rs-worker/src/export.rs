//! The `export` request: a result serialised to CSV, TSV or JSON in the
//! worker, straight from the column values, as docs/export-formats.md
//! defines the row bytes (a port of the C worker's export.c). An export is
//! a `Stream` with `ex` set, so credit, `more`, `cancel` of a paused export
//! and closing a connection reuse the stream code. Transports:
//!
//!   "frames": codec frames {"id", "data": <bin>, "rows": 1}, credit-based;
//!   "raw":    frames whose payload is 0x00, the request id (8 bytes, big
//!             endian), then the bytes, never touched by the codec;
//!   "fifo":   the bytes go to a named pipe the parent created and holds
//!             open; the request's one reply comes at the end.
//!
//! The C worker's fourth transport, "socket" (handing the worker the HTTP
//! client's descriptor), is not implemented here; `hello` does not list it,
//! so the parent never asks.

use std::io::Write as _;

use crate::codec::{self, Value, W};
use crate::errors::E;
use crate::ffi::{self, Db, Stmt};
use crate::worker::{self, Ctx, Shared, Stream, now_ns};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Enc {
    Csv,
    Tsv,
    JsonObjects,
    JsonArrays,
    Ndjson,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum T {
    Frames,
    Raw,
    Fifo,
}

const RAW_HEAD: usize = 13; // frame length (4) + 0x00 + request id (8)

pub struct Export {
    enc: Enc,
    transport: T,
    chunk_bytes: usize,
    max_rows: Option<i64>,
    rows: i64,
    truncated: bool,
    first_row: bool,
    out: Vec<u8>,
    /// JSON objects: `"name":` per column, None for a repeated name.
    keys: Vec<Option<Vec<u8>>>,
}

impl Export {
    pub fn unlisted(&self) -> bool {
        self.transport == T::Fifo
    }

    fn base(&self) -> usize {
        if self.transport == T::Raw { RAW_HEAD } else { 0 }
    }
}

pub fn hello(w: &mut W) {
    w.map_open();
    w.cstr("formats");
    w.array(3);
    w.cstr("csv");
    w.cstr("tsv");
    w.cstr("json");
    w.end();
    w.cstr("transports");
    w.array(3);
    w.cstr("frames");
    w.cstr("raw");
    w.cstr("fifo");
    w.end();
    w.end();
}

pub fn new_stream(msg: &Value, id: i64) -> Result<Stream, E> {
    let enc = match msg.get("encoding") {
        Some(v) if v.is_str("csv") => Enc::Csv,
        Some(v) if v.is_str("tsv") => Enc::Tsv,
        Some(v) if v.is_str("json-objects") => Enc::JsonObjects,
        Some(v) if v.is_str("json-arrays") => Enc::JsonArrays,
        Some(v) if v.is_str("ndjson") => Enc::Ndjson,
        _ => return Err(E::builtin("ValueError", "unknown export encoding")),
    };
    let transport = match msg.get("transport") {
        None | Some(Value::Nil) => T::Frames,
        Some(v) if v.is_str("frames") => T::Frames,
        Some(v) if v.is_str("raw") => T::Raw,
        Some(v) if v.is_str("fifo") => T::Fifo,
        Some(v) if v.is_str("socket") => {
            return Err(E::new("NotSupported", "the Rust worker has no socket handoff"));
        }
        _ => return Err(E::builtin("ValueError", "unknown export transport")),
    };
    let chunk = msg.int_or("chunk_bytes", 65536);
    if !(1..=(1 << 26)).contains(&chunk) {
        return Err(E::builtin("ValueError", "chunk_bytes must be between 1 and 67108864"));
    }
    let max_rows = msg.int_field("max_rows", 0).map(|m| m.max(0));
    let mut ex = Export {
        enc,
        transport,
        chunk_bytes: chunk as usize + if transport == T::Raw { RAW_HEAD } else { 0 },
        max_rows,
        rows: 0,
        truncated: false,
        first_row: true,
        out: Vec::new(),
        keys: Vec::new(),
    };
    if transport == T::Raw {
        ex.out.reserve(RAW_HEAD + chunk as usize + 2);
        ex.out.resize(RAW_HEAD, 0);
    }
    let mut s = Stream::new(id, msg.int_or("credit", 1));
    s.time_limit = msg.int_field("time_limit_ms", 0).map(|v| v as f64);
    s.ex = Some(Box::new(ex));
    Ok(s)
}

// -- values -------------------------------------------------------------------

/// Python's float.__repr__ for a finite double: the shortest digits that
/// read back as `d`, positional for exponents -4..15 with at least one digit
/// after the point, else d.ddde+XX.
pub fn py_float_repr(d: f64, o: &mut Vec<u8>) {
    let s = format!("{d:e}"); // shortest round-trip digits, as d.ddde<exp>
    let (mant, exp) = s.split_once('e').unwrap_or((&s, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let (neg, mant) = match mant.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, mant),
    };
    let mut dig: Vec<u8> = mant.bytes().filter(|c| c.is_ascii_digit()).collect();
    while dig.len() > 1 && *dig.last().unwrap() == b'0' {
        dig.pop();
    }
    let nd = dig.len() as i32;
    if neg {
        o.push(b'-');
    }
    if (-4..16).contains(&exp) {
        if exp >= 0 {
            for i in 0..=exp {
                o.push(if i < nd { dig[i as usize] } else { b'0' });
            }
            o.push(b'.');
            if nd > exp + 1 {
                o.extend_from_slice(&dig[(exp + 1) as usize..]);
            } else {
                o.push(b'0');
            }
        } else {
            o.extend_from_slice(b"0.");
            for _ in 0..(-exp - 1) {
                o.push(b'0');
            }
            o.extend_from_slice(&dig);
        }
    } else {
        o.push(dig[0]);
        if nd > 1 {
            o.push(b'.');
            o.extend_from_slice(&dig[1..]);
        }
        let _ = write!(o, "e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.unsigned_abs());
    }
}

fn int_text(o: &mut Vec<u8>, v: i64) {
    let mut b = [0u8; 24];
    let n = codec::i64_text(v, &mut b);
    o.extend_from_slice(&b[..n]);
}

/// A CSV/TSV field: quoted when empty or holding the delimiter, `"`, CR or
/// LF; quotes doubled.
fn csv_field(o: &mut Vec<u8>, p: &[u8], delim: u8) {
    let quote = p.is_empty() || p.iter().any(|&c| c == delim || c == b'"' || c == b'\n' || c == b'\r');
    if !quote {
        o.extend_from_slice(p);
        return;
    }
    o.push(b'"');
    for &c in p {
        if c == b'"' {
            o.push(b'"');
        }
        o.push(c);
    }
    o.push(b'"');
}

/// A JSON string as Python's json.dumps(s, ensure_ascii=False) writes it.
pub fn json_str(o: &mut Vec<u8>, p: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    o.push(b'"');
    let mut start = 0;
    for (i, &c) in p.iter().enumerate() {
        if c >= 0x20 && c != b'"' && c != b'\\' {
            continue;
        }
        o.extend_from_slice(&p[start..i]);
        start = i + 1;
        match c {
            b'"' => o.extend_from_slice(b"\\\""),
            b'\\' => o.extend_from_slice(b"\\\\"),
            b'\n' => o.extend_from_slice(b"\\n"),
            b'\r' => o.extend_from_slice(b"\\r"),
            b'\t' => o.extend_from_slice(b"\\t"),
            8 => o.extend_from_slice(b"\\b"),
            12 => o.extend_from_slice(b"\\f"),
            _ => o.extend_from_slice(&[b'\\', b'u', b'0', b'0', HEX[(c >> 4) as usize], HEX[(c & 15) as usize]]),
        }
    }
    o.extend_from_slice(&p[start..]);
    o.push(b'"');
}

fn text<'a>(st: &'a Stmt, i: i32) -> Result<&'a [u8], E> {
    let p = st.column_text(i);
    if std::str::from_utf8(p).is_err() {
        return Err(worker::decode_error(st, i, p));
    }
    Ok(p)
}

fn json_value(o: &mut Vec<u8>, st: &Stmt, i: i32) -> Result<(), E> {
    match st.column_type(i) {
        ffi::SQLITE_INTEGER => int_text(o, st.column_int(i)),
        ffi::SQLITE_FLOAT => {
            let d = st.column_double(i);
            if d.is_finite() { py_float_repr(d, o) } else { o.extend_from_slice(b"null") }
        }
        ffi::SQLITE_TEXT => json_str(o, text(st, i)?),
        ffi::SQLITE_BLOB => {
            o.extend_from_slice(b"{\"$base64\":true,\"encoded\":\"");
            codec::b64encode(o, st.column_blob(i));
            o.extend_from_slice(b"\"}");
        }
        _ => o.extend_from_slice(b"null"),
    }
    Ok(())
}

fn csv_value(o: &mut Vec<u8>, st: &Stmt, i: i32, delim: u8) -> Result<(), E> {
    match st.column_type(i) {
        ffi::SQLITE_INTEGER => int_text(o, st.column_int(i)),
        ffi::SQLITE_FLOAT => {
            let d = st.column_double(i);
            if d.is_nan() {
                o.extend_from_slice(b"NaN");
            } else if d.is_infinite() {
                o.extend_from_slice(if d > 0.0 { b"Infinity" } else { b"-Infinity" });
            } else {
                py_float_repr(d, o);
            }
        }
        ffi::SQLITE_TEXT => csv_field(o, text(st, i)?, delim),
        ffi::SQLITE_BLOB => {
            let b = st.column_blob(i);
            if b.is_empty() {
                o.extend_from_slice(b"\"\""); // the empty string's rule
            } else {
                codec::b64encode(o, b); // base64 never needs quoting
            }
        }
        _ => {} // NULL: an empty, unquoted field
    }
    Ok(())
}

fn export_row(x: &mut Export, st: &Stmt, ncols: i32) -> Result<(), E> {
    let o = &mut x.out;
    if o.capacity() - o.len() < 4096 && o.try_reserve(x.chunk_bytes.max(4096)).is_err() {
        return Err(E::new("ResourceLimitExceeded", "worker ran out of memory (memory limit)"));
    }
    match x.enc {
        Enc::Csv | Enc::Tsv => {
            let delim = if x.enc == Enc::Csv { b',' } else { b'\t' };
            for i in 0..ncols {
                if i > 0 {
                    o.push(delim);
                }
                csv_value(o, st, i, delim)?;
            }
            o.extend_from_slice(b"\r\n");
        }
        Enc::JsonArrays => {
            if !x.first_row {
                o.push(b',');
            }
            o.push(b'[');
            for i in 0..ncols {
                if i > 0 {
                    o.push(b',');
                }
                json_value(o, st, i)?;
            }
            o.extend_from_slice(b"]\n");
        }
        Enc::JsonObjects | Enc::Ndjson => {
            if !x.first_row && x.enc == Enc::JsonObjects {
                o.push(b',');
            }
            o.push(b'{');
            let mut k = 0;
            for i in 0..ncols {
                let Some(key) = &x.keys[i as usize] else { continue }; // the first occurrence wins
                if k > 0 {
                    o.push(b',');
                }
                k += 1;
                o.extend_from_slice(key);
                json_value(o, st, i)?;
            }
            o.extend_from_slice(b"}\n");
        }
    }
    x.first_row = false;
    Ok(())
}

fn export_keys(x: &mut Export, st: &Stmt, ncols: i32) {
    x.keys = (0..ncols)
        .map(|i| {
            let name = st.column_name(i);
            if (0..i).any(|k| st.column_name(k) == name) {
                return None;
            }
            let mut kb = Vec::new();
            json_str(&mut kb, &codec::utf8_lossy(name));
            kb.push(b':');
            Some(kb)
        })
        .collect();
}

// -- producing chunks -----------------------------------------------------------

/// Fill the buffer with rows until it reaches chunk_bytes, the rows run out
/// or max_rows is reached. Ok(true): bytes or columns ready; Ok(false): the
/// export is over. The first call prepares and takes the first step.
fn export_chunk(cx: &Ctx, msg: &Value, db: &Db, s: &mut Stream) -> Result<bool, E> {
    let start = now_ns();
    let ms = s.time_limit.map(|r| if r > 0.0 { r as i64 } else { 0 });
    let (n, check) = cx.limit(ms);
    let progress = db.progress(n, &check);
    let r = (|| {
        cx.enter()?;
        let r = (|| {
            if s.time_limit.is_some_and(|r| r <= 0.0) {
                return Err(E::interrupted());
            }
            let x = s.ex.as_mut().expect("an export stream");
            if !s.started {
                s.started = true;
                s.st = worker::prepare(db, msg.get("sql"))?;
                worker::bind_params(db, s.st.as_mut(), msg.get("params"))?;
                s.ncols = s.st.as_ref().map(|st| st.column_count()).unwrap_or(0);
                if let Some(st) = s.st.as_mut() {
                    if !st.readonly() {
                        // Exports run on read connections, which refuse
                        // writes anyway; this says so before anything runs.
                        return Err(E::new("QueryError", "export runs reads only"));
                    }
                    let rc = st.step();
                    if rc != ffi::SQLITE_ROW && rc != ffi::SQLITE_DONE {
                        return Err(E::sqlite(Some(db), rc));
                    }
                    s.has_row = rc == ffi::SQLITE_ROW;
                    if matches!(x.enc, Enc::JsonObjects | Enc::Ndjson) {
                        export_keys(x, st, s.ncols);
                    }
                }
                return Ok(true); // the columns are known
            }
            while s.has_row && x.out.len() < x.chunk_bytes {
                let Some(st) = s.st.as_mut() else { break };
                if s.ncols == 0 {
                    break;
                }
                if x.max_rows.is_some_and(|m| x.rows >= m) {
                    x.truncated = true;
                    s.has_row = false;
                    break;
                }
                if cx.cancelled() {
                    return Err(E::interrupted());
                }
                export_row(x, st, s.ncols)?;
                x.rows += 1;
                let rc = st.step();
                if rc != ffi::SQLITE_ROW && rc != ffi::SQLITE_DONE {
                    return Err(E::sqlite(Some(db), rc));
                }
                s.has_row = rc == ffi::SQLITE_ROW;
            }
            if s.has_row && x.max_rows.is_some_and(|m| x.rows >= m) {
                x.truncated = true;
                s.has_row = false;
            }
            Ok(x.out.len() > 0)
        })();
        cx.leave();
        r
    })();
    drop(progress);
    if let Some(rem) = s.time_limit.as_mut() {
        *rem -= (now_ns() - start) as f64 / 1e6;
    }
    r
}

fn columns_value(w: &mut W, s: &Stream) {
    w.array(s.ncols as u32);
    if let Some(st) = &s.st {
        for i in 0..s.ncols {
            w.str_lossy(st.column_name(i));
        }
    }
    w.end();
}

fn send(sh: &Shared, w: &mut W) -> Result<(), E> {
    w.end();
    if !w.ok() {
        return Err(worker::wbuf_error(w));
    }
    let f = w.finish();
    sh.write_all(f);
    Ok(())
}

fn send_end(sh: &Shared, db: &Db, s: &mut Stream, w: &mut W) -> Result<(), E> {
    let x = s.ex.as_ref().unwrap();
    w.start(w.codec, w.limit);
    w.map_open();
    w.cstr("id");
    w.int(s.id);
    w.cstr("end");
    w.bool(true);
    w.cstr("row_count");
    w.int(x.rows);
    w.cstr("truncated");
    w.bool(x.truncated);
    if !db.autocommit() {
        w.cstr("in_tx");
        w.bool(true);
    }
    s.ended = true;
    s.st = None;
    send(sh, w)
}

/// Send the buffered chunk as one data frame.
fn send_data(sh: &Shared, s: &mut Stream, w: &mut W) -> Result<(), E> {
    let cap = w.limit;
    let x = s.ex.as_mut().unwrap();
    if x.transport == T::Raw {
        let n = x.out.len() - RAW_HEAD + 9; // payload: 0x00, id, bytes
        if n > cap {
            return Err(E::new(
                "ResourceLimitExceeded",
                format!("reply too large: frame of more than {cap} bytes exceeds the {cap}-byte frame size cap"),
            ));
        }
        x.out[..4].copy_from_slice(&(n as u32).to_be_bytes());
        x.out[4] = 0;
        x.out[5..13].copy_from_slice(&(s.id as u64).to_be_bytes());
        sh.write_all(&x.out);
        x.out.truncate(RAW_HEAD);
        return Ok(());
    }
    w.start(w.codec, cap);
    w.map_open();
    w.cstr("id");
    w.int(s.id);
    w.cstr("data");
    w.bin(&x.out);
    w.cstr("rows");
    w.int(1);
    x.out.clear();
    send(sh, w)
}

/// Like the stream pump: send up to `credit` data frames, then leave the
/// cursor open; the end frame when the rows run out.
pub fn pump(cx: &Ctx, sh: &Shared, msg: &Value, db: &Db, s: &mut Stream, w: &mut W) -> Result<(), E> {
    w.start(w.codec, w.limit);
    let (codec, limit) = (w.codec, w.limit);
    if !s.started {
        export_chunk(cx, msg, db, s)?;
        w.start(codec, limit);
        w.map_open();
        w.cstr("id");
        w.int(s.id);
        w.cstr("columns");
        columns_value(w, s);
        send(sh, w)?;
    }
    while s.credit > 0 {
        if cx.cancelled() {
            return Err(E::new("QueryInterrupted", "query cancelled"));
        }
        export_chunk(cx, msg, db, s)?;
        let x = s.ex.as_ref().unwrap();
        if x.out.len() > x.base() {
            send_data(sh, s, w)?;
            s.credit -= 1;
            if s.credit == 0 {
                break; // the end frame waits for the next `more`, as streams do
            }
        }
        if !s.has_row {
            return send_end(sh, db, s, w);
        }
    }
    Ok(())
}

/// Writes of at most 8 KB: on macOS a blocking write of 16 KB or more to a
/// FIFO read through kqueue can stall for good (scripts/macos_fifo_stall.py).
fn fifo_write(f: &mut std::fs::File, p: &[u8]) -> bool {
    p.chunks(8192).all(|c| f.write_all(c).is_ok())
}

/// The "fifo" transport: the whole export runs in this request; the reply
/// (row_count, truncated) is left in `w`.
pub fn run_fifo(cx: &Ctx, _sh: &Shared, msg: &Value, db: &Db, s: &mut Stream, w: &mut W) -> Result<(), E> {
    use std::os::unix::fs::OpenOptionsExt;
    let Some(Value::Str(path)) = msg.get("data_path") else {
        return Err(E::builtin("ValueError", "export with transport 'fifo' needs data_path"));
    };
    let path = String::from_utf8_lossy(path).into_owned();
    #[cfg(target_os = "linux")]
    const O_NONBLOCK: i32 = 0o4000;
    #[cfg(not(target_os = "linux"))]
    const O_NONBLOCK: i32 = 0x4;
    // A non-blocking open fails at once (ENXIO) if the parent is not holding
    // the read end; then a blocking descriptor, the pipe's backpressure.
    let probe = std::fs::OpenOptions::new().write(true).custom_flags(O_NONBLOCK).open(&path);
    let mut f = match probe.and_then(|_| std::fs::OpenOptions::new().write(true).open(&path)) {
        Ok(f) => f,
        Err(e) => return Err(E::new("QueryError", format!("cannot open the export pipe: {e}"))),
    };
    export_chunk(cx, msg, db, s)?;
    let mut cw = W::new(codec::Codec::Json, sh_max_frame(w));
    columns_value(&mut cw, s);
    let mut ok = cw.ok() && fifo_write(&mut f, cw.finish());
    while ok {
        if cx.cancelled() {
            return Err(E::new("QueryInterrupted", "query cancelled"));
        }
        export_chunk(cx, msg, db, s)?;
        let x = s.ex.as_mut().unwrap();
        if !x.out.is_empty() {
            ok = fifo_write(&mut f, &x.out);
            x.out.clear();
        }
        if !s.has_row {
            break;
        }
    }
    if !ok {
        return Err(E::new("QueryInterrupted", "export pipe closed by the reader"));
    }
    drop(f);
    let x = s.ex.as_ref().unwrap();
    w.start(w.codec, w.limit);
    w.map_open();
    w.cstr("id");
    w.int(s.id);
    w.cstr("result");
    w.map_open();
    w.cstr("row_count");
    w.int(x.rows);
    w.cstr("truncated");
    w.bool(x.truncated);
    w.end();
    if !db.autocommit() {
        w.cstr("in_tx");
        w.bool(true);
    }
    s.ended = true;
    Ok(())
}

fn sh_max_frame(w: &W) -> usize {
    w.limit
}

#[cfg(test)]
mod tests {
    use super::py_float_repr;

    fn r(d: f64) -> String {
        let mut o = Vec::new();
        py_float_repr(d, &mut o);
        String::from_utf8(o).unwrap()
    }

    #[test]
    fn float_repr_matches_python() {
        assert_eq!(r(1.0), "1.0");
        assert_eq!(r(0.1), "0.1");
        assert_eq!(r(-2.5), "-2.5");
        assert_eq!(r(1e16), "1e+16");
        assert_eq!(r(1234567890123456.0), "1234567890123456.0");
        assert_eq!(r(0.0001), "0.0001");
        assert_eq!(r(0.00001), "1e-05");
        assert_eq!(r(1.5e-300), "1.5e-300");
        assert_eq!(r(5e-324), "5e-324");
        assert_eq!(r(0.0), "0.0");
        assert_eq!(r(-0.0), "-0.0");
        assert_eq!(r(123.456), "123.456");
        assert_eq!(r(1.7976931348623157e308), "1.7976931348623157e+308");
    }
}
