//! DuckDB values to the wire form the Python worker produces, and wire
//! parameters back to DuckDB values (as `duckdb`'s Python client converts
//! them). Pure functions: the FFI layer reads the raw storage and calls these.
//!
//! The Python worker's rows are Python objects (`datetime`, `Decimal`, ...)
//! encoded by `protocol.STANDARD_VALUE_TYPES`; so this module reproduces
//! the Python client's conversions *and* those encoders: `isoformat()`,
//! `str(Decimal)`, timedelta normalisation, and so on.

use crate::value::{EXT_DATE, EXT_DATETIME, EXT_DECIMAL, EXT_MAP, EXT_TIME, EXT_TIMEDELTA, EXT_UUID, Value};

pub const MICROS_PER_DAY: i64 = 86_400_000_000;

// -- calendar --------------------------------------------------------------------

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// Outside Python's `date` range (years 1 to 9999).
pub struct OutOfRange(pub i64);

pub fn date_iso(days: i64) -> Result<String, OutOfRange> {
    let (y, m, d) = civil_from_days(days);
    if !(1..=9999).contains(&y) {
        return Err(OutOfRange(y));
    }
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}

pub fn time_iso(micros: i64) -> String {
    let micros = micros.rem_euclid(MICROS_PER_DAY);
    let (h, rem) = (micros / 3_600_000_000, micros % 3_600_000_000);
    let (mi, rem) = (rem / 60_000_000, rem % 60_000_000);
    let (s, us) = (rem / 1_000_000, rem % 1_000_000);
    if us == 0 {
        format!("{h:02}:{mi:02}:{s:02}")
    } else {
        format!("{h:02}:{mi:02}:{s:02}.{us:06}")
    }
}

/// `datetime.isoformat()` of a naive datetime, micros since the epoch.
pub fn datetime_iso(micros: i64) -> Result<String, OutOfRange> {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let date = date_iso(days)?;
    Ok(format!("{date}T{}", time_iso(micros.rem_euclid(MICROS_PER_DAY))))
}

/// `timezone(timedelta(seconds=offset)).isoformat` suffix: `+05:30`.
pub fn offset_iso(offset_s: i64) -> String {
    let sign = if offset_s < 0 { '-' } else { '+' };
    let a = offset_s.abs();
    let (h, m, s) = (a / 3600, (a % 3600) / 60, a % 60);
    if s == 0 { format!("{sign}{h:02}:{m:02}") } else { format!("{sign}{h:02}:{m:02}:{s:02}") }
}

// DuckDB's infinite dates and timestamps; Python's client maps them to
// date.max/min and datetime.max/min.
pub const DATE_INF: i32 = i32::MAX;
pub const DATE_NINF: i32 = -i32::MAX;
pub const TS_INF: i64 = i64::MAX;
pub const TS_NINF: i64 = -i64::MAX;
const DATETIME_MAX_MICROS: i64 = 253_402_300_799_999_999; // 9999-12-31T23:59:59.999999
const DATETIME_MIN_MICROS: i64 = -62_135_596_800_000_000; // 0001-01-01T00:00:00

fn ext_str(code: i8, s: String) -> Value {
    Value::Ext(code, Box::new(Value::Str(s)))
}

/// A conversion Python's client would refuse (a builtin ValueError there).
#[derive(Debug)]
pub struct ConvError(pub String);

impl From<OutOfRange> for ConvError {
    fn from(e: OutOfRange) -> ConvError {
        ConvError(format!("year {} is out of range", e.0))
    }
}

pub fn date(days: i32) -> Result<Value, ConvError> {
    let iso = match days {
        DATE_INF => "9999-12-31".to_string(),
        DATE_NINF => "0001-01-01".to_string(),
        d => date_iso(d as i64)?,
    };
    Ok(ext_str(EXT_DATE, iso))
}

fn clamp_ts(micros: i64) -> i64 {
    match micros {
        TS_INF => DATETIME_MAX_MICROS,
        TS_NINF => DATETIME_MIN_MICROS,
        m => m,
    }
}

pub fn timestamp(micros: i64) -> Result<Value, ConvError> {
    Ok(ext_str(EXT_DATETIME, datetime_iso(clamp_ts(micros))?))
}

/// TIMESTAMP WITH TIME ZONE: sent as the UTC instant with the session's
/// zone name, which the parent applies (`astimezone(ZoneInfo(zone))`), as
/// the Python client returns it in that zone.
pub fn timestamp_tz(micros: i64, zone: &str) -> Result<Value, ConvError> {
    let iso = format!("{}+00:00", datetime_iso(clamp_ts(micros))?);
    Ok(Value::Ext(EXT_DATETIME, Box::new(Value::List(vec![Value::Str(iso), Value::str(zone)]))))
}

pub fn time(micros: i64) -> Value {
    ext_str(EXT_TIME, time_iso(micros))
}

pub fn time_tz(micros: i64, offset_s: i32) -> Value {
    ext_str(EXT_TIME, format!("{}{}", time_iso(micros), offset_iso(offset_s as i64)))
}

/// INTERVAL as the Python client converts it:
/// `timedelta(days=months * 30 + days, microseconds=micros)`, sent as
/// `[days, seconds, microseconds]` after Python's normalisation.
pub fn interval(months: i32, days: i32, micros: i64) -> Result<Value, ConvError> {
    let total = (months as i128 * 30 + days as i128) * MICROS_PER_DAY as i128 + micros as i128;
    let d = total.div_euclid(MICROS_PER_DAY as i128);
    let rem = total.rem_euclid(MICROS_PER_DAY as i128);
    if d.abs() > 999_999_999 {
        return Err(ConvError(format!("days={d}; must have magnitude <= 999999999")));
    }
    Ok(Value::Ext(
        EXT_TIMEDELTA,
        Box::new(Value::List(vec![Value::Int(d), Value::Int(rem / 1_000_000), Value::Int(rem % 1_000_000)])),
    ))
}

pub fn uuid(hi_flipped: u64, lo: u64) -> Value {
    let v = ((hi_flipped ^ (1 << 63)) as u128) << 64 | lo as u128;
    let h = format!("{v:032x}");
    ext_str(EXT_UUID, format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32]))
}

/// `str(Decimal)` of the value `unscaled * 10**-scale`, as the Python client
/// builds it (the coefficient keeps its trailing zeros: `Decimal('1.500')`).
pub fn decimal_str(unscaled: i128, scale: u8) -> String {
    let neg = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let exp = -(scale as i64);
    let n = digits.len() as i64;
    let leftdigits = exp + n;
    let dotplace = if exp <= 0 && leftdigits > -6 { leftdigits } else { 1 };
    let (intpart, fracpart) = if dotplace <= 0 {
        ("0".to_string(), format!(".{}{}", "0".repeat((-dotplace) as usize), digits))
    } else if dotplace >= n {
        (format!("{digits}{}", "0".repeat((dotplace - n) as usize)), String::new())
    } else {
        (digits[..dotplace as usize].to_string(), format!(".{}", &digits[dotplace as usize..]))
    };
    let e = if leftdigits == dotplace { String::new() } else { format!("E{:+}", leftdigits - dotplace) };
    format!("{}{intpart}{fracpart}{e}", if neg { "-" } else { "" })
}

pub fn decimal(unscaled: i128, scale: u8) -> Value {
    ext_str(EXT_DECIMAL, decimal_str(unscaled, scale))
}

/// BIT storage (a padding-count byte, then the bits) as the Python client's
/// string of 0s and 1s.
pub fn bit_string(data: &[u8]) -> String {
    let Some((&pad, bytes)) = data.split_first() else {
        return String::new();
    };
    let mut s = String::with_capacity(bytes.len() * 8);
    for (i, b) in bytes.iter().enumerate() {
        for bit in (0..8).rev() {
            if i == 0 && (7 - bit) < pad as usize {
                continue;
            }
            s.push(if b >> bit & 1 == 1 { '1' } else { '0' });
        }
    }
    s
}

/// BIGNUM storage (3-byte header with the sign, then the big-endian
/// magnitude; every byte inverted when negative) as decimal text, which is
/// what the Python client returns for it (a `str`).
pub fn bignum_string(data: &[u8]) -> String {
    if data.len() < 3 {
        return "0".into();
    }
    let negative = data[0] & 0x80 == 0;
    let mag: Vec<u8> = data[3..].iter().map(|&b| if negative { !b } else { b }).collect();
    // Repeated division by 10^9 of the big-endian magnitude.
    let mut num: Vec<u32> = mag.iter().map(|&b| b as u32).collect();
    let mut groups: Vec<u32> = Vec::new();
    while num.iter().any(|&x| x != 0) {
        let mut rem: u64 = 0;
        for x in num.iter_mut() {
            let cur = (rem << 8) | *x as u64;
            *x = (cur / 1_000_000_000) as u32;
            rem = cur % 1_000_000_000;
        }
        groups.push(rem as u32);
        while num.first() == Some(&0) {
            num.remove(0);
        }
    }
    if groups.is_empty() {
        return "0".into();
    }
    let mut s = String::new();
    if negative {
        s.push('-');
    }
    s.push_str(&groups.last().expect("non-empty").to_string());
    for g in groups.iter().rev().skip(1) {
        s.push_str(&format!("{g:09}"));
    }
    s
}

/// A MAP as the Python client returns it (a dict), marked as
/// `protocol._mark` does: a plain dict when every key is a string (and it
/// is not a lone `$b`/`$d`/`$x`), else the `map` value type.
pub fn map_value(pairs: Vec<(Value, Value)>) -> Value {
    let plain = pairs.iter().all(|(k, _)| matches!(k, Value::Str(_))) && !is_wrapper_like(&pairs);
    if plain {
        Value::Map(pairs)
    } else {
        Value::Ext(EXT_MAP, Box::new(Value::List(pairs.into_iter().map(|(k, v)| Value::List(vec![k, v])).collect())))
    }
}

fn is_wrapper_like(pairs: &[(Value, Value)]) -> bool {
    pairs.len() == 1 && matches!(&pairs[0].0, Value::Str(s) if s == "$b" || s == "$d" || s == "$x")
}

// -- parameters: wire values to DuckDB values --------------------------------------

/// A parameter as the Python client would bind it.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    U64(u64),
    I128(i128),
    U128(u128),
    F32(f32),
    F64(f64),
    Str(String),
    Blob(Vec<u8>),
    Decimal { width: u8, scale: u8, value: i128 },
    Timestamp(i64),
    TimestampTz(i64),
    Date(i32),
    Time(i64),
    TimeTz(i64, i32),
    Interval { months: i32, days: i32, micros: i64 },
    Uuid(u128),
    List(PTy, Vec<Param>),
    Struct(Vec<(String, Param)>),
    Map(PTy, PTy, Vec<(Param, Param)>),
}

/// The type of a parameter (for list elements and map keys/values).
#[derive(Clone, Debug, PartialEq)]
pub enum PTy {
    Null,
    Bool,
    I32,
    I64,
    U64,
    I128,
    U128,
    F32,
    F64,
    Varchar,
    Blob,
    Decimal(u8, u8),
    Timestamp,
    TimestampTz,
    Date,
    Time,
    TimeTz,
    Interval,
    Uuid,
    List(Box<PTy>),
    Struct(Vec<(String, PTy)>),
    Map(Box<PTy>, Box<PTy>),
}

impl Param {
    pub fn ty(&self) -> PTy {
        match self {
            Param::Null => PTy::Null,
            Param::Bool(_) => PTy::Bool,
            Param::I32(_) => PTy::I32,
            Param::I64(_) => PTy::I64,
            Param::U64(_) => PTy::U64,
            Param::I128(_) => PTy::I128,
            Param::U128(_) => PTy::U128,
            Param::F32(_) => PTy::F32,
            Param::F64(_) => PTy::F64,
            Param::Str(_) => PTy::Varchar,
            Param::Blob(_) => PTy::Blob,
            Param::Decimal { width, scale, .. } => PTy::Decimal(*width, *scale),
            Param::Timestamp(_) => PTy::Timestamp,
            Param::TimestampTz(_) => PTy::TimestampTz,
            Param::Date(_) => PTy::Date,
            Param::Time(_) => PTy::Time,
            Param::TimeTz(..) => PTy::TimeTz,
            Param::Interval { .. } => PTy::Interval,
            Param::Uuid(_) => PTy::Uuid,
            Param::List(t, _) => PTy::List(Box::new(t.clone())),
            Param::Struct(f) => PTy::Struct(f.iter().map(|(k, v)| (k.clone(), v.ty())).collect()),
            Param::Map(k, v, _) => PTy::Map(Box::new(k.clone()), Box::new(v.clone())),
        }
    }
}

/// A parameter Python's client would refuse: its error class and message.
#[derive(Debug)]
pub struct ParamError(pub &'static str, pub String);

fn int_param(i: i128) -> Param {
    if let Ok(v) = i32::try_from(i) {
        Param::I32(v)
    } else if let Ok(v) = i64::try_from(i) {
        Param::I64(v)
    } else if let Ok(v) = u64::try_from(i) {
        Param::U64(v)
    } else {
        Param::I128(i)
    }
}

pub fn param(v: &Value) -> Result<Param, ParamError> {
    Ok(match v {
        Value::Null => Param::Null,
        Value::Bool(b) => Param::Bool(*b),
        Value::Int(i) => int_param(*i),
        Value::BigInt(s) => match s.parse::<u128>() {
            Ok(u) => Param::U128(u),
            Err(_) => {
                return Err(ParamError(
                    "InvalidInputException",
                    format!("Invalid Input Error: Python integer too large for 128-bit integer type: {s}"),
                ));
            }
        },
        Value::Float(f) => Param::F64(*f),
        Value::Str(s) => Param::Str(s.clone()),
        Value::Bytes(b) => Param::Blob(b.clone()),
        Value::List(items) => list_param(items)?,
        Value::Map(items) => {
            if items.is_empty() {
                Param::Map(PTy::Null, PTy::Null, vec![])
            } else if items.iter().all(|(k, _)| matches!(k, Value::Str(_))) {
                let mut fields = Vec::with_capacity(items.len());
                for (k, x) in items {
                    let Value::Str(k) = k else { unreachable!() };
                    fields.push((k.clone(), param(x)?));
                }
                Param::Struct(fields)
            } else {
                map_param(items)?
            }
        }
        Value::Ext(code, payload) => ext_param(*code, payload)?,
    })
}

fn bad(what: &str, v: &Value) -> ParamError {
    ParamError("InvalidInputException", format!("Invalid Input Error: could not read {what} parameter {v:?}"))
}

fn ext_param(code: i8, p: &Value) -> Result<Param, ParamError> {
    use crate::value::*;
    Ok(match code {
        EXT_DATETIME => match p {
            Value::Str(s) => {
                let (micros, offset) = parse_datetime(s).ok_or_else(|| bad("datetime", p))?;
                match offset {
                    None => Param::Timestamp(micros),
                    Some(off) => Param::TimestampTz(micros - off * 1_000_000),
                }
            }
            Value::List(l) if l.len() == 2 => {
                let s = l[0].as_str().ok_or_else(|| bad("datetime", p))?;
                let (micros, offset) = parse_datetime(s).ok_or_else(|| bad("datetime", p))?;
                Param::TimestampTz(micros - offset.unwrap_or(0) * 1_000_000)
            }
            _ => return Err(bad("datetime", p)),
        },
        EXT_DATE => {
            let s = p.as_str().ok_or_else(|| bad("date", p))?;
            Param::Date(parse_date(s).ok_or_else(|| bad("date", p))? as i32)
        }
        EXT_TIME => {
            let s = p.as_str().ok_or_else(|| bad("time", p))?;
            let (micros, offset) = parse_time(s).ok_or_else(|| bad("time", p))?;
            match offset {
                None => Param::Time(micros),
                // The Python client refuses a time whose zone is west of
                // UTC (it reads utcoffset().days, which is -1 there).
                Some(off) if off < 0 => {
                    return Err(ParamError(
                        "InvalidInputException",
                        "Invalid Input Error: Failed to convert 'tzinfo' object, utcoffset returned an invalid timedelta (days)".into(),
                    ));
                }
                Some(off) => Param::TimeTz(micros, off as i32),
            }
        }
        EXT_DECIMAL => decimal_param(p.as_str().ok_or_else(|| bad("decimal", p))?),
        EXT_UUID => {
            let s = p.as_str().ok_or_else(|| bad("uuid", p))?;
            let hex: String = s.chars().filter(|c| *c != '-').collect();
            if hex.len() != 32 {
                return Err(bad("uuid", p));
            }
            Param::Uuid(u128::from_str_radix(&hex, 16).map_err(|_| bad("uuid", p))?)
        }
        EXT_TIMEDELTA => {
            let Value::List(l) = p else { return Err(bad("timedelta", p)) };
            if l.len() != 3 {
                return Err(bad("timedelta", p));
            }
            let n = |i: usize| l[i].as_i64().ok_or_else(|| bad("timedelta", p));
            let (d, s, us) = (n(0)?, n(1)?, n(2)?);
            let micros = s.checked_mul(1_000_000).and_then(|x| x.checked_add(us)).ok_or_else(|| bad("timedelta", p))?;
            Param::Interval { months: 0, days: i32::try_from(d).map_err(|_| bad("timedelta", p))?, micros }
        }
        EXT_MAP => {
            let Value::List(l) = p else { return Err(bad("map", p)) };
            let mut items = Vec::with_capacity(l.len());
            for kv in l {
                let Value::List(kv) = kv else { return Err(bad("map", p)) };
                if kv.len() != 2 {
                    return Err(bad("map", p));
                }
                items.push((kv[0].clone(), kv[1].clone()));
            }
            if items.is_empty() { Param::Map(PTy::Null, PTy::Null, vec![]) } else { map_param(&items)? }
        }
        EXT_TUPLE => {
            let Value::List(l) = p else { return Err(bad("tuple", p)) };
            list_param(l)?
        }
        _ => return Err(bad("unknown", p)),
    })
}

/// `Decimal` as the Python client binds it: DECIMAL(width, scale) from its
/// digits and exponent; DOUBLE when that does not fit 38 digits; NaN and
/// infinities as FLOAT.
fn decimal_param(s: &str) -> Param {
    let t = s.trim();
    let (neg, body) = match t.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let lower = body.to_ascii_lowercase();
    if lower.contains("nan") {
        return Param::F32(f32::NAN);
    }
    if lower.starts_with("inf") {
        return Param::F32(if neg { f32::NEG_INFINITY } else { f32::INFINITY });
    }
    let (mant, exp) = match lower.find('e') {
        Some(i) => (&body[..i], body[i + 1..].parse::<i64>().unwrap_or(0)),
        None => (body, 0),
    };
    let (ip, fp) = match mant.find('.') {
        Some(i) => (&mant[..i], &mant[i + 1..]),
        None => (mant, ""),
    };
    let digits_raw = format!("{ip}{fp}");
    let exp = exp - fp.len() as i64;
    let digits = digits_raw.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let as_double = || Param::F64(s.parse::<f64>().unwrap_or(f64::NAN));
    if !digits.bytes().all(|c| c.is_ascii_digit()) {
        return as_double();
    }
    let n = digits.len() as i64;
    let (width, scale, coeff_digits) = if exp >= 0 {
        (n + exp, 0, format!("{digits}{}", "0".repeat(exp as usize)))
    } else {
        (n.max(-exp + 1), -exp, digits.to_string())
    };
    if width > 38 || scale > 38 {
        return as_double();
    }
    let Ok(mut value) = coeff_digits.parse::<i128>() else {
        return as_double();
    };
    if neg {
        value = -value;
    }
    Param::Decimal { width: width as u8, scale: scale as u8, value }
}

fn unify(a: &PTy, b: &PTy) -> Option<PTy> {
    use PTy::*;
    if a == b {
        return Some(a.clone());
    }
    let rank = |t: &PTy| match t {
        I32 => Some(1),
        I64 => Some(2),
        U64 => Some(3),
        I128 => Some(4),
        U128 => Some(5),
        _ => None,
    };
    Some(match (a, b) {
        (Null, t) | (t, Null) => t.clone(),
        (Varchar, _) | (_, Varchar) => Varchar,
        (Bool, x) | (x, Bool) if rank(x).is_some() => x.clone(),
        (F64 | F32, x) | (x, F64 | F32) if rank(x).is_some() || matches!(x, F64 | F32) => F64,
        (x, y) if rank(x).is_some() && rank(y).is_some() => {
            if rank(x) > rank(y) { x.clone() } else { y.clone() }
        }
        (List(x), List(y)) => List(Box::new(unify(x, y)?)),
        (Struct(x), Struct(y)) if x.len() == y.len() && x.iter().zip(y).all(|(a, b)| a.0 == b.0) => {
            Struct(x.iter().zip(y).map(|(a, b)| Some((a.0.clone(), unify(&a.1, &b.1)?))).collect::<Option<Vec<_>>>()?)
        }
        (Map(xk, xv), Map(yk, yv)) => Map(Box::new(unify(xk, yk)?), Box::new(unify(xv, yv)?)),
        _ => return None,
    })
}

/// A value as `str()` gives it in Python, for a list that unified to VARCHAR.
fn to_varchar(p: Param) -> Param {
    match p {
        Param::Null | Param::Str(_) => p,
        Param::Bool(b) => Param::Str(if b { "True".into() } else { "False".into() }),
        Param::I32(i) => Param::Str(i.to_string()),
        Param::I64(i) => Param::Str(i.to_string()),
        Param::U64(i) => Param::Str(i.to_string()),
        Param::I128(i) => Param::Str(i.to_string()),
        Param::U128(i) => Param::Str(i.to_string()),
        Param::F64(f) =>Param::Str(py_float_repr(f)),
        Param::F32(f) => Param::Str(py_float_repr(f as f64)),
        other => Param::Str(format!("{other:?}")),
    }
}

fn py_float_repr(f: f64) -> String {
    if f.is_nan() {
        return "nan".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "inf".into() } else { "-inf".into() };
    }
    format!("{f:?}")
}

fn cast(p: Param, to: &PTy) -> Param {
    match (p, to) {
        (Param::Null, _) => Param::Null,
        (p, PTy::Varchar) => to_varchar(p),
        (Param::Bool(b), PTy::I32) => Param::I32(b as i32),
        (Param::Bool(b), PTy::I64) => Param::I64(b as i64),
        (Param::Bool(b), PTy::U64) => Param::U64(b as u64),
        (Param::Bool(b), PTy::I128) => Param::I128(b as i128),
        (Param::I32(i), PTy::F64) => Param::F64(i as f64),
        (Param::I64(i), PTy::F64) => Param::F64(i as f64),
        (Param::U64(i), PTy::F64) => Param::F64(i as f64),
        (Param::I128(i), PTy::F64) => Param::F64(i as f64),
        (Param::F32(f), PTy::F64) => Param::F64(f as f64),
        (Param::I32(i), PTy::I64) => Param::I64(i as i64),
        (Param::I32(i), PTy::I128) => Param::I128(i as i128),
        (Param::I64(i), PTy::I128) => Param::I128(i as i128),
        (Param::U64(i), PTy::I128) => Param::I128(i as i128),
        (Param::I32(i), PTy::U64) if i >= 0 => Param::U64(i as u64),
        (Param::I64(i), PTy::U64) if i >= 0 => Param::U64(i as u64),
        (Param::List(_, items), PTy::List(child)) => {
            Param::List((**child).clone(), items.into_iter().map(|x| cast(x, child)).collect())
        }
        (Param::Struct(fields), PTy::Struct(types)) => {
            Param::Struct(fields.into_iter().zip(types).map(|((k, v), (_, t))| (k, cast(v, t))).collect())
        }
        (Param::Map(_, _, pairs), PTy::Map(kt, vt)) => {
            Param::Map((**kt).clone(), (**vt).clone(), pairs.into_iter().map(|(k, v)| (cast(k, kt), cast(v, vt))).collect())
        }
        (p, _) => p,
    }
}

fn list_param(items: &[Value]) -> Result<Param, ParamError> {
    let mut ps = Vec::with_capacity(items.len());
    for x in items {
        ps.push(param(x)?);
    }
    let mut ty = PTy::Null;
    for p in &ps {
        ty = unify(&ty, &p.ty()).ok_or_else(|| {
            ParamError("InvalidInputException", "Invalid Input Error: list parameter with values of incompatible types".into())
        })?;
    }
    let ps = ps.into_iter().map(|p| cast(p, &ty)).collect();
    Ok(Param::List(ty, ps))
}

fn map_param(items: &[(Value, Value)]) -> Result<Param, ParamError> {
    let mut kt = PTy::Null;
    let mut vt = PTy::Null;
    let mut pairs = Vec::with_capacity(items.len());
    for (k, v) in items {
        let (k, v) = (param(k)?, param(v)?);
        let err = || ParamError("InvalidInputException", "Invalid Input Error: map parameter with incompatible types".into());
        kt = unify(&kt, &k.ty()).ok_or_else(err)?;
        vt = unify(&vt, &v.ty()).ok_or_else(err)?;
        pairs.push((k, v));
    }
    let pairs = pairs.into_iter().map(|(k, v)| (cast(k, &kt), cast(v, &vt))).collect();
    Ok(Param::Map(kt, vt, pairs))
}

fn num(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// `YYYY-MM-DD` to days since the epoch.
pub fn parse_date(s: &str) -> Option<i64> {
    let mut it = s.splitn(3, '-');
    let (y, m, d) = (num(it.next()?)?, num(it.next()?)?, num(it.next()?)?);
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m as u32) as i64 {
        return None;
    }
    Some(days_from_civil(y, m as u32, d as u32))
}

/// `HH:MM[:SS[.ffffff]][+HH:MM[:SS[.ffffff]]]` to (micros, offset seconds).
pub fn parse_time(s: &str) -> Option<(i64, Option<i64>)> {
    let (t, off) = match s.find(['+', '-', 'Z']) {
        Some(i) => (&s[..i], Some(&s[i..])),
        None => (s, None),
    };
    let micros = hms_micros(t)?;
    let offset = match off {
        None => None,
        Some("Z") => Some(0),
        Some(o) => {
            let sign = if o.starts_with('-') { -1 } else { 1 };
            let m = hms_micros(&o[1..])?;
            Some(sign * (m / 1_000_000))
        }
    };
    Some((micros, offset))
}

fn hms_micros(t: &str) -> Option<i64> {
    let (hms, frac) = match t.find('.') {
        Some(i) => (&t[..i], &t[i + 1..]),
        None => (t, ""),
    };
    let parts: Vec<&str> = hms.split(':').collect();
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    let h = num(parts[0])?;
    let m = parts.get(1).map(|p| num(p)).unwrap_or(Some(0))?;
    let sec = parts.get(2).map(|p| num(p)).unwrap_or(Some(0))?;
    let us = if frac.is_empty() {
        0
    } else {
        if frac.len() > 6 || !frac.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        format!("{frac:0<6}").parse::<i64>().ok()?
    };
    Some(((h * 60 + m) * 60 + sec) * 1_000_000 + us)
}

/// `datetime.isoformat()` output to (local micros since epoch, offset seconds).
pub fn parse_datetime(s: &str) -> Option<(i64, Option<i64>)> {
    let (d, t) = match s.find(['T', ' ']) {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, "00:00:00"),
    };
    let days = parse_date(d)?;
    let (micros, offset) = parse_time(t)?;
    Some((days * MICROS_PER_DAY + micros, offset))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_print_as_python() {
        assert_eq!(decimal_str(1500, 3), "1.500");
        assert_eq!(decimal_str(1000, 10), "1.000E-7");
        assert_eq!(decimal_str(-5, 1), "-0.5");
        assert_eq!(decimal_str(0, 3), "0.000");
        assert_eq!(decimal_str(125, 2), "1.25");
        assert_eq!(decimal_str(5, 0), "5");
        assert_eq!(decimal_str(1, 6), "0.000001");
        assert_eq!(decimal_str(1, 7), "1E-7");
    }

    #[test]
    fn calendar() {
        assert_eq!(date_iso(0).ok().unwrap(), "1970-01-01");
        assert_eq!(date_iso(19724).ok().unwrap(), "2024-01-02");
        assert_eq!(days_from_civil(2024, 2, 29), 19782);
        assert_eq!(datetime_iso(-1).ok().unwrap(), "1969-12-31T23:59:59.999999");
        assert_eq!(datetime_iso(DATETIME_MIN_MICROS).ok().unwrap(), "0001-01-01T00:00:00");
        assert_eq!(datetime_iso(DATETIME_MAX_MICROS).ok().unwrap(), "9999-12-31T23:59:59.999999");
        assert_eq!(parse_datetime("2020-01-02T03:04:05.000006+00:00"), Some((1577934245000006, Some(0))));
        assert_eq!(parse_time("01:02:03+05:30"), Some((3723000000, Some(19800))));
        assert_eq!(offset_iso(-3600), "-01:00");
    }

    #[test]
    fn intervals_normalise_as_timedelta() {
        let v = interval(0, 0, -1).ok().unwrap();
        assert_eq!(format!("{v:?}"), format!("{:?}", Value::Ext(EXT_TIMEDELTA, Box::new(Value::List(vec![Value::Int(-1), Value::Int(86399), Value::Int(999999)])))));
    }

    #[test]
    fn decimal_params() {
        assert_eq!(decimal_param("1.50"), Param::Decimal { width: 3, scale: 2, value: 150 });
        assert_eq!(decimal_param("1E+5"), Param::Decimal { width: 6, scale: 0, value: 100000 });
        assert_eq!(decimal_param("0.001"), Param::Decimal { width: 4, scale: 3, value: 1 });
        assert_eq!(decimal_param("-0.00"), Param::Decimal { width: 3, scale: 2, value: 0 });
        assert!(matches!(decimal_param("1E-40"), Param::F64(_)));
        assert!(matches!(decimal_param("NaN"), Param::F32(f) if f.is_nan()));
    }

    #[test]
    fn bits_and_bignums() {
        assert_eq!(bit_string(&[4, 0b1010]), "1010");
        assert_eq!(bignum_string(&[0x80, 0, 1, 123]), "123");
    }
}
