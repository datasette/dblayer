//! The message model: what both codecs decode to and encode from.
//!
//! Mirrors what the Python side puts on the wire (`dblayer/worker/protocol.py`):
//! the portable types, lists, maps (messages and user dicts; keys need not be
//! strings in MessagePack) and the registered value types, which travel as
//! `Ext(code, payload)` (JSON `{"$x": [tag, payload]}`, MessagePack ext type
//! `code` holding `payload` packed).

/// MessagePack ext code for integers outside 64 bits (decimal text).
pub const EXT_BIGINT: i8 = 1;
pub const EXT_DATETIME: i8 = 16;
pub const EXT_DATE: i8 = 17;
pub const EXT_TIME: i8 = 18;
pub const EXT_DECIMAL: i8 = 19;
pub const EXT_UUID: i8 = 20;
pub const EXT_TIMEDELTA: i8 = 21;
pub const EXT_MAP: i8 = 22;
pub const EXT_TUPLE: i8 = 23;

/// `(tag, ext code)` of `protocol.STANDARD_VALUE_TYPES`.
pub const VALUE_TYPES: [(&str, i8); 8] = [
    ("datetime", EXT_DATETIME),
    ("date", EXT_DATE),
    ("time", EXT_TIME),
    ("decimal", EXT_DECIMAL),
    ("uuid", EXT_UUID),
    ("timedelta", EXT_TIMEDELTA),
    ("map", EXT_MAP),
    ("tuple", EXT_TUPLE),
];

pub fn tag_for(code: i8) -> Option<&'static str> {
    VALUE_TYPES.iter().find(|(_, c)| *c == code).map(|(t, _)| *t)
}

pub fn code_for(tag: &str) -> Option<i8> {
    VALUE_TYPES.iter().find(|(t, _)| *t == tag).map(|(_, c)| *c)
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// Any integer that fits 128 bits (HUGEINT, and everything smaller).
    Int(i128),
    /// An integer outside `i128` (UHUGEINT above `i128::MAX`, or a parameter
    /// too large for DuckDB): its decimal text.
    BigInt(String),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    /// A dict, in order. Message keys are strings.
    Map(Vec<(Value, Value)>),
    /// A registered value type: ext code and its payload.
    Ext(i8, Box<Value>),
}

impl Value {
    pub fn str(s: impl Into<String>) -> Value {
        Value::Str(s.into())
    }

    /// A dict from string keys.
    pub fn obj(items: Vec<(&str, Value)>) -> Value {
        Value::Map(items.into_iter().map(|(k, v)| (Value::Str(k.to_string()), v)).collect())
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(items) => items
                .iter()
                .find(|(k, _)| matches!(k, Value::Str(s) if s == key))
                .map(|(_, v)| v),
            _ => None,
        }
    }

    /// `get`, treating an explicit null as absent (Python's `msg.get(k)`
    /// followed by an `is None` test).
    pub fn opt(&self, key: &str) -> Option<&Value> {
        match self.get(key) {
            None | Some(Value::Null) => None,
            v => v,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => i64::try_from(*i).ok(),
            Value::Bool(b) => Some(*b as i64),
            _ => None,
        }
    }

    /// A number as Python's `int(x)` would take it (floats truncate).
    pub fn as_int_lossy(&self) -> Option<i64> {
        match self {
            Value::Float(f) if f.is_finite() => Some(*f as i64),
            v => v.as_i64(),
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            Value::Bool(b) => Some(*b as i64 as f64),
            _ => None,
        }
    }

    pub fn is_true(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::Str(s) => !s.is_empty(),
            Value::Bytes(b) => !b.is_empty(),
            Value::List(l) => !l.is_empty(),
            Value::Map(m) => !m.is_empty(),
            _ => true,
        }
    }

    /// Insert or replace a key of a dict.
    pub fn set(&mut self, key: &str, value: Value) {
        if let Value::Map(items) = self {
            for (k, v) in items.iter_mut() {
                if matches!(k, Value::Str(s) if s == key) {
                    *v = value;
                    return;
                }
            }
            items.push((Value::Str(key.to_string()), value));
        }
    }

    /// Identity of a request (`id`, `target`, `stream`): ints mostly, but
    /// any value is compared by its encoded form.
    pub fn key(&self) -> String {
        match self {
            Value::Int(i) => i.to_string(),
            Value::Str(s) => format!("s:{s}"),
            other => format!("{other:?}"),
        }
    }
}
