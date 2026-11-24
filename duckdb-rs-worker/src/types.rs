//! DuckDB logical types as a tree, and their names as Python's
//! `str(DuckDBPyType)` (DuckDB's `LogicalType::ToString`) prints them: the
//! `types` the protocol sends with every result.

#[derive(Clone, Debug, PartialEq)]
pub enum Ty {
    Null,
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    I128,
    U128,
    F32,
    F64,
    Varchar,
    Blob,
    Bit,
    Bignum,
    Date,
    Time,
    TimeNs,
    TimeTz,
    Timestamp,
    TimestampS,
    TimestampMs,
    TimestampNs,
    TimestampTz,
    Interval,
    Uuid,
    /// `internal` is the storage width in bytes (2, 4, 8, 16).
    Decimal { width: u8, scale: u8, internal: u8 },
    /// `internal` is the index width in bytes (1, 2, 4).
    Enum { internal: u8, values: Vec<String> },
    List(Box<TyNode>),
    Array(Box<TyNode>, u64),
    Struct(Vec<(String, TyNode)>),
    Map(Box<TyNode>, Box<TyNode>),
    Union(Vec<(String, TyNode)>),
    /// A type this worker does not convert (GEOMETRY, VARIANT, ...): its
    /// DuckDB type id.
    Other(u32),
}

#[derive(Clone, Debug, PartialEq)]
pub struct TyNode {
    pub ty: Ty,
    /// A type alias (`JSON` is VARCHAR with alias `JSON`).
    pub alias: Option<String>,
}

impl TyNode {
    #[cfg(test)]
    pub fn new(ty: Ty) -> TyNode {
        TyNode { ty, alias: None }
    }

    pub fn name(&self) -> String {
        if let Some(a) = &self.alias {
            return a.clone();
        }
        match &self.ty {
            Ty::Null => "\"NULL\"".into(),
            Ty::Bool => "BOOLEAN".into(),
            Ty::I8 => "TINYINT".into(),
            Ty::I16 => "SMALLINT".into(),
            Ty::I32 => "INTEGER".into(),
            Ty::I64 => "BIGINT".into(),
            Ty::U8 => "UTINYINT".into(),
            Ty::U16 => "USMALLINT".into(),
            Ty::U32 => "UINTEGER".into(),
            Ty::U64 => "UBIGINT".into(),
            Ty::I128 => "HUGEINT".into(),
            Ty::U128 => "UHUGEINT".into(),
            Ty::F32 => "FLOAT".into(),
            Ty::F64 => "DOUBLE".into(),
            Ty::Varchar => "VARCHAR".into(),
            Ty::Blob => "BLOB".into(),
            Ty::Bit => "BIT".into(),
            Ty::Bignum => "BIGNUM".into(),
            Ty::Date => "DATE".into(),
            Ty::Time => "TIME".into(),
            Ty::TimeNs => "TIME_NS".into(),
            Ty::TimeTz => "TIME WITH TIME ZONE".into(),
            Ty::Timestamp => "TIMESTAMP".into(),
            Ty::TimestampS => "TIMESTAMP_S".into(),
            Ty::TimestampMs => "TIMESTAMP_MS".into(),
            Ty::TimestampNs => "TIMESTAMP_NS".into(),
            Ty::TimestampTz => "TIMESTAMP WITH TIME ZONE".into(),
            Ty::Interval => "INTERVAL".into(),
            Ty::Uuid => "UUID".into(),
            Ty::Decimal { width, scale, .. } => format!("DECIMAL({width},{scale})"),
            Ty::Enum { values, .. } => {
                let quoted: Vec<String> = values.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect();
                format!("ENUM({})", quoted.join(", "))
            }
            Ty::List(child) => format!("{}[]", child.name()),
            Ty::Array(child, n) => format!("{}[{n}]", child.name()),
            Ty::Struct(fields) if fields.is_empty() => "STRUCT".into(),
            // An unnamed struct (a row value): the Python client calls it a
            // TUPLE and returns a tuple.
            Ty::Struct(fields) if fields.iter().all(|(n, _)| n.is_empty()) => format!("TUPLE({})", fields_list(fields)),
            Ty::Struct(fields) => format!("STRUCT({})", fields_list(fields)),
            Ty::Union(fields) => format!("UNION({})", fields_list(fields)),
            Ty::Map(k, v) => format!("MAP({}, {})", k.name(), v.name()),
            Ty::Other(id) => format!("TYPE_{id}"),
        }
    }

    /// True if a value of this type contains a TIMESTAMP WITH TIME ZONE
    /// (whose conversion needs the session's time zone).
    pub fn has_tz(&self) -> bool {
        match &self.ty {
            Ty::TimestampTz => true,
            Ty::List(c) | Ty::Array(c, _) => c.has_tz(),
            Ty::Map(k, v) => k.has_tz() || v.has_tz(),
            Ty::Struct(f) | Ty::Union(f) => f.iter().any(|(_, t)| t.has_tz()),
            _ => false,
        }
    }

    /// The first type in the tree this worker cannot convert, if any.
    pub fn unsupported(&self) -> Option<u32> {
        match &self.ty {
            Ty::Other(id) => Some(*id),
            Ty::List(c) | Ty::Array(c, _) => c.unsupported(),
            Ty::Map(k, v) => k.unsupported().or_else(|| v.unsupported()),
            Ty::Struct(f) | Ty::Union(f) => f.iter().find_map(|(_, t)| t.unsupported()),
            _ => None,
        }
    }
}

fn fields_list(fields: &[(String, TyNode)]) -> String {
    let parts: Vec<String> = fields
        .iter()
        .map(|(n, t)| if n.is_empty() { t.name() } else { format!("{} {}", quote_ident(n), t.name()) })
        .collect();
    parts.join(", ")
}

/// DuckDB's `KeywordHelper::WriteOptionallyQuoted`: plain lower-case
/// identifiers that are not reserved words are left bare.
pub fn quote_ident(name: &str) -> String {
    let plain = !name.is_empty()
        && name.bytes().enumerate().all(|(i, c)| c == b'_' || c.is_ascii_lowercase() || (i > 0 && c.is_ascii_digit()))
        && !RESERVED.contains(&name);
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// DuckDB's reserved keywords (`duckdb_keywords()` where category is
/// 'reserved'), which `WriteOptionallyQuoted` always quotes.
const RESERVED: &[&str] = &[
    "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast",
    "check", "collate", "column", "constraint", "create", "default", "deferrable", "desc", "describe",
    "distinct", "do", "else", "end", "except", "false", "fetch", "for", "foreign", "from", "grant", "group",
    "having", "in", "initially", "intersect", "into", "lateral", "leading", "limit", "not", "null", "offset",
    "on", "only", "or", "order", "pivot", "pivot_longer", "pivot_wider", "placing", "primary", "qualify",
    "references", "returning", "select", "show", "some", "summarize", "symmetric", "table", "then", "to",
    "trailing", "true", "union", "unique", "unpivot", "using", "variadic", "when", "where", "window", "with",
];

/// `protocol.nested_columns`: a declared type that can hold a map, a
/// struct or an array, so its values may need marking. (A `TUPLE(...)`
/// column alone is not one: the Python worker sends its tuples as lists.)
pub fn is_nested_name(name: &str) -> bool {
    let up = name.to_uppercase();
    up.contains("MAP(") || up.contains("STRUCT(") || up.contains("UNION(") || up.contains(']')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        let s = TyNode::new(Ty::Struct(vec![
            ("a b".into(), TyNode::new(Ty::I32)),
            ("Select".into(), TyNode::new(Ty::I32)),
            ("ok".into(), TyNode::new(Ty::List(Box::new(TyNode::new(Ty::Date))))),
        ]));
        assert_eq!(s.name(), "STRUCT(\"a b\" INTEGER, \"Select\" INTEGER, ok DATE[])");
        let e = TyNode::new(Ty::Enum { internal: 1, values: vec!["a".into(), "it's".into()] });
        assert_eq!(e.name(), "ENUM('a', 'it''s')");
        let m = TyNode::new(Ty::Map(Box::new(TyNode::new(Ty::I32)), Box::new(TyNode::new(Ty::Varchar))));
        assert_eq!(m.name(), "MAP(INTEGER, VARCHAR)");
        assert!(is_nested_name("INTEGER[3]") && !is_nested_name("DECIMAL(10,2)"));
    }
}
