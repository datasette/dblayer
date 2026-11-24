//! Statement kinds, and the small text helpers of `dblayer/backends/duckdb.py`.

use libduckdb_sys as d;

/// Python's `StatementType` names (`str(s.type).rsplit(".", 1)[-1]`).
pub fn kind_name(t: u32) -> &'static str {
    match t {
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_SELECT => "SELECT",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_INSERT => "INSERT",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_UPDATE => "UPDATE",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXPLAIN => "EXPLAIN",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DELETE => "DELETE",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_PREPARE => "PREPARE",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CREATE => "CREATE",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXECUTE => "EXECUTE",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ALTER => "ALTER",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_TRANSACTION => "TRANSACTION",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_COPY => "COPY",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ANALYZE => "ANALYZE",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_VARIABLE_SET => "VARIABLE_SET",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CREATE_FUNC => "CREATE_FUNC",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DROP => "DROP",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXPORT => "EXPORT",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_PRAGMA => "PRAGMA",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_VACUUM => "VACUUM",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_CALL => "CALL",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_SET => "SET",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_LOAD => "LOAD",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_RELATION => "RELATION",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_EXTENSION => "EXTENSION",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN => "LOGICAL_PLAN",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_ATTACH => "ATTACH",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_DETACH => "DETACH",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_MULTI => "MULTI",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_COPY_DATABASE => "COPY_DATABASE",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_UPDATE_EXTENSIONS => "UPDATE_EXTENSIONS",
        d::duckdb_statement_type_DUCKDB_STATEMENT_TYPE_MERGE_INTO => "MERGE_INTO",
        _ => "INVALID",
    }
}

pub const DML: [&str; 4] = ["INSERT", "UPDATE", "DELETE", "MERGE_INTO"];
/// Statement kinds that return rows, and the only ones read connections run.
pub const RESULT_TYPES: [&str; 4] = ["SELECT", "EXPLAIN", "PRAGMA", "CALL"];

pub fn is_dml(kind: &str) -> bool {
    DML.contains(&kind)
}

pub fn is_result(kind: &str) -> bool {
    RESULT_TYPES.contains(&kind)
}

/// The kind of a statement that failed to prepare (a binder error, a
/// missing table), from its text. The Python backend learns the kind from
/// a parse alone, so it refuses a write on a read connection before DuckDB
/// binds anything; the C API gives the kind only for a prepared statement.
pub fn guess_kind(sql: &str) -> &'static str {
    let words = top_level_words(sql);
    let first = words.first().map(String::as_str).unwrap_or(if sql.contains('(') { "SELECT" } else { "" });
    let by_word = |w: &str| -> Option<&'static str> {
        Some(match w {
            "SELECT" | "VALUES" | "FROM" | "TABLE" | "SHOW" | "DESCRIBE" | "SUMMARIZE" | "PIVOT" | "UNPIVOT" => "SELECT",
            "EXPLAIN" => "EXPLAIN",
            "PRAGMA" => "PRAGMA",
            "CALL" => "CALL",
            "INSERT" => "INSERT",
            "UPDATE" => "UPDATE",
            "DELETE" => "DELETE",
            "MERGE" => "MERGE_INTO",
            "BEGIN" | "COMMIT" | "ROLLBACK" | "START" | "END" | "ABORT" => "TRANSACTION",
            "CREATE" => "CREATE",
            "DROP" => "DROP",
            "ALTER" => "ALTER",
            "COPY" => "COPY",
            "SET" | "RESET" => "SET",
            "ATTACH" => "ATTACH",
            "DETACH" => "DETACH",
            "LOAD" | "INSTALL" => "LOAD",
            "EXPORT" => "EXPORT",
            "VACUUM" => "VACUUM",
            "ANALYZE" => "ANALYZE",
            "CHECKPOINT" | "FORCE" => "PRAGMA",
            _ => return None,
        })
    };
    if first == "WITH" {
        // WITH ... INSERT/UPDATE/DELETE: the first such word outside
        // parentheses decides.
        for w in &words[1..] {
            if let Some(k @ ("INSERT" | "UPDATE" | "DELETE" | "MERGE_INTO")) = by_word(w) {
                return k;
            }
        }
        return "SELECT";
    }
    by_word(first).unwrap_or("INVALID")
}

/// Upper-cased words outside parentheses, quotes and comments.
fn top_level_words(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut depth = 0i32;
    let mut words = Vec::new();
    let mut cur = String::new();
    let flush = |cur: &mut String, words: &mut Vec<String>| {
        if !cur.is_empty() {
            words.push(cur.to_uppercase());
            cur.clear();
        }
    };
    while i < b.len() {
        let c = b[i];
        if c == b'-' && b.get(i + 1) == Some(&b'-') {
            flush(&mut cur, &mut words);
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            flush(&mut cur, &mut words);
            let mut nest = 1;
            i += 2;
            while i < b.len() && nest > 0 {
                if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                    nest += 1;
                    i += 2;
                } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                    nest -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if c == b'\'' || c == b'"' {
            flush(&mut cur, &mut words);
            i += 1;
            while i < b.len() && b[i] != c {
                i += 1;
            }
            i += 1;
            continue;
        }
        if c == b'(' {
            flush(&mut cur, &mut words);
            depth += 1;
        } else if c == b')' {
            flush(&mut cur, &mut words);
            depth -= 1;
        } else if c.is_ascii_alphanumeric() || c == b'_' {
            if depth <= 0 {
                cur.push(c as char);
            }
        } else {
            flush(&mut cur, &mut words);
        }
        i += 1;
    }
    flush(&mut cur, &mut words);
    words
}

/// `_strip_semicolon`.
pub fn strip_semicolon(sql: &str) -> String {
    let s = sql.trim();
    match s.strip_suffix(';') {
        Some(rest) => rest.trim_end().to_string(),
        None => s.to_string(),
    }
}

/// `_split_top_level`: split on commas outside parentheses and quotes.
fn split_top_level(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut buf = String::new();
    let mut quote: Option<char> = None;
    for ch in s.chars() {
        if let Some(q) = quote {
            buf.push(ch);
            if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(buf.trim().to_string());
                buf.clear();
                continue;
            }
            _ => {}
        }
        buf.push(ch);
    }
    if !buf.is_empty() {
        parts.push(buf.trim().to_string());
    }
    parts
}

fn is_ident(s: &str) -> bool {
    let mut it = s.chars();
    matches!(it.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && it.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// `_index_columns`: column names of a CREATE INDEX statement (None for an
/// expression), matched case-insensitively against the table's columns.
pub fn index_columns(sql: Option<&str>, columns: &[String]) -> Vec<Option<String>> {
    let Some(sql) = sql.filter(|s| !s.is_empty()) else {
        return vec![];
    };
    let (Some(open), Some(close)) = (sql.find('('), sql.rfind(')')) else {
        return vec![];
    };
    if close <= open {
        return vec![];
    }
    let fold = |s: &str| s.to_lowercase();
    split_top_level(&sql[open + 1..close])
        .into_iter()
        .map(|part| {
            let name = if part.len() >= 2 && part.starts_with('"') && part.ends_with('"') {
                Some(part[1..part.len() - 1].replace("\"\"", "\""))
            } else if is_ident(&part) {
                Some(part.clone())
            } else {
                None
            };
            name.and_then(|n| columns.iter().find(|c| fold(c) == fold(&n)).cloned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_from_text() {
        assert_eq!(guess_kind("insert into missing values (1)"), "INSERT");
        assert_eq!(guess_kind("  -- c\n /* x /* y */ */ (select 1)"), "SELECT");
        assert_eq!(guess_kind("with x as (select 1) insert into t select * from x"), "INSERT");
        assert_eq!(guess_kind("with x as (insert into t values (1)) select 1"), "SELECT");
        assert_eq!(guess_kind("begin"), "TRANSACTION");
    }

    #[test]
    fn index_column_names() {
        let cols = vec!["Name".to_string(), "id".to_string()];
        assert_eq!(
            index_columns(Some("CREATE INDEX i ON t(name, \"id\", lower(name));"), &cols),
            vec![Some("Name".to_string()), Some("id".to_string()), None]
        );
        assert_eq!(strip_semicolon(" create table t (a int); "), "create table t (a int)");
    }
}
