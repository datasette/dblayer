//! SQL text helpers with the rules of the Python side (a port of the C
//! worker's sqlutil.c): the statement's leading verb as
//! `dblayer.backends.sqlite._leading_keyword` finds it, CPython's
//! "is this DML" test for `rowcount`, comment-aware left strip, and a path
//! as a `file:` URI.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kw {
    None,
    Other,
    Select,
    Insert,
    Update,
    Delete,
    Replace,
    Values,
    Merge,
}

fn ident_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}
fn name_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_' || c >= 0x80
}
fn name_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80
}

fn verb(w: &[u8]) -> Kw {
    const V: [(&[u8], Kw); 7] = [
        (b"SELECT", Kw::Select),
        (b"INSERT", Kw::Insert),
        (b"UPDATE", Kw::Update),
        (b"DELETE", Kw::Delete),
        (b"REPLACE", Kw::Replace),
        (b"VALUES", Kw::Values),
        (b"MERGE", Kw::Merge),
    ];
    V.iter().find(|(s, _)| s.eq_ignore_ascii_case(w)).map(|(_, k)| *k).unwrap_or(Kw::Other)
}

fn find(s: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    if from >= s.len() {
        return None;
    }
    s[from..].windows(needle.len()).position(|w| w == needle).map(|p| p + from)
}

fn memchr(s: &[u8], from: usize, c: u8) -> Option<usize> {
    s.get(from..)?.iter().position(|&b| b == c).map(|p| p + from)
}

pub fn leading_keyword(s: &[u8]) -> Kw {
    let n = s.len();
    let mut depth = 0i32;
    let mut have_first = false;
    let mut i = 0;
    while i < n {
        let c = s[i];
        if c == b'\'' {
            let mut j = i + 1;
            loop {
                if j >= n {
                    i = n;
                    break;
                }
                if s[j] == b'\'' {
                    if j + 1 < n && s[j + 1] == b'\'' {
                        j += 2;
                        continue;
                    }
                    i = j + 1;
                    break;
                }
                j += 1;
            }
        } else if c == b'"' || c == b'`' {
            let mut j = i + 1;
            loop {
                let Some(end) = memchr(s, j, c) else {
                    i = n;
                    break;
                };
                if end + 1 < n && s[end + 1] == c {
                    j = end + 2;
                    continue;
                }
                i = end + 1;
                break;
            }
        } else if c == b'[' {
            i = memchr(s, i + 1, b']').map(|e| e + 1).unwrap_or(n);
        } else if c == b'-' {
            if i + 1 < n && s[i + 1] == b'-' {
                i = memchr(s, i + 2, b'\n').map(|e| e + 1).unwrap_or(n);
            } else {
                i += 1;
            }
        } else if c == b'/' {
            if i + 1 < n && s[i + 1] == b'*' {
                i = find(s, i + 2, b"*/").map(|e| e + 2).unwrap_or(n);
            } else {
                i += 1;
            }
        } else if c == b':' {
            if i + 1 < n && s[i + 1] == b':' {
                i += 2;
            } else if i + 1 < n && name_start(s[i + 1]) && (i == 0 || !ident_char(s[i - 1])) {
                let mut j = i + 2;
                while j < n && name_char(s[j]) {
                    j += 1;
                }
                i = j;
            } else {
                i += 1;
            }
        } else if c == b'?' {
            let mut j = i + 1;
            while j < n && s[j].is_ascii_digit() {
                j += 1;
            }
            i = j;
        } else if c == b'(' {
            depth += 1;
            i += 1;
        } else if c == b')' {
            depth = (depth - 1).max(0);
            i += 1;
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let mut j = i + 1;
            while j < n && ident_char(s[j]) {
                j += 1;
            }
            if i > 0 && ident_char(s[i - 1]) {
                i = j;
                continue;
            }
            let word = &s[i..j];
            let kw = verb(word);
            let is_with = word.eq_ignore_ascii_case(b"WITH");
            if !have_first {
                if depth > 0 && kw == Kw::Other && !is_with {
                    i = j;
                    continue;
                }
                have_first = true;
                if !is_with {
                    return kw;
                }
            } else if depth == 0 && kw != Kw::Other {
                return kw;
            }
            i = j;
        } else {
            i += 1;
        }
    }
    if have_first { Kw::Other } else { Kw::None }
}

/// Skip whitespace and comments; `None` if nothing is left.
pub fn lstrip_sql(s: &[u8]) -> Option<usize> {
    let mut p = 0;
    let end = s.len();
    while p < end {
        match s[p] {
            b' ' | b'\t' | 0x0c | b'\n' | b'\r' => p += 1,
            b'-' if p + 1 < end && s[p + 1] == b'-' => {
                p += 2;
                while p < end && s[p] != b'\n' {
                    p += 1;
                }
                if p == end {
                    return None;
                }
                p += 1;
            }
            b'/' if p + 1 < end && s[p + 1] == b'*' => {
                p += 2;
                while p + 1 < end && !(s[p] == b'*' && s[p + 1] == b'/') {
                    p += 1;
                }
                if p + 1 >= end {
                    return None;
                }
                p += 2;
            }
            _ => return Some(p),
        }
    }
    None
}

/// CPython's sqlite3 "is DML" rule (rowcount is reported only for these).
pub fn cpython_is_dml(s: &[u8]) -> bool {
    let Some(p) = lstrip_sql(s) else { return false };
    let t = &s[p..];
    let starts = |w: &[u8]| t.len() >= w.len() && t[..w.len()].eq_ignore_ascii_case(w);
    starts(b"insert") || starts(b"update") || starts(b"delete") || starts(b"replace")
}

pub fn path_as_uri(path: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(7 + path.len() * 3);
    out.push_str("file://");
    for &c in path {
        if c.is_ascii_alphanumeric() || matches!(c, b'/' | b'_' | b'.' | b'-' | b'~') {
            out.push(c as char);
        } else {
            out.push('%');
            out.push(HEX[(c >> 4) as usize] as char);
            out.push(HEX[(c & 15) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords() {
        assert_eq!(leading_keyword(b"  select 1"), Kw::Select);
        assert_eq!(leading_keyword(b"with x as (select 1) insert into t select * from x"), Kw::Insert);
        assert_eq!(leading_keyword(b"-- c\n/* x */ Replace into t values (1)"), Kw::Replace);
        assert_eq!(leading_keyword(b"create table t (x)"), Kw::Other);
        assert_eq!(leading_keyword(b"  "), Kw::None);
        assert!(cpython_is_dml(b" /* a */ INSERT into t values (1)"));
        assert!(!cpython_is_dml(b"with x as (select 1) insert into t select * from x"));
        assert_eq!(lstrip_sql(b"select 1; -- tail"), Some(0));
        assert_eq!(lstrip_sql(b" -- tail"), None);
    }

    #[test]
    fn uri() {
        assert_eq!(path_as_uri(b"/a b/c?.db"), "file:///a%20b/c%3F.db");
    }
}
