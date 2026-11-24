//! Error replies: SQLite result codes mapped as `SqliteDialect.map_exception`
//! maps them in-process, with the exception class CPython's `_sqlite`
//! module would raise as the cause (the C worker's err_* functions).

use crate::ffi::{self, Db};

include!(concat!(env!("OUT_DIR"), "/errnames.rs"));

pub fn errname(code: i32) -> Option<&'static str> {
    ERRNAMES.iter().find(|(c, _)| *c == code).map(|(_, n)| *n)
}

#[derive(Debug, Default)]
pub struct E {
    /// Exception class name; empty: no error.
    pub ty: &'static str,
    pub msg: String,
    pub code: Option<String>,
    pub cause: Option<(&'static str, String)>,
    pub builtin: bool,
    pub fatal: bool,
    /// The extended result code, when the error came from SQLite.
    pub ext: i32,
}

impl E {
    pub fn new(ty: &'static str, msg: impl Into<String>) -> E {
        E { ty, msg: msg.into(), ..E::default() }
    }

    pub fn coded(ty: &'static str, code: &str, msg: impl Into<String>) -> E {
        E { ty, msg: msg.into(), code: Some(code.to_string()), ..E::default() }
    }

    /// A built-in Python exception (`TypeError`, `ValueError`, ...).
    pub fn builtin(ty: &'static str, msg: impl Into<String>) -> E {
        E { ty, msg: msg.into(), builtin: true, ..E::default() }
    }

    /// A sqlite3.ProgrammingError raised by CPython's _sqlite module.
    pub fn prog(msg: impl Into<String>) -> E {
        let m = msg.into();
        E { ty: "QueryError", cause: Some(("sqlite3.ProgrammingError", m.clone())), msg: m, ..E::default() }
    }

    /// sqlite3.OperationalError("interrupted") raised without a SQLite code.
    pub fn interrupted() -> E {
        E {
            ty: "QueryInterrupted",
            msg: "query interrupted (time limit exceeded or cancelled)".into(),
            cause: Some(("sqlite3.OperationalError", "interrupted".into())),
            ..E::default()
        }
    }

    pub fn protocol(msg: impl Into<String>) -> E {
        E::coded("QueryError", "PROTOCOL", msg)
    }

    /// What SQLite reports for `rc` on `db` (None: no connection).
    pub fn sqlite(db: Option<&Db>, rc: i32) -> E {
        let mut ext = db.map(|d| d.extended_errcode()).unwrap_or(rc);
        if ext == ffi::SQLITE_OK || (ext & 0xff) != (rc & 0xff) {
            ext = rc;
        }
        let primary = ext & 0xff;
        let msg = match db {
            Some(d) => d.errmsg(),
            None => ffi::errstr(rc),
        };
        E::sqlite_parts(ext, primary, msg)
    }

    pub fn sqlite_parts(ext: i32, primary: i32, msg: String) -> E {
        let name = errname(ext);
        let cls = py_exc(primary);
        let mut e = if primary == ffi::SQLITE_INTERRUPT {
            E::new("QueryInterrupted", "query interrupted (time limit exceeded or cancelled)")
        } else if cls == "sqlite3.IntegrityError" {
            E { ty: "IntegrityError", msg: msg.clone(), code: name.map(String::from), ..E::default() }
        } else if ext == ffi::SQLITE_NOMEM || ext == ffi::SQLITE_TOOBIG || ext == ffi::SQLITE_FULL {
            // Python's sqlite3 raises MemoryError for SQLITE_NOMEM and its
            // worker then exits; here the heap limit leaves the process
            // intact, so it is an ordinary ResourceLimitExceeded.
            E::new("ResourceLimitExceeded", msg.clone())
        } else if name == Some("SQLITE_BUSY_SNAPSHOT") {
            E::new("WriteConflict", msg.clone())
        } else {
            E { ty: "QueryError", msg: msg.clone(), code: name.map(String::from), ..E::default() }
        };
        e.cause = Some((cls, msg));
        e.ext = ext;
        e
    }
}

/// The class CPython's _pysqlite_seterror() raises for a primary code.
fn py_exc(primary: i32) -> &'static str {
    use ffi::*;
    match primary {
        SQLITE_INTERNAL | SQLITE_NOTFOUND => "sqlite3.InternalError",
        SQLITE_NOMEM => "MemoryError",
        SQLITE_ERROR | SQLITE_PERM | SQLITE_ABORT | SQLITE_BUSY | SQLITE_LOCKED | SQLITE_READONLY
        | SQLITE_INTERRUPT | SQLITE_IOERR | SQLITE_FULL | SQLITE_CANTOPEN | SQLITE_PROTOCOL | SQLITE_EMPTY
        | SQLITE_SCHEMA => "sqlite3.OperationalError",
        SQLITE_CORRUPT => "sqlite3.DatabaseError",
        SQLITE_TOOBIG => "sqlite3.DataError",
        SQLITE_CONSTRAINT | SQLITE_MISMATCH => "sqlite3.IntegrityError",
        SQLITE_MISUSE | SQLITE_RANGE => "sqlite3.InterfaceError",
        _ => "sqlite3.DatabaseError",
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn names() {
        assert_eq!(super::errname(19 | (6 << 8)), Some("SQLITE_CONSTRAINT_PRIMARYKEY"));
        assert_eq!(super::errname(1), Some("SQLITE_ERROR"));
    }
}
