//! The schema, as dblayer.backends.sqlite._read_schema reads it and
//! dblayer.worker.protocol.schema_to_wire shapes it (a port of the C
//! worker's schema.c; see there for the shape).

use crate::codec::W;
use crate::ffi::{self, Db, Stmt};

struct Master {
    ty: Vec<u8>,
    name: Vec<u8>,
    tbl_name: Option<Vec<u8>>,
    sql: Option<Vec<u8>>,
}

fn text(st: &Stmt, i: i32) -> Option<Vec<u8>> {
    if st.column_type(i) == 5 { None } else { Some(st.column_text(i).to_vec()) }
}

fn text_or_nil(w: &mut W, s: Option<&[u8]>) {
    match s {
        Some(s) => w.str_lossy(s),
        None => w.nil(),
    }
}

/// A column value: text, integer, float, blob or NULL.
fn column(w: &mut W, st: &Stmt, i: i32) {
    match st.column_type(i) {
        5 => w.nil(),
        ffi::SQLITE_INTEGER => w.int(st.column_int(i)),
        ffi::SQLITE_FLOAT => w.double(st.column_double(i)),
        ffi::SQLITE_BLOB => w.bin(st.column_blob(i)),
        _ => w.str_lossy(st.column_text(i)),
    }
}

fn prep(db: &Db, sql: &str, arg: Option<&[u8]>) -> Result<Stmt, i32> {
    let (st, _) = db.prepare(sql.as_bytes())?;
    let mut st = st.ok_or(ffi::SQLITE_ERROR)?;
    if let Some(a) = arg {
        let rc = st.bind_text(1, a);
        if rc != ffi::SQLITE_OK {
            return Err(rc);
        }
    }
    Ok(st)
}

fn done(rc: i32) -> Result<(), i32> {
    if rc == ffi::SQLITE_DONE { Ok(()) } else { Err(rc) }
}

fn write_columns(db: &Db, w: &mut W, table: &[u8]) -> Result<(), i32> {
    let mut st = prep(db, "select cid, name, type, \"notnull\", dflt_value, pk, hidden from pragma_table_xinfo(?)", Some(table))?;
    w.cstr("columns");
    w.array_open();
    let rc = loop {
        let rc = st.step();
        if rc != ffi::SQLITE_ROW {
            break rc;
        }
        let hidden = st.column_int(6);
        w.array(7);
        column(w, &st, 1);
        w.str_lossy(st.column_text(2));
        w.bool(st.column_int(3) != 0);
        column(w, &st, 4);
        column(w, &st, 5);
        w.bool(hidden == 1);
        w.bool(hidden == 2 || hidden == 3);
        w.end();
    };
    w.end();
    done(rc)
}

/// The primary key columns of `table`: written when `w` is given; the count.
fn write_pks(db: &Db, mut w: Option<&mut W>, table: &[u8]) -> Result<u32, i32> {
    let mut st = prep(db, "select name, pk from pragma_table_info(?) where pk > 0 order by pk", Some(table))?;
    let mut n = 0;
    let rc = loop {
        let rc = st.step();
        if rc != ffi::SQLITE_ROW {
            break rc;
        }
        if let Some(w) = w.as_deref_mut() {
            column(w, &st, 0);
        }
        n += 1;
    };
    done(rc)?;
    Ok(n)
}

struct Fk {
    id: i64,
    other: Option<Vec<u8>>,
    from: Option<Vec<u8>>,
    to: Option<Vec<u8>>,
    on_update: Option<Vec<u8>>,
    on_delete: Option<Vec<u8>>,
}

fn write_foreign_keys(db: &Db, w: &mut W, table: &[u8]) -> Result<(), i32> {
    let mut st = prep(
        db,
        "select id, seq, \"table\", \"from\", \"to\", on_update, on_delete, match from pragma_foreign_key_list(?) order by id, seq",
        Some(table),
    )?;
    let mut rows = Vec::new();
    let rc = loop {
        let rc = st.step();
        if rc != ffi::SQLITE_ROW {
            break rc;
        }
        rows.push(Fk {
            id: st.column_int(0),
            other: text(&st, 2),
            from: text(&st, 3),
            to: text(&st, 4),
            on_update: text(&st, 5),
            on_delete: text(&st, 6),
        });
    };
    drop(st);
    done(rc)?;
    w.cstr("foreign_keys");
    w.array_open();
    // chunk_by: rows of one foreign key are adjacent (ordered by id, seq).
    for group in rows.chunk_by(|a, b| a.id == b.id) {
        let first = &group[0];
        let any_null = group.iter().any(|r| r.to.is_none());
        w.array(5);
        w.array(group.len() as u32);
        for r in group {
            text_or_nil(w, r.from.as_deref());
        }
        w.end();
        text_or_nil(w, first.other.as_deref());
        let mut npk = 0;
        if any_null && let Some(other) = &first.other {
            npk = write_pks(db, None, other)?;
        }
        if npk > 0 {
            // references the other table's primary key
            w.array(npk);
            write_pks(db, Some(w), first.other.as_deref().unwrap_or_default())?;
            w.end();
        } else {
            w.array(group.len() as u32);
            for r in group {
                text_or_nil(w, r.to.as_deref());
            }
            w.end();
        }
        text_or_nil(w, first.on_update.as_deref());
        text_or_nil(w, first.on_delete.as_deref());
        w.end();
    }
    w.end();
    Ok(())
}

struct Index {
    name: Option<Vec<u8>>,
    origin: Option<Vec<u8>>,
    unique: bool,
    partial: bool,
}

fn write_indexes(db: &Db, w: &mut W, table: &[u8], master: &[Master]) -> Result<(), i32> {
    let mut st = prep(db, "select seq, name, \"unique\", origin, partial from pragma_index_list(?) order by seq", Some(table))?;
    let mut rows = Vec::new();
    let rc = loop {
        let rc = st.step();
        if rc != ffi::SQLITE_ROW {
            break rc;
        }
        rows.push(Index { name: text(&st, 1), unique: st.column_int(2) != 0, origin: text(&st, 3), partial: st.column_int(4) != 0 });
    };
    drop(st);
    done(rc)?;
    // By name, as bytes (strcmp). Sorting an empty Vec is fine in Rust.
    rows.sort_by(|a, b| a.name.as_deref().unwrap_or_default().cmp(b.name.as_deref().unwrap_or_default()));
    w.cstr("indexes");
    w.array(rows.len() as u32);
    for r in &rows {
        w.array(6);
        text_or_nil(w, r.name.as_deref());
        w.array_open();
        let mut ic = prep(db, "select name from pragma_index_info(?) order by seqno", Some(r.name.as_deref().unwrap_or_default()))?;
        let rc = loop {
            let rc = ic.step();
            if rc != ffi::SQLITE_ROW {
                break rc;
            }
            column(w, &ic, 0);
        };
        drop(ic);
        done(rc)?;
        w.end();
        w.bool(r.unique);
        text_or_nil(w, r.origin.as_deref());
        w.bool(r.partial);
        let def = master
            .iter()
            .filter(|m| m.ty == b"index" && r.name.as_deref() == Some(&m.name[..]))
            .next_back()
            .and_then(|m| m.sql.as_deref());
        text_or_nil(w, def);
        w.end();
    }
    w.end();
    Ok(())
}

/// Python's (sql or "").lstrip().upper().startswith("CREATE VIRTUAL")
fn looks_virtual(sql: Option<&[u8]>) -> bool {
    let Some(s) = sql else { return false };
    let i = s.iter().position(|&c| !(c == b' ' || (b'\t'..=b'\r').contains(&c) || (0x1c..=0x1f).contains(&c))).unwrap_or(s.len());
    let t = &s[i..];
    t.len() >= 14 && t[..14].eq_ignore_ascii_case(b"CREATE VIRTUAL")
}

fn write_table(db: &Db, w: &mut W, m: &Master, is_view: bool, kinds: &[(Vec<u8>, Vec<u8>)], master: &[Master]) -> Result<(), i32> {
    w.map(9);
    w.cstr("name");
    w.str_lossy(&m.name);
    w.cstr("kind");
    w.cstr(if is_view { "view" } else { "table" });
    write_columns(db, w, &m.name)?;
    if is_view {
        w.cstr("foreign_keys");
        w.array(0);
        w.end();
        w.cstr("indexes");
        w.array(0);
        w.end();
    } else {
        write_foreign_keys(db, w, &m.name)?;
        write_indexes(db, w, &m.name, master)?;
    }
    let kind: &[u8] = kinds.iter().filter(|(n, _)| *n == m.name).next_back().map(|(_, t)| &t[..]).unwrap_or(b"table");
    w.cstr("definition");
    text_or_nil(w, m.sql.as_deref());
    w.cstr("virtual");
    w.bool(!is_view && (kind == b"virtual" || looks_virtual(m.sql.as_deref())));
    w.cstr("hidden");
    w.bool(!is_view && kind == b"shadow");
    w.cstr("checks");
    w.array(0);
    w.end();
    w.end();
    Ok(())
}

/// Write the whole schema; Err is a SQLite result code.
pub fn write(db: &Db, w: &mut W) -> Result<(), i32> {
    let mut st = prep(db, "pragma schema_version", None)?;
    let rc = st.step();
    if rc != ffi::SQLITE_ROW {
        return Err(rc);
    }
    let version = st.column_int(0);
    drop(st);
    let mut st = prep(db, "select type, name, tbl_name, sql from sqlite_master order by rowid", None)?;
    let mut master = Vec::new();
    let rc = loop {
        let rc = st.step();
        if rc != ffi::SQLITE_ROW {
            break rc;
        }
        master.push(Master {
            ty: text(&st, 0).unwrap_or_default(),
            name: text(&st, 1).unwrap_or_default(),
            tbl_name: text(&st, 2),
            sql: text(&st, 3),
        });
    };
    drop(st);
    done(rc)?;
    let mut kinds = Vec::new();
    if let Ok(mut st) = prep(db, "select name, type from pragma_table_list where schema = 'main'", None) {
        while st.step() == ffi::SQLITE_ROW {
            kinds.push((text(&st, 0).unwrap_or_default(), text(&st, 1).unwrap_or_else(|| b"table".to_vec())));
        }
    }
    w.map(4);
    w.cstr("version");
    w.int(version);
    for pass in 0..2 {
        let want: &[u8] = if pass == 0 { b"table" } else { b"view" };
        w.cstr(if pass == 0 { "tables" } else { "views" });
        w.array_open();
        for m in master.iter().filter(|m| m.ty == want && !m.name.starts_with(b"sqlite_")) {
            write_table(db, w, m, pass == 1, &kinds, &master)?;
        }
        w.end();
    }
    w.cstr("triggers");
    w.array_open();
    for m in master.iter().filter(|m| m.ty == b"trigger") {
        w.array(3);
        w.str_lossy(&m.name);
        text_or_nil(w, m.tbl_name.as_deref());
        text_or_nil(w, m.sql.as_deref());
        w.end();
    }
    w.end();
    w.end();
    Ok(())
}
