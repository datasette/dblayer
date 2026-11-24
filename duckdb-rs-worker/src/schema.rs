//! Introspection, as `_read_schema` and `_VERSION_SQL` in
//! `dblayer/backends/duckdb.py`, in the wire form of
//! `protocol.schema_to_wire`.

use crate::errors::DuckError;
use crate::ffi::{Connection, Ctx};
use crate::sql::{index_columns, strip_semicolon};
use crate::value::Value;

const HERE: &str = "database_name = current_database() AND schema_name = current_schema()";

fn version_sql() -> String {
    format!(
        "SELECT md5(coalesce(string_agg(x, chr(10) ORDER BY x), '')) FROM (
    SELECT 't' || table_name || chr(31) || coalesce(sql, '') AS x
        FROM duckdb_tables() WHERE {HERE}
    UNION ALL SELECT 'v' || view_name || chr(31) || coalesce(sql, '')
        FROM duckdb_views() WHERE NOT internal AND {HERE}
    UNION ALL SELECT 'i' || index_name || chr(31) || coalesce(sql, '')
        FROM duckdb_indexes() WHERE {HERE}
)"
    )
}

/// Every row of a query, as wire values.
pub fn query_rows(conn: &Connection, sql: &str) -> Result<Vec<Vec<Value>>, DuckError> {
    let ex = conn.extract(sql)?;
    let mut p = ex.prepare(conn, 0)?;
    let mut res = p.execute(false)?;
    let columns = res.columns.clone();
    let mut out = Vec::new();
    while let Some(chunk) = res.fetch()? {
        let rows = chunk
            .rows(&columns, 0, chunk.len(), &Ctx { tz: "UTC" })
            .map_err(|e| DuckError::from_message(format!("Conversion Error: {}", e.0)))?;
        for r in rows {
            if let Value::List(cells) = r {
                out.push(cells);
            }
        }
    }
    Ok(out)
}

/// The columns of `table` in the current schema, in order (empty when it
/// does not exist): what `_insert_rows` in `duckdb_rsworker.py` looks up.
pub fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>, DuckError> {
    let sql = format!(
        "SELECT c.column_name FROM duckdb_columns() c JOIN duckdb_tables() t ON c.table_oid = t.table_oid \
         WHERE t.{} AND t.table_name = $1 ORDER BY c.column_index",
        HERE.replace(" AND ", " AND t.")
    );
    let ex = conn.extract(&sql)?;
    let mut p = ex.prepare(conn, 0)?;
    p.bind(1, &crate::convert::Param::Str(table.to_string()))?;
    let mut res = p.execute(false)?;
    let columns = res.columns.clone();
    let mut out = Vec::new();
    while let Some(chunk) = res.fetch()? {
        let rows = chunk
            .rows(&columns, 0, chunk.len(), &Ctx { tz: "UTC" })
            .map_err(|e| DuckError::from_message(format!("Conversion Error: {}", e.0)))?;
        for r in rows {
            if let Value::List(cells) = r {
                out.extend(cells.first().and_then(Value::as_str).map(String::from));
            }
        }
    }
    Ok(out)
}

pub fn version(conn: &Connection) -> Result<String, DuckError> {
    let rows = query_rows(conn, &version_sql())?;
    Ok(rows.first().and_then(|r| r.first()).and_then(Value::as_str).unwrap_or("").to_string())
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

fn opt_s(v: &Value) -> Value {
    match v {
        Value::Str(x) => Value::Str(x.clone()),
        _ => Value::Null,
    }
}

fn strs(v: &Value) -> Vec<String> {
    match v {
        Value::List(l) => l.iter().map(s).collect(),
        _ => vec![],
    }
}

fn list(items: Vec<String>) -> Value {
    Value::List(items.into_iter().map(Value::Str).collect())
}

fn definition(sql: &Value) -> Value {
    match sql {
        Value::Str(x) => Value::Str(strip_semicolon(x)),
        _ => Value::Null,
    }
}

struct Col {
    name: String,
    ty: String,
    nullable: bool,
    default: Value,
    generated: bool,
}

pub fn read(conn: &Connection) -> Result<Value, DuckError> {
    let version = version(conn)?;
    let tables_rows = query_rows(conn, &format!("SELECT table_name, sql FROM duckdb_tables() WHERE {HERE} ORDER BY table_oid"))?;
    let views_rows = query_rows(
        conn,
        &format!("SELECT view_name, sql FROM duckdb_views() WHERE NOT internal AND {HERE} ORDER BY view_oid"),
    )?;
    let mut cols: Vec<(String, Vec<Col>)> = Vec::new();
    for r in query_rows(
        conn,
        &format!(
            "SELECT table_name, column_name, data_type, is_nullable, column_default, is_generated \
             FROM duckdb_columns() WHERE {HERE} ORDER BY table_oid, column_index"
        ),
    )? {
        let t = s(&r[0]);
        let c = Col { name: s(&r[1]), ty: s(&r[2]), nullable: r[3].is_true(), default: opt_s(&r[4]), generated: r[5].is_true() };
        match cols.iter_mut().find(|(n, _)| *n == t) {
            Some((_, v)) => v.push(c),
            None => cols.push((t, vec![c])),
        }
    }
    // (table, constraint_type, column names, constraint name, referenced table, referenced columns)
    let constraints = query_rows(
        conn,
        &format!(
            "SELECT table_name, constraint_type, constraint_column_names, constraint_name, \
             referenced_table, referenced_column_names FROM duckdb_constraints() \
             WHERE {HERE} AND constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY') \
             ORDER BY table_oid, constraint_index"
        ),
    )?;
    let index_rows = query_rows(
        conn,
        &format!("SELECT table_name, index_name, is_unique, sql FROM duckdb_indexes() WHERE {HERE} ORDER BY index_name"),
    )?;
    let pk_of = |table: &str| -> Vec<String> {
        // The last PRIMARY KEY row wins, as in the Python dict.
        constraints
            .iter()
            .rfind(|c| s(&c[0]) == table && s(&c[1]) == "PRIMARY KEY")
            .map(|c| strs(&c[2]))
            .unwrap_or_default()
    };
    let columns_of = |name: &str| -> Vec<Value> {
        let pk = pk_of(name);
        let Some((_, cs)) = cols.iter().find(|(n, _)| n == name) else { return vec![] };
        cs.iter()
            .map(|c| {
                let pos = pk.iter().position(|p| *p == c.name);
                Value::List(vec![
                    Value::str(&c.name),
                    Value::str(&c.ty),
                    Value::Bool(!c.nullable && pos.is_none()),
                    if c.generated { Value::Null } else { c.default.clone() },
                    Value::Int(pos.map(|p| p as i128 + 1).unwrap_or(0)),
                    Value::Bool(false),
                    Value::Bool(c.generated),
                ])
            })
            .collect()
    };
    let mut tables = Vec::new();
    for r in &tables_rows {
        let name = s(&r[0]);
        let columns = columns_of(&name);
        let names: Vec<String> = cols.iter().find(|(n, _)| *n == name).map(|(_, cs)| cs.iter().map(|c| c.name.clone()).collect()).unwrap_or_default();
        let mut fks = Vec::new();
        // (name, columns, unique, origin, partial, definition)
        let mut indexes: Vec<(String, Value)> = Vec::new();
        for c in constraints.iter().filter(|c| s(&c[0]) == name) {
            let ctype = s(&c[1]);
            if ctype == "FOREIGN KEY" {
                let other = s(&c[4]);
                let mut other_cols = strs(&c[5]);
                if other_cols.is_empty() {
                    other_cols = pk_of(&other);
                }
                fks.push(Value::List(vec![list(strs(&c[2])), Value::Str(other), list(other_cols), Value::Null, Value::Null]));
            } else {
                let iname = s(&c[3]);
                indexes.push((
                    iname.clone(),
                    Value::List(vec![
                        Value::Str(iname),
                        list(strs(&c[2])),
                        Value::Bool(true),
                        Value::str(if ctype == "PRIMARY KEY" { "pk" } else { "u" }),
                        Value::Bool(false),
                        Value::Null,
                    ]),
                ));
            }
        }
        for i in index_rows.iter().filter(|i| s(&i[0]) == name) {
            let iname = s(&i[1]);
            let isql = i[3].as_str();
            let icols = index_columns(isql, &names).into_iter().map(|c| c.map(Value::Str).unwrap_or(Value::Null)).collect();
            indexes.push((
                iname.clone(),
                Value::List(vec![
                    Value::Str(iname),
                    Value::List(icols),
                    Value::Bool(i[2].is_true()),
                    Value::str("c"),
                    Value::Bool(false),
                    definition(&i[3]),
                ]),
            ));
        }
        indexes.sort_by(|a, b| a.0.cmp(&b.0));
        tables.push(table_wire(&name, "table", columns, fks, indexes.into_iter().map(|(_, v)| v).collect(), definition(&r[1])));
    }
    let views = views_rows
        .iter()
        .map(|r| {
            let name = s(&r[0]);
            table_wire(&name, "view", columns_of(&name), vec![], vec![], definition(&r[1]))
        })
        .collect();
    Ok(Value::obj(vec![
        ("version", Value::Str(version)),
        ("tables", Value::List(tables)),
        ("views", Value::List(views)),
        ("triggers", Value::List(vec![])),
    ]))
}

fn table_wire(name: &str, kind: &str, columns: Vec<Value>, fks: Vec<Value>, indexes: Vec<Value>, definition: Value) -> Value {
    Value::obj(vec![
        ("name", Value::str(name)),
        ("kind", Value::str(kind)),
        ("columns", Value::List(columns)),
        ("foreign_keys", Value::List(fks)),
        ("indexes", Value::List(indexes)),
        ("definition", definition),
        ("virtual", Value::Bool(false)),
        ("hidden", Value::Bool(false)),
        ("checks", Value::List(vec![])),
    ])
}
