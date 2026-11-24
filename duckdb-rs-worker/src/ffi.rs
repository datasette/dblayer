//! Safe wrappers over DuckDB's C API (`libduckdb-sys`) and the two libc calls
//! the worker needs. Every `unsafe` block in the crate is in this file.
//!
//! Ownership follows the C API: each wrapper owns one handle and destroys it
//! in `Drop`. A `Connection` is used by one thread at a time (its owner); the
//! only call made on it from other threads is `duckdb_interrupt`, through an
//! `InterruptHandle`, which DuckDB documents as safe from another thread.
//! The worker makes sure no interrupt is sent after the connection is
//! disconnected (`worker::ConnShared::closed`).

#![allow(clippy::missing_safety_doc)]

use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;

use libduckdb_sys as d;

use crate::convert::{self, ConvError, PTy, Param};
use crate::errors::DuckError;
use crate::types::{Ty, TyNode};
use crate::value::{EXT_TUPLE, Value};

fn cstring(s: &str) -> Result<CString, DuckError> {
    CString::new(s).map_err(|_| DuckError::new(crate::errors::INVALID_INPUT, "Invalid Input Error: string contains a NUL byte"))
}

/// Copy a C string owned by DuckDB (not freed here).
fn borrowed(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    // SAFETY: DuckDB returns NUL-terminated strings valid until the owning
    // object is destroyed; we copy before that.
    unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

/// Copy and free a C string DuckDB allocated for the caller (`duckdb_free`).
fn owned(p: *mut c_char) -> Option<String> {
    if p.is_null() {
        return None;
    }
    let s = borrowed(p);
    // SAFETY: the pointer came from DuckDB with "free with duckdb_free".
    unsafe { d::duckdb_free(p as *mut c_void) };
    Some(s)
}

pub fn library_version() -> String {
    // SAFETY: returns a static string.
    borrowed(unsafe { d::duckdb_library_version() })
}

// -- database ----------------------------------------------------------------------

pub struct Database {
    raw: d::duckdb_database,
}

// SAFETY: a duckdb_database may be used from any thread (DuckDB instances are
// thread-safe); connections are made from it on several threads.
unsafe impl Send for Database {}
unsafe impl Sync for Database {}

impl Database {
    /// Open (or create) the file with these settings; errors as text.
    pub fn open(path: &str, settings: &[(String, String)]) -> Result<Database, DuckError> {
        let cpath = cstring(path)?;
        let mut config: d::duckdb_config = ptr::null_mut();
        // SAFETY: out-pointer to a local; destroyed below on every path.
        if unsafe { d::duckdb_create_config(&mut config) } != d::DuckDBSuccess {
            return Err(DuckError::from_message("Invalid Input Error: could not create config"));
        }
        for (k, v) in settings {
            let (ck, cv) = match (cstring(k), cstring(v)) {
                (Ok(a), Ok(b)) => (a, b),
                (Err(e), _) | (_, Err(e)) => {
                    // SAFETY: config was created above.
                    unsafe { d::duckdb_destroy_config(&mut config) };
                    return Err(e);
                }
            };
            // SAFETY: valid config and NUL-terminated strings.
            if unsafe { d::duckdb_set_config(config, ck.as_ptr(), cv.as_ptr()) } != d::DuckDBSuccess {
                // SAFETY: as above.
                unsafe { d::duckdb_destroy_config(&mut config) };
                return Err(DuckError::from_message(format!(
                    "Invalid Input Error: The following options were not recognized: {k}"
                )));
            }
        }
        let mut raw: d::duckdb_database = ptr::null_mut();
        let mut err: *mut c_char = ptr::null_mut();
        // SAFETY: valid path, out-pointers to locals.
        let state = unsafe { d::duckdb_open_ext(cpath.as_ptr(), &mut raw, config, &mut err) };
        // SAFETY: config is no longer needed once open returns.
        unsafe { d::duckdb_destroy_config(&mut config) };
        if state != d::DuckDBSuccess {
            let msg = owned(err).unwrap_or_else(|| "IO Error: could not open database".into());
            if !raw.is_null() {
                // SAFETY: duckdb_close accepts a database left by a failed open.
                unsafe { d::duckdb_close(&mut raw) };
            }
            return Err(DuckError::from_message(msg));
        }
        Ok(Database { raw })
    }

    pub fn connect(&self) -> Result<Connection, DuckError> {
        let mut raw: d::duckdb_connection = ptr::null_mut();
        // SAFETY: self.raw is an open database.
        if unsafe { d::duckdb_connect(self.raw, &mut raw) } != d::DuckDBSuccess {
            return Err(DuckError::from_message("Connection Error: could not connect"));
        }
        Ok(Connection { raw })
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // SAFETY: owned handle; closing checkpoints and releases the file lock.
        unsafe { d::duckdb_close(&mut self.raw) };
    }
}

// -- connection --------------------------------------------------------------------

pub struct Connection {
    raw: d::duckdb_connection,
}

// SAFETY: a connection is moved to the thread that owns it and used there
// only (interrupts go through InterruptHandle).
unsafe impl Send for Connection {}

/// The one cross-thread operation on a connection.
#[derive(Clone, Copy)]
pub struct InterruptHandle(d::duckdb_connection);

// SAFETY: duckdb_interrupt is documented for use from another thread; the
// worker never calls it after the connection is disconnected.
unsafe impl Send for InterruptHandle {}
unsafe impl Sync for InterruptHandle {}

impl InterruptHandle {
    pub fn interrupt(&self) {
        // SAFETY: see the type's comment.
        unsafe { d::duckdb_interrupt(self.0) };
    }
}

impl Connection {
    pub fn interrupt_handle(&self) -> InterruptHandle {
        InterruptHandle(self.raw)
    }

    /// Run SQL with no parameters, discarding any result.
    pub fn query(&self, sql: &str) -> Result<(), DuckError> {
        self.query_once(sql)
    }

    /// Split SQL into statements (a parse; nothing is bound or run).
    pub fn extract(&self, sql: &str) -> Result<Extracted, DuckError> {
        self.extract_once(sql)
    }

    fn query_once(&self, sql: &str) -> Result<(), DuckError> {
        let c = cstring(sql)?;
        let mut res = RawResult::zeroed();
        // SAFETY: open connection, NUL-terminated SQL, result destroyed by RawResult.
        let state = unsafe { d::duckdb_query(self.raw, c.as_ptr(), &mut res.0) };
        if state != d::DuckDBSuccess {
            return Err(res.error());
        }
        Ok(())
    }

    /// Split SQL into statements (a parse; nothing is bound or run).
    fn extract_once(&self, sql: &str) -> Result<Extracted, DuckError> {
        let c = cstring(sql)?;
        let mut raw: d::duckdb_extracted_statements = ptr::null_mut();
        // SAFETY: open connection; the extracted object is destroyed by Extracted.
        let n = unsafe { d::duckdb_extract_statements(self.raw, c.as_ptr(), &mut raw) };
        let ex = Extracted { raw, count: n as usize };
        if n == 0 {
            // SAFETY: valid (possibly error-carrying) extracted object.
            let err = unsafe { d::duckdb_extract_statements_error(ex.raw) };
            if !err.is_null() {
                return Err(DuckError::from_message(borrowed(err)));
            }
        }
        Ok(ex)
    }

    /// The session's `TimeZone` setting (TIMESTAMP WITH TIME ZONE values are
    /// returned in it).
    pub fn timezone(&self) -> Option<String> {
        self.setting("TimeZone")
    }

    pub fn setting(&self, name: &str) -> Option<String> {
        let cname = cstring(name).ok()?;
        let mut ctx: d::duckdb_client_context = ptr::null_mut();
        // SAFETY: open connection; the context is destroyed below.
        unsafe { d::duckdb_connection_get_client_context(self.raw, &mut ctx) };
        if ctx.is_null() {
            return None;
        }
        // SAFETY: valid context and name; the value is destroyed below.
        let mut v = unsafe { d::duckdb_client_context_get_config_option(ctx, cname.as_ptr(), ptr::null_mut()) };
        let out = if v.is_null() {
            None
        } else {
            // SAFETY: valid value; the varchar is freed by `owned`.
            let s = owned(unsafe { d::duckdb_get_varchar(v) });
            // SAFETY: owned value.
            unsafe { d::duckdb_destroy_value(&mut v) };
            s
        };
        // SAFETY: owned context.
        unsafe { d::duckdb_destroy_client_context(&mut ctx) };
        out
    }

    /// An appender on `table` for `columns` (all, in table order, if empty).
    pub fn appender(&self, table: &str, columns: &[String]) -> Result<Appender, DuckError> {
        let ct = cstring(table)?;
        let mut raw: d::duckdb_appender = ptr::null_mut();
        // SAFETY: open connection; the appender is destroyed by Appender.
        let state = unsafe { d::duckdb_appender_create_ext(self.raw, ptr::null(), ptr::null(), ct.as_ptr(), &mut raw) };
        let app = Appender { raw };
        if state != d::DuckDBSuccess {
            return Err(app.error());
        }
        for c in columns {
            let cc = cstring(c)?;
            // SAFETY: valid appender and column name.
            if unsafe { d::duckdb_appender_add_column(app.raw, cc.as_ptr()) } != d::DuckDBSuccess {
                return Err(app.error());
            }
        }
        Ok(app)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: owned handle.
        unsafe { d::duckdb_disconnect(&mut self.raw) };
    }
}

// -- statements --------------------------------------------------------------------

pub struct Extracted {
    raw: d::duckdb_extracted_statements,
    pub count: usize,
}

impl Extracted {
    pub fn prepare(&self, conn: &Connection, index: usize) -> Result<Prepared, DuckError> {
        self.prepare_once(conn, index)
    }

    fn prepare_once(&self, conn: &Connection, index: usize) -> Result<Prepared, DuckError> {
        let mut raw: d::duckdb_prepared_statement = ptr::null_mut();
        // SAFETY: index < count is checked by the caller; Prepared destroys the
        // statement even when the prepare failed, as the API requires.
        let state = unsafe { d::duckdb_prepare_extracted_statement(conn.raw, self.raw, index as u64, &mut raw) };
        let p = Prepared { raw };
        if state != d::DuckDBSuccess {
            // SAFETY: valid (failed) prepared statement.
            let msg = borrowed(unsafe { d::duckdb_prepare_error(p.raw) });
            if msg.is_empty() {
                // DuckDB 2.0 fails the prepare of an extracted statement
                // with no message when the connection still has an open
                // streaming result (duckdb_prepare says why).
                return Err(DuckError::new(
                    crate::errors::RESOURCE_IN_USE,
                    "Resource In Use Error: connection has an open result; drain or destroy it before starting a new query",
                ));
            }
            return Err(DuckError::from_message(msg));
        }
        Ok(p)
    }
}

impl Drop for Extracted {
    fn drop(&mut self) {
        // SAFETY: owned handle.
        unsafe { d::duckdb_destroy_extracted(&mut self.raw) };
    }
}

pub struct Prepared {
    raw: d::duckdb_prepared_statement,
}

// SAFETY: statements, results and chunks are not tied to the thread that
// made them; each is owned and used by one thread at a time (they move with
// the connection's state into its thread, and never leave it).
unsafe impl Send for Prepared {}
unsafe impl Send for RawResult {}
unsafe impl Send for Chunk {}

impl Prepared {
    /// `duckdb_statement_type` of the statement.
    pub fn statement_type(&self) -> u32 {
        // SAFETY: valid prepared statement.
        unsafe { d::duckdb_prepared_statement_type(self.raw) }
    }

    pub fn nparams(&self) -> usize {
        // SAFETY: valid prepared statement.
        unsafe { d::duckdb_nparams(self.raw) as usize }
    }

    /// Name of parameter `i` (1-based): `"1"` for `?`/`$1`, `"a"` for `$a`.
    pub fn param_name(&self, i: usize) -> String {
        // SAFETY: valid statement; the string is freed by `owned`.
        owned(unsafe { d::duckdb_parameter_name(self.raw, i as u64) } as *mut c_char).unwrap_or_default()
    }

    pub fn clear_bindings(&mut self) {
        // SAFETY: valid statement.
        unsafe { d::duckdb_clear_bindings(self.raw) };
    }

    /// Bind parameter `i` (1-based).
    pub fn bind(&mut self, i: usize, p: &Param) -> Result<(), DuckError> {
        let v = OwnedValue::from_param(p);
        // SAFETY: valid statement and value; DuckDB copies the value.
        if unsafe { d::duckdb_bind_value(self.raw, i as u64, v.0) } != d::DuckDBSuccess {
            return Err(DuckError::new(crate::errors::INVALID_INPUT, format!("Invalid Input Error: could not bind parameter {i}")));
        }
        Ok(())
    }

    /// Run the statement. `streaming`: rows are produced as they are
    /// fetched (SELECT and the like); otherwise the result is materialised
    /// before this returns (DML, DDL), so the statement has fully run.
    pub fn execute(&mut self, streaming: bool) -> Result<QueryResult, DuckError> {
        self.execute_once(streaming)
    }

    fn execute_once(&mut self, streaming: bool) -> Result<QueryResult, DuckError> {
        let mut res = RawResult::zeroed();
        // SAFETY: valid statement; the result is destroyed by RawResult.
        let state = unsafe {
            if streaming {
                #[allow(deprecated)]
                d::duckdb_execute_prepared_streaming(self.raw, &mut res.0)
            } else {
                d::duckdb_execute_prepared(self.raw, &mut res.0)
            }
        };
        if state != d::DuckDBSuccess {
            return Err(res.error());
        }
        Ok(QueryResult::new(res))
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        // SAFETY: owned handle.
        unsafe { d::duckdb_destroy_prepare(&mut self.raw) };
    }
}

// -- results -----------------------------------------------------------------------

struct RawResult(d::duckdb_result);

impl RawResult {
    fn zeroed() -> RawResult {
        // SAFETY: duckdb_result is a plain C struct for which all-zero is the
        // documented "empty" state that duckdb_destroy_result accepts.
        RawResult(unsafe { std::mem::zeroed() })
    }

    fn error(&mut self) -> DuckError {
        // SAFETY: a result filled in (possibly with an error) by DuckDB.
        let msg = borrowed(unsafe { d::duckdb_result_error(&mut self.0) });
        // SAFETY: as above.
        let kind = unsafe { d::duckdb_result_error_type(&mut self.0) };
        if kind == crate::errors::INVALID {
            DuckError::from_message(msg)
        } else {
            DuckError::new(kind, msg)
        }
    }
}

impl Drop for RawResult {
    fn drop(&mut self) {
        // SAFETY: owned result.
        unsafe { d::duckdb_destroy_result(&mut self.0) };
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReturnType {
    Rows,
    ChangedRows,
    Nothing,
}

pub struct QueryResult {
    res: RawResult,
    pub columns: Vec<(String, TyNode)>,
}

impl QueryResult {
    fn new(mut res: RawResult) -> QueryResult {
        // SAFETY: a successful result.
        let n = unsafe { d::duckdb_column_count(&mut res.0) };
        let mut columns = Vec::with_capacity(n as usize);
        for i in 0..n {
            // SAFETY: i < column count; the name is owned by the result.
            let name = borrowed(unsafe { d::duckdb_column_name(&mut res.0, i) });
            // SAFETY: i < column count; the type is destroyed by ty_of.
            let lt = unsafe { d::duckdb_column_logical_type(&mut res.0, i) };
            columns.push((name, take_type(lt)));
        }
        QueryResult { res, columns }
    }

    pub fn return_type(&self) -> ReturnType {
        // SAFETY: valid result (passed by value as the API takes it).
        match unsafe { d::duckdb_result_return_type(self.res.0) } {
            d::duckdb_result_type_DUCKDB_RESULT_TYPE_QUERY_RESULT => ReturnType::Rows,
            d::duckdb_result_type_DUCKDB_RESULT_TYPE_CHANGED_ROWS => ReturnType::ChangedRows,
            _ => ReturnType::Nothing,
        }
    }

    pub fn rows_changed(&mut self) -> u64 {
        // SAFETY: valid, materialised result.
        unsafe { d::duckdb_rows_changed(&mut self.res.0) }
    }

    /// The next chunk of rows, `None` at the end. A streaming result reports
    /// an error that happens while it runs (an interrupt) here.
    pub fn fetch(&mut self) -> Result<Option<Chunk>, DuckError> {
        // SAFETY: valid result; the chunk is destroyed by Chunk.
        let raw = unsafe { d::duckdb_fetch_chunk(self.res.0) };
        if raw.is_null() {
            // SAFETY: valid result.
            let err = unsafe { d::duckdb_result_error(&mut self.res.0) };
            if !err.is_null() {
                return Err(self.res.error());
            }
            return Ok(None);
        }
        Ok(Some(Chunk { raw }))
    }
}

pub struct Chunk {
    raw: d::duckdb_data_chunk,
}

impl Drop for Chunk {
    fn drop(&mut self) {
        // SAFETY: owned chunk.
        unsafe { d::duckdb_destroy_data_chunk(&mut self.raw) };
    }
}

/// How cells are converted: the session's time zone for TIMESTAMPTZ.
pub struct Ctx<'a> {
    pub tz: &'a str,
}

impl Chunk {
    pub fn len(&self) -> usize {
        // SAFETY: valid chunk.
        unsafe { d::duckdb_data_chunk_get_size(self.raw) as usize }
    }

    /// Rows `from..to` of this chunk, converted to wire values.
    pub fn rows(&self, columns: &[(String, TyNode)], from: usize, to: usize, ctx: &Ctx) -> Result<Vec<Value>, ConvError> {
        let readers: Vec<Reader> = columns
            .iter()
            .enumerate()
            // SAFETY: column index < the chunk's column count (the result's).
            .map(|(i, (_, node))| Reader::new(unsafe { d::duckdb_data_chunk_get_vector(self.raw, i as u64) }, node))
            .collect();
        // `protocol.wire_rows` marks values only in columns whose type name
        // can hold a map, struct or array; a TUPLE column is not one, so
        // its tuples travel as lists.
        let plain: Vec<bool> = columns
            .iter()
            .map(|(_, t)| matches!(&t.ty, Ty::Struct(_)) && !crate::types::is_nested_name(&t.name()))
            .collect();
        let mut out = Vec::with_capacity(to.saturating_sub(from));
        for row in from..to {
            let mut cells = Vec::with_capacity(readers.len());
            for (i, r) in readers.iter().enumerate() {
                let v = r.read(row, ctx)?;
                cells.push(if plain[i] { unmark_tuples(v) } else { v });
            }
            out.push(Value::List(cells));
        }
        Ok(out)
    }

}

// -- logical types -------------------------------------------------------------------

/// The type tree of a logical type, destroying the handle.
fn take_type(mut lt: d::duckdb_logical_type) -> TyNode {
    let node = type_of(lt);
    // SAFETY: owned logical type.
    unsafe { d::duckdb_destroy_logical_type(&mut lt) };
    node
}

fn width_of(id: u32) -> u8 {
    match id {
        d::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT | d::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => 2,
        d::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER | d::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => 4,
        d::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT | d::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => 8,
        d::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => 16,
        _ => 1,
    }
}

fn type_of(lt: d::duckdb_logical_type) -> TyNode {
    // SAFETY (every call in this function): `lt` is a valid logical type;
    // child types are destroyed by take_type and strings freed by owned.
    let id = unsafe { d::duckdb_get_type_id(lt) };
    let alias = owned(unsafe { d::duckdb_logical_type_get_alias(lt) });
    let ty = match id {
        d::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => Ty::Bool,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Ty::I8,
        d::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => Ty::I16,
        d::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => Ty::I32,
        d::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Ty::I64,
        d::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => Ty::U8,
        d::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => Ty::U16,
        d::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => Ty::U32,
        d::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => Ty::U64,
        d::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => Ty::I128,
        d::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => Ty::U128,
        d::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => Ty::F32,
        d::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Ty::F64,
        d::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => Ty::Varchar,
        d::DUCKDB_TYPE_DUCKDB_TYPE_BLOB => Ty::Blob,
        d::DUCKDB_TYPE_DUCKDB_TYPE_BIT => Ty::Bit,
        d::DUCKDB_TYPE_DUCKDB_TYPE_BIGNUM => Ty::Bignum,
        d::DUCKDB_TYPE_DUCKDB_TYPE_DATE => Ty::Date,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TIME => Ty::Time,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => Ty::TimeNs,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => Ty::TimeTz,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => Ty::Timestamp,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => Ty::TimestampS,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => Ty::TimestampMs,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => Ty::TimestampNs,
        d::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => Ty::TimestampTz,
        d::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => Ty::Interval,
        d::DUCKDB_TYPE_DUCKDB_TYPE_UUID => Ty::Uuid,
        d::DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => Ty::Null,
        d::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => Ty::Decimal {
            width: unsafe { d::duckdb_decimal_width(lt) },
            scale: unsafe { d::duckdb_decimal_scale(lt) },
            internal: width_of(unsafe { d::duckdb_decimal_internal_type(lt) }),
        },
        d::DUCKDB_TYPE_DUCKDB_TYPE_ENUM => {
            let n = unsafe { d::duckdb_enum_dictionary_size(lt) };
            let values = (0..n).map(|i| owned(unsafe { d::duckdb_enum_dictionary_value(lt, i as u64) }).unwrap_or_default()).collect();
            Ty::Enum { internal: width_of(unsafe { d::duckdb_enum_internal_type(lt) }), values }
        }
        d::DUCKDB_TYPE_DUCKDB_TYPE_LIST => Ty::List(Box::new(take_type(unsafe { d::duckdb_list_type_child_type(lt) }))),
        d::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
            let n = unsafe { d::duckdb_array_type_array_size(lt) };
            Ty::Array(Box::new(take_type(unsafe { d::duckdb_array_type_child_type(lt) })), n)
        }
        d::DUCKDB_TYPE_DUCKDB_TYPE_MAP => Ty::Map(
            Box::new(take_type(unsafe { d::duckdb_map_type_key_type(lt) })),
            Box::new(take_type(unsafe { d::duckdb_map_type_value_type(lt) })),
        ),
        d::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
            let n = unsafe { d::duckdb_struct_type_child_count(lt) };
            Ty::Struct(
                (0..n)
                    .map(|i| {
                        let name = owned(unsafe { d::duckdb_struct_type_child_name(lt, i) }).unwrap_or_default();
                        (name, take_type(unsafe { d::duckdb_struct_type_child_type(lt, i) }))
                    })
                    .collect(),
            )
        }
        d::DUCKDB_TYPE_DUCKDB_TYPE_UNION => {
            let n = unsafe { d::duckdb_union_type_member_count(lt) };
            Ty::Union(
                (0..n)
                    .map(|i| {
                        let name = owned(unsafe { d::duckdb_union_type_member_name(lt, i) }).unwrap_or_default();
                        (name, take_type(unsafe { d::duckdb_union_type_member_type(lt, i) }))
                    })
                    .collect(),
            )
        }
        other => Ty::Other(other),
    };
    TyNode { ty, alias }
}

// -- reading vectors -----------------------------------------------------------------

/// A vector and its children, ready to read cells from.
struct Reader<'a> {
    node: &'a TyNode,
    data: *const u8,
    validity: *const u64,
    kids: Vec<Reader<'a>>,
    /// Number of entries in a list child vector (bounds for list offsets).
    child_len: usize,
}

impl<'a> Reader<'a> {
    fn new(v: d::duckdb_vector, node: &'a TyNode) -> Reader<'a> {
        // SAFETY (every call here): `v` is a valid vector of type `node`
        // (DuckDB built both from the same result); children are fetched by
        // the accessor for that type.
        let data = unsafe { d::duckdb_vector_get_data(v) } as *const u8;
        let validity = unsafe { d::duckdb_vector_get_validity(v) } as *const u64;
        let mut child_len = 0;
        let kids = match &node.ty {
            Ty::List(c) => {
                child_len = unsafe { d::duckdb_list_vector_get_size(v) } as usize;
                vec![Reader::new(unsafe { d::duckdb_list_vector_get_child(v) }, c)]
            }
            Ty::Map(_, _) => {
                child_len = unsafe { d::duckdb_list_vector_get_size(v) } as usize;
                let entries = unsafe { d::duckdb_list_vector_get_child(v) };
                let (k, val) = match &node.ty {
                    Ty::Map(k, val) => (k.as_ref(), val.as_ref()),
                    _ => unreachable!(),
                };
                vec![
                    Reader::new(unsafe { d::duckdb_struct_vector_get_child(entries, 0) }, k),
                    Reader::new(unsafe { d::duckdb_struct_vector_get_child(entries, 1) }, val),
                ]
            }
            Ty::Array(c, _) => vec![Reader::new(unsafe { d::duckdb_array_vector_get_child(v) }, c)],
            Ty::Struct(fields) => fields
                .iter()
                .enumerate()
                .map(|(i, (_, t))| Reader::new(unsafe { d::duckdb_struct_vector_get_child(v, i as u64) }, t))
                .collect(),
            Ty::Union(members) => {
                // A union vector is a struct: the tag, then one child per member.
                static TAG: TyNode = TyNode { ty: Ty::U8, alias: None };
                let mut kids = vec![Reader::new(unsafe { d::duckdb_struct_vector_get_child(v, 0) }, &TAG)];
                for (i, (_, t)) in members.iter().enumerate() {
                    kids.push(Reader::new(unsafe { d::duckdb_struct_vector_get_child(v, i as u64 + 1) }, t));
                }
                kids
            }
            _ => vec![],
        };
        Reader { node, data, validity, kids, child_len }
    }

    fn valid(&self, row: usize) -> bool {
        if self.validity.is_null() {
            return true;
        }
        // SAFETY: the validity mask covers every row of the vector.
        unsafe { (*self.validity.add(row / 64) >> (row % 64)) & 1 == 1 }
    }

    /// The value at `row` as a `T` (the vector's storage type).
    fn at<T: Copy>(&self, row: usize) -> T {
        // SAFETY: `T` is the storage type for this vector's logical type and
        // row is within the vector.
        unsafe { ptr::read_unaligned((self.data as *const T).add(row)) }
    }

    fn bytes(&self, row: usize) -> &'a [u8] {
        let s: d::duckdb_string_t = self.at(row);
        // SAFETY: the union's length field is common to both layouts; short
        // strings (<= 12 bytes) are inlined, longer ones point to memory owned
        // by the vector, which outlives this read.
        unsafe {
            let len = s.value.inlined.length as usize;
            if len <= 12 {
                let p = (self.data as *const d::duckdb_string_t).add(row) as *const u8;
                std::slice::from_raw_parts(p.add(4), len)
            } else {
                std::slice::from_raw_parts(s.value.pointer.ptr as *const u8, len)
            }
        }
    }

    fn read(&self, row: usize, ctx: &Ctx) -> Result<Value, ConvError> {
        if !self.valid(row) {
            return Ok(Value::Null);
        }
        Ok(match &self.node.ty {
            Ty::Null => Value::Null,
            Ty::Bool => Value::Bool(self.at::<u8>(row) != 0),
            Ty::I8 => Value::Int(self.at::<i8>(row) as i128),
            Ty::I16 => Value::Int(self.at::<i16>(row) as i128),
            Ty::I32 => Value::Int(self.at::<i32>(row) as i128),
            Ty::I64 => Value::Int(self.at::<i64>(row) as i128),
            Ty::U8 => Value::Int(self.at::<u8>(row) as i128),
            Ty::U16 => Value::Int(self.at::<u16>(row) as i128),
            Ty::U32 => Value::Int(self.at::<u32>(row) as i128),
            Ty::U64 => Value::Int(self.at::<u64>(row) as i128),
            Ty::I128 => {
                let h: d::duckdb_hugeint = self.at(row);
                Value::Int(((h.upper as i128) << 64) | h.lower as i128)
            }
            Ty::U128 => {
                let h: d::duckdb_uhugeint = self.at(row);
                let v = ((h.upper as u128) << 64) | h.lower as u128;
                match i128::try_from(v) {
                    Ok(i) => Value::Int(i),
                    Err(_) => Value::BigInt(v.to_string()),
                }
            }
            Ty::F32 => Value::Float(self.at::<f32>(row) as f64),
            Ty::F64 => Value::Float(self.at::<f64>(row)),
            Ty::Varchar => Value::Str(String::from_utf8_lossy(self.bytes(row)).into_owned()),
            Ty::Blob => Value::Bytes(self.bytes(row).to_vec()),
            Ty::Bit => Value::Str(convert::bit_string(self.bytes(row))),
            Ty::Bignum => Value::Str(convert::bignum_string(self.bytes(row))),
            Ty::Date => convert::date(self.at::<i32>(row))?,
            Ty::Time => convert::time(self.at::<i64>(row)),
            Ty::TimeNs => convert::time(self.at::<i64>(row) / 1000),
            Ty::TimeTz => {
                let bits: u64 = self.at(row);
                // SAFETY: pure function on the stored bits.
                let t = unsafe { d::duckdb_from_time_tz(d::duckdb_time_tz { bits }) };
                let micros = ((t.time.hour as i64 * 60 + t.time.min as i64) * 60 + t.time.sec as i64) * 1_000_000
                    + t.time.micros as i64;
                convert::time_tz(micros, t.offset)
            }
            Ty::Timestamp => convert::timestamp(self.at::<i64>(row))?,
            Ty::TimestampS => convert::timestamp(scale_ts(self.at::<i64>(row), 1_000_000, true))?,
            Ty::TimestampMs => convert::timestamp(scale_ts(self.at::<i64>(row), 1000, true))?,
            Ty::TimestampNs => convert::timestamp(scale_ts(self.at::<i64>(row), 1000, false))?,
            Ty::TimestampTz => convert::timestamp_tz(self.at::<i64>(row), ctx.tz)?,
            Ty::Interval => {
                let iv: d::duckdb_interval = self.at(row);
                convert::interval(iv.months, iv.days, iv.micros)?
            }
            Ty::Uuid => {
                let h: d::duckdb_hugeint = self.at(row);
                convert::uuid(h.upper as u64, h.lower)
            }
            Ty::Decimal { scale, internal, .. } => {
                let unscaled = match internal {
                    2 => self.at::<i16>(row) as i128,
                    4 => self.at::<i32>(row) as i128,
                    8 => self.at::<i64>(row) as i128,
                    _ => {
                        let h: d::duckdb_hugeint = self.at(row);
                        ((h.upper as i128) << 64) | h.lower as i128
                    }
                };
                convert::decimal(unscaled, *scale)
            }
            Ty::Enum { internal, values } => {
                let idx = match internal {
                    1 => self.at::<u8>(row) as usize,
                    2 => self.at::<u16>(row) as usize,
                    _ => self.at::<u32>(row) as usize,
                };
                Value::Str(values.get(idx).cloned().unwrap_or_default())
            }
            Ty::List(_) => {
                let e: d::duckdb_list_entry = self.at(row);
                let (off, len) = (e.offset as usize, e.length as usize);
                let end = off.saturating_add(len).min(self.child_len);
                let mut items = Vec::with_capacity(end.saturating_sub(off));
                for i in off..end {
                    items.push(self.kids[0].read(i, ctx)?);
                }
                Value::List(items)
            }
            Ty::Map(_, _) => {
                let e: d::duckdb_list_entry = self.at(row);
                let (off, len) = (e.offset as usize, e.length as usize);
                let end = off.saturating_add(len).min(self.child_len);
                let mut pairs = Vec::with_capacity(end.saturating_sub(off));
                for i in off..end {
                    pairs.push((self.kids[0].read(i, ctx)?, self.kids[1].read(i, ctx)?));
                }
                convert::map_value(pairs)
            }
            Ty::Array(_, n) => {
                let n = *n as usize;
                let mut items = Vec::with_capacity(n);
                for i in row * n..row * n + n {
                    items.push(self.kids[0].read(i, ctx)?);
                }
                Value::Ext(EXT_TUPLE, Box::new(Value::List(items)))
            }
            Ty::Struct(fields) if !fields.is_empty() && fields.iter().all(|(n, _)| n.is_empty()) => {
                // An unnamed struct: a tuple in the Python client.
                let mut items = Vec::with_capacity(fields.len());
                for k in &self.kids {
                    items.push(k.read(row, ctx)?);
                }
                Value::Ext(EXT_TUPLE, Box::new(Value::List(items)))
            }
            Ty::Struct(fields) => {
                let mut pairs = Vec::with_capacity(fields.len());
                for (i, (name, _)) in fields.iter().enumerate() {
                    pairs.push((Value::Str(name.clone()), self.kids[i].read(row, ctx)?));
                }
                convert::map_value(pairs)
            }
            Ty::Union(_) => {
                let tag = self.kids[0].at::<u8>(row) as usize;
                match self.kids.get(tag + 1) {
                    Some(k) => k.read(row, ctx)?,
                    None => Value::Null,
                }
            }
            Ty::Other(id) => return Err(ConvError(format!("DuckDB type {id} is not supported by the Rust worker"))),
        })
    }
}

/// Tuples as plain lists, all the way down.
fn unmark_tuples(v: Value) -> Value {
    match v {
        Value::Ext(EXT_TUPLE, inner) => match *inner {
            Value::List(items) => Value::List(items.into_iter().map(unmark_tuples).collect()),
            other => other,
        },
        other => other,
    }
}

/// TIMESTAMP_S/MS (multiply) and TIMESTAMP_NS (truncating divide) to
/// microseconds, keeping the infinities.
fn scale_ts(v: i64, factor: i64, multiply: bool) -> i64 {
    if v == convert::TS_INF || v == convert::TS_NINF {
        return v;
    }
    if multiply { v.saturating_mul(factor) } else { v / factor }
}

// -- values for binding and appending ------------------------------------------------

struct OwnedValue(d::duckdb_value);

impl Drop for OwnedValue {
    fn drop(&mut self) {
        // SAFETY: owned value.
        unsafe { d::duckdb_destroy_value(&mut self.0) };
    }
}

struct OwnedType(d::duckdb_logical_type);

impl Drop for OwnedType {
    fn drop(&mut self) {
        // SAFETY: owned type.
        unsafe { d::duckdb_destroy_logical_type(&mut self.0) };
    }
}

fn hugeint(v: i128) -> d::duckdb_hugeint {
    d::duckdb_hugeint { lower: v as u64, upper: (v >> 64) as i64 }
}

impl OwnedType {
    fn of(t: &PTy) -> OwnedType {
        // SAFETY (every call here): creating logical types from valid inputs;
        // child types are owned by OwnedType and DuckDB copies them.
        let simple = |id: u32| OwnedType(unsafe { d::duckdb_create_logical_type(id) });
        match t {
            PTy::Null => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL),
            PTy::Bool => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN),
            PTy::I32 => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER),
            PTy::I64 => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT),
            PTy::U64 => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT),
            PTy::I128 => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT),
            PTy::U128 => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT),
            PTy::F32 => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT),
            PTy::F64 => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE),
            PTy::Varchar => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR),
            PTy::Blob => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_BLOB),
            PTy::Decimal(w, s) => OwnedType(unsafe { d::duckdb_create_decimal_type(*w, *s) }),
            PTy::Timestamp => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP),
            PTy::TimestampTz => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ),
            PTy::Date => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_DATE),
            PTy::Time => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_TIME),
            PTy::TimeTz => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ),
            PTy::Interval => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL),
            PTy::Uuid => simple(d::DUCKDB_TYPE_DUCKDB_TYPE_UUID),
            PTy::List(c) => {
                let child = OwnedType::of(c);
                OwnedType(unsafe { d::duckdb_create_list_type(child.0) })
            }
            PTy::Map(k, v) => {
                let (k, v) = (OwnedType::of(k), OwnedType::of(v));
                OwnedType(unsafe { d::duckdb_create_map_type(k.0, v.0) })
            }
            PTy::Struct(fields) => {
                let types: Vec<OwnedType> = fields.iter().map(|(_, t)| OwnedType::of(t)).collect();
                let names: Vec<CString> =
                    fields.iter().map(|(n, _)| CString::new(n.replace('\0', "")).expect("no NUL")).collect();
                let mut raw_types: Vec<d::duckdb_logical_type> = types.iter().map(|t| t.0).collect();
                let raw_names: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
                OwnedType(unsafe {
                    d::duckdb_create_struct_type(raw_types.as_mut_ptr(), raw_names.as_ptr() as *mut *const c_char, fields.len() as u64)
                })
            }
        }
    }
}

impl OwnedValue {
    fn from_param(p: &Param) -> OwnedValue {
        // SAFETY (every call here): DuckDB value constructors copy their
        // inputs; child values and types are owned wrappers dropped after
        // the parent value is created (DuckDB copies them).
        OwnedValue(unsafe {
            match p {
                Param::Null => d::duckdb_create_null_value(),
                Param::Bool(b) => d::duckdb_create_bool(*b),
                Param::I32(i) => d::duckdb_create_int32(*i),
                Param::I64(i) => d::duckdb_create_int64(*i),
                Param::U64(i) => d::duckdb_create_uint64(*i),
                Param::I128(i) => d::duckdb_create_hugeint(hugeint(*i)),
                Param::U128(u) => d::duckdb_create_uhugeint(d::duckdb_uhugeint { lower: *u as u64, upper: (*u >> 64) as u64 }),
                Param::F32(f) => d::duckdb_create_float(*f),
                Param::F64(f) => d::duckdb_create_double(*f),
                Param::Str(s) => d::duckdb_create_varchar_length(s.as_ptr() as *const c_char, s.len() as u64),
                Param::Blob(b) => d::duckdb_create_blob(b.as_ptr(), b.len() as u64),
                Param::Decimal { width, scale, value } => {
                    d::duckdb_create_decimal(d::duckdb_decimal { width: *width, scale: *scale, value: hugeint(*value) })
                }
                Param::Timestamp(m) => d::duckdb_create_timestamp(d::duckdb_timestamp { micros: *m }),
                Param::TimestampTz(m) => d::duckdb_create_timestamp_tz(d::duckdb_timestamp { micros: *m }),
                Param::Date(days) => d::duckdb_create_date(d::duckdb_date { days: *days }),
                Param::Time(m) => d::duckdb_create_time(d::duckdb_time { micros: *m }),
                Param::TimeTz(m, off) => d::duckdb_create_time_tz_value(d::duckdb_create_time_tz(*m, *off)),
                Param::Interval { months, days, micros } => {
                    d::duckdb_create_interval(d::duckdb_interval { months: *months, days: *days, micros: *micros })
                }
                Param::Uuid(u) => d::duckdb_create_uuid(d::duckdb_uhugeint { lower: *u as u64, upper: (*u >> 64) as u64 }),
                Param::List(t, items) => {
                    let ty = OwnedType::of(t);
                    let vals: Vec<OwnedValue> = items.iter().map(OwnedValue::from_param).collect();
                    let mut raw: Vec<d::duckdb_value> = vals.iter().map(|v| v.0).collect();
                    d::duckdb_create_list_value(ty.0, raw.as_mut_ptr(), raw.len() as u64)
                }
                Param::Struct(fields) => {
                    let ty = OwnedType::of(&p.ty());
                    let vals: Vec<OwnedValue> = fields.iter().map(|(_, v)| OwnedValue::from_param(v)).collect();
                    let mut raw: Vec<d::duckdb_value> = vals.iter().map(|v| v.0).collect();
                    d::duckdb_create_struct_value(ty.0, raw.as_mut_ptr())
                }
                Param::Map(..) => {
                    let ty = OwnedType::of(&p.ty());
                    let Param::Map(_, _, pairs) = p else { unreachable!() };
                    let keys: Vec<OwnedValue> = pairs.iter().map(|(k, _)| OwnedValue::from_param(k)).collect();
                    let vals: Vec<OwnedValue> = pairs.iter().map(|(_, v)| OwnedValue::from_param(v)).collect();
                    let mut rk: Vec<d::duckdb_value> = keys.iter().map(|v| v.0).collect();
                    let mut rv: Vec<d::duckdb_value> = vals.iter().map(|v| v.0).collect();
                    d::duckdb_create_map_value(ty.0, rk.as_mut_ptr(), rv.as_mut_ptr(), pairs.len() as u64)
                }
            }
        })
    }
}

// -- appender --------------------------------------------------------------------------

pub struct Appender {
    raw: d::duckdb_appender,
}

impl Appender {
    fn error(&self) -> DuckError {
        // SAFETY: valid (possibly failed) appender; the error data is destroyed below.
        let mut e = unsafe { d::duckdb_appender_error_data(self.raw) };
        if e.is_null() {
            return DuckError::from_message("Invalid Input Error: appender failed");
        }
        // SAFETY: valid error data.
        let kind = unsafe { d::duckdb_error_data_error_type(e) };
        let msg = borrowed(unsafe { d::duckdb_error_data_message(e) });
        // SAFETY: owned error data.
        unsafe { d::duckdb_destroy_error_data(&mut e) };
        if kind == crate::errors::INVALID { DuckError::from_message(msg) } else { DuckError::new(kind, msg) }
    }

    /// Append one row.
    pub fn row(&mut self, values: &[Param]) -> Result<(), DuckError> {
        for p in values {
            let v = OwnedValue::from_param(p);
            // SAFETY: valid appender and value (copied by DuckDB).
            if unsafe { d::duckdb_append_value(self.raw, v.0) } != d::DuckDBSuccess {
                return Err(self.error());
            }
        }
        // SAFETY: valid appender.
        if unsafe { d::duckdb_appender_end_row(self.raw) } != d::DuckDBSuccess {
            return Err(self.error());
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), DuckError> {
        // SAFETY: valid appender.
        if unsafe { d::duckdb_appender_flush(self.raw) } != d::DuckDBSuccess {
            return Err(self.error());
        }
        Ok(())
    }

    /// Drop buffered rows, so nothing is flushed later.
    pub fn clear(&mut self) {
        // SAFETY: valid appender.
        unsafe { d::duckdb_appender_clear(self.raw) };
    }
}

impl Drop for Appender {
    fn drop(&mut self) {
        // SAFETY: owned appender. Rows were flushed or cleared by the caller;
        // destroy flushes anything left, which is then nothing.
        unsafe { d::duckdb_appender_destroy(&mut self.raw) };
    }
}

// -- process -------------------------------------------------------------------------

#[repr(C)]
struct RLimit {
    cur: u64,
    max: u64,
}

unsafe extern "C" {
    fn setrlimit(resource: i32, rlim: *const RLimit) -> i32;
    fn getrlimit(resource: i32, rlim: *mut RLimit) -> i32;
    fn dup(fd: i32) -> i32;
    fn dup2(fd: i32, fd2: i32) -> i32;
}

/// Linux only: cap the address space at `current_bytes + limit_bytes`, as
/// the Python worker does. Returns true if applied.
pub fn set_rlimit_as(total_bytes: u64) -> bool {
    if !cfg!(target_os = "linux") {
        return false;
    }
    const RLIMIT_AS: i32 = 9;
    let mut cur = RLimit { cur: 0, max: 0 };
    // SAFETY: out-pointer to a local.
    if unsafe { getrlimit(RLIMIT_AS, &mut cur) } != 0 {
        return false;
    }
    let limit = if cur.max == u64::MAX { total_bytes } else { total_bytes.min(cur.max) };
    let new = RLimit { cur: limit, max: cur.max };
    // SAFETY: pointer to a local.
    unsafe { setrlimit(RLIMIT_AS, &new) == 0 }
}

/// Move the protocol's output off fd 1: returns a duplicate of stdout for
/// replies and points fd 1 at stderr, so nothing else (a DuckDB message,
/// say) can write into the protocol stream.
pub fn private_stdout() -> Option<i32> {
    // SAFETY: plain descriptor calls on the process's own fds.
    unsafe {
        let fd = dup(1);
        if fd < 0 {
            return None;
        }
        if dup2(2, 1) < 0 {
            return None;
        }
        Some(fd)
    }
}

/// A `File` for a descriptor this process owns.
pub fn file_from_fd(fd: i32) -> std::fs::File {
    use std::os::fd::FromRawFd;
    // SAFETY: `fd` is a descriptor we own (from private_stdout) and nothing
    // else closes it.
    unsafe { std::fs::File::from_raw_fd(fd) }
}
