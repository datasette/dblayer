//! The only module with `unsafe` code: hand-written bindings to the SQLite C
//! API (the amalgamation build.rs compiles in) and to the few POSIX calls
//! the worker needs, wrapped in safe types.
//!
//! Invariants the wrappers keep, so that the rest of the crate cannot break
//! them:
//!
//! * `Db` and `Stmt` hold raw pointers and are therefore neither `Send` nor
//!   `Sync`: a connection, and its statements, stay on the thread that
//!   opened it. That is the rule `SQLITE_CONFIG_MULTITHREAD` relies on (no
//!   per-connection mutexes), enforced by the compiler.
//! * The one thing another thread may do to a connection is
//!   `sqlite3_interrupt`. It goes through an `Interrupter`, a shared cell
//!   that `Db`'s destructor empties (under its mutex) before it closes the
//!   connection: an interrupt can never reach a closed connection.
//! * Column text and blobs are returned as slices borrowed from the `Stmt`;
//!   `step` and `reset` take `&mut self`, so no slice survives the step that
//!   would invalidate it.
//! * A progress handler's context is owned by a guard that removes the
//!   handler when dropped.
//! * Statements are closed by `Drop` (`sqlite3_finalize`); a connection by
//!   `sqlite3_close_v2`, which defers the close while statements remain.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::sync::{Arc, Mutex};

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3 {
    _p: [u8; 0],
}
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct sqlite3_stmt {
    _p: [u8; 0],
}

type Destructor = isize; // SQLITE_STATIC (0) or SQLITE_TRANSIENT (-1)
const SQLITE_TRANSIENT: Destructor = -1;

unsafe extern "C" {
    fn sqlite3_libversion() -> *const c_char;
    fn sqlite3_config(op: c_int, ...) -> c_int;
    fn sqlite3_initialize() -> c_int;
    fn sqlite3_open_v2(filename: *const c_char, db: *mut *mut sqlite3, flags: c_int, vfs: *const c_char) -> c_int;
    fn sqlite3_close_v2(db: *mut sqlite3) -> c_int;
    fn sqlite3_busy_timeout(db: *mut sqlite3, ms: c_int) -> c_int;
    fn sqlite3_busy_handler(
        db: *mut sqlite3,
        cb: Option<unsafe extern "C" fn(*mut c_void, c_int) -> c_int>,
        arg: *mut c_void,
    ) -> c_int;
    fn sqlite3_db_config(db: *mut sqlite3, op: c_int, ...) -> c_int;
    fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        cb: *const c_void,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    fn sqlite3_limit(db: *mut sqlite3, id: c_int, new_val: c_int) -> c_int;
    fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        sql: *const c_char,
        n: c_int,
        stmt: *mut *mut sqlite3_stmt,
        tail: *mut *const c_char,
    ) -> c_int;
    fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_stmt_readonly(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, i: c_int) -> c_int;
    fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, i: c_int, v: i64) -> c_int;
    fn sqlite3_bind_double(stmt: *mut sqlite3_stmt, i: c_int, v: f64) -> c_int;
    fn sqlite3_bind_text64(stmt: *mut sqlite3_stmt, i: c_int, p: *const c_char, n: u64, d: Destructor, enc: u8)
    -> c_int;
    fn sqlite3_bind_blob64(stmt: *mut sqlite3_stmt, i: c_int, p: *const c_void, n: u64, d: Destructor) -> c_int;
    fn sqlite3_bind_zeroblob(stmt: *mut sqlite3_stmt, i: c_int, n: c_int) -> c_int;
    fn sqlite3_bind_parameter_count(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_bind_parameter_name(stmt: *mut sqlite3_stmt, i: c_int) -> *const c_char;
    fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_column_name(stmt: *mut sqlite3_stmt, i: c_int) -> *const c_char;
    fn sqlite3_column_type(stmt: *mut sqlite3_stmt, i: c_int) -> c_int;
    fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, i: c_int) -> i64;
    fn sqlite3_column_double(stmt: *mut sqlite3_stmt, i: c_int) -> f64;
    fn sqlite3_column_text(stmt: *mut sqlite3_stmt, i: c_int) -> *const u8;
    fn sqlite3_column_blob(stmt: *mut sqlite3_stmt, i: c_int) -> *const c_void;
    fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, i: c_int) -> c_int;
    fn sqlite3_changes64(db: *mut sqlite3) -> i64;
    fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> i64;
    fn sqlite3_set_last_insert_rowid(db: *mut sqlite3, v: i64);
    fn sqlite3_get_autocommit(db: *mut sqlite3) -> c_int;
    fn sqlite3_extended_errcode(db: *mut sqlite3) -> c_int;
    fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    fn sqlite3_errstr(rc: c_int) -> *const c_char;
    fn sqlite3_interrupt(db: *mut sqlite3);
    fn sqlite3_is_interrupted(db: *mut sqlite3) -> c_int;
    fn sqlite3_progress_handler(
        db: *mut sqlite3,
        n: c_int,
        cb: Option<unsafe extern "C" fn(*mut c_void) -> c_int>,
        arg: *mut c_void,
    );
    fn sqlite3_sleep(ms: c_int) -> c_int;
    fn sqlite3_soft_heap_limit64(n: i64) -> i64;
    fn sqlite3_hard_heap_limit64(n: i64) -> i64;
    fn sqlite3_enable_load_extension(db: *mut sqlite3, onoff: c_int) -> c_int;
    fn sqlite3_load_extension(
        db: *mut sqlite3,
        file: *const c_char,
        proc_: *const c_char,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    fn sqlite3_free(p: *mut c_void);

    // POSIX
    fn dup(fd: c_int) -> c_int;
    fn dup2(fd: c_int, fd2: c_int) -> c_int;
    fn signal(sig: c_int, handler: usize) -> usize;
    fn _exit(code: c_int) -> !;
    #[cfg(target_os = "linux")]
    fn getrlimit(resource: c_int, rl: *mut RLimit) -> c_int;
    #[cfg(target_os = "linux")]
    fn setrlimit(resource: c_int, rl: *const RLimit) -> c_int;
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct RLimit {
    cur: u64,
    max: u64,
}

pub const SQLITE_OK: i32 = 0;
pub const SQLITE_ERROR: i32 = 1;
pub const SQLITE_INTERNAL: i32 = 2;
pub const SQLITE_PERM: i32 = 3;
pub const SQLITE_ABORT: i32 = 4;
pub const SQLITE_BUSY: i32 = 5;
pub const SQLITE_LOCKED: i32 = 6;
pub const SQLITE_NOMEM: i32 = 7;
pub const SQLITE_READONLY: i32 = 8;
pub const SQLITE_INTERRUPT: i32 = 9;
pub const SQLITE_IOERR: i32 = 10;
pub const SQLITE_CORRUPT: i32 = 11;
pub const SQLITE_NOTFOUND: i32 = 12;
pub const SQLITE_FULL: i32 = 13;
pub const SQLITE_CANTOPEN: i32 = 14;
pub const SQLITE_PROTOCOL: i32 = 15;
pub const SQLITE_EMPTY: i32 = 16;
pub const SQLITE_SCHEMA: i32 = 17;
pub const SQLITE_TOOBIG: i32 = 18;
pub const SQLITE_CONSTRAINT: i32 = 19;
pub const SQLITE_MISMATCH: i32 = 20;
pub const SQLITE_MISUSE: i32 = 21;
pub const SQLITE_RANGE: i32 = 25;
pub const SQLITE_ROW: i32 = 100;
pub const SQLITE_DONE: i32 = 101;
pub const SQLITE_READONLY_ROLLBACK: i32 = SQLITE_READONLY | (3 << 8);

pub const SQLITE_INTEGER: i32 = 1;
pub const SQLITE_FLOAT: i32 = 2;
pub const SQLITE_TEXT: i32 = 3;
pub const SQLITE_BLOB: i32 = 4;

pub const SQLITE_OPEN_READWRITE: i32 = 0x2;
pub const SQLITE_OPEN_CREATE: i32 = 0x4;
pub const SQLITE_OPEN_URI: i32 = 0x40;

const SQLITE_CONFIG_MULTITHREAD: c_int = 2;
const SQLITE_CONFIG_MEMSTATUS: c_int = 9;
const SQLITE_DBCONFIG_LOOKASIDE: c_int = 1001;

pub const LIMIT_LENGTH: i32 = 0;
pub const LIMIT_SQL_LENGTH: i32 = 1;
pub const LIMIT_COLUMN: i32 = 2;
pub const LIMIT_EXPR_DEPTH: i32 = 3;
pub const LIMIT_COMPOUND_SELECT: i32 = 4;
pub const LIMIT_VDBE_OP: i32 = 5;
pub const LIMIT_FUNCTION_ARG: i32 = 6;
pub const LIMIT_ATTACHED: i32 = 7;
pub const LIMIT_LIKE_PATTERN_LENGTH: i32 = 8;
pub const LIMIT_VARIABLE_NUMBER: i32 = 9;
pub const LIMIT_TRIGGER_DEPTH: i32 = 10;
pub const LIMIT_WORKER_THREADS: i32 = 11;

fn cstr_bytes<'a>(p: *const c_char) -> &'a [u8] {
    if p.is_null() {
        return b"";
    }
    // SAFETY: SQLite returns NUL-terminated strings that live at least until
    // the next call on the same object; callers copy or tie the lifetime.
    unsafe { CStr::from_ptr(p) }.to_bytes()
}

pub fn libversion() -> String {
    String::from_utf8_lossy(cstr_bytes(unsafe { sqlite3_libversion() })).into_owned()
}

pub fn errstr(rc: i32) -> String {
    String::from_utf8_lossy(cstr_bytes(unsafe { sqlite3_errstr(rc) })).into_owned()
}

/// Process set-up before any connection: threading mode, memory statistics.
pub fn configure(multi_thread: bool, memstatus: bool) -> bool {
    // SAFETY: called once from main before sqlite3_initialize and before any
    // other thread uses SQLite, as sqlite3_config requires.
    unsafe {
        if multi_thread && sqlite3_config(SQLITE_CONFIG_MULTITHREAD) != SQLITE_OK {
            return false;
        }
        sqlite3_config(SQLITE_CONFIG_MEMSTATUS, memstatus as c_int);
        sqlite3_initialize() == SQLITE_OK
    }
}

pub fn hard_heap_limit(bytes: i64) {
    unsafe { sqlite3_hard_heap_limit64(bytes) };
}

pub fn soft_heap_limit(bytes: i64) {
    unsafe { sqlite3_soft_heap_limit64(bytes) };
}

// -- interrupts -------------------------------------------------------------

struct RawDb(*mut sqlite3);
// SAFETY: the pointer is only used for sqlite3_interrupt (documented safe
// from any thread) while the Mutex holds it non-null; Db::drop nulls it
// under the same mutex before closing the connection.
unsafe impl Send for RawDb {}

/// A handle another thread may use to interrupt a connection, valid for as
/// long as anyone holds it: once the connection is closed it does nothing.
#[derive(Clone)]
pub struct Interrupter(Arc<Mutex<RawDb>>);

impl Interrupter {
    pub fn interrupt(&self) {
        let g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if !g.0.is_null() {
            // SAFETY: non-null means the connection is open (see RawDb).
            unsafe { sqlite3_interrupt(g.0) };
        }
    }
}

// -- busy handler -----------------------------------------------------------

struct BusyCtx {
    db: *mut sqlite3,
    busy_ms: i32,
}

/// busy_timeout that a cancel can stop: SQLite's default back-off
/// schedule, slept in slices of at most 5 ms, giving up as soon as the
/// connection is interrupted (SQLite's own busy handler sleeps through
/// sqlite3_interrupt). Same as the C worker's busy_cb.
unsafe extern "C" fn busy_cb(p: *mut c_void, count: c_int) -> c_int {
    // SAFETY: p is the Box<BusyCtx> owned by the Db that registered it.
    let c = unsafe { &*(p as *const BusyCtx) };
    const DELAYS: [i32; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];
    const TOTALS: [i32; 12] = [0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228];
    let n = DELAYS.len() as i32;
    let mut delay = if count < n { DELAYS[count as usize] } else { DELAYS[11] };
    let prior = if count < n { TOTALS[count as usize] } else { TOTALS[11] + DELAYS[11] * (count - (n - 1)) };
    if prior + delay > c.busy_ms {
        delay = c.busy_ms - prior;
        if delay <= 0 {
            return 0;
        }
    }
    while delay > 0 {
        if unsafe { sqlite3_is_interrupted(c.db) } != 0 {
            return 0;
        }
        let slice = delay.min(5);
        unsafe { sqlite3_sleep(slice) };
        delay -= slice;
    }
    if unsafe { sqlite3_is_interrupted(c.db) } != 0 {
        return 0;
    }
    1
}

// -- connections ------------------------------------------------------------

/// An open SQLite connection. Not `Send`: it stays on its thread.
pub struct Db {
    raw: *mut sqlite3,
    shared: Interrupter,
    busy: Option<Box<BusyCtx>>,
}

/// A failed sqlite3_open_v2: the result code and SQLite's message.
pub struct OpenError {
    pub rc: i32,
    pub ext: i32,
    pub msg: String,
}

impl Db {
    pub fn open(uri: &str, flags: i32) -> Result<Db, OpenError> {
        let c = CString::new(uri).map_err(|_| OpenError { rc: SQLITE_CANTOPEN, ext: SQLITE_CANTOPEN, msg: "path contains a NUL".into() })?;
        let mut raw = std::ptr::null_mut();
        // SAFETY: valid NUL-terminated string and out-pointer.
        let rc = unsafe { sqlite3_open_v2(c.as_ptr(), &mut raw, flags, std::ptr::null()) };
        if rc != SQLITE_OK {
            let (ext, msg) = if raw.is_null() {
                (rc, errstr(rc))
            } else {
                // SAFETY: raw is a (failed) connection handle, closed below.
                unsafe { (sqlite3_extended_errcode(raw), String::from_utf8_lossy(cstr_bytes(sqlite3_errmsg(raw))).into_owned()) }
            };
            unsafe { sqlite3_close_v2(raw) };
            return Err(OpenError { rc, ext, msg });
        }
        Ok(Db { raw, shared: Interrupter(Arc::new(Mutex::new(RawDb(raw)))), busy: None })
    }

    pub fn interrupter(&self) -> Interrupter {
        self.shared.clone()
    }

    pub fn busy_timeout(&self, ms: i32) {
        unsafe { sqlite3_busy_timeout(self.raw, ms) };
    }

    /// Replace the busy timeout with the interruptible handler.
    pub fn set_busy_handler(&mut self, busy_ms: i32) {
        let ctx = Box::new(BusyCtx { db: self.raw, busy_ms });
        let p = &*ctx as *const BusyCtx as *mut c_void;
        // SAFETY: ctx lives in self.busy until Db is dropped, after the
        // connection (and so its handler) is gone.
        unsafe { sqlite3_busy_handler(self.raw, Some(busy_cb), p) };
        self.busy = Some(ctx);
    }

    pub fn lookaside(&self, size: i32, count: i32) {
        unsafe { sqlite3_db_config(self.raw, SQLITE_DBCONFIG_LOOKASIDE, std::ptr::null_mut::<c_void>(), size as c_int, count as c_int) };
    }

    /// sqlite3_exec with no callback; the result code.
    pub fn exec(&self, sql: &str) -> i32 {
        let Ok(c) = CString::new(sql) else { return SQLITE_ERROR };
        unsafe { sqlite3_exec(self.raw, c.as_ptr(), std::ptr::null(), std::ptr::null_mut(), std::ptr::null_mut()) }
    }

    pub fn limit(&self, id: i32, value: i32) -> i32 {
        unsafe { sqlite3_limit(self.raw, id, value) }
    }

    /// Compile the first statement of `sql`. Ok((None, _)) for an empty
    /// statement; the second value is where the tail starts.
    pub fn prepare(&self, sql: &[u8]) -> Result<(Option<Stmt>, usize), i32> {
        if sql.len() > i32::MAX as usize {
            return Err(SQLITE_TOOBIG);
        }
        let mut st = std::ptr::null_mut();
        let mut tail: *const c_char = std::ptr::null();
        // SAFETY: (ptr, len) describe sql, which outlives the call; tail
        // points into sql or is null.
        let rc = unsafe { sqlite3_prepare_v2(self.raw, sql.as_ptr() as *const c_char, sql.len() as c_int, &mut st, &mut tail) };
        let stmt = if st.is_null() { None } else { Some(Stmt { raw: st }) };
        if rc != SQLITE_OK {
            return Err(rc);
        }
        let off = if tail.is_null() { sql.len() } else { (tail as usize).saturating_sub(sql.as_ptr() as usize).min(sql.len()) };
        Ok((stmt, off))
    }

    pub fn changes(&self) -> i64 {
        unsafe { sqlite3_changes64(self.raw) }
    }
    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { sqlite3_last_insert_rowid(self.raw) }
    }
    pub fn set_last_insert_rowid(&self, v: i64) {
        unsafe { sqlite3_set_last_insert_rowid(self.raw, v) }
    }
    pub fn autocommit(&self) -> bool {
        unsafe { sqlite3_get_autocommit(self.raw) != 0 }
    }
    pub fn extended_errcode(&self) -> i32 {
        unsafe { sqlite3_extended_errcode(self.raw) }
    }
    pub fn errmsg(&self) -> String {
        String::from_utf8_lossy(cstr_bytes(unsafe { sqlite3_errmsg(self.raw) })).into_owned()
    }

    /// Install `check` as the progress handler (every `n` VM steps; a true
    /// return interrupts) until the returned guard is dropped.
    pub fn progress<'a>(&'a self, n: i32, check: &'a dyn Fn() -> bool) -> ProgressGuard<'a> {
        let boxed: Box<&'a dyn Fn() -> bool> = Box::new(check);
        let p = &*boxed as *const &dyn Fn() -> bool as *mut c_void;
        // SAFETY: the closure reference lives in the guard, which removes
        // the handler before it is dropped.
        unsafe { sqlite3_progress_handler(self.raw, n, Some(progress_cb), p) };
        ProgressGuard { db: self, _ctx: boxed }
    }

    pub fn load_extension(&self, path: &str) -> Result<(), String> {
        let c = CString::new(path).map_err(|_| "path contains a NUL".to_string())?;
        let mut msg: *mut c_char = std::ptr::null_mut();
        // SAFETY: valid arguments; msg is freed with sqlite3_free.
        unsafe {
            sqlite3_enable_load_extension(self.raw, 1);
            let rc = sqlite3_load_extension(self.raw, c.as_ptr(), std::ptr::null(), &mut msg);
            sqlite3_enable_load_extension(self.raw, 0);
            if rc != SQLITE_OK {
                let m = if msg.is_null() { "error".to_string() } else { String::from_utf8_lossy(cstr_bytes(msg)).into_owned() };
                sqlite3_free(msg as *mut c_void);
                return Err(m);
            }
        }
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let mut g = self.shared.0.lock().unwrap_or_else(|e| e.into_inner());
        g.0 = std::ptr::null_mut();
        // SAFETY: no interrupt can reach the connection now; close_v2 defers
        // if statements are still open (they are dropped elsewhere).
        unsafe { sqlite3_close_v2(self.raw) };
        drop(g);
    }
}

unsafe extern "C" fn progress_cb(p: *mut c_void) -> c_int {
    // SAFETY: p points to the &dyn Fn held by a live ProgressGuard.
    let f = unsafe { &*(p as *const &dyn Fn() -> bool) };
    f() as c_int
}

pub struct ProgressGuard<'a> {
    db: &'a Db,
    _ctx: Box<&'a dyn Fn() -> bool>,
}

impl Drop for ProgressGuard<'_> {
    fn drop(&mut self) {
        unsafe { sqlite3_progress_handler(self.db.raw, 0, None, std::ptr::null_mut()) };
    }
}

// -- statements -------------------------------------------------------------

pub struct Stmt {
    raw: *mut sqlite3_stmt,
}

impl Drop for Stmt {
    fn drop(&mut self) {
        unsafe { sqlite3_finalize(self.raw) };
    }
}

impl Stmt {
    pub fn step(&mut self) -> i32 {
        unsafe { sqlite3_step(self.raw) }
    }
    pub fn reset(&mut self) {
        unsafe { sqlite3_reset(self.raw) };
    }
    pub fn readonly(&self) -> bool {
        unsafe { sqlite3_stmt_readonly(self.raw) != 0 }
    }
    pub fn bind_null(&mut self, i: i32) -> i32 {
        unsafe { sqlite3_bind_null(self.raw, i) }
    }
    pub fn bind_int(&mut self, i: i32, v: i64) -> i32 {
        unsafe { sqlite3_bind_int64(self.raw, i, v) }
    }
    pub fn bind_double(&mut self, i: i32, v: f64) -> i32 {
        unsafe { sqlite3_bind_double(self.raw, i, v) }
    }
    /// Text, copied by SQLite (SQLITE_TRANSIENT): the statement never
    /// points into Rust memory.
    pub fn bind_text(&mut self, i: i32, v: &[u8]) -> i32 {
        unsafe { sqlite3_bind_text64(self.raw, i, v.as_ptr() as *const c_char, v.len() as u64, SQLITE_TRANSIENT, 1) }
    }
    pub fn bind_blob(&mut self, i: i32, v: &[u8]) -> i32 {
        if v.is_empty() {
            return unsafe { sqlite3_bind_zeroblob(self.raw, i, 0) };
        }
        unsafe { sqlite3_bind_blob64(self.raw, i, v.as_ptr() as *const c_void, v.len() as u64, SQLITE_TRANSIENT) }
    }
    pub fn param_count(&self) -> i32 {
        unsafe { sqlite3_bind_parameter_count(self.raw) }
    }
    pub fn param_name(&self, i: i32) -> Option<&[u8]> {
        let p = unsafe { sqlite3_bind_parameter_name(self.raw, i) };
        if p.is_null() { None } else { Some(cstr_bytes(p)) }
    }
    pub fn column_count(&self) -> i32 {
        unsafe { sqlite3_column_count(self.raw) }
    }
    pub fn column_name(&self, i: i32) -> &[u8] {
        cstr_bytes(unsafe { sqlite3_column_name(self.raw, i) })
    }
    pub fn column_type(&self, i: i32) -> i32 {
        unsafe { sqlite3_column_type(self.raw, i) }
    }
    pub fn column_int(&self, i: i32) -> i64 {
        unsafe { sqlite3_column_int64(self.raw, i) }
    }
    pub fn column_double(&self, i: i32) -> f64 {
        unsafe { sqlite3_column_double(self.raw, i) }
    }
    pub fn column_text(&self, i: i32) -> &[u8] {
        // SAFETY: text first, then bytes (the documented order); the slice
        // is valid until the next step/reset, which need &mut self.
        unsafe {
            let p = sqlite3_column_text(self.raw, i);
            let n = sqlite3_column_bytes(self.raw, i);
            if p.is_null() || n <= 0 { b"" } else { std::slice::from_raw_parts(p, n as usize) }
        }
    }
    pub fn column_blob(&self, i: i32) -> &[u8] {
        unsafe {
            let p = sqlite3_column_blob(self.raw, i) as *const u8;
            let n = sqlite3_column_bytes(self.raw, i);
            if p.is_null() || n <= 0 { b"" } else { std::slice::from_raw_parts(p, n as usize) }
        }
    }
}

// -- POSIX ------------------------------------------------------------------

/// Point fd 1 at stderr and return a private copy of the real stdout, so a
/// stray print cannot corrupt the protocol stream.
pub fn private_stdout() -> Option<std::fs::File> {
    use std::os::fd::FromRawFd;
    let fd = unsafe { dup(1) };
    if fd < 0 {
        return None;
    }
    unsafe { dup2(2, 1) };
    // SAFETY: fd is a fresh descriptor this process owns.
    Some(unsafe { std::fs::File::from_raw_fd(fd) })
}

/// The parent's Ctrl-C is not ours (we exit when stdin closes); a write to
/// a closed pipe is an error, not a signal.
pub fn ignore_signals() {
    const SIG_IGN: usize = 1;
    const SIGINT: c_int = 2;
    const SIGPIPE: c_int = 13;
    unsafe {
        signal(SIGINT, SIG_IGN);
        signal(SIGPIPE, SIG_IGN);
    }
}

/// Exit at once: no destructors, no atexit handlers, nothing flushed but
/// what was written (as the C worker's `_exit`).
pub fn exit_now(code: i32) -> ! {
    unsafe { _exit(code) }
}

/// RLIMIT_AS at the current address-space size plus `mb` (Linux only).
#[cfg(target_os = "linux")]
pub fn set_rlimit_as(mb: i64) -> bool {
    const RLIMIT_AS: c_int = 9;
    const RLIM_INFINITY: u64 = u64::MAX;
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else { return false };
    let Some(vm_kb) = status
        .lines()
        .find(|l| l.starts_with("VmSize:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse::<u64>().ok())
    else {
        return false;
    };
    let mut rl = RLimit { cur: 0, max: 0 };
    if unsafe { getrlimit(RLIMIT_AS, &mut rl) } != 0 {
        return false;
    }
    let mut limit = vm_kb * 1024 + (mb as u64) * 1024 * 1024;
    if rl.max != RLIM_INFINITY && limit > rl.max {
        limit = rl.max;
    }
    rl.cur = limit;
    unsafe { setrlimit(RLIMIT_AS, &rl) == 0 }
}

#[cfg(not(target_os = "linux"))]
pub fn set_rlimit_as(_mb: i64) -> bool {
    false
}
