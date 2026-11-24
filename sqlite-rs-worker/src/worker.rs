//! The server: lanes, routing, cancellation, the watchdog and every
//! request handler. Behaviour follows the C worker (cworker/src/worker.c)
//! request for request; see the module comments there for the reasons.
//!
//! Threads
//!   main      the main lane: requests that name no connection (and, in
//!             single-tenant mode, every request); replies.
//!   reader    reads and decodes frames; answers `hello`; acts on `cancel` at
//!             once; routes everything else to a lane; on end of input
//!             cancels whatever runs and exits after 5 s if nothing else has.
//!   watchdog  transaction deadlines (rolls back and sends `expired`), the
//!             time-limit backstop, and re-interrupting a cancelled statement
//!             every 5 ms until it returns.
//!   lanes     (--multi) one per connection: its `open` and every later
//!             request for it, in order.
//!
//! Ownership: a connection (`Conn`, holding the `Db`) and its streams are
//! owned by its lane's thread, and nothing else can reach them (`Db` is not
//! `Send`). Shared state (`State`, one mutex) holds only what other threads
//! need: queues, the job each lane is running, each connection's
//! transaction number and deadline, and an `Interrupter` per connection.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering::*};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use crate::codec::{self, Codec, Fail, Value, W, field_is_str};
use crate::errors::E;
use crate::export;
use crate::ffi::{self, Db, Interrupter, Stmt};
use crate::schema;
use crate::sqlutil::{self, Kw};

pub const PROTOCOL_VERSION: i64 = 1;
pub const DEFAULT_MAX_FRAME: usize = 256 * 1024 * 1024;
const FETCH_BATCH: i64 = 256;
const REINTERRUPT_NS: i64 = 5_000_000;
const SHUTDOWN_GRACE_S: u64 = 5;
const MAX_EXPIRED: usize = 64;
const SQLITE_ENGINE: &str = "dblayer.worker.engines:sqlite";
const LANE_STACK_BYTES: usize = 4 * 1024 * 1024;
const LASTROWID_SENTINEL: i64 = i64::MIN;

static EPOCH: OnceLock<Instant> = OnceLock::new();

pub fn now_ns() -> i64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as i64
}

#[derive(Clone, Debug, Default)]
pub struct Opts {
    pub debug: bool,
    pub multi: bool,
    pub max_conns: usize,
    pub memory_limit_mb: i64,
    pub heap_limit_mb: i64,
    pub soft_heap_limit_mb: i64,
    pub cache_kb: i64,
    pub lookaside: Option<(i32, i32)>,
}

// -- jobs and lanes ---------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Req,
    More,
    CloseStream,
    Expire,
    Shutdown,
}

const R_CANCELLED: u8 = 0;
const R_TIME_LIMIT: u8 = 1;
const R_DEADLINE: u8 = 2;

/// The part of a job other threads touch: cancellation and timers.
pub struct Ctl {
    pub id: Option<i64>,
    pub kind: Kind,
    pub cancelled: AtomicBool,
    reason: AtomicU8,
    /// Inside a statement (changed under the state mutex).
    active: AtomicBool,
    /// Time-limit backstop, monotonic ns; 0: none.
    backstop_ns: AtomicI64,
    next_interrupt_ns: AtomicI64,
    /// The connection it runs on, once known; 0: none.
    conn: AtomicI64,
}

impl Ctl {
    fn new(kind: Kind, id: Option<i64>) -> Arc<Ctl> {
        Arc::new(Ctl {
            id,
            kind,
            cancelled: AtomicBool::new(false),
            reason: AtomicU8::new(R_CANCELLED),
            active: AtomicBool::new(false),
            backstop_ns: AtomicI64::new(0),
            next_interrupt_ns: AtomicI64::new(0),
            conn: AtomicI64::new(0),
        })
    }
}

pub struct Job {
    pub ctl: Arc<Ctl>,
    pub msg: Value,
    credit: i64,
    exp_conn: i64,
    exp_tx: i32,
}

impl Job {
    fn new(kind: Kind, id: Option<i64>, msg: Value) -> Job {
        Job { ctl: Ctl::new(kind, id), msg, credit: 0, exp_conn: 0, exp_tx: 0 }
    }
    fn field(&self, k: &str) -> Option<&Value> {
        self.msg.get(k)
    }
}

struct Lane {
    is_main: bool,
    q: VecDeque<Job>,
    current: Option<Arc<Ctl>>,
    /// Multi-tenant: its connection, once open.
    conn: Option<i64>,
    cond: Arc<Condvar>,
}

/// What other threads need to know about a connection.
struct ConnMeta {
    lane: u64,
    tx: i32,
    tx_seq: i32,
    deadline_ns: i64,
    expired: VecDeque<i32>,
    intr: Interrupter,
}

struct StreamMeta {
    lane: u64,
}

#[derive(Default)]
struct State {
    lanes: BTreeMap<u64, Lane>,
    lane_seq: u64,
    nthread_lanes: usize,
    conns: BTreeMap<i64, ConnMeta>,
    streams: HashMap<i64, StreamMeta>,
    conn_seq: i64,
    eof: bool,
}

const MAIN_LANE: u64 = 0;

pub struct Shared {
    pub opts: Opts,
    out: Mutex<File>,
    parent_gone: AtomicBool,
    codec: AtomicU8,
    max_frame: AtomicUsize,
    rlimit_as: AtomicBool,
    exit_requested: AtomicBool,
    started: (Mutex<bool>, Condvar),
    mu: Mutex<State>,
    wcond: Condvar,
    lanecond: Condvar,
}

fn lock(m: &Mutex<State>) -> MutexGuard<'_, State> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Shared {
    pub fn new(opts: Opts, out: File) -> Arc<Shared> {
        let mut st = State::default();
        st.lanes.insert(MAIN_LANE, Lane { is_main: true, q: VecDeque::new(), current: None, conn: None, cond: Arc::new(Condvar::new()) });
        st.lane_seq = MAIN_LANE;
        Arc::new(Shared {
            opts,
            out: Mutex::new(out),
            parent_gone: AtomicBool::new(false),
            codec: AtomicU8::new(0),
            max_frame: AtomicUsize::new(DEFAULT_MAX_FRAME),
            rlimit_as: AtomicBool::new(false),
            exit_requested: AtomicBool::new(false),
            started: (Mutex::new(false), Condvar::new()),
            mu: Mutex::new(st),
            wcond: Condvar::new(),
            lanecond: Condvar::new(),
        })
    }

    pub fn set_rlimit_as(&self, v: bool) {
        self.rlimit_as.store(v, SeqCst);
    }

    fn codec(&self) -> Codec {
        if self.codec.load(Relaxed) == 1 { Codec::Msgpack } else { Codec::Json }
    }

    fn max_frame(&self) -> usize {
        self.max_frame.load(Relaxed)
    }

    pub fn write_all(&self, p: &[u8]) {
        let mut out = self.out.lock().unwrap_or_else(|e| e.into_inner());
        if self.parent_gone.load(Relaxed) {
            return;
        }
        if out.write_all(p).is_err() {
            self.parent_gone.store(true, SeqCst);
        }
    }

    fn send(&self, w: &mut W) {
        if w.ok() {
            let f = w.finish();
            self.write_all(f);
        }
    }

    /// Let the reader start (after RLIMIT_AS is set).
    pub fn start(&self) {
        let (m, c) = &self.started;
        *m.lock().unwrap() = true;
        c.notify_all();
    }
}

fn enqueue(st: &mut State, lane: u64, job: Job, front: bool) {
    let Some(ln) = st.lanes.get_mut(&lane) else { return };
    if front {
        ln.q.push_front(job);
    } else {
        ln.q.push_back(job);
    }
    ln.cond.notify_one();
}

/// [mu] Cancel a job: it fails with an error for `reason`. A running
/// statement is interrupted now and every 5 ms until it returns.
fn cancel_job(sh: &Shared, st: &State, ctl: &Ctl, reason: u8) {
    ctl.cancelled.store(true, SeqCst);
    ctl.reason.store(reason, SeqCst);
    if ctl.active.load(SeqCst)
        && let Some(meta) = st.conns.get(&ctl.conn.load(SeqCst))
    {
        meta.intr.interrupt();
        ctl.next_interrupt_ns.store(now_ns() + REINTERRUPT_NS, SeqCst);
        sh.wcond.notify_one();
    }
}

/// [mu] The lane running (or holding queued) request `id`.
fn lane_of_request(st: &State, id: i64) -> Option<(u64, Arc<Ctl>, bool)> {
    for (lid, ln) in &st.lanes {
        if let Some(c) = &ln.current
            && c.id == Some(id)
            && c.kind != Kind::Expire
        {
            return Some((*lid, c.clone(), true));
        }
        for j in &ln.q {
            if j.ctl.id == Some(id) && j.ctl.kind != Kind::Expire {
                return Some((*lid, j.ctl.clone(), false));
            }
        }
    }
    None
}

/// [mu] The connection a message names (by `conn`, or the only one).
fn named_conn(st: &State, msg: &Value) -> Result<i64, E> {
    let v = msg.get("conn");
    match v {
        None | Some(Value::Nil) => {
            if st.conns.len() == 1 {
                Ok(*st.conns.keys().next().unwrap())
            } else {
                Err(E::protocol(format!("request has no 'conn' and the worker has {} connections", st.conns.len())))
            }
        }
        Some(Value::Int(i)) => {
            if st.conns.contains_key(i) {
                Ok(*i)
            } else {
                Err(E::protocol(format!("no connection {i} in this worker")))
            }
        }
        Some(_) => Err(E::protocol("no connection in this worker")),
    }
}

/// [mu] Cancel everything on every lane and queue a shutdown behind it.
fn cancel_all_lanes(sh: &Shared, st: &mut State, main_too: bool) {
    let ids: Vec<u64> = st.lanes.keys().copied().collect();
    for lid in ids {
        let ln = &st.lanes[&lid];
        if ln.is_main && !main_too {
            continue;
        }
        for j in &ln.q {
            j.ctl.cancelled.store(true, SeqCst);
        }
        if let Some(c) = ln.current.clone() {
            cancel_job(sh, st, &c, R_CANCELLED);
        }
        enqueue(st, lid, Job::new(Kind::Shutdown, None, Value::Nil), false);
    }
}

/// [mu] Wait until every connection lane's thread has exited, or `until`.
fn wait_lanes<'a>(sh: &'a Shared, mut st: MutexGuard<'a, State>, until: Instant) -> (MutexGuard<'a, State>, bool) {
    while st.nthread_lanes > 0 {
        let now = Instant::now();
        if now >= until {
            break;
        }
        st = sh.lanecond.wait_timeout(st, until - now).unwrap_or_else(|e| e.into_inner()).0;
    }
    let ok = st.nthread_lanes == 0;
    (st, ok)
}

// -- lane-local state ---------------------------------------------------------

pub struct Stream {
    pub id: i64,
    pub st: Option<Stmt>,
    pub started: bool,
    pub has_row: bool,
    first: bool,
    pub ncols: i32,
    pub credit: i64,
    chunk_rows: i64,
    pub time_limit: Option<f64>, // remaining ms
    max_bytes: Option<i64>,
    bytes: i64,
    pub ex: Option<Box<export::Export>>,
    /// The end frame (or final reply) was sent.
    pub ended: bool,
}

impl Stream {
    pub fn new(id: i64, credit: i64) -> Stream {
        Stream {
            id,
            st: None,
            started: false,
            has_row: false,
            first: true,
            ncols: 0,
            credit,
            chunk_rows: 1000,
            time_limit: None,
            max_bytes: None,
            bytes: 0,
            ex: None,
            ended: false,
        }
    }
}

pub struct Conn {
    pub id: i64,
    // Streams before the connection: dropped (finalized) first.
    streams: HashMap<i64, Stream>,
    pub db: Db,
    write: bool,
    path: Vec<u8>,
    busy_ms: i32,
}

impl Conn {
    fn in_tx(&self) -> bool {
        !self.db.autocommit()
    }
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.streams.clear();
        if self.in_tx() {
            self.db.exec("ROLLBACK");
        }
    }
}

struct Local {
    lane: u64,
    conns: HashMap<i64, Conn>,
    w: W,
}

// -- statements ---------------------------------------------------------------

/// Statement entry and exit (SqliteConnection._statement): refuse to start
/// once cancelled; mark the job active so a cancel interrupts it.
pub struct Ctx<'a> {
    pub sh: &'a Shared,
    pub ctl: &'a Arc<Ctl>,
}

impl Ctx<'_> {
    pub fn enter(&self) -> Result<(), E> {
        let _g = lock(&self.sh.mu);
        if self.ctl.cancelled.load(SeqCst) {
            return Err(E::interrupted());
        }
        self.ctl.active.store(true, SeqCst);
        Ok(())
    }

    pub fn leave(&self) {
        let _g = lock(&self.sh.mu);
        self.ctl.active.store(false, SeqCst);
    }

    pub fn cancelled(&self) -> bool {
        self.ctl.cancelled.load(Relaxed)
    }

    /// The progress handler's check (_TimeLimit; it also sees cancellation),
    /// and how often SQLite should call it.
    pub fn limit(&self, time_limit_ms: Option<i64>) -> (i32, impl Fn() -> bool + '_) {
        let deadline = time_limit_ms.map(|ms| now_ns().saturating_add(ms.saturating_mul(1_000_000)));
        let n = if time_limit_ms.is_some_and(|ms| ms <= 20) { 1 } else { 1000 };
        let ctl = self.ctl;
        (n, move || ctl.cancelled.load(Relaxed) || deadline.is_some_and(|d| now_ns() >= d))
    }
}

/// CPython's pysqlite_statement_create. Ok(None): an empty statement.
pub fn prepare(db: &Db, sqlv: Option<&Value>) -> Result<Option<Stmt>, E> {
    let Some(Value::Str(sql)) = sqlv else { return Err(E::builtin("TypeError", "sql must be a string")) };
    if sql.contains(&0) {
        return Err(E::prog("the query contains a null character"));
    }
    if sql.len() as i64 > db.limit(ffi::LIMIT_SQL_LENGTH, -1) as i64 {
        let mut e = E::new("QueryError", "query string is too large");
        e.cause = Some(("sqlite3.DataError", "query string is too large".into()));
        return Err(e);
    }
    let (st, tail) = db.prepare(sql).map_err(|rc| E::sqlite(Some(db), rc))?;
    if sqlutil::lstrip_sql(&sql[tail..]).is_some() {
        return Err(E::prog("You can only execute one statement at a time."));
    }
    Ok(st)
}

fn bind_value(db: &Db, st: &mut Stmt, i: i32, v: &Value) -> Result<(), E> {
    let rc = match v {
        Value::Nil => st.bind_null(i),
        Value::Bool(b) => st.bind_int(i, *b as i64),
        Value::Int(n) => st.bind_int(i, *n),
        Value::BigInt(_) => return Err(E::builtin("OverflowError", "Python int too large to convert to SQLite INTEGER")),
        Value::Float(d) => st.bind_double(i, *d),
        Value::Str(s) => st.bind_text(i, s),
        Value::Bin(b) => st.bind_blob(i, b),
        Value::Ext(code, inner) => {
            // Python's sqlite3 still has default adapters for datetime
            // (isoformat(" ")) and date (isoformat()); every other value
            // type is a binding error naming the Python type.
            let mut inner: &Value = inner;
            if *code == 16
                && let Value::Arr(a) = inner
                && a.len() == 2
            {
                inner = &a[0]; // [isoformat, zone name]
            }
            if (*code == 16 || *code == 17)
                && let Value::Str(s) = inner
            {
                let mut s = s.clone();
                if *code == 16 && s.len() > 10 && s[10] == b'T' {
                    s[10] = b' ';
                }
                st.bind_text(i, &s)
            } else {
                const NAMES: [&str; 8] = [
                    "datetime.datetime",
                    "datetime.date",
                    "datetime.time",
                    "decimal.Decimal",
                    "UUID",
                    "datetime.timedelta",
                    "dict",
                    "tuple",
                ];
                let k = *code as i32 - 16;
                return Err(if (0..8).contains(&k) {
                    E::prog(format!("Error binding parameter {i}: type '{}' is not supported", NAMES[k as usize]))
                } else {
                    E::prog(format!("Error binding parameter {i}: type 'ext {code}' is not supported"))
                });
            }
        }
        Value::Arr(_) => return Err(E::prog(format!("Error binding parameter {i}: type 'list' is not supported"))),
        Value::Map(_) => return Err(E::prog(format!("Error binding parameter {i}: type 'dict' is not supported"))),
    };
    if rc != ffi::SQLITE_OK {
        return Err(E::sqlite(Some(db), rc));
    }
    Ok(())
}

/// CPython's bind_parameters (3.14 rules: a sequence for named
/// placeholders is an error).
pub fn bind_params(db: &Db, st: Option<&mut Stmt>, params: Option<&Value>) -> Result<(), E> {
    let num = st.as_ref().map(|s| s.param_count()).unwrap_or(0);
    match params {
        None | Some(Value::Nil) | Some(Value::Arr(_)) => {
            let empty = Vec::new();
            let items = match params {
                Some(Value::Arr(a)) => a,
                _ => &empty,
            };
            if items.len() != num as usize {
                return Err(E::prog(format!(
                    "Incorrect number of bindings supplied. The current statement uses {num}, and there are {} supplied.",
                    items.len()
                )));
            }
            let Some(st) = st else { return Ok(()) };
            for i in 1..=num {
                if let Some(name) = st.param_name(i)
                    && name.first() != Some(&b'?')
                {
                    return Err(E::prog(format!(
                        "Binding {i} ('{}') is a named parameter, but you supplied a sequence which requires nameless (qmark) placeholders.",
                        String::from_utf8_lossy(name)
                    )));
                }
                bind_value(db, st, i, &items[i as usize - 1])?;
            }
            Ok(())
        }
        Some(Value::Map(items)) => {
            let Some(st) = st else { return Ok(()) };
            // A large dict is indexed once rather than searched per
            // placeholder (the first of a repeated key wins, as lookups do).
            let index: Option<HashMap<&[u8], &Value>> = (items.len() > 16).then(|| {
                let mut m = HashMap::with_capacity(items.len());
                for (k, v) in items {
                    if let Value::Str(k) = k {
                        m.entry(&k[..]).or_insert(v);
                    }
                }
                m
            });
            for i in 1..=num {
                let Some(name) = st.param_name(i).map(|n| n.to_vec()) else {
                    return Err(E::prog(format!("Binding {i} has no name, but you supplied a dictionary (which has only names).")));
                };
                let key = &name[1..];
                let v = match &index {
                    Some(m) => m.get(key).copied(),
                    None => items.iter().find(|(k, _)| matches!(k, Value::Str(s) if s == key)).map(|(_, v)| v),
                };
                let Some(v) = v else {
                    return Err(E::prog(format!(
                        "You did not supply a value for binding parameter {}.",
                        String::from_utf8_lossy(&name)
                    )));
                };
                bind_value(db, st, i, v)?;
            }
            Ok(())
        }
        Some(_) => Err(E::prog("parameters are of unsupported type")),
    }
}

pub fn write_columns(w: &mut W, st: Option<&Stmt>, ncols: i32) {
    w.cstr("columns");
    w.array(ncols as u32);
    if let Some(st) = st {
        for i in 0..ncols {
            w.str_lossy(st.column_name(i));
        }
    }
    w.end();
}

pub fn decode_error(st: &Stmt, i: i32, p: &[u8]) -> E {
    let txt = String::from_utf8_lossy(&codec::utf8_lossy(p)).into_owned();
    let cn = String::from_utf8_lossy(&codec::utf8_lossy(st.column_name(i))).into_owned();
    let mut e = E::new("QueryError", format!("Could not decode to UTF-8 column '{cn}' with text '{txt}'"));
    e.cause = Some(("sqlite3.OperationalError", e.msg.clone()));
    e
}

/// Convert (and, if `encode`, write) the current row as CPython would,
/// adding its size as _row_bytes() counts it.
fn emit_row(st: &Stmt, ncols: i32, w: &mut W, encode: bool, size: &mut i64) -> Result<(), E> {
    if encode {
        w.array(ncols as u32);
    }
    for i in 0..ncols {
        match st.column_type(i) {
            ffi::SQLITE_INTEGER => {
                *size += 8;
                if encode {
                    w.int(st.column_int(i));
                }
            }
            ffi::SQLITE_FLOAT => {
                *size += 8;
                if encode {
                    w.double(st.column_double(i));
                }
            }
            ffi::SQLITE_TEXT => {
                let p = st.column_text(i);
                let Some(chars) = codec::utf8_chars(p) else { return Err(decode_error(st, i, p)) };
                *size += chars as i64;
                if encode {
                    w.str(p);
                }
            }
            ffi::SQLITE_BLOB => {
                let p = st.column_blob(i);
                *size += p.len() as i64;
                if encode {
                    w.bin(p);
                }
            }
            _ => {
                *size += 1;
                if encode {
                    w.nil();
                }
            }
        }
    }
    if encode {
        w.end();
    }
    Ok(())
}

fn step_ok(rc: i32) -> bool {
    rc == ffi::SQLITE_ROW || rc == ffi::SQLITE_DONE
}

#[derive(Clone, Copy, Default)]
struct Limits {
    tl: Option<i64>,
    max_rows: Option<i64>,
    max_bytes: Option<i64>,
}

fn get_limits(m: &Value) -> Limits {
    Limits { tl: m.int_field("time_limit_ms", 0), max_rows: m.int_field("max_rows", 0), max_bytes: m.int_field("max_result_bytes", 0) }
}

fn too_large(max: i64) -> E {
    E::new("ResourceLimitExceeded", format!("result exceeded max_result_bytes={max}"))
}

/// SqliteConnection._execute: writes the result map into w.
fn run_execute(cx: &Ctx, c: &Conn, sqlv: Option<&Value>, params: Option<&Value>, lim: Limits, w: &mut W, recover: bool) -> Result<(), E> {
    let sql = sqlv.and_then(|v| v.as_bytes()).unwrap_or(b"");
    let kw = sqlutil::leading_keyword(sql);
    let is_write = matches!(kw, Kw::Insert | Kw::Update | Kw::Delete | Kw::Replace);
    let is_insert = matches!(kw, Kw::Insert | Kw::Replace);
    let dml = sqlutil::cpython_is_dml(sql);
    let mark = w.mark();
    let mut retried = false;
    loop {
        let db = &c.db;
        let rowid_before = db.last_insert_rowid();
        // A sentinel, not a before/after comparison: an insert that reuses
        // the previous insert's rowid (insert 5, delete, insert 5) must
        // report it, and an upsert that updates that row must not.
        if is_insert {
            db.set_last_insert_rowid(LASTROWID_SENTINEL);
        }
        let r = execute_once(cx, db, sqlv, params, lim, w, is_write, is_insert, dml);
        if is_insert && db.last_insert_rowid() == LASTROWID_SENTINEL {
            db.set_last_insert_rowid(rowid_before);
        }
        match r {
            Err(e) if recover && !retried && !c.write && e.ext == ffi::SQLITE_READONLY_ROLLBACK => {
                // _execute_recovering: a hot journal left by a killed writer.
                retried = true;
                recover_hot_journal(&c.path, c.busy_ms);
                w.rewind(mark);
            }
            r => return r,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_once(
    cx: &Ctx,
    db: &Db,
    sqlv: Option<&Value>,
    params: Option<&Value>,
    lim: Limits,
    w: &mut W,
    is_write: bool,
    is_insert: bool,
    dml: bool,
) -> Result<(), E> {
    let (n, check) = cx.limit(lim.tl);
    let _progress = db.progress(n, &check);
    cx.enter()?;
    let r = (|| {
        w.map_open();
        let mut st = prepare(db, sqlv)?;
        bind_params(db, st.as_mut(), params)?;
        let mut rc = ffi::SQLITE_DONE;
        if let Some(s) = st.as_mut() {
            rc = s.step();
            if !step_ok(rc) {
                return Err(E::sqlite(Some(db), rc));
            }
        }
        let mut lastrowid = db.last_insert_rowid();
        let ncols = st.as_ref().map(|s| s.column_count()).unwrap_or(0);
        write_columns(w, st.as_ref(), ncols);
        let mut truncated = false;
        if let Some(s) = st.as_mut().filter(|_| ncols > 0) {
            let limit = lim.max_rows.map(|m| m.saturating_add(1));
            let (mut count, mut size, mut batch) = (0i64, 0i64, 0i64);
            let mut rows_open = false;
            while rc == ffi::SQLITE_ROW && limit.is_none_or(|l| count < l) {
                let encode = lim.max_rows.is_none_or(|m| count < m);
                if encode && !rows_open {
                    w.cstr("rows");
                    w.array_open();
                    rows_open = true;
                }
                emit_row(s, ncols, w, encode, &mut size)?;
                count += 1;
                batch += 1;
                rc = s.step();
                if !step_ok(rc) {
                    return Err(E::sqlite(Some(db), rc));
                }
                if let Some(mb) = lim.max_bytes
                    && (batch == FETCH_BATCH || rc == ffi::SQLITE_DONE || Some(count) == limit)
                {
                    batch = 0;
                    if size > mb {
                        return Err(too_large(mb));
                    }
                }
                if !w.ok() {
                    return Err(wbuf_error(w));
                }
            }
            if rows_open {
                w.end();
            }
            if let Some(m) = lim.max_rows
                && count > m
            {
                truncated = true;
                if is_write {
                    // sqlite3 reports rowcount for RETURNING statements only
                    // once they have run to completion.
                    while rc == ffi::SQLITE_ROW {
                        rc = s.step();
                    }
                    if !step_ok(rc) {
                        return Err(E::sqlite(Some(db), rc));
                    }
                }
            }
        }
        let mut rowcount = -1;
        if dml && rc == ffi::SQLITE_DONE {
            rowcount = db.changes();
        }
        if is_write && rowcount == -1 {
            // "WITH ... INSERT": sqlite3 does not see DML; ask SQLite.
            rowcount = db.changes();
            lastrowid = db.last_insert_rowid();
        }
        drop(st);
        if truncated {
            w.cstr("truncated");
            w.bool(true);
        }
        if rowcount != -1 {
            w.cstr("rowcount");
            w.int(rowcount);
        }
        if is_insert && rowcount > 0 && lastrowid != LASTROWID_SENTINEL {
            w.cstr("last_pk");
            w.int(lastrowid);
        }
        w.end();
        Ok(())
    })();
    cx.leave();
    r
}

/// Run one statement that returns no rows we keep (BEGIN, COMMIT, ...).
fn exec_simple(cx: &Ctx, db: &Db, sql: &str) -> Result<(), E> {
    let (n, check) = cx.limit(None);
    let _progress = db.progress(n, &check);
    cx.enter()?;
    let r = match db.prepare(sql.as_bytes()) {
        Ok((Some(mut st), _)) => {
            let mut rc;
            loop {
                rc = st.step();
                if rc != ffi::SQLITE_ROW {
                    break;
                }
            }
            if step_ok(rc) { Ok(()) } else { Err(E::sqlite(Some(db), rc)) }
        }
        Ok((None, _)) => Ok(()),
        Err(rc) => Err(E::sqlite(Some(db), rc)),
    };
    cx.leave();
    r
}

fn recover_hot_journal(path: &[u8], busy_ms: i32) {
    let mut j = path.to_vec();
    j.extend_from_slice(b"-journal");
    let exists = std::str::from_utf8(&j).is_ok_and(|p| std::path::Path::new(p).exists());
    if !exists {
        return;
    }
    let uri = format!("{}?mode=rw", sqlutil::path_as_uri(path));
    if let Ok(rw) = Db::open(&uri, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_URI) {
        rw.busy_timeout(busy_ms);
        rw.exec("select count(*) from sqlite_master");
    }
}

// -- replies ------------------------------------------------------------------

fn reply_begin(sh: &Shared, w: &mut W, ctl: &Ctl) {
    w.start(sh.codec(), sh.max_frame());
    w.map_open();
    w.cstr("id");
    match ctl.id {
        Some(i) => w.int(i),
        None => w.nil(),
    }
}

fn reply_ok(sh: &Shared, w: &mut W, ctl: &Ctl) {
    reply_begin(sh, w, ctl);
    w.cstr("ok");
    w.bool(true);
}

fn reply_in_tx(w: &mut W, c: &Conn) {
    if c.in_tx() {
        w.cstr("in_tx");
        w.bool(true);
    }
}

pub fn wbuf_error(w: &W) -> E {
    if w.failed == Fail::TooLarge {
        E::new(
            "ResourceLimitExceeded",
            format!("reply too large: frame of more than {} bytes exceeds the {}-byte frame size cap", w.limit, w.limit),
        )
    } else {
        E::new("ResourceLimitExceeded", "worker ran out of memory (memory limit)")
    }
}

fn tx_number(v: Option<&Value>) -> i32 {
    // "t<N>" -> N; anything else -> -1
    let Some(Value::Str(s)) = v else { return -1 };
    if s.len() < 2 || s.len() > 12 || s[0] != b't' {
        return -1;
    }
    let mut n: i64 = 0;
    for &c in &s[1..] {
        if !c.is_ascii_digit() {
            return -1;
        }
        n = n * 10 + (c - b'0') as i64;
    }
    n as i32
}

fn tx_text(v: Option<&Value>) -> String {
    match v {
        Some(Value::Str(s)) => String::from_utf8_lossy(&s[..s.len().min(64)]).into_owned(),
        Some(Value::Int(i)) => i.to_string(),
        _ => "None".into(),
    }
}

/// Done: the reply is in w (to be finished and sent) or already sent.
#[derive(PartialEq, Eq)]
enum Done {
    Reply,
    Sent,
}

type R = Result<Done, E>;

// -- the lane: requests -------------------------------------------------------

struct Run<'a> {
    sh: &'a Shared,
    job: &'a Job,
}

impl<'a> Run<'a> {
    fn cx(&self) -> Ctx<'a> {
        Ctx { sh: self.sh, ctl: &self.job.ctl }
    }

    /// Server.check_tx
    fn check_tx(&self, c: &Conn) -> Result<(), E> {
        let v = self.job.field("tx");
        if matches!(v, None | Some(Value::Nil)) {
            return Ok(());
        }
        let n = tx_number(v);
        let (cur, exp) = {
            let st = lock(&self.sh.mu);
            let m = &st.conns[&c.id];
            (m.tx, n > 0 && m.expired.contains(&n))
        };
        if n > 0 && n == cur {
            return Ok(());
        }
        let t = tx_text(v);
        Err(if exp {
            E::new("TransactionExpired", format!("transaction {t} passed its deadline and was rolled back"))
        } else {
            E::new("TransactionClosed", format!("transaction {t} is not open"))
        })
    }

    fn open(&self, local: &mut Local) -> R {
        let job = self.job;
        if let Some(engine) = job.field("engine")
            && *engine != Value::Nil
            && !engine.is_str(SQLITE_ENGINE)
        {
            let name = engine.as_bytes().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_else(|| "?".into());
            return Err(E::new("NotSupported", format!("the C worker runs SQLite only; engine {name} is not supported")));
        }
        let Some(Value::Str(path)) = job.field("path") else { return Err(E::builtin("KeyError", "'path'")) };
        let write = field_is_str(&job.msg, "mode", "write");
        let nil = Value::Nil;
        let cfg = job.field("config").unwrap_or(&nil);
        if let Some(Value::Arr(a)) = cfg.get("init")
            && !a.is_empty()
        {
            return Err(E::new("NotSupported", "init hooks ('module:function') need the Python worker"));
        }
        let limits = match cfg.get("sql_limits") {
            Some(Value::Map(m)) => &m[..],
            _ => &[],
        };
        for (k, _) in limits {
            if !SQL_LIMITS.iter().any(|(n, _)| k.is_str(n)) {
                let name = k.as_bytes().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default();
                return Err(E::builtin("ValueError", format!("unknown SQL limit '{name}'")));
            }
        }
        let cfg_bool = |k: &str, d: bool| match cfg.get(k) {
            Some(Value::Bool(b)) => *b,
            Some(Value::Int(i)) => *i != 0,
            _ => d,
        };
        let immutable = cfg_bool("immutable", false);
        let create = cfg_bool("create", true) && !immutable;
        let busy = match cfg.get("busy_timeout") {
            Some(Value::Float(f)) => *f,
            Some(Value::Int(i)) => *i as f64,
            _ => 5.0,
        };
        let busy_ms = (busy * 1000.0) as i32;
        let heap_mb = cfg.int_or("memory_limit_mb", 0);
        if write && immutable {
            return Err(E::new("NotSupported", "database is immutable; writes are not allowed"));
        }
        let path_s = String::from_utf8_lossy(path).into_owned();
        if create && !write {
            // A read before the first write: an empty file is a valid database.
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&path_s) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => {
                    let errno = e.raw_os_error().unwrap_or(0);
                    let text = os_error_text(&e);
                    return Err(E::coded("QueryError", "WORKER", format!("worker error: OSError: [Errno {errno}] {text}: '{path_s}'")));
                }
            }
        }
        if !write && !immutable {
            recover_hot_journal(path, busy_ms);
        }
        let q = if write {
            if create { "?mode=rwc" } else { "?mode=rw" }
        } else if immutable {
            "?immutable=1"
        } else {
            "?mode=ro"
        };
        let uri = format!("{}{q}", sqlutil::path_as_uri(path));
        let mut db = Db::open(&uri, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_URI)
            .map_err(|oe| {
                let ext = if oe.ext == ffi::SQLITE_OK || (oe.ext & 0xff) != (oe.rc & 0xff) { oe.rc } else { oe.ext };
                E::sqlite_parts(ext, ext & 0xff, oe.msg)
            })?;
        db.busy_timeout(busy_ms);
        let o = &self.sh.opts;
        if let Some((size, count)) = o.lookaside {
            db.lookaside(size, count);
        }
        if o.cache_kb > 0 {
            db.exec(&format!("pragma cache_size = -{}", o.cache_kb));
        }
        // hard_heap_limit is process-wide: in a multi-tenant worker it is
        // set once, from --heap-limit-mb.
        if heap_mb > 0 && !o.multi {
            db.exec(&format!("pragma hard_heap_limit = {}", heap_mb * 1024 * 1024));
        }
        for (k, v) in limits {
            if let Value::Int(v) = v
                && let Some((_, id)) = SQL_LIMITS.iter().find(|(n, _)| k.is_str(n))
            {
                db.limit(*id, *v as i32);
            }
        }
        if let Some(Value::Arr(exts)) = cfg.get("extensions") {
            for p in exts {
                let ep = p.as_bytes().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default();
                if let Err(m) = db.load_extension(&ep) {
                    return Err(E::coded("QueryError", "INIT", format!("cannot load extension {ep}: {m}")));
                }
            }
        }
        db.set_busy_handler(busy_ms); // replaces the busy_timeout set above
        let intr = db.interrupter();
        let id = {
            let mut st = lock(&self.sh.mu);
            st.conn_seq += 1;
            let id = st.conn_seq;
            st.conns.insert(id, ConnMeta { lane: local.lane, tx: 0, tx_seq: 0, deadline_ns: 0, expired: VecDeque::new(), intr });
            if o.multi
                && let Some(ln) = st.lanes.get_mut(&local.lane)
                && !ln.is_main
            {
                ln.conn = Some(id);
            }
            id
        };
        local.conns.insert(id, Conn { id, streams: HashMap::new(), db, write, path: path.clone(), busy_ms });
        reply_ok(self.sh, &mut local.w, &job.ctl);
        local.w.cstr("conn");
        local.w.int(id);
        Ok(Done::Reply)
    }

    fn execute(&self, c: &Conn, w: &mut W) -> R {
        self.check_tx(c)?;
        let lim = get_limits(&self.job.msg);
        if let Some(tl) = lim.tl {
            let _st = lock(&self.sh.mu);
            self.job.ctl.backstop_ns.store(now_ns() + tl.saturating_mul(1_000_000) + 5_000_000, SeqCst);
            self.sh.wcond.notify_one();
        }
        reply_begin(self.sh, w, &self.job.ctl);
        w.cstr("result");
        let r = run_execute(&self.cx(), c, self.job.field("sql"), self.job.field("params"), lim, w, true);
        {
            let _st = lock(&self.sh.mu);
            self.job.ctl.backstop_ns.store(0, SeqCst);
        }
        r?;
        reply_in_tx(w, c);
        Ok(Done::Reply)
    }

    fn batch(&self, c: &Conn, w: &mut W) -> R {
        self.check_tx(c)?;
        let mut lim = get_limits(&self.job.msg);
        lim.max_bytes = None;
        let Some(Value::Arr(stmts)) = self.job.field("statements") else {
            return Err(E::builtin("KeyError", "'statements'"));
        };
        reply_begin(self.sh, w, &self.job.ctl);
        w.cstr("result");
        w.array(stmts.len() as u32);
        for pair in stmts {
            let Value::Arr(p) = pair else { return Err(E::builtin("ValueError", "a batch statement is [sql, params]")) };
            if p.len() != 2 {
                return Err(E::builtin("ValueError", "a batch statement is [sql, params]"));
            }
            run_execute(&self.cx(), c, Some(&p[0]), Some(&p[1]), lim, w, false)?;
        }
        w.end();
        reply_in_tx(w, c);
        Ok(Done::Reply)
    }

    fn execute_many(&self, c: &Conn, w: &mut W) -> R {
        self.check_tx(c)?;
        let sqlv = self.job.field("sql");
        let sql = sqlv.and_then(|v| v.as_bytes()).unwrap_or(b"");
        let dml = sqlutil::cpython_is_dml(sql);
        let cx = self.cx();
        let db = &c.db;
        let (n, check) = cx.limit(None);
        let _progress = db.progress(n, &check);
        cx.enter()?;
        let r = (|| {
            let mut rowcount: i64 = if dml { 0 } else { -1 };
            let mut st = prepare(db, sqlv)?;
            if st.as_ref().is_some_and(|s| s.readonly()) {
                return Err(E::prog("executemany() can only execute DML statements."));
            }
            let empty = Vec::new();
            let seq = match self.job.field("params_seq") {
                Some(Value::Arr(a)) => a,
                _ => &empty,
            };
            if let Some(s) = st.as_mut() {
                for p in seq {
                    s.reset();
                    bind_params(db, Some(s), Some(p))?;
                    let rc = s.step();
                    if !step_ok(rc) {
                        return Err(E::sqlite(Some(db), rc));
                    }
                    if rc == ffi::SQLITE_DONE && dml {
                        rowcount += db.changes();
                    }
                }
            }
            Ok(rowcount)
        })();
        cx.leave();
        let rowcount = r?;
        reply_begin(self.sh, w, &self.job.ctl);
        w.cstr("result");
        w.map(1);
        w.cstr("rowcount");
        w.int(rowcount);
        w.end();
        reply_in_tx(w, c);
        Ok(Done::Reply)
    }

    fn begin(&self, c: &Conn, w: &mut W) -> R {
        let open = lock(&self.sh.mu).conns[&c.id].tx != 0;
        if open {
            return Err(E::coded("QueryError", "SQLITE_ERROR", "cannot start a transaction within a transaction"));
        }
        let mode = self.job.field("mode");
        let sql = match mode {
            None | Some(Value::Nil) => "BEGIN IMMEDIATE",
            Some(m) if m.is_str("write") => "BEGIN IMMEDIATE",
            Some(m) if m.is_str("read") => "BEGIN",
            m => return Err(E::builtin("ValueError", format!("unknown transaction mode '{}'", tx_text(m)))),
        };
        exec_simple(&self.cx(), &c.db, sql)?;
        let deadline = self.job.msg.int_field("deadline_ms", 0);
        let tx = {
            let mut st = lock(&self.sh.mu);
            let m = st.conns.get_mut(&c.id).unwrap();
            m.tx_seq += 1;
            m.tx = m.tx_seq;
            if let Some(ms) = deadline {
                m.deadline_ns = (now_ns() + ms.saturating_mul(1_000_000)).max(1);
                self.sh.wcond.notify_one();
            }
            m.tx
        };
        reply_ok(self.sh, w, &self.job.ctl);
        w.cstr("tx");
        w.cstr(&format!("t{tx}"));
        Ok(Done::Reply)
    }

    fn clear_tx(&self, c: &Conn) {
        let mut st = lock(&self.sh.mu);
        if let Some(m) = st.conns.get_mut(&c.id) {
            m.tx = 0;
            m.deadline_ns = 0;
        }
    }

    fn commit(&self, c: &Conn, w: &mut W) -> R {
        self.check_tx(c)?;
        {
            // a commit that has started is not cut short
            let mut st = lock(&self.sh.mu);
            st.conns.get_mut(&c.id).unwrap().deadline_ns = 0;
        }
        if c.in_tx() {
            exec_simple(&self.cx(), &c.db, "COMMIT")?;
        }
        self.clear_tx(c);
        reply_ok(self.sh, w, &self.job.ctl);
        Ok(Done::Reply)
    }

    fn rollback(&self, c: &Conn, w: &mut W) -> R {
        let n = tx_number(self.job.field("tx"));
        let was_expired = {
            let mut st = lock(&self.sh.mu);
            let m = st.conns.get_mut(&c.id).unwrap();
            if n > 0 && m.tx != n {
                if let Some(i) = m.expired.iter().position(|&t| t == n) {
                    m.expired.remove(i);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        if was_expired {
            reply_ok(self.sh, w, &self.job.ctl);
            w.cstr("expired");
            w.bool(true);
            return Ok(Done::Reply);
        }
        self.check_tx(c)?;
        if c.in_tx() {
            exec_simple(&self.cx(), &c.db, "ROLLBACK")?;
        }
        self.clear_tx(c);
        reply_ok(self.sh, w, &self.job.ctl);
        Ok(Done::Reply)
    }

    fn op(&self, c: &Conn) -> R {
        self.check_tx(c)?;
        let name = match self.job.field("name") {
            Some(Value::Str(s)) => String::from_utf8_lossy(s).into_owned(),
            _ => "None".into(),
        };
        Err(E::coded(
            "NotSupported",
            "NOT_IMPLEMENTED",
            format!("named operation {name} is not implemented by the C worker (the parent runs it)"),
        ))
    }

    fn schema_version(&self, c: &Conn, w: &mut W) -> R {
        self.check_tx(c)?;
        let db = &c.db;
        let v = match db.prepare(b"pragma schema_version") {
            Ok((Some(mut st), _)) => {
                let rc = st.step();
                if rc != ffi::SQLITE_ROW {
                    return Err(E::sqlite(Some(db), rc));
                }
                st.column_int(0)
            }
            Ok((None, _)) => return Err(E::sqlite(Some(db), ffi::SQLITE_ERROR)),
            Err(rc) => return Err(E::sqlite(Some(db), rc)),
        };
        reply_begin(self.sh, w, &self.job.ctl);
        w.cstr("result");
        w.int(v);
        reply_in_tx(w, c);
        Ok(Done::Reply)
    }

    fn schema(&self, c: &Conn, w: &mut W) -> R {
        self.check_tx(c)?;
        let own_tx = c.db.autocommit();
        if own_tx {
            exec_simple(&self.cx(), &c.db, "BEGIN")?;
        }
        reply_begin(self.sh, w, &self.job.ctl);
        w.cstr("result");
        let r = schema::write(&c.db, w);
        let err = r.err().map(|rc| E::sqlite(Some(&c.db), rc));
        if own_tx && c.in_tx() {
            c.db.exec("ROLLBACK");
        }
        if let Some(e) = err {
            return Err(e);
        }
        reply_in_tx(w, c);
        Ok(Done::Reply)
    }

    // -- streams --------------------------------------------------------------

    fn stream(&self, c: &mut Conn, w: &mut W) -> R {
        self.check_tx(c)?;
        let m = &self.job.msg;
        let chunk = m.int_or("chunk_rows", 1000);
        if chunk < 1 {
            return Err(E::builtin("ValueError", "chunk_rows must be at least 1"));
        }
        let id = self.job.ctl.id.unwrap_or(0);
        let mut s = Stream::new(id, m.int_or("credit", 1));
        s.chunk_rows = chunk;
        s.time_limit = m.int_field("time_limit_ms", 0).map(|v| v as f64);
        s.max_bytes = m.int_field("max_result_bytes", 0);
        self.register_stream(c, id);
        let r = pump(&self.cx(), &self.job.msg, &c.db, &mut s, w);
        self.keep_or_drop(c, s, &r);
        r
    }

    fn register_stream(&self, c: &mut Conn, id: i64) {
        let lane = lane_of_conn(self.sh, c.id);
        lock(&self.sh.mu).streams.insert(id, StreamMeta { lane });
        c.streams.remove(&id); // an older stream with this id
    }

    /// After a pump: keep the stream while it is paused (its end not yet
    /// sent), else forget it.
    fn keep_or_drop(&self, c: &mut Conn, s: Stream, r: &R) {
        if r.is_ok() && !s.ended {
            c.streams.insert(s.id, s);
        } else {
            let id = s.id;
            drop(s);
            lock(&self.sh.mu).streams.remove(&id);
        }
    }

    fn export(&self, c: &mut Conn, w: &mut W) -> R {
        self.check_tx(c)?;
        let id = self.job.ctl.id.unwrap_or(0);
        let mut s = export::new_stream(&self.job.msg, id)?;
        if s.ex.as_ref().unwrap().unlisted() {
            // fifo: the whole export runs in this request; never paused.
            return export::run_fifo(&self.cx(), self.sh, &self.job.msg, &c.db, &mut s, w).map(|_| Done::Reply);
        }
        self.register_stream(c, id);
        let r = export::pump(&self.cx(), self.sh, &self.job.msg, &c.db, &mut s, w).map(|_| Done::Sent);
        self.keep_or_drop(c, s, &r);
        r
    }
}


fn lane_of_conn(sh: &Shared, conn: i64) -> u64 {
    lock(&sh.mu).conns.get(&conn).map(|m| m.lane).unwrap_or(MAIN_LANE)
}

/// _SqliteStream._next_chunk: Ok(true) rows written, Ok(false) end.
fn next_chunk(cx: &Ctx, msg: &Value, db: &Db, s: &mut Stream, w: &mut W) -> Result<bool, E> {
    let start = now_ns();
    let ms = s.time_limit.map(|r| if r > 0.0 { r as i64 } else { 0 });
    let (n, check) = cx.limit(ms);
    let progress = db.progress(n, &check);
    let r = (|| {
        cx.enter()?;
        let r = (|| {
            if s.time_limit.is_some_and(|r| r <= 0.0) {
                return Err(E::interrupted());
            }
            if !s.started {
                s.started = true;
                s.st = prepare(db, msg.get("sql"))?;
                bind_params(db, s.st.as_mut(), msg.get("params"))?;
                s.ncols = s.st.as_ref().map(|x| x.column_count()).unwrap_or(0);
                if let Some(st) = s.st.as_mut() {
                    let rc = st.step();
                    if !step_ok(rc) {
                        return Err(E::sqlite(Some(db), rc));
                    }
                    s.has_row = rc == ffi::SQLITE_ROW;
                }
                if s.ncols == 0 {
                    return Ok(false);
                }
            }
            let mut count = 0;
            let mut size = 0;
            let Some(st) = s.st.as_mut() else { return Ok(false) };
            while count < s.chunk_rows && s.has_row {
                if count == 0 {
                    w.cstr("rows");
                    w.array_open();
                }
                emit_row(st, s.ncols, w, true, &mut size)?;
                count += 1;
                let rc = st.step();
                if !step_ok(rc) {
                    return Err(E::sqlite(Some(db), rc));
                }
                s.has_row = rc == ffi::SQLITE_ROW;
                if !w.ok() {
                    return Err(wbuf_error(w));
                }
            }
            if count > 0 {
                w.end();
            }
            if count > 0
                && let Some(max) = s.max_bytes
            {
                s.bytes += size;
                if s.bytes > max {
                    return Err(too_large(max));
                }
            }
            Ok(count > 0)
        })();
        cx.leave();
        r
    })();
    drop(progress);
    if let Some(rem) = s.time_limit.as_mut() {
        *rem -= (now_ns() - start) as f64 / 1e6;
    }
    r
}

/// Server._pump: send up to `credit` frames, then leave the cursor open.
fn pump(cx: &Ctx, msg: &Value, db: &Db, s: &mut Stream, w: &mut W) -> R {
    while s.credit > 0 {
        if cx.cancelled() {
            return Err(E::new("QueryInterrupted", "query cancelled"));
        }
        w.start(cx.sh.codec(), cx.sh.max_frame());
        w.map_open();
        w.cstr("id");
        w.int(s.id);
        let more = next_chunk(cx, msg, db, s, w)?;
        if s.first {
            write_columns(w, s.st.as_ref(), s.ncols);
            s.first = false;
        }
        if !more {
            s.has_row = false;
            s.ended = true;
            s.st = None;
            w.cstr("end");
            w.bool(true);
            w.end();
            if !w.ok() {
                return Err(wbuf_error(w));
            }
            cx.sh.send(w);
            return Ok(Done::Sent);
        }
        s.credit -= 1;
        w.end();
        if !w.ok() {
            return Err(wbuf_error(w));
        }
        cx.sh.send(w);
    }
    Ok(Done::Sent)
}

const SQL_LIMITS: [(&str, i32); 12] = [
    ("attached", ffi::LIMIT_ATTACHED),
    ("column", ffi::LIMIT_COLUMN),
    ("compound_select", ffi::LIMIT_COMPOUND_SELECT),
    ("expr_depth", ffi::LIMIT_EXPR_DEPTH),
    ("function_arg", ffi::LIMIT_FUNCTION_ARG),
    ("length", ffi::LIMIT_LENGTH),
    ("like_pattern_length", ffi::LIMIT_LIKE_PATTERN_LENGTH),
    ("sql_length", ffi::LIMIT_SQL_LENGTH),
    ("trigger_depth", ffi::LIMIT_TRIGGER_DEPTH),
    ("variable_number", ffi::LIMIT_VARIABLE_NUMBER),
    ("vdbe_op", ffi::LIMIT_VDBE_OP),
    ("worker_threads", ffi::LIMIT_WORKER_THREADS),
];

fn os_error_text(e: &std::io::Error) -> String {
    // "No such file or directory (os error 2)" -> strerror text
    let s = e.to_string();
    match s.rfind(" (os error") {
        Some(i) => s[..i].to_string(),
        None => s,
    }
}

const CONN_TYPES: [&str; 12] = [
    "close",
    "execute",
    "execute_many",
    "batch",
    "stream",
    "begin",
    "commit",
    "rollback",
    "op",
    "schema",
    "schema_version",
    "export",
];

fn is_conn_type(t: Option<&Value>) -> bool {
    t.is_some_and(|t| CONN_TYPES.iter().any(|c| t.is_str(c)))
}

/// Server.translate: a cancelled request's error says why it was cancelled.
fn translate(job: &Job, e: &mut E) {
    if !job.ctl.cancelled.load(SeqCst) || e.ty.is_empty() {
        return;
    }
    // A lock wait cut short by the cancel (busy handler) ends in SQLITE_BUSY.
    let busy = (e.ext & 0xff) == ffi::SQLITE_BUSY;
    if e.ty != "QueryInterrupted" && !busy {
        return;
    }
    let (ty, msg) = match job.ctl.reason.load(SeqCst) {
        R_DEADLINE => ("TransactionExpired", "transaction passed its deadline and was rolled back by the worker"),
        R_TIME_LIMIT => ("QueryInterrupted", "query interrupted (time limit exceeded)"),
        _ => ("QueryInterrupted", "query cancelled"),
    };
    *e = E::new(ty, msg);
}

fn send_error(sh: &Shared, w: &mut W, job: &Job, e: &E, in_tx: bool) {
    w.start(sh.codec(), sh.max_frame());
    w.map_open();
    w.cstr("id");
    match job.ctl.id {
        Some(i) => w.int(i),
        None => w.nil(),
    }
    w.cstr("error");
    w.map_open();
    w.cstr("type");
    w.cstr(e.ty);
    w.cstr("message");
    w.str_lossy(e.msg.as_bytes());
    if e.builtin {
        w.cstr("builtin");
        w.bool(true);
    } else {
        if let Some(code) = &e.code {
            w.cstr("code");
            w.cstr(code);
        }
        if let Some((ct, cm)) = &e.cause {
            w.cstr("cause");
            w.map(2);
            w.cstr("type");
            w.cstr(ct);
            w.cstr("message");
            w.str_lossy(cm.as_bytes());
            w.end();
        }
    }
    w.end();
    if in_tx {
        w.cstr("in_tx");
        w.bool(true);
    }
    if e.fatal {
        w.cstr("fatal");
        w.bool(true);
    }
    w.end();
    sh.send(w);
}

/// Server._expire: the deadline passed; roll back and tell the parent.
fn run_expire(sh: &Shared, local: &mut Local, job: &Job) {
    let ok = {
        let mut st = lock(&sh.mu);
        match st.conns.get_mut(&job.exp_conn) {
            Some(m) if m.tx == job.exp_tx => {
                m.tx = 0;
                true
            }
            _ => false,
        }
    };
    if !ok {
        return;
    }
    let Some(c) = local.conns.get(&job.exp_conn) else { return };
    if c.in_tx() {
        let cx = Ctx { sh, ctl: &job.ctl };
        if let Err(e) = exec_simple(&cx, &c.db, "ROLLBACK") {
            eprintln!("dblayer.worker: rollback at deadline failed: {}", e.msg);
        }
    }
    let w = &mut local.w;
    w.start(sh.codec(), sh.max_frame());
    w.map(3);
    w.cstr("type");
    w.cstr("expired");
    w.cstr("conn");
    w.int(job.exp_conn);
    w.cstr("tx");
    w.cstr(&format!("t{}", job.exp_tx));
    w.end();
    sh.send(w);
}

/// Remove a stream of this lane wherever it is (a failed stream request or
/// `more`): a stream left behind keeps its statement active, and SQLite
/// clears an interrupt only once no statement is active.
fn drop_stream(sh: &Shared, local: &mut Local, id: i64) {
    for c in local.conns.values_mut() {
        c.streams.remove(&id);
    }
    lock(&sh.mu).streams.remove(&id);
}

fn find_stream(local: &Local, id: i64) -> Option<i64> {
    local.conns.values().find(|c| c.streams.contains_key(&id)).map(|c| c.id)
}

fn h_more(sh: &Shared, local: &mut Local, job: &Job) -> R {
    let id = job.ctl.id.unwrap_or(0);
    let Some(cid) = find_stream(local, id) else { return Ok(Done::Sent) }; // finished or closed meanwhile
    job.ctl.conn.store(cid, SeqCst);
    let Local { conns, w, .. } = local;
    let c = conns.get_mut(&cid).unwrap();
    let mut s = c.streams.remove(&id).unwrap();
    s.credit += job.credit;
    let run = Run { sh, job };
    let cx = run.cx();
    let r = if s.ex.is_some() {
        export::pump(&cx, sh, &job.msg, &c.db, &mut s, w).map(|_| Done::Sent)
    } else {
        pump(&cx, &job.msg, &c.db, &mut s, w)
    };
    run.keep_or_drop(c, s, &r);
    r
}

fn h_close_stream(sh: &Shared, local: &mut Local, job: &Job) -> R {
    let id = job.ctl.id.unwrap_or(0);
    let Some(cid) = find_stream(local, id) else { return Ok(Done::Sent) }; // its last frame was already sent
    job.ctl.conn.store(cid, SeqCst);
    drop_stream(sh, local, id);
    reply_begin(sh, &mut local.w, &job.ctl);
    local.w.cstr("end");
    local.w.bool(true);
    local.w.cstr("cancelled");
    local.w.bool(true);
    Ok(Done::Reply)
}

fn close_conn(sh: &Shared, local: &mut Local, id: i64) {
    {
        let mut st = lock(&sh.mu);
        st.conns.remove(&id);
        if let Some(ln) = st.lanes.get_mut(&local.lane)
            && ln.conn == Some(id)
        {
            ln.conn = None;
        }
        if let Some(c) = local.conns.get(&id) {
            for sid in c.streams.keys() {
                st.streams.remove(sid);
            }
        }
    }
    local.conns.remove(&id); // Drop: streams, ROLLBACK, close
}

fn close_all(sh: &Shared, local: &mut Local) {
    let ids: Vec<i64> = local.conns.keys().copied().collect();
    for id in ids {
        close_conn(sh, local, id);
    }
}

fn h_sleep(sh: &Shared, local: &mut Local, job: &Job) -> R {
    if sh.opts.multi {
        // Other lanes' connections belong to their threads: stop those
        // first. One that will not stop leaves nothing to do but exit.
        let mut st = lock(&sh.mu);
        cancel_all_lanes(sh, &mut st, false);
        let (st, ok) = wait_lanes(sh, st, Instant::now() + Duration::from_secs(SHUTDOWN_GRACE_S));
        drop(st);
        if !ok {
            eprintln!("dblayer.worker: a connection did not stop; exiting");
            ffi::exit_now(0);
        }
    }
    close_all(sh, local);
    reply_ok(sh, &mut local.w, &job.ctl);
    sh.exit_requested.store(true, SeqCst);
    Ok(Done::Reply)
}

fn h_debug(sh: &Shared, local: &mut Local, job: &Job) -> R {
    let m = &job.msg;
    let action = m.get("action");
    let is = |s: &str| action.is_some_and(|a| a.is_str(s));
    if is("sleep") {
        let secs = match m.get("seconds") {
            Some(Value::Float(f)) => *f,
            Some(Value::Int(i)) => *i as f64,
            _ => 1.0,
        };
        std::thread::sleep(Duration::from_secs_f64(secs.max(0.0)));
        reply_ok(sh, &mut local.w, &job.ctl);
        return Ok(Done::Reply);
    }
    if is("oversized_frame") {
        sh.write_all(&[0x80, 0, 0, 0]);
        sh.write_all(b"xxxxxxxxxxxxxxxx");
        return Ok(Done::Sent);
    }
    if is("truncated_frame") {
        sh.write_all(&[0, 0, 0, 100]);
        sh.write_all(b"{\"id\":");
        ffi::exit_now(3);
    }
    if is("garbage") {
        let junk = b"\xff\xfe not a valid payload";
        sh.write_all(&[0, 0, 0, junk.len() as u8]);
        sh.write_all(junk);
        return Ok(Done::Sent);
    }
    if is("alloc") {
        let mb = m.int_or("mb", 0).max(0) as usize;
        let mut blocks: Vec<Vec<u8>> = Vec::new();
        let mut got = 0;
        while got < mb {
            let mut b: Vec<u8> = Vec::new();
            if b.try_reserve_exact(1 << 20).is_err() {
                break;
            }
            b.resize(1 << 20, 1);
            blocks.push(b);
            got += 1;
        }
        drop(blocks);
        if got < mb {
            let mut e = E::new("ResourceLimitExceeded", "worker ran out of memory (memory limit)");
            e.fatal = true;
            return Err(e);
        }
        reply_ok(sh, &mut local.w, &job.ctl);
        local.w.cstr("allocated_mb");
        local.w.int(got as i64);
        return Ok(Done::Reply);
    }
    if is("exit") {
        ffi::exit_now(m.int_or("code", 1) as i32);
    }
    if is("print") {
        let t = m.get("text").and_then(|v| v.as_bytes()).unwrap_or(b"");
        eprintln!("{}", String::from_utf8_lossy(t));
        reply_ok(sh, &mut local.w, &job.ctl);
        return Ok(Done::Reply);
    }
    Err(E::protocol(format!("unknown debug action '{}'", tx_text(action))))
}

fn run_job(sh: &Shared, local: &mut Local, job: &Job) {
    local.w.start(sh.codec(), sh.max_frame());
    if job.ctl.kind == Kind::Expire {
        run_expire(sh, local, job);
        return;
    }
    let ty = job.field("type");
    let is_stream_req = job.ctl.kind == Kind::Req && ty.is_some_and(|t| t.is_str("stream") || t.is_str("export"));
    let r: R = (|| {
        if job.ctl.cancelled.load(SeqCst) {
            return Err(E::new("QueryInterrupted", "query cancelled"));
        }
        match job.ctl.kind {
            Kind::More => return h_more(sh, local, job),
            Kind::CloseStream => return h_close_stream(sh, local, job),
            _ => {}
        }
        let is = |s: &str| ty.is_some_and(|t| t.is_str(s));
        if is("open") {
            return Run { sh, job }.open(local);
        }
        if is("sleep") {
            return h_sleep(sh, local, job);
        }
        if is("_debug") && sh.opts.debug {
            return h_debug(sh, local, job);
        }
        if !is_conn_type(ty) {
            let t = tx_text(ty);
            return Err(E::protocol(if matches!(ty, Some(Value::Str(_))) {
                format!("unknown message type '{t}'")
            } else {
                format!("unknown message type {t}")
            }));
        }
        let cid = {
            let st = lock(&sh.mu);
            let cid = named_conn(&st, &job.msg)?;
            // A connection is used only on its own lane's thread.
            if st.conns[&cid].lane != local.lane {
                return Err(E::protocol(format!("connection {cid} is not served by this lane")));
            }
            job.ctl.conn.store(cid, SeqCst);
            cid
        };
        if is("close") {
            job.ctl.conn.store(0, SeqCst);
            close_conn(sh, local, cid);
            reply_ok(sh, &mut local.w, &job.ctl);
            return Ok(Done::Reply);
        }
        let run = Run { sh, job };
        let Local { conns, w, .. } = local;
        let Some(c) = conns.get_mut(&cid) else { return Err(E::protocol(format!("no connection {cid} in this worker"))) };
        if is("execute") {
            run.execute(c, w)
        } else if is("execute_many") {
            run.execute_many(c, w)
        } else if is("batch") {
            run.batch(c, w)
        } else if is("stream") {
            run.stream(c, w)
        } else if is("export") {
            run.export(c, w)
        } else if is("begin") {
            run.begin(c, w)
        } else if is("commit") {
            run.commit(c, w)
        } else if is("rollback") {
            run.rollback(c, w)
        } else if is("op") {
            run.op(c)
        } else if is("schema") {
            run.schema(c, w)
        } else {
            run.schema_version(c, w)
        }
    })();
    let streaming = job.ctl.kind == Kind::More || is_stream_req;
    let mut r = r;
    if matches!(r, Ok(Done::Reply)) && !streaming && job.ctl.cancelled.load(SeqCst) {
        // cancelled while it ran: the caller gets the cancellation
        r = Err(E::new("QueryInterrupted", "query cancelled"));
    }
    if let Ok(Done::Reply) = r {
        local.w.end();
        if local.w.ok() {
            sh.send(&mut local.w);
        } else {
            r = Err(wbuf_error(&local.w));
        }
    }
    if let Err(mut e) = r {
        if e.ty.is_empty() {
            e = wbuf_error(&local.w);
        }
        translate(job, &mut e);
        let cid = job.ctl.conn.load(SeqCst);
        let in_tx = local.conns.get(&cid).is_some_and(|c| c.in_tx());
        send_error(sh, &mut local.w, job, &e, in_tx);
        if streaming {
            drop_stream(sh, local, job.ctl.id.unwrap_or(0));
        }
        if e.fatal && sh.opts.multi {
            // The whole process goes, as the parent expects after "fatal".
            ffi::exit_now(0);
        }
        if e.fatal {
            sh.exit_requested.store(true, SeqCst);
        }
    }
    if local.w.buf.capacity() > (1 << 20) {
        local.w.shrink(); // give back the memory of a big reply
    }
}

fn dequeue(sh: &Shared, lane: u64) -> Job {
    let mut st = lock(&sh.mu);
    loop {
        let ln = st.lanes.get_mut(&lane).expect("lane exists while its thread runs");
        if let Some(j) = ln.q.pop_front() {
            ln.current = Some(j.ctl.clone());
            return j;
        }
        let cond = ln.cond.clone();
        st = cond.wait(st).unwrap_or_else(|e| e.into_inner());
    }
}

fn clear_current(sh: &Shared, lane: u64) -> bool {
    let mut st = lock(&sh.mu);
    let ln = st.lanes.get_mut(&lane).unwrap();
    ln.current = None;
    ln.conn.is_none() && ln.q.is_empty()
}

/// A connection lane's thread (multi-tenant mode): runs the `open`, then the
/// connection's requests in order; exits once the connection is closed (or
/// never opened) and nothing is queued, or at shutdown.
fn lane_main(sh: Arc<Shared>, lane: u64) {
    let mut local = Local { lane, conns: HashMap::new(), w: W::new(Codec::Json, DEFAULT_MAX_FRAME) };
    loop {
        let job = dequeue(&sh, lane);
        if job.ctl.kind == Kind::Shutdown {
            clear_current(&sh, lane);
            break;
        }
        run_job(&sh, &mut local, &job);
        drop(job);
        if clear_current(&sh, lane) {
            break;
        }
    }
    close_all(&sh, &mut local); // shutdown: roll back and close our own connection
    let rest = {
        let mut st = lock(&sh.mu);
        let ln = st.lanes.remove(&lane);
        st.nthread_lanes -= 1;
        sh.lanecond.notify_all();
        ln
    };
    drop(rest); // only shutdown jobs can be left: nothing routes here now
}

/// The main lane, on the process's main thread; never returns.
pub fn main_lane(sh: Arc<Shared>) -> ! {
    let mut local = Local { lane: MAIN_LANE, conns: HashMap::new(), w: W::new(Codec::Json, DEFAULT_MAX_FRAME) };
    loop {
        let job = dequeue(&sh, MAIN_LANE);
        if job.ctl.kind == Kind::Shutdown {
            clear_current(&sh, MAIN_LANE);
            break;
        }
        run_job(&sh, &mut local, &job);
        drop(job);
        clear_current(&sh, MAIN_LANE);
        if sh.exit_requested.load(SeqCst) || (!sh.opts.multi && sh.parent_gone.load(SeqCst)) {
            break;
        }
    }
    if sh.opts.multi {
        // Each lane closes its own connection; the reader's grace period
        // exits regardless if one will not stop.
        let mut st = lock(&sh.mu);
        cancel_all_lanes(&sh, &mut st, false);
        let (st, ok) = wait_lanes(&sh, st, Instant::now() + Duration::from_secs(SHUTDOWN_GRACE_S));
        drop(st);
        if !ok {
            ffi::exit_now(0);
        }
    }
    close_all(&sh, &mut local);
    ffi::exit_now(0);
}

// -- watchdog -----------------------------------------------------------------

/// [mu] Server._on_deadline
fn on_deadline(sh: &Shared, st: &mut State, conn: i64) {
    let (tx, lane) = {
        let m = st.conns.get_mut(&conn).unwrap();
        m.deadline_ns = 0;
        if m.expired.len() == MAX_EXPIRED {
            m.expired.pop_front();
        }
        m.expired.push_back(m.tx);
        (m.tx, m.lane)
    };
    if let Some(cur) = st.lanes.get(&lane).and_then(|l| l.current.clone())
        && cur.conn.load(SeqCst) == conn
    {
        cancel_job(sh, st, &cur, R_DEADLINE);
    }
    let mut x = Job::new(Kind::Expire, None, Value::Nil);
    x.exp_conn = conn;
    x.exp_tx = tx;
    enqueue(st, lane, x, true);
}

pub fn watchdog_main(sh: Arc<Shared>) {
    let mut st = lock(&sh.mu);
    loop {
        let now = now_ns();
        let mut next = now + 3_600_000_000_000;
        let due: Vec<i64> = st
            .conns
            .iter()
            .filter(|(_, m)| m.tx != 0 && m.deadline_ns != 0)
            .filter_map(|(id, m)| {
                if now >= m.deadline_ns {
                    Some(*id)
                } else {
                    next = next.min(m.deadline_ns);
                    None
                }
            })
            .collect();
        for id in due {
            on_deadline(&sh, &mut st, id);
        }
        let currents: Vec<Arc<Ctl>> = st.lanes.values().filter_map(|l| l.current.clone()).collect();
        for j in currents {
            let b = j.backstop_ns.load(SeqCst);
            if b != 0 {
                if now >= b {
                    j.backstop_ns.store(0, SeqCst);
                    cancel_job(&sh, &st, &j, R_TIME_LIMIT);
                } else {
                    next = next.min(b);
                }
            }
            if j.cancelled.load(SeqCst)
                && j.active.load(SeqCst)
                && let Some(m) = st.conns.get(&j.conn.load(SeqCst))
            {
                if now >= j.next_interrupt_ns.load(SeqCst) {
                    m.intr.interrupt();
                    j.next_interrupt_ns.store(now + REINTERRUPT_NS, SeqCst);
                }
                next = next.min(j.next_interrupt_ns.load(SeqCst));
            }
        }
        let wait = (next - now_ns()).max(0) as u64;
        st = sh.wcond.wait_timeout(st, Duration::from_nanos(wait)).unwrap_or_else(|e| e.into_inner()).0;
    }
}

// -- reader ---------------------------------------------------------------------

fn hello(sh: &Shared, msg: &Value) {
    let mut codec = Codec::Json;
    if let Some(Value::Arr(cs)) = msg.get("codecs") {
        for c in cs {
            if c.is_str("msgpack") {
                codec = Codec::Msgpack;
                break;
            }
            if c.is_str("json") {
                codec = Codec::Json;
                break;
            }
        }
    }
    if let Some(m) = msg.int_field("max_frame_bytes", 0)
        && m > 0
    {
        sh.max_frame.store(m as usize, SeqCst);
    }
    let mut w = W::new(Codec::Json, sh.max_frame());
    w.map_open();
    w.cstr("id");
    match msg.get("id") {
        Some(v) => w.val(v),
        None => w.nil(),
    }
    w.cstr("type");
    w.cstr("hello");
    w.cstr("protocol");
    w.int(PROTOCOL_VERSION);
    w.cstr("codec");
    w.cstr(if codec == Codec::Msgpack { "msgpack" } else { "json" });
    w.cstr("sqlite_version");
    w.cstr(&ffi::libversion());
    w.cstr("implementation");
    w.cstr("rust");
    w.cstr("pid");
    w.int(std::process::id() as i64);
    w.cstr("rlimit_as");
    w.bool(sh.rlimit_as.load(SeqCst));
    w.cstr("ops");
    w.bool(false);
    w.cstr("export");
    export::hello(&mut w);
    if sh.opts.multi {
        // Requests for different connections run concurrently, so the
        // parent may have many in flight (one per connection).
        w.cstr("multi");
        w.bool(true);
        w.cstr("max_connections");
        w.int(sh.opts.max_conns as i64);
        w.cstr("threading");
        w.cstr("multi-thread");
    }
    w.end();
    sh.send(&mut w);
    sh.codec.store(if codec == Codec::Msgpack { 1 } else { 0 }, SeqCst);
}

fn reply_error_now(sh: &Shared, id: Option<i64>, ty: &str, msg: &str) {
    let mut w = W::new(sh.codec(), sh.max_frame());
    w.map_open();
    w.cstr("id");
    match id {
        Some(i) => w.int(i),
        None => w.nil(),
    }
    w.cstr("error");
    w.map(2);
    w.cstr("type");
    w.cstr(ty);
    w.cstr("message");
    w.cstr(msg);
    w.end();
    w.end();
    sh.send(&mut w);
}

/// Multi-tenant `open`: a new lane and thread, which runs the open and then
/// the connection's requests. Refused past --max-connections.
fn open_lane(sh: &Arc<Shared>, job: Job) {
    let mut st = lock(&sh.mu);
    if st.nthread_lanes >= sh.opts.max_conns || st.eof {
        let eof = st.eof;
        drop(st);
        let msg = format!("worker connection cap reached: {} connections open (--max-connections)", sh.opts.max_conns);
        if eof {
            reply_error_now(sh, job.ctl.id, "WorkerCrashed", "worker is shutting down");
        } else {
            reply_error_now(sh, job.ctl.id, "ResourceLimitExceeded", &msg);
        }
        return;
    }
    st.lane_seq += 1;
    let lane = st.lane_seq;
    let id = job.ctl.id;
    let mut q = VecDeque::new();
    q.push_back(job);
    st.lanes.insert(lane, Lane { is_main: false, q, current: None, conn: None, cond: Arc::new(Condvar::new()) });
    let sh2 = sh.clone();
    // A lane starts under the process's RLIMIT_AS: ffi::spawn_detached, so
    // a thread that cannot be had is this open's ResourceLimitExceeded,
    // never an abort of every tenant (as std::thread could; see there).
    let spawned = crate::ffi::spawn_detached(LANE_STACK_BYTES, Box::new(move || lane_main(sh2, lane)));
    match spawned {
        Ok(_) => st.nthread_lanes += 1,
        Err(e) => {
            st.lanes.remove(&lane);
            drop(st);
            reply_error_now(sh, id, "ResourceLimitExceeded", &format!("cannot start a connection thread: {e}"));
        }
    }
}

fn dispatch(sh: &Arc<Shared>, msg: Value) {
    let id = match msg.get("id") {
        Some(Value::Int(i)) => Some(*i),
        _ => None,
    };
    let ty = msg.get("type");
    let is = |s: &str| ty.is_some_and(|t| t.is_str(s));
    if is("hello") {
        hello(sh, &msg);
        return;
    }
    if is("cancel") {
        let target = match msg.get("target") {
            Some(Value::Int(i)) => *i,
            _ => i64::MIN,
        };
        let mut st = lock(&sh.mu);
        match lane_of_request(&st, target) {
            Some((_, ctl, true)) => cancel_job(sh, &st, &ctl, R_CANCELLED),
            Some((_, ctl, false)) => ctl.cancelled.store(true, SeqCst),
            None => {}
        }
        // An open stream is also closed, whether or not a job was found
        // (the C worker's fix, worker.c): the job that last pumped the
        // stream stays its lane's current job from sending its last frame
        // until the lane clears it, and a cancel arriving in that window
        // only marked the finished job, so the stream stayed open with no
        // reply and its statement active (a stale interrupt then failed the
        // connection's next statement). A job still pumping fails and drops
        // the stream first; this close then finds nothing and sends nothing.
        if let Some(lane) = st.streams.get(&target).map(|s| s.lane) {
            enqueue(&mut st, lane, Job::new(Kind::CloseStream, Some(target), Value::Nil), false);
        }
        return;
    }
    if is("more") {
        let sid = msg.int_or("stream", 0);
        let mut m = Job::new(Kind::More, Some(sid), Value::Nil);
        m.credit = msg.int_or("credit", 1);
        let mut st = lock(&sh.mu);
        // The stream's lane: where its cursor is, or where the request that
        // opens it still runs or waits.
        let lane = st
            .streams
            .get(&sid)
            .map(|s| s.lane)
            .or_else(|| lane_of_request(&st, sid).map(|(l, _, _)| l))
            .unwrap_or(MAIN_LANE);
        enqueue(&mut st, lane, m, false);
        return;
    }
    let is_open = is("open");
    let job = Job::new(Kind::Req, id, msg);
    if sh.opts.multi && is_open {
        open_lane(sh, job);
        return;
    }
    let mut st = lock(&sh.mu);
    let mut lane = MAIN_LANE;
    let t = job.field("type");
    if sh.opts.multi && (is_conn_type(t) || (t.is_some_and(|t| t.is_str("_debug")) && job.field("conn").is_some())) {
        lane = named_conn(&st, &job.msg).ok().and_then(|c| st.conns.get(&c)).map(|m| m.lane).unwrap_or(MAIN_LANE);
    }
    enqueue(&mut st, lane, job, false);
}

fn on_eof(sh: &Shared) {
    let mut st = lock(&sh.mu);
    if !st.eof {
        st.eof = true;
        cancel_all_lanes(sh, &mut st, true);
    }
}

/// Read one frame: Ok(None) at a clean end of input.
fn read_frame(input: &mut impl Read, cap: usize) -> Result<Option<Vec<u8>>, String> {
    let mut hdr = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        match input.read(&mut hdr[got..]) {
            Ok(0) => return if got == 0 { Ok(None) } else { Err("truncated frame header".into()) },
            Ok(k) => got += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.to_string()),
        }
    }
    let n = u32::from_be_bytes(hdr) as usize;
    if n > cap {
        return Err(format!("frame of {n} bytes exceeds the {cap}-byte frame size cap; dropping the connection"));
    }
    let mut buf = Vec::new();
    if buf.try_reserve_exact(n).is_err() {
        return Err("out of memory reading a frame; dropping the connection".into());
    }
    buf.resize(n, 0);
    input.read_exact(&mut buf).map_err(|_| "truncated frame".to_string())?;
    Ok(Some(buf))
}

pub fn reader_main(sh: Arc<Shared>, mut input: impl Read) -> ! {
    {
        let (m, c) = &sh.started;
        let mut g = m.lock().unwrap();
        while !*g {
            g = c.wait(g).unwrap();
        }
    }
    loop {
        match read_frame(&mut input, sh.max_frame()) {
            Ok(Some(payload)) => match codec::decode_msg(sh.codec(), &payload) {
                Ok(msg) => dispatch(&sh, msg),
                Err(e) => {
                    eprintln!("dblayer.worker: bad input ({e}); dropping the connection");
                    break;
                }
            },
            Ok(None) => break,
            Err(e) => {
                eprintln!("dblayer.worker: {e}");
                break;
            }
        }
    }
    on_eof(&sh);
    // Exit even if a statement ignores every interrupt.
    std::thread::sleep(Duration::from_secs(SHUTDOWN_GRACE_S));
    ffi::exit_now(0);
}

#[cfg(test)]
pub fn frame_for_test(input: &[u8], cap: usize) -> Result<Option<Vec<u8>>, String> {
    read_frame(&mut std::io::Cursor::new(input), cap)
}
