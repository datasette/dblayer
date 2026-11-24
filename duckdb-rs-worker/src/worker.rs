//! The worker: frames on stdin and stdout, one DuckDB instance per database
//! file shared by many connections, and a thread per connection.
//!
//! Threads
//! -------
//! * The **main thread** reads and decodes requests. It answers `hello`,
//!   `open`, `sleep` and the control messages (`cancel`, `more`) itself and
//!   hands every connection-scoped request to that connection's thread, in
//!   arrival order. So `cancel` is acted on while a statement runs.
//! * **One thread per connection** runs its requests one at a time. Two
//!   connections of the same database run at the same time (the Python
//!   worker serves one request at a time per process), so a point read on
//!   one connection is not held up by a long query on another.
//! * **The timer thread** runs time limits, repeated interrupts and
//!   transaction deadlines.
//!
//! Replies are written under one lock, a frame at a time.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::convert::{self, Param};
use crate::errors::{self, DuckError};
use crate::ffi::{self, Connection, Ctx, Database, InterruptHandle, Prepared, QueryResult, ReturnType};
use crate::schema;
use crate::sql;
use crate::timer::Timer;
use crate::types::TyNode;
use crate::value::Value;
use crate::{json, msgpack};

pub const PROTOCOL_VERSION: i128 = 1;
pub const DEFAULT_MAX_FRAME_BYTES: u64 = 256 * 1024 * 1024;
const FETCH_BATCH: usize = 1024;
const REINTERRUPT: Duration = Duration::from_millis(5);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// A connection thread's stack: std::thread's default, kept when these
/// threads moved to ffi::spawn_raw.
const CONN_STACK_BYTES: usize = 2 * 1024 * 1024;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// -- output ------------------------------------------------------------------------

pub struct Out {
    file: Mutex<File>,
    /// 0 json, 1 msgpack.
    codec: AtomicU8,
    pub max_frame: AtomicU64,
}

impl Out {
    pub fn new(file: File) -> Out {
        Out { file: Mutex::new(file), codec: AtomicU8::new(0), max_frame: AtomicU64::new(DEFAULT_MAX_FRAME_BYTES) }
    }

    fn encode(&self, msg: &Value) -> Vec<u8> {
        if self.codec.load(Ordering::Relaxed) == 1 { msgpack::encode(msg) } else { json::encode(msg) }
    }

    /// Encode and write one frame. A reply over the frame cap is replaced
    /// by a ResourceLimitExceeded error for the same request. Write errors
    /// (the parent has gone) are ignored: end of input follows.
    pub fn send(&self, msg: &Value) {
        let mut payload = self.encode(msg);
        let cap = self.max_frame.load(Ordering::Relaxed);
        if payload.len() as u64 > cap {
            let id = msg.get("id").cloned().unwrap_or(Value::Null);
            let err = Value::obj(vec![
                ("id", id),
                (
                    "error",
                    Value::obj(vec![
                        ("type", Value::str("ResourceLimitExceeded")),
                        (
                            "message",
                            Value::Str(format!(
                                "reply too large: frame of {} bytes exceeds the {cap}-byte frame size cap",
                                payload.len()
                            )),
                        ),
                    ]),
                ),
            ]);
            payload = self.encode(&err);
        }
        self.write_frame(&payload);
    }

    pub fn write_frame(&self, payload: &[u8]) {
        let mut f = lock(&self.file);
        let _ = f.write_all(&(payload.len() as u32).to_be_bytes());
        let _ = f.write_all(payload);
        let _ = f.flush();
    }

    pub fn write_raw(&self, bytes: &[u8]) {
        let mut f = lock(&self.file);
        let _ = f.write_all(bytes);
        let _ = f.flush();
    }
}

// -- errors ------------------------------------------------------------------------

/// Why a request failed, before it is put on the wire.
#[derive(Debug)]
pub enum Fail {
    /// Raised by DuckDB (or as the Python client raises it): mapped by
    /// `DuckDBDialect.map_exception`, with the driver error as the cause.
    Duck(DuckError),
    /// A dblayer exception raised by the backend itself (no driver cause).
    Db { class: &'static str, message: String, code: Option<String> },
    /// A builtin Python exception (`ValueError`, ...), raised as itself.
    Builtin { class: &'static str, message: String },
}

pub fn qerr(message: impl Into<String>, code: &str) -> Fail {
    Fail::Db { class: "QueryError", message: message.into(), code: Some(code.to_string()) }
}

fn dberr(class: &'static str, message: impl Into<String>) -> Fail {
    Fail::Db { class, message: message.into(), code: None }
}

fn interrupted(message: &str) -> Fail {
    dberr("QueryInterrupted", message)
}

impl From<DuckError> for Fail {
    fn from(e: DuckError) -> Fail {
        Fail::Duck(e)
    }
}

impl From<convert::ConvError> for Fail {
    fn from(e: convert::ConvError) -> Fail {
        Fail::Builtin { class: "ValueError", message: e.0 }
    }
}

impl Fail {
    fn is_interrupt(&self) -> bool {
        match self {
            Fail::Duck(e) => e.kind == errors::INTERRUPT,
            Fail::Db { class, .. } => *class == "QueryInterrupted",
            Fail::Builtin { .. } => false,
        }
    }

    fn is_duck(&self) -> bool {
        matches!(self, Fail::Duck(_))
    }
}

fn cause_of(e: &DuckError) -> Value {
    Value::obj(vec![("type", Value::Str(format!("_duckdb.{}", e.class()))), ("message", Value::str(&e.message))])
}

/// `DuckDBDialect.map_exception` plus the worker's `error_payload`.
fn map_duck(e: &DuckError) -> Value {
    let msg = e.message.clone();
    let (class, message, code): (&str, String, Option<String>) = match e.kind {
        errors::INTERRUPT => ("QueryInterrupted", "query interrupted (time limit exceeded or cancelled)".into(), None),
        errors::CONSTRAINT => ("IntegrityError", msg, Some(e.code().into())),
        errors::OUT_OF_MEMORY => ("ResourceLimitExceeded", msg, None),
        errors::CONNECTION => ("QueryError", format!("worker error: {}: {msg}", e.class()), Some("WORKER".into())),
        errors::TRANSACTION => {
            let lower = msg.to_lowercase();
            if msg.contains("Failed to commit") && lower.contains("constraint") {
                ("IntegrityError", msg, Some("Transaction".into()))
            } else if lower.contains("conflict") {
                ("WriteConflict", msg, None)
            } else if msg.contains("transaction is aborted") {
                ("QueryError", msg, Some("TRANSACTION_ABORTED".into()))
            } else {
                ("QueryError", msg, Some(e.code().into()))
            }
        }
        _ => ("QueryError", msg, Some(e.code().into())),
    };
    let mut err = Value::obj(vec![("type", Value::str(class)), ("message", Value::Str(message))]);
    if let Some(c) = code {
        err.set("code", Value::Str(c));
    }
    err.set("cause", cause_of(e));
    err
}

/// The `error` object of a reply. `cancel_reason`: the request was stopped
/// by the worker (`Server.translate`): an interrupt it caused is reported
/// by its reason.
fn error_payload(f: &Fail, cancel_reason: Option<&str>) -> Value {
    if let Some(reason) = cancel_reason
        && f.is_interrupt()
    {
        let (class, message) = match reason {
            "deadline" => ("TransactionExpired", "transaction passed its deadline and was rolled back by the worker"),
            "time_limit" => ("QueryInterrupted", "query interrupted (time limit exceeded)"),
            _ => ("QueryInterrupted", "query cancelled"),
        };
        let cause = match f {
            Fail::Duck(e) => cause_of(e),
            _ => Value::obj(vec![
                ("type", Value::str("__main__._Interrupted")),
                ("message", Value::Str(format!("request stopped by the worker ({reason})"))),
            ]),
        };
        return Value::obj(vec![("type", Value::str(class)), ("message", Value::str(message)), ("cause", cause)]);
    }
    match f {
        Fail::Duck(e) => map_duck(e),
        Fail::Db { class, message, code } => {
            let mut err = Value::obj(vec![("type", Value::str(*class)), ("message", Value::str(message))]);
            if let Some(c) = code {
                err.set("code", Value::str(c));
            }
            err
        }
        Fail::Builtin { class, message } => {
            Value::obj(vec![("type", Value::str(*class)), ("message", Value::str(message)), ("builtin", Value::Bool(true))])
        }
    }
}

// -- jobs and shared connection state ----------------------------------------------

pub struct JobCtl {
    pub id: Value,
    cancelled: AtomicBool,
    reason: Mutex<&'static str>,
}

impl JobCtl {
    fn new(id: Value) -> Arc<JobCtl> {
        Arc::new(JobCtl { id, cancelled: AtomicBool::new(false), reason: Mutex::new("cancelled") })
    }

    fn cancel(&self, reason: &'static str) {
        *lock(&self.reason) = reason;
        self.cancelled.store(true, Ordering::SeqCst);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    fn reason(&self) -> Option<&'static str> {
        if self.is_cancelled() { Some(*lock(&self.reason)) } else { None }
    }
}

enum Work {
    Request(Value),
    More(i64),
    CloseStream,
    /// A transaction deadline passed: roll back (no reply).
    Expire,
    /// Close the connection; reply unless `sleep` or shutdown asked.
    Close { reply: bool, done: Option<Sender<()>> },
}

struct Job {
    ctl: Arc<JobCtl>,
    work: Work,
}

#[derive(Default)]
struct CState {
    closed: bool,
    current: Option<Arc<JobCtl>>,
    /// The statement running now (0: none), and the last number given out.
    active: u64,
    seq: u64,
    /// The dblayer transaction open on this connection.
    tx: Option<String>,
    /// The transaction whose deadline timer is armed (cleared by commit and
    /// rollback before they start, so a late timer does nothing).
    deadline_tx: Option<String>,
    expired: Vec<String>,
    expire_pending: Option<String>,
}

struct ConnShared {
    state: Mutex<CState>,
    handle: Mutex<InterruptHandle>,
    sender: Sender<Job>,
}

/// State shared by every thread.
struct Globals {
    out: Arc<Out>,
    timer: Arc<Timer>,
    /// Requests queued or running, by id: for `cancel`.
    jobs: Mutex<HashMap<String, (i64, Arc<JobCtl>)>>,
    /// Open (paused) streams, by id: their connection.
    streams: Mutex<HashMap<String, i64>>,
    dbs: Mutex<HashMap<String, DbEntry>>,
}

/// Interrupt statement `stmt` of a connection, again every few ms until it
/// returns: DuckDB drops an interrupt that arrives before a statement has
/// really started, and an interrupt of an idle connection does nothing.
fn interrupt_while_active(shared: &Arc<ConnShared>, timer: &Arc<Timer>, stmt: u64) {
    {
        let st = lock(&shared.state);
        if st.closed || st.active != stmt || stmt == 0 {
            return;
        }
        lock(&shared.handle).interrupt();
    }
    let (s, t) = (Arc::clone(shared), Arc::clone(timer));
    timer.schedule(REINTERRUPT, move || interrupt_while_active(&s, &t, stmt));
}

/// Stop the request `ctl` running or queued on `shared` (reason
/// "cancelled", "deadline", "time_limit").
fn cancel_job(shared: &Arc<ConnShared>, timer: &Arc<Timer>, ctl: &Arc<JobCtl>, reason: &'static str) {
    ctl.cancel(reason);
    let stmt = {
        let st = lock(&shared.state);
        match &st.current {
            Some(c) if Arc::ptr_eq(c, ctl) => st.active,
            _ => 0,
        }
    };
    if stmt != 0 {
        interrupt_while_active(shared, timer, stmt);
    }
}

// -- the server (main thread) ---------------------------------------------------------

struct ConnHandle {
    shared: Arc<ConnShared>,
    thread: ffi::RawThread,
}

pub struct Server {
    g: Arc<Globals>,
    debug: bool,
    rlimit_as: bool,
    conns: HashMap<i64, ConnHandle>,
    next_conn: i64,
    closed_threads: Vec<ffi::RawThread>,
}

pub struct ReadError;

/// Read one frame: `Ok(None)` at a clean end of input.
pub(crate) fn read_frame(input: &mut impl Read, cap: u64) -> Result<Option<Vec<u8>>, ReadError> {
    let mut header = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        match input.read(&mut header[got..]) {
            Ok(0) => return if got == 0 { Ok(None) } else { Err(ReadError) },
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(ReadError),
        }
    }
    let n = u32::from_be_bytes(header) as u64;
    if n > cap {
        eprintln!("dblayer-duckdb-rsworker: frame of {n} bytes exceeds the {cap}-byte frame size cap; dropping the connection");
        return Err(ReadError);
    }
    let mut buf = vec![0u8; n as usize];
    input.read_exact(&mut buf).map_err(|_| ReadError)?;
    Ok(Some(buf))
}

impl Server {
    pub fn new(out: File, debug: bool, rlimit_as: bool) -> Server {
        let g = Arc::new(Globals {
            out: Arc::new(Out::new(out)),
            timer: Timer::start(),
            jobs: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            dbs: Mutex::new(HashMap::new()),
        });
        Server { g, debug, rlimit_as, conns: HashMap::new(), next_conn: 1, closed_threads: Vec::new() }
    }

    pub fn set_rlimit_as(&mut self, on: bool) {
        self.rlimit_as = on;
    }

    pub fn serve(&mut self, input: &mut impl Read) {
        loop {
            let cap = self.g.out.max_frame.load(Ordering::Relaxed);
            let payload = match read_frame(input, cap) {
                Ok(Some(p)) => p,
                Ok(None) | Err(ReadError) => break,
            };
            let decoded = if self.g.out.codec.load(Ordering::Relaxed) == 1 {
                msgpack::decode(&payload)
            } else {
                json::decode(&payload)
            };
            let msg = match decoded {
                Ok(m @ Value::Map(_)) => m,
                Ok(_) => {
                    eprintln!("dblayer-duckdb-rsworker: bad input (frame is not a message); dropping the connection");
                    break;
                }
                Err(e) => {
                    eprintln!("dblayer-duckdb-rsworker: bad input ({e}); dropping the connection");
                    break;
                }
            };
            if !self.dispatch(msg) {
                break;
            }
        }
        self.shutdown();
    }

    fn reply_error(&self, id: Value, f: &Fail) {
        self.g.out.send(&Value::obj(vec![("id", id), ("error", error_payload(f, None))]));
    }

    /// Handle one message; false to stop serving.
    fn dispatch(&mut self, msg: Value) -> bool {
        let id = msg.get("id").cloned().unwrap_or(Value::Null);
        let kind = msg.get("type").and_then(Value::as_str).unwrap_or("").to_string();
        match kind.as_str() {
            "hello" => self.hello(&msg),
            "cancel" => self.cancel(&msg),
            "more" => {
                let Some(sid) = msg.get("stream") else { return true };
                let credit = msg.get("credit").and_then(Value::as_int_lossy).unwrap_or(1);
                let key = sid.key();
                let conn = lock(&self.g.streams).get(&key).copied();
                if let Some(c) = conn {
                    self.submit(c, sid.clone(), Work::More(credit));
                }
            }
            "open" => match self.open(&msg) {
                Ok(cid) => self.g.out.send(&Value::obj(vec![("ok", Value::Bool(true)), ("conn", Value::Int(cid as i128)), ("id", id)])),
                Err(f) => self.reply_error(id, &f),
            },
            "sleep" => {
                self.close_all();
                self.g.out.send(&Value::obj(vec![("id", id), ("ok", Value::Bool(true))]));
                return false;
            }
            "_debug" if self.debug => return self.debug_action(&msg, id),
            "close" | "execute" | "execute_many" | "batch" | "stream" | "begin" | "commit" | "rollback" | "schema"
            | "schema_version" | "op" => match self.conn_for(&msg) {
                Ok(cid) => {
                    if kind == "close" {
                        if let Some(h) = self.conns.remove(&cid) {
                            let ctl = JobCtl::new(id.clone());
                            lock(&self.g.jobs).insert(id.key(), (cid, Arc::clone(&ctl)));
                            let _ = h.shared.sender.send(Job { ctl, work: Work::Close { reply: true, done: None } });
                            self.closed_threads.push(h.thread);
                        }
                    } else {
                        self.submit(cid, id, Work::Request(msg));
                    }
                }
                Err(f) => self.reply_error(id, &f),
            },
            other => self.reply_error(id, &qerr(format!("unknown message type {}", py_repr_value(&Value::str(other))), "PROTOCOL")),
        }
        true
    }

    fn submit(&self, cid: i64, id: Value, work: Work) {
        let Some(h) = self.conns.get(&cid) else { return };
        let ctl = JobCtl::new(id.clone());
        lock(&self.g.jobs).insert(id.key(), (cid, Arc::clone(&ctl)));
        let _ = h.shared.sender.send(Job { ctl, work });
    }

    fn conn_for(&self, msg: &Value) -> Result<i64, Fail> {
        match msg.opt("conn") {
            None => {
                if self.conns.len() == 1 {
                    Ok(*self.conns.keys().next().expect("one connection"))
                } else {
                    Err(qerr(format!("request has no 'conn' and the worker has {} connections", self.conns.len()), "PROTOCOL"))
                }
            }
            Some(v) => match v.as_i64() {
                Some(c) if self.conns.contains_key(&c) => Ok(c),
                _ => Err(qerr(format!("no connection {} in this worker", py_repr_value(v)), "PROTOCOL")),
            },
        }
    }

    fn hello(&mut self, msg: &Value) {
        let wanted: Vec<String> = match msg.get("codecs") {
            Some(Value::List(l)) => l.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
            _ => vec![],
        };
        let codec = wanted.iter().find(|c| *c == "json" || *c == "msgpack").cloned().unwrap_or_else(|| "json".into());
        if let Some(n) = msg.get("max_frame_bytes").and_then(Value::as_i64)
            && n > 0
        {
            self.g.out.max_frame.store(n as u64, Ordering::Relaxed);
        }
        let reply = Value::obj(vec![
            ("id", msg.get("id").cloned().unwrap_or(Value::Null)),
            ("type", Value::str("hello")),
            ("protocol", Value::Int(PROTOCOL_VERSION)),
            ("codec", Value::str(&codec)),
            ("duckdb_version", Value::Str(ffi::library_version())),
            ("pid", Value::Int(std::process::id() as i128)),
            ("rlimit_as", Value::Bool(self.rlimit_as)),
            ("implementation", Value::str("rust")),
            ("ops", Value::Bool(false)),
            ("native_ops", Value::List(vec![Value::str("append")])),
            ("concurrent", Value::Bool(true)),
        ]);
        // The hello reply is always JSON; everything after it in the codec.
        self.g.out.codec.store(0, Ordering::Relaxed);
        self.g.out.send(&reply);
        self.g.out.codec.store(if codec == "msgpack" { 1 } else { 0 }, Ordering::Relaxed);
    }

    fn cancel(&self, msg: &Value) {
        let Some(target) = msg.get("target") else { return };
        let key = target.key();
        let found = lock(&self.g.jobs).get(&key).cloned();
        if let Some((cid, ctl)) = found {
            if let Some(h) = self.conns.get(&cid) {
                cancel_job(&h.shared, &self.g.timer, &ctl, "cancelled");
            } else {
                ctl.cancel("cancelled");
            }
            return;
        }
        let stream_conn = lock(&self.g.streams).get(&key).copied();
        if let Some(cid) = stream_conn {
            self.submit(cid, target.clone(), Work::CloseStream);
        }
    }

    fn open(&mut self, msg: &Value) -> Result<i64, Fail> {
        let engine = msg.opt("engine").and_then(Value::as_str).unwrap_or("dblayer.backends.duckdb:DuckDB");
        if !engine.contains("duckdb") {
            return Err(dberr("NotSupported", format!("the Rust worker runs DuckDB only, not engine {engine:?}")));
        }
        let path = msg.get("path").and_then(Value::as_str).ok_or_else(|| qerr("open needs a path", "PROTOCOL"))?;
        let write = msg.get("mode").and_then(Value::as_str) == Some("write");
        let config = msg.get("config").cloned().unwrap_or(Value::Map(vec![]));
        let opts = OpenOptions::from_config(&config)?;
        if write && opts.read_only {
            return Err(dberr("NotSupported", "database is read-only; writes are not allowed"));
        }
        let key = format!("{path}\u{0}{}", String::from_utf8_lossy(&json::encode(&config)));
        let conn = {
            let mut dbs = lock(&self.g.dbs);
            if !dbs.contains_key(&key) {
                dbs.insert(key.clone(), open_database(path, &opts)?);
            }
            let entry = dbs.get_mut(&key).expect("just inserted");
            let conn = match entry.spare.pop() {
                Some(c) => c,
                None => match entry.db.connect() {
                    Ok(c) => c,
                    Err(e) => {
                        if entry.conns == 0 {
                            dbs.remove(&key); // releases the file lock
                        }
                        return Err(e.into());
                    }
                },
            };
            entry.conns += 1;
            conn
        };
        let cid = self.next_conn;
        self.next_conn += 1;
        let (tx, rx) = channel::<Job>();
        let shared = Arc::new(ConnShared {
            state: Mutex::new(CState::default()),
            handle: Mutex::new(conn.interrupt_handle()),
            sender: tx,
        });
        let worker = ConnWorker {
            id: cid,
            conn: Some(conn),
            write,
            shared: Arc::clone(&shared),
            g: Arc::clone(&self.g),
            db_key: key,
            duck_in_tx: false,
            tx_failed: false,
            tx_seq: 0,
            streams: HashMap::new(),
            dirty: std::cell::Cell::new(false),
        };
        // Started under RLIMIT_AS: ffi::spawn_raw (see there), with std's
        // default 2 MiB stack, so failing to get a thread is this error.
        let thread = ffi::spawn_raw(CONN_STACK_BYTES, Box::new(move || worker.run(rx)))
            .map_err(|e| qerr(format!("worker error: could not start a thread: {e}"), "WORKER"))?;
        self.conns.insert(cid, ConnHandle { shared, thread });
        Ok(cid)
    }

    /// Close every connection (rolled back), waiting for their threads.
    fn close_all(&mut self) {
        let mut waits = Vec::new();
        for (_, h) in self.conns.drain() {
            let (tx, rx) = channel();
            let ctl = JobCtl::new(Value::Null);
            let _ = h.shared.sender.send(Job { ctl, work: Work::Close { reply: false, done: Some(tx) } });
            waits.push((rx, h.thread));
        }
        for (rx, t) in waits {
            let _ = rx.recv();
            t.join();
        }
        for t in self.closed_threads.drain(..) {
            t.join();
        }
    }

    /// End of input: stop what runs, roll back, close and exit, within
    /// SHUTDOWN_GRACE even if a statement ignores every interrupt.
    fn shutdown(&mut self) {
        // Under RLIMIT_AS too: std::thread::spawn would abort if it could
        // not get the thread; without the guard the shutdown is merely
        // not time-bounded.
        if let Ok(t) = ffi::spawn_raw(
            64 * 1024,
            Box::new(|| {
                std::thread::sleep(SHUTDOWN_GRACE);
                std::process::exit(0);
            }),
        ) {
            t.detach();
        }
        let jobs: Vec<(i64, Arc<JobCtl>)> = lock(&self.g.jobs).values().cloned().collect();
        for (cid, ctl) in jobs {
            match self.conns.get(&cid) {
                Some(h) => cancel_job(&h.shared, &self.g.timer, &ctl, "cancelled"),
                None => ctl.cancel("cancelled"),
            }
        }
        self.close_all();
        let _ = std::io::stderr().flush();
        std::process::exit(0);
    }

    fn debug_action(&mut self, msg: &Value, id: Value) -> bool {
        let action = msg.get("action").and_then(Value::as_str).unwrap_or("");
        let ok = |extra: Vec<(&str, Value)>| {
            let mut v = Value::obj(vec![("id", id.clone()), ("ok", Value::Bool(true))]);
            for (k, x) in extra {
                v.set(k, x);
            }
            v
        };
        match action {
            // Blocks the main thread: nothing in the worker reads input meanwhile.
            "sleep" => {
                let s = msg.get("seconds").and_then(Value::as_f64).unwrap_or(1.0);
                std::thread::sleep(Duration::from_secs_f64(s.clamp(0.0, 3600.0)));
                self.g.out.send(&ok(vec![]));
            }
            "oversized_frame" => {
                let mut b = (1u32 << 31).to_be_bytes().to_vec();
                b.extend_from_slice(&[b'x'; 16]);
                self.g.out.write_raw(&b);
            }
            "truncated_frame" => {
                let mut b = 100u32.to_be_bytes().to_vec();
                b.extend_from_slice(b"{\"id\":");
                self.g.out.write_raw(&b);
                std::process::exit(3);
            }
            "garbage" => self.g.out.write_frame(b"\xff\xfe not a valid payload"),
            "alloc" => {
                let mb = msg.get("mb").and_then(Value::as_i64).unwrap_or(0).max(0) as usize;
                let mut blocks: Vec<Vec<u8>> = Vec::new();
                for _ in 0..mb {
                    let mut b: Vec<u8> = Vec::new();
                    if b.try_reserve_exact(1 << 20).is_err() {
                        self.g.out.send(&Value::obj(vec![
                            ("id", id),
                            ("error", Value::obj(vec![("type", Value::str("ResourceLimitExceeded")), ("message", Value::str("worker ran out of memory (memory limit)"))])),
                            ("fatal", Value::Bool(true)),
                        ]));
                        return false;
                    }
                    b.resize(1 << 20, 1);
                    blocks.push(b);
                }
                self.g.out.send(&ok(vec![("allocated_mb", Value::Int(blocks.len() as i128))]));
            }
            "exit" => std::process::exit(msg.get("code").and_then(Value::as_i64).unwrap_or(1) as i32),
            "print" => {
                eprintln!("{}", msg.get("text").and_then(Value::as_str).unwrap_or(""));
                self.g.out.send(&ok(vec![]));
            }
            other => self.reply_error(id, &qerr(format!("unknown debug action {}", py_repr_value(&Value::str(other))), "PROTOCOL")),
        }
        true
    }
}

/// Python's `repr()` of a str or int, for messages that quote one.
pub fn py_repr_value(v: &Value) -> String {
    match v {
        Value::Str(s) => {
            if s.contains('\'') && !s.contains('"') {
                format!("\"{s}\"")
            } else {
                format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
            }
        }
        Value::Int(i) => i.to_string(),
        Value::Null => "None".into(),
        Value::Bool(b) => if *b { "True" } else { "False" }.into(),
        other => format!("{other:?}"),
    }
}

// -- opening a database -----------------------------------------------------------------

struct OpenOptions {
    memory_limit: Option<String>,
    threads: Option<i64>,
    read_only: bool,
    allow_external_access: bool,
    extra: Vec<(String, String)>,
    create: bool,
}

impl OpenOptions {
    /// The keyword arguments `DuckDBWorker` sends (`DuckDB.__init__`'s).
    fn from_config(c: &Value) -> Result<OpenOptions, Fail> {
        let mut o = OpenOptions {
            memory_limit: Some("256MB".into()),
            threads: Some(2),
            read_only: false,
            allow_external_access: false,
            extra: vec![],
            create: true,
        };
        let Value::Map(items) = c else { return Ok(o) };
        for (k, v) in items {
            let Value::Str(k) = k else { continue };
            match k.as_str() {
                "memory_limit" => o.memory_limit = v.as_str().map(String::from),
                "threads" => o.threads = v.as_i64(),
                "read_only" => o.read_only = v.is_true(),
                "allow_external_access" => o.allow_external_access = v.is_true(),
                "create" => o.create = v.is_true(),
                "config" => {
                    if let Value::Map(extra) = v {
                        for (ek, ev) in extra {
                            let Value::Str(ek) = ek else { continue };
                            let text = match ev {
                                Value::Str(s) => s.clone(),
                                Value::Bool(b) => b.to_string(),
                                Value::Int(i) => i.to_string(),
                                Value::Float(f) => f.to_string(),
                                other => format!("{other:?}"),
                            };
                            o.extra.push((ek.clone(), text));
                        }
                    }
                }
                "write_queue" | "max_write_connections" | "executor" => {}
                other => {
                    return Err(Fail::Builtin {
                        class: "TypeError",
                        message: format!("DuckDB.__init__() got an unexpected keyword argument '{other}'"),
                    });
                }
            }
        }
        if o.read_only {
            o.create = false;
        }
        Ok(o)
    }
}

/// One open database file: the DuckDB instance, how many connections use
/// it, and connections made before its settings were locked.
pub struct DbEntry {
    // Field order is drop order: connections before the instance.
    spare: Vec<Connection>,
    db: Database,
    conns: usize,
}

/// How many configured connections a database keeps (see `open_database`).
const SPARE_CONNECTIONS: usize = 8;

/// DuckDB fills a streaming result's buffer (`streaming_buffer_size`, 10 MiB
/// by default, half of it before the first fetch returns). For a query that
/// yields one row per chunk (an unbounded recursive CTE) that takes seconds,
/// where the Python client returns its first rows in about a millisecond.
/// The setting is per connection and cannot be changed once the
/// configuration is locked, so connections are made and configured before
/// the lock, and reused (`release_db`).
const STREAMING_BUFFER: &str = "16KB";

fn open_database(path: &str, o: &OpenOptions) -> Result<DbEntry, Fail> {
    if !o.create && !std::path::Path::new(path).exists() {
        return Err(qerr(format!("unable to open database file {}", py_repr_value(&Value::str(path))), "CANTOPEN"));
    }
    let mut settings: Vec<(String, String)> = Vec::new();
    if let Some(m) = &o.memory_limit {
        settings.push(("memory_limit".into(), m.clone()));
    }
    if let Some(t) = o.threads {
        settings.push(("threads".into(), t.to_string()));
    }
    if !o.allow_external_access {
        settings.push(("enable_external_access".into(), "false".into()));
    }
    if o.read_only {
        settings.push(("access_mode".into(), "READ_ONLY".into()));
    }
    settings.extend(o.extra.iter().cloned());
    let db = Database::open(path, &settings)?;
    let mut spare = Vec::with_capacity(SPARE_CONNECTIONS);
    for _ in 0..SPARE_CONNECTIONS {
        let c = db.connect()?;
        c.query(&format!("SET streaming_buffer_size = '{STREAMING_BUFFER}'"))?;
        spare.push(c);
    }
    // Lock the settings so SQL cannot raise the limits.
    let c = db.connect()?;
    c.query("SET lock_configuration = true")?;
    drop(c);
    Ok(DbEntry { spare, db, conns: 0 })
}

/// A connection is done with its database: keep it for a later open (it is
/// configured; one made after the lock could not be), and close the
/// instance after its last connection.
fn release_db(g: &Globals, key: &str, conn: Connection, reusable: bool) {
    let mut dbs = lock(&g.dbs);
    let Some(entry) = dbs.get_mut(key) else { return };
    entry.conns = entry.conns.saturating_sub(1);
    if reusable && entry.spare.len() < SPARE_CONNECTIONS {
        entry.spare.push(conn);
    } else {
        drop(conn);
    }
    if entry.conns == 0 {
        // Closing the instance (checkpoint, file lock released) happens
        // when the entry drops, here.
        dbs.remove(key);
    }
}

// -- a connection's thread ---------------------------------------------------------------

/// Rows still to come from a result.
struct RowSource {
    // Field order is drop order: the result before its statement.
    res: QueryResult,
    _prepared: Option<Prepared>,
    chunk: Option<ffi::Chunk>,
    pos: usize,
    exhausted: bool,
    tz: String,
}

impl RowSource {
    fn take(&mut self, n: usize) -> Result<Vec<Value>, Fail> {
        let mut out = Vec::with_capacity(n.min(FETCH_BATCH * 2));
        while out.len() < n {
            let have = self.chunk.as_ref().map(|c| c.len()).unwrap_or(0);
            if self.pos >= have {
                self.chunk = None;
                if self.exhausted {
                    break;
                }
                match self.res.fetch()? {
                    None => {
                        self.exhausted = true;
                        break;
                    }
                    Some(c) => {
                        self.chunk = Some(c);
                        self.pos = 0;
                        continue;
                    }
                }
            }
            let k = (n - out.len()).min(have - self.pos);
            let chunk = self.chunk.as_ref().expect("a chunk");
            out.extend(chunk.rows(&self.res.columns, self.pos, self.pos + k, &Ctx { tz: &self.tz })?);
            self.pos += k;
        }
        Ok(out)
    }

    /// Count (without converting) the rows still to come.
    fn count_rest(&mut self) -> Result<usize, Fail> {
        let mut n = self.chunk.as_ref().map(|c| c.len()).unwrap_or(0).saturating_sub(self.pos);
        self.chunk = None;
        while !self.exhausted {
            match self.res.fetch()? {
                None => self.exhausted = true,
                Some(c) => n += c.len(),
            }
        }
        Ok(n)
    }
}

/// `protocol.Results` in wire form.
struct Results {
    columns: Vec<(String, TyNode)>,
    rows: Vec<Value>,
    truncated: bool,
    rowcount: i64,
}

impl Results {
    fn empty(rowcount: i64) -> Results {
        Results { columns: vec![], rows: vec![], truncated: false, rowcount }
    }

    fn into_wire(self) -> Value {
        let mut out = Value::obj(vec![("columns", Value::List(self.columns.iter().map(|(n, _)| Value::str(n)).collect()))]);
        if !self.columns.is_empty() {
            out.set("types", Value::List(self.columns.iter().map(|(_, t)| Value::Str(t.name())).collect()));
        }
        if !self.rows.is_empty() {
            out.set("rows", Value::List(self.rows));
        }
        if self.truncated {
            out.set("truncated", Value::Bool(true));
        }
        if self.rowcount != -1 {
            out.set("rowcount", Value::Int(self.rowcount as i128));
        }
        out
    }
}

/// `_row_bytes`: what `max_result_bytes` counts.
fn row_bytes(row: &Value) -> u64 {
    let Value::List(cells) = row else { return 0 };
    cells
        .iter()
        .map(|v| match v {
            Value::Null => 1,
            Value::Str(s) => s.chars().count() as u64,
            Value::Bytes(b) => b.len() as u64,
            _ => 8,
        })
        .sum()
}

struct Stream {
    id: Value,
    sql: String,
    params: Option<Value>,
    chunk_rows: usize,
    remaining_ms: Option<f64>,
    max_bytes: Option<u64>,
    bytes: u64,
    started: bool,
    wrapped: bool,
    done: bool,
    first: bool,
    columns: Vec<(String, TyNode)>,
    rows: Option<RowSource>,
    credit: i64,
}

struct ConnWorker {
    id: i64,
    conn: Option<Connection>,
    write: bool,
    shared: Arc<ConnShared>,
    g: Arc<Globals>,
    db_key: String,
    /// DuckDB's own transaction state (`DuckDBConnection._in_tx`).
    duck_in_tx: bool,
    /// A DuckDB error happened in the open transaction (`_tx_failed`).
    tx_failed: bool,
    tx_seq: u64,
    streams: HashMap<String, Stream>,
    /// The session holds state a later user of the connection must not see
    /// (a temporary object, a variable, a prepared statement): not reused.
    dirty: std::cell::Cell<bool>,
}

type Reply = Result<Option<Value>, Fail>;

impl ConnWorker {
    fn conn(&self) -> &Connection {
        self.conn.as_ref().expect("connection open")
    }

    fn run(mut self, rx: Receiver<Job>) {
        while let Ok(job) = rx.recv() {
            self.expire_if_pending();
            match job.work {
                Work::Close { reply, done } => {
                    self.close();
                    if reply {
                        self.g.out.send(&Value::obj(vec![("ok", Value::Bool(true)), ("id", job.ctl.id.clone())]));
                    }
                    self.forget(&job.ctl);
                    if let Some(d) = done {
                        let _ = d.send(());
                    }
                    return;
                }
                Work::Expire => {
                    self.forget(&job.ctl);
                }
                work => self.run_job(job.ctl, work),
            }
        }
        self.close();
    }

    fn forget(&self, ctl: &Arc<JobCtl>) {
        let mut jobs = lock(&self.g.jobs);
        if let Some((_, c)) = jobs.get(&ctl.id.key())
            && Arc::ptr_eq(c, ctl)
        {
            jobs.remove(&ctl.id.key());
        }
    }

    fn in_tx(&self) -> bool {
        lock(&self.shared.state).tx.is_some()
    }

    fn run_job(&mut self, ctl: Arc<JobCtl>, work: Work) {
        lock(&self.shared.state).current = Some(Arc::clone(&ctl));
        let is_stream = matches!(&work, Work::More(_)) || matches!(&work, Work::Request(m) if m.get("type").and_then(Value::as_str) == Some("stream"));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if ctl.is_cancelled() {
                return Err(interrupted("query cancelled"));
            }
            match work {
                Work::Request(msg) => self.handle(&ctl, &msg),
                Work::More(credit) => self.more(&ctl, credit),
                Work::CloseStream => self.close_stream_request(&ctl),
                _ => Ok(None),
            }
        }));
        lock(&self.shared.state).current = None;
        let result = result.unwrap_or_else(|p| {
            let what = p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            Err(qerr(format!("worker error: panic: {what}"), "WORKER"))
        });
        match result {
            Ok(Some(mut reply)) => {
                reply.set("id", ctl.id.clone());
                self.g.out.send(&reply);
            }
            Ok(None) => {}
            Err(f) => {
                let mut reply = Value::obj(vec![("id", ctl.id.clone()), ("error", error_payload(&f, ctl.reason()))]);
                if self.in_tx() {
                    reply.set("in_tx", Value::Bool(true));
                }
                self.g.out.send(&reply);
                if is_stream {
                    self.drop_stream(&ctl.id.key());
                }
            }
        }
        self.forget(&ctl);
    }

    fn with_tx(&self, mut body: Value) -> Value {
        if self.in_tx() {
            body.set("in_tx", Value::Bool(true));
        }
        body
    }

    /// Mark the DuckDB transaction failed after a DuckDB error in it
    /// (`DuckDBConnection._guarded`).
    fn guarded<T>(&mut self, r: Result<T, Fail>) -> Result<T, Fail> {
        if let Err(f) = &r
            && f.is_duck()
            && self.duck_in_tx
        {
            self.tx_failed = true;
        }
        r
    }

    fn handle(&mut self, ctl: &Arc<JobCtl>, msg: &Value) -> Reply {
        // A new request on a connection ends any stream the parent left
        // paused on it (an abandoned stream is not always cancelled), as a
        // new execute on a DuckDB cursor closes its previous result: an open
        // result would refuse every new statement (RESOURCE_IN_USE).
        if !self.streams.is_empty() {
            let keys: Vec<String> = self.streams.keys().cloned().collect();
            for k in keys {
                self.drop_stream(&k);
            }
        }
        let kind = msg.get("type").and_then(Value::as_str).unwrap_or("");
        if kind != "begin" && kind != "rollback" {
            self.check_tx(msg.opt("tx"))?;
        }
        match kind {
            "execute" => {
                let tl = msg.opt("time_limit_ms").and_then(Value::as_f64);
                let max_rows = msg.opt("max_rows").and_then(Value::as_int_lossy);
                let max_bytes = msg.opt("max_result_bytes").and_then(Value::as_int_lossy);
                let r = self.execute(ctl, msg.get("sql").and_then(Value::as_str).unwrap_or(""), msg.opt("params"), tl, max_rows, max_bytes);
                let r = self.guarded(r)?;
                Ok(Some(self.with_tx(Value::obj(vec![("result", r.into_wire())]))))
            }
            "execute_many" => {
                let seq = match msg.get("params_seq") {
                    Some(Value::List(l)) => l.clone(),
                    _ => vec![],
                };
                let r = self.execute_many(ctl, msg.get("sql").and_then(Value::as_str).unwrap_or(""), &seq);
                let n = self.guarded(r)?;
                Ok(Some(self.with_tx(Value::obj(vec![("result", Value::obj(vec![("rowcount", Value::Int(n as i128))]))]))))
            }
            "batch" => {
                let tl = msg.opt("time_limit_ms").and_then(Value::as_f64);
                let max_rows = msg.opt("max_rows").and_then(Value::as_int_lossy);
                let stmts = match msg.get("statements") {
                    Some(Value::List(l)) => l.clone(),
                    _ => vec![],
                };
                let mut out = Vec::with_capacity(stmts.len());
                for s in &stmts {
                    let (sql, params) = match s {
                        Value::List(p) if !p.is_empty() => (p[0].as_str().unwrap_or("").to_string(), p.get(1).filter(|v| **v != Value::Null)),
                        _ => return Err(Fail::Builtin { class: "ValueError", message: "bad batch statement".into() }),
                    };
                    let r = self.execute(ctl, &sql, params, tl, max_rows, None);
                    out.push(self.guarded(r)?.into_wire());
                }
                Ok(Some(self.with_tx(Value::obj(vec![("result", Value::List(out))]))))
            }
            "stream" => self.stream(ctl, msg),
            "begin" => self.begin(msg),
            "commit" => self.commit(),
            "rollback" => self.rollback(msg.opt("tx")),
            "schema" => {
                let r = self.statement(ctl, None, |w| w.schema());
                let s = self.guarded(r)?;
                Ok(Some(self.with_tx(Value::obj(vec![("result", s)]))))
            }
            "schema_version" => {
                let r = self.statement(ctl, None, |w| schema::version(w.conn()).map_err(Fail::from));
                let v = self.guarded(r)?;
                Ok(Some(self.with_tx(Value::obj(vec![("result", Value::Str(v))]))))
            }
            "op" => self.op(ctl, msg),
            other => Err(qerr(format!("unknown message type {}", py_repr_value(&Value::str(other))), "PROTOCOL")),
        }
    }

    fn check_tx(&self, tx: Option<&Value>) -> Result<(), Fail> {
        let Some(tx) = tx else { return Ok(()) };
        let tx = tx.as_str().map(String::from).unwrap_or_else(|| py_repr_value(tx));
        let st = lock(&self.shared.state);
        if st.tx.as_deref() == Some(tx.as_str()) {
            return Ok(());
        }
        if st.expired.contains(&tx) {
            return Err(dberr("TransactionExpired", format!("transaction {tx} passed its deadline and was rolled back")));
        }
        Err(dberr("TransactionClosed", format!("transaction {tx} is not open")))
    }

    /// A deadline passed while this thread was busy or idle: roll back and
    /// tell the parent (`Server._expire`).
    fn expire_if_pending(&mut self) {
        let tx = {
            let mut st = lock(&self.shared.state);
            let Some(tx) = st.expire_pending.take() else { return };
            if st.tx.as_deref() != Some(tx.as_str()) {
                return;
            }
            st.tx = None;
            st.deadline_tx = None;
            tx
        };
        if let Err(e) = self.duck_rollback() {
            eprintln!("dblayer-duckdb-rsworker: rollback at deadline failed: {e:?}");
        }
        self.g.out.send(&Value::obj(vec![
            ("type", Value::str("expired")),
            ("conn", Value::Int(self.id as i128)),
            ("tx", Value::Str(tx)),
        ]));
    }

    // -- statements ------------------------------------------------------------

    /// Run `f` as one statement: refused if the request was cancelled,
    /// interrupted (repeatedly) at its time limit or on cancel.
    fn statement<T>(&mut self, ctl: &Arc<JobCtl>, time_limit_ms: Option<f64>, f: impl FnOnce(&mut Self) -> Result<T, Fail>) -> Result<T, Fail> {
        let stmt = {
            let mut st = lock(&self.shared.state);
            if ctl.is_cancelled() {
                return Err(interrupted("query interrupted (cancelled)"));
            }
            st.seq += 1;
            st.active = st.seq;
            st.seq
        };
        let token = time_limit_ms.map(|ms| {
            let (s, t) = (Arc::clone(&self.shared), Arc::clone(&self.g.timer));
            self.g.timer.schedule(Duration::from_secs_f64(ms.max(0.0) / 1000.0), move || interrupt_while_active(&s, &t, stmt))
        });
        let r = f(self);
        if let Some(t) = token {
            self.g.timer.cancel(t);
        }
        lock(&self.shared.state).active = 0;
        r
    }

    fn classify(&self, kind: &str) -> Result<(), Fail> {
        if kind == "TRANSACTION" {
            return Err(qerr("BEGIN/COMMIT/ROLLBACK are managed by dblayer; use db.transaction()", "TRANSACTION_STATEMENT"));
        }
        if !self.write && !sql::is_result(kind) {
            return Err(qerr(format!("attempt to write a read-only connection ({kind} statement)"), "READ_ONLY"));
        }
        Ok(())
    }

    /// `_check`: one statement per call, read-only kinds on read
    /// connections. `None` for an empty statement.
    fn prepare(&self, sql_text: &str) -> Result<Option<(Prepared, &'static str)>, Fail> {
        let ex = self.conn().extract(sql_text)?;
        if ex.count == 0 {
            return Ok(None);
        }
        if ex.count > 1 {
            return Err(qerr("only one statement can be executed at a time", "MULTIPLE_STATEMENTS"));
        }
        match ex.prepare(self.conn(), 0) {
            Ok(p) => {
                let kind = sql::kind_name(p.statement_type());
                self.classify(kind)?;
                if leaves_session_state(kind, sql_text) {
                    self.dirty.set(true);
                }
                Ok(Some((p, kind)))
            }
            Err(e) => {
                self.classify(sql::guess_kind(sql_text))?;
                Err(e.into())
            }
        }
    }

    /// Read connections outside a transaction run inside a read-only one.
    fn ro_wrapper(&mut self) -> Result<bool, Fail> {
        if self.write || self.duck_in_tx {
            return Ok(false);
        }
        self.conn().query("BEGIN TRANSACTION READ ONLY")?;
        Ok(true)
    }

    fn end_wrapper(&mut self, ok: bool) -> Result<(), Fail> {
        let r = self.conn().query(if ok { "COMMIT" } else { "ROLLBACK" });
        if ok { r.map_err(Fail::from) } else { Ok(()) }
    }

    fn execute(
        &mut self,
        ctl: &Arc<JobCtl>,
        sql_text: &str,
        params: Option<&Value>,
        tl: Option<f64>,
        max_rows: Option<i64>,
        max_bytes: Option<i64>,
    ) -> Result<Results, Fail> {
        let Some((mut p, kind)) = self.prepare(sql_text)? else {
            return Ok(Results::empty(-1));
        };
        let wrapped = self.ro_wrapper()?;
        let r = self.statement(ctl, tl, |w| w.run_statement(kind, &mut p, params, max_rows, max_bytes));
        drop(p);
        if wrapped {
            self.end_wrapper(r.is_ok())?;
        }
        r
    }

    fn start(&self, kind: &str, p: &mut Prepared, params: Option<&Value>) -> Result<QueryResult, Fail> {
        bind_params(p, params)?;
        Ok(p.execute(sql::is_result(kind))?)
    }

    fn tz_for(&self, columns: &[(String, TyNode)]) -> String {
        if columns.iter().any(|(_, t)| t.has_tz()) { self.conn().timezone().unwrap_or_else(|| "UTC".into()) } else { String::new() }
    }

    fn run_statement(&mut self, kind: &str, p: &mut Prepared, params: Option<&Value>, max_rows: Option<i64>, max_bytes: Option<i64>) -> Result<Results, Fail> {
        let mut res = self.start(kind, p, params)?;
        if sql::is_dml(kind) && res.return_type() == ReturnType::ChangedRows {
            return Ok(Results::empty(res.rows_changed() as i64));
        }
        if !sql::is_dml(kind) && !sql::is_result(kind) {
            return Ok(Results::empty(-1));
        }
        let columns = res.columns.clone();
        if let Some((_, t)) = columns.iter().find(|(_, t)| t.unsupported().is_some()) {
            return Err(dberr("NotSupported", format!("result type {} is not supported by the Rust worker", t.name())));
        }
        let tz = self.tz_for(&columns);
        // The statement (`p`) outlives this result: the caller drops it after.
        let mut src = RowSource { res, _prepared: None, chunk: None, pos: 0, exhausted: false, tz };
        Self::collect(&mut src, kind, columns, max_rows, max_bytes)
    }

    fn collect(src: &mut RowSource, kind: &str, columns: Vec<(String, TyNode)>, max_rows: Option<i64>, max_bytes: Option<i64>) -> Result<Results, Fail> {
        let limit = max_rows.map(|m| m.max(0) as usize + 1);
        let mut rows: Vec<Value> = Vec::new();
        let mut size = 0u64;
        loop {
            let n = match limit {
                None => FETCH_BATCH,
                Some(l) => FETCH_BATCH.min(l.saturating_sub(rows.len())),
            };
            if n == 0 {
                break;
            }
            let batch = src.take(n)?;
            if batch.is_empty() {
                break;
            }
            if let Some(mb) = max_bytes {
                size += batch.iter().map(row_bytes).sum::<u64>();
                if size > mb.max(0) as u64 {
                    return Err(dberr("ResourceLimitExceeded", format!("result exceeded max_result_bytes={mb}")));
                }
            }
            rows.extend(batch);
        }
        let mr = max_rows.map(|m| m.max(0) as usize);
        let truncated = mr.is_some_and(|m| rows.len() > m);
        let mut total = rows.len();
        if truncated {
            if sql::is_dml(kind) {
                total += src.count_rest()?;
            }
            rows.truncate(mr.expect("max_rows"));
        }
        let rowcount = if sql::is_dml(kind) { total as i64 } else { -1 };
        Ok(Results { columns, rows, truncated, rowcount })
    }

    fn execute_many(&mut self, ctl: &Arc<JobCtl>, sql_text: &str, seq: &[Value]) -> Result<i64, Fail> {
        let Some((mut p, kind)) = self.prepare(sql_text)? else { return Ok(0) };
        if seq.is_empty() {
            return Ok(0);
        }
        let wrapped = self.ro_wrapper()?;
        let r = self.statement(ctl, None, |_| {
            let mut total: i64 = 0;
            let mut counted = true;
            for params in seq {
                p.clear_bindings();
                let params = if *params == Value::Null { None } else { Some(params) };
                bind_params(&mut p, params)?;
                let mut res = p.execute(false)?;
                if res.return_type() == ReturnType::ChangedRows && sql::is_dml(kind) {
                    total += res.rows_changed() as i64;
                } else {
                    counted = false;
                }
            }
            Ok(if counted { total } else { -1 })
        });
        drop(p);
        if wrapped {
            self.end_wrapper(r.is_ok())?;
        }
        r
    }

    // -- transactions ------------------------------------------------------------

    fn begin(&mut self, msg: &Value) -> Reply {
        let mode = msg.get("mode").and_then(Value::as_str).unwrap_or("write");
        if lock(&self.shared.state).tx.is_some() {
            return Err(qerr("cannot start a transaction within a transaction", "SQLITE_ERROR"));
        }
        let sql_text = match mode {
            "write" => "BEGIN TRANSACTION",
            "read" => "BEGIN TRANSACTION READ ONLY",
            other => {
                return Err(Fail::Builtin { class: "ValueError", message: format!("unknown transaction mode {}", py_repr_value(&Value::str(other))) });
            }
        };
        self.conn().query(sql_text)?;
        self.duck_in_tx = true;
        self.tx_failed = false;
        self.tx_seq += 1;
        let tx = format!("t{}", self.tx_seq);
        let deadline = msg.opt("deadline_ms").and_then(Value::as_f64);
        {
            let mut st = lock(&self.shared.state);
            st.tx = Some(tx.clone());
            st.deadline_tx = deadline.map(|_| tx.clone());
        }
        if let Some(ms) = deadline {
            let (s, t, txc) = (Arc::clone(&self.shared), Arc::clone(&self.g.timer), tx.clone());
            self.g.timer.schedule(Duration::from_secs_f64(ms.max(0.0) / 1000.0), move || on_deadline(&s, &t, &txc));
        }
        Ok(Some(Value::obj(vec![("ok", Value::Bool(true)), ("tx", Value::Str(tx))])))
    }

    fn clear_tx(&mut self) {
        let mut st = lock(&self.shared.state);
        st.tx = None;
        st.deadline_tx = None;
    }

    /// COMMIT, unless a statement failed in this transaction and DuckDB
    /// aborted it: then roll back and say so (DuckDB would turn the COMMIT
    /// into a silent rollback).
    fn commit(&mut self) -> Reply {
        // A commit that has started is not cut short by the deadline.
        lock(&self.shared.state).deadline_tx = None;
        if self.duck_in_tx {
            let r = self.duck_commit();
            self.duck_in_tx = false;
            self.tx_failed = false;
            self.clear_tx();
            r?;
        } else {
            self.clear_tx();
        }
        Ok(Some(Value::obj(vec![("ok", Value::Bool(true))])))
    }

    fn duck_commit(&mut self) -> Result<(), Fail> {
        if self.tx_failed
            && let Err(e) = self.conn().query("SELECT 1")
            && e.kind == errors::TRANSACTION
        {
            self.conn().query("ROLLBACK")?;
            return Err(qerr(
                "the transaction was aborted by an earlier error and has been rolled back (DuckDB cannot \
                 continue a transaction after a failed statement)",
                "TRANSACTION_ABORTED",
            ));
        }
        self.conn().query("COMMIT")?;
        Ok(())
    }

    fn duck_rollback(&mut self) -> Result<(), Fail> {
        if !self.duck_in_tx {
            return Ok(());
        }
        let r = self.conn().query("ROLLBACK");
        self.duck_in_tx = false;
        self.tx_failed = false;
        r.map_err(Fail::from)
    }

    fn rollback(&mut self, tx: Option<&Value>) -> Reply {
        if let Some(tx) = tx.and_then(Value::as_str) {
            let mut st = lock(&self.shared.state);
            if st.tx.as_deref() != Some(tx)
                && let Some(i) = st.expired.iter().position(|e| e == tx)
            {
                st.expired.remove(i);
                return Ok(Some(Value::obj(vec![("ok", Value::Bool(true)), ("expired", Value::Bool(true))])));
            }
        }
        self.check_tx(tx)?;
        lock(&self.shared.state).deadline_tx = None;
        let r = self.duck_rollback();
        self.clear_tx();
        r?;
        Ok(Some(Value::obj(vec![("ok", Value::Bool(true))])))
    }

    // -- schema ------------------------------------------------------------------

    fn schema(&mut self) -> Result<Value, Fail> {
        let own = !self.duck_in_tx;
        if own {
            self.conn().query("BEGIN TRANSACTION READ ONLY")?;
        }
        let r = schema::read(self.conn());
        if own {
            let _ = self.conn().query("ROLLBACK");
        }
        Ok(r?)
    }

    // -- named operations ----------------------------------------------------------

    fn op(&mut self, ctl: &Arc<JobCtl>, msg: &Value) -> Reply {
        let name = msg.get("name").and_then(Value::as_str).unwrap_or("");
        if name != "append" {
            return Err(dberr("NotSupported", format!("operation {} is not implemented by the Rust worker", py_repr_value(&Value::str(name)))));
        }
        let empty = Value::Map(vec![]);
        let args = msg.get("args").unwrap_or(&empty);
        let table = args.get("table").and_then(Value::as_str).ok_or_else(|| qerr("append needs a table", "PROTOCOL"))?.to_string();
        let columns: Vec<String> = match args.get("columns") {
            Some(Value::List(l)) => l.iter().filter_map(|v| v.as_str().map(String::from)).collect(),
            _ => vec![],
        };
        let rows: &[Value] = match args.get("rows") {
            Some(Value::List(l)) => l,
            _ => &[],
        };
        if !self.write {
            return Err(qerr("attempt to write a read-only connection (append)", "READ_ONLY"));
        }
        // "check": decline (the parent then creates the table or adds
        // columns the default way) unless the table has every column, so
        // the common case is one round trip, as on the C++ worker.
        if args.get("check").is_some_and(Value::is_true) {
            let have: Vec<String> = schema::table_columns(self.conn(), &table)?.iter().map(|c| c.to_lowercase()).collect();
            if have.is_empty() || columns.iter().any(|c| !have.contains(&c.to_lowercase())) {
                let declined = Value::obj(vec![("declined", Value::Bool(true))]);
                return Ok(Some(self.with_tx(Value::obj(vec![("result", declined)]))));
            }
        }
        let r = self.statement(ctl, None, |w| {
            let mut app = w.conn().appender(&table, &columns)?;
            let mut params: Vec<Param> = Vec::with_capacity(columns.len());
            let res = (|| -> Result<(), Fail> {
                for row in rows {
                    let Value::List(cells) = row else {
                        return Err(Fail::Builtin { class: "ValueError", message: "append rows must be lists".into() });
                    };
                    params.clear();
                    for c in cells {
                        params.push(convert::param(c).map_err(param_fail)?);
                    }
                    app.row(&params)?;
                }
                app.flush()?;
                Ok(())
            })();
            if res.is_err() {
                app.clear(); // never let buffered rows be flushed later
            }
            res
        });
        self.guarded(r)?;
        Ok(Some(self.with_tx(Value::obj(vec![("result", Value::obj(vec![("rowcount", Value::Int(rows.len() as i128))]))]))))
    }

    // -- streams --------------------------------------------------------------------

    fn stream(&mut self, ctl: &Arc<JobCtl>, msg: &Value) -> Reply {
        let chunk_rows = msg.opt("chunk_rows").and_then(Value::as_int_lossy).unwrap_or(1000);
        if chunk_rows < 1 {
            return Err(Fail::Builtin { class: "ValueError", message: "chunk_rows must be at least 1".into() });
        }
        let s = Stream {
            id: ctl.id.clone(),
            sql: msg.get("sql").and_then(Value::as_str).unwrap_or("").to_string(),
            params: msg.opt("params").cloned(),
            chunk_rows: chunk_rows as usize,
            remaining_ms: msg.opt("time_limit_ms").and_then(Value::as_f64),
            max_bytes: msg.opt("max_result_bytes").and_then(Value::as_int_lossy).map(|b| b.max(0) as u64),
            bytes: 0,
            started: false,
            wrapped: false,
            done: false,
            first: true,
            columns: vec![],
            rows: None,
            credit: msg.opt("credit").and_then(Value::as_int_lossy).unwrap_or(1),
        };
        let key = ctl.id.key();
        lock(&self.g.streams).insert(key.clone(), self.id);
        self.streams.insert(key.clone(), s);
        self.pump(ctl, &key)
    }

    fn more(&mut self, ctl: &Arc<JobCtl>, credit: i64) -> Reply {
        let key = ctl.id.key();
        match self.streams.get_mut(&key) {
            None => Ok(None), // finished or closed meanwhile
            Some(s) => {
                s.credit += credit;
                self.pump(ctl, &key)
            }
        }
    }

    fn close_stream_request(&mut self, ctl: &Arc<JobCtl>) -> Reply {
        let key = ctl.id.key();
        if !self.streams.contains_key(&key) {
            return Ok(None); // its last frame was already sent
        }
        self.drop_stream(&key);
        Ok(Some(Value::obj(vec![("end", Value::Bool(true)), ("cancelled", Value::Bool(true))])))
    }

    /// Send up to `credit` frames of rows (`Server._pump`).
    fn pump(&mut self, ctl: &Arc<JobCtl>, key: &str) -> Reply {
        loop {
            let Some(s) = self.streams.get(key) else { return Ok(None) };
            if s.credit <= 0 {
                return Ok(None);
            }
            if ctl.is_cancelled() {
                return Err(interrupted("query cancelled"));
            }
            let chunk = self.next_chunk(ctl, key)?;
            let s = self.streams.get_mut(key).expect("stream");
            let mut msg = Value::obj(vec![("id", s.id.clone())]);
            if s.first {
                msg.set("columns", Value::List(s.columns.iter().map(|(n, _)| Value::str(n)).collect()));
                if !s.columns.is_empty() {
                    msg.set("types", Value::List(s.columns.iter().map(|(_, t)| Value::Str(t.name())).collect()));
                }
                s.first = false;
            }
            match chunk {
                None => {
                    self.drop_stream(key);
                    msg.set("end", Value::Bool(true));
                    self.g.out.send(&msg);
                    return Ok(None);
                }
                Some(rows) => {
                    s.credit -= 1;
                    msg.set("rows", Value::List(rows));
                    self.g.out.send(&msg);
                }
            }
        }
    }

    /// `_DuckDBStream.__anext__`: the next chunk, or `None` at the end; the
    /// stream is closed (its read-only transaction ended) on the end or an
    /// error.
    fn next_chunk(&mut self, ctl: &Arc<JobCtl>, key: &str) -> Result<Option<Vec<Value>>, Fail> {
        let r = self.next_chunk_inner(ctl, key);
        let r = self.guarded(r);
        match &r {
            Ok(None) => self.finish_stream(key, true),
            Err(_) => self.finish_stream(key, false),
            Ok(Some(_)) => {}
        }
        r
    }

    fn next_chunk_inner(&mut self, ctl: &Arc<JobCtl>, key: &str) -> Result<Option<Vec<Value>>, Fail> {
        let mut s = self.streams.remove(key).expect("stream");
        if s.done {
            self.streams.insert(key.to_string(), s);
            return Ok(None);
        }
        let started_at = Instant::now();
        let budget = s.remaining_ms;
        let r = if budget.is_some_and(|b| b <= 0.0) {
            Err(interrupted("query interrupted (time limit exceeded)"))
        } else {
            self.statement(ctl, budget, |w| -> Result<Option<Vec<Value>>, Fail> {
                if !s.started {
                    s.started = true;
                    let Some((mut p, kind)) = w.prepare(&s.sql)? else {
                        return Ok(None);
                    };
                    s.wrapped = w.ro_wrapper()?;
                    let res = w.start(kind, &mut p, s.params.as_ref())?;
                    let keep = sql::is_result(kind) || (sql::is_dml(kind) && res.return_type() == ReturnType::Rows);
                    if !keep || res.columns.is_empty() {
                        return Ok(None);
                    }
                    if let Some((_, t)) = res.columns.iter().find(|(_, t)| t.unsupported().is_some()) {
                        return Err(dberr("NotSupported", format!("result type {} is not supported by the Rust worker", t.name())));
                    }
                    s.columns = res.columns.clone();
                    let tz = w.tz_for(&s.columns);
                    s.rows = Some(RowSource { res, _prepared: Some(p), chunk: None, pos: 0, exhausted: false, tz });
                }
                let Some(src) = s.rows.as_mut() else { return Ok(None) };
                let rows = src.take(s.chunk_rows)?;
                Ok(if rows.is_empty() { None } else { Some(rows) })
            })
        };
        if let Some(b) = s.remaining_ms.as_mut() {
            *b -= started_at.elapsed().as_secs_f64() * 1000.0;
        }
        let r = match r {
            Ok(Some(rows)) => {
                if let Some(mb) = s.max_bytes {
                    s.bytes += rows.iter().map(row_bytes).sum::<u64>();
                    if s.bytes > mb {
                        self.streams.insert(key.to_string(), s);
                        return Err(dberr("ResourceLimitExceeded", format!("result exceeded max_result_bytes={mb}")));
                    }
                }
                Ok(Some(rows))
            }
            other => other,
        };
        self.streams.insert(key.to_string(), s);
        r
    }

    /// `_DuckDBStream.aclose`: discard the result and end the read-only
    /// transaction (COMMIT after a clean end, ROLLBACK after an error).
    fn finish_stream(&mut self, key: &str, ok: bool) {
        let (wrapped, rows) = match self.streams.get_mut(key) {
            Some(s) if !s.done => {
                s.done = true;
                (std::mem::take(&mut s.wrapped), s.rows.take())
            }
            _ => (false, None),
        };
        drop(rows); // the result, before its transaction ends
        if wrapped {
            let _ = self.end_wrapper(ok);
        }
    }

    fn drop_stream(&mut self, key: &str) {
        self.finish_stream(key, true);
        self.streams.remove(key);
        lock(&self.g.streams).remove(key);
    }

    // -- closing ----------------------------------------------------------------------

    fn close(&mut self) {
        if self.conn.is_none() {
            return;
        }
        let keys: Vec<String> = self.streams.keys().cloned().collect();
        for k in keys {
            self.drop_stream(&k);
        }
        self.clear_tx();
        let _ = self.duck_rollback();
        {
            // No interrupt may reach the connection once it is gone.
            let mut st = lock(&self.shared.state);
            st.closed = true;
        }
        if let Some(c) = self.conn.take() {
            release_db(&self.g, &self.db_key, c, !self.dirty.get());
        }
    }
}

fn on_deadline(shared: &Arc<ConnShared>, timer: &Arc<Timer>, tx: &str) {
    let (current, stmt) = {
        let mut st = lock(&shared.state);
        if st.deadline_tx.as_deref() != Some(tx) || st.tx.as_deref() != Some(tx) {
            return;
        }
        st.deadline_tx = None;
        st.expired.push(tx.to_string());
        let n = st.expired.len();
        if n > 64 {
            st.expired.drain(..n - 64);
        }
        st.expire_pending = Some(tx.to_string());
        if let Some(c) = &st.current {
            c.cancel("deadline");
        }
        (st.current.clone(), st.active)
    };
    if current.is_some() && stmt != 0 {
        interrupt_while_active(shared, timer, stmt);
    }
    let _ = shared.sender.send(Job { ctl: JobCtl::new(Value::Null), work: Work::Expire });
}

/// A statement that leaves state in the DuckDB session, which a reused
/// connection would carry over to its next user: temporary objects,
/// variables, prepared statements, attached databases, the current schema.
fn leaves_session_state(kind: &str, sql_text: &str) -> bool {
    match kind {
        "SET" | "VARIABLE_SET" | "PREPARE" | "ATTACH" | "DETACH" | "CREATE_FUNC" => true,
        "CREATE" | "DROP" | "ALTER" => sql_text.to_ascii_uppercase().contains("TEMP"),
        _ => false,
    }
}

fn param_fail(e: convert::ParamError) -> Fail {
    let kind = if e.0 == "InvalidInputException" { errors::INVALID_INPUT } else { errors::INVALID };
    Fail::Duck(DuckError::new(kind, e.1))
}

/// Bind parameters as the Python client does: a list binds `?`/`$1` by
/// position, a dict binds `$name` case-insensitively; extra and missing
/// parameters are errors with the client's messages.
fn bind_params(p: &mut Prepared, params: Option<&Value>) -> Result<(), Fail> {
    let n = p.nparams();
    let mut provided: Vec<(String, &Value)> = Vec::new();
    match params {
        None | Some(Value::Null) => {}
        Some(Value::List(items)) => {
            for (i, v) in items.iter().enumerate() {
                provided.push(((i + 1).to_string(), v));
            }
        }
        Some(Value::Map(items)) => {
            for (k, v) in items {
                let key = match k {
                    Value::Str(s) => s.to_lowercase(),
                    other => py_repr_value(other),
                };
                if let Some(slot) = provided.iter_mut().find(|(pk, _)| *pk == key) {
                    slot.1 = v;
                } else {
                    provided.push((key, v));
                }
            }
        }
        Some(other) => {
            return Err(Fail::Duck(DuckError::new(
                errors::INVALID_INPUT,
                format!("Invalid Input Error: Prepared parameters can only be passed as a list or a dictionary, not {other:?}"),
            )));
        }
    }
    if n == 0 {
        if provided.is_empty() {
            return Ok(());
        }
        return Err(Fail::Duck(DuckError::new(
            errors::BINDER,
            format!("Binder Error: Parameter/argument count mismatch for prepared statement. Expected 0, got {}", provided.len()),
        )));
    }
    let names: Vec<String> = (1..=n).map(|i| p.param_name(i)).collect();
    let lowered: Vec<String> = names.iter().map(|s| s.to_lowercase()).collect();
    let excess: Vec<&str> = provided.iter().filter(|(k, _)| !lowered.contains(k)).map(|(k, _)| k.as_str()).collect();
    if !excess.is_empty() {
        return Err(Fail::Duck(DuckError::new(
            errors::INVALID_INPUT,
            format!("Invalid Input Error: Parameter argument/count mismatch, identifiers of the excess parameters: {}", excess.join(", ")),
        )));
    }
    let missing: Vec<&str> = names
        .iter()
        .zip(&lowered)
        .filter(|(_, l)| !provided.iter().any(|(k, _)| k == *l))
        .map(|(n, _)| n.as_str())
        .collect();
    if !missing.is_empty() {
        return Err(Fail::Duck(DuckError::new(
            errors::INVALID_INPUT,
            format!("Invalid Input Error: Values were not provided for the following parameters: {}", missing.join(", ")),
        )));
    }
    for (i, l) in lowered.iter().enumerate() {
        let v = provided.iter().find(|(k, _)| k == l).map(|(_, v)| *v).expect("checked above");
        let param = convert::param(v).map_err(param_fail)?;
        p.bind(i + 1, &param)?;
    }
    Ok(())
}
