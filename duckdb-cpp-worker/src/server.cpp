// The request loop: framing on stdin, routing to connection threads,
// control messages, transactions and deadlines, shutdown. See worker.cpp
// for the thread layout.
#include "server.hpp"

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>
#include <unistd.h>

namespace dbw {

extern bool g_rlimit_as;
Timer g_timer;
std::atomic<uint64_t> g_settings_gen{1};

namespace {

const double REINTERRUPT_S = 0.005;
const double SHUTDOWN_GRACE_S = 5.0;

bool g_debug = false;
std::mutex g_mu;  // conns, streams, thread count
std::map<int64_t, ConnPtr> g_conns;
std::map<int64_t, std::weak_ptr<Conn>> g_streams;
int64_t g_next_conn = 1;
int g_threads = 0;
std::condition_variable g_threads_cv;

std::mutex g_inst_mu;  // instances: opened and closed one at a time
std::map<std::string, std::shared_ptr<Instance>> g_instances;

}  // namespace

// -- timer -------------------------------------------------------------------------

uint64_t Timer::schedule(double delay_s, std::function<void()> fn) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!started_) {
        started_ = true;
        std::thread([this] { loop(); }).detach();
    }
    uint64_t token = next_++;
    auto at = std::chrono::steady_clock::now() +
              std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::duration<double>(delay_s));
    heap_.emplace(at, token);
    live_.emplace(token, std::move(fn));
    cv_.notify_one();
    return token;
}

void Timer::cancel(uint64_t token) {
    if (token == 0) return;
    std::lock_guard<std::mutex> lock(mu_);
    live_.erase(token);
}

void Timer::loop() {
    std::unique_lock<std::mutex> lock(mu_);
    for (;;) {
        while (!heap_.empty() && live_.find(heap_.begin()->second) == live_.end())
            heap_.erase(heap_.begin());
        if (heap_.empty()) {
            cv_.wait(lock);
            continue;
        }
        auto at = heap_.begin()->first;
        if (std::chrono::steady_clock::now() < at) {
            cv_.wait_until(lock, at);
            continue;
        }
        uint64_t token = heap_.begin()->second;
        heap_.erase(heap_.begin());
        auto it = live_.find(token);
        std::function<void()> fn = std::move(it->second);
        live_.erase(it);
        // Callbacks run without the timer lock: they take connection
        // locks and may schedule again.
        lock.unlock();
        fn();
        lock.lock();
    }
}

// -- statements and interrupts ------------------------------------------------------

ConnPtr conn_ptr(Conn &c) {
    std::lock_guard<std::mutex> lock(g_mu);
    auto it = g_conns.find(c.id);
    return it == g_conns.end() ? ConnPtr() : it->second;
}

void interrupt_while_active(const std::weak_ptr<Conn> &wc, uint64_t stmt) {
    ConnPtr c = wc.lock();
    if (!c) return;
    {
        std::lock_guard<std::mutex> lock(c->mu);
        if (!c->active || c->stmt != stmt || c->c == nullptr) return;
        duckdb_interrupt(c->c);
    }
    std::weak_ptr<Conn> w = c;
    g_timer.schedule(REINTERRUPT_S, [w, stmt] { interrupt_while_active(w, stmt); });
}

Statement::Statement(Conn &c, double time_limit_ms) : c_(c) {
    uint64_t stmt;
    {
        std::lock_guard<std::mutex> lock(c.mu);
        if (c.current && c.current->cancelled)
            throw Error("QueryInterrupted", "query interrupted (cancelled)");
        stmt = ++c.stmt;
        c.active = true;
    }
    if (time_limit_ms >= 0) {
        std::weak_ptr<Conn> w = conn_ptr(c);
        token_ = g_timer.schedule(std::max(time_limit_ms, 0.0) / 1000.0, [w, stmt] { interrupt_while_active(w, stmt); });
    }
}

Statement::~Statement() {
    g_timer.cancel(token_);
    std::lock_guard<std::mutex> lock(c_.mu);
    c_.active = false;
}

bool job_cancelled(Conn &c, const JobPtr &job) {
    std::lock_guard<std::mutex> lock(c.mu);
    return job->cancelled;
}

// Cancel `job` (under c.mu): a running statement is interrupted until it
// returns; a statement not yet started refuses to start.
static void cancel_locked(const ConnPtr &c, const JobPtr &job, const char *reason) {
    job->cancelled = true;
    job->reason = reason;
    if (c->current == job && c->active && c->c != nullptr) {
        duckdb_interrupt(c->c);
        uint64_t stmt = c->stmt;
        std::weak_ptr<Conn> w = c;
        g_timer.schedule(REINTERRUPT_S, [w, stmt] { interrupt_while_active(w, stmt); });
    }
}

void register_stream(int64_t sid, const ConnPtr &c) {
    std::lock_guard<std::mutex> lock(g_mu);
    g_streams[sid] = c;
}

void forget_stream(int64_t sid) {
    std::lock_guard<std::mutex> lock(g_mu);
    g_streams.erase(sid);
}

// -- replies ------------------------------------------------------------------------

void begin_reply(Writer &wr) {
    wr.start();
    w_map_open(wr.w());
}

void end_reply(Writer &wr, Conn *c, int64_t id, bool has_id) {
    wbuf *w = wr.w();
    if (c != nullptr) {
        bool in_tx;
        {
            std::lock_guard<std::mutex> lock(c->mu);
            in_tx = !c->tx.empty();
        }
        if (in_tx) {
            w_cstr(w, "in_tx");
            w_bool(w, 1);
        }
    }
    w_cstr(w, "id");
    if (has_id) w_int(w, id);
    else w_nil(w);
    w_end(w);
}

bool send_reply_frame(Writer &wr, int64_t id) { return wr.send(id, true); }

static void send_ok(Writer &wr, int64_t id, bool has_id, const char *extra_key = nullptr, const std::string *extra = nullptr,
                    bool expired = false) {
    begin_reply(wr);
    wbuf *w = wr.w();
    w_cstr(w, "ok");
    w_bool(w, 1);
    if (extra_key) {
        w_cstr(w, extra_key);
        w_cstr(w, extra->c_str());
    }
    if (expired) {
        w_cstr(w, "expired");
        w_bool(w, 1);
    }
    end_reply(wr, nullptr, id, has_id);
    wr.send(id, has_id);
}

static void send_error(Writer &wr, int64_t id, bool has_id, const Error &e, Conn *c) {
    wr.start();
    wbuf *w = wr.w();
    w_map_open(w);
    w_cstr(w, "error");
    write_error(wr, e);
    end_reply(wr, c, id, has_id);
    wr.send(id, has_id);
}

// -- request helpers ------------------------------------------------------------------

std::string val_str(const val *v) {
    if (v == nullptr) return "";
    if (v->t == V_STR) return std::string(v->u.s.p, v->u.s.n);
    return "";
}

std::string Msg::str(const char *key, const char *dflt) const {
    const val *x = get(key);
    if (x == nullptr || x->t != V_STR) return dflt;
    return std::string(x->u.s.p, x->u.s.n);
}

bool Msg::has(const char *key) const {
    const val *x = get(key);
    return x != nullptr && x->t != V_NIL;
}

int64_t int_field(const Msg &m, const char *key, int64_t dflt) {
    const val *x = m.get(key);
    if (x == nullptr || x->t == V_NIL) return dflt;
    if (x->t == V_INT) return x->u.i;
    if (x->t == V_FLOAT) return (int64_t)x->u.d;
    if (x->t == V_BOOL) return x->u.b;
    return dflt;
}

double time_limit_of(const Msg &m) {
    const val *x = m.get("time_limit_ms");
    if (x == nullptr || x->t == V_NIL) return -1;
    if (x->t == V_INT) return (double)x->u.i;
    if (x->t == V_FLOAT) return x->u.d;
    return -1;
}

static void check_tx(Conn &c, const Msg &m) {
    const val *t = m.get("tx");
    if (t == nullptr || t->t != V_STR) return;
    std::string tx(t->u.s.p, t->u.s.n);
    std::lock_guard<std::mutex> lock(c.mu);
    if (tx == c.tx) return;
    for (const std::string &e : c.expired)
        if (e == tx)
            throw Error("TransactionExpired", "transaction " + tx + " passed its deadline and was rolled back");
    throw Error("TransactionClosed", "transaction " + tx + " is not open");
}

void run_sql_simple(Conn &c, const char *sql) {
    duckdb_result res;
    if (duckdb_query(c.c, sql, &res) != DuckDBSuccess) {
        std::string msg = duckdb_result_error(&res) ? duckdb_result_error(&res) : "unknown error";
        duckdb_destroy_result(&res);
        throw duckdb_error(msg);
    }
    duckdb_destroy_result(&res);
}

// -- instances ---------------------------------------------------------------------------

static std::string config_value_text(const val *v) {
    switch (v->t) {
    case V_BOOL: return v->u.b ? "true" : "false";
    case V_INT: return std::to_string(v->u.i);
    case V_FLOAT: {
        char b[64];
        snprintf(b, sizeof b, "%.17g", v->u.d);
        return b;
    }
    case V_STR: return std::string(v->u.s.p, v->u.s.n);
    default: return "";
    }
}

// The DuckDB(path, **config) of the Python backend: memory_limit,
// threads, read_only, allow_external_access, config, create.
static std::shared_ptr<Instance> open_instance(const std::string &path, const val *cfg) {
    std::vector<std::pair<std::string, std::string>> settings;
    bool read_only = false, external = false, create = true;
    std::string key = path;
    if (cfg != nullptr && cfg->t == V_MAP) {
        for (size_t i = 0; i < cfg->u.a.n; i++) {
            const val *k = &cfg->u.a.v[2 * i], *v = &cfg->u.a.v[2 * i + 1];
            std::string name = val_str(k);
            key += "\x1f" + name + "=" + (v->t == V_MAP ? std::string("{...}") : config_value_text(v));
            if (name == "memory_limit" || name == "threads") {
                if (v->t != V_NIL) settings.emplace_back(name, config_value_text(v));
            } else if (name == "read_only") {
                read_only = v->t == V_BOOL && v->u.b;
            } else if (name == "allow_external_access") {
                external = v->t == V_BOOL && v->u.b;
            } else if (name == "create") {
                create = !(v->t == V_BOOL && !v->u.b);
            } else if (name == "config" && v->t == V_MAP) {
                for (size_t j = 0; j < v->u.a.n; j++) {
                    std::string ek = val_str(&v->u.a.v[2 * j]);
                    std::string ev = config_value_text(&v->u.a.v[2 * j + 1]);
                    key += "," + ek + "=" + ev;
                }
            } else {
                throw Error("NotSupported", "the DuckDB C++ worker does not support the option " + py_repr(name));
            }
        }
        // Extra settings last, as DuckDB._config() applies them.
        if (!external) settings.emplace_back("enable_external_access", "false");
        const val *extra = map_get(cfg, "config");
        if (extra != nullptr && extra->t == V_MAP)
            for (size_t j = 0; j < extra->u.a.n; j++)
                settings.emplace_back(val_str(&extra->u.a.v[2 * j]), config_value_text(&extra->u.a.v[2 * j + 1]));
    } else {
        settings.emplace_back("enable_external_access", "false");
    }
    if (read_only) {
        create = false;
        settings.emplace_back("access_mode", "READ_ONLY");
    }

    std::lock_guard<std::mutex> lock(g_inst_mu);
    auto it = g_instances.find(key);
    if (it != g_instances.end()) {
        it->second->conns++;
        return it->second;
    }
    struct stat st;
    if (!create && stat(path.c_str(), &st) != 0)
        throw query_error("unable to open database file " + py_repr(path), "CANTOPEN");
    duckdb_config config;
    if (duckdb_create_config(&config) != DuckDBSuccess) throw Error("ResourceLimitExceeded", "out of memory");
    for (auto &kv : settings) {
        if (duckdb_set_config(config, kv.first.c_str(), kv.second.c_str()) != DuckDBSuccess) {
            duckdb_destroy_config(&config);
            throw query_error("Invalid Input Error: The following options were not recognized: " + kv.first,
                              "InvalidInput");
        }
    }
    auto inst = std::make_shared<Instance>();
    inst->key = key;
    inst->path = path;
    char *err = nullptr;
    duckdb_state s = duckdb_open_ext(path.c_str(), &inst->db, config, &err);
    duckdb_destroy_config(&config);
    if (s != DuckDBSuccess) {
        std::string msg = err ? err : "unable to open database";
        if (err) duckdb_free(err);
        throw duckdb_error(msg);
    }
    // Lock the configuration, so SQL cannot raise memory_limit or turn
    // external access back on.
    duckdb_connection tmp;
    if (duckdb_connect(inst->db, &tmp) != DuckDBSuccess) {
        duckdb_close(&inst->db);
        throw Error("QueryError", "could not connect to the database");
    }
    duckdb_result res;
    bool ok = duckdb_query(tmp, "SET lock_configuration = true", &res) == DuckDBSuccess;
    std::string msg = ok ? "" : (duckdb_result_error(&res) ? duckdb_result_error(&res) : "");
    duckdb_destroy_result(&res);
    duckdb_disconnect(&tmp);
    if (!ok) {
        duckdb_close(&inst->db);
        throw duckdb_error(msg);
    }
    inst->conns = 1;
    g_instances[key] = inst;
    return inst;
}

static void release_instance(const std::shared_ptr<Instance> &inst) {
    if (!inst) return;
    std::lock_guard<std::mutex> lock(g_inst_mu);
    if (--inst->conns > 0) return;
    g_instances.erase(inst->key);
    // Closing checkpoints the WAL into the file and releases the lock.
    duckdb_close(&inst->db);
}

// -- transactions -----------------------------------------------------------------------

static void clear_tx(Conn &c) {
    uint64_t token;
    {
        std::lock_guard<std::mutex> lock(c.mu);
        c.tx.clear();
        token = c.deadline_token;
        c.deadline_token = 0;
    }
    g_timer.cancel(token);
}

static void on_deadline(const std::weak_ptr<Conn> &wc, const std::string &tx) {
    ConnPtr c = wc.lock();
    if (!c) return;
    std::lock_guard<std::mutex> lock(c->mu);
    if (c->tx != tx) return;
    c->deadline_token = 0;
    c->expired.push_back(tx);
    while (c->expired.size() > 64) c->expired.pop_front();
    if (c->current) cancel_locked(c, c->current, "deadline");
    auto job = std::make_shared<Job>();
    job->type = "_expire";
    job->tx = tx;
    c->q.push_front(job);
    c->cv.notify_one();
}

static void do_begin(Conn &c, Writer &wr, const JobPtr &job) {
    const Msg &m = *job->msg;
    std::string mode = m.str("mode", "write");
    {
        std::lock_guard<std::mutex> lock(c.mu);
        if (!c.tx.empty()) throw query_error("cannot start a transaction within a transaction", "SQLITE_ERROR");
    }
    const char *sql;
    if (mode == "write") sql = "BEGIN TRANSACTION";
    else if (mode == "read") sql = "BEGIN TRANSACTION READ ONLY";
    else {
        Error e("ValueError", "unknown transaction mode " + py_repr(mode));
        e.builtin = true;
        throw e;
    }
    if (job_cancelled(c, job)) throw Error("QueryInterrupted", "query interrupted (cancelled)");
    run_sql_simple(c, sql);
    c.duck_in_tx = true;
    c.tx_failed = false;
    std::string tx = "t" + std::to_string(++c.tx_seq);
    {
        std::lock_guard<std::mutex> lock(c.mu);
        c.tx = tx;
    }
    int64_t deadline_ms = int_field(m, "deadline_ms", -1);
    if (m.has("deadline_ms")) {
        std::weak_ptr<Conn> w = conn_ptr(c);
        uint64_t token = g_timer.schedule((double)deadline_ms / 1000.0, [w, tx] { on_deadline(w, tx); });
        std::lock_guard<std::mutex> lock(c.mu);
        c.deadline_token = token;
    }
    send_ok(wr, job->id, job->has_id, "tx", &tx);
}

// DuckDBConnection.commit(): DuckDB turns COMMIT of a transaction aborted
// by an earlier error into a silent rollback; refuse that instead.
static void duck_commit(Conn &c) {
    if (!c.duck_in_tx) return;
    struct Reset {
        Conn &c;
        ~Reset() {
            c.duck_in_tx = false;
            c.tx_failed = false;
        }
    } reset{c};
    if (c.tx_failed) {
        duckdb_result res;
        bool ok = duckdb_query(c.c, "SELECT 1", &res) == DuckDBSuccess;
        std::string msg = ok ? "" : (duckdb_result_error(&res) ? duckdb_result_error(&res) : "");
        duckdb_destroy_result(&res);
        if (!ok && duckdb_exception_class(msg) == "TransactionException") {
            run_sql_simple(c, "ROLLBACK");
            throw query_error(
                "the transaction was aborted by an earlier error and has been rolled back (DuckDB cannot continue a "
                "transaction after a failed statement)",
                "TRANSACTION_ABORTED");
        }
        if (!ok) throw duckdb_error(msg);
    }
    run_sql_simple(c, "COMMIT");
}

static void duck_rollback(Conn &c) {
    if (!c.duck_in_tx) return;
    struct Reset {
        Conn &c;
        ~Reset() {
            c.duck_in_tx = false;
            c.tx_failed = false;
        }
    } reset{c};
    run_sql_simple(c, "ROLLBACK");
}

static void do_commit(Conn &c, Writer &wr, const JobPtr &job) {
    check_tx(c, *job->msg);
    uint64_t token;
    {
        // A commit that has started is not cut short by the deadline.
        std::lock_guard<std::mutex> lock(c.mu);
        token = c.deadline_token;
        c.deadline_token = 0;
    }
    g_timer.cancel(token);
    close_all_streams(c);
    duck_commit(c);
    clear_tx(c);
    send_ok(wr, job->id, job->has_id);
}

static void do_rollback(Conn &c, Writer &wr, const JobPtr &job) {
    const val *t = job->msg->get("tx");
    if (t != nullptr && t->t == V_STR) {
        std::string tx(t->u.s.p, t->u.s.n);
        std::unique_lock<std::mutex> lock(c.mu);
        if (tx != c.tx) {
            for (auto it = c.expired.begin(); it != c.expired.end(); ++it) {
                if (*it == tx) {
                    c.expired.erase(it);
                    lock.unlock();
                    send_ok(wr, job->id, job->has_id, nullptr, nullptr, true);
                    return;
                }
            }
        }
    }
    check_tx(c, *job->msg);
    close_all_streams(c);
    duck_rollback(c);
    clear_tx(c);
    send_ok(wr, job->id, job->has_id);
}

static void do_expire(Conn &c, Writer &wr, const JobPtr &job) {
    {
        std::lock_guard<std::mutex> lock(c.mu);
        if (c.tx != job->tx) return;
        c.tx.clear();
    }
    close_all_streams(c);
    try {
        duck_rollback(c);
    } catch (const Error &e) {
        fprintf(stderr, "dblayer-duckdb-worker: rollback at deadline failed: %s\n", e.message.c_str());
    }
    wr.start();
    wbuf *w = wr.w();
    w_map(w, 3);
    w_cstr(w, "type");
    w_cstr(w, "expired");
    w_cstr(w, "conn");
    w_int(w, c.id);
    w_cstr(w, "tx");
    w_cstr(w, job->tx.c_str());
    w_end(w);
    wr.send(0, false);
}

// -- requests on a connection -----------------------------------------------------------------

static void close_conn(Conn &c) {
    close_all_streams(c);
    clear_tx(c);
    try {
        duck_rollback(c);
    } catch (const Error &) {
    }
    {
        std::lock_guard<std::mutex> lock(c.mu);
        if (c.c != nullptr) duckdb_disconnect(&c.c);
        c.c = nullptr;
    }
    release_instance(c.inst);
    c.inst.reset();
}

static void do_execute(Conn &c, Writer &wr, const JobPtr &job) {
    const Msg &m = *job->msg;
    check_tx(c, m);
    begin_reply(wr);
    w_cstr(wr.w(), "result");
    exec_one(c, wr, m.str("sql"), m.get("params"), time_limit_of(m), int_field(m, "max_rows", -1),
             int_field(m, "max_result_bytes", -1));
    end_reply(wr, &c, job->id, job->has_id);
    wr.send(job->id, job->has_id);
}

static void do_batch(Conn &c, Writer &wr, const JobPtr &job) {
    const Msg &m = *job->msg;
    check_tx(c, m);
    const val *stmts = m.get("statements");
    double tl = time_limit_of(m);
    int64_t max_rows = int_field(m, "max_rows", -1);
    begin_reply(wr);
    w_cstr(wr.w(), "result");
    size_t n = (stmts != nullptr && stmts->t == V_ARR) ? stmts->u.a.n : 0;
    w_array(wr.w(), (uint32_t)n);
    for (size_t i = 0; i < n; i++) {
        const val *s = &stmts->u.a.v[i];
        if (s->t != V_ARR || s->u.a.n < 1) throw query_error("bad batch statement", "PROTOCOL");
        const val *params = s->u.a.n > 1 ? &s->u.a.v[1] : nullptr;
        exec_one(c, wr, val_str(&s->u.a.v[0]), params, tl, max_rows, -1);
    }
    w_end(wr.w());
    end_reply(wr, &c, job->id, job->has_id);
    wr.send(job->id, job->has_id);
}

static void do_execute_many(Conn &c, Writer &wr, const JobPtr &job) {
    const Msg &m = *job->msg;
    check_tx(c, m);
    int64_t n = exec_many(c, m.str("sql"), m.get("params_seq"));
    begin_reply(wr);
    wbuf *w = wr.w();
    w_cstr(w, "result");
    w_map(w, 1);
    w_cstr(w, "rowcount");
    w_int(w, n);
    w_end(w);
    end_reply(wr, &c, job->id, job->has_id);
    wr.send(job->id, job->has_id);
}

static void do_schema(Conn &c, Writer &wr, const JobPtr &job, bool version_only) {
    check_tx(c, *job->msg);
    std::string version;
    {
        Statement st(c, -1);
        if (version_only) {
            version = schema_version(c.c);
        } else {
            bool own = !c.duck_in_tx;
            if (own) run_sql_simple(c, "BEGIN TRANSACTION READ ONLY");
            Writer tmp;  // not used: the schema is written straight into the reply
            (void)tmp;
            begin_reply(wr);
            w_cstr(wr.w(), "result");
            try {
                write_schema(wr, c.c);
            } catch (...) {
                if (own) {
                    try {
                        run_sql_simple(c, "ROLLBACK");
                    } catch (...) {
                    }
                }
                throw;
            }
            if (own) run_sql_simple(c, "ROLLBACK");
        }
    }
    if (version_only) {
        begin_reply(wr);
        w_cstr(wr.w(), "result");
        w_cstr(wr.w(), version.c_str());
    }
    end_reply(wr, &c, job->id, job->has_id);
    wr.send(job->id, job->has_id);
}

// Python worker's translate(): a request cut short by its transaction's
// deadline reports TransactionExpired; one cut short by cancel,
// QueryInterrupted.
static Error translate(Conn &c, const JobPtr &job, const Error &e) {
    bool cancelled;
    std::string reason;
    {
        std::lock_guard<std::mutex> lock(c.mu);
        cancelled = job->cancelled;
        reason = job->reason;
    }
    if (!cancelled || !is_interrupt(e)) return e;
    Error n;
    if (reason == "deadline") {
        n = Error("TransactionExpired", "transaction passed its deadline and was rolled back by the worker");
    } else if (reason == "time_limit") {
        n = Error("QueryInterrupted", "query interrupted (time limit exceeded)");
    } else {
        n = Error("QueryInterrupted", "query cancelled");
    }
    if (!e.cause_type.empty()) {
        n.cause_type = e.cause_type;
        n.cause_message = e.cause_message;
    } else {
        n.cause_type = "__main__._Interrupted";
        n.cause_message = "request stopped by the worker (" + reason + ")";
    }
    return n;
}

static void handle(Conn &c, Writer &wr, const JobPtr &job) {
    const std::string &t = job->type;
    if (t == "execute") do_execute(c, wr, job);
    else if (t == "batch") do_batch(c, wr, job);
    else if (t == "execute_many") do_execute_many(c, wr, job);
    else if (t == "stream") stream_start(c, job);
    else if (t == "_more") stream_more(c, job);
    else if (t == "_close_stream") {
        if (c.streams.count(job->stream) == 0) return;  // its last frame was already sent
        stream_close(c, job->stream, true);
        begin_reply(wr);
        w_cstr(wr.w(), "end");
        w_bool(wr.w(), 1);
        w_cstr(wr.w(), "cancelled");
        w_bool(wr.w(), 1);
        end_reply(wr, nullptr, job->id, true);
        wr.send(job->id, true);
    } else if (t == "begin") do_begin(c, wr, job);
    else if (t == "commit") do_commit(c, wr, job);
    else if (t == "rollback") do_rollback(c, wr, job);
    else if (t == "schema") do_schema(c, wr, job, false);
    else if (t == "schema_version") do_schema(c, wr, job, true);
    else if (t == "_expire") do_expire(c, wr, job);
    else if (t == "op") {
        check_tx(c, *job->msg);
        begin_reply(wr);
        w_cstr(wr.w(), "result");
        run_op(c, wr, *job->msg);
        end_reply(wr, &c, job->id, job->has_id);
        wr.send(job->id, job->has_id);
    } else throw query_error("unknown message type " + py_repr(t), "PROTOCOL");
}

static void conn_thread(ConnPtr c, JobPtr open_job, std::string path, const val *config) {
    Writer wr;
    c->wr = &wr;
    bool opened = false;
    // Open: the instance (shared by the database's connections) and a
    // duckdb_connection of our own.
    try {
        c->inst = open_instance(path, config);
        duckdb_connection dc;
        if (duckdb_connect(c->inst->db, &dc) != DuckDBSuccess) {
            release_instance(c->inst);
            c->inst.reset();
            throw Error("QueryError", "could not connect to the database");
        }
        {
            std::lock_guard<std::mutex> lock(c->mu);
            c->c = dc;
        }
        opened = true;
        begin_reply(wr);
        w_cstr(wr.w(), "ok");
        w_bool(wr.w(), 1);
        w_cstr(wr.w(), "conn");
        w_int(wr.w(), c->id);
        end_reply(wr, nullptr, open_job->id, open_job->has_id);
        wr.send(open_job->id, open_job->has_id);
    } catch (const Error &e) {
        send_error(wr, open_job->id, open_job->has_id, e, nullptr);
    }
    open_job.reset();
    if (!opened) {
        std::lock_guard<std::mutex> lock(g_mu);
        g_conns.erase(c->id);
    }
    while (opened) {
        JobPtr job;
        {
            std::unique_lock<std::mutex> lock(c->mu);
            c->cv.wait(lock, [&] { return !c->q.empty(); });
            job = c->q.front();
            c->q.pop_front();
            c->current = job;
        }
        if (job->type == "_shutdown" || job->type == "close") {
            close_conn(*c);
            {
                std::lock_guard<std::mutex> lock(g_mu);
                g_conns.erase(c->id);
            }
            if (job->type == "close") send_ok(wr, job->id, job->has_id);
            break;
        }
        bool cancelled_before = job_cancelled(*c, job);
        try {
            if (cancelled_before) {
                Error e("QueryInterrupted", "query cancelled");
                e.cause_type = "__main__._Interrupted";
                e.cause_message = "request stopped by the worker (cancelled)";
                send_error(wr, job->id, job->has_id, e, nullptr);
            } else {
                handle(*c, wr, job);
            }
        } catch (const Error &raw) {
            if (raw.duckdb && c->duck_in_tx) c->tx_failed = true;
            Error e = translate(*c, job, raw);
            send_error(wr, job->id, job->has_id, e, c.get());
            if (job->type == "stream" || job->type == "_more") {
                int64_t sid = job->type == "stream" ? job->id : job->stream;
                if (c->streams.count(sid)) stream_close(*c, sid, false);
            }
        } catch (const std::bad_alloc &) {
            Error e("ResourceLimitExceeded", "worker ran out of memory (memory limit)");
            send_error(wr, job->id, job->has_id, e, c.get());
        }
        std::lock_guard<std::mutex> lock(c->mu);
        c->current.reset();
    }
    c->wr = nullptr;
    std::lock_guard<std::mutex> lock(g_mu);
    g_threads--;
    g_threads_cv.notify_all();
}

// -- the main thread ---------------------------------------------------------------------------

namespace {

Writer *g_main_wr = nullptr;

void do_hello(const Msg &m) {
    const val *codecs = m.get("codecs");
    int codec = CODEC_JSON;
    const char *name = "json";
    if (codecs != nullptr && codecs->t == V_ARR) {
        for (size_t i = 0; i < codecs->u.a.n; i++) {
            if (val_is_str(&codecs->u.a.v[i], "msgpack")) {
                codec = CODEC_MSGPACK;
                name = "msgpack";
                break;
            }
            if (val_is_str(&codecs->u.a.v[i], "json")) break;
        }
    }
    int64_t cap = int_field(m, "max_frame_bytes", -1);
    if (cap > 0) g_wire.max_frame_bytes = (size_t)cap;
    Writer &wr = *g_main_wr;
    wr.start(CODEC_JSON);
    wbuf *w = wr.w();
    w_map_open(w);
    w_cstr(w, "type");
    w_cstr(w, "hello");
    w_cstr(w, "protocol");
    w_int(w, 1);
    w_cstr(w, "codec");
    w_cstr(w, name);
    w_cstr(w, "duckdb_version");
    w_cstr(w, duckdb_library_version());
    w_cstr(w, "pid");
    w_int(w, (int64_t)getpid());
    w_cstr(w, "rlimit_as");
    w_bool(w, g_rlimit_as);
    w_cstr(w, "implementation");
    w_cstr(w, "cpp");
    w_cstr(w, "ops");
    w_array(w, 1);
    w_cstr(w, "insert_rows");
    w_end(w);
    w_cstr(w, "concurrent");
    w_bool(w, 1);
    int64_t id = int_field(m, "id", 0);
    w_cstr(w, "id");
    if (m.has("id")) w_int(w, id);
    else w_nil(w);
    w_end(w);
    wr.send(id, m.has("id"));
    g_wire.codec = codec;  // everything after hello in the agreed codec
}

void enqueue(const ConnPtr &c, const JobPtr &job, bool front = false) {
    std::lock_guard<std::mutex> lock(c->mu);
    if (front) c->q.push_front(job);
    else c->q.push_back(job);
    c->cv.notify_one();
}

ConnPtr conn_for(const Msg &m) {
    std::lock_guard<std::mutex> lock(g_mu);
    const val *cid = m.get("conn");
    if (cid == nullptr || cid->t == V_NIL) {
        if (g_conns.size() == 1) return g_conns.begin()->second;
        throw query_error("request has no 'conn' and the worker has " + std::to_string(g_conns.size()) + " connections",
                          "PROTOCOL");
    }
    int64_t id = cid->t == V_INT ? cid->u.i : -1;
    auto it = g_conns.find(id);
    if (it == g_conns.end())
        throw query_error("no connection " + std::to_string(id) + " in this worker", "PROTOCOL");
    return it->second;
}

std::vector<ConnPtr> all_conns() {
    std::lock_guard<std::mutex> lock(g_mu);
    std::vector<ConnPtr> out;
    for (auto &kv : g_conns) out.push_back(kv.second);
    return out;
}

// Stop everything: cancel what runs, drop what is queued, close every
// connection (rolling back), wait up to `grace` for the threads.
void shutdown_all(double grace) {
    for (const ConnPtr &c : all_conns()) {
        std::lock_guard<std::mutex> lock(c->mu);
        for (auto &j : c->q) j->cancelled = true;
        c->q.clear();
        if (c->current) cancel_locked(c, c->current, "cancelled");
        auto job = std::make_shared<Job>();
        job->type = "_shutdown";
        c->q.push_front(job);
        c->cv.notify_one();
    }
    std::unique_lock<std::mutex> lock(g_mu);
    g_threads_cv.wait_for(lock, std::chrono::duration<double>(grace), [] { return g_threads == 0; });
}

void do_open(std::unique_ptr<Msg> msg, int64_t id, bool has_id) {
    std::string engine = msg->str("engine");
    if (engine.find("duckdb") == std::string::npos) {
        Error e("NotSupported", "the DuckDB C++ worker runs DuckDB only; engine " +
                                    py_repr(engine.empty() ? "dblayer.worker.engines:sqlite" : engine) +
                                    " is not supported");
        send_error(*g_main_wr, id, has_id, e, nullptr);
        return;
    }
    std::string path = msg->str("path");
    if (path.empty()) {
        send_error(*g_main_wr, id, has_id, query_error("open needs a path", "PROTOCOL"), nullptr);
        return;
    }
    auto c = std::make_shared<Conn>();
    c->write = msg->str("mode", "read") == "write";
    auto job = std::make_shared<Job>();
    job->id = id;
    job->has_id = has_id;
    job->type = "open";
    const val *config = msg->get("config");
    job->msg = std::move(msg);
    {
        std::lock_guard<std::mutex> lock(g_mu);
        c->id = g_next_conn++;
        g_conns[c->id] = c;
        g_threads++;
    }
    std::thread(conn_thread, c, job, path, config).detach();
}

[[noreturn]] void exit_now(int code) {
    fflush(stderr);
    _exit(code);
}

void do_debug(const Msg &m, int64_t id, bool has_id) {
    std::string action = m.str("action");
    Writer &wr = *g_main_wr;
    if (action == "sleep") {  // blocks the whole worker: nothing reads stdin meanwhile
        double s = 1;
        const val *x = m.get("seconds");
        if (x && x->t == V_INT) s = (double)x->u.i;
        if (x && x->t == V_FLOAT) s = x->u.d;
        usleep((useconds_t)(s * 1e6));
        send_ok(wr, id, has_id);
    } else if (action == "oversized_frame") {
        uint8_t b[20] = {0x80, 0, 0, 0};
        memset(b + 4, 'x', 16);
        write_frame(b, sizeof b);
    } else if (action == "truncated_frame") {
        uint8_t b[10] = {0, 0, 0, 100, '{', '"', 'i', 'd', '"', ':'};
        write_frame(b, sizeof b);
        exit_now(3);
    } else if (action == "garbage") {
        const char *g = "\xff\xfe not a valid payload";
        size_t n = strlen(g);
        std::vector<uint8_t> b(4 + n);
        b[0] = (uint8_t)(n >> 24), b[1] = (uint8_t)(n >> 16), b[2] = (uint8_t)(n >> 8), b[3] = (uint8_t)n;
        memcpy(b.data() + 4, g, n);
        write_frame(b.data(), b.size());
    } else if (action == "alloc") {
        int64_t mb = int_field(m, "mb", 0);
        std::vector<void *> blocks;
        bool failed = false;
        for (int64_t i = 0; i < mb; i++) {
            void *p = malloc(1024 * 1024);
            if (!p) {
                failed = true;
                break;
            }
            memset(p, 1, 1024 * 1024);
            blocks.push_back(p);
        }
        for (void *p : blocks) free(p);
        if (failed) {
            send_error(wr, id, has_id, Error("ResourceLimitExceeded", "worker ran out of memory (memory limit)"), nullptr);
        } else {
            begin_reply(wr);
            w_cstr(wr.w(), "ok");
            w_bool(wr.w(), 1);
            w_cstr(wr.w(), "allocated_mb");
            w_int(wr.w(), mb);
            end_reply(wr, nullptr, id, has_id);
            wr.send(id, has_id);
        }
    } else if (action == "exit") {
        exit_now((int)int_field(m, "code", 1));
    } else if (action == "print") {
        std::string text = m.str("text");
        fprintf(stdout, "%s\n", text.c_str());  // fd 1 is stderr here
        fflush(stdout);
        send_ok(wr, id, has_id);
    } else {
        send_error(wr, id, has_id, query_error("unknown debug action " + py_repr(action), "PROTOCOL"), nullptr);
    }
}

void dispatch(std::unique_ptr<Msg> msg) {
    const Msg &m = *msg;
    std::string type = m.str("type");
    bool has_id = m.has("id");
    int64_t id = int_field(m, "id", 0);
    if (type == "hello") {
        do_hello(m);
        return;
    }
    if (type == "cancel") {
        int64_t target = int_field(m, "target", 0);
        for (const ConnPtr &c : all_conns()) {
            std::lock_guard<std::mutex> lock(c->mu);
            if (c->current && c->current->has_id && c->current->id == target && c->current->type != "_close_stream") {
                cancel_locked(c, c->current, "cancelled");
                return;
            }
            for (auto &j : c->q) {
                if (j->has_id && j->id == target && j->type != "_close_stream") {
                    j->cancelled = true;
                    return;
                }
            }
        }
        ConnPtr owner;
        {
            std::lock_guard<std::mutex> lock(g_mu);
            auto it = g_streams.find(target);
            if (it != g_streams.end()) owner = it->second.lock();
        }
        if (owner) {
            auto job = std::make_shared<Job>();
            job->type = "_close_stream";
            job->id = target;
            job->has_id = true;
            job->stream = target;
            enqueue(owner, job);
        }
        return;
    }
    if (type == "more") {
        int64_t sid = int_field(m, "stream", 0);
        ConnPtr owner;
        {
            std::lock_guard<std::mutex> lock(g_mu);
            auto it = g_streams.find(sid);
            if (it != g_streams.end()) owner = it->second.lock();
        }
        if (owner) {
            auto job = std::make_shared<Job>();
            job->type = "_more";
            job->id = sid;
            job->has_id = true;
            job->stream = sid;
            job->credit = int_field(m, "credit", 1);
            enqueue(owner, job);
        }
        return;
    }
    if (type == "open") {
        do_open(std::move(msg), id, has_id);
        return;
    }
    if (type == "sleep") {
        shutdown_all(SHUTDOWN_GRACE_S);
        send_ok(*g_main_wr, id, has_id);
        exit_now(0);
    }
    if (type == "_debug") {
        if (!g_debug) {
            send_error(*g_main_wr, id, has_id, query_error("unknown message type '_debug'", "PROTOCOL"), nullptr);
            return;
        }
        do_debug(m, id, has_id);
        return;
    }
    ConnPtr c;
    try {
        c = conn_for(m);
    } catch (const Error &e) {
        send_error(*g_main_wr, id, has_id, e, nullptr);
        return;
    }
    auto job = std::make_shared<Job>();
    job->id = id;
    job->has_id = has_id;
    job->type = type;
    job->msg = std::move(msg);
    enqueue(c, job);
}

void on_eof() {
    shutdown_all(SHUTDOWN_GRACE_S);
    exit_now(0);
}

}  // namespace

void run_server(int in_fd, bool debug, long /*memory_limit_mb*/) {
    g_debug = debug;
    Writer main_writer;
    g_main_wr = &main_writer;
    std::vector<uint8_t> buf;
    size_t start = 0;
    uint8_t chunk[1 << 18];
    for (;;) {
        ssize_t n = read(in_fd, chunk, sizeof chunk);
        if (n < 0) {
            if (errno == EINTR) continue;
            on_eof();
        }
        if (n == 0) {
            if (buf.size() - start > 0)
                fprintf(stderr, "dblayer-duckdb-worker: end of input inside a frame; dropping the connection\n");
            on_eof();
        }
        buf.insert(buf.end(), chunk, chunk + n);
        for (;;) {
            size_t avail = buf.size() - start;
            if (avail < 4) break;
            const uint8_t *p = buf.data() + start;
            uint32_t len = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3];
            if (len > g_wire.max_frame_bytes) {
                fprintf(stderr,
                        "dblayer-duckdb-worker: frame of %u bytes exceeds the %zu-byte frame size cap; dropping the "
                        "connection\n",
                        len, g_wire.max_frame_bytes);
                on_eof();
            }
            if (avail < 4 + (size_t)len) break;
            auto msg = std::make_unique<Msg>();
            msg->data.assign(p + 4, p + 4 + len);
            start += 4 + len;
            char err[200];
            // hello is always JSON; afterwards the agreed codec.
            int codec = g_wire.codec;
            if (decode_msg(codec, msg->data.data(), msg->data.size(), &msg->a, &msg->v, err, sizeof err) != 0 ||
                msg->v.t != V_MAP) {
                fprintf(stderr, "dblayer-duckdb-worker: bad input (%s); dropping the connection\n", err);
                on_eof();
            }
            dispatch(std::move(msg));
        }
        if (start > 0 && start == buf.size()) {
            buf.clear();
            start = 0;
        } else if (start > (1u << 20)) {
            buf.erase(buf.begin(), buf.begin() + (long)start);
            start = 0;
        }
    }
}

}  // namespace dbw
