// Statements on a connection thread, as DuckDBConnection runs them
// (dblayer/backends/duckdb.py): one statement per call, read-only
// enforcement by statement type plus BEGIN TRANSACTION READ ONLY, results
// streamed from DuckDB and written straight into the reply.
#include "server.hpp"

#include <chrono>
#include <cstring>

namespace dbw {

namespace {

bool in(const std::string &k, std::initializer_list<const char *> set) {
    for (const char *s : set)
        if (k == s) return true;
    return false;
}

bool is_dml(const std::string &k) { return in(k, {"INSERT", "UPDATE", "DELETE", "MERGE_INTO"}); }
bool is_result(const std::string &k) { return in(k, {"SELECT", "EXPLAIN", "PRAGMA", "CALL"}); }

const char *statement_type_name(duckdb_statement_type t) {
    switch (t) {
    case DUCKDB_STATEMENT_TYPE_SELECT: return "SELECT";
    case DUCKDB_STATEMENT_TYPE_INSERT: return "INSERT";
    case DUCKDB_STATEMENT_TYPE_UPDATE: return "UPDATE";
    case DUCKDB_STATEMENT_TYPE_EXPLAIN: return "EXPLAIN";
    case DUCKDB_STATEMENT_TYPE_DELETE: return "DELETE";
    case DUCKDB_STATEMENT_TYPE_PREPARE: return "PREPARE";
    case DUCKDB_STATEMENT_TYPE_CREATE: return "CREATE";
    case DUCKDB_STATEMENT_TYPE_EXECUTE: return "EXECUTE";
    case DUCKDB_STATEMENT_TYPE_ALTER: return "ALTER";
    case DUCKDB_STATEMENT_TYPE_TRANSACTION: return "TRANSACTION";
    case DUCKDB_STATEMENT_TYPE_COPY: return "COPY";
    case DUCKDB_STATEMENT_TYPE_ANALYZE: return "ANALYZE";
    case DUCKDB_STATEMENT_TYPE_VARIABLE_SET: return "VARIABLE_SET";
    case DUCKDB_STATEMENT_TYPE_CREATE_FUNC: return "CREATE_FUNC";
    case DUCKDB_STATEMENT_TYPE_DROP: return "DROP";
    case DUCKDB_STATEMENT_TYPE_EXPORT: return "EXPORT";
    case DUCKDB_STATEMENT_TYPE_PRAGMA: return "PRAGMA";
    case DUCKDB_STATEMENT_TYPE_VACUUM: return "VACUUM";
    case DUCKDB_STATEMENT_TYPE_CALL: return "CALL";
    case DUCKDB_STATEMENT_TYPE_SET: return "SET";
    case DUCKDB_STATEMENT_TYPE_LOAD: return "LOAD";
    case DUCKDB_STATEMENT_TYPE_RELATION: return "RELATION";
    case DUCKDB_STATEMENT_TYPE_EXTENSION: return "EXTENSION";
    case DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN: return "LOGICAL_PLAN";
    case DUCKDB_STATEMENT_TYPE_ATTACH: return "ATTACH";
    case DUCKDB_STATEMENT_TYPE_DETACH: return "DETACH";
    case DUCKDB_STATEMENT_TYPE_MULTI: return "MULTI";
    case DUCKDB_STATEMENT_TYPE_COPY_DATABASE: return "COPY_DATABASE";
    case DUCKDB_STATEMENT_TYPE_UPDATE_EXTENSIONS: return "UPDATE_EXTENSIONS";
    case DUCKDB_STATEMENT_TYPE_MERGE_INTO: return "MERGE_INTO";
    default: return "INVALID";
    }
}

// The first keyword, past whitespace and comments (block comments nest).
std::string leading_keyword(const std::string &sql) {
    size_t i = 0, n = sql.size();
    for (;;) {
        while (i < n && (isspace((unsigned char)sql[i]) || sql[i] == '(')) i++;
        if (i + 1 < n && sql[i] == '-' && sql[i + 1] == '-') {
            while (i < n && sql[i] != '\n') i++;
            continue;
        }
        if (i + 1 < n && sql[i] == '/' && sql[i + 1] == '*') {
            int depth = 0;
            while (i + 1 < n) {
                if (sql[i] == '/' && sql[i + 1] == '*') depth++, i += 2;
                else if (sql[i] == '*' && sql[i + 1] == '/') {
                    depth--, i += 2;
                    if (depth == 0) break;
                } else i++;
            }
            continue;
        }
        break;
    }
    std::string k;
    while (i < n && (isalpha((unsigned char)sql[i]) || sql[i] == '_')) k += (char)toupper((unsigned char)sql[i++]);
    return k;
}

// The parse-time statement type the Python package's extract_statements()
// reports, from the leading keyword: used where the prepared type differs
// (PRAGMA) and when a statement cannot be prepared.
std::string keyword_kind(const std::string &kw) {
    static const std::map<std::string, std::string> kinds = {
        {"SELECT", "SELECT"}, {"WITH", "SELECT"}, {"VALUES", "SELECT"}, {"FROM", "SELECT"}, {"TABLE", "SELECT"},
        {"SHOW", "SELECT"}, {"DESCRIBE", "SELECT"}, {"DESC", "SELECT"}, {"SUMMARIZE", "SELECT"},
        {"PIVOT", "SELECT"}, {"UNPIVOT", "SELECT"}, {"EXPLAIN", "EXPLAIN"}, {"PRAGMA", "PRAGMA"},
        {"CALL", "CALL"}, {"CHECKPOINT", "CALL"}, {"INSERT", "INSERT"}, {"UPDATE", "UPDATE"},
        {"DELETE", "DELETE"}, {"TRUNCATE", "DELETE"}, {"MERGE", "MERGE_INTO"}, {"CREATE", "CREATE"},
        {"DROP", "DROP"}, {"DEALLOCATE", "DROP"}, {"ALTER", "ALTER"}, {"COPY", "COPY"},
        {"ATTACH", "ATTACH"}, {"DETACH", "DETACH"}, {"SET", "SET"}, {"RESET", "SET"}, {"USE", "SET"},
        {"BEGIN", "TRANSACTION"}, {"START", "TRANSACTION"}, {"COMMIT", "TRANSACTION"}, {"END", "TRANSACTION"},
        {"ROLLBACK", "TRANSACTION"}, {"ABORT", "TRANSACTION"}, {"VACUUM", "VACUUM"}, {"ANALYZE", "VACUUM"},
        {"LOAD", "LOAD"}, {"INSTALL", "LOAD"}, {"EXPORT", "EXPORT"}, {"IMPORT", "PRAGMA"},
        {"PREPARE", "PREPARE"}, {"EXECUTE", "EXECUTE"}, {"UPDATE_EXTENSIONS", "UPDATE_EXTENSIONS"},
    };
    auto it = kinds.find(kw);
    return it == kinds.end() ? "" : it->second;
}

void check_kind(Conn &c, const std::string &kind) {
    if (kind == "TRANSACTION")
        throw query_error("BEGIN/COMMIT/ROLLBACK are managed by dblayer; use db.transaction()", "TRANSACTION_STATEMENT");
    if (!c.write && !is_result(kind))
        throw query_error("attempt to write a read-only connection (" + kind + " statement)", "READ_ONLY");
}

struct Extracted {
    duckdb_extracted_statements e = nullptr;
    ~Extracted() { duckdb_destroy_extracted(&e); }
};

// A streaming result of a prepared statement. Throws Error.
void execute_streaming(duckdb_prepared_statement p, duckdb_result &res) {
    duckdb_pending_result pend = nullptr;
    if (duckdb_pending_prepared_streaming(p, &pend) != DuckDBSuccess) {
        const char *e = duckdb_pending_error(pend);
        std::string msg = e ? e : "could not execute the statement";
        duckdb_destroy_pending(&pend);
        throw duckdb_error(msg);
    }
    duckdb_state st = duckdb_execute_pending(pend, &res);
    duckdb_destroy_pending(&pend);
    if (st != DuckDBSuccess) {
        const char *e = duckdb_result_error(&res);
        std::string msg = e ? e : "could not execute the statement";
        duckdb_destroy_result(&res);
        throw duckdb_error(msg);
    }
}

// After duckdb_fetch_chunk returned NULL: the error, if the statement failed.
void check_fetch(duckdb_result &res) {
    const char *e = duckdb_result_error(&res);
    if (e != nullptr && *e) throw duckdb_error(e);
}

void drain(duckdb_result &res) {
    for (;;) {
        duckdb_data_chunk ch = duckdb_fetch_chunk(res);
        if (!ch) break;
        duckdb_destroy_data_chunk(&ch);
    }
    check_fetch(res);
}

struct Result {
    duckdb_result res{};
    bool live = false;
    ~Result() {
        if (live) duckdb_destroy_result(&res);
    }
};

struct Chunk {
    duckdb_data_chunk c = nullptr;
    ~Chunk() {
        if (c) duckdb_destroy_data_chunk(&c);
    }
};

// Settings statements may change the session's TimeZone.
void note_settings(const std::string &kind) {
    if (in(kind, {"SET", "VARIABLE_SET", "PRAGMA", "LOAD"})) g_settings_gen++;
}

bool needs_zone(duckdb_prepared_statement p) {
    for (idx_t i = 0, n = duckdb_prepared_statement_column_count(p); i < n; i++) {
        duckdb_logical_type t = duckdb_prepared_statement_column_logical_type(p, i);
        duckdb_type id = duckdb_get_type_id(t);
        // A parameter's type is only known once the statement runs.
        bool z = id == DUCKDB_TYPE_INVALID || id == DUCKDB_TYPE_ANY || type_has_timestamptz(t);
        duckdb_destroy_logical_type(&t);
        if (z) return true;
    }
    return false;
}

// Our own BEGIN TRANSACTION READ ONLY around a statement on a read
// connection outside a transaction.
struct ReadOnlyWrapper {
    Conn &c;
    bool on = false;
    explicit ReadOnlyWrapper(Conn &conn) : c(conn) {
        if (c.write || c.duck_in_tx) return;
        run_sql_simple(c, "BEGIN TRANSACTION READ ONLY");
        on = true;
    }
    void commit() {
        if (!on) return;
        on = false;
        run_sql_simple(c, "COMMIT");
    }
    ~ReadOnlyWrapper() {
        if (!on) return;
        try {
            run_sql_simple(c, "ROLLBACK");
        } catch (...) {
        }
    }
};

}  // namespace

std::vector<std::vector<std::string>> query_strings(duckdb_connection c, const std::string &sql) {
    duckdb_result res;
    if (duckdb_query(c, sql.c_str(), &res) != DuckDBSuccess) {
        std::string msg = duckdb_result_error(&res) ? duckdb_result_error(&res) : "query failed";
        duckdb_destroy_result(&res);
        throw duckdb_error(msg);
    }
    std::vector<std::vector<std::string>> rows;
    idx_t cols = duckdb_column_count(&res);
    for (;;) {
        duckdb_data_chunk ch = duckdb_fetch_chunk(res);
        if (!ch) break;
        idx_t n = duckdb_data_chunk_get_size(ch);
        for (idx_t r = 0; r < n; r++) {
            std::vector<std::string> row;
            for (idx_t col = 0; col < cols; col++) {
                duckdb_vector v = duckdb_data_chunk_get_vector(ch, col);
                uint64_t *valid = duckdb_vector_get_validity(v);
                if (valid && !duckdb_validity_row_is_valid(valid, r)) {
                    row.emplace_back("\x01NULL");
                    continue;
                }
                duckdb_string_t *s = &((duckdb_string_t *)duckdb_vector_get_data(v))[r];
                row.emplace_back(duckdb_string_t_data(s), duckdb_string_t_length(*s));
            }
            rows.push_back(std::move(row));
        }
        duckdb_destroy_data_chunk(&ch);
    }
    duckdb_destroy_result(&res);
    return rows;
}

const std::string &session_zone(Conn &c) {
    uint64_t gen = g_settings_gen.load();
    if (c.zone.empty() || c.zone_gen != gen) {
        auto rows = query_strings(c.c, "SELECT current_setting('TimeZone')");
        c.zone = rows.empty() || rows[0].empty() ? "UTC" : rows[0][0];
        c.zone_gen = gen;
    }
    return c.zone;
}

bool check_statement(Conn &c, const std::string &sql, Prepared &out) {
    Extracted ext;
    idx_t n = duckdb_extract_statements(c.c, sql.c_str(), &ext.e);
    if (n == 0) {
        const char *e = duckdb_extract_statements_error(ext.e);
        if (e != nullptr && *e) throw duckdb_error(e);
        return false;
    }
    if (n > 1) throw query_error("only one statement can be executed at a time", "MULTIPLE_STATEMENTS");
    std::string kw_kind = keyword_kind(leading_keyword(sql));
    if (duckdb_prepare_extracted_statement(c.c, ext.e, 0, &out.p) != DuckDBSuccess) {
        const char *e = duckdb_prepare_error(out.p);
        std::string msg = e ? e : "could not prepare the statement";
        // A statement that cannot be prepared is still refused by type
        // first, as the Python backend refuses it before running it.
        if (!kw_kind.empty()) check_kind(c, kw_kind);
        throw duckdb_error(msg);
    }
    out.kind = kw_kind == "PRAGMA" ? kw_kind : statement_type_name(duckdb_prepared_statement_type(out.p));
    check_kind(c, out.kind);
    return true;
}

void exec_one(Conn &c, Writer &wr, const std::string &sql, const val *params, double time_limit_ms, int64_t max_rows,
              int64_t max_bytes) {
    wbuf *w = wr.w();
    Prepared p;
    if (!check_statement(c, sql, p)) {
        w_map(w, 1);
        w_cstr(w, "columns");
        w_array(w, 0);
        w_end(w);
        w_end(w);
        return;
    }
    bind_params(p.p, params);
    std::string zone;
    if (needs_zone(p.p)) zone = session_zone(c);
    ReadOnlyWrapper wrap(c);
    bool dml = is_dml(p.kind), result_kind = is_result(p.kind);
    {
        Statement st(c, time_limit_ms);
        Result r;
        execute_streaming(p.p, r.res);
        r.live = true;
        note_settings(p.kind);
        duckdb_result_type rt = duckdb_result_return_type(r.res);
        idx_t ncols = duckdb_column_count(&r.res);
        if (dml && rt == DUCKDB_RESULT_TYPE_CHANGED_ROWS) {
            int64_t count = 0;
            for (;;) {
                Chunk ch;
                ch.c = duckdb_fetch_chunk(r.res);
                if (!ch.c) break;
                if (duckdb_data_chunk_get_size(ch.c) > 0 && ncols > 0)
                    count += ((int64_t *)duckdb_vector_get_data(duckdb_data_chunk_get_vector(ch.c, 0)))[0];
            }
            check_fetch(r.res);
            w_map(w, 2);
            w_cstr(w, "columns");
            w_array(w, 0);
            w_end(w);
            w_cstr(w, "rowcount");
            w_int(w, count);
            w_end(w);
        } else if (ncols == 0 || (!dml && !result_kind)) {
            drain(r.res);
            w_map(w, 1);
            w_cstr(w, "columns");
            w_array(w, 0);
            w_end(w);
            w_end(w);
        } else {
            ResultCodec rc;
            rc.zone = zone;
            rc.init(&r.res);
            w_map_open(w);
            w_cstr(w, "columns");
            w_array(w, (uint32_t)rc.names.size());
            for (auto &s : rc.names) w_str_lossy(w, s.data(), s.size());
            w_end(w);
            w_cstr(w, "types");
            w_array(w, (uint32_t)rc.types.size());
            for (auto &s : rc.types) w_str_lossy(w, s.data(), s.size());
            w_end(w);
            // max_rows + 1 rows are fetched (to know about truncation), and
            // every fetched row counts towards max_result_bytes.
            int64_t limit = max_rows >= 0 ? max_rows + 1 : INT64_MAX;
            int64_t fetched = 0, written = 0, size = 0;
            bool rows_open = false, stopped = false;
            while (!stopped) {
                Chunk ch;
                ch.c = duckdb_fetch_chunk(r.res);
                if (!ch.c) {
                    check_fetch(r.res);
                    break;
                }
                idx_t n = duckdb_data_chunk_get_size(ch.c);
                idx_t take = n;
                if ((int64_t)take > limit - fetched) {
                    take = (idx_t)(limit - fetched);
                    stopped = true;
                }
                if (max_bytes >= 0) {
                    for (idx_t i = 0; i < take; i++) size += rc.row_bytes(ch.c, i);
                    if (size > max_bytes)
                        throw Error("ResourceLimitExceeded", "result exceeded max_result_bytes=" + std::to_string(max_bytes));
                }
                idx_t send = take;
                if (max_rows >= 0 && written + (int64_t)send > max_rows) send = (idx_t)(max_rows - written);
                if (send > 0) {
                    if (!rows_open) {
                        w_cstr(w, "rows");
                        w_array_open(w);
                        rows_open = true;
                    }
                    rc.write_rows(wr, ch.c, 0, send);
                    written += (int64_t)send;
                }
                fetched += (int64_t)take;
                if ((int64_t)take < (int64_t)n) {
                    stopped = true;
                    if (dml) fetched += (int64_t)(n - take);  // RETURNING: count every row
                }
                if (fetched >= limit) stopped = true;
            }
            bool truncated = max_rows >= 0 && fetched > max_rows;
            if (truncated && dml) {
                for (;;) {
                    Chunk ch;
                    ch.c = duckdb_fetch_chunk(r.res);
                    if (!ch.c) break;
                    fetched += (int64_t)duckdb_data_chunk_get_size(ch.c);
                }
                check_fetch(r.res);
            }
            if (rows_open) w_end(w);
            if (truncated) {
                w_cstr(w, "truncated");
                w_bool(w, 1);
            }
            if (dml) {
                w_cstr(w, "rowcount");
                w_int(w, fetched);
            }
            w_end(w);
        }
    }
    wrap.commit();
}

int64_t exec_many(Conn &c, const std::string &sql, const val *params_seq) {
    Prepared p;
    if (!check_statement(c, sql, p)) return 0;
    size_t n = (params_seq != nullptr && params_seq->t == V_ARR) ? params_seq->u.a.n : 0;
    if (n == 0) return 0;
    ReadOnlyWrapper wrap(c);
    int64_t total = 0;
    bool counted = true;
    {
        Statement st(c, -1);
        for (size_t i = 0; i < n; i++) {
            duckdb_clear_bindings(p.p);
            bind_params(p.p, &params_seq->u.a.v[i]);
            duckdb_result res;
            if (duckdb_execute_prepared(p.p, &res) != DuckDBSuccess) {
                std::string msg = duckdb_result_error(&res) ? duckdb_result_error(&res) : "execute failed";
                duckdb_destroy_result(&res);
                throw duckdb_error(msg);
            }
            // The Python package's rowcount: the rows changed, or -1 once
            // a statement returned rows (or nothing).
            if (duckdb_result_return_type(res) == DUCKDB_RESULT_TYPE_CHANGED_ROWS) total += (int64_t)duckdb_rows_changed(&res);
            else counted = false;
            duckdb_destroy_result(&res);
        }
        note_settings(p.kind);
    }
    wrap.commit();
    return counted ? total : -1;
}

// -- streams ----------------------------------------------------------------------------

Stream::~Stream() {
    if (chunk) duckdb_destroy_data_chunk(&chunk);
    if (has_res) duckdb_destroy_result(&res);
    if (prep) duckdb_destroy_prepare(&prep);
}

void stream_close(Conn &c, int64_t sid, bool ok) {
    auto it = c.streams.find(sid);
    if (it == c.streams.end()) return;
    std::unique_ptr<Stream> s = std::move(it->second);
    c.streams.erase(it);
    forget_stream(sid);
    bool wrapped = s->wrapped;
    s.reset();  // the result goes first: COMMIT/ROLLBACK need the connection
    if (wrapped) {
        try {
            run_sql_simple(c, ok ? "COMMIT" : "ROLLBACK");
        } catch (...) {
            if (ok) throw;
        }
    }
}

void close_all_streams(Conn &c) {
    std::vector<int64_t> ids;
    for (auto &kv : c.streams) ids.push_back(kv.first);
    for (int64_t id : ids) {
        try {
            stream_close(c, id, false);
        } catch (...) {
        }
    }
}

namespace {

// _DuckDBStream._next_chunk: up to chunk_rows rows (across DuckDB chunks)
// into an open rows array; returns the number of rows (0: the end).
int64_t next_rows(Conn &c, Stream &s, Writer &wr, bool &rows_open, const char *key) {
    auto t0 = std::chrono::steady_clock::now();
    struct Spent {
        Stream &s;
        std::chrono::steady_clock::time_point t0;
        ~Spent() {
            if (s.remaining_ms >= 0)
                s.remaining_ms -= std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
        }
    } spent{s, t0};
    if (s.remaining_ms >= 0 && s.remaining_ms <= 0) throw Error("QueryInterrupted", "query interrupted (time limit exceeded)");
    Statement st(c, s.remaining_ms >= 0 ? s.remaining_ms : -1);
    int64_t got = 0;
    while (got < s.chunk_rows) {
        if (s.chunk == nullptr) {
            s.chunk = duckdb_fetch_chunk(s.res);
            s.offset = 0;
            if (s.chunk == nullptr) {
                check_fetch(s.res);
                break;
            }
        }
        idx_t n = duckdb_data_chunk_get_size(s.chunk);
        idx_t take = std::min<idx_t>(n - s.offset, (idx_t)(s.chunk_rows - got));
        if (s.max_bytes >= 0) {
            for (idx_t i = 0; i < take; i++) s.bytes += s.codec->row_bytes(s.chunk, s.offset + i);
        }
        if (take > 0) {
            if (!rows_open) {
                w_cstr(wr.w(), key);
                w_array_open(wr.w());
                rows_open = true;
            }
            s.codec->write_rows(wr, s.chunk, s.offset, s.offset + take);
        }
        got += (int64_t)take;
        s.offset += take;
        if (s.offset >= n) {
            duckdb_destroy_data_chunk(&s.chunk);
            s.chunk = nullptr;
        }
    }
    if (s.max_bytes >= 0 && s.bytes > s.max_bytes)
        throw Error("ResourceLimitExceeded", "result exceeded max_result_bytes=" + std::to_string(s.max_bytes));
    return got;
}

// Worker._pump: send frames while there is credit.
void pump(Conn &c, Stream &s, const JobPtr &job) {
    Writer &wr = *c.wr;
    int64_t sid = s.id;
    while (s.credit > 0) {
        if (job_cancelled(c, job)) throw Error("QueryInterrupted", "query cancelled");
        wr.start();
        wbuf *w = wr.w();
        w_map_open(w);
        w_cstr(w, "id");
        w_int(w, sid);
        bool first = s.first;
        if (first) {
            w_cstr(w, "columns");
            w_array(w, (uint32_t)s.codec->names.size());
            for (auto &n : s.codec->names) w_str_lossy(w, n.data(), n.size());
            w_end(w);
            if (!s.codec->types.empty()) {
                w_cstr(w, "types");
                w_array(w, (uint32_t)s.codec->types.size());
                for (auto &t : s.codec->types) w_str_lossy(w, t.data(), t.size());
                w_end(w);
            }
            s.first = false;
        }
        bool rows_open = false;
        int64_t got = s.has_res ? next_rows(c, s, wr, rows_open, "rows") : 0;
        if (got == 0) {
            w_cstr(w, "end");
            w_bool(w, 1);
            w_end(w);
            stream_close(c, sid, true);
            wr.send(sid, true);
            return;
        }
        w_end(w);  // rows
        w_end(w);  // frame
        s.credit--;
        wr.send(sid, true);
    }
}

}  // namespace

void stream_start(Conn &c, const JobPtr &job) {
    const Msg &m = *job->msg;
    {
        const val *t = m.get("tx");
        if (t != nullptr && t->t == V_STR) {
            std::string tx(t->u.s.p, t->u.s.n);
            std::lock_guard<std::mutex> lock(c.mu);
            if (tx != c.tx) {
                for (auto &e : c.expired)
                    if (e == tx) throw Error("TransactionExpired", "transaction " + tx + " passed its deadline and was rolled back");
                throw Error("TransactionClosed", "transaction " + tx + " is not open");
            }
        }
    }
    int64_t chunk_rows = int_field(m, "chunk_rows", 1000);
    if (chunk_rows < 1) {
        Error e("ValueError", "chunk_rows must be at least 1");
        e.builtin = true;
        throw e;
    }
    auto s = std::make_unique<Stream>();
    s->id = job->id;
    s->chunk_rows = chunk_rows;
    s->credit = int_field(m, "credit", 1);
    s->remaining_ms = time_limit_of(m);
    s->max_bytes = int_field(m, "max_result_bytes", -1);
    s->codec = std::make_unique<ResultCodec>();
    Stream &st = *s;
    c.streams[st.id] = std::move(s);
    register_stream(st.id, conn_ptr(c));
    // The first chunk's work (as in _next_chunk): check, wrap, execute.
    {
        if (st.remaining_ms >= 0 && st.remaining_ms <= 0)
            throw Error("QueryInterrupted", "query interrupted (time limit exceeded)");
        Prepared p;
        if (check_statement(c, m.str("sql"), p)) {
            bind_params(p.p, m.get("params"));
            if (needs_zone(p.p)) st.codec->zone = session_zone(c);
            if (!c.write && !c.duck_in_tx) {
                run_sql_simple(c, "BEGIN TRANSACTION READ ONLY");
                st.wrapped = true;
            }
            auto t0 = std::chrono::steady_clock::now();
            {
                Statement stmt(c, st.remaining_ms);
                execute_streaming(p.p, st.res);
            }
            if (st.remaining_ms >= 0)
                st.remaining_ms -= std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
            st.has_res = true;
            st.prep = p.p;
            p.p = nullptr;
            st.kind = p.kind;
            note_settings(st.kind);
            bool dml = is_dml(st.kind);
            bool counts = dml && duckdb_result_return_type(st.res) == DUCKDB_RESULT_TYPE_CHANGED_ROWS;
            if (is_result(st.kind) || (dml && !counts)) {
                st.codec->init(&st.res);
            } else {
                drain(st.res);
                duckdb_destroy_result(&st.res);
                st.has_res = false;
            }
            if (st.codec->names.empty() && st.has_res) {
                drain(st.res);
                duckdb_destroy_result(&st.res);
                st.has_res = false;
            }
        }
    }
    pump(c, st, job);
}

void stream_more(Conn &c, const JobPtr &job) {
    auto it = c.streams.find(job->stream);
    if (it == c.streams.end()) return;  // finished or closed meanwhile
    it->second->credit += job->credit;
    pump(c, *it->second, job);
}

void run_op(Conn &, Writer &wr, const Msg &) {
    // Declined: the parent runs the core's default implementation.
    wbuf *w = wr.w();
    w_map(w, 1);
    w_cstr(w, "declined");
    w_bool(w, 1);
    w_end(w);
}

void run_sql(duckdb_connection c, const char *sql, QueryResult &out) {
    if (duckdb_query(c, sql, &out.res) != DuckDBSuccess) {
        std::string msg = duckdb_result_error(&out.res) ? duckdb_result_error(&out.res) : "query failed";
        throw duckdb_error(msg);
    }
    out.ok = true;
}

}  // namespace dbw
