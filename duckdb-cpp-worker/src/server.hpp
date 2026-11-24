// Connections, jobs and the timer: the state the main thread, the
// connection threads and the timer thread share (server.cpp), and the
// statement execution that runs on connection threads (exec.cpp).
#pragma once

#include "worker.hpp"

#include <functional>

namespace dbw {

// -- the timer thread -------------------------------------------------------------

class Timer {
  public:
    // Run `fn` on the timer thread after `delay_s`; returns a token.
    uint64_t schedule(double delay_s, std::function<void()> fn);
    void cancel(uint64_t token);

  private:
    void loop();
    std::mutex mu_;
    std::condition_variable cv_;
    std::multimap<std::chrono::steady_clock::time_point, uint64_t> heap_;
    std::map<uint64_t, std::function<void()>> live_;
    uint64_t next_ = 1;
    bool started_ = false;
};
extern Timer g_timer;

// -- connections ----------------------------------------------------------------------

struct Conn;

struct Job {
    std::unique_ptr<Msg> msg;  // null for internal jobs
    std::string type;
    int64_t id = 0;
    bool has_id = false;
    // Guarded by the connection's mutex.
    bool cancelled = false;
    std::string reason = "cancelled";  // "cancelled", "deadline", "time_limit"
    // Internal jobs: _more, _close_stream, _expire.
    int64_t stream = 0;
    int64_t credit = 0;
    std::string tx;
};
using JobPtr = std::shared_ptr<Job>;

struct Instance {
    std::string key, path;
    duckdb_database db = nullptr;
    int conns = 0;
};

// An open stream (Python worker: Stream + _DuckDBStream). Lives on its
// connection thread between frames, its result open, paused for credit.
struct Stream {
    int64_t id = 0;
    duckdb_prepared_statement prep = nullptr;
    duckdb_result res{};
    bool has_res = false;
    bool wrapped = false;  // inside our own BEGIN TRANSACTION READ ONLY
    bool first = true;     // columns not sent yet
    bool started = false;
    std::string sql;
    std::unique_ptr<Msg> msg;  // owns the params
    std::string kind;
    double remaining_ms = -1;  // time budget across chunks; < 0: none
    int64_t max_bytes = -1, bytes = 0;
    int64_t chunk_rows = 1000;
    int64_t credit = 0;
    duckdb_data_chunk chunk = nullptr;  // partly sent chunk
    idx_t offset = 0;
    std::unique_ptr<ResultCodec> codec;
    ~Stream();
};

struct Conn {
    int64_t id = 0;
    bool write = false;
    std::shared_ptr<Instance> inst;
    duckdb_connection c = nullptr;

    std::mutex mu;
    std::condition_variable cv;
    std::deque<JobPtr> q;      // guarded by mu
    JobPtr current;            // guarded by mu
    bool active = false;       // a statement is running (guarded by mu)
    uint64_t stmt = 0;         // statement counter (guarded by mu)
    std::string tx;            // open protocol transaction id (guarded by mu)
    std::deque<std::string> expired;  // guarded by mu
    uint64_t deadline_token = 0;

    // Connection thread only.
    int tx_seq = 0;
    bool duck_in_tx = false;   // DuckDBConnection._in_tx
    bool tx_failed = false;    // DuckDBConnection._tx_failed
    std::map<int64_t, std::unique_ptr<Stream>> streams;
    std::string zone;          // session TimeZone, cached
    uint64_t zone_gen = 0;
    Writer *wr = nullptr;
};
using ConnPtr = std::shared_ptr<Conn>;

// Statement scope (DuckDBConnection._statement): refuse to start once the
// job is cancelled, mark the statement active so a cancel or time limit
// can interrupt it, and arm its time limit (ms < 0: none).
class Statement {
  public:
    Statement(Conn &c, double time_limit_ms);
    ~Statement();
    Statement(const Statement &) = delete;
    Statement &operator=(const Statement &) = delete;

  private:
    Conn &c_;
    uint64_t token_ = 0;
};

// Interrupt statement `stmt` of `c`, again every 5 ms while it runs.
void interrupt_while_active(const std::weak_ptr<Conn> &c, uint64_t stmt);
ConnPtr conn_ptr(Conn &c);

// Settings generation: bumped after any statement that may change a
// session setting (TimeZone), so cached zones are re-read.
extern std::atomic<uint64_t> g_settings_gen;

// -- statements (exec.cpp) ----------------------------------------------------------

// Parse and check one statement as DuckDBConnection._check does. Returns
// false for an empty statement; `kind` is the statement type name.
struct Prepared {
    duckdb_prepared_statement p = nullptr;
    std::string kind;
    ~Prepared() {
        if (p) duckdb_destroy_prepare(&p);
    }
};
bool check_statement(Conn &c, const std::string &sql, Prepared &out);

void run_sql_simple(Conn &c, const char *sql);  // BEGIN / COMMIT / ...; throws

double time_limit_of(const Msg &m);  // -1: none
int64_t int_field(const Msg &m, const char *key, int64_t dflt);

// The "result" value of an execute (one entry of a batch).
void exec_one(Conn &c, Writer &wr, const std::string &sql, const val *params, double time_limit_ms,
              int64_t max_rows, int64_t max_bytes);
int64_t exec_many(Conn &c, const std::string &sql, const val *params_seq);

// Streams: start, pump `credit` frames, close.
void stream_start(Conn &c, const JobPtr &job);
void stream_more(Conn &c, const JobPtr &job);
void stream_close(Conn &c, int64_t sid, bool ok);
void close_all_streams(Conn &c);
bool job_cancelled(Conn &c, const JobPtr &job);

// Named operations run here (insert_rows through the appender).
void run_op(Conn &c, Writer &wr, const Msg &m);

// Register / forget a stream id (main thread routes `more` and `cancel`).
void register_stream(int64_t sid, const ConnPtr &c);
void forget_stream(int64_t sid);

// The connection's TimeZone setting, read before a statement whose result
// holds TIMESTAMP WITH TIME ZONE values.
const std::string &session_zone(Conn &c);

bool send_reply_frame(Writer &wr, int64_t id);  // finish and send
void begin_reply(Writer &wr);                     // open map, codec chosen
void end_reply(Writer &wr, Conn *c, int64_t id, bool has_id);  // in_tx, id, close

}  // namespace dbw
