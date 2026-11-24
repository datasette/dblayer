// Shared declarations of the DuckDB worker (see worker.cpp).
#pragma once

#include "wire.hpp"

#define DUCKDB_API_NO_DEPRECATED_WARNINGS
#include "duckdb.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dbw {

// -- errors ---------------------------------------------------------------------------

// An error reply: `type` is a dblayer exception class name, `code` its
// QueryError code (empty: none), `cause_*` the driver exception as the
// Python DuckDB backend would see it (`_duckdb.ConstraintException`).
struct Error : std::exception {
    std::string type, message, code, cause_type, cause_message;
    bool builtin = false;
    bool duckdb = false;  // raised by DuckDB (marks a transaction failed)
    Error() = default;
    Error(std::string t, std::string m, std::string c = "") : type(std::move(t)), message(std::move(m)), code(std::move(c)) {}
    const char *what() const noexcept override { return message.c_str(); }
};

Error query_error(const std::string &message, const std::string &code);
// Map a DuckDB error message ("Constraint Error: ...") as
// DuckDBDialect.map_exception maps the Python exception.
Error duckdb_error(const std::string &message);
// The Python class DuckDB's Python package raises for this message.
std::string duckdb_exception_class(const std::string &message);
bool is_interrupt(const Error &e);
void write_error(Writer &wr, const Error &e);  // the "error" object
std::string py_repr(const std::string &s);     // Python repr() of a str

// -- decoded requests ---------------------------------------------------------------

struct Msg {
    std::vector<uint8_t> data;
    arena a{};
    val v{};
    Msg() = default;
    Msg(const Msg &) = delete;
    Msg &operator=(const Msg &) = delete;
    ~Msg() { arena_free(&a); }
    const val *get(const char *key) const { return map_get(&v, key); }
    std::string str(const char *key, const char *dflt = "") const;
    bool has(const char *key) const;
};

std::string val_str(const val *v);

// -- types and values ------------------------------------------------------------------

// LogicalType::ToString, as `str(cursor.description[i][1])` shows it.
std::string type_name(duckdb_logical_type t);
// The keyword list type_name() quotes struct field names against.
void load_keywords(duckdb_connection c);

// How the columns of one result are written: built once per result from
// the column types; the zone is the session's TimeZone (for TIMESTAMPTZ).
struct ColumnCodec;
struct ResultCodec {
    std::vector<std::unique_ptr<ColumnCodec>> cols;
    std::vector<std::string> names, types;
    bool needs_zone = false;
    std::string zone;
    ResultCodec();
    ~ResultCodec();
    void init(duckdb_result *res);
    // Append rows [from, to) of `chunk` to the open rows array.
    void write_rows(Writer &wr, duckdb_data_chunk chunk, idx_t from, idx_t to);
    // _row_bytes() of the Python backend for one row.
    int64_t row_bytes(duckdb_data_chunk chunk, idx_t row);
};
bool type_has_timestamptz(duckdb_logical_type t);

// Bind request parameters (a list for `?`/`$1`, a map for `$name`) as the
// Python package binds Python values. Throws Error.
void bind_params(duckdb_prepared_statement p, const val *params);

// -- schema ----------------------------------------------------------------------------

void write_schema(Writer &wr, duckdb_connection c);  // the "result" value
std::string schema_version(duckdb_connection c);

// Run one statement that returns rows; throws Error. The callback sees
// each chunk.
struct QueryResult {
    duckdb_result res{};
    bool ok = false;
    ~QueryResult() { duckdb_destroy_result(&res); }
};
void run_sql(duckdb_connection c, const char *sql, QueryResult &out);
// Every row of a simple query, as strings (NULL as "\x01NULL").
std::vector<std::vector<std::string>> query_strings(duckdb_connection c, const std::string &sql);

}  // namespace dbw
