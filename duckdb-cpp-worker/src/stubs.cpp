// Placeholders, replaced piece by piece.
#include "server.hpp"
namespace dbw {
static Error todo() { return Error("NotSupported", "not implemented yet in the DuckDB C++ worker"); }
std::string type_name(duckdb_logical_type) { return "?"; }
void write_schema(Writer &, duckdb_connection) { throw todo(); }
std::string schema_version(duckdb_connection) { throw todo(); }
bool check_statement(Conn &, const std::string &, Prepared &) { throw todo(); }
void exec_one(Conn &, Writer &, const std::string &, const val *, double, int64_t, int64_t) { throw todo(); }
int64_t exec_many(Conn &, const std::string &, const val *) { throw todo(); }
void stream_start(Conn &, const JobPtr &) { throw todo(); }
void stream_more(Conn &, const JobPtr &) { throw todo(); }
void stream_close(Conn &, int64_t, bool) {}
void close_all_streams(Conn &) {}
void run_op(Conn &, Writer &, const Msg &) { throw todo(); }
}  // namespace dbw
namespace dbw { struct ColumnCodec {}; Stream::~Stream() {} ResultCodec::ResultCodec() {} ResultCodec::~ResultCodec() {} }
