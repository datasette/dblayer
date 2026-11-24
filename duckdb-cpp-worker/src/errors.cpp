// Errors as the Python DuckDB backend reports them: DuckDB's message
// prefix ("Constraint Error: ...") gives the exception class the duckdb
// Python package would raise, and DuckDBDialect.map_exception
// (dblayer/backends/duckdb.py) turns that into a dblayer error.
#include "worker.hpp"

#include <cctype>
#include <cstring>

namespace dbw {

namespace {

struct Prefix {
    const char *prefix;  // ExceptionTypeToString + " Error: "
    const char *cls;     // the duckdb Python package's exception class
};

// From ExceptionType to the Python class (duckdb's PyThrowException);
// types without a class of their own raise duckdb.Error.
const Prefix PREFIXES[] = {
    {"Catalog Error: ", "CatalogException"},
    {"Binder Error: ", "BinderException"},
    {"Parser Error: ", "ParserException"},
    {"Syntax Error: ", "SyntaxException"},
    {"Constraint Error: ", "ConstraintException"},
    {"Conversion Error: ", "ConversionException"},
    {"Out of Range Error: ", "OutOfRangeException"},
    {"Invalid Input Error: ", "InvalidInputException"},
    {"Invalid type Error: ", "InvalidTypeException"},
    {"Mismatch Type Error: ", "TypeMismatchException"},
    {"TransactionContext Error: ", "TransactionException"},
    {"INTERRUPT Error: ", "InterruptException"},
    {"Out of Memory Error: ", "OutOfMemoryException"},
    {"Permission Error: ", "PermissionException"},
    {"Dependency Error: ", "DependencyException"},
    {"Sequence Error: ", "SequenceException"},
    {"Serialization Error: ", "SerializationException"},
    {"Not implemented Error: ", "NotImplementedException"},
    {"IO Error: ", "IOException"},
    {"HTTP Error: ", "HTTPException"},
    {"Connection Error: ", "ConnectionException"},
    {"FATAL Error: ", "FatalException"},
    {"INTERNAL Error: ", "InternalException"},
    {"Resource In Use Error: ", "ResourceInUseException"},
};

bool icontains(const std::string &s, const char *needle) {
    std::string a(s), b(needle);
    for (auto &ch : a) ch = (char)tolower((unsigned char)ch);
    for (auto &ch : b) ch = (char)tolower((unsigned char)ch);
    return a.find(b) != std::string::npos;
}

}  // namespace

Error query_error(const std::string &message, const std::string &code) { return Error("QueryError", message, code); }

std::string duckdb_exception_class(const std::string &message) {
    for (const Prefix &p : PREFIXES)
        if (message.compare(0, strlen(p.prefix), p.prefix) == 0)
            return p.cls;
    return "Error";
}

Error duckdb_error(const std::string &message) {
    std::string cls = duckdb_exception_class(message);
    std::string code = cls;
    const std::string suffix = "Exception";
    if (code.size() > suffix.size() && code.compare(code.size() - suffix.size(), suffix.size(), suffix) == 0)
        code.resize(code.size() - suffix.size());
    Error e;
    e.duckdb = true;
    e.cause_type = "_duckdb." + cls;
    e.cause_message = message;
    if (cls == "InterruptException") {
        e.type = "QueryInterrupted";
        e.message = "query interrupted (time limit exceeded or cancelled)";
    } else if (cls == "ConstraintException") {
        e.type = "IntegrityError";
        e.message = message;
        e.code = code;
    } else if (cls == "OutOfMemoryException") {
        e.type = "ResourceLimitExceeded";
        e.message = message;
    } else if (cls == "ConnectionException") {
        // map_exception declines; the Python worker wraps it.
        e.type = "QueryError";
        e.message = "worker error: " + cls + ": " + message;
        e.code = "WORKER";
    } else if (cls == "TransactionException") {
        if (message.find("Failed to commit") != std::string::npos && icontains(message, "constraint")) {
            e.type = "IntegrityError";
            e.message = message;
            e.code = "Transaction";
        } else if (icontains(message, "conflict")) {
            e.type = "WriteConflict";
            e.message = message;
        } else if (message.find("transaction is aborted") != std::string::npos) {
            e.type = "QueryError";
            e.message = message;
            e.code = "TRANSACTION_ABORTED";
        } else {
            e.type = "QueryError";
            e.message = message;
            e.code = code;
        }
    } else {
        e.type = "QueryError";
        e.message = message;
        e.code = code;
    }
    return e;
}

bool is_interrupt(const Error &e) { return e.type == "QueryInterrupted"; }

void write_error(Writer &wr, const Error &e) {
    wbuf *w = wr.w();
    if (e.builtin) {
        w_map(w, 3);
        w_cstr(w, "type");
        w_cstr(w, e.type.c_str());
        w_cstr(w, "message");
        w_str_lossy(w, e.message.data(), e.message.size());
        w_cstr(w, "builtin");
        w_bool(w, 1);
        w_end(w);
        return;
    }
    bool has_code = !e.code.empty();
    bool has_cause = !e.cause_type.empty();
    w_map(w, 2 + (has_code ? 1 : 0) + (has_cause ? 1 : 0));
    w_cstr(w, "type");
    w_cstr(w, e.type.c_str());
    w_cstr(w, "message");
    w_str_lossy(w, e.message.data(), e.message.size());
    if (has_code) {
        w_cstr(w, "code");
        w_cstr(w, e.code.c_str());
    }
    if (has_cause) {
        w_cstr(w, "cause");
        w_map(w, 2);
        w_cstr(w, "type");
        w_cstr(w, e.cause_type.c_str());
        w_cstr(w, "message");
        w_str_lossy(w, e.cause_message.data(), e.cause_message.size());
        w_end(w);
    }
    w_end(w);
}

std::string py_repr(const std::string &s) {
    char q = (s.find('\'') != std::string::npos && s.find('"') == std::string::npos) ? '"' : '\'';
    std::string out(1, q);
    for (unsigned char ch : s) {
        if (ch == '\\') out += "\\\\";
        else if (ch == (unsigned char)q) { out += '\\'; out += (char)ch; }
        else if (ch == '\n') out += "\\n";
        else if (ch == '\r') out += "\\r";
        else if (ch == '\t') out += "\\t";
        else if (ch < 0x20 || ch == 0x7f) {
            char b[8];
            snprintf(b, sizeof b, "\\x%02x", ch);
            out += b;
        } else out += (char)ch;
    }
    out += q;
    return out;
}

}  // namespace dbw
