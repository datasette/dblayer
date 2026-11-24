// Type names as DuckDB's LogicalType::ToString() writes them (the C API
// has no such function): what the Python package shows as
// str(cursor.description[i][1]), and so what result `types` must say.
#include "worker.hpp"

#include <set>

namespace dbw {

namespace {

std::mutex g_kw_mu;
std::set<std::string> g_keywords;  // every category: KeywordHelper::IsKeyword
bool g_kw_loaded = false;

bool is_keyword(const std::string &s) {
    std::string low(s);
    for (auto &ch : low) ch = (char)tolower((unsigned char)ch);
    std::lock_guard<std::mutex> lock(g_kw_mu);
    return g_keywords.count(low) > 0;
}

// KeywordHelper::WriteOptionallyQuoted(text, '"', allow_caps = true)
std::string optionally_quoted(const std::string &text) {
    bool needs = false;
    for (size_t i = 0; i < text.size(); i++) {
        char ch = text[i];
        if (i > 0 && ch >= '0' && ch <= '9') continue;
        if (ch >= 'a' && ch <= 'z') continue;
        if (ch >= 'A' && ch <= 'Z') continue;
        if (ch == '_') continue;
        needs = true;
        break;
    }
    if (!needs) needs = is_keyword(text);
    if (!needs) return text;
    std::string out = "\"";
    for (char ch : text) {
        if (ch == '"') out += "\"\"";
        else out += ch;
    }
    return out + "\"";
}

std::string quoted_literal(const char *s) {
    std::string out = "'";
    for (const char *p = s; *p; p++) {
        if (*p == '\'') out += "''";
        else out += *p;
    }
    return out + "'";
}

std::string take(char *s) {
    std::string out = s ? s : "";
    if (s) duckdb_free(s);
    return out;
}

struct TypeHandle {
    duckdb_logical_type t;
    explicit TypeHandle(duckdb_logical_type x) : t(x) {}
    ~TypeHandle() { duckdb_destroy_logical_type(&t); }
};

}  // namespace

void load_keywords(duckdb_connection c) {
    {
        std::lock_guard<std::mutex> lock(g_kw_mu);
        if (g_kw_loaded) return;
    }
    std::set<std::string> kws;
    for (auto &row : query_strings(c, "SELECT keyword_name FROM duckdb_keywords()"))
        if (!row.empty()) kws.insert(row[0]);
    std::lock_guard<std::mutex> lock(g_kw_mu);
    g_keywords.swap(kws);
    g_kw_loaded = true;
}

std::string type_name(duckdb_logical_type t) {
    duckdb_type id = duckdb_get_type_id(t);
    std::string alias = take(duckdb_logical_type_get_alias(t));
    if (!alias.empty()) return alias;
    switch (id) {
    case DUCKDB_TYPE_BOOLEAN: return "BOOLEAN";
    case DUCKDB_TYPE_TINYINT: return "TINYINT";
    case DUCKDB_TYPE_SMALLINT: return "SMALLINT";
    case DUCKDB_TYPE_INTEGER: return "INTEGER";
    case DUCKDB_TYPE_BIGINT: return "BIGINT";
    case DUCKDB_TYPE_UTINYINT: return "UTINYINT";
    case DUCKDB_TYPE_USMALLINT: return "USMALLINT";
    case DUCKDB_TYPE_UINTEGER: return "UINTEGER";
    case DUCKDB_TYPE_UBIGINT: return "UBIGINT";
    case DUCKDB_TYPE_FLOAT: return "FLOAT";
    case DUCKDB_TYPE_DOUBLE: return "DOUBLE";
    case DUCKDB_TYPE_TIMESTAMP: return "TIMESTAMP";
    case DUCKDB_TYPE_DATE: return "DATE";
    case DUCKDB_TYPE_TIME: return "TIME";
    case DUCKDB_TYPE_INTERVAL: return "INTERVAL";
    case DUCKDB_TYPE_HUGEINT: return "HUGEINT";
    case DUCKDB_TYPE_UHUGEINT: return "UHUGEINT";
    case DUCKDB_TYPE_VARCHAR: return "VARCHAR";
    case DUCKDB_TYPE_BLOB: return "BLOB";
    case DUCKDB_TYPE_DECIMAL:
        return "DECIMAL(" + std::to_string(duckdb_decimal_width(t)) + "," + std::to_string(duckdb_decimal_scale(t)) + ")";
    case DUCKDB_TYPE_TIMESTAMP_S: return "TIMESTAMP_S";
    case DUCKDB_TYPE_TIMESTAMP_MS: return "TIMESTAMP_MS";
    case DUCKDB_TYPE_TIMESTAMP_NS: return "TIMESTAMP_NS";
    case DUCKDB_TYPE_ENUM: {
        std::string out = "ENUM(";
        uint32_t n = duckdb_enum_dictionary_size(t);
        for (uint32_t i = 0; i < n; i++) {
            if (i) out += ", ";
            char *v = duckdb_enum_dictionary_value(t, i);
            out += quoted_literal(v ? v : "");
            if (v) duckdb_free(v);
        }
        return out + ")";
    }
    case DUCKDB_TYPE_LIST: {
        TypeHandle child(duckdb_list_type_child_type(t));
        return type_name(child.t) + "[]";
    }
    case DUCKDB_TYPE_ARRAY: {
        TypeHandle child(duckdb_array_type_child_type(t));
        return type_name(child.t) + "[" + std::to_string(duckdb_array_type_array_size(t)) + "]";
    }
    case DUCKDB_TYPE_STRUCT: {
        idx_t n = duckdb_struct_type_child_count(t);
        if (n == 0) return "STRUCT";
        std::string out = "STRUCT(";
        for (idx_t i = 0; i < n; i++) {
            if (i) out += ", ";
            TypeHandle child(duckdb_struct_type_child_type(t, i));
            out += optionally_quoted(take(duckdb_struct_type_child_name(t, i))) + " " + type_name(child.t);
        }
        return out + ")";
    }
    case DUCKDB_TYPE_MAP: {
        TypeHandle k(duckdb_map_type_key_type(t)), v(duckdb_map_type_value_type(t));
        return "MAP(" + type_name(k.t) + ", " + type_name(v.t) + ")";
    }
    case DUCKDB_TYPE_UNION: {
        idx_t n = duckdb_union_type_member_count(t);
        std::string out = "UNION(";
        for (idx_t i = 0; i < n; i++) {
            if (i) out += ", ";
            TypeHandle child(duckdb_union_type_member_type(t, i));
            out += optionally_quoted(take(duckdb_union_type_member_name(t, i))) + " " + type_name(child.t);
        }
        return out + ")";
    }
    case DUCKDB_TYPE_UUID: return "UUID";
    case DUCKDB_TYPE_BIT: return "BIT";
    case DUCKDB_TYPE_TIME_TZ: return "TIME WITH TIME ZONE";
    case DUCKDB_TYPE_TIMESTAMP_TZ: return "TIMESTAMP WITH TIME ZONE";
    case DUCKDB_TYPE_ANY: return "ANY";
    case DUCKDB_TYPE_BIGNUM: return "BIGNUM";
    case DUCKDB_TYPE_SQLNULL: return "\"NULL\"";
    case DUCKDB_TYPE_STRING_LITERAL: return "STRING_LITERAL";
    case DUCKDB_TYPE_INTEGER_LITERAL: return "INTEGER_LITERAL";
    case DUCKDB_TYPE_TIME_NS: return "TIME_NS";
    default: return "UNKNOWN";
    }
}

bool type_has_timestamptz(duckdb_logical_type t) {
    switch (duckdb_get_type_id(t)) {
    case DUCKDB_TYPE_TIMESTAMP_TZ: return true;
    case DUCKDB_TYPE_LIST: {
        TypeHandle c(duckdb_list_type_child_type(t));
        return type_has_timestamptz(c.t);
    }
    case DUCKDB_TYPE_ARRAY: {
        TypeHandle c(duckdb_array_type_child_type(t));
        return type_has_timestamptz(c.t);
    }
    case DUCKDB_TYPE_MAP: {
        TypeHandle k(duckdb_map_type_key_type(t)), v(duckdb_map_type_value_type(t));
        return type_has_timestamptz(k.t) || type_has_timestamptz(v.t);
    }
    case DUCKDB_TYPE_STRUCT: {
        for (idx_t i = 0, n = duckdb_struct_type_child_count(t); i < n; i++) {
            TypeHandle c(duckdb_struct_type_child_type(t, i));
            if (type_has_timestamptz(c.t)) return true;
        }
        return false;
    }
    case DUCKDB_TYPE_UNION: {
        for (idx_t i = 0, n = duckdb_union_type_member_count(t); i < n; i++) {
            TypeHandle c(duckdb_union_type_member_type(t, i));
            if (type_has_timestamptz(c.t)) return true;
        }
        return false;
    }
    default: return false;
    }
}

}  // namespace dbw
