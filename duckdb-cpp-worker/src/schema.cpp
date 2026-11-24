// Introspection, as _read_schema() in dblayer/backends/duckdb.py builds
// it, written straight in protocol.schema_to_wire() form.
#include "worker.hpp"

#include <algorithm>
#include <cctype>

namespace dbw {

namespace {

const char *HERE = "database_name = current_database() AND schema_name = current_schema()";
const std::string NUL = "\x01NULL";
const char SEP = '\x1f';

std::string version_sql() {
    std::string h = HERE;
    return "SELECT md5(coalesce(string_agg(x, chr(10) ORDER BY x), '')) FROM ("
           "SELECT 't' || table_name || chr(31) || coalesce(sql, '') AS x FROM duckdb_tables() WHERE " + h +
           " UNION ALL SELECT 'v' || view_name || chr(31) || coalesce(sql, '') FROM duckdb_views() WHERE NOT internal AND " + h +
           " UNION ALL SELECT 'i' || index_name || chr(31) || coalesce(sql, '') FROM duckdb_indexes() WHERE " + h + ")";
}

std::vector<std::string> split(const std::string &s) {
    std::vector<std::string> out;
    if (s == NUL || s.empty()) return out;
    size_t start = 0;
    for (;;) {
        size_t p = s.find(SEP, start);
        out.push_back(s.substr(start, p == std::string::npos ? std::string::npos : p - start));
        if (p == std::string::npos) break;
        start = p + 1;
    }
    return out;
}

bool is_null(const std::string &s) { return s == NUL; }

std::string strip_semicolon(const std::string &sql) {
    size_t a = 0, b = sql.size();
    while (a < b && isspace((unsigned char)sql[a])) a++;
    while (b > a && isspace((unsigned char)sql[b - 1])) b--;
    std::string s = sql.substr(a, b - a);
    if (!s.empty() && s.back() == ';') {
        s.pop_back();
        while (!s.empty() && isspace((unsigned char)s.back())) s.pop_back();
    }
    return s;
}

// _split_top_level: split on commas outside parentheses and quotes.
std::vector<std::string> split_top_level(const std::string &s) {
    std::vector<std::string> parts;
    int depth = 0;
    std::string buf;
    char quote = 0;
    auto trim = [](const std::string &x) {
        size_t a = 0, b = x.size();
        while (a < b && isspace((unsigned char)x[a])) a++;
        while (b > a && isspace((unsigned char)x[b - 1])) b--;
        return x.substr(a, b - a);
    };
    for (char ch : s) {
        if (quote) {
            buf += ch;
            if (ch == quote) quote = 0;
            continue;
        }
        if (ch == '\'' || ch == '"') quote = ch;
        else if (ch == '(') depth++;
        else if (ch == ')') depth--;
        else if (ch == ',' && depth == 0) {
            parts.push_back(trim(buf));
            buf.clear();
            continue;
        }
        buf += ch;
    }
    if (!buf.empty()) parts.push_back(trim(buf));
    return parts;
}

std::string fold(const std::string &s) {
    std::string out(s);
    for (auto &ch : out) ch = (char)tolower((unsigned char)ch);
    return out;
}

bool is_ident(const std::string &s) {
    if (s.empty() || !(isalpha((unsigned char)s[0]) || s[0] == '_')) return false;
    for (char ch : s)
        if (!(isalnum((unsigned char)ch) || ch == '_' || ch == '$')) return false;
    return true;
}

// _index_columns: the columns of a CREATE INDEX statement (null for an
// expression). `out` holds the matching column names, "" + flag for null.
std::vector<std::pair<bool, std::string>> index_columns(const std::string &sql, const std::vector<std::string> &columns) {
    std::vector<std::pair<bool, std::string>> out;
    if (sql.empty() || is_null(sql)) return out;
    size_t open = sql.find('('), close = sql.rfind(')');
    if (open == std::string::npos || close == std::string::npos || close <= open) return out;
    for (const std::string &part : split_top_level(sql.substr(open + 1, close - open - 1))) {
        bool have = false;
        std::string name;
        if (part.size() >= 2 && part.front() == '"' && part.back() == '"') {
            name = part.substr(1, part.size() - 2);
            std::string unq;
            for (size_t i = 0; i < name.size(); i++) {
                unq += name[i];
                if (name[i] == '"' && i + 1 < name.size() && name[i + 1] == '"') i++;
            }
            name = unq;
            have = true;
        } else if (is_ident(part)) {
            name = part;
            have = true;
        }
        bool found = false;
        std::string match;
        if (have) {
            for (const std::string &c : columns)
                if (fold(c) == fold(name)) {
                    found = true;
                    match = c;
                    break;
                }
        }
        out.emplace_back(found, match);
    }
    return out;
}

void w_string(wbuf *w, const std::string &s) { w_str_lossy(w, s.data(), s.size()); }

void w_opt_string(wbuf *w, const std::string &s) {
    if (is_null(s)) w_nil(w);
    else w_string(w, s);
}

struct Col {
    std::string name, type, def;
    bool nullable, generated;
};

struct Constraint {
    std::string type, name, ref_table;
    std::vector<std::string> cols, ref_cols;
};

struct IndexRow {
    std::string name, sql;
    bool unique;
};

struct Idx {
    std::string name;
    std::vector<std::pair<bool, std::string>> cols;
    bool unique;
    std::string origin, definition;  // definition: NUL for none
};

void write_columns(wbuf *w, const std::vector<Col> &cols, const std::vector<std::string> &pk) {
    w_array(w, (uint32_t)cols.size());
    for (const Col &c : cols) {
        auto it = std::find(pk.begin(), pk.end(), c.name);
        int pkpos = it == pk.end() ? 0 : (int)(it - pk.begin()) + 1;
        w_array(w, 7);
        w_string(w, c.name);
        w_string(w, is_null(c.type) ? std::string() : c.type);
        w_bool(w, !c.nullable && pkpos == 0);
        if (c.generated) w_nil(w);
        else w_opt_string(w, c.def);
        w_int(w, pkpos);
        w_bool(w, 0);  // hidden
        w_bool(w, c.generated);
        w_end(w);
    }
    w_end(w);
}

}  // namespace

std::string schema_version(duckdb_connection c) {
    auto rows = query_strings(c, version_sql());
    return rows.empty() || rows[0].empty() ? "" : rows[0][0];
}

void write_schema(Writer &wr, duckdb_connection c) {
    std::string here = HERE;
    std::string version = schema_version(c);
    auto tables = query_strings(c, "SELECT table_name, sql FROM duckdb_tables() WHERE " + here + " ORDER BY table_oid");
    auto views = query_strings(c, "SELECT view_name, sql FROM duckdb_views() WHERE NOT internal AND " + here +
                                      " ORDER BY view_oid");
    std::map<std::string, std::vector<Col>> cols;
    for (auto &r : query_strings(c, "SELECT table_name, column_name, data_type, is_nullable::VARCHAR, column_default, "
                                    "is_generated::VARCHAR FROM duckdb_columns() WHERE " + here +
                                        " ORDER BY table_oid, column_index"))
        cols[r[0]].push_back(Col{r[1], r[2], r[4], r[3] == "true", r[5] == "true"});
    std::map<std::string, std::vector<Constraint>> constraints;
    for (auto &r : query_strings(
             c, "SELECT table_name, constraint_type, array_to_string(constraint_column_names, chr(31)), constraint_name, "
                "referenced_table, array_to_string(referenced_column_names, chr(31)) FROM duckdb_constraints() WHERE " +
                    here + " AND constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY') ORDER BY table_oid, "
                           "constraint_index"))
        constraints[r[0]].push_back(Constraint{r[1], r[3], r[4], split(r[2]), split(r[5])});
    std::map<std::string, std::vector<IndexRow>> indexes;
    for (auto &r : query_strings(c, "SELECT table_name, index_name, is_unique::VARCHAR, sql FROM duckdb_indexes() WHERE " +
                                        here + " ORDER BY index_name"))
        indexes[r[0]].push_back(IndexRow{r[1], r[3], r[2] == "true"});

    std::map<std::string, std::vector<std::string>> pks;
    for (auto &kv : constraints)
        for (auto &k : kv.second)
            if (k.type == "PRIMARY KEY") pks[kv.first] = k.cols;

    wbuf *w = wr.w();
    w_map(w, 4);
    w_cstr(w, "version");
    w_string(w, version);
    w_cstr(w, "tables");
    w_array(w, (uint32_t)tables.size());
    for (auto &t : tables) {
        const std::string &name = t[0];
        const std::vector<Col> &tc = cols[name];
        const std::vector<std::string> &pk = pks[name];
        std::vector<std::string> names;
        for (auto &col : tc) names.push_back(col.name);
        std::vector<Idx> idx;
        w_map(w, 9);
        w_cstr(w, "name");
        w_string(w, name);
        w_cstr(w, "kind");
        w_cstr(w, "table");
        w_cstr(w, "columns");
        write_columns(w, tc, pk);
        w_cstr(w, "foreign_keys");
        std::vector<const Constraint *> fks;
        for (auto &k : constraints[name]) {
            if (k.type == "FOREIGN KEY") fks.push_back(&k);
            else {
                Idx i;
                i.name = k.name;
                for (auto &cn : k.cols) i.cols.emplace_back(true, cn);
                i.unique = true;
                i.origin = k.type == "PRIMARY KEY" ? "pk" : "u";
                i.definition = NUL;
                idx.push_back(std::move(i));
            }
        }
        w_array(w, (uint32_t)fks.size());
        for (const Constraint *k : fks) {
            w_array(w, 5);
            w_array(w, (uint32_t)k->cols.size());
            for (auto &cn : k->cols) w_string(w, cn);
            w_end(w);
            w_string(w, k->ref_table);
            std::vector<std::string> other = k->ref_cols;
            if (other.empty()) other = pks[k->ref_table];
            w_array(w, (uint32_t)other.size());
            for (auto &cn : other) w_string(w, cn);
            w_end(w);
            w_nil(w);
            w_nil(w);
            w_end(w);
        }
        w_end(w);
        for (auto &ir : indexes[name]) {
            Idx i;
            i.name = ir.name;
            i.cols = index_columns(ir.sql, names);
            i.unique = ir.unique;
            i.origin = "c";
            i.definition = is_null(ir.sql) ? NUL : strip_semicolon(ir.sql);
            idx.push_back(std::move(i));
        }
        std::stable_sort(idx.begin(), idx.end(), [](const Idx &a, const Idx &b) { return a.name < b.name; });
        w_cstr(w, "indexes");
        w_array(w, (uint32_t)idx.size());
        for (auto &i : idx) {
            w_array(w, 6);
            w_string(w, i.name);
            w_array(w, (uint32_t)i.cols.size());
            for (auto &cn : i.cols) {
                if (cn.first) w_string(w, cn.second);
                else w_nil(w);
            }
            w_end(w);
            w_bool(w, i.unique);
            w_string(w, i.origin);
            w_bool(w, 0);
            w_opt_string(w, i.definition);
            w_end(w);
        }
        w_end(w);
        w_cstr(w, "definition");
        if (is_null(t[1])) w_nil(w);
        else w_string(w, strip_semicolon(t[1]));
        w_cstr(w, "virtual");
        w_bool(w, 0);
        w_cstr(w, "hidden");
        w_bool(w, 0);
        w_cstr(w, "checks");
        w_array(w, 0);
        w_end(w);
        w_end(w);
    }
    w_end(w);
    w_cstr(w, "views");
    w_array(w, (uint32_t)views.size());
    for (auto &v : views) {
        w_map(w, 9);
        w_cstr(w, "name");
        w_string(w, v[0]);
        w_cstr(w, "kind");
        w_cstr(w, "view");
        w_cstr(w, "columns");
        write_columns(w, cols[v[0]], pks[v[0]]);
        w_cstr(w, "foreign_keys");
        w_array(w, 0);
        w_end(w);
        w_cstr(w, "indexes");
        w_array(w, 0);
        w_end(w);
        w_cstr(w, "definition");
        if (is_null(v[1])) w_nil(w);
        else w_string(w, strip_semicolon(v[1]));
        w_cstr(w, "virtual");
        w_bool(w, 0);
        w_cstr(w, "hidden");
        w_bool(w, 0);
        w_cstr(w, "checks");
        w_array(w, 0);
        w_end(w);
        w_end(w);
    }
    w_end(w);
    w_cstr(w, "triggers");
    w_array(w, 0);
    w_end(w);
    w_end(w);
}

}  // namespace dbw
