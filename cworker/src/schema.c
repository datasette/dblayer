/* The schema, as dblayer.backends.sqlite._read_schema reads it and
 * dblayer.worker.protocol.schema_to_wire shapes it:
 *
 *   {"version": N,
 *    "tables": [{"name", "kind": "table", "columns": [[name, type, not_null,
 *                default, primary_key, hidden, generated], ...],
 *                "foreign_keys": [[[cols], other, [other_cols], on_update,
 *                on_delete], ...], "indexes": [[name, [cols], unique, origin,
 *                partial, definition], ...], "definition", "virtual",
 *                "hidden", "checks": []}, ...],
 *    "views": [same, kind "view"], "triggers": [[name, table, sql], ...]}
 */
#define _GNU_SOURCE
#define _DARWIN_C_SOURCE
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "sqlite3.h"

#include "codec.h"

int schema_write(sqlite3 *db, wbuf *w);

typedef struct {
    char *type, *name, *tbl_name, *sql; /* sql may be NULL */
} master_row;

typedef struct {
    char *name, *type;
} kind_row;

static char *dup_col(sqlite3_stmt *st, int i) {
    const unsigned char *p = sqlite3_column_text(st, i);
    if (p == NULL)
        return NULL;
    size_t n = (size_t)sqlite3_column_bytes(st, i);
    char *s = malloc(n + 1);
    if (s == NULL)
        return NULL;
    memcpy(s, p, n);
    s[n] = 0;
    return s;
}

static void w_text_or_nil(wbuf *w, const char *s) {
    if (s)
        w_str_lossy(w, s, strlen(s));
    else
        w_nil(w);
}

/* A column value: text, integer, float or NULL (dflt_value, on_update...). */
static void w_column(wbuf *w, sqlite3_stmt *st, int i) {
    switch (sqlite3_column_type(st, i)) {
    case SQLITE_NULL: w_nil(w); break;
    case SQLITE_INTEGER: w_int(w, sqlite3_column_int64(st, i)); break;
    case SQLITE_FLOAT: w_double(w, sqlite3_column_double(st, i)); break;
    case SQLITE_BLOB: w_bin(w, sqlite3_column_blob(st, i) ? sqlite3_column_blob(st, i) : "",
                            (size_t)sqlite3_column_bytes(st, i)); break;
    default: w_str_lossy(w, (const char *)sqlite3_column_text(st, i), (size_t)sqlite3_column_bytes(st, i));
    }
}

static int prep(sqlite3 *db, const char *sql, const char *arg, sqlite3_stmt **st) {
    int rc = sqlite3_prepare_v2(db, sql, -1, st, NULL);
    if (rc == SQLITE_OK && arg)
        rc = sqlite3_bind_text(*st, 1, arg, -1, SQLITE_STATIC);
    return rc;
}

static int write_columns(sqlite3 *db, wbuf *w, const char *table) {
    sqlite3_stmt *st;
    int rc = prep(db, "select cid, name, type, \"notnull\", dflt_value, pk, hidden from pragma_table_xinfo(?)",
                  table, &st);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(st);
        return rc;
    }
    w_cstr(w, "columns");
    w_array_open(w);
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        int64_t hidden = sqlite3_column_int64(st, 6);
        const char *type = (const char *)sqlite3_column_text(st, 2);
        w_array(w, 7);
        w_column(w, st, 1);
        w_str_lossy(w, type ? type : "", type ? (size_t)sqlite3_column_bytes(st, 2) : 0);
        w_bool(w, sqlite3_column_int64(st, 3) != 0);
        w_column(w, st, 4);
        w_column(w, st, 5);
        w_bool(w, hidden == 1);
        w_bool(w, hidden == 2 || hidden == 3);
        w_end(w);
    }
    w_end(w);
    sqlite3_finalize(st);
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}

typedef struct {
    int64_t id;
    char *other, *from, *to, *on_update, *on_delete;
} fk_row;

static int write_pks(sqlite3 *db, wbuf *w, const char *table, int *count) {
    sqlite3_stmt *st;
    int rc = prep(db, "select name, pk from pragma_table_info(?) where pk > 0 order by pk", table, &st);
    *count = 0;
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            if (w)
                w_column(w, st, 0);
            (*count)++;
        }
    }
    sqlite3_finalize(st);
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}

static int write_foreign_keys(sqlite3 *db, wbuf *w, const char *table) {
    sqlite3_stmt *st;
    int rc = prep(db, "select id, seq, \"table\", \"from\", \"to\", on_update, on_delete, match "
                      "from pragma_foreign_key_list(?) order by id, seq", table, &st);
    fk_row *rows = NULL;
    size_t n = 0, cap = 0;
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            if (n == cap) {
                cap = cap ? cap * 2 : 8;
                fk_row *nr = realloc(rows, cap * sizeof *rows);
                if (nr == NULL) { rc = SQLITE_NOMEM; break; }
                rows = nr;
            }
            fk_row *r = &rows[n++];
            r->id = sqlite3_column_int64(st, 0);
            r->other = dup_col(st, 2);
            r->from = dup_col(st, 3);
            r->to = dup_col(st, 4);
            r->on_update = dup_col(st, 5);
            r->on_delete = dup_col(st, 6);
        }
        if (rc == SQLITE_DONE)
            rc = SQLITE_OK;
    }
    sqlite3_finalize(st);
    if (rc == SQLITE_OK) {
        w_cstr(w, "foreign_keys");
        w_array_open(w);
        for (size_t i = 0; i < n && rc == SQLITE_OK;) {
            size_t e = i;
            int any_null = 0;
            while (e < n && rows[e].id == rows[i].id) {
                any_null |= rows[e].to == NULL;
                e++;
            }
            w_array(w, 5);
            w_array(w, (uint32_t)(e - i));
            for (size_t k = i; k < e; k++)
                w_text_or_nil(w, rows[k].from);
            w_end(w);
            w_text_or_nil(w, rows[i].other);
            int npk = 0;
            if (any_null && rows[i].other)
                rc = write_pks(db, NULL, rows[i].other, &npk);
            if (npk > 0) {
                /* references the other table's primary key */
                w_array(w, (uint32_t)npk);
                rc = write_pks(db, w, rows[i].other, &npk);
                w_end(w);
            } else {
                w_array(w, (uint32_t)(e - i));
                for (size_t k = i; k < e; k++)
                    w_text_or_nil(w, rows[k].to);
                w_end(w);
            }
            w_text_or_nil(w, rows[i].on_update);
            w_text_or_nil(w, rows[i].on_delete);
            w_end(w);
            i = e;
        }
        w_end(w);
    }
    for (size_t i = 0; i < n; i++) {
        free(rows[i].other); free(rows[i].from); free(rows[i].to);
        free(rows[i].on_update); free(rows[i].on_delete);
    }
    free(rows);
    return rc;
}

typedef struct {
    char *name, *origin;
    int unique, partial;
} index_row;

static int cmp_index(const void *a, const void *b) {
    const index_row *x = a, *y = b;
    return strcmp(x->name ? x->name : "", y->name ? y->name : "");
}

static int write_indexes(sqlite3 *db, wbuf *w, const char *table, master_row *master, size_t nmaster) {
    sqlite3_stmt *st;
    int rc = prep(db, "select seq, name, \"unique\", origin, partial from pragma_index_list(?) order by seq",
                  table, &st);
    index_row *rows = NULL;
    size_t n = 0, cap = 0;
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            if (n == cap) {
                cap = cap ? cap * 2 : 8;
                index_row *nr = realloc(rows, cap * sizeof *rows);
                if (nr == NULL) { rc = SQLITE_NOMEM; break; }
                rows = nr;
            }
            index_row *r = &rows[n++];
            r->name = dup_col(st, 1);
            r->unique = sqlite3_column_int64(st, 2) != 0;
            r->origin = dup_col(st, 3);
            r->partial = sqlite3_column_int64(st, 4) != 0;
        }
        if (rc == SQLITE_DONE)
            rc = SQLITE_OK;
    }
    sqlite3_finalize(st);
    if (rc == SQLITE_OK) {
        qsort(rows, n, sizeof *rows, cmp_index);
        w_cstr(w, "indexes");
        w_array(w, (uint32_t)n);
        for (size_t i = 0; i < n && rc == SQLITE_OK; i++) {
            w_array(w, 6);
            w_text_or_nil(w, rows[i].name);
            sqlite3_stmt *ic;
            rc = prep(db, "select name from pragma_index_info(?) order by seqno", rows[i].name, &ic);
            w_array_open(w);
            if (rc == SQLITE_OK) {
                while ((rc = sqlite3_step(ic)) == SQLITE_ROW)
                    w_column(w, ic, 0);
                if (rc == SQLITE_DONE)
                    rc = SQLITE_OK;
            }
            sqlite3_finalize(ic);
            w_end(w);
            w_bool(w, rows[i].unique);
            w_text_or_nil(w, rows[i].origin);
            w_bool(w, rows[i].partial);
            const char *def = NULL;
            for (size_t k = 0; k < nmaster; k++)
                if (strcmp(master[k].type, "index") == 0 && rows[i].name && strcmp(master[k].name, rows[i].name) == 0)
                    def = master[k].sql;
            w_text_or_nil(w, def);
            w_end(w);
        }
        w_end(w);
    }
    for (size_t i = 0; i < n; i++) {
        free(rows[i].name);
        free(rows[i].origin);
    }
    free(rows);
    return rc;
}

/* Python's (sql or "").lstrip().upper().startswith("CREATE VIRTUAL") */
static int looks_virtual(const char *sql) {
    if (sql == NULL)
        return 0;
    while (*sql == ' ' || (*sql >= '\t' && *sql <= '\r') || (*sql >= 0x1c && *sql <= 0x1f))
        sql++;
    return strncasecmp(sql, "CREATE VIRTUAL", 14) == 0;
}

static int write_table(sqlite3 *db, wbuf *w, master_row *m, int is_view, kind_row *kinds, size_t nkinds,
                       master_row *master, size_t nmaster) {
    w_map(w, 9);
    w_cstr(w, "name");
    w_text_or_nil(w, m->name);
    w_cstr(w, "kind");
    w_cstr(w, is_view ? "view" : "table");
    int rc = write_columns(db, w, m->name);
    if (rc != SQLITE_OK)
        return rc;
    if (is_view) {
        w_cstr(w, "foreign_keys");
        w_array(w, 0);
        w_end(w);
        w_cstr(w, "indexes");
        w_array(w, 0);
        w_end(w);
    } else {
        rc = write_foreign_keys(db, w, m->name);
        if (rc == SQLITE_OK)
            rc = write_indexes(db, w, m->name, master, nmaster);
        if (rc != SQLITE_OK)
            return rc;
    }
    const char *kind = "table";
    for (size_t i = 0; i < nkinds; i++)
        if (strcmp(kinds[i].name, m->name) == 0)
            kind = kinds[i].type;
    w_cstr(w, "definition");
    w_text_or_nil(w, m->sql);
    w_cstr(w, "virtual");
    w_bool(w, !is_view && (strcmp(kind, "virtual") == 0 || looks_virtual(m->sql)));
    w_cstr(w, "hidden");
    w_bool(w, !is_view && strcmp(kind, "shadow") == 0);
    w_cstr(w, "checks");
    w_array(w, 0);
    w_end(w);
    w_end(w);
    return SQLITE_OK;
}

int schema_write(sqlite3 *db, wbuf *w) {
    sqlite3_stmt *st = NULL;
    master_row *master = NULL;
    kind_row *kinds = NULL;
    size_t nmaster = 0, nkinds = 0, cap = 0;
    int64_t version = 0;
    int rc = prep(db, "pragma schema_version", NULL, &st);
    if (rc == SQLITE_OK && (rc = sqlite3_step(st)) == SQLITE_ROW) {
        version = sqlite3_column_int64(st, 0);
        rc = SQLITE_OK;
    }
    sqlite3_finalize(st);
    if (rc != SQLITE_OK)
        return rc;
    rc = prep(db, "select type, name, tbl_name, sql from sqlite_master order by rowid", NULL, &st);
    if (rc == SQLITE_OK) {
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            if (nmaster == cap) {
                cap = cap ? cap * 2 : 32;
                master_row *nm = realloc(master, cap * sizeof *master);
                if (nm == NULL) { rc = SQLITE_NOMEM; break; }
                master = nm;
            }
            master_row *r = &master[nmaster++];
            r->type = dup_col(st, 0);
            r->name = dup_col(st, 1);
            r->tbl_name = dup_col(st, 2);
            r->sql = dup_col(st, 3);
            if (!r->type) r->type = strdup("");
            if (!r->name) r->name = strdup("");
        }
        if (rc == SQLITE_DONE)
            rc = SQLITE_OK;
    }
    sqlite3_finalize(st);
    if (rc == SQLITE_OK) {
        /* SQLite < 3.37 has no pragma_table_list: every table is a table. */
        cap = 0;
        if (prep(db, "select name, type from pragma_table_list where schema = 'main'", NULL, &st) == SQLITE_OK) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                if (nkinds == cap) {
                    cap = cap ? cap * 2 : 32;
                    kind_row *nk = realloc(kinds, cap * sizeof *kinds);
                    if (nk == NULL) break;
                    kinds = nk;
                }
                kinds[nkinds].name = dup_col(st, 0);
                kinds[nkinds].type = dup_col(st, 1);
                if (!kinds[nkinds].name) kinds[nkinds].name = strdup("");
                if (!kinds[nkinds].type) kinds[nkinds].type = strdup("table");
                nkinds++;
            }
        }
        sqlite3_finalize(st);
    }
    if (rc == SQLITE_OK) {
        w_map(w, 4);
        w_cstr(w, "version");
        w_int(w, version);
        for (int pass = 0; pass < 2 && rc == SQLITE_OK; pass++) {
            const char *want = pass == 0 ? "table" : "view";
            w_cstr(w, pass == 0 ? "tables" : "views");
            w_array_open(w);
            for (size_t i = 0; i < nmaster && rc == SQLITE_OK; i++) {
                master_row *m = &master[i];
                if (strcmp(m->type, want) != 0 || strncmp(m->name, "sqlite_", 7) == 0)
                    continue;
                rc = write_table(db, w, m, pass == 1, kinds, nkinds, master, nmaster);
            }
            w_end(w);
        }
        if (rc == SQLITE_OK) {
            w_cstr(w, "triggers");
            w_array_open(w);
            for (size_t i = 0; i < nmaster; i++) {
                if (strcmp(master[i].type, "trigger") != 0)
                    continue;
                w_array(w, 3);
                w_text_or_nil(w, master[i].name);
                w_text_or_nil(w, master[i].tbl_name);
                w_text_or_nil(w, master[i].sql);
                w_end(w);
            }
            w_end(w);
            w_end(w);
        }
    }
    for (size_t i = 0; i < nmaster; i++) {
        free(master[i].type); free(master[i].name); free(master[i].tbl_name); free(master[i].sql);
    }
    free(master);
    for (size_t i = 0; i < nkinds; i++) {
        free(kinds[i].name);
        free(kinds[i].type);
    }
    free(kinds);
    return rc;
}
