/* SQL text helpers that must agree with the Python side. */
#ifndef DBLAYER_SQLUTIL_H
#define DBLAYER_SQLUTIL_H

#include <stddef.h>

enum { KW_OTHER = 0, KW_SELECT, KW_INSERT, KW_UPDATE, KW_DELETE, KW_REPLACE, KW_VALUES, KW_MERGE, KW_NONE };

/* dblayer.core.params.leading_keyword (SQLite flavor): the statement's verb,
 * ignoring comments and parentheses; for WITH ... the verb after the CTEs. */
int leading_keyword(const char *sql, size_t n);

/* CPython's lstrip_sql (Modules/_sqlite/statement.c): skip whitespace and
 * comments. Returns NULL if nothing but whitespace and comments remains. */
const char *lstrip_sql(const char *p, const char *end);

/* CPython's is_dml: the first word after lstrip_sql starts with insert,
 * update, delete or replace (case-insensitive prefix match). */
int cpython_is_dml(const char *sql, size_t n);

/* pathlib.Path(path).as_uri() for an absolute POSIX path (malloc'd). */
char *path_as_uri(const char *path);

#endif
