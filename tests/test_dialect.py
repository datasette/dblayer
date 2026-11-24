"""Dialect helpers that milestone 2's named operations are written against."""

import sqlite3

import pytest

from dblayer import IntegrityError, QueryError, QueryInterrupted
from dblayer.backends.base import Dialect
from dblayer.backends.postgres import PostgresDialect
from dblayer.backends.sqlite import SqliteDialect


@pytest.fixture(params=[Dialect(), SqliteDialect(), PostgresDialect()], ids=["generic", "sqlite", "postgres"])
def dialect(request):
    return request.param


def test_quote_identifier(dialect):
    assert dialect.quote_identifier("t") == '"t"'
    assert dialect.quote_identifier('we"ird') == '"we""ird"'
    assert dialect.quote_identifiers(["a", "b c"]) == '"a", "b c"'


def test_native_types():
    assert SqliteDialect().native_type("integer") == "INTEGER"
    assert SqliteDialect().native_type("FLOAT") == "REAL"
    assert SqliteDialect().native_type("blob") == "BLOB"
    assert Dialect().native_type("blob") == "BYTEA"
    assert Dialect().native_type("varchar(20)") == "varchar(20)"


def test_insert_sql(dialect):
    assert dialect.insert_sql("t", ["a", "b"]) == 'INSERT INTO "t" ("a", "b") VALUES (?, ?)'


def test_conflict_variants():
    # SQLite uses the portable ON CONFLICT forms, not OR IGNORE / OR REPLACE.
    s = SqliteDialect()
    assert s.insert_sql("t", ["a"], conflict="ignore").endswith("ON CONFLICT DO NOTHING")
    g = Dialect()
    assert g.insert_sql("t", ["a"], conflict="ignore").endswith("ON CONFLICT DO NOTHING")
    with pytest.raises(ValueError):
        g.insert_sql("t", ["a"], conflict="replace")
    assert "DO UPDATE SET" in g.insert_sql(
        "t", ["id", "a"], conflict="replace", conflict_columns=["id"]
    )


def test_upsert_sql(dialect):
    sql = dialect.upsert_sql("t", ["id", "a", "b"], ["id"])
    assert sql == (
        'INSERT INTO "t" ("id", "a", "b") VALUES (?, ?, ?) ON CONFLICT ("id") '
        'DO UPDATE SET "a" = excluded."a", "b" = excluded."b"'
    )
    assert dialect.upsert_sql("t", ["id"], ["id"]).endswith('ON CONFLICT ("id") DO NOTHING')


def test_sqlite_upsert_sql_runs():
    d = SqliteDialect()
    conn = sqlite3.connect(":memory:")
    conn.execute("create table t (id integer primary key, a, b)")
    sql = d.upsert_sql("t", ["id", "a", "b"], ["id"])
    conn.execute(sql, (1, "x", "y"))
    conn.execute(sql, (1, "x2", "y2"))
    assert conn.execute("select * from t").fetchall() == [(1, "x2", "y2")]


def test_sqlite_map_exception():
    d = SqliteDialect()
    conn = sqlite3.connect(":memory:")
    conn.execute("create table t (id integer primary key)")
    conn.execute("insert into t values (1)")
    try:
        conn.execute("insert into t values (1)")
    except sqlite3.Error as e:
        mapped = d.map_exception(e)
        assert isinstance(mapped, IntegrityError)
        assert mapped.code == "SQLITE_CONSTRAINT_PRIMARYKEY"
    try:
        conn.execute("selec")
    except sqlite3.Error as e:
        mapped = d.map_exception(e)
        assert type(mapped) is QueryError and mapped.code == "SQLITE_ERROR"
    assert isinstance(d.map_exception(sqlite3.OperationalError("interrupted")), QueryInterrupted)
    assert d.map_exception(ValueError("x")) is None
    assert Dialect().map_exception(sqlite3.Error("x")) is None


def test_upsert_sql_replace_columns(dialect):
    sql = dialect.upsert_sql("t", ["id", "a"], ["id"], update_columns=["id", "a", "b"])
    assert sql.endswith('DO UPDATE SET "a" = excluded."a", "b" = excluded."b"')


def test_literals(dialect):
    assert dialect.literal(None) == "NULL"
    assert dialect.literal(3) == "3"
    assert dialect.literal(1.5) == "1.5"
    assert dialect.literal("it's") == "'it''s'"
    with pytest.raises(TypeError):
        dialect.literal(object())
    assert SqliteDialect().literal(b"\x01\xff") == "X'01ff'"


def test_default_clause(dialect):
    assert dialect.default_clause("'uk'") == "DEFAULT 'uk'"
    assert dialect.default_clause("-1.5") == "DEFAULT -1.5"
    assert dialect.default_clause("CURRENT_TIMESTAMP") == "DEFAULT CURRENT_TIMESTAMP"
    assert dialect.default_clause("1 + 1") == "DEFAULT (1 + 1)"


def test_sqlite_create_table_sql_runs():
    from dblayer.core.schema import ColumnDef, ForeignKey

    d = SqliteDialect()
    conn = sqlite3.connect(":memory:")
    conn.execute(d.create_table_sql("p", [ColumnDef("id", "integer")], pk=["id"]))
    sql = d.create_table_sql(
        "c",
        [
            ColumnDef("id", "integer"),
            ColumnDef("pid", "integer", not_null=True),
            ColumnDef("name", "text", default="'x'"),
            ColumnDef("data", "blob"),
        ],
        pk=["id"],
        uniques=[["name"]],
        foreign_keys=[ForeignKey(("pid",), "p", ("id",), on_delete="CASCADE")],
    )
    conn.execute(sql)
    cols = conn.execute("select name, type, \"notnull\", dflt_value, pk from pragma_table_info('c')").fetchall()
    assert cols == [
        ("id", "INTEGER", 0, None, 1),
        ("pid", "INTEGER", 1, None, 0),
        ("name", "TEXT", 0, "'x'", 0),
        ("data", "BLOB", 0, None, 0),
    ]
    # INTEGER + table-constraint PRIMARY KEY is still the rowid alias.
    conn.execute("insert into c (pid) values (1)")
    assert conn.execute("select id, rowid from c").fetchall() == [(1, 1)]
    fk = conn.execute("select \"table\", \"from\", \"to\", on_delete from pragma_foreign_key_list('c')").fetchall()
    assert fk == [("p", "pid", "id", "CASCADE")]
    assert SqliteDialect().native_alter == frozenset({"rename"})
    assert Dialect().native_alter >= {"rename", "drop", "types"}


def test_postgres_types_and_literals():
    d = PostgresDialect()
    assert d.native_type("integer") == "BIGINT"
    assert d.native_type("float") == "DOUBLE PRECISION"
    assert d.native_type("blob") == "BYTEA"
    assert d.native_type("timestamptz") == "timestamptz"
    # Portable columns have no boolean type: booleans render as numbers.
    assert d.literal(True) == "1" and d.literal(False) == "0"
    assert d.literal(b"\x01\xff") == "'\\x01ff'::bytea"
    assert d.native_alter == frozenset({"rename", "drop", "types", "pk"})
    assert d.rowid_column is None


def test_postgres_alter_sql():
    d = PostgresDialect()
    assert d.alter_column_type_sql("t", "a", "integer") == (
        'ALTER TABLE "t" ALTER COLUMN "a" TYPE BIGINT USING "a"::BIGINT'
    )
    assert d.set_primary_key_sql("t", "t_pkey", ["a", "b"]) == [
        'ALTER TABLE "t" DROP CONSTRAINT "t_pkey", ADD PRIMARY KEY ("a", "b")'
    ]
    assert d.set_primary_key_sql("t", None, ["a"]) == ['ALTER TABLE "t" ADD PRIMARY KEY ("a")']
    assert d.set_primary_key_sql("t", "t_pkey", []) == ['ALTER TABLE "t" DROP CONSTRAINT "t_pkey"']
    assert d.set_primary_key_sql("t", None, []) == []
    rename, constraints = d.swap_rename_sql("_dblayer_import_1_ab", "t")
    assert rename == 'ALTER TABLE "_dblayer_import_1_ab" RENAME TO "t"'
    assert constraints.startswith("DO $dblayer$") and "starts_with(conname, '_dblayer_import_1_ab')" in constraints


def test_postgres_map_exception():
    asyncpg = pytest.importorskip("asyncpg")
    from dblayer import ResourceLimitExceeded, TransactionExpired, WriteConflict

    d = PostgresDialect()
    e = d.map_exception(asyncpg.exceptions.UniqueViolationError("dup"))
    assert isinstance(e, IntegrityError) and e.code == "23505"
    assert isinstance(d.map_exception(asyncpg.exceptions.NotNullViolationError("n")), IntegrityError)
    assert isinstance(d.map_exception(asyncpg.exceptions.QueryCanceledError("c")), QueryInterrupted)
    assert isinstance(d.map_exception(asyncpg.exceptions.SerializationError("s")), WriteConflict)
    assert isinstance(d.map_exception(asyncpg.exceptions.DeadlockDetectedError("d")), WriteConflict)
    assert isinstance(
        d.map_exception(asyncpg.exceptions.IdleInTransactionSessionTimeoutError("i")),
        TransactionExpired,
    )
    assert isinstance(d.map_exception(asyncpg.exceptions.OutOfMemoryError("m")), ResourceLimitExceeded)
    e = d.map_exception(asyncpg.exceptions.UndefinedTableError("no t"))
    assert type(e) is QueryError and e.code == "42P01"
    assert isinstance(d.map_exception(TimeoutError()), QueryInterrupted)
    assert d.map_exception(ValueError("x")) is None
