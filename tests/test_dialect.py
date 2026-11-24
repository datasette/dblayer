"""Dialect helpers that milestone 2's named operations are written against."""

import sqlite3

import pytest

from dblayer import IntegrityError, QueryError, QueryInterrupted
from dblayer.backends.base import Dialect
from dblayer.backends.sqlite import SqliteDialect


@pytest.fixture(params=[Dialect(), SqliteDialect()], ids=["generic", "sqlite"])
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
    s = SqliteDialect()
    assert s.insert_sql("t", ["a"], conflict="ignore").startswith("INSERT OR IGNORE INTO")
    assert s.insert_sql("t", ["a"], conflict="replace").startswith("INSERT OR REPLACE INTO")
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
