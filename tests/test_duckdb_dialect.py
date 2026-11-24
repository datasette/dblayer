"""DuckDBDialect against the real engine (skipped without the duckdb extra)."""

import pytest

from dblayer import IntegrityError, ParameterError, QueryError, QueryInterrupted, WriteConflict
from dblayer.backends.duckdb import DuckDBDialect
from dblayer.core.params import DUCKDB_FLAVOR, ParamStyle, find_placeholders, rewrite
from dblayer.core.schema import ColumnDef, ColumnInfo, ForeignKey, Index, TableInfo

duckdb = pytest.importorskip("duckdb")


@pytest.fixture
def conn():
    c = duckdb.connect()
    yield c
    c.close()


def run(conn, sql, params=None):
    native, p = rewrite(sql, params, ParamStyle.NAMED_DOLLAR, DUCKDB_FLAVOR)
    return conn.execute(native, p).fetchall()


def test_rewritten_params_run(conn):
    assert run(conn, "select :a, :b, :a", {"a": 1, "b": 2}) == [(1, 2, 1)]
    assert run(conn, "select ?, ?", [1, 2]) == [(1, 2)]
    assert run(conn, "select '1'::int, :x::int", {"x": "5"}) == [(1, 5)]


@pytest.mark.parametrize(
    "sql",
    [
        "select $$ :x ? $$, :real",
        "select $t$ it's :x $t$, :real",
        r"select E'it\'s :x', :real",
        "select /* a /* :x */ :y */ :real",
        "select 1 as `:x`, :real",
        "select 1 as \":x\", :real",
        "select [1, 2, 3][2:3], :real",
        "select {'k': 1}, :real",
    ],
)
def test_flavor_rules_match_engine(conn, sql):
    assert [p.name for p in find_placeholders(sql, DUCKDB_FLAVOR)] == ["real"]
    assert run(conn, sql, {"real": 7})[0][-1] == 7


def test_case_insensitive_names_are_refused():
    with pytest.raises(ParameterError):
        rewrite("select :a, :A", {"a": 1, "A": 2}, ParamStyle.NAMED_DOLLAR, DUCKDB_FLAVOR)
    # the same name twice is fine
    assert rewrite("select :a, :a", {"a": 1}, ParamStyle.NAMED_DOLLAR)[0] == "select $a, $a"


def test_types_literals(conn):
    d = DuckDBDialect()
    assert d.native_type("integer") == "BIGINT"
    assert d.native_type("float") == "DOUBLE"
    assert d.literal(True) == "1"
    assert conn.execute("select " + d.literal(b"\x00\x01\xff")).fetchall() == [(b"\x00\x01\xff",)]


def test_create_table_and_upsert_run(conn):
    d = DuckDBDialect()
    conn.execute(d.create_table_sql("p", [ColumnDef("id", "integer")], pk=["id"]))
    conn.execute(
        d.create_table_sql(
            "c",
            [
                ColumnDef("id", "integer"),
                ColumnDef("pid", "integer", not_null=True),
                ColumnDef("name", "text", default="'x'"),
                ColumnDef("data", "blob"),
            ],
            pk=["id"],
            uniques=[["name"]],
            foreign_keys=[ForeignKey(("pid",), "p", ("id",))],
        )
    )
    conn.execute("insert into p values (1)")
    sql = d.upsert_sql("c", ["id", "pid", "name"], ["id"])
    conn.execute(sql, [1, 1, "a"])
    conn.execute(sql, [1, 1, "b"])
    assert conn.execute("select id, name from c").fetchall() == [(1, "b")]
    conn.execute(d.insert_sql("c", ["id", "pid"], conflict="ignore"), [1, 1])
    assert conn.execute("select count(*) from c").fetchall() == [(1,)]


def _info(indexes=(), pk=("id",)):
    cols = tuple(
        ColumnInfo(n, "BIGINT", primary_key=(pk.index(n) + 1 if n in pk else 0))
        for n in ("id", "a", "b")
    )
    return TableInfo("t", "table", columns=cols, indexes=tuple(indexes))


def test_native_alter_for():
    d = DuckDBDialect()
    full = d.native_alter
    assert d.native_alter_for(_info(), rename={"a": "x"}, drop=["b"], types={"a": "text"}) == full
    with_index = _info([Index("i", ("a",), origin="c")])
    assert d.native_alter_for(with_index, rename={"b": "x"}, drop=[], types={}) == frozenset()
    keyed = _info([Index("u", ("a",), unique=True, origin="u")])
    assert "drop" not in d.native_alter_for(keyed, rename={}, drop=["a"], types={})
    assert "types" not in d.native_alter_for(keyed, rename={}, drop=[], types={"id": "text"})
    assert "rename" in d.native_alter_for(keyed, rename={"a": "z"}, drop=["a"], types={})


def _error(conn, sql):
    try:
        conn.execute(sql)
    except duckdb.Error as e:
        return e
    raise AssertionError("no error")


def test_map_exception(conn):
    d = DuckDBDialect()
    conn.execute("create table t (id integer primary key, v integer)")
    conn.execute("insert into t values (1, 0)")
    e = d.map_exception(_error(conn, "insert into t values (1, 0)"))
    assert isinstance(e, IntegrityError) and e.code == "Constraint"
    e = d.map_exception(_error(conn, "selec 1"))
    assert type(e) is QueryError and e.code == "Parser"
    assert isinstance(d.map_exception(duckdb.InterruptException("x")), QueryInterrupted)
    a, b = conn.cursor(), conn.cursor()
    a.execute("begin")
    b.execute("begin")
    a.execute("update t set v = 1 where id = 1")
    assert isinstance(d.map_exception(_error(b, "update t set v = 2 where id = 1")), WriteConflict)
    a.execute("rollback")
    b.execute("rollback")
    assert d.map_exception(ValueError("x")) is None
