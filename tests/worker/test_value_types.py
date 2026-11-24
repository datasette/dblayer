"""Value types DuckDB returns, through both codecs, unit-level and end to
end through a DuckDB worker (compared with in-process DuckDB)."""

from __future__ import annotations

import datetime as dt
import decimal
import uuid
from zoneinfo import ZoneInfo

import pytest

from dblayer.core.results import Column, Results
from dblayer.worker.protocol import available_codecs, get_codec, results_from_wire, results_to_wire

CODECS = [c for c in ("json", "msgpack") if c in available_codecs()]

LA = ZoneInfo("America/Los_Angeles")

SCALARS = [
    dt.date(2024, 1, 2),
    dt.time(12, 34, 56, 789000),
    dt.time(12, 0, tzinfo=dt.timezone(dt.timedelta(hours=1))),
    dt.datetime(2024, 1, 2, 3, 4, 5, 123456),
    dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc),
    dt.datetime(2024, 1, 1, 17, 4, 5, tzinfo=LA),
    dt.datetime(2024, 7, 1, 17, 4, 5, tzinfo=LA),  # summer time
    dt.datetime(9999, 12, 31, 23, 59, 59, 999999),
    decimal.Decimal("1.25"),
    decimal.Decimal("-12345678901234567890.000000001"),
    decimal.Decimal("NaN"),
    uuid.UUID("f05d813a-1dd1-4a72-b7da-634840630eb8"),
    dt.timedelta(days=32, seconds=10800),
    dt.timedelta(microseconds=-1),
    170141183460469231731687303715884105727,
]


def _same(a: object, b: object) -> bool:
    """Equal, of the same type, and (for datetimes) in the same zone."""
    if type(a) is not type(b):
        return False
    if isinstance(a, decimal.Decimal) and a.is_nan():
        return isinstance(b, decimal.Decimal) and b.is_nan()
    if isinstance(a, (dt.datetime, dt.time)):
        assert isinstance(b, (dt.datetime, dt.time))
        return a == b and a.utcoffset() == b.utcoffset() and str(a.tzinfo) == str(b.tzinfo)
    if isinstance(a, (list, tuple)):
        assert isinstance(b, (list, tuple))
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        assert isinstance(b, dict)
        return a.keys() == b.keys() and all(_same(a[k], b[k]) for k in a)
    return a == b


@pytest.mark.parametrize("codec_name", CODECS)
@pytest.mark.parametrize("value", SCALARS, ids=lambda v: type(v).__name__ + ":" + str(v)[:20])
def test_scalar_round_trip(codec_name, value):
    codec = get_codec(codec_name)
    back = codec.decode(codec.encode({"params": [value], "rows": [[value]]}))
    assert _same(back["params"][0], value)
    assert _same(back["rows"][0][0], value)


NESTED = [
    ("INTEGER[]", [1, 2, 3]),
    ("DATE[]", [dt.date(2024, 1, 1), None]),
    ("STRUCT(a INTEGER, b DATE[])", {"a": 1, "b": [dt.date(2024, 1, 1)]}),
    ("MAP(INTEGER, VARCHAR)", {1: "x", 2: "y"}),
    ("MAP(VARCHAR, INTEGER)", {"k": 1}),
    ("MAP(DATE, DECIMAL(4,1))", {dt.date(2024, 1, 1): decimal.Decimal("1.5")}),
    ("INTEGER[2]", (1, 2)),
    ('STRUCT("$b" VARCHAR)', {"$b": "not base64"}),
    ('STRUCT("$x" INTEGER)', {"$x": 1}),
    ("STRUCT(a MAP(INTEGER, INTEGER)[])", {"a": [{1: 2}, {3: 4}]}),
]


@pytest.mark.parametrize("codec_name", CODECS)
@pytest.mark.parametrize(("type_", "value"), NESTED, ids=[t for t, _ in NESTED])
def test_nested_values_in_results_round_trip(codec_name, type_, value):
    codec = get_codec(codec_name)
    r = Results.from_tuples([Column("v", type_), Column("n", "BIGINT")], [(value, 1), (None, 2)])
    back = results_from_wire(codec.decode(codec.encode(results_to_wire(r))))
    assert back.columns == ("v", "n")
    assert [c.type for c in back.columns] == [type_, "BIGINT"]
    assert _same(back.rows[0]["v"], value), back.rows[0]["v"]
    assert back.rows[1]["v"] is None


def test_plain_columns_are_not_walked():
    """Columns whose type cannot hold a map or array go out as they are."""
    from dblayer.worker.protocol import nested_columns

    assert nested_columns(["BIGINT", "VARCHAR", None, "DATE", "DECIMAL(10,2)"]) == []
    assert nested_columns(["INTEGER[]", "MAP(INTEGER, INTEGER)", "STRUCT(a INTEGER)", "INTEGER[3]"]) == [0, 1, 2, 3]


# -- end to end: DuckDB worker against in-process DuckDB -------------------------

DUCKDB_VALUES_SQL = """
select date '2024-01-02' d, time '12:34:56.789' t, timestamp '2024-01-02 03:04:05.123456' ts,
  timestamptz '2024-01-02 03:04:05+02' tstz, 1.25::decimal(10,2) dec,
  '9e2c6b1c-8a52-4e4f-9d1a-0c6f5a1b2c3d'::uuid u, interval '1 month 2 days 3 hours' iv,
  [1,2,3] l, {'a': 1, 'b': [date '2024-01-01']} s, map([1,2],['x','y']) m, map(['k'],[1]) m2,
  170141183460469231731687303715884105727::hugeint h, 'abc'::blob b, [1,2]::int[2] arr,
  timestamp_ns '2024-01-01 00:00:00.123456789' tns, time with time zone '12:00:00+01' ttz,
  '{"a":1}'::json j, 'infinity'::timestamp inf, bit '0101' bits, 1.5::float f,
  {'$b': 'x'} dollar, [map([date '2024-01-01'], [1.5::decimal(3,1)])] lm
"""


def _worker_class(kind: str) -> type:
    """The Python DuckDB worker, or the C++ one (skipped when not built)."""
    if kind == "cpp":
        from dblayer.backends.duckdb_cppworker import DuckDBCppWorker, binary_unavailable

        reason = binary_unavailable()
        if reason:
            pytest.skip(reason)
        return DuckDBCppWorker
    from dblayer.backends.duckdb_worker import DuckDBWorker

    return DuckDBWorker


WORKERS = ["python", "cpp"]


@pytest.mark.parametrize("worker", WORKERS)
@pytest.mark.parametrize("codec_name", CODECS)
async def test_duckdb_values_match_in_process(tmp_path, codec_name, worker):
    pytest.importorskip("duckdb")
    from dblayer import Database
    from dblayer.backends.duckdb import DuckDB

    local = Database(DuckDB(tmp_path / "a.duckdb"))
    remote = Database(_worker_class(worker)(tmp_path / "b.duckdb", codec=codec_name))
    try:
        a = await local.execute(DUCKDB_VALUES_SQL)
        b = await remote.execute(DUCKDB_VALUES_SQL)
        assert a.columns == b.columns
        assert [c.type for c in a.columns] == [c.type for c in b.columns]
        for name in a.columns:
            assert _same(a.rows[0][name], b.rows[0][name]), (name, a.rows[0][name], b.rows[0][name])
        # Streams carry the same values and column types.
        chunks = [c async for c in remote.stream(DUCKDB_VALUES_SQL)]
        assert _same(list(chunks[0][0]), list(a.rows[0].as_tuple()))
    finally:
        await local.close()
        await remote.close()


@pytest.mark.parametrize("worker", WORKERS)
@pytest.mark.parametrize("codec_name", CODECS)
async def test_duckdb_value_parameters_round_trip(tmp_path, codec_name, worker):
    pytest.importorskip("duckdb")
    from dblayer import Database

    db = Database(_worker_class(worker)(tmp_path / "p.duckdb", codec=codec_name))
    try:
        await db.execute_write(
            "create table t (d date, ts timestamp, dec decimal(18,3), u uuid, iv interval, tz timestamptz)"
        )
        row = {
            "d": dt.date(2024, 2, 29),
            "ts": dt.datetime(2024, 2, 29, 1, 2, 3, 456000),
            "dec": decimal.Decimal("123.456"),
            "u": uuid.UUID("9e2c6b1c-8a52-4e4f-9d1a-0c6f5a1b2c3d"),
            "iv": dt.timedelta(hours=5, microseconds=7),
            "tz": dt.datetime(2024, 2, 29, 1, 2, 3, tzinfo=dt.timezone.utc),
        }
        await db.execute_write("insert into t values (:d, :ts, :dec, :u, :iv, :tz)", row)
        got = (await db.execute("select d, ts, dec, u, iv, tz from t where d = :d", {"d": row["d"]})).rows[0]
        for k, v in row.items():
            if k == "tz":
                assert got[k] == v  # same instant; returned in the session's zone
            else:
                assert _same(got[k], v), (k, got[k])
    finally:
        await db.close()
