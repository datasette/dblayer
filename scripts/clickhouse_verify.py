"""Feasibility checks behind ``dblayer/backends/clickhouse.py`` (chDB).

Run: ``uv run --python 3.14 --extra clickhouse python -u scripts/clickhouse_verify.py``
(chDB 4.5.1 ships bindings for Python 3.9 to 3.14 only.)

Observed on chDB 4.5.1, ClickHouse 26.11.1.1, macOS arm64, Python 3.14,
under load average 15 to 26 (other agents building):

* ``import chdb`` (which imports pandas and pyarrow) costs about 160 MB of
  resident memory; opening a session about 1 MB more.
* **One embedded server per process, at one path.** A second
  ``Connection`` to a *different* directory raises "EmbeddedServer already
  initialized with path ..., cannot connect with different". Further
  connections to the same directory work and share the catalog. Once every
  connection is closed, a different directory can be opened. So many
  dblayer databases share one chDB data directory, one ClickHouse
  *database* each (``CREATE DATABASE``), like PostgreSQL schemas.
* Queries on two connections run in parallel (the GIL is released): two
  0.26 s scans took 0.28 s together.
* ``SET`` is per connection and persists. ``USE db`` too.
* Typed parameters ``{name:Type}``: values are parsed in ClickHouse's
  *Escaped* format, so ``\\``, tab and newline must be escaped, ``\\N`` is
  NULL, ``\\xFF`` gives a raw byte. A raw tab or newline in a value is an
  error. Values never reach the SQL parser (an injection attempt came back
  as the literal string).
* ``readonly=1`` refuses INSERT, DDL, ``SET`` (any setting, including in a
  query's own ``SETTINGS``), ``OPTIMIZE``, ``SYSTEM``, and every table
  function tried: ``file``, ``url``, ``remote``, ``s3``, ``sqlite``,
  ``executable``. It does **not** refuse ``SELECT ... INTO OUTFILE``, which
  wrote a file on the host, nor ``USE other_db`` (reads of other databases
  of the same server are allowed). Settings fixed before ``SET readonly=1``
  stay in force and cannot be changed afterwards.
* ``readonly=2`` (settings may change) allows ``file('/etc/hosts')`` and
  makes network connections for ``url()`` and ``remote()``: unusable for
  untrusted SQL.
* ``max_execution_time`` stops a scan within ~1 ms of the limit (201 ms
  for 200 ms), also ``sleepEachRow`` (212 ms).
* ``StreamingResult.cancel()`` from another thread, while ``fetch()`` waits
  on a query that produces no block (``count()`` over 10^11 numbers),
  blocked until the query finished (9.6 s). After a partial fetch it
  returns in 0.2 ms.
* ``KILL QUERY WHERE query LIKE '/* tag */%'`` from another connection
  cancels a running query: the victim raised QUERY_WAS_CANCELLED 0.2 s
  after the KILL (SYNC waited 220 ms).
* No transactions: ``BEGIN TRANSACTION`` raises "Transactions are not
  supported" (NOT_IMPLEMENTED) on MergeTree in chDB.
* Duplicate primary keys are accepted. ``UPDATE`` (lightweight update)
  needs ``enable_block_number_column = 1, enable_block_offset_column = 1``
  on the table; ``DELETE FROM`` (lightweight delete) works on any MergeTree.
  Neither reports how many rows it changed (``rows_written`` is 0).
  ``INSERT`` reports ``rows_written``.
* Arrow output: strings come as Arrow ``string`` even when not UTF-8
  (pyarrow then fails to decode); ``output_format_arrow_string_as_string=0``
  gives ``binary``. DateTime comes as uint32 seconds, Date as date32,
  DateTime64 as timestamp, Decimal, UUID, Array, Map and Tuple map.
* Two statements in one call (``SELECT 1; SELECT 2``) both run.
* Point read by primary key on 1M rows: 1.3 ms; ``SELECT 1``: 0.5 ms
  (Arrow), 1.0 ms (TSV).
* ClickHouse lexer: nested ``/* /* */ */`` comments, ``$$heredoc$$``
  strings, backslash escapes in every '...' string, ``"x"`` and
  ```x``` identifiers, ``a ? b : c`` ternary.
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from pathlib import Path

import chdb
from chdb.state import sqlitelike


def q(conn: sqlitelike.Connection, sql: str, fmt: str = "TSV", params: dict | None = None) -> str:
    try:
        return conn.query(sql, fmt, params=params).bytes().decode(errors="replace").strip()[:200]
    except Exception as e:  # noqa: BLE001
        return "ERR " + str(e).splitlines()[0][:160]


def main() -> None:
    print("python", sys.version.split()[0], "chdb", ".".join(chdb.chdb_version), "engine", chdb.engine_version)
    tmp = Path(tempfile.mkdtemp(prefix="chverify"))
    a = sqlitelike.Connection(str(tmp / "a"))
    try:
        sqlitelike.Connection(str(tmp / "b"))
        print("second directory: opened")
    except Exception as e:  # noqa: BLE001
        print("second directory:", str(e).splitlines()[0][:120])
    a2 = sqlitelike.Connection(str(tmp / "a"))
    print(q(a, "CREATE DATABASE d1"), q(a, "CREATE TABLE d1.t (id Int64, v String) ENGINE = MergeTree ORDER BY id"))
    print("same directory, second connection sees d1.t:", q(a2, "SELECT name FROM system.tables WHERE database = 'd1'"))
    a2.close()

    print("params:", q(a, "SELECT {x:Int64} + 1, {s:String}", params={"x": 41, "s": "'); DROP TABLE d1.t; --"}))
    for v in ["a\\nb", "tab\there", "\\N", "\\xff"]:
        print(f"param {v!r}:", q(a, "SELECT {s:Nullable(String)} AS s, hex(s)", params={"s": v}))

    r = sqlitelike.Connection(str(tmp / "a"))
    q(r, "USE d1")
    q(r, "SET max_execution_time = 1")
    q(r, "SET readonly = 1")
    for label, sql in [
        ("insert", "INSERT INTO t VALUES (9, 'z')"),
        ("create", "CREATE TABLE z (x Int64) ENGINE = Memory"),
        ("set", "SET max_execution_time = 7"),
        ("settings clause", "SELECT 1 SETTINGS max_execution_time = 7"),
        ("file()", "SELECT * FROM file('/etc/hosts', 'LineAsString') LIMIT 1"),
        ("url()", "SELECT * FROM url('http://127.0.0.1:9/', 'LineAsString')"),
        ("remote()", "SELECT * FROM remote('127.0.0.1:9', system.one)"),
        ("executable()", "SELECT * FROM executable('ls', 'LineAsString', 'x String')"),
        ("INTO OUTFILE", f"SELECT 1 INTO OUTFILE '{tmp / 'outfile.txt'}'"),
        ("USE default", "USE default"),
        ("timeout", "SELECT count() FROM numbers(1000000000000)"),
    ]:
        print(f"readonly=1 {label}:", q(r, sql))
    print("INTO OUTFILE wrote a file:", (tmp / "outfile.txt").exists())
    r.close()

    w = sqlitelike.Connection(str(tmp / "a"))
    res: dict = {}

    def victim() -> None:
        t = time.perf_counter()
        res["out"] = q(w, "/* dblayer:abc */ SELECT count() FROM numbers(100000000000)")
        res["ms"] = round((time.perf_counter() - t) * 1000)

    th = threading.Thread(target=victim)
    th.start()
    time.sleep(0.3)
    q(a, "KILL QUERY WHERE query LIKE '/* dblayer:abc */%' ASYNC")
    th.join(10)
    print("KILL QUERY:", res)

    print("BEGIN:", q(a, "BEGIN TRANSACTION"))
    q(a, "INSERT INTO d1.t VALUES (1, 'a'), (1, 'dup')")
    print("duplicate key rows:", q(a, "SELECT count() FROM d1.t WHERE id = 1"))
    print("UPDATE without block columns:", q(a, "UPDATE d1.t SET v = 'x' WHERE id = 1")[:90])
    print("SELECT 1; SELECT 2:", q(a, "SELECT 1; SELECT 2").replace("\n", " "))
    w.close()
    a.close()
    os.system(f"rm -rf {tmp}")


if __name__ == "__main__":
    main()
