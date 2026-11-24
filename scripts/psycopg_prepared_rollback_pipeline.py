"""psycopg 3: with server-side prepared statements, a pipeline holding
BEGIN, a statement and ROLLBACK leaves psycopg's statement cache out of step
with the server: psycopg forgets its prepared statements when it sees a
ROLLBACK (and deallocates them), but the statement prepared in the same
pipeline is recorded as prepared afterwards. A later pipeline then executes
the deallocated name: "prepared statement "_pg3_N" does not exist", and the
connection is left inside a failed transaction.

Run with ``uv run --extra psycopg python scripts/psycopg_prepared_rollback_pipeline.py``.
Uses only the ``dblayer_test`` database (``DBLAYER_TEST_POSTGRES``); creates
nothing.

Observed with psycopg 3.3.9 (libpq 18), PostgreSQL 18.3:

    run 1: ok [(2,)]
    run 1, again: ok [(2,)]
    run 2: InvalidSqlStatementName: prepared statement "_pg3_3" does not exist
    run 3: InFailedSqlTransaction: current transaction is aborted, commands ignored ...

This is why ``PostgresPsycopg`` defaults to ``prepare_threshold=None``.
"""

from __future__ import annotations

import os
import urllib.parse

import psycopg

DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")


def main() -> None:
    parts = urllib.parse.urlsplit(DSN)
    query = urllib.parse.parse_qs(parts.query)
    name = query["dbname"][0] if "dbname" in query else parts.path.lstrip("/")
    if name != "dblayer_test":
        raise SystemExit(f"refusing {DSN!r}: only the dblayer_test database may be used")
    with psycopg.Connection.connect(DSN, autocommit=True, prepare_threshold=0) as conn:
        for run in (1, 2, 3):
            try:
                with conn.pipeline():
                    conn.execute("BEGIN")
                    cur = conn.execute("select %s::int + 1", [run])
                    conn.execute("ROLLBACK")
                print(f"run {run}: ok {cur.fetchall()}")
                # The same statement again, outside a pipeline.
                print(f"run {run}, again: ok {conn.execute('select %s::int + 1', [run]).fetchall()}")
            except psycopg.Error as e:
                print(f"run {run}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
