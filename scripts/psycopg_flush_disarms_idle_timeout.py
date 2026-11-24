"""A Flush message sent after Sync disarms PostgreSQL's
``idle_in_transaction_session_timeout``, and psycopg 3 sends exactly that at
the end of every pipeline (``AsyncPipeline.__aexit__``: Sync, then
``PQsendFlushRequest``). ``transaction_timeout`` (PostgreSQL 17+) is not
affected. This is why ``PostgresPsycopg`` sets both.

Run with ``uv run --extra psycopg python scripts/psycopg_flush_disarms_idle_timeout.py``.
Uses only the ``dblayer_test`` database (``DBLAYER_TEST_POSTGRES``); creates
nothing.

Observed on PostgreSQL 18.3, psycopg 3.3.9 (libpq 18):

    idle_in_transaction_session_timeout, raw libpq, Sync only:      session ended
    idle_in_transaction_session_timeout, raw libpq, Sync then Flush: still idle in transaction
    idle_in_transaction_session_timeout, psycopg pipeline():         still idle in transaction
    transaction_timeout,                 raw libpq, Sync then Flush: session ended
    transaction_timeout,                 psycopg pipeline():         session ended
"""

from __future__ import annotations

import os
import time
import urllib.parse

import psycopg
from psycopg import pq

DSN = os.environ.get("DBLAYER_TEST_POSTGRES", "postgresql:///dblayer_test?host=/tmp")


def _check_dsn() -> None:
    parts = urllib.parse.urlsplit(DSN)
    query = urllib.parse.parse_qs(parts.query)
    name = query["dbname"][0] if "dbname" in query else parts.path.lstrip("/")
    if name != "dblayer_test":
        raise SystemExit(f"refusing {DSN!r}: only the dblayer_test database may be used")


def _drain(pg: pq.abc.PGconn) -> None:
    """Read results up to and including the next PIPELINE_SYNC."""
    while True:
        while pg.is_busy():
            pg.consume_input()
        r = pg.get_result()
        if r is not None and r.status == pq.ExecStatus.PIPELINE_SYNC:
            return


def trial(setting: str, how: str) -> str:
    a = psycopg.Connection.connect(DSN, autocommit=True)
    b = psycopg.Connection.connect(DSN, autocommit=True)
    try:
        assert a.execute("select current_database()").fetchone() == ("dblayer_test",)
        a.execute(f"BEGIN; SET LOCAL {setting} = 500")
        if how == "psycopg pipeline()":
            with a.pipeline():
                a.cursor().execute("select 1")
        else:
            pg = a.pgconn
            pg.enter_pipeline_mode()
            pg.send_query_params(b"select 1", None)
            pg.pipeline_sync()
            if how.endswith("then Flush"):
                pg.send_flush_request()
            pg.flush()
            _drain(pg)
            pg.exit_pipeline_mode()
        pid = a.info.backend_pid
        time.sleep(1.0)  # twice the timeout
        rows = b.execute("select state from pg_stat_activity where pid = %s", [pid]).fetchall()
        return f"still {rows[0][0]}" if rows else "session ended"
    finally:
        a.close()
        b.close()


def main() -> None:
    _check_dsn()
    for setting, how in [
        ("idle_in_transaction_session_timeout", "raw libpq, Sync only"),
        ("idle_in_transaction_session_timeout", "raw libpq, Sync then Flush"),
        ("idle_in_transaction_session_timeout", "psycopg pipeline()"),
        ("transaction_timeout", "raw libpq, Sync then Flush"),
        ("transaction_timeout", "psycopg pipeline()"),
    ]:
        print(f"{setting:37} {how + ':':28} {trial(setting, how)}")


if __name__ == "__main__":
    main()
