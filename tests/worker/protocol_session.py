"""Record a real session with a worker process, for the example in
``docs/worker-protocol.md``.

    uv run python -m tests.worker.protocol_session --write

rewrites the recorded section of the doc; ``test_protocol_doc.py`` checks
that the doc still matches what the code sends. Values that change between
runs (pids, versions, timings) are normalised.
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any

from dblayer import Database
from dblayer.backends.sqlite_worker import SqliteWorker, WorkerProcess, reap_workers

DOC = Path(__file__).resolve().parents[2] / "docs" / "worker-protocol.md"
START = "<!-- recorded-session:start -->"
END = "<!-- recorded-session:end -->"


def _normalise(msg: dict[str, Any]) -> dict[str, Any]:
    out = dict(msg)
    for key, value in (("pid", 12345), ("sqlite_version", "3.x.y"), ("python", "3.x.y")):
        if key in out:
            out[key] = value
    if out.get("type") == "open":
        out["path"] = "/data/blog.db"
    if "rlimit_as" in out:
        out["rlimit_as"] = False
    if "schema" in str(out.get("result", ""))[:0]:
        pass
    return out


def _shorten(msg: dict[str, Any]) -> dict[str, Any]:
    """Keep the example readable: the full schema reply is summarised."""
    result = msg.get("result")
    if isinstance(result, dict) and "tables" in result:
        tables = [
            {"name": t["name"], "columns": [c[:2] for c in t["columns"]], "...": "..."}
            for t in result["tables"]
        ]
        msg = {**msg, "result": {"version": result["version"], "tables": tables, "...": "..."}}
    return msg


async def record() -> list[tuple[str, dict[str, Any]]]:
    rec: list[tuple[str, dict[str, Any]]] = []
    with tempfile.TemporaryDirectory() as d:
        WorkerProcess.record_new = rec
        db = Database(SqliteWorker(Path(d) / "blog.db"), max_read_connections=1)
        try:
            # Only the write connection is recorded below (one process).
            async with db.transaction() as tx:
                await tx.create_table("posts", {"id": "integer", "title": "text"}, pk="id")
            WorkerProcess.record_new = None
            rec.append(("#", {"note": "insert with alter, as an interactive transaction"}))
            async with db.transaction() as tx:
                cols = [c.name for c in await tx.table_columns("posts")]
                if "slug" not in cols:
                    await tx.execute('alter table "posts" add column "slug" TEXT')
                await tx.execute_many(
                    "insert into posts (id, title, slug) values (:id, :title, :slug)",
                    [{"id": 1, "title": "One", "slug": "one"}, {"id": 2, "title": "Two", "slug": "two"}],
                )
            rec.append(("#", {"note": "the same work as one named operation"}))
            await db.insert_rows(
                "posts", [{"id": 3, "title": "Three", "slug": "three", "tags": "a"}], alter=True
            )
            rec.append(("#", {"note": "an error reply"}))
            try:
                await db.execute_write("insert into posts (id, title) values (1, 'dup')")
            except Exception:
                pass
            rec.append(("#", {"note": "a transaction past its deadline: the worker rolls back by itself"}))
            try:
                async with db.transaction(deadline_ms=50) as tx:
                    await tx.execute("insert into posts (id, title) values (9, 'late')")
                    time.sleep(0.2)  # the parent is unresponsive: no timer runs
                    await tx.execute("select 1")
            except Exception:
                pass
            await asyncio.sleep(0.05)
        finally:
            WorkerProcess.record_new = None
            await db.close()
        reap_workers(5)
    return rec


async def record_stream() -> list[tuple[str, dict[str, Any]]]:
    """A stream with credit, read from a read worker, then abandoned."""
    with tempfile.TemporaryDirectory() as d:
        db = Database(SqliteWorker(Path(d) / "blog.db"))
        try:
            await db.execute_write("create table posts (id integer primary key, title text)")
            await db.execute_write(
                "insert into posts (title) select 'post ' || value from json_each('[1,2,3,4,5,6,7]')"
            )
            await db.execute("select 1")
            pool = db._pool
            assert pool is not None
            conn = pool._idle_read[0].conn
            rec: list[tuple[str, dict[str, Any]]] = []
            conn.process.recorder = rec  # type: ignore[attr-defined]
            async with db.stream("select id, title from posts", chunk_rows=2) as chunks:
                n = 0
                async for _chunk in chunks:
                    n += 1
                    if n == 3:
                        break
            conn.process.recorder = None  # type: ignore[attr-defined]
        finally:
            await db.close()
        reap_workers(5)
    return rec


def render(rec: list[tuple[str, dict[str, Any]]]) -> str:
    lines = []
    for direction, msg in rec:
        if direction == "#":
            lines.append(f"\n# {msg['note']}")
            continue
        arrow = "→" if direction == "->" else "←"
        lines.append(f"{arrow} {json.dumps(_shorten(_normalise(msg)), ensure_ascii=False)}")
    return "\n".join(lines).strip()


def section(main: list, stream: list) -> str:
    return (
        f"{START}\n"
        "Recorded from the write worker of one database (JSON codec), then from a\n"
        "read worker serving a stream. Generated by `uv run python -m\n"
        "tests.worker.protocol_session --write`; pids and versions normalised.\n\n"
        "```\n" + render(main) + "\n\n# a stream with credit 2, abandoned after three chunks\n"
        + render(stream) + "\n```\n" + END
    )


def current_section() -> str:
    text = DOC.read_text()
    return text[text.index(START) : text.index(END) + len(END)]


def generate() -> str:
    main = asyncio.run(record())
    stream = asyncio.run(record_stream())
    return section(main, stream)


import time  # noqa: E402  (used inside record())


def shape(text: str) -> list[str]:
    """The recorded session reduced to message kinds, for comparison:
    ``→ execute`` / ``← result``... (values may differ between runs)."""
    out = []
    for line in text.splitlines():
        m = re.match(r"^([→←]) (\{.*\})$", line)
        if m:
            msg = json.loads(m.group(2))
            kind = msg.get("type") or next(
                (k for k in ("error", "result", "rows", "end", "ok") if k in msg), "?"
            )
            out.append(f"{m.group(1)} {kind}")
    return out


if __name__ == "__main__":
    new = generate()
    if "--write" in sys.argv:
        text = DOC.read_text()
        DOC.write_text(text.replace(current_section(), new))
        print(f"rewrote the recorded session in {DOC}")
    else:
        print(new)
