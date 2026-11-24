"""How often concurrent DuckDB writers conflict with the write queue off.

``DuckDB(path, write_queue=False)`` sets ``concurrent_writers``, so the
core pool lets up to ``max_write_connections`` write transactions run at
once and DuckDB's optimistic concurrency control decides. Each scenario
runs 8 writer tasks x 25 read-modify-write transactions (200 in all) and
counts the transactions that failed with ``WriteConflict`` on their first
attempt. With the queue on (the default) the same workload never conflicts.

Measured on DuckDB 2.0.1, M-series Mac (``uv run --extra duckdb pytest -s
tests/test_duckdb_conflicts.py``), three runs:

=========================  ==========================  ===========
scenario                   conflict rate, queue off    queue on
=========================  ==========================  ===========
append (distinct new ids)  0/200 (0%)                  0%
update 1 of 1,000 rows     0/200 (0%)                  0%
update one hot row         162-166/200 (81-83%)        0%
=========================  ==========================  ===========

Appends never conflict (as DuckDB documents). Updates conflict only when
two open transactions touch the same row: rare when spread over many rows,
nearly always for a hot row, because every transaction that read the row
before another committed its update fails. No update was lost in any run:
the counter always equals the number of committed transactions.
"""

from __future__ import annotations

import asyncio
import random

import pytest

from dblayer import Database, WriteConflict

pytest.importorskip("duckdb")

from dblayer.backends.duckdb import DuckDB  # noqa: E402

WRITERS = 8
PER_WRITER = 25


async def _run(db: Database, scenario: str) -> tuple[int, int]:
    """Returns (committed, conflicts)."""
    await db.execute_write("create table rows (id integer primary key, v integer)")
    await db.insert_rows("rows", [{"id": i, "v": 0} for i in range(1000)], batch_size=1000)
    next_id = iter(range(10_000, 1_000_000))
    committed = conflicts = 0
    rng = random.Random(42)

    async def one() -> None:
        nonlocal committed, conflicts
        try:
            async with db.transaction() as tx:
                if scenario == "append":
                    await tx.execute("insert into rows values (?, 0)", [next(next_id)])
                else:
                    key = 0 if scenario == "hot" else rng.randrange(1000)
                    v = (await tx.execute("select v from rows where id = ?", [key])).rows[0][0]
                    await asyncio.sleep(0)  # let other writers interleave
                    await tx.execute("update rows set v = ? where id = ?", [v + 1, key])
            committed += 1
        except WriteConflict:
            conflicts += 1

    async def writer() -> None:
        for _ in range(PER_WRITER):
            await one()

    await asyncio.gather(*(writer() for _ in range(WRITERS)))
    if scenario != "append":
        total = (await db.execute("select sum(v) from rows")).rows[0][0]
        assert total == committed, "an update was lost"
    return committed, conflicts


@pytest.mark.parametrize("scenario", ["append", "spread", "hot"])
@pytest.mark.parametrize("queue", [False, True], ids=["queue-off", "queue-on"])
async def test_conflict_rate(tmp_path, scenario, queue, capsys):
    backend = DuckDB(tmp_path / "c.duckdb", write_queue=queue, max_write_connections=WRITERS)
    assert backend.capabilities.concurrent_writers is (not queue)
    db = Database(backend)
    try:
        committed, conflicts = await _run(db, scenario)
    finally:
        await db.close()
    total = WRITERS * PER_WRITER
    assert committed + conflicts == total
    rate = conflicts / total
    with capsys.disabled():
        print(f"\n{scenario:7} queue={'on ' if queue else 'off'} conflicts {conflicts}/{total} = {rate:.0%}")
    if queue or scenario == "append":
        assert conflicts == 0
    elif scenario == "hot":
        assert conflicts > total // 4
