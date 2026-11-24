"""The C worker's multi-tenant mode (``dblayer-cworker --multi``), driven
over raw pipes: a thread per connection, requests for different
connections running at the same time, and cancel, time limits, deadlines,
memory limits and the connection cap acting on one connection without
disturbing the others.

Timing assertions are conditions with generous bounds (CI machines are
shared); measured figures are printed (``-s``)."""

from __future__ import annotations

import os
import select
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from dblayer.backends.sqlite_cworker import binary_unavailable, find_binary
from dblayer.worker.protocol import FrameDecoder, frame, get_codec

UNAVAILABLE = binary_unavailable()
pytestmark = pytest.mark.skipif(UNAVAILABLE is not None, reason=UNAVAILABLE or "")

UNBOUNDED = "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c"


def counting(n: int) -> str:
    return f"with recursive c(x) as (select 1 union all select x + 1 from c where x < {n}) select count(*) from c"


class Raw:
    """A multi-tenant C worker on raw pipes (JSON codec). Replies may
    arrive in any order: ``reply(id)`` keeps the others for later."""

    def __init__(self, *args: str) -> None:
        self.p = subprocess.Popen(
            [find_binary(), "--multi", *args],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.codec = get_codec("json")
        self.dec = FrameDecoder()
        self.pending: list[dict[str, Any]] = []
        self.ids = 100
        self.hello = self.call({"type": "hello", "protocol": 1, "codecs": ["json"]})

    def send(self, msg: dict[str, Any]) -> int:
        if "id" not in msg and msg["type"] not in ("cancel", "more"):
            self.ids += 1
            msg["id"] = self.ids
        assert self.p.stdin is not None
        self.p.stdin.write(frame(self.codec.encode(msg)))
        self.p.stdin.flush()
        return msg.get("id", 0)

    def _read(self, timeout: float) -> None:
        assert self.p.stdout is not None
        fd = self.p.stdout.fileno()
        ready, _, _ = select.select([fd], [], [], timeout)
        if not ready:
            raise TimeoutError("no reply")
        data = os.read(fd, 1 << 16)
        if not data:
            raise EOFError("worker exited")
        self.pending.extend(self.codec.decode(f) for f in self.dec.feed(data))

    def reply(self, mid: int | None, timeout: float = 10.0, **match: Any) -> dict[str, Any]:
        """The next message with ``id == mid`` (``None``: a notice), and
        every other field in ``match`` equal."""
        end = time.monotonic() + timeout
        while True:
            for i, m in enumerate(self.pending):
                if m.get("id") == mid and all(m.get(k) == v for k, v in match.items()):
                    return self.pending.pop(i)
            self._read(max(end - time.monotonic(), 0))

    def call(self, msg: dict[str, Any], timeout: float = 10.0) -> dict[str, Any]:
        return self.reply(self.send(msg), timeout)

    def open(self, path: Path, mode: str = "write") -> int:
        r = self.call({"type": "open", "path": str(path), "mode": mode})
        assert "conn" in r, r
        return r["conn"]

    def close(self) -> int:
        assert self.p.stdin is not None
        self.p.stdin.close()
        try:
            code = self.p.wait(10)
        finally:
            if self.p.poll() is None:
                self.p.kill()
                self.p.wait()
            for f in (self.p.stdout, self.p.stderr):
                if f is not None:
                    f.close()
        return code


@pytest.fixture
def raw():
    made: list[Raw] = []

    def make(*args: str) -> Raw:
        r = Raw(*args)
        made.append(r)
        return r

    yield make
    for r in made:
        if r.p.poll() is None:
            r.close()


def test_hello_says_multi_tenant(raw):
    w = raw("--max-connections", "7")
    assert w.hello["multi"] is True
    assert w.hello["max_connections"] == 7
    assert w.hello["threading"] == "multi-thread"
    assert w.close() == 0


def test_requests_for_different_connections_run_concurrently(raw, tmp_path):
    """Eight connections each sleep 0.5 s inside the worker: one at a time
    that would take 4 s; on lanes it takes about 0.5 s."""
    w = raw("--debug")
    conns = [w.open(tmp_path / f"c{i}.db") for i in range(8)]
    t0 = time.perf_counter()
    ids = [w.send({"type": "_debug", "action": "sleep", "seconds": 0.5, "conn": c}) for c in conns]
    for mid in ids:
        assert w.reply(mid)["ok"]
    elapsed = time.perf_counter() - t0
    print(f"\n[cworker --multi] 8 x 0.5 s sleeps on 8 connections: {elapsed * 1000:.0f} ms")
    assert elapsed < 2.0  # serial would be 4 s

    # The same with real statements (CPU-bound, so machine load shows).
    sql = counting(3_000_000)
    t0 = time.perf_counter()
    assert w.call({"type": "execute", "sql": sql, "conn": conns[0]})["result"]["rows"] == [[3_000_000]]
    one = time.perf_counter() - t0
    t0 = time.perf_counter()
    ids = [w.send({"type": "execute", "sql": sql, "conn": c}) for c in conns[:4]]
    for mid in ids:
        assert w.reply(mid, timeout=30)["result"]["rows"] == [[3_000_000]]
    four = time.perf_counter() - t0
    print(f"[cworker --multi] counting query: one {one * 1000:.0f} ms, four at once {four * 1000:.0f} ms "
          f"(load {os.getloadavg()[0]:.1f})")
    assert w.close() == 0


def test_cancel_interrupts_one_connection_only(raw, tmp_path):
    w = raw()
    a, b = w.open(tmp_path / "a.db"), w.open(tmp_path / "b.db")
    slow = w.send({"type": "execute", "sql": UNBOUNDED, "conn": a})
    time.sleep(0.1)
    # B is served while A's statement runs.
    t0 = time.perf_counter()
    for i in range(20):
        assert w.call({"type": "execute", "sql": f"select {i}", "conn": b})["result"]["rows"] == [[i]]
    per = (time.perf_counter() - t0) / 20
    print(f"\n[cworker --multi] round trip on B while A runs an unbounded query: {per * 1e6:.0f} us")
    w.send({"type": "cancel", "target": slow})
    err = w.reply(slow)["error"]
    assert err["type"] == "QueryInterrupted"
    assert w.call({"type": "execute", "sql": "select 1", "conn": a})["result"]["rows"] == [[1]]
    assert w.close() == 0


def test_time_limits_are_per_connection(raw, tmp_path):
    w = raw()
    a, b = w.open(tmp_path / "a.db"), w.open(tmp_path / "b.db")
    t0 = time.perf_counter()
    ra = w.send({"type": "execute", "sql": UNBOUNDED, "conn": a, "time_limit_ms": 100})
    rb = w.send({"type": "execute", "sql": UNBOUNDED, "conn": b, "time_limit_ms": 600})
    assert w.reply(ra)["error"]["type"] == "QueryInterrupted"
    ta = time.perf_counter() - t0
    assert w.reply(rb)["error"]["type"] == "QueryInterrupted"
    tb = time.perf_counter() - t0
    print(f"\n[cworker --multi] limits 100 / 600 ms stopped at {ta * 1000:.0f} / {tb * 1000:.0f} ms")
    assert ta < tb and tb >= 0.55
    assert w.close() == 0


def test_deadlines_are_per_connection(raw, tmp_path):
    w = raw()
    a, b = w.open(tmp_path / "a.db"), w.open(tmp_path / "b.db")
    for c in (a, b):
        w.call({"type": "execute", "sql": "create table t (x)", "conn": c})
    ta = w.call({"type": "begin", "mode": "write", "deadline_ms": 100, "conn": a})["tx"]
    tb = w.call({"type": "begin", "mode": "write", "deadline_ms": 10_000, "conn": b})["tx"]
    w.call({"type": "execute", "sql": "insert into t values (1)", "conn": a, "tx": ta})
    w.call({"type": "execute", "sql": "insert into t values (2)", "conn": b, "tx": tb})
    notice = w.reply(None, timeout=5, type="expired")
    assert (notice["conn"], notice["tx"]) == (a, ta)
    # B's transaction is untouched and commits.
    assert w.call({"type": "commit", "tx": tb, "conn": b})["ok"]
    assert w.call({"type": "rollback", "tx": ta, "conn": a}).get("expired") is True
    rows = lambda c: w.call({"type": "execute", "sql": "select x from t", "conn": c})["result"].get("rows", [])
    assert rows(a) == [] and rows(b) == [[2]]
    assert w.close() == 0


def test_connection_cap(raw, tmp_path):
    w = raw("--max-connections", "2")
    a = w.open(tmp_path / "a.db")
    w.open(tmp_path / "b.db")
    err = w.call({"type": "open", "path": str(tmp_path / "c.db"), "mode": "read"})["error"]
    assert err["type"] == "ResourceLimitExceeded" and "cap" in err["message"]
    assert w.call({"type": "close", "conn": a})["ok"]
    # The lane's thread exits after its close; then there is room again.
    for _ in range(100):
        r = w.call({"type": "open", "path": str(tmp_path / "c.db"), "mode": "read"})
        if "conn" in r:
            break
        time.sleep(0.01)
    assert "conn" in r, r
    assert w.close() == 0


def test_requests_for_a_closed_or_unknown_connection_are_errors(raw, tmp_path):
    w = raw()
    a = w.open(tmp_path / "a.db")
    w.open(tmp_path / "b.db")
    w.open(tmp_path / "c.db")
    assert w.call({"type": "close", "conn": a})["ok"]
    err = w.call({"type": "execute", "sql": "select 1", "conn": a})["error"]
    assert err["code"] == "PROTOCOL" and "no connection" in err["message"]
    err = w.call({"type": "execute", "sql": "select 1"})["error"]  # two open: must name one
    assert err["code"] == "PROTOCOL"
    assert w.close() == 0


def test_heap_limit_is_process_wide_and_one_tenant_recovers(raw, tmp_path):
    """One tenant asks for more than the process-wide heap limit: it gets
    ResourceLimitExceeded, the worker carries on, and another tenant can
    use that memory afterwards."""
    w = raw("--heap-limit-mb", "64")
    a, b = w.open(tmp_path / "a.db"), w.open(tmp_path / "b.db")
    err = w.call({"type": "execute", "sql": "select length(randomblob(200000000))", "conn": a})["error"]
    assert err["type"] == "ResourceLimitExceeded"
    r = w.call({"type": "execute", "sql": "select length(randomblob(30000000))", "conn": b})
    assert r["result"]["rows"] == [[30_000_000]]
    assert w.call({"type": "execute", "sql": "pragma hard_heap_limit", "conn": a})["result"]["rows"] == [[64 << 20]]
    assert w.close() == 0


def test_per_connection_page_cache(raw, tmp_path):
    w = raw("--cache-kb", "512")
    a = w.open(tmp_path / "a.db")
    assert w.call({"type": "execute", "sql": "pragma cache_size", "conn": a})["result"]["rows"] == [[-512]]
    assert w.close() == 0


def test_deep_expression_fits_a_lane_stack(raw, tmp_path):
    w = raw()
    a = w.open(tmp_path / "a.db")
    depth = 990  # SQLITE_MAX_EXPR_DEPTH is 1000
    r = w.call({"type": "execute", "sql": "select " + "(" * depth + "1" + ")" * depth, "conn": a})
    assert r["result"]["rows"] == [[1]]
    assert w.close() == 0


def test_streams_on_two_connections_interleave(raw, tmp_path):
    w = raw()
    a, b = w.open(tmp_path / "a.db"), w.open(tmp_path / "b.db")
    sa = w.send({"type": "stream", "sql": counting(10).replace("count(*)", "x"), "chunk_rows": 3, "credit": 1, "conn": a})
    sb = w.send({"type": "stream", "sql": counting(10).replace("count(*)", "x"), "chunk_rows": 4, "credit": 1, "conn": b})
    assert w.reply(sa)["rows"] == [[1], [2], [3]]
    assert w.reply(sb)["rows"] == [[1], [2], [3], [4]]
    w.send({"type": "more", "stream": sb, "credit": 5})
    w.send({"type": "more", "stream": sa, "credit": 1})
    assert w.reply(sa)["rows"] == [[4], [5], [6]]
    assert w.reply(sb)["rows"] == [[5], [6], [7], [8]]
    assert w.reply(sb)["rows"] == [[9], [10]]
    assert w.reply(sb).get("end") is True
    w.send({"type": "cancel", "target": sa})
    assert w.reply(sa) == {"id": sa, "end": True, "cancelled": True}
    assert w.close() == 0


def test_end_of_input_with_busy_connections_exits(raw, tmp_path):
    w = raw()
    conns = [w.open(tmp_path / f"e{i}.db") for i in range(5)]
    w.call({"type": "execute", "sql": "create table t (x)", "conn": conns[0]})
    tx = w.call({"type": "begin", "mode": "write", "conn": conns[0]})["tx"]
    w.call({"type": "execute", "sql": "insert into t values (1)", "conn": conns[0], "tx": tx})
    for c in conns[1:]:
        w.send({"type": "execute", "sql": UNBOUNDED, "conn": c})
    time.sleep(0.1)
    t0 = time.perf_counter()
    assert w.close() == 0
    print(f"\n[cworker --multi] end of input with 4 running statements: exited in {(time.perf_counter() - t0) * 1000:.0f} ms")
    import sqlite3

    assert sqlite3.connect(tmp_path / "e0.db").execute("select count(*) from t").fetchone() == (0,)


def test_cancel_stops_a_lock_wait(raw, tmp_path):
    """A connection waiting for a lock another connection holds (inside
    busy_timeout, 5 s by default) stops at once when cancelled; so does end
    of input."""
    w = raw()
    holder, waiter = w.open(tmp_path / "l.db"), w.open(tmp_path / "l.db")
    tx = w.call({"type": "begin", "mode": "write", "conn": holder})["tx"]
    assert tx
    t0 = time.perf_counter()
    blocked = w.send({"type": "begin", "mode": "write", "conn": waiter})
    time.sleep(0.2)
    w.send({"type": "cancel", "target": blocked})
    err = w.reply(blocked)["error"]
    elapsed = time.perf_counter() - t0
    print(f"\n[cworker --multi] cancel of a lock wait returned after {elapsed * 1000:.0f} ms")
    assert err["type"] == "QueryInterrupted" and elapsed < 2.0
    w.send({"type": "begin", "mode": "write", "conn": waiter})
    time.sleep(0.1)
    t0 = time.perf_counter()
    assert w.close() == 0
    assert time.perf_counter() - t0 < 2.0


def test_lookaside_option(raw, tmp_path):
    w = raw("--lookaside", "0,0")
    a = w.open(tmp_path / "a.db")
    assert w.call({"type": "execute", "sql": "select 1", "conn": a})["result"]["rows"] == [[1]]
    assert w.close() == 0
    bad = subprocess.run([find_binary(), "--multi", "--lookaside", "x"], capture_output=True, text=True)
    assert bad.returncode == 2 and "SIZE,COUNT" in bad.stderr
