"""The worker process on its own, driven with blocking pipes (no parent
backend): the wire protocol as ``python -m dblayer.worker`` speaks it."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from dblayer.worker.protocol import FrameDecoder, frame, get_codec


class Raw:
    def __init__(self, *args: str) -> None:
        self.p = subprocess.Popen(
            [sys.executable, "-m", "dblayer.worker", *args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.codec = get_codec("json")
        self.dec = FrameDecoder()
        self.buf: list[bytes] = []
        self.next_id = 0

    def send(self, msg: dict) -> None:
        assert self.p.stdin is not None
        self.p.stdin.write(frame(self.codec.encode(msg)))
        self.p.stdin.flush()

    def recv(self) -> dict:
        assert self.p.stdout is not None
        while not self.buf:
            data = os.read(self.p.stdout.fileno(), 65536)
            assert data, "worker closed its stdout"
            self.buf += self.dec.feed(data)
        return self.codec.decode(self.buf.pop(0))

    def call(self, **msg) -> dict:
        self.next_id += 1
        self.send({"id": self.next_id, **msg})
        reply = self.recv()
        assert reply["id"] == self.next_id
        return reply

    def close(self) -> int:
        assert self.p.stdin is not None
        self.p.stdin.close()
        code = self.p.wait(5)
        for f in (self.p.stdout, self.p.stderr):
            if f is not None:
                f.close()
        return code


@pytest.fixture
def raw():
    w = Raw()
    yield w
    if w.p.poll() is None:
        w.p.kill()
        w.p.wait()
    w.close() if w.p.stdin and not w.p.stdin.closed else None


@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_hello_agrees_codec(raw, codec, tmp_path):
    if codec == "msgpack":
        pytest.importorskip("msgpack")
    reply = raw.call(type="hello", protocol=1, codecs=[codec, "json"])
    assert reply["type"] == "hello" and reply["codec"] == codec
    assert reply["pid"] == raw.p.pid and reply["sqlite_version"]
    raw.codec = get_codec(codec)
    assert raw.call(type="open", path=str(tmp_path / "h.db"), mode="write")["ok"]
    r = raw.call(type="execute", sql="select 1 as a, x'00ff' as b")["result"]
    assert r == {"columns": ["a", "b"], "rows": [[1, b"\x00\xff"]]}


def test_open_execute_and_errors(raw, tmp_path: Path):
    raw.call(type="hello", protocol=1, codecs=["json"])
    assert raw.call(type="open", path=str(tmp_path / "a.db"), mode="write") == {"id": 2, "ok": True, "conn": 1}
    r = raw.call(type="execute", sql="create table t (id integer primary key, v text)")
    assert r["result"] == {"columns": []}  # rowcount -1 is the default, left out
    r = raw.call(type="execute", sql="insert into t (v) values (:v)", params={"v": "x"})
    assert r["result"]["rowcount"] == 1 and r["result"]["last_pk"] == 1
    err = raw.call(type="execute", sql="insert into t (id) values (1)")["error"]
    assert err["type"] == "IntegrityError" and err["code"] == "SQLITE_CONSTRAINT_PRIMARYKEY"
    assert err["cause"]["type"] == "sqlite3.IntegrityError"


def test_exits_and_rolls_back_on_stdin_eof(raw, tmp_path: Path):
    path = tmp_path / "b.db"
    raw.call(type="hello", protocol=1, codecs=["json"])
    raw.call(type="open", path=str(path), mode="write")
    raw.call(type="execute", sql="create table t (x)")
    assert raw.call(type="begin", mode="write")["tx"] == "t1"
    raw.call(type="execute", sql="insert into t values (1)", tx="t1")
    assert raw.close() == 0
    import sqlite3

    conn = sqlite3.connect(path)
    assert conn.execute("select count(*) from t").fetchone() == (0,)
    conn.close()


def test_worker_tracks_transaction_deadline_itself(raw, tmp_path: Path):
    raw.call(type="hello", protocol=1, codecs=["json"])
    raw.call(type="open", path=str(tmp_path / "c.db"), mode="write")
    tx = raw.call(type="begin", mode="write", deadline_ms=100)["tx"]
    time.sleep(0.3)  # the parent says nothing; the worker acts alone
    assert raw.recv() == {"type": "expired", "conn": 1, "tx": tx}
    err = raw.call(type="execute", sql="select 1", tx=tx)["error"]
    assert err["type"] == "TransactionExpired"
    assert raw.call(type="rollback", tx=tx)["ok"]


def test_prints_from_init_hooks_do_not_corrupt_the_stream(tmp_path):
    w = Raw("--debug")
    try:
        w.call(type="hello", protocol=1, codecs=["json"])
        assert w.call(type="_debug", action="print", text="hello from a hook")["ok"]
        assert w.call(type="open", path=str(tmp_path / "p.db"), mode="write")["ok"]
    finally:
        w.close()
    assert w.p.returncode == 0
