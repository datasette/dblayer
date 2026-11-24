"""The demo app's fleet: a small one (30 databases, cap 4, half-second idle
timeout) behind one Registry, driven through httpx's ``ASGITransport``.

Needs the ``demo`` group. PostgreSQL members are marked unavailable when no
server is reachable, and DuckDB ones when duckdb is not installed; the
tests only count on the in-process SQLite members.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("starlette")
pytest.importorskip("httpx")
pytest.importorskip("jinja2")

import httpx  # noqa: E402

from dblayer_demo import create_app  # noqa: E402
from dblayer_demo.config import DemoConfig  # noqa: E402
from dblayer_demo.fleet import FANOUT_MAX, FanoutBusy, Fleet, fleet_mix  # noqa: E402

SIZE = 30
CAP = 4


@pytest.fixture
async def fleet_client(tmp_path: Path) -> AsyncIterator[tuple[Fleet, httpx.AsyncClient]]:
    config = DemoConfig(
        data_dir=tmp_path,
        fleet_size=SIZE,
        fleet_max_open_connections=CAP,
        fleet_max_threads=4,
        fleet_idle_timeout=0.5,
    )
    app = create_app(config, specs=[])
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://demo", timeout=60
        ) as client:
            yield app.state.fleet, client


async def _stats(client: httpx.AsyncClient) -> dict[str, Any]:
    r = await client.get("/-/stats.json")
    assert r.status_code == 200
    return r.json()


async def _wait_asleep(client: httpx.AsyncClient, timeout: float = 10.0) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        s = (await _stats(client))["fleet"]
        if s["awake"] == 0 and s["open_connections"] == 0:
            return s
        assert asyncio.get_running_loop().time() < deadline, s
        await asyncio.sleep(0.1)


def test_fleet_mix_adds_up() -> None:
    for size in [0, 1, 9, 10, 30, 1000, 1234]:
        assert sum(fleet_mix(size).values()) == max(size, 0)
    assert fleet_mix(1000) == {
        "sqlite": 500, "sqlite-cworker": 200, "sqlite-cpool": 100, "duckdb": 100, "duckdb-worker": 50,
        "postgres": 30, "sqlite-worker": 20,
    }


async def test_fleet_starts_cold_and_page_lists_every_member(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    fleet, client = fleet_client
    assert len(fleet.members) == SIZE
    s = (await _stats(client))["fleet"]
    assert s["fleet_size"] == SIZE
    assert s["registered"] + s["unavailable"] == SIZE
    assert s["cold"] == s["registered"]  # nothing materialised yet
    assert not list((fleet.dir).glob("sqlite-*.db"))

    r = await client.get("/-/fleet")
    assert r.status_code == 200
    for name in fleet.members:
        assert f'href="/{name}"' in r.text
    assert "Fleet: 30 databases" in r.text

    r = await client.get("/-/fleet?backend=sqlite&state=cold")
    shown = re.findall(r'<a class="s-cold" href="/(sqlite-\d+)"', r.text)
    assert len(shown) == fleet.mix["sqlite"]
    grid = "".join(re.findall(r'<div class="grid">.*?</div>', r.text, re.S))
    assert "sqlite-worker" not in grid and "sqlite-cworker" not in grid
    r = await client.get("/-/fleet?backend=sqlite-cworker")
    assert r.status_code == 200
    shown = re.findall(r'href="/(sqlite-cworker-\d+)"', r.text)
    assert len(shown) == fleet.mix["sqlite-cworker"] > 0
    assert (await client.get("/-/fleet?state=bogus")).status_code == 400

    home = await client.get("/")
    assert 'href="/-/fleet"' in home.text and 'id="stats"' in home.text


async def test_fleet_database_browses_like_a_featured_one(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    fleet, client = fleet_client
    m = fleet.members["sqlite-0002"]
    assert m.state == "cold"
    r = await client.get("/sqlite-0002")
    assert r.status_code == 200
    assert m.branch in r.text  # the page title: this branch's own name
    assert "450 rows" in r.text  # its own slice of reviews
    assert m.state == "awake" and m.materialised == 1
    assert m.path is not None and m.path.exists()

    r = await client.get("/sqlite-0002/branch")
    assert m.branch in r.text
    r = await client.get("/sqlite-0002/books?_offset=100")
    assert r.status_code == 200 and 'rel="prev"' in r.text
    r = await client.get(
        "/sqlite-0002/-/query", params={"sql": "select name from branch"}
    )
    assert r.status_code == 200 and m.branch in r.text
    r = await client.get("/sqlite-0002/-/query", params={"sql": "delete from books"})
    assert r.status_code == 400

    # Different databases hold different data.
    r3 = await client.get("/sqlite-0003/-/query", params={"sql": "select name from branch"})
    assert fleet.members["sqlite-0003"].branch in r3.text and m.branch not in r3.text


async def test_fanout_respects_the_cap_and_moves_the_stats(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    fleet, client = fleet_client
    before = (await _stats(client))["fleet"]
    r = await client.get("/-/fanout.json", params={"n": 120})
    assert r.status_code == 200
    d = r.json()
    assert d["ran"] == 120 and d["ok"] == 120 and d["errors"] == 0, d["error_samples"]
    assert 1 < d["distinct"] <= before["registered"]
    assert d["peak"]["open_connections"] <= CAP
    assert d["cap_held"] is True
    assert d["peak"]["waiting"] > 0  # the cap really queued callers
    after = (await _stats(client))["fleet"]
    assert after["wakes"] >= before["wakes"] + d["distinct"]
    assert after["cold"] <= before["cold"] - d["distinct"]
    assert after["open_connections"] <= CAP

    page = await client.get("/-/fanout", params={"n": 20})
    assert page.status_code == 200 and "Peak during the run" in page.text and "(held)" in page.text


async def test_fanout_is_bounded_and_one_at_a_time(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    fleet, client = fleet_client
    d = (await client.get("/-/fanout.json", params={"n": 10**9})).json()
    assert d["requested"] == FANOUT_MAX and d["ran"] == FANOUT_MAX
    assert (await client.get("/-/fanout.json", params={"n": "x"})).status_code == 400
    assert (await client.get("/-/fanout.json", params={"action": "drop"})).status_code == 400
    async with fleet._fanout_lock:
        with pytest.raises(FanoutBusy):
            await fleet.fanout(5)
        assert (await client.get("/-/fanout.json", params={"n": 5})).status_code == 429


async def test_databases_sleep_after_the_idle_timeout_then_evict_and_rebuild(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    fleet, client = fleet_client
    d = (await client.get("/-/fanout.json", params={"n": 40})).json()
    assert d["ok"] == 40
    assert (await _stats(client))["fleet"]["awake"] > 0
    s = await _wait_asleep(client)
    assert s["sleeps"] >= d["distinct"] and s["idle_closes"] > 0
    asleep_before = s["asleep"]
    assert asleep_before >= d["distinct"]

    e = (await client.get("/-/fanout.json", params={"n": 5, "action": "evict"})).json()
    assert e["ok"] == 5
    s2 = (await _stats(client))["fleet"]
    assert s2["evictions"] == 5 and s2["asleep"] == asleep_before - 5
    evicted = [m for m in fleet.members.values() if m.materialised and m.state == "cold"]
    assert len(evicted) == 5
    m = evicted[0]
    assert m.path is not None and not m.path.exists()
    r = await client.get(f"/{m.name}/-/query", params={"sql": "select name from branch"})
    assert r.status_code == 200 and m.branch in r.text  # rebuilt from the template
    assert m.materialised == 2


async def test_one_member_of_every_backend_kind_browses(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    """Each kind builds from the right template: a worker-process SQLite
    member is a SQLite file, not a copy of the DuckDB template, and a
    worker-process DuckDB member is a copy of the DuckDB template."""
    fleet, client = fleet_client
    seen: set[str] = set()
    for name, m in fleet.members.items():
        if m.kind in seen or m.state == "unavailable":
            continue
        seen.add(m.kind)
        r = await client.get(f"/{name}")
        assert r.status_code == 200, (name, r.text[:300])
        assert m.branch in r.text
        r = await client.get(f"/{name}/-/query", params={"sql": "select name from branch"})
        assert r.status_code == 200 and m.branch in r.text, name
    assert {"sqlite", "sqlite-worker"} <= seen
    from dblayer.backends.sqlite_cworker import binary_unavailable

    if binary_unavailable() is None:
        assert "sqlite-cworker" in seen
        # A C worker member is a SQLite file copied from the SQLite template.
        cw = next(m for m in fleet.members.values() if m.kind == "sqlite-cworker")
        assert cw.path is not None and cw.path.read_bytes()[:16] == b"SQLite format 3\x00"
        # A multi-tenant C pool member likewise, served by the fleet's own pool.
        assert "sqlite-cpool" in seen
        cp = next(m for m in fleet.members.values() if m.kind == "sqlite-cpool")
        assert cp.path is not None and cp.path.read_bytes()[:16] == b"SQLite format 3\x00"
        pool = fleet._cpool_instance
        assert pool is not None and 1 <= len(pool.processes()) <= pool.size
    try:
        import duckdb  # noqa: F401
    except ImportError:
        return
    assert {"duckdb", "duckdb-worker"} <= seen


async def test_stats_count_worker_processes_and_their_memory(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    """The stats panel reports this process and the worker processes
    separately; a DuckDB-worker member's process is counted while it is
    awake and gone once it has slept."""
    pytest.importorskip("duckdb")
    fleet, client = fleet_client
    s = await _stats(client)
    assert {"processes", "rss_mb", "footprint_mb"} <= set(s["workers"])
    assert s["process"]["rss_mb"] and s["total"]["rss_mb"] >= s["process"]["rss_mb"]
    member = next(m for m in fleet.members.values() if m.kind == "duckdb-worker")
    r = await client.get(f"/{member.name}/-/query", params={"sql": "select name from branch"})
    assert r.status_code == 200 and member.branch in r.text
    s = await _stats(client)
    assert s["workers"]["processes"] >= 1
    assert s["workers"]["rss_mb"] > 0
    await _wait_asleep(client)
    deadline = asyncio.get_running_loop().time() + 10
    while (await _stats(client))["workers"]["processes"]:
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.1)
    r = await client.get("/-/fleet")
    assert r.status_code == 200 and "worker processes" in r.text


async def test_cworker_members_unavailable_without_the_binary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a built C worker its members are listed as unavailable, the
    reason naming the build command, and the fleet still has every member."""
    monkeypatch.setenv("DBLAYER_CWORKER", str(tmp_path / "no-such-dblayer-cworker"))
    config = DemoConfig(data_dir=tmp_path, fleet_size=SIZE, fleet_max_open_connections=CAP,
                        fleet_max_threads=4, fleet_idle_timeout=0.5)
    app = create_app(config, specs=[])
    async with app.router.lifespan_context(app):
        fleet = app.state.fleet
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://demo") as client:
            cw = [m for m in fleet.members.values() if m.kind == "sqlite-cworker"]
            assert len(cw) == fleet.mix["sqlite-cworker"] > 0
            assert len(fleet.members) == SIZE
            for m in cw:
                assert m.state == "unavailable"
                assert m.demo.error and "make -C cworker" in m.demo.error, m.demo.error
            s = (await _stats(client))["fleet"]
            assert s["fleet_size"] == SIZE
            assert s["unavailable"] >= len(cw)
            assert s["registered"] + s["unavailable"] == SIZE
            r = await client.get("/-/fleet?backend=sqlite-cworker&state=unavailable")
            assert r.status_code == 200 and "make -C cworker" in r.text
            # The rest of the fleet is unaffected.
            r = await client.get("/sqlite-0001/-/query", params={"sql": "select name from branch"})
            assert r.status_code == 200


async def test_stats_count_c_worker_processes(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    """A C worker member's process is a worker process in the stats panel."""
    from dblayer.backends.sqlite_cworker import binary_unavailable
    from dblayer.backends.sqlite_worker import live_worker_pids

    reason = binary_unavailable()
    if reason:
        pytest.skip(reason)
    fleet, client = fleet_client
    await _wait_asleep(client)
    before = set(live_worker_pids())
    member = next(m for m in fleet.members.values() if m.kind == "sqlite-cworker")
    r = await client.get(f"/{member.name}/-/query", params={"sql": "select name from branch"})
    assert r.status_code == 200 and member.branch in r.text
    new = set(live_worker_pids()) - before
    assert new, "the C worker member started no worker process"
    s = await _stats(client)
    assert s["workers"]["processes"] >= len(new)
    assert s["workers"]["rss_mb"] > 0
    await _wait_asleep(client)
    deadline = asyncio.get_running_loop().time() + 10
    while set(live_worker_pids()) & new:
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.1)


async def test_auto_refresh_updates_in_place_without_reloading(
    fleet_client: tuple[Fleet, httpx.AsyncClient],
) -> None:
    """A full-page meta refresh made the grid flicker; with JavaScript the
    page patches itself, and the meta refresh is only a <noscript> fallback."""
    _, client = fleet_client
    r = await client.get("/-/fleet?refresh=1")
    assert r.status_code == 200
    outside_noscript = re.sub(r"<noscript>.*?</noscript>", "", r.text, flags=re.S)
    assert 'http-equiv="refresh"' not in outside_noscript
    assert '<noscript><meta http-equiv="refresh"' in r.text
    assert 'id="fleet-live"' in r.text and "setInterval(update" in r.text
    plain = await client.get("/-/fleet")
    assert "setInterval(update" not in plain.text and 'http-equiv="refresh"' not in plain.text
