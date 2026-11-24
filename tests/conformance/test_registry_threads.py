"""The registry owns the shared thread pool: databases added to it run their
blocking calls on it, so the thread count is fixed by ``max_threads``."""

import asyncio
import threading

import pytest

from dblayer import Database, Registry
from dblayer.backends.base import UsesExecutor

SLOW = (
    "with recursive c(x) as (select 1 union all select x + 1 from c where x < 100000) "
    "select count(*) from c"
)


def _registry_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name.startswith("dblayer-registry")]


@pytest.fixture
def executor_backend(make_backend):
    def make():
        b = make_backend()
        if not isinstance(b, UsesExecutor):
            pytest.skip("backend does not run on an executor")
        return b

    return make


async def test_added_databases_use_the_registry_executor(executor_backend):
    async with Registry(max_threads=2) as registry:
        db = registry.add("a", Database(executor_backend()))
        assert db._backend.executor is registry.executor
        await db.execute("select 1")
        await registry.remove("a")
        assert db._backend.executor is None


async def test_thread_count_is_fixed_by_max_threads(executor_backend):
    async with Registry(max_threads=3) as registry:
        dbs = [registry.add(f"d{i}", Database(executor_backend())) for i in range(30)]
        peak = 0

        async def run(d):
            nonlocal peak
            await d.execute(SLOW)
            peak = max(peak, len(_registry_threads()))

        await asyncio.gather(*(run(d) for d in dbs))
        assert 1 <= peak <= 3
        assert len(_registry_threads()) <= 3


async def test_explicit_executor_is_kept(executor_backend):
    from concurrent.futures import ThreadPoolExecutor

    own = ThreadPoolExecutor(max_workers=1)
    try:
        backend = executor_backend()
        backend.executor = own
        async with Registry() as registry:
            registry.add("a", Database(backend))
            assert backend.executor is own
            assert (await registry.peek("a").execute("select 1")).rows == [(1,)]
    finally:
        own.shutdown()


async def test_closing_the_registry_stops_its_threads(executor_backend):
    registry = Registry(max_threads=2)
    db = registry.add("a", Database(executor_backend()))
    await asyncio.gather(*(db.execute(SLOW) for _ in range(3)))
    executor = registry.executor
    await registry.close()
    assert not any(t.is_alive() for t in executor._threads)  # type: ignore[attr-defined]
