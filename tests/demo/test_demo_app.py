"""The demo app (``dblayer_demo``), driven through httpx's ``ASGITransport``.

Needs the ``demo`` dependency group (``uv run --all-extras --group demo
pytest``); skipped otherwise. Each test runs once per demo database and
skips when that backend is unavailable here (worker backend not written
yet, driver not installed, PostgreSQL not reachable).
"""

from __future__ import annotations

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
from dblayer_demo.databases import (  # noqa: E402
    DEMO_SPECS,
    Unavailable,
    check_postgres_connection,
    check_postgres_dsn,
)

SLUGS = [s.slug for s in DEMO_SPECS]

# A query that runs far past the app's 1000 ms limit on each engine.
SLOW_SQL = {
    "sqlite": "with recursive c(x) as (select 1 union all select x + 1 from c) select count(*) from c",
    "postgres": "select pg_sleep(10)",
    "duckdb": "select sum(a.range * b.range) from range(1000000) a, range(1000000) b",
}
SLOW_SQL["sqlite-worker"] = SLOW_SQL["sqlite"]
SLOW_SQL["sqlite-cworker"] = SLOW_SQL["sqlite"]
SLOW_SQL["sqlite-cpool"] = SLOW_SQL["sqlite"]
SLOW_SQL["sqlite-wasm"] = SLOW_SQL["sqlite"]
SLOW_SQL["sqlite-apsw"] = SLOW_SQL["sqlite"]
SLOW_SQL["duckdb-worker"] = SLOW_SQL["duckdb"]
SLOW_SQL["duckdb-rsworker"] = SLOW_SQL["duckdb"]
SLOW_SQL["postgres-psycopg"] = SLOW_SQL["postgres"]


@pytest.fixture(scope="session")
def data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    # Shared by every test, so each file is seeded once per session.
    return tmp_path_factory.mktemp("demo-data")


async def _client(data_dir: Path, slugs: list[str]) -> AsyncIterator[tuple[Any, httpx.AsyncClient]]:
    config = DemoConfig(data_dir=data_dir, fleet_size=0)
    app = create_app(config, [s for s in DEMO_SPECS if s.slug in slugs])
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://demo") as client:
            yield app, client


@pytest.fixture
async def all_client(data_dir: Path) -> AsyncIterator[tuple[Any, httpx.AsyncClient]]:
    async for pair in _client(data_dir, SLUGS):
        yield pair


@pytest.fixture(params=SLUGS)
async def client(request: pytest.FixtureRequest, data_dir: Path) -> AsyncIterator[httpx.AsyncClient]:
    """A client for an app with just this one database, skipped if it is
    unavailable here."""
    slug: str = request.param
    async for app, c in _client(data_dir, [slug]):
        demo = app.state.demos[slug]
        if not demo.available:
            pytest.skip(f"{slug} unavailable: {demo.error}")
        c.slug = slug  # type: ignore[attr-defined]
        yield c


def _rows(html: str) -> list[list[str]]:
    """Cells of the first ``table.rows``'s body, tags stripped."""
    body = html.split("<tbody>", 1)[1].split("</tbody>", 1)[0]
    return [
        [re.sub(r"<[^>]+>", "", cell) for cell in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        for tr in re.findall(r"<tr>(.*?)</tr>", body, re.S)
    ]


async def test_homepage_lists_all_four_databases(all_client: tuple[Any, httpx.AsyncClient]) -> None:
    app, client = all_client
    r = await client.get("/")
    assert r.status_code == 200
    for title in [
        "SQLite, in process",
        "SQLite, in a worker process",
        "PostgreSQL",
        "DuckDB",
        "DuckDB, in a worker process",
    ]:
        assert title in r.text
    for slug, demo in app.state.demos.items():
        if demo.available:
            assert f'href="/{slug}"' in r.text
        else:
            # Listed, not linked, with the reason.
            assert f'href="/{slug}"' not in r.text
            assert "unavailable:" in r.text
            assert (await client.get(f"/{slug}")).status_code == 503
    # In-process SQLite needs nothing but the standard library.
    assert app.state.demos["sqlite"].available


async def test_unknown_database_is_404(all_client: tuple[Any, httpx.AsyncClient]) -> None:
    _, client = all_client
    r = await client.get("/no-such-db")
    assert r.status_code == 404
    assert "No such database" in r.text


async def test_database_page(client: httpx.AsyncClient) -> None:
    r = await client.get(f"/{client.slug}")  # type: ignore[attr-defined]
    assert r.status_code == 200
    for name in ["authors", "books", "reviews", "book_ratings"]:
        assert f'/{client.slug}/{name}"' in r.text  # type: ignore[attr-defined]
    assert "200 rows" in r.text and "600 rows" in r.text and "40 rows" in r.text
    assert '<textarea name="sql"' in r.text
    assert "blurb" in r.text  # column lists


async def test_table_page_and_pagination(client: httpx.AsyncClient) -> None:
    slug = client.slug  # type: ignore[attr-defined]
    r = await client.get(f"/{slug}/books")
    assert r.status_code == 200
    rows = _rows(r.text)
    assert len(rows) == 100
    assert [row[0] for row in rows[:3]] == ["1", "2", "3"]
    assert 'href="?_offset=100" rel="next"' in r.text
    assert 'rel="prev"' not in r.text
    assert "NULL" in r.text  # pages and cover have NULLs
    assert "&lt;blob:" in r.text
    assert "characters)" in r.text  # a long blurb, cut short
    assert "Foreign keys" in r.text and "Indexes" in r.text

    r2 = await client.get(f"/{slug}/books?_offset=100")
    rows2 = _rows(r2.text)
    assert [row[0] for row in rows2] == [str(i) for i in range(101, 201)]
    assert 'href="?_offset=0" rel="prev"' in r2.text
    assert 'rel="next"' not in r2.text

    # The view pages too.
    r3 = await client.get(f"/{slug}/book_ratings?_offset=150")
    assert r3.status_code == 200 and len(_rows(r3.text)) == 50

    assert (await client.get(f"/{slug}/no_such_table")).status_code == 404
    assert (await client.get(f"/{slug}/books?_offset=abc")).status_code == 400


async def test_table_definition(client: httpx.AsyncClient) -> None:
    r = await client.get(f"/{client.slug}/authors")  # type: ignore[attr-defined]
    if client.slug in ("postgres", "postgres-psycopg"):  # type: ignore[attr-defined]
        assert "cannot supply CREATE statements" in r.text
    else:
        assert re.search(r'<pre class="sql">CREATE TABLE (&#34;)?authors', r.text, re.I)


async def test_sql_query(client: httpx.AsyncClient) -> None:
    slug = client.slug  # type: ignore[attr-defined]
    r = await client.get(
        f"/{slug}/-/query",
        params={"sql": "select id, title from books where id <= :n order by id", "n": "3"},
    )
    assert r.status_code == 200
    assert [row[0] for row in _rows(r.text)] == ["1", "2", "3"]
    assert 'name="n" value="3"' in r.text
    assert "truncated" not in r.text


async def test_sql_escapes_html(client: httpx.AsyncClient) -> None:
    r = await client.get(
        f"/{client.slug}/-/query",  # type: ignore[attr-defined]
        params={"sql": "select '<script>alert(1)</script>' as x"},
    )
    assert r.status_code == 200
    assert "<script>alert" not in r.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in r.text


async def test_sql_truncated(client: httpx.AsyncClient) -> None:
    r = await client.get(
        f"/{client.slug}/-/query",  # type: ignore[attr-defined]
        params={"sql": "select r.id, a.id from reviews r, authors a"},
    )
    assert r.status_code == 200
    assert len(_rows(r.text)) == 1000
    assert "truncated: the query returned more than 1000 rows" in r.text


async def test_write_statement_rejected(client: httpx.AsyncClient) -> None:
    slug = client.slug  # type: ignore[attr-defined]
    for sql in ["delete from books", "drop table reviews", "insert into authors (id, name) values (999, 'x')"]:
        r = await client.get(f"/{slug}/-/query", params={"sql": sql})
        assert r.status_code == 400, sql
        assert 'class="error"' in r.text
        assert "cannot run here" in r.text
    r = await client.get(f"/{slug}/-/query", params={"sql": "select count(*) from books"})
    assert _rows(r.text) == [["200"]]
    assert "reviews" in (await client.get(f"/{slug}")).text


async def test_bad_sql_error(client: httpx.AsyncClient) -> None:
    slug = client.slug  # type: ignore[attr-defined]
    r = await client.get(f"/{slug}/-/query", params={"sql": "selec 1"})
    assert r.status_code == 400
    assert "SQL error" in r.text
    assert "cannot run here" not in r.text  # it is not a write
    r = await client.get(f"/{slug}/-/query", params={"sql": "select * from no_such_table"})
    assert r.status_code == 400 and "SQL error" in r.text


async def test_time_limit(client: httpx.AsyncClient) -> None:
    slug = client.slug  # type: ignore[attr-defined]
    r = await client.get(f"/{slug}/-/query", params={"sql": SLOW_SQL[slug]})
    assert r.status_code == 400
    assert "went past the time limit of 1000 ms" in r.text
    # The database is still usable afterwards.
    r = await client.get(f"/{slug}/-/query", params={"sql": "select 1 as one"})
    assert r.status_code == 200 and _rows(r.text) == [["1"]]


def test_postgres_refuses_other_databases() -> None:
    # Refused before any connection is attempted.
    for dsn in ["postgresql:///postgres?host=/tmp", "postgresql://u@h/prod", "postgresql://h/?dbname=x"]:
        with pytest.raises(Unavailable, match="must name the 'dblayer_test' database"):
            check_postgres_dsn(dsn)
    check_postgres_dsn("postgresql://u:p@h:5432/dblayer_test")


async def test_postgres_connection_failure_says_how_to_set_the_dsn(tmp_path: Path) -> None:
    pytest.importorskip("asyncpg")
    from dblayer import Database
    from dblayer_demo.databases import postgres_backend

    # No server listens on this socket directory: fails fast, never
    # reaching any real server.
    db = Database(postgres_backend(f"postgresql:///dblayer_test?host={tmp_path}", "dblayer_demo", "t"))
    try:
        with pytest.raises(Unavailable, match="--postgres-dsn or \\$DBLAYER_TEST_POSTGRES"):
            await check_postgres_connection(db, "dblayer_demo")
    finally:
        await db.close()


def test_postgres_dsn_option(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DBLAYER_TEST_POSTGRES", "postgresql://env@h/dblayer_test")
    assert DemoConfig().postgres_dsn == "postgresql://env@h/dblayer_test"
    assert DemoConfig(postgres_dsn="postgresql://cli@h/dblayer_test").postgres_dsn.startswith(
        "postgresql://cli@"
    )
