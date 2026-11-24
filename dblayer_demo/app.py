"""The Starlette application: routes, lifespan and page handlers.

Every read goes through ``Database.execute`` (the read path: read-only
connections, a time limit, a row cap), so SQL typed into the query box can
never write: the library rejects it and the page shows the error.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import jinja2
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from dblayer import Database, DblayerError, NotSupported, QueryError, Results
from dblayer.core.params import find_placeholders, leading_keyword
from dblayer.core.schema import TableInfo

from .config import DemoConfig
from .fleet import Fleet
from .databases import DemoDatabase, DemoSpec, close_all, open_all
from .render import cell, describe_error, is_numeric

TEMPLATES = Jinja2Templates(
    env=jinja2.Environment(
        loader=jinja2.FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=True,
    )
)
TEMPLATES.env.filters["cell"] = cell
TEMPLATES.env.tests["numeric"] = is_numeric


COUNT_TIME_LIMIT_MS = 200
"""Row counts give up quickly: a huge table shows "many" instead."""
QUERY_TIME_LIMIT_MS = 1000
QUERY_MAX_ROWS = 1000
PAGE_SIZE = 100
WRITE_KEYWORDS = frozenset(
    "INSERT UPDATE DELETE REPLACE UPSERT MERGE CREATE DROP ALTER TRUNCATE ATTACH DETACH "
    "VACUUM REINDEX ANALYZE GRANT REVOKE COPY COMMENT CHECKPOINT INSTALL LOAD".split()
)
"""Statements that write: an error from one of these gets a read-only hint.
(``WITH ... DELETE`` is not spotted; the error itself still says why.)"""


def _limits() -> dict[str, int]:
    return {"time_limit_ms": QUERY_TIME_LIMIT_MS, "max_rows": QUERY_MAX_ROWS}


def _demos(request: Request) -> dict[str, DemoDatabase]:
    return request.app.state.demos


def _fleet(request: Request) -> Fleet | None:
    return request.app.state.fleet


def _database(request: Request) -> tuple[DemoDatabase, Database]:
    """A featured database or a fleet member, by URL name: every page
    works the same for both."""
    name = request.path_params["database"]
    demo = _demos(request).get(name)
    fleet = _fleet(request)
    if demo is None and fleet is not None and name in fleet.members:
        demo = fleet.members[name].demo
    if demo is None:
        raise HTTPException(404, "No such database")
    if demo.db is None:
        raise HTTPException(503, f"{demo.spec.title} is unavailable: {demo.error}")
    return demo, demo.db


async def count_rows(db: Database, name: str) -> int | None:
    """``count(*)`` under a short time limit; ``None`` if it ran out (or
    failed). There is no cheaper portable way through dblayer's API."""
    q = db.dialect.quote_identifier
    try:
        r = await db.execute(
            f"select count(*) from {q(name)}", time_limit_ms=COUNT_TIME_LIMIT_MS
        )
    except DblayerError:
        return None
    return int(r.rows[0][0])


async def homepage(request: Request) -> Response:
    rows: list[dict[str, Any]] = []
    for demo in _demos(request).values():
        row: dict[str, Any] = {"demo": demo, "tables": None, "views": None, "error": demo.error}
        if demo.db is not None:
            try:
                row["tables"] = len(await demo.db.table_names())
                row["views"] = len(await demo.db.view_names())
            except DblayerError as e:
                row["error"] = describe_error(e)
        rows.append(row)
    return TEMPLATES.TemplateResponse(request, "index.html", {"rows": rows})


async def database_page(request: Request) -> Response:
    demo, db = _database(request)
    schema = await db.schema()
    tables = []
    for name in schema.table_names():
        info = schema.tables[name]
        if info.hidden:
            continue
        tables.append({"info": info, "count": await count_rows(db, name)})
    views = [{"info": schema.views[name]} for name in schema.view_names()]
    return TEMPLATES.TemplateResponse(
        request,
        "database.html",
        {"demo": demo, "tables": tables, "views": views, "sql": "", **_limits()},
    )


def _order_by(db: Database, info: TableInfo) -> str:
    """A stable order for paging: the primary key, else the implicit row id
    (SQLite tables), else every column by position (views; PostgreSQL and
    DuckDB tables without a key)."""
    q = db.dialect.quote_identifier
    if info.primary_keys:
        return ", ".join(q(c) for c in info.primary_keys)
    rowid = db.dialect.rowid_column
    if info.kind == "table" and rowid and not info.virtual:
        return q(rowid)
    return ", ".join(str(i) for i in range(1, len(info.columns) + 1)) or "1"


def _offset(request: Request) -> int:
    try:
        return max(0, int(request.query_params.get("_offset", "0")))
    except ValueError:
        raise HTTPException(400, "_offset must be an integer") from None


async def table_page(request: Request) -> Response:
    demo, db = _database(request)
    name = request.path_params["table"]
    info = (await db.schema()).get(name)
    if info is None:
        raise HTTPException(404, f"No table or view named {name!r}")
    offset = _offset(request)
    q = db.dialect.quote_identifier
    # LIMIT one more than max_rows: ``truncated`` then says whether there is
    # a next page, with no second query.
    sql = (
        f"select * from {q(name)} order by {_order_by(db, info)} "
        f"limit {PAGE_SIZE + 1} offset {offset}"
    )
    results: Results | None = None
    error: str | None = None
    try:
        results = await db.execute(sql, time_limit_ms=QUERY_TIME_LIMIT_MS, max_rows=PAGE_SIZE)
    except DblayerError as e:
        error = describe_error(e, time_limit_ms=QUERY_TIME_LIMIT_MS)
    definition: str | None = None
    definition_note: str | None = None
    try:
        definition = await db.definition(name)
    except NotSupported:
        definition_note = "This backend cannot supply CREATE statements (definition())."
    except DblayerError as e:
        definition_note = describe_error(e)
    types = {c.name: c.type for c in info.columns}
    return TEMPLATES.TemplateResponse(
        request,
        "table.html",
        {
            "demo": demo,
            "info": info,
            "sql": sql,
            "results": results,
            "error": error,
            "types": types,
            "count": await count_rows(db, name),
            "offset": offset,
            "page_size": PAGE_SIZE,
            "next_offset": offset + PAGE_SIZE if results is not None and results.truncated else None,
            "prev_offset": max(0, offset - PAGE_SIZE) if offset > 0 else None,
            "definition": definition,
            "definition_note": definition_note,
        },
    )


def named_parameters(db: Database, sql: str) -> list[str]:
    """``:name`` placeholders in ``sql``, in order, without duplicates,
    found with dblayer's own scanner (so strings, comments and ``::casts``
    are skipped the way the engine would)."""
    names: list[str] = []
    for p in find_placeholders(sql, db.dialect.flavor):
        if p.kind == "named" and p.name and p.name not in names:
            names.append(p.name)
    return names


async def query_page(request: Request) -> Response:
    demo, db = _database(request)
    sql = request.query_params.get("sql", "").strip()
    params: dict[str, str] = {}
    results: Results | None = None
    error: str | None = None
    if sql:
        try:
            names = named_parameters(db, sql)
        except DblayerError as e:
            names = []
            error = describe_error(e)
        params = {n: request.query_params.get(n, "") for n in names}
        if error is None:
            try:
                results = await db.execute(
                    sql,
                    params or None,
                    time_limit_ms=QUERY_TIME_LIMIT_MS,
                    max_rows=QUERY_MAX_ROWS,
                )
            except DblayerError as e:
                error = describe_error(e, time_limit_ms=QUERY_TIME_LIMIT_MS)
                keyword = (leading_keyword(sql, db.dialect.flavor) or "").upper()
                if isinstance(e, QueryError) and keyword in WRITE_KEYWORDS:
                    error += (
                        f"\n\n{keyword} statements cannot run here: this page"
                        " uses the read path, Database.execute(), on read-only connections."
                    )
    return TEMPLATES.TemplateResponse(
        request,
        "query.html",
        {
            "demo": demo,
            "sql": sql,
            "params": params,
            "results": results,
            "error": error,
            **_limits(),
        },
        status_code=400 if error else 200,
    )


async def http_error(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, HTTPException)
    return TEMPLATES.TemplateResponse(
        request,
        "error.html",
        {"status": exc.status_code, "message": exc.detail},
        status_code=exc.status_code,
    )


def create_app(
    config: DemoConfig | Path | str = "demo-data", specs: list[DemoSpec] | None = None
) -> Starlette:
    """``config`` (or just a data directory); ``specs`` defaults to
    ``DEMO_SPECS`` (tests pass their own)."""
    if not isinstance(config, DemoConfig):
        config = DemoConfig(data_dir=Path(config))

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        demos = await open_all(config, specs)
        app.state.demos = {d.spec.slug: d for d in demos}
        fleet: Fleet | None = None
        if config.fleet_size > 0:
            fleet = Fleet(config)
            pg = app.state.demos.get("postgres")
            await fleet.start(
                postgres_error=pg.error if pg is not None else None,
                postgres_db=pg.db if pg is not None else None,
            )
        app.state.fleet = fleet
        try:
            yield
        finally:
            if fleet is not None:
                await fleet.close()
            await close_all(demos)

    routes = [
        Route("/", homepage, name="index"),
        Route("/{database}", database_page, name="database"),
        Route("/{database}/-/query", query_page, name="query"),
        Route("/{database}/{table:path}", table_page, name="table"),
    ]
    return Starlette(
        routes=routes, lifespan=lifespan, exception_handlers={HTTPException: http_error}
    )
