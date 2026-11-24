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

from dblayer import Database, DblayerError

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


def _limits() -> dict[str, int]:
    return {"time_limit_ms": QUERY_TIME_LIMIT_MS, "max_rows": QUERY_MAX_ROWS}


def _demos(request: Request) -> dict[str, DemoDatabase]:
    return request.app.state.demos


def _database(request: Request) -> tuple[DemoDatabase, Database]:
    demo = _demos(request).get(request.path_params["database"])
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


async def http_error(request: Request, exc: Exception) -> Response:
    assert isinstance(exc, HTTPException)
    return TEMPLATES.TemplateResponse(
        request,
        "error.html",
        {"status": exc.status_code, "message": exc.detail},
        status_code=exc.status_code,
    )


def create_app(
    data_dir: Path | str = "demo-data", specs: list[DemoSpec] | None = None
) -> Starlette:
    """``specs`` defaults to ``DEMO_SPECS`` (tests pass their own)."""
    data_dir = Path(data_dir)

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        demos = await open_all(data_dir, specs)
        app.state.demos = {d.spec.slug: d for d in demos}
        try:
            yield
        finally:
            await close_all(demos)

    routes = [
        Route("/", homepage, name="index"),
        Route("/{database}", database_page, name="database"),
    ]
    return Starlette(
        routes=routes, lifespan=lifespan, exception_handlers={HTTPException: http_error}
    )
