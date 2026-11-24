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
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from dblayer import DblayerError

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


def _demos(request: Request) -> dict[str, DemoDatabase]:
    return request.app.state.demos


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

    return Starlette(routes=[Route("/", homepage, name="index")], lifespan=lifespan)
