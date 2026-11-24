"""The Starlette application: routes, lifespan and page handlers."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import jinja2
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from .databases import DemoSpec, close_all, open_all

TEMPLATES = Jinja2Templates(
    env=jinja2.Environment(
        loader=jinja2.FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=True,
    )
)


async def homepage(request: Request) -> Response:
    return TEMPLATES.TemplateResponse(request, "index.html", {"databases": []})


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
