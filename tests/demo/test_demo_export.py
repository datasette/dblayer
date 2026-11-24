"""The demo's ``.csv``, ``.tsv``, ``.json`` and ``.ndjson`` endpoints, on
every demo database."""

from __future__ import annotations

import csv
import io
import json

import pytest

pytest.importorskip("starlette")
pytest.importorskip("httpx")
pytest.importorskip("jinja2")

import httpx  # noqa: E402

from .test_demo_app import client, data_dir  # noqa: E402,F401 (fixtures)


async def test_table_exports(client: httpx.AsyncClient) -> None:
    slug = client.slug  # type: ignore[attr-defined]
    r = await client.get(f"/{slug}/books.csv?_size=max")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert r.headers["x-dblayer-export"] in ("native", "fallback")
    rows = list(csv.reader(io.StringIO(r.text, newline="")))
    assert len(rows) == 201 and rows[1][0] == "1"
    r = await client.get(f"/{slug}/books.json?_size=5")
    doc = json.loads(r.content)
    assert len(doc["rows"]) == 5 and doc["truncated"] is True
    r = await client.get(f"/{slug}/books.json?_size=5&_shape=array")
    assert len(json.loads(r.content)) == 5
    r = await client.get(f"/{slug}/books.ndjson?_size=max")
    assert len(r.content.splitlines()) == 200
    r = await client.get(f"/{slug}/books.tsv?_size=3&_dl=1")
    assert r.headers["content-disposition"] == 'attachment; filename="books.tsv"'
    assert r.text.count("\r\n") == 4
    # native and fallback agree on the rows
    a = await client.get(f"/{slug}/books.json?_size=max")
    b = await client.get(f"/{slug}/books.json?_size=max&_native=0")
    assert b.headers["x-dblayer-export"] == "fallback"
    assert len(json.loads(a.content)["rows"]) == len(json.loads(b.content)["rows"]) == 200


async def test_query_exports(client: httpx.AsyncClient) -> None:
    slug = client.slug  # type: ignore[attr-defined]
    r = await client.get(f"/{slug}/-/query.csv", params={"sql": "select id from books where id < :n order by id", "n": "4"})
    assert r.status_code == 200
    assert r.text == "id\r\n1\r\n2\r\n3\r\n"
    r = await client.get(f"/{slug}/-/query.json", params={"sql": "select * from no_such_table"})
    assert r.status_code == 400
    r = await client.get(f"/{slug}/-/query.csv", params={"sql": "delete from books"})
    assert r.status_code == 400
    r = await client.get(f"/{slug}/-/query.xml", params={"sql": "select 1"})
    assert r.status_code == 404
    page = await client.get(f"/{slug}/books")
    assert f'/{slug}/books.csv?_size=max' in page.text
