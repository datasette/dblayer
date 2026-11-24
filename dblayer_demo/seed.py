"""The demo dataset, written through dblayer's named operations.

A small imaginary library: ``authors``, ``books`` (a foreign key to authors,
a ``DATE``, NULLs, a blob ``cover`` and a long ``blurb``) and ``reviews`` (a
``TIMESTAMP``), with two indexes and a ``book_ratings`` view. The rows come
from a fixed random seed, so every backend gets the same data.
"""

from __future__ import annotations

import datetime
import random
from collections.abc import Iterator
from typing import Any

from dblayer import Database

__all__ = ["seed", "SEED_TABLES", "VIEW_NAME"]

N_AUTHORS = 40
N_BOOKS = 200
N_REVIEWS = 600
SEED_TABLES = ("authors", "books", "reviews")
VIEW_NAME = "book_ratings"

_FIRST = "Ada Basil Clara Dmitri Esme Farid Greta Hugo Ines Jun Kofi Lena Mateo Nadia Otto Priya Quentin Rosa Sven Tamsin".split()
_LAST = "Abara Brightwater Castellanos Dunmore Eriksen Fairweather Grünewald Hollis Ivanova Jaramillo Kowalczyk Lindqvist Moreau Nakamura Okafor".split()
_COUNTRIES = ["UK", "France", "Japan", "Nigeria", "Brazil", "Canada", "India", "Sweden", None]
_ADJ = "Silent Copper Hidden Last Winter Burning Paper Distant Glass Hollow Midnight Salt Velvet Wandering Quiet".split()
_NOUN = "Orchard Lighthouse Archive Harbour Cartographer Garden Clockmaker River Observatory Bridge Library Comet Atlas Kingdom Ferry".split()
_WORDS = (
    "a story of tides and letters, of a town that keeps its secrets in the walls; "
    "the narrator returns after twenty years to find the old house sold and the river "
    "rerouted, and begins, one notebook at a time, to piece together what happened on "
    "the night of the flood. part mystery, part love letter to small places"
).split()
_REVIEWERS = "bookworm42 marginalia night_reader pageturner quietshelf riverreads tea_and_tomes".split()
_COMMENTS = [
    "Loved it.",
    "Slow start, wonderful ending.",
    "Not for me.",
    "Read it in one sitting!",
    "The middle third drags.",
    None,
    None,
]


def _blurb(rng: random.Random) -> str:
    # Most blurbs are a paragraph; one in five is long enough to need
    # truncating in a table cell.
    n = rng.randrange(20, 60) if rng.random() > 0.2 else rng.randrange(250, 400)
    words = [rng.choice(_WORDS) for _ in range(n)]
    return " ".join(words).capitalize() + "."


def rows() -> dict[str, list[dict[str, Any]]]:
    rng = random.Random(20261123)
    authors = [
        {
            "id": i,
            "name": f"{rng.choice(_FIRST)} {rng.choice(_LAST)}",
            "born": datetime.date(1930, 1, 1) + datetime.timedelta(days=rng.randrange(0, 26000)),
            "country": rng.choice(_COUNTRIES),
        }
        for i in range(1, N_AUTHORS + 1)
    ]
    books = []
    for i in range(1, N_BOOKS + 1):
        books.append(
            {
                "id": i,
                "title": f"The {rng.choice(_ADJ)} {rng.choice(_NOUN)}",
                "author_id": rng.randrange(1, N_AUTHORS + 1),
                "published": datetime.date(1960, 1, 1)
                + datetime.timedelta(days=rng.randrange(0, 23000)),
                "pages": rng.randrange(90, 900) if rng.random() > 0.15 else None,
                "price": round(rng.uniform(4, 40), 2),
                "cover": bytes(rng.randrange(256) for _ in range(rng.randrange(8, 48)))
                if rng.random() > 0.6
                else None,
                "blurb": _blurb(rng),
            }
        )
    reviews = [
        {
            "id": i,
            "book_id": rng.randrange(1, N_BOOKS + 1),
            "reviewer": rng.choice(_REVIEWERS),
            "rating": rng.randrange(1, 6),
            "reviewed_at": datetime.datetime(2020, 1, 1)
            + datetime.timedelta(seconds=rng.randrange(0, 6 * 365 * 86400)),
            "comment": rng.choice(_COMMENTS),
        }
        for i in range(1, N_REVIEWS + 1)
    ]
    return {"authors": authors, "books": books, "reviews": reviews}


def _for_dialect(dialect_name: str, rows: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """SQLite has no date types: store ISO 8601 text, as is conventional
    (and as Python 3.12 deprecated sqlite3's implicit datetime adapters)."""
    if dialect_name != "sqlite":
        yield from rows
        return
    for row in rows:
        yield {
            k: v.isoformat(sep=" ") if isinstance(v, datetime.datetime)
            else v.isoformat() if isinstance(v, datetime.date)
            else v
            for k, v in row.items()
        }


async def seed(db: Database) -> bool:
    """Create and fill the demo tables unless they are already there.
    Returns ``True`` if anything was written. Idempotent: the view is
    created last, so its presence means a completed seed; otherwise every
    step is ``if_not_exists`` / ``ignore``."""
    if VIEW_NAME in await db.view_names():
        return False
    d = db.dialect
    await db.create_table(
        "authors",
        {"id": "integer", "name": "text", "born": "DATE", "country": "text"},
        pk="id",
        not_null=["name"],
        if_not_exists=True,
    )
    await db.create_table(
        "books",
        {
            "id": "integer",
            "title": "text",
            "author_id": "integer",
            "published": "DATE",
            "pages": "integer",
            "price": "float",
            "cover": "blob",
            "blurb": "text",
        },
        pk="id",
        not_null=["title"],
        foreign_keys=[("author_id", "authors", "id")] if db.capabilities.foreign_keys else [],
        if_not_exists=True,
    )
    await db.create_table(
        "reviews",
        {
            "id": "integer",
            "book_id": "integer",
            "reviewer": "text",
            "rating": "integer",
            "reviewed_at": "TIMESTAMP",
            "comment": "text",
        },
        pk="id",
        foreign_keys=[("book_id", "books", "id")] if db.capabilities.foreign_keys else [],
        if_not_exists=True,
    )
    data = rows()
    for table in SEED_TABLES:
        await db.insert_rows(table, _for_dialect(d.name, data[table]), pk="id", ignore=True)
    await db.create_index("books", ["author_id"], if_not_exists=True)
    await db.create_index("reviews", ["book_id", "rating"], if_not_exists=True)
    q = d.quote_identifier
    # No named operation creates a view; this is plain DDL on the write path.
    await db.execute_write(
        f"create view {q(VIEW_NAME)} as "
        "select b.id as id, b.title as title, a.name as author, count(r.id) as reviews, "
        "round(avg(r.rating), 2) as avg_rating "
        "from books b join authors a on a.id = b.author_id "
        "left join reviews r on r.book_id = b.id "
        "group by b.id, b.title, a.name"
    )
    return True
