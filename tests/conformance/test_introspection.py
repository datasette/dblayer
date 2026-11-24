"""Introspection methods, schema(), schema_version() and change_hint()."""

import pytest

from dblayer import TableNotFound
from dblayer.core.schema import ColumnInfo, ForeignKey, Index, Schema

from ..helpers import is_text_type


@pytest.fixture
async def blog(db):
    for sql in [
        "create table authors (id integer primary key, name text not null, "
        "country text default 'uk')",
        "create table posts (id integer primary key, author_id integer "
        "references authors(id), title text, body text)",
        "create table tags (post_id integer not null, tag text not null, "
        "primary key (tag, post_id), "
        "foreign key (post_id) references posts(id))",
        "create unique index idx_posts_title on posts (title)",
        "create index idx_posts_author_title on posts (author_id, title)",
        "create view post_titles as select id, title from posts",
    ]:
        await db.execute_write(sql)
    return db


async def test_table_and_view_names(blog):
    assert sorted(await blog.table_names()) == ["authors", "posts", "tags"]
    assert await blog.view_names() == ["post_titles"]


async def test_table_columns(blog):
    cols = await blog.table_columns("authors")
    assert all(isinstance(c, ColumnInfo) for c in cols)
    assert [c.name for c in cols] == ["id", "name", "country"]
    by_name = {c.name: c for c in cols}
    assert "int" in by_name["id"].type.lower()
    assert is_text_type(by_name["name"].type)
    assert by_name["name"].not_null is True
    assert by_name["country"].not_null is False
    assert by_name["country"].default is not None and "uk" in by_name["country"].default
    assert by_name["id"].primary_key == 1
    assert by_name["name"].primary_key == 0
    assert not any(c.hidden for c in cols)


async def test_view_columns(blog):
    assert [c.name for c in await blog.table_columns("post_titles")] == ["id", "title"]


async def test_primary_keys(blog):
    assert await blog.primary_keys("authors") == ["id"]
    assert await blog.primary_keys("tags") == ["tag", "post_id"]


async def test_no_primary_key(db):
    await db.execute_write("create table loose (a text, b text)")
    assert await db.primary_keys("loose") == []


async def test_foreign_keys(blog):
    fks = await blog.foreign_keys("posts")
    assert all(isinstance(f, ForeignKey) for f in fks)
    assert [(f.columns, f.other_table, f.other_columns) for f in fks] == [
        (("author_id",), "authors", ("id",))
    ]
    fks = await blog.foreign_keys("tags")
    assert [(f.columns, f.other_table, f.other_columns) for f in fks] == [
        (("post_id",), "posts", ("id",))
    ]
    assert await blog.foreign_keys("authors") == []


async def test_indexes(blog):
    idx = {i.name: i for i in await blog.indexes("posts")}
    assert all(isinstance(i, Index) for i in idx.values())
    assert idx["idx_posts_title"].columns == ("title",)
    assert idx["idx_posts_title"].unique is True
    assert idx["idx_posts_author_title"].columns == ("author_id", "title")
    assert idx["idx_posts_author_title"].unique is False


async def test_definition(blog):
    if not blog.capabilities.table_definition_sql:
        pytest.skip("no definition SQL")
    assert "create table" in (await blog.definition("authors")).lower()
    assert "create view" in (await blog.definition("post_titles")).lower()
    assert "create unique index" in (await blog.definition("idx_posts_title")).lower()
    with pytest.raises(TableNotFound):
        await blog.definition("nope")


@pytest.mark.parametrize("method", ["table_columns", "primary_keys", "foreign_keys", "indexes"])
async def test_unknown_table(blog, method):
    with pytest.raises(TableNotFound):
        await getattr(blog, method)("nope")


async def test_schema_equals_sum_of_methods(blog):
    schema = await blog.schema()
    assert isinstance(schema, Schema)
    assert sorted(schema.tables) == sorted(await blog.table_names())
    assert sorted(schema.views) == sorted(await blog.view_names())
    for name, info in schema.tables.items():
        assert list(info.columns) == await blog.table_columns(name)
        assert list(info.primary_keys) == await blog.primary_keys(name)
        assert list(info.foreign_keys) == await blog.foreign_keys(name)
        assert list(info.indexes) == await blog.indexes(name)
    assert schema.version == await blog.schema_version()


async def test_schema_version_changes_after_ddl_only(db):
    await db.execute_write("create table t (x integer)")
    v1 = await db.schema_version()
    await db.execute_write("insert into t (x) values (1)")
    assert await db.schema_version() == v1
    await db.execute_write("create table u (y integer)")
    v2 = await db.schema_version()
    assert v2 != v1
    await db.execute_write("alter table t add column z text")
    assert await db.schema_version() != v2


async def test_schema_cache_sees_own_writes(db):
    await db.execute_write("create table a (x integer)")
    assert await db.table_names() == ["a"]
    await db.execute_write("create table b (x integer)")
    assert sorted(await db.table_names()) == ["a", "b"]
    await db.execute_write("alter table a add column y text")
    assert [c.name for c in await db.table_columns("a")] == ["x", "y"]


async def test_schema_cache_sees_other_writers(make_backend):
    # Two Database objects on the same backend: changes through one are seen
    # by the other's cache (via change hint or version check).
    from dblayer import Database

    backend = make_backend()
    a = Database(backend)
    b = Database(backend)
    try:
        await a.execute_write("create table one (x integer)")
        assert await b.table_names() == ["one"]
        await a.execute_write("create table two (x integer)")
        assert sorted(await b.table_names()) == ["one", "two"]
    finally:
        await a.close()
        await b.close()


async def test_change_hint_does_not_open_a_connection(make_db):
    db = make_db()
    db.change_hint()
    assert db.stats().opened_total == 0


@pytest.mark.requires("cheap_change_hint")
async def test_change_hint_moves_after_ddl(db):
    await db.execute_write("create table t (x integer)")
    h1 = db.change_hint()
    assert h1 == db.change_hint()
    await db.execute_write("create table u (x integer)")
    assert db.change_hint() != h1


@pytest.mark.requires("cheap_change_hint")
async def test_change_hint_survives_sleep_without_waking(db):
    await db.execute_write("create table t (x integer)")
    await db.sleep()
    before = db.stats().opened_total
    db.change_hint()
    assert db.stats().opened_total == before
    assert db.stats().open_total == 0
