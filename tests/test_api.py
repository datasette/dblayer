"""Public API surface: exports, exception hierarchy, Row/Results types."""

import pickle
import threading

import pytest

import dblayer
from dblayer import Column, Database, Results, Row
from dblayer.backends import SqliteInProcess
from dblayer.backends.base import Backend, BackendConnection, Capabilities

SPEC_EXCEPTIONS = [
    "QueryError",
    "IntegrityError",
    "QueryInterrupted",
    "ResourceLimitExceeded",
    "TransactionExpired",
    "WriteConflict",
    "NestedWriteError",
    "WorkerCrashed",
    "NotSupported",
    "DatabaseClosed",
    "SchemaChanged",
]


@pytest.mark.parametrize("name", SPEC_EXCEPTIONS)
def test_every_spec_exception_is_exported(name):
    cls = getattr(dblayer, name)
    assert issubclass(cls, dblayer.DblayerError)
    assert name in dblayer.__all__


def test_exception_hierarchy():
    assert issubclass(dblayer.IntegrityError, dblayer.QueryError)
    assert issubclass(dblayer.ParameterError, dblayer.QueryError)
    assert issubclass(dblayer.TableNotFound, dblayer.QueryError)
    e = dblayer.QueryError("boom", code="X")
    assert (str(e), e.message, e.code) == ("boom", "boom", "X")


def test_exports():
    for name in ("Database", "Results", "WriteResult", "Row"):
        assert hasattr(dblayer, name)


def test_row_access():
    r = Results.from_tuples(["id", "title", "id"], [(1, "a", 9)])
    row = r.rows[0]
    assert row[0] == 1 and row["title"] == "a" and row[-1] == 9
    assert row["id"] == 1  # first occurrence wins
    assert row == (1, "a", 9)
    assert tuple(row) == (1, "a", 9)
    assert dict(Results.from_tuples(["a", "b"], [(1, 2)]).rows[0]) == {"a": 1, "b": 2}
    assert row.get("missing", "d") == "d"
    with pytest.raises(KeyError):
        row["missing"]
    assert len(row) == 3 and row[1:] == ("a", 9)


def test_columns_are_strings_with_types():
    c = Column("id", "INTEGER")
    assert c == "id" and c.name == "id" and c.type == "INTEGER"
    assert pickle.loads(pickle.dumps(c)).type == "INTEGER"
    assert Results.from_tuples(["a", "b"], []).columns == ("a", "b")


def test_sqlite_backend_satisfies_protocols(tmp_path):
    backend = SqliteInProcess(tmp_path / "x.db")
    assert isinstance(backend, Backend)
    assert isinstance(backend.capabilities, Capabilities)
    assert set(Capabilities.names()) >= {
        "concurrent_writers", "transactional_ddl", "implicit_rowid", "cheap_change_hint",
        "hard_kill", "memory_limit", "table_definition_sql", "full_text_search",
        "sleep_releases_everything",
    }


async def test_sqlite_connection_satisfies_protocol(tmp_path):
    backend = SqliteInProcess(tmp_path / "x.db")
    conn = await backend.open(write=True)
    try:
        assert isinstance(conn, BackendConnection)
    finally:
        await conn.close()


def test_creating_database_opens_nothing(tmp_path):
    path = tmp_path / "new.db"
    threads = threading.active_count()
    db = Database(SqliteInProcess(path), name="new")
    assert not path.exists()
    assert db.stats().open_total == 0
    assert threading.active_count() == threads
