"""What the two in-process SQLite backends share, whatever the driver:
the standard library ``sqlite3`` (``sqlite.py``) and apsw
(``sqlite_apsw.py``).

* ``SqliteDialectBase``: the SQL dialect. Subclasses say which SQLite
  version they run against (``native_alter`` depends on it) and how to read
  an error code and message out of their driver's exceptions; the mapping
  from SQLite error codes to dblayer exceptions (``map_sqlite_error``) is
  shared.
* ``read_schema``: the schema-reading SQL. It needs only ``execute(sql,
  params)`` returning something with ``fetchone``/``fetchall`` that can be
  iterated, which both drivers' connections provide.
* ``SqliteTarget``: path or named memory database, URIs, the change hint,
  creating an empty file on first read, and the hot-journal check.
* Small helpers: ``row_bytes``, ``stat``, ``make_capabilities``.
"""

from __future__ import annotations

import os
import urllib.parse
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ..core.params import SQLITE_FLAVOR, ParamStyle
from ..core.schema import ColumnInfo, ForeignKey, Index, Schema, TableInfo, Trigger
from ..errors import (
    DblayerError,
    IntegrityError,
    QueryError,
    QueryInterrupted,
    ResourceLimitExceeded,
    WriteConflict,
)
from .base import Capabilities, Dialect

FETCH_BATCH = 256
WRITE_VERBS = frozenset({"INSERT", "UPDATE", "DELETE", "REPLACE"})


def map_sqlite_error(code: str | None, message: str, integrity: bool) -> DblayerError:
    """dblayer exception for a SQLite error. ``code`` is the (extended)
    result code name, ``SQLITE_...``, or ``None`` when the driver raised the
    error itself; ``integrity`` is true for constraint violations."""
    if code == "SQLITE_INTERRUPT" or (code is None and message == "interrupted"):
        return QueryInterrupted("query interrupted (time limit exceeded or cancelled)")
    if integrity:
        return IntegrityError(message, code)
    if code in ("SQLITE_NOMEM", "SQLITE_TOOBIG", "SQLITE_FULL"):
        return ResourceLimitExceeded(message)
    if code == "SQLITE_BUSY_SNAPSHOT":
        return WriteConflict(message)
    return QueryError(message, code)


class SqliteDialectBase(Dialect):
    name = "sqlite"
    paramstyle = ParamStyle.NAMED_COLON
    flavor = SQLITE_FLAVOR
    type_map = {
        "integer": "INTEGER",
        "float": "REAL",
        "text": "TEXT",
        "blob": "BLOB",
    }

    # Conflict handling uses the generic ON CONFLICT syntax (SQLite 3.24+),
    # not INSERT OR IGNORE / OR REPLACE: OR IGNORE also skips NOT NULL and
    # CHECK violations, and OR REPLACE deletes the old row (new rowid,
    # delete side effects), neither of which PostgreSQL or DuckDB can match.

    # Native RENAME COLUMN (3.25+) also rewrites indexes, triggers, views and
    # other tables' foreign keys. DROP COLUMN (3.35+) refuses indexed, key and
    # referenced columns, and there is no ALTER COLUMN TYPE, so those (and pk
    # changes) rebuild the table. Subclasses set native_alter with
    # native_alter_for_version().
    rowid_column = "rowid"
    foreign_keys_enforced_sql = "PRAGMA foreign_keys"

    @staticmethod
    def native_alter_for_version(version: tuple[int, ...]) -> frozenset[str]:
        return frozenset({"rename"}) if version >= (3, 25, 0) else frozenset()

    def blob_literal(self, value: bytes) -> str:
        return "X'" + value.hex() + "'"

    def swap_rename_sql(self, staging: str, target: str) -> list[str]:
        # Since 3.26 RENAME re-parses the whole schema and fails ("error in
        # view v: no such table: main.target") while views still name the
        # just-dropped target. Legacy mode renames without that check (and
        # without rewriting references, which already say "target").
        # Assumes legacy_alter_table is normally off on dblayer connections.
        return [
            "PRAGMA legacy_alter_table = ON",
            self.rename_table_sql(staging, target),
            "PRAGMA legacy_alter_table = OFF",
        ]

    def error_info(self, exc: BaseException) -> tuple[str | None, str, bool] | None:
        """``(code, message, integrity)`` for a driver exception, or
        ``None`` when ``exc`` is not one. Overridden per driver."""
        return None

    def map_exception(self, exc: BaseException) -> DblayerError | None:
        info = self.error_info(exc)
        if info is None:
            return None
        return map_sqlite_error(*info)


def row_bytes(row: Sequence[Any]) -> int:
    """The estimate ``max_result_bytes`` is checked against."""
    size = 0
    for v in row:
        if v is None:
            size += 1
        elif isinstance(v, (str, bytes)):
            size += len(v)
        else:
            size += 8
    return size


def stat(path: str) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    return (st.st_size, st.st_mtime_ns)


def make_capabilities(
    *, is_memory: bool, fts: bool, version: tuple[int, ...], memory_limit: bool = False
) -> Capabilities:
    return Capabilities(
        concurrent_writers=False,
        transactional_ddl=True,
        implicit_rowid=True,
        cheap_change_hint=not is_memory,
        hard_kill=False,
        memory_limit=memory_limit,
        table_definition_sql=True,
        full_text_search=fts,
        sleep_releases_everything=not is_memory,
        returning=version >= (3, 35, 0),
        snapshot_reads=not is_memory,
    )


def has_fts(compile_options: Sequence[str]) -> bool:
    opts = set(compile_options)
    return "ENABLE_FTS5" in opts or "ENABLE_FTS4" in opts or "ENABLE_FTS3" in opts


class SqliteTarget:
    """The database a backend points at: a file, or a named shared-cache
    memory database. Mixed into each backend class (slotted)."""

    __slots__ = ("_path", "memory_name", "immutable", "create")

    _path: str | None
    memory_name: str | None
    immutable: bool
    create: bool

    def _init_target(
        self,
        path: str | os.PathLike[str] | None,
        memory_name: str | None,
        immutable: bool,
        create: bool,
    ) -> None:
        if (path is None) == (memory_name is None):
            raise ValueError("pass exactly one of path or memory_name")
        if memory_name is not None and immutable:
            raise ValueError("an in-memory database cannot be immutable")
        self._path = os.path.abspath(os.fspath(path)) if path is not None else None
        self.memory_name = memory_name
        self.immutable = immutable
        self.create = create and not immutable

    @staticmethod
    def unique_memory_name() -> str:
        return f"dblayer-{uuid.uuid4().hex}"

    @property
    def path(self) -> Path | None:
        return None if self._path is None else Path(self._path)

    @property
    def is_memory(self) -> bool:
        return self.memory_name is not None

    def _target_repr(self) -> str:
        return repr(f"memory:{self.memory_name}" if self.is_memory else self._path)

    def change_hint(self) -> object:
        """Size and mtime of the database file and its ``-wal`` file. Uses
        ``os.stat`` only; never opens a connection. ``None`` for memory
        databases (no cheap hint exists)."""
        if self._path is None:
            return None
        return (stat(self._path), stat(self._path + "-wal"))

    def _uri(self, write: bool) -> str:
        if self.memory_name is not None:
            return f"file:{urllib.parse.quote(self.memory_name)}?mode=memory&cache=shared"
        assert self._path is not None
        base = Path(self._path).as_uri()
        if write:
            return base + ("?mode=rwc" if self.create else "?mode=rw")
        if self.immutable:
            return base + "?immutable=1"
        return base + "?mode=ro"

    def _rw_uri(self) -> str:
        assert self._path is not None
        return Path(self._path).as_uri() + "?mode=rw"

    def _prepare_file(self, write: bool) -> bool:
        """Before opening a file connection: create an empty file for a
        read before the first write (an empty file is a valid empty
        database, so this beats failing with CANTOPEN). Returns whether a
        read connection should first try hot-journal recovery."""
        if self._path is None:
            return False
        if self.create and not write:
            try:
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError:
                pass
            else:
                os.close(fd)
        return not write and not self.immutable and self.has_journal()

    def has_journal(self) -> bool:
        return self._path is not None and os.path.exists(self._path + "-journal")


def read_schema(raw: Any, missing_pragma: type[BaseException] | tuple[type[BaseException], ...]) -> Schema:
    """Read the whole schema through ``raw.execute``. ``missing_pragma``:
    the driver's exception for "no such table" on ``pragma_table_list``
    (SQLite < 3.37)."""
    version = raw.execute("pragma schema_version").fetchone()[0]
    master = raw.execute(
        "select type, name, tbl_name, sql from sqlite_master order by rowid"
    ).fetchall()
    kinds: dict[str, str] = {}
    try:
        for _schema, name, ttype, *_rest in raw.execute(
            "select * from pragma_table_list where schema = 'main'"
        ):
            kinds[name] = ttype
    except missing_pragma:
        pass  # SQLite < 3.37
    index_sql = {name: sql for type_, name, _t, sql in master if type_ == "index"}
    tables: dict[str, TableInfo] = {}
    views: dict[str, TableInfo] = {}
    triggers: dict[str, Trigger] = {}

    pk_cache: dict[str, tuple[str, ...]] = {}

    def columns_of(name: str) -> tuple[ColumnInfo, ...]:
        cols = []
        for cid, cname, ctype, notnull, dflt, pk, hidden in raw.execute(
            "select cid, name, type, \"notnull\", dflt_value, pk, hidden "
            "from pragma_table_xinfo(?)",
            (name,),
        ):
            cols.append(
                ColumnInfo(
                    name=cname,
                    type=ctype or "",
                    not_null=bool(notnull),
                    default=dflt,
                    primary_key=pk,
                    hidden=hidden == 1,
                    generated=hidden in (2, 3),
                )
            )
        return tuple(cols)

    def pks_of(name: str) -> tuple[str, ...]:
        if name not in pk_cache:
            rows = raw.execute(
                "select name, pk from pragma_table_info(?) where pk > 0 order by pk", (name,)
            ).fetchall()
            pk_cache[name] = tuple(r[0] for r in rows)
        return pk_cache[name]

    for type_, name, tbl_name, sql in master:
        if type_ == "trigger":
            triggers[name] = Trigger(name=name, table=tbl_name, definition=sql)
            continue
        if type_ not in ("table", "view") or name.startswith("sqlite_"):
            continue
        columns = columns_of(name)
        if type_ == "view":
            views[name] = TableInfo(name=name, kind="view", columns=columns, definition=sql)
            continue
        fks: dict[int, list[tuple[Any, ...]]] = {}
        for fid, seq, other, frm, to, on_update, on_delete, _match in raw.execute(
            "select id, seq, \"table\", \"from\", \"to\", on_update, on_delete, match "
            "from pragma_foreign_key_list(?) order by id, seq",
            (name,),
        ):
            fks.setdefault(fid, []).append((other, frm, to, on_update, on_delete))
        foreign_keys = []
        for parts in fks.values():
            other = parts[0][0]
            to_cols = tuple(p[2] for p in parts)
            if any(c is None for c in to_cols):
                to_cols = pks_of(other) or to_cols
            foreign_keys.append(
                ForeignKey(
                    columns=tuple(p[1] for p in parts),
                    other_table=other,
                    other_columns=tuple(to_cols),
                    on_update=parts[0][3],
                    on_delete=parts[0][4],
                )
            )
        indexes = []
        for _seq, iname, unique, origin, partial in raw.execute(
            "select seq, name, \"unique\", origin, partial from pragma_index_list(?) "
            "order by seq",
            (name,),
        ).fetchall():
            icols = tuple(
                r[0]
                for r in raw.execute(
                    "select name from pragma_index_info(?) order by seqno", (iname,)
                )
            )
            indexes.append(
                Index(
                    name=iname,
                    columns=icols,
                    unique=bool(unique),
                    origin=origin,
                    partial=bool(partial),
                    definition=index_sql.get(iname),
                )
            )
        kind = kinds.get(name, "table")
        tables[name] = TableInfo(
            name=name,
            kind="table",
            columns=columns,
            foreign_keys=tuple(foreign_keys),
            indexes=tuple(sorted(indexes, key=lambda i: i.name)),
            definition=sql,
            virtual=kind == "virtual" or (sql or "").lstrip().upper().startswith("CREATE VIRTUAL"),
            hidden=kind == "shadow",
        )
    return Schema(version=version, tables=tables, views=views, triggers=triggers)

