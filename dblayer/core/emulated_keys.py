"""Primary-key semantics for named operations on engines that do not
enforce keys (``capabilities.unique_constraints`` false: ClickHouse).

``insert_rows`` (plain, ``ignore``, ``replace``) and ``upsert_rows`` keep
the contract they have on every other backend by checking keys before they
write: read the batch's keys back, then raise, skip, or delete and
re-insert. Written against ``TxLike`` and the dialect only, like the rest
of ``operations.py``.

Guarantee, and what is lost: the core serialises a database's writers
through one write slot, so no other dblayer write runs between the check
and the write. Plain SQL ``INSERT`` from callers is not checked (the
engine accepts duplicates), and on an engine without transactions a
``replace`` / upsert is a delete followed by an insert, so a concurrent
reader can see the old rows gone and the new ones not yet there, and a
failure between the two loses the old rows.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ..errors import IntegrityError

if TYPE_CHECKING:
    from .operations import TxLike

__all__ = ["insert_keyed", "upsert_keyed", "check_no_overlap"]

_CHUNK = 500  # keys per lookup statement


def _key(row: Mapping[str, Any], pk: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row.get(c) for c in pk)


def _where_in(tx: "TxLike", pk: Sequence[str], n: int) -> str:
    d = tx.dialect
    if len(pk) == 1:
        return f"{d.quote_identifier(pk[0])} IN ({', '.join('?' for _ in range(n))})"
    one = "(" + ", ".join("?" for _ in pk) + ")"
    return f"({d.quote_identifiers(pk)}) IN ({', '.join(one for _ in range(n))})"


def _flat(keys: Sequence[tuple[Any, ...]]) -> list[Any]:
    return [v for k in keys for v in k]


async def _existing(
    tx: "TxLike", table: str, pk: Sequence[str], keys: Sequence[tuple[Any, ...]], columns: str
) -> list[tuple[Any, ...]]:
    """Rows (``columns``) of ``table`` whose key is in ``keys``."""
    d = tx.dialect
    out: list[tuple[Any, ...]] = []
    unique = list(dict.fromkeys(keys))
    for i in range(0, len(unique), _CHUNK):
        part = unique[i : i + _CHUNK]
        r = await tx.execute(
            f"SELECT {columns} FROM {d.quote_identifier(table)} WHERE {_where_in(tx, pk, len(part))}",
            _flat(part),
        )
        out.extend(tuple(row) for row in r.rows)
    return out


async def _delete_keys(
    tx: "TxLike", table: str, pk: Sequence[str], keys: Sequence[tuple[Any, ...]]
) -> None:
    d = tx.dialect
    unique = list(dict.fromkeys(keys))
    for i in range(0, len(unique), _CHUNK):
        part = unique[i : i + _CHUNK]
        await tx.execute(
            f"DELETE FROM {d.quote_identifier(table)} WHERE {_where_in(tx, pk, len(part))}",
            _flat(part),
        )


async def _insert(tx: "TxLike", table: str, rows: Sequence[Mapping[str, Any]]) -> int:
    from .operations import _groups

    total = 0
    for cols, group in _groups(rows):
        r = await tx.execute_many(
            tx.dialect.insert_sql(table, cols), [[row[c] for c in cols] for row in group]
        )
        total += max(r.rowcount, 0)
    return total


async def insert_keyed(
    tx: "TxLike",
    table: str,
    rows: Sequence[Mapping[str, Any]],
    pk: Sequence[str],
    *,
    ignore: bool,
    replace: bool,
) -> int | None:
    """``insert_rows`` with key checks. Returns the row count, or ``None``
    when the table has no key (nothing to check: the caller inserts as
    usual)."""
    if not pk:
        return None
    keys = [_key(r, pk) for r in rows]
    cols = tx.dialect.quote_identifiers(pk)
    existing = set(await _existing(tx, table, pk, keys, cols))
    if replace:
        # Whole-row replace, last row of a key wins (as ON CONFLICT DO
        # UPDATE row by row does); every row counts.
        last = {k: r for k, r in zip(keys, rows)}
        await _delete_keys(tx, table, pk, [k for k in last if k in existing])
        await _insert(tx, table, list(last.values()))
        return len(rows)
    seen: set[tuple[Any, ...]] = set()
    keep: list[Mapping[str, Any]] = []
    for k, r in zip(keys, rows):
        if k in existing or k in seen:
            if ignore:
                continue
            raise IntegrityError(
                f"UNIQUE constraint failed: {table}.{', '.join(pk)} (key {list(k)!r} "
                "already exists; checked by dblayer, the engine does not enforce keys)",
                "UNIQUE",
            )
        seen.add(k)
        keep.append(r)
    return await _insert(tx, table, keep)


async def upsert_keyed(
    tx: "TxLike", table: str, rows: Sequence[Mapping[str, Any]], pk: Sequence[str]
) -> int:
    """``upsert_rows``: rows whose key exists get the supplied columns
    merged over their current values; the merged rows replace the old
    ones (delete, then insert). Every row counts, as on the other
    backends."""
    from .operations import _table

    info = _table(await tx.schema(), table)
    names = [c.name for c in info.columns if not c.hidden and not c.generated]
    keys = [_key(r, pk) for r in rows]
    current = {
        _key(dict(zip(names, values)), pk): dict(zip(names, values))
        for values in await _existing(tx, table, pk, keys, tx.dialect.quote_identifiers(names))
    }
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    fresh: dict[tuple[Any, ...], dict[str, Any]] = {}
    for k, r in zip(keys, rows):
        if k in current or k in merged:
            base = merged.get(k) or dict(current[k])
            base.update(r)
            merged[k] = base
        elif k in fresh:
            fresh[k].update(r)
        else:
            fresh[k] = dict(r)
    await _delete_keys(tx, table, pk, list(merged))
    await _insert(tx, table, [*merged.values(), *fresh.values()])
    return len(rows)


async def check_no_overlap(tx: "TxLike", source: str, target: str, pk: Sequence[str]) -> None:
    """Raise ``IntegrityError`` if a key of ``source`` is already in
    ``target`` (staged-import ``append``, before its ``INSERT ... SELECT``)."""
    d = tx.dialect
    cols = d.quote_identifiers(pk)
    key = cols if len(pk) == 1 else f"({cols})"
    r = await tx.execute(
        f"SELECT {cols} FROM {d.quote_identifier(source)} WHERE {key} IN "
        f"(SELECT {cols} FROM {d.quote_identifier(target)}) LIMIT 1"
    )
    if r.rows:
        raise IntegrityError(
            f"UNIQUE constraint failed: {target}.{', '.join(pk)} (key {list(r.rows[0])!r} "
            "already exists; checked by dblayer, the engine does not enforce keys)",
            "UNIQUE",
        )
