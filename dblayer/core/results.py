"""Result types: ``Column``, ``Row``, ``Results`` and ``WriteResult``."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, overload

__all__ = ["Column", "Row", "Results", "WriteResult", "make_index"]


class Column(str):
    """A result column. It *is* its name (a ``str`` subclass), so
    ``results.columns == ("id", "title")`` holds, and it also carries the
    declared ``type`` when the backend knows it (``None`` otherwise)."""

    type: str | None

    def __new__(cls, name: str, type: str | None = None) -> "Column":
        obj = super().__new__(cls, name)
        obj.type = type
        return obj

    @property
    def name(self) -> str:
        return str.__str__(self)

    def __repr__(self) -> str:
        if self.type is None:
            return f"Column({self.name!r})"
        return f"Column({self.name!r}, {self.type!r})"

    def __reduce__(self) -> tuple[Any, ...]:
        return (Column, (self.name, self.type))


def make_index(columns: Iterable[str]) -> dict[str, int]:
    """Map column name to position; the first occurrence wins for duplicates."""
    index: dict[str, int] = {}
    for i, name in enumerate(columns):
        index.setdefault(str(name), i)
    return index


class Row(Sequence[Any]):
    """One row: tuple-style access by position and mapping-style by name.

    ``row[0]``, ``row["title"]``, ``tuple(row)``, ``dict(row)`` all work, and a
    row compares equal to a tuple with the same values.
    """

    __slots__ = ("_values", "_index")

    def __init__(self, values: Sequence[Any], index: Mapping[str, int]) -> None:
        self._values = tuple(values)
        self._index = index

    @overload
    def __getitem__(self, key: int) -> Any: ...
    @overload
    def __getitem__(self, key: str) -> Any: ...
    @overload
    def __getitem__(self, key: slice) -> tuple[Any, ...]: ...
    def __getitem__(self, key: int | str | slice) -> Any:
        if isinstance(key, str):
            return self._values[self._index[key]]
        return self._values[key]

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def keys(self) -> list[str]:
        return list(self._index)

    def get(self, key: str, default: Any = None) -> Any:
        i = self._index.get(key)
        return default if i is None else self._values[i]

    def as_dict(self) -> dict[str, Any]:
        return {name: self._values[i] for name, i in self._index.items()}

    def as_tuple(self) -> tuple[Any, ...]:
        return self._values

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Row):
            return self._values == other._values
        if isinstance(other, tuple):
            return self._values == other
        if isinstance(other, list):
            return list(self._values) == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._values)

    def _cmp_values(self, other: object) -> tuple[Any, ...] | None:
        if isinstance(other, Row):
            return other._values
        if isinstance(other, tuple):
            return other
        return None

    def __lt__(self, other: object) -> bool:
        o = self._cmp_values(other)
        return NotImplemented if o is None else self._values < o

    def __le__(self, other: object) -> bool:
        o = self._cmp_values(other)
        return NotImplemented if o is None else self._values <= o

    def __gt__(self, other: object) -> bool:
        o = self._cmp_values(other)
        return NotImplemented if o is None else self._values > o

    def __ge__(self, other: object) -> bool:
        o = self._cmp_values(other)
        return NotImplemented if o is None else self._values >= o

    def __repr__(self) -> str:
        return f"Row({self.as_dict()!r})"


def _rows_from_tuples(
    columns: Sequence[str], tuples: Iterable[Sequence[Any]]
) -> list[Row]:
    index = make_index(columns)
    return [Row(t, index) for t in tuples]


@dataclass(slots=True)
class Results:
    """The result of one statement.

    ``rowcount`` and ``last_pk`` are only meaningful for writes; reads leave
    them at ``-1`` and ``None``. They are here because the backend
    connection's single ``execute`` method serves both.
    """

    columns: tuple[Column, ...]
    rows: list[Row]
    truncated: bool = False
    rowcount: int = -1
    last_pk: Any = None

    @classmethod
    def from_tuples(
        cls,
        columns: Sequence[str | Column],
        tuples: Iterable[Sequence[Any]],
        *,
        truncated: bool = False,
        rowcount: int = -1,
        last_pk: Any = None,
    ) -> "Results":
        cols = tuple(c if isinstance(c, Column) else Column(c) for c in columns)
        return cls(
            columns=cols,
            rows=_rows_from_tuples(cols, tuples),
            truncated=truncated,
            rowcount=rowcount,
            last_pk=last_pk,
        )

    def __iter__(self) -> Iterator[Row]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def first(self) -> Row | None:
        return self.rows[0] if self.rows else None

    def dicts(self) -> list[dict[str, Any]]:
        return [r.as_dict() for r in self.rows]


@dataclass(slots=True)
class WriteResult:
    """The result of one write.

    ``last_pk`` is the backend's notion of the last inserted key; on SQLite it
    is ``cursor.lastrowid`` after an ``INSERT``/``REPLACE`` that changed rows
    (the rowid, which is the primary key for ``INTEGER PRIMARY KEY`` tables),
    otherwise ``None``. ``rows`` holds ``RETURNING`` rows, at most
    ``returning_limit`` of them; ``truncated`` says more were produced.
    """

    rowcount: int
    last_pk: Any = None
    columns: tuple[Column, ...] = ()
    rows: list[Row] = field(default_factory=list)
    truncated: bool = False
