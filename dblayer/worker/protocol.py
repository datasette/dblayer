"""Wire protocol between the parent (``dblayer.backends.sqlite_worker``) and
a worker process (``python -m dblayer.worker``).

Framing
-------
Each frame is a 4-byte big-endian unsigned length followed by that many
bytes of payload. Both ends refuse a frame whose length exceeds their frame
size cap (``max_frame_bytes``): the reader drops the connection, which on the
parent side means killing and replacing the worker.

Codecs
------
The first exchange (``hello``) is always JSON; it agrees the codec for every
later frame in both directions.

* ``json`` (standard library): ``bytes`` travel as ``{"$b": "<base64>"}``;
  integers stay exact (both ends are Python); non-finite floats use
  Python's ``NaN`` / ``Infinity`` extensions. A user dict whose only key is
  ``$b`` or ``$d`` (a row with a single column of that name) is escaped as
  ``{"$d": [key, value]}`` so it cannot be mistaken for a bytes value.
* ``msgpack`` (optional extra): ``bytes``, floats and NaN are native.
  Integers outside 64 bits (which SQLite cannot store anyway, but a caller
  may pass as a parameter so that SQLite raises its own error) travel as
  ext type 1 holding their decimal text.

Other value types travel through ``register_value_type`` (see
``ValueType``). The standard library types an engine such as DuckDB returns
are registered by default: ``datetime`` (naive, fixed-offset or ``ZoneInfo``
zone, kept), ``date``, ``time``, ``Decimal``, ``UUID`` and ``timedelta``,
plus two shapes neither codec keeps on its own: tuples (DuckDB fixed-size
arrays) and dicts with non-string keys (DuckDB maps; JSON would turn their
keys into strings). ``results_to_wire`` marks those two only in columns whose
declared type can hold them (``MAP``, ``STRUCT``, ``UNION``, arrays), so
plain results pay nothing.

Messages are dicts. Every request carries an ``id``; every reply repeats it.
See ``docs/worker-protocol.md`` for the message table, which
``tests/worker/test_protocol_doc.py`` checks against ``MESSAGE_TYPES`` here.
"""

from __future__ import annotations

import base64
import datetime as _dt
import decimal
import json
import re
import struct
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from ..core.results import Column, Results
from ..core.schema import ColumnInfo, ForeignKey, Index, Schema, TableInfo, Trigger

__all__ = [
    "PROTOCOL_VERSION",
    "HEADER",
    "DEFAULT_MAX_FRAME_BYTES",
    "MESSAGE_TYPES",
    "Codec",
    "JsonCodec",
    "MsgpackCodec",
    "CODECS",
    "available_codecs",
    "get_codec",
    "frame",
    "FrameTooLarge",
    "ValueType",
    "VALUE_TYPES",
    "register_value_type",
    "unregister_value_type",
    "FrameDecoder",
    "STANDARD_VALUE_TYPES",
    "WireMap",
    "WireTuple",
    "nested_columns",
    "wire_rows",
    "results_to_wire",
    "results_from_wire",
    "schema_to_wire",
    "schema_from_wire",
]

PROTOCOL_VERSION = 1
HEADER = struct.Struct(">I")
DEFAULT_MAX_FRAME_BYTES = 256 * 1024 * 1024

# Request types the worker understands (parent -> worker), and the
# notices it may send unprompted. Checked against the protocol doc.
MESSAGE_TYPES: dict[str, str] = {
    "hello": "request",
    "open": "request",
    "close": "request",
    "execute": "request",
    "execute_many": "request",
    "batch": "request",
    "stream": "request",
    "more": "control",
    "begin": "request",
    "commit": "request",
    "rollback": "request",
    "op": "request",
    "schema": "request",
    "schema_version": "request",
    "cancel": "control",
    "sleep": "request",
    "expired": "notice",
}


class FrameTooLarge(Exception):
    """A frame's declared length is over the cap."""

    def __init__(self, length: int, cap: int) -> None:
        super().__init__(f"frame of {length} bytes exceeds the {cap}-byte frame size cap")
        self.length = length
        self.cap = cap


class Codec(Protocol):
    name: str

    def encode(self, msg: Mapping[str, Any]) -> bytes: ...
    def decode(self, data: bytes | bytearray | memoryview) -> dict[str, Any]: ...


# -- extra value types ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ValueType:
    """A value type beyond the portable set (``None``, ``int``, ``float``,
    ``str``, ``bytes``, ``bool``) that should cross the wire intact: dates,
    decimals, UUIDs for a DuckDB or PostgreSQL worker.

    The extension point for both codecs: ``register_value_type`` makes JSON
    send ``{"$x": [tag, to_wire(value)]}`` and MessagePack an ext type
    ``ext_code`` holding ``to_wire(value)`` packed. ``to_wire`` must return
    portable data (a string is simplest); ``from_wire`` rebuilds the value.
    Both ends must register the same types: worker-side registration
    belongs in the engine module the worker imports. None are registered by
    default."""

    tag: str
    ext_code: int
    """MessagePack ext type code, 16 to 127 (1 is taken by big integers)."""
    cls: type
    to_wire: Callable[[Any], Any]
    from_wire: Callable[[Any], Any]


VALUE_TYPES: dict[str, ValueType] = {}
_BY_EXT: dict[int, ValueType] = {}
_BY_CLS: dict[type, ValueType | None] = {}


def register_value_type(vt: ValueType) -> None:
    """Register (or replace) a value type. Order matters for subclasses:
    the first registered type an object is an instance of wins, so register
    ``datetime`` before ``date``."""
    if not 16 <= vt.ext_code <= 127:
        raise ValueError("ext_code must be between 16 and 127")
    VALUE_TYPES[vt.tag] = vt
    _BY_EXT[vt.ext_code] = vt
    _BY_CLS.clear()


def unregister_value_type(tag: str) -> None:
    vt = VALUE_TYPES.pop(tag, None)
    if vt is not None:
        _BY_EXT.pop(vt.ext_code, None)
    _BY_CLS.clear()


def _value_type(o: Any) -> ValueType | None:
    cls = type(o)
    try:
        return _BY_CLS[cls]
    except KeyError:
        pass
    found = None
    for vt in VALUE_TYPES.values():
        if isinstance(o, vt.cls):
            found = vt
            break
    _BY_CLS[cls] = found
    return found


# -- standard value types (registered by default) ----------------------------


class WireMap:
    """A dict that neither codec would carry intact: one with a key that is
    not a string (JSON makes keys strings), or a single ``$b``/``$d``/``$x``
    key (JSON would read it as a codec wrapper). Sent as its item pairs;
    decoded back to a plain dict."""

    __slots__ = ("items",)

    def __init__(self, items: list[list[Any]]) -> None:
        self.items = items


class WireTuple:
    """A tuple inside a value (a DuckDB fixed-size array): both codecs
    would send it as a list. Decoded back to a tuple."""

    __slots__ = ("values",)

    def __init__(self, values: list[Any]) -> None:
        self.values = values


def _datetime_to(v: _dt.datetime) -> Any:
    key = getattr(v.tzinfo, "key", None)
    if key is not None:
        return [v.isoformat(), key]  # a ZoneInfo zone, kept by name
    return v.isoformat()


def _datetime_from(v: Any) -> _dt.datetime:
    if isinstance(v, list):
        from zoneinfo import ZoneInfo

        return _dt.datetime.fromisoformat(v[0]).astimezone(ZoneInfo(v[1]))
    return _dt.datetime.fromisoformat(v)


def _hashable(k: Any) -> Any:
    return tuple(_hashable(x) for x in k) if isinstance(k, list) else k


STANDARD_VALUE_TYPES: tuple[ValueType, ...] = (
    ValueType("datetime", 16, _dt.datetime, _datetime_to, _datetime_from),
    ValueType("date", 17, _dt.date, _dt.date.isoformat, _dt.date.fromisoformat),
    ValueType("time", 18, _dt.time, _dt.time.isoformat, _dt.time.fromisoformat),
    ValueType("decimal", 19, decimal.Decimal, str, decimal.Decimal),
    ValueType("uuid", 20, uuid.UUID, str, uuid.UUID),
    ValueType(
        "timedelta", 21, _dt.timedelta,
        lambda v: [v.days, v.seconds, v.microseconds],
        lambda v: _dt.timedelta(days=v[0], seconds=v[1], microseconds=v[2]),
    ),
    ValueType(
        "map", 22, WireMap,
        lambda v: v.items,
        lambda v: {_hashable(k): x for k, x in v},
    ),
    ValueType("tuple", 23, WireTuple, lambda v: v.values, tuple),
)
"""Registered when this module is imported, in both processes."""

for _vt in STANDARD_VALUE_TYPES:
    register_value_type(_vt)


def _mark(v: Any) -> Any:
    """``v`` with every dict and tuple that a codec would change wrapped
    in ``WireMap`` / ``WireTuple``, recursively."""
    t = type(v)
    if t is dict:
        items = [[k, _mark(x)] for k, x in v.items()]
        if any(type(k) is not str for k, _ in items) or (
            len(items) == 1 and items[0][0] in ("$b", "$d", "$x")
        ):
            return WireMap(items)
        return {k: x for k, x in items}
    if t is list:
        return [_mark(x) for x in v]
    if t is tuple:
        return WireTuple([_mark(x) for x in v])
    return v


_NESTED_TYPE = re.compile(r"MAP\(|STRUCT\(|UNION\(|\]")


def nested_columns(types: Sequence[str | None]) -> list[int]:
    """Indexes of the columns whose declared type can hold a map, a struct
    or an array (and so may need ``_mark``)."""
    return [i for i, t in enumerate(types) if t is not None and _NESTED_TYPE.search(t.upper())]


def wire_rows(rows: Iterable[Sequence[Any]], nested: Sequence[int]) -> list[Any]:
    """Rows ready to encode: as they are, except for the ``nested``
    columns, whose values are marked."""
    if not nested:
        return rows if isinstance(rows, list) else list(rows)
    out = []
    for row in rows:
        r = list(row)
        for i in nested:
            r[i] = _mark(r[i])
        out.append(r)
    return out


# -- JSON ------------------------------------------------------------------------


def _json_default(o: Any) -> Any:
    if isinstance(o, (bytes, bytearray, memoryview)):
        return {"$b": base64.b64encode(o).decode("ascii")}
    if isinstance(o, (tuple, set, frozenset)):
        return list(o)
    vt = _value_type(o)
    if vt is not None:
        return {"$x": [vt.tag, vt.to_wire(o)]}
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def _json_hook(d: dict[str, Any]) -> Any:
    if len(d) == 1:
        if "$b" in d:
            return base64.b64decode(d["$b"])
        if "$d" in d:
            k, v = d["$d"]
            return {k: v}
        if "$x" in d:
            tag, payload = d["$x"]
            return VALUE_TYPES[tag].from_wire(payload)
    return d


def _escape(o: Any) -> Any:
    """Escape user dicts that look like a codec wrapper. Only op arguments
    and params can contain user dicts, so only they are walked."""
    if isinstance(o, dict):
        out = {k: _escape(v) for k, v in o.items()}
        if len(out) == 1 and next(iter(out)) in ("$b", "$d", "$x"):
            ((k, v),) = out.items()
            return {"$d": [k, v]}
        return out
    if isinstance(o, list):
        return [_escape(v) for v in o]
    return o


class JsonCodec:
    name = "json"

    def __init__(self) -> None:
        self._enc = json.JSONEncoder(
            default=_json_default, allow_nan=True, separators=(",", ":"), ensure_ascii=False
        )
        self._dec = json.JSONDecoder(object_hook=_json_hook)

    def encode(self, msg: Mapping[str, Any]) -> bytes:
        if msg.get("type") == "op" or isinstance(msg.get("params"), dict):
            msg = dict(msg)
            for key in ("args", "params"):
                if key in msg:
                    msg[key] = _escape(msg[key])
        return self._enc.encode(msg).encode("utf-8", "surrogatepass")

    def decode(self, data: bytes | bytearray | memoryview) -> dict[str, Any]:
        return self._dec.decode(bytes(data).decode("utf-8", "surrogatepass"))


# -- MessagePack -----------------------------------------------------------------

_BIGINT_EXT = 1


class MsgpackCodec:
    name = "msgpack"

    def __init__(self) -> None:
        import msgpack

        self._msgpack = msgpack
        self._packer = msgpack.Packer(use_bin_type=True, default=self._default)

    @staticmethod
    def _default(o: Any) -> Any:
        if isinstance(o, (bytearray, memoryview)):
            return bytes(o)
        if isinstance(o, (set, frozenset)):
            return list(o)
        if isinstance(o, int) and not isinstance(o, bool):
            # Called by the packer for integers outside 64 bits.
            return MsgpackCodec._ext(o)
        vt = _value_type(o)
        if vt is not None:
            import msgpack

            # Packed with this same hook, so values nested inside (dates in
            # a map) are carried too.
            return msgpack.ExtType(
                vt.ext_code,
                msgpack.packb(vt.to_wire(o), use_bin_type=True, default=MsgpackCodec._default),
            )
        raise TypeError(f"cannot serialize {type(o).__name__}")

    @staticmethod
    def _ext(o: int) -> Any:
        import msgpack

        return msgpack.ExtType(_BIGINT_EXT, str(o).encode("ascii"))

    def _ext_hook(self, code: int, data: bytes) -> Any:
        if code == _BIGINT_EXT:
            return int(data.decode("ascii"))
        vt = _BY_EXT.get(code)
        if vt is not None:
            return vt.from_wire(
                self._msgpack.unpackb(data, raw=False, strict_map_key=False, ext_hook=self._ext_hook)
            )
        return self._msgpack.ExtType(code, data)

    def encode(self, msg: Mapping[str, Any]) -> bytes:
        return self._packer.pack(msg)

    def decode(self, data: bytes | bytearray | memoryview) -> dict[str, Any]:
        return self._msgpack.unpackb(
            data, raw=False, strict_map_key=False, ext_hook=self._ext_hook
        )


def available_codecs() -> list[str]:
    names = ["json"]
    try:
        import msgpack  # noqa: F401
    except ImportError:
        pass
    else:
        names.insert(0, "msgpack")
    return names


CODECS: dict[str, Callable[[], Codec]] = {"json": JsonCodec, "msgpack": MsgpackCodec}


def get_codec(name: str) -> Codec:
    try:
        factory = CODECS[name]
    except KeyError:
        raise ValueError(f"unknown codec {name!r}") from None
    return factory()


# -- framing -----------------------------------------------------------------------


def frame(payload: bytes, max_frame_bytes: int | None = None) -> bytes:
    """Length-prefix ``payload``. Raises ``FrameTooLarge`` over the cap."""
    n = len(payload)
    if max_frame_bytes is not None and n > max_frame_bytes:
        raise FrameTooLarge(n, max_frame_bytes)
    return HEADER.pack(n) + payload


class FrameDecoder:
    """Incremental decoder: ``feed()`` bytes as they arrive and get back the
    complete payloads. ``pending`` is the number of buffered bytes of an
    incomplete frame (non-zero at end of input means a truncated frame).
    Raises ``FrameTooLarge`` as soon as a header declares a frame over the
    cap, before buffering it."""

    __slots__ = ("_buf", "max_frame_bytes", "_need")

    def __init__(self, max_frame_bytes: int | None = DEFAULT_MAX_FRAME_BYTES) -> None:
        self._buf = bytearray()
        self.max_frame_bytes = max_frame_bytes
        self._need: int | None = None

    @property
    def pending(self) -> int:
        return len(self._buf)

    def feed(self, data: bytes) -> list[bytes]:
        buf = self._buf
        buf += data
        out: list[bytes] = []
        while True:
            if self._need is None:
                if len(buf) < 4:
                    break
                (n,) = HEADER.unpack_from(buf)
                if self.max_frame_bytes is not None and n > self.max_frame_bytes:
                    raise FrameTooLarge(n, self.max_frame_bytes)
                self._need = n
            n = self._need
            if len(buf) < 4 + n:
                break
            out.append(bytes(buf[4 : 4 + n]))
            del buf[: 4 + n]
            self._need = None
        return out


# -- result and schema shapes ---------------------------------------------------------


def results_to_wire(r: Results) -> dict[str, Any]:
    """Compact form of ``Results``: defaults are left out."""
    out: dict[str, Any] = {"columns": [str.__str__(c) for c in r.columns]}
    types = [c.type for c in r.columns]
    nested: list[int] = []
    if any(t is not None for t in types):
        out["types"] = types
        nested = nested_columns(types)
    if r.rows:
        out["rows"] = wire_rows((row.as_tuple() for row in r.rows), nested)
    if r.truncated:
        out["truncated"] = True
    if r.rowcount != -1:
        out["rowcount"] = r.rowcount
    if r.last_pk is not None:
        out["last_pk"] = r.last_pk
    return out


def results_from_wire(d: Mapping[str, Any]) -> Results:
    types = d.get("types") or ()
    return Results.from_tuples(
        [Column(c, types[i] if i < len(types) else None) for i, c in enumerate(d.get("columns", ()))],
        d.get("rows", ()),
        truncated=d.get("truncated", False),
        rowcount=d.get("rowcount", -1),
        last_pk=d.get("last_pk"),
    )


def _col_to(c: ColumnInfo) -> list[Any]:
    return [c.name, c.type, c.not_null, c.default, c.primary_key, c.hidden, c.generated]


def _col_from(v: list[Any]) -> ColumnInfo:
    return ColumnInfo(
        name=v[0], type=v[1], not_null=v[2], default=v[3],
        primary_key=v[4], hidden=v[5], generated=v[6],
    )


def _table_to(t: TableInfo) -> dict[str, Any]:
    return {
        "name": t.name,
        "kind": t.kind,
        "columns": [_col_to(c) for c in t.columns],
        "foreign_keys": [
            [list(f.columns), f.other_table, list(f.other_columns), f.on_update, f.on_delete]
            for f in t.foreign_keys
        ],
        "indexes": [
            [i.name, list(i.columns), i.unique, i.origin, i.partial, i.definition]
            for i in t.indexes
        ],
        "definition": t.definition,
        "virtual": t.virtual,
        "hidden": t.hidden,
        "checks": list(t.checks),
    }


def _table_from(d: Mapping[str, Any]) -> TableInfo:
    return TableInfo(
        name=d["name"],
        kind=d["kind"],
        columns=tuple(_col_from(c) for c in d["columns"]),
        foreign_keys=tuple(
            ForeignKey(
                columns=tuple(f[0]), other_table=f[1], other_columns=tuple(f[2]),
                on_update=f[3], on_delete=f[4],
            )
            for f in d["foreign_keys"]
        ),
        indexes=tuple(
            Index(
                name=i[0], columns=tuple(i[1]), unique=i[2], origin=i[3],
                partial=i[4], definition=i[5],
            )
            for i in d["indexes"]
        ),
        definition=d["definition"],
        virtual=d["virtual"],
        hidden=d["hidden"],
        checks=tuple(d["checks"]),
    )


def schema_to_wire(s: Schema) -> dict[str, Any]:
    return {
        "version": s.version,
        "tables": [_table_to(t) for t in s.tables.values()],
        "views": [_table_to(t) for t in s.views.values()],
        "triggers": [[t.name, t.table, t.definition] for t in s.triggers.values()],
    }


def schema_from_wire(d: Mapping[str, Any]) -> Schema:
    tables = {t["name"]: _table_from(t) for t in d["tables"]}
    views = {t["name"]: _table_from(t) for t in d["views"]}
    triggers = {t[0]: Trigger(name=t[0], table=t[1], definition=t[2]) for t in d["triggers"]}
    return Schema(version=d["version"], tables=tables, views=views, triggers=triggers)
