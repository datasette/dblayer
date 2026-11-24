"""Placeholder tokenizer and rewriter.

Callers write SQL with canonical placeholders:

* ``:name`` with a mapping of parameters, or
* ``?`` with a sequence of parameters.

``rewrite()`` turns that into a backend's native placeholder style. The SQL is
otherwise passed through byte for byte. Placeholder recognition is done by a
small hand-written scanner (not a regex over the whole statement) that skips:

* string literals ``'...'`` (with ``''`` escapes, and Postgres/DuckDB
  ``E'...'`` backslash escapes when the flavor enables them),
* quoted identifiers ``"..."``, `````...````` and (SQLite flavor) ``[...]``,
* ``-- line`` and ``/* block */`` comments (nested block comments in the
  Postgres flavor),
* dollar-quoted strings ``$$...$$`` / ``$tag$...$tag$`` (Postgres, DuckDB),
* Postgres ``::type`` casts.

Rules for a ``:name`` placeholder: the ``:`` must not be part of ``::``, the
name must start with a letter or underscore, and the character before the
``:`` must not be an identifier character. That last rule keeps array slices
such as ``arr[1:2]`` and ``arr[lo:hi]`` intact.

With a mapping, ``?`` characters are left alone when the target style does not
use ``?`` as a placeholder (Postgres, where ``?`` is a jsonb operator);
otherwise a stray ``?`` with a mapping is an error, as is any ``:name`` with a
sequence. ``?NNN`` numbered placeholders are not supported. SQLite's own
``@name`` / ``$name`` forms are not recognised and pass through unchanged.

Results are cached per (sql, style, flavor, mode), so repeated statements cost
a dictionary lookup plus binding.
"""

from __future__ import annotations

import enum
import functools
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..errors import ParameterError

__all__ = [
    "ParamStyle",
    "SqlFlavor",
    "SQLITE_FLAVOR",
    "POSTGRES_FLAVOR",
    "DUCKDB_FLAVOR",
    "CLICKHOUSE_FLAVOR",
    "Params",
    "Placeholder",
    "find_placeholders",
    "rewrite",
    "leading_keyword",
    "dollar_parameters",
    "statement_count",
]

type Params = Mapping[str, Any] | Sequence[Any] | None


class ParamStyle(enum.Enum):
    """Target placeholder styles.

    ============== ======================== ===================
    style          mapping input becomes    sequence input
    ============== ======================== ===================
    NAMED_COLON    ``:name`` + dict         ``?`` + list
    QMARK          ``?`` + list             ``?`` + list
    NUMERIC_DOLLAR ``$1`` (deduped) + list  ``$1..$n`` + list
    NAMED_DOLLAR   ``$name`` + dict         ``?`` + list
    ============== ======================== ===================

    NAMED_COLON is sqlite3's native form, NUMERIC_DOLLAR is asyncpg's and
    NAMED_DOLLAR is DuckDB's.
    """

    NAMED_COLON = "named_colon"
    QMARK = "qmark"
    NUMERIC_DOLLAR = "numeric_dollar"
    NAMED_DOLLAR = "named_dollar"

    @property
    def qmark_is_placeholder(self) -> bool:
        return self is not ParamStyle.NUMERIC_DOLLAR


@dataclass(frozen=True, slots=True)
class SqlFlavor:
    """Lexical rules that differ between engines."""

    name: str
    bracket_identifiers: bool = False
    backtick_identifiers: bool = True
    dollar_quotes: bool = False
    nested_block_comments: bool = False
    escape_strings: bool = False
    backslash_strings: bool = False
    """Every ``'...'`` string, and every ``"..."`` and ```...``` quoted
    name, takes backslash escapes (ClickHouse)."""
    hash_comments: bool = False
    """``#`` starts a comment to the end of the line (ClickHouse: ``# `` and
    ``#!``; any other ``#`` is a syntax error there, so treating every ``#``
    as a comment never hides text the engine would run)."""


SQLITE_FLAVOR = SqlFlavor("sqlite", bracket_identifiers=True)
POSTGRES_FLAVOR = SqlFlavor(
    "postgres", dollar_quotes=True, nested_block_comments=True, escape_strings=True
)
# Checked against DuckDB 2.0.1 (scripts/duckdb_verify.py): $$/$tag$ quotes,
# E'' strings, nested /* */ comments and `backtick` identifiers; [ ] is a
# list literal or slice, never an identifier quote.
DUCKDB_FLAVOR = SqlFlavor(
    "duckdb", dollar_quotes=True, escape_strings=True, nested_block_comments=True
)

# Checked against ClickHouse 26.11 (scripts/clickhouse_verify.py): backslash
# escapes in every '...' string, nested /* */ comments, $$heredoc$$ and
# `backtick` identifiers; [ ] is an array literal.
CLICKHOUSE_FLAVOR = SqlFlavor(
    "clickhouse",
    dollar_quotes=True,
    nested_block_comments=True,
    backslash_strings=True,
    hash_comments=True,
)

_DEFAULT_FLAVOR = {
    ParamStyle.NAMED_COLON: SQLITE_FLAVOR,
    ParamStyle.QMARK: SQLITE_FLAVOR,
    ParamStyle.NUMERIC_DOLLAR: POSTGRES_FLAVOR,
    ParamStyle.NAMED_DOLLAR: DUCKDB_FLAVOR,
}


@dataclass(frozen=True, slots=True)
class Placeholder:
    kind: str
    """``"named"``, ``"qmark"`` or ``"numbered"`` (``?NNN``)."""
    start: int
    end: int
    name: str | None = None


# Characters at which the scanner has to look closely. Everything else is
# skipped in bulk. This is only a "jump to the next interesting character"
# helper; the grammar itself is handled by the scanner below.
_INTERESTING = re.compile(r"""['"`\[\-/$:?()#]|[A-Za-z_]""")
_INTERESTING_NO_WORDS = re.compile(r"""['"`\[\-/$:?#]""")
_INTERESTING_SEMICOLONS = re.compile(r"""['"`\[\-/$:?();#]|[A-Za-z_]""")


def _is_ident_char(ch: str) -> bool:
    return ch.isalnum() or ch == "_" or ch == "$"


def _is_name_start(ch: str) -> bool:
    return ch.isalpha() or ch == "_"


def _is_name_char(ch: str) -> bool:
    return ch.isalnum() or ch == "_"


def _scan(
    sql: str,
    flavor: SqlFlavor,
    words: bool,
    dollars: bool = False,
    semicolons: bool = False,
) -> Iterator[tuple[str, int, int]]:
    """Yield significant tokens as ``(kind, start, end)``.

    kinds: ``named``, ``qmark``, ``numbered``; when ``words`` is true,
    ``word``, ``(`` and ``)``; when ``dollars`` is true, ``dollar`` (a
    ``$1`` parameter); when ``semicolons`` is true (with ``words``), ``;``. Strings, comments and quoted identifiers are
    consumed silently.
    """
    n = len(sql)
    i = 0
    jump = (
        _INTERESTING_SEMICOLONS
        if semicolons
        else (_INTERESTING if words else _INTERESTING_NO_WORDS)
    )
    while i < n:
        m = jump.search(sql, i)
        if m is None:
            return
        i = m.start()
        c = sql[i]
        if c == "'":
            backslash = flavor.backslash_strings or (
                flavor.escape_strings
                and i > 0
                and sql[i - 1] in "eE"
                and (i < 2 or not _is_ident_char(sql[i - 2]))
            )
            i = _skip_string(sql, i, backslash)
        elif c == '"':
            i = _skip_string(sql, i, True, '"') if flavor.backslash_strings else _skip_quoted(sql, i, '"')
        elif c == "`":
            if not flavor.backtick_identifiers:
                i += 1
            elif flavor.backslash_strings:
                i = _skip_string(sql, i, True, "`")
            else:
                i = _skip_quoted(sql, i, "`")
        elif c == "#":
            if flavor.hash_comments:
                end = sql.find("\n", i + 1)
                i = n if end == -1 else end + 1
            else:
                i += 1
        elif c == "[":
            if flavor.bracket_identifiers:
                end = sql.find("]", i + 1)
                i = n if end == -1 else end + 1
            else:
                i += 1
        elif c == "-":
            if i + 1 < n and sql[i + 1] == "-":
                end = sql.find("\n", i + 2)
                i = n if end == -1 else end + 1
            else:
                i += 1
        elif c == "/":
            if i + 1 < n and sql[i + 1] == "*":
                i = _skip_block_comment(sql, i, flavor.nested_block_comments)
            else:
                i += 1
        elif c == "$":
            if (
                dollars
                and i + 1 < n
                and sql[i + 1].isdigit()
                and (i == 0 or not _is_ident_char(sql[i - 1]))
            ):
                j = i + 1
                while j < n and sql[j].isdigit():
                    j += 1
                yield ("dollar", i, j)
                i = j
            else:
                i = _skip_dollar(sql, i, flavor)
        elif c == ":":
            if i + 1 < n and sql[i + 1] == ":":
                i += 2  # ::type cast
                continue
            if (
                i + 1 < n
                and _is_name_start(sql[i + 1])
                and (i == 0 or not _is_ident_char(sql[i - 1]))
            ):
                j = i + 2
                while j < n and _is_name_char(sql[j]):
                    j += 1
                yield ("named", i, j)
                i = j
            else:
                i += 1
        elif c == "?":
            j = i + 1
            while j < n and sql[j].isdigit():
                j += 1
            yield ("numbered" if j > i + 1 else "qmark", i, j)
            i = j
        elif c == "(" or c == ")" or c == ";":
            yield (c, i, i + 1)
            i += 1
        else:
            # Start of a word (only reachable when words=True). Words that
            # touch a preceding identifier char were already consumed.
            j = i + 1
            while j < n and _is_ident_char(sql[j]):
                j += 1
            if i > 0 and _is_ident_char(sql[i - 1]):
                i = j  # middle of e.g. "1abc"; not a word start
                continue
            yield ("word", i, j)
            i = j


def _skip_string(sql: str, i: int, backslash: bool, q: str = "'") -> int:
    n = len(sql)
    j = i + 1
    while j < n:
        ch = sql[j]
        if backslash and ch == "\\":
            j += 2
            continue
        if ch == q:
            if j + 1 < n and sql[j + 1] == q:
                j += 2
                continue
            return j + 1
        j += 1
    return n


def _skip_quoted(sql: str, i: int, q: str) -> int:
    n = len(sql)
    j = i + 1
    while True:
        end = sql.find(q, j)
        if end == -1:
            return n
        if end + 1 < n and sql[end + 1] == q:
            j = end + 2
            continue
        return end + 1


def _skip_block_comment(sql: str, i: int, nested: bool) -> int:
    n = len(sql)
    if not nested:
        end = sql.find("*/", i + 2)
        return n if end == -1 else end + 2
    depth = 1
    j = i + 2
    while j < n:
        if sql.startswith("/*", j):
            depth += 1
            j += 2
        elif sql.startswith("*/", j):
            depth -= 1
            j += 2
            if depth == 0:
                return j
        else:
            j += 1
    return n


def _skip_dollar(sql: str, i: int, flavor: SqlFlavor) -> int:
    n = len(sql)
    if not flavor.dollar_quotes or (i > 0 and _is_ident_char(sql[i - 1])):
        return i + 1
    j = i + 1
    if j < n and sql[j].isdigit():
        return i + 1  # $1 style parameter, not a dollar quote
    while j < n and _is_name_char(sql[j]):
        j += 1
    if j < n and sql[j] == "$":
        tag = sql[i : j + 1]
        end = sql.find(tag, j + 1)
        return n if end == -1 else end + len(tag)
    return i + 1


@functools.lru_cache(maxsize=2048)
def _placeholders(sql: str, flavor: SqlFlavor) -> tuple[Placeholder, ...]:
    out = []
    for kind, start, end in _scan(sql, flavor, words=False):
        name = sql[start + 1 : end] if kind == "named" else None
        out.append(Placeholder(kind, start, end, name))
    return tuple(out)


def find_placeholders(sql: str, flavor: SqlFlavor = SQLITE_FLAVOR) -> list[Placeholder]:
    """Every placeholder-like token outside strings, comments and quoted
    identifiers, in order."""
    return list(_placeholders(sql, flavor))


@dataclass(frozen=True, slots=True)
class _Template:
    sql: str
    names: tuple[str, ...] | None
    """For named input: the names to bind, in output order (list output) or
    the set to copy (dict output)."""
    dict_out: bool
    qmark_count: int


@functools.lru_cache(maxsize=2048)
def _template(sql: str, style: ParamStyle, flavor: SqlFlavor, mode: str) -> _Template:
    phs = _placeholders(sql, flavor)
    named = [p for p in phs if p.kind == "named"]
    qmarks = [p for p in phs if p.kind == "qmark"]
    numbered = [p for p in phs if p.kind == "numbered"]

    if mode == "none":
        if named:
            raise ParameterError(
                f"SQL has named placeholder :{named[0].name} but no parameters "
                "were supplied",
                code="MISSING_PARAMETER",
            )
        return _Template(sql, None, False, 0)

    if mode == "named":
        if style.qmark_is_placeholder and (qmarks or numbered):
            raise ParameterError(
                "SQL mixes ? placeholders with named parameters",
                code="MIXED_PLACEHOLDERS",
            )
        if style is ParamStyle.NAMED_COLON:
            # sqlite3 understands :name natively; leave the SQL untouched.
            names = tuple(dict.fromkeys(p.name for p in named))  # type: ignore[misc]
            return _Template(sql, names, True, 0)
        parts: list[str] = []
        pos = 0
        order: list[str] = []
        numbers: dict[str, int] = {}
        for p in named:
            assert p.name is not None
            parts.append(sql[pos : p.start])
            if style is ParamStyle.QMARK:
                parts.append("?")
                order.append(p.name)
            elif style is ParamStyle.NUMERIC_DOLLAR:
                if p.name not in numbers:
                    numbers[p.name] = len(numbers) + 1
                    order.append(p.name)
                parts.append(f"${numbers[p.name]}")
            else:  # NAMED_DOLLAR
                parts.append("$" + p.name)
                if p.name not in order:
                    # DuckDB matches $name case-insensitively, so :a and :A
                    # would silently bind the same value.
                    clash = [o for o in order if o.casefold() == p.name.casefold()]
                    if clash:
                        raise ParameterError(
                            f"parameters :{clash[0]} and :{p.name} differ only in case, "
                            "which this backend cannot tell apart",
                            code="AMBIGUOUS_PARAMETER",
                        )
                    order.append(p.name)
            pos = p.end
        parts.append(sql[pos:])
        return _Template(
            "".join(parts), tuple(order), style is ParamStyle.NAMED_DOLLAR, 0
        )

    # positional
    if named:
        raise ParameterError(
            f"SQL has named placeholder :{named[0].name} but positional "
            "parameters were supplied",
            code="MIXED_PLACEHOLDERS",
        )
    if numbered:
        raise ParameterError(
            "numbered ?NNN placeholders are not supported", code="UNSUPPORTED_PLACEHOLDER"
        )
    if style is not ParamStyle.NUMERIC_DOLLAR:
        return _Template(sql, None, False, len(qmarks))
    parts = []
    pos = 0
    for k, p in enumerate(qmarks, start=1):
        parts.append(sql[pos : p.start])
        parts.append(f"${k}")
        pos = p.end
    parts.append(sql[pos:])
    return _Template("".join(parts), None, False, len(qmarks))


def rewrite(
    sql: str,
    params: Params,
    style: ParamStyle,
    flavor: SqlFlavor | None = None,
) -> tuple[str, dict[str, Any] | list[Any]]:
    """Rewrite canonical placeholders in ``sql`` to ``style``.

    Returns ``(native_sql, native_params)``; ``native_params`` is a dict for
    dict-taking styles and a list otherwise. Raises ``ParameterError`` for
    missing names, wrong counts or mixed styles.
    """
    if flavor is None:
        flavor = _DEFAULT_FLAVOR[style]
    if params is None:
        mode = "none"
    elif isinstance(params, Mapping):
        mode = "named"
    elif isinstance(params, (str, bytes, bytearray, memoryview)):
        raise ParameterError(
            f"parameters must be a mapping or a sequence, not {type(params).__name__}",
            code="BAD_PARAMETERS",
        )
    elif isinstance(params, Sequence):
        mode = "positional"
    else:
        raise ParameterError(
            f"parameters must be a mapping or a sequence, not {type(params).__name__}",
            code="BAD_PARAMETERS",
        )

    t = _template(sql, style, flavor, mode)

    if mode == "none":
        return t.sql, []
    if mode == "named":
        assert isinstance(params, Mapping) and t.names is not None
        missing = [n for n in t.names if n not in params]
        if missing:
            raise ParameterError(
                "missing value for parameter " + ", ".join(f":{n}" for n in missing),
                code="MISSING_PARAMETER",
            )
        if t.dict_out:
            return t.sql, {n: params[n] for n in t.names}
        return t.sql, [params[n] for n in t.names]
    assert isinstance(params, Sequence)
    if len(params) != t.qmark_count:
        raise ParameterError(
            f"SQL has {t.qmark_count} ? placeholder(s) but {len(params)} "
            "parameter(s) were supplied",
            code="WRONG_PARAMETER_COUNT",
        )
    return t.sql, list(params)


@functools.lru_cache(maxsize=2048)
def dollar_parameters(
    sql: str, flavor: SqlFlavor = POSTGRES_FLAVOR
) -> tuple[tuple[int, int, int], ...]:
    """``(start, end, number)`` of every ``$n`` parameter outside strings,
    comments, quoted identifiers and dollar quotes."""
    return tuple(
        (start, end, int(sql[start + 1 : end]))
        for kind, start, end in _scan(sql, flavor, words=False, dollars=True)
        if kind == "dollar"
    )


@functools.lru_cache(maxsize=2048)
def statement_count(sql: str, flavor: SqlFlavor = SQLITE_FLAVOR) -> int:
    """How many statements ``sql`` holds: ``;``-separated parts with any
    code in them, ignoring strings, comments and quoted identifiers
    (``"select 1;"`` is one, ``"select 1; select 2"`` two, ``""`` none)."""
    count = 0
    code = False
    for kind, _start, _end in _scan(sql, flavor, words=True, semicolons=True):
        if kind == ";":
            count += code
            code = False
        else:
            code = True
    return count + code


_STATEMENT_WORDS = frozenset(
    {"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "VALUES", "MERGE"}
)


@functools.lru_cache(maxsize=2048)
def leading_keyword(sql: str, flavor: SqlFlavor = SQLITE_FLAVOR) -> str | None:
    """The statement's verb, upper-cased, ignoring comments and parentheses.

    For ``WITH ...`` the verb after the common table expressions is returned
    (``WITH x AS (...) INSERT ...`` gives ``"INSERT"``). Returns ``None`` for
    an empty statement.
    """
    depth = 0
    first: str | None = None
    for kind, start, end in _scan(sql, flavor, words=True):
        if kind == "(":
            depth += 1
        elif kind == ")":
            depth = max(0, depth - 1)
        elif kind == "word":
            word = sql[start:end].upper()
            if first is None:
                if depth > 0 and word not in _STATEMENT_WORDS and word != "WITH":
                    continue
                first = word
                if first != "WITH":
                    return first
            elif depth == 0 and word in _STATEMENT_WORDS:
                return word
    return first
