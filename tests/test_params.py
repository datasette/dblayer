"""The placeholder tokenizer and rewriter. A mistake here silently corrupts
queries, so every target style is covered even before its backend exists."""

import pytest

from dblayer import ParameterError
from dblayer.core.params import (
    DUCKDB_FLAVOR,
    POSTGRES_FLAVOR,
    SQLITE_FLAVOR,
    ParamStyle,
    find_placeholders,
    leading_keyword,
    rewrite,
)

NC = ParamStyle.NAMED_COLON
QM = ParamStyle.QMARK
ND = ParamStyle.NUMERIC_DOLLAR
DN = ParamStyle.NAMED_DOLLAR


def names(sql, flavor=SQLITE_FLAVOR):
    return [(p.kind, p.name) for p in find_placeholders(sql, flavor)]


# -- basic rewriting, every style ------------------------------------------


@pytest.mark.parametrize(
    "style,expected_sql,expected_params",
    [
        (NC, "select * from t where a = :a and b = :b or c = :a", {"a": 1, "b": 2}),
        (QM, "select * from t where a = ? and b = ? or c = ?", [1, 2, 1]),
        (ND, "select * from t where a = $1 and b = $2 or c = $1", [1, 2]),
        (DN, "select * from t where a = $a and b = $b or c = $a", {"a": 1, "b": 2}),
    ],
)
def test_named_input(style, expected_sql, expected_params):
    sql = "select * from t where a = :a and b = :b or c = :a"
    assert rewrite(sql, {"a": 1, "b": 2, "unused": 3}, style) == (expected_sql, expected_params)


@pytest.mark.parametrize(
    "style,expected_sql",
    [
        (NC, "select ? , ?"),
        (QM, "select ? , ?"),
        (ND, "select $1 , $2"),
        (DN, "select ? , ?"),
    ],
)
def test_positional_input(style, expected_sql):
    assert rewrite("select ? , ?", (1, "x"), style) == (expected_sql, [1, "x"])


@pytest.mark.parametrize("style", list(ParamStyle))
def test_no_params(style):
    assert rewrite("select 1", None, style) == ("select 1", [])
    assert rewrite("select 1", {}, style)[0] == "select 1"
    assert rewrite("select 1", [], style)[0] == "select 1"


@pytest.mark.parametrize("style", list(ParamStyle))
def test_missing_named_parameter(style):
    with pytest.raises(ParameterError) as e:
        rewrite("select :a, :b", {"a": 1}, style)
    assert ":b" in str(e.value) and e.value.code == "MISSING_PARAMETER"


@pytest.mark.parametrize("style", list(ParamStyle))
def test_named_placeholder_without_params(style):
    with pytest.raises(ParameterError):
        rewrite("select :a", None, style)


@pytest.mark.parametrize("style", list(ParamStyle))
def test_wrong_positional_count(style):
    with pytest.raises(ParameterError) as e:
        rewrite("select ?, ?", [1], style)
    assert e.value.code == "WRONG_PARAMETER_COUNT"
    with pytest.raises(ParameterError):
        rewrite("select ?", [1, 2], style)


@pytest.mark.parametrize("style", list(ParamStyle))
def test_named_with_sequence_is_an_error(style):
    with pytest.raises(ParameterError) as e:
        rewrite("select :a, ?", [1, 2], style)
    assert e.value.code == "MIXED_PLACEHOLDERS"


@pytest.mark.parametrize("style", [NC, QM, DN])
def test_qmark_with_mapping_is_an_error_where_qmark_is_a_placeholder(style):
    with pytest.raises(ParameterError):
        rewrite("select :a, ?", {"a": 1}, style)


def test_postgres_qmark_with_mapping_is_left_alone():
    # ? is a jsonb operator in PostgreSQL
    sql = "select data ? 'k' from t where id = :id"
    assert rewrite(sql, {"id": 3}, ND) == ("select data ? 'k' from t where id = $1", [3])


@pytest.mark.parametrize("bad", ["abc", b"abc", 42, object()])
def test_params_must_be_mapping_or_sequence(bad):
    with pytest.raises(ParameterError):
        rewrite("select ?", bad, QM)


def test_numbered_qmark_not_supported():
    with pytest.raises(ParameterError):
        rewrite("select ?1", [1], QM)


def test_named_colon_leaves_sql_untouched():
    sql = "select :a /* :b */, ':c'"
    assert rewrite(sql, {"a": 1}, NC) == (sql, {"a": 1})


def test_names_with_digits_underscores_and_unicode():
    assert names("select :a1, :_b, :über_2") == [
        ("named", "a1"),
        ("named", "_b"),
        ("named", "über_2"),
    ]
    assert rewrite("select :a1, :_b", {"a1": 1, "_b": 2}, ND) == ("select $1, $2", [1, 2])


def test_placeholder_adjacent_to_punctuation():
    sql = "insert into t values(:a,:b);"
    assert rewrite(sql, {"a": 1, "b": 2}, ND) == ("insert into t values($1,$2);", [1, 2])
    assert rewrite("select (?)+?", [1, 2], ND) == ("select ($1)+$2", [1, 2])
    assert rewrite("select :a", {"a": 1}, ND) == ("select $1", [1])
    assert rewrite(":a", {"a": 1}, ND) == ("$1", [1])


# -- things that must be skipped -------------------------------------------


@pytest.mark.parametrize("style", list(ParamStyle))
@pytest.mark.parametrize(
    "literal",
    [
        "':x'",
        "'?'",
        "'it''s :x ?'",
        "':x'':y'",
        '":x"',
        '"col ? name"',
        '"a"":x"',
        "`:x`",
        "`a``:x`",
        "-- :x ?\n",
        "/* :x ? */",
        "/* multi\nline :x\n? */",
    ],
)
def test_skipped_everywhere(style, literal):
    flavor = {NC: SQLITE_FLAVOR, QM: SQLITE_FLAVOR, ND: POSTGRES_FLAVOR, DN: DUCKDB_FLAVOR}[style]
    sql = f"select {literal} :real"
    assert names(sql, flavor) == [("named", "real")]
    out, params = rewrite(sql, {"real": 7}, style)
    assert literal in out
    assert out.count(literal) == 1


def test_line_comment_at_end_without_newline():
    assert names("select :a -- :b") == [("named", "a")]


def test_unterminated_string_swallows_rest():
    assert names("select :a, 'oops :b") == [("named", "a")]
    assert names("select :a /* never closed :b") == [("named", "a")]


def test_square_brackets_sqlite_only():
    sql = "select [:x] from t where a = :a"
    assert names(sql, SQLITE_FLAVOR) == [("named", "a")]
    # PostgreSQL: [ is an array subscript, so a placeholder inside is real
    assert names("select arr[:i] from t", POSTGRES_FLAVOR) == [("named", "i")]
    assert rewrite("select arr[:i] from t", {"i": 1}, ND) == ("select arr[$1] from t", [1])


def test_double_colon_cast_is_not_a_placeholder():
    sql = "select :v::int, x::text, '1'::numeric(10,2) from t"
    assert names(sql, POSTGRES_FLAVOR) == [("named", "v")]
    assert rewrite(sql, {"v": "3"}, ND) == (
        "select $1::int, x::text, '1'::numeric(10,2) from t",
        ["3"],
    )
    assert rewrite("select a::b", {}, ND) == ("select a::b", [])


def test_array_slices_are_not_placeholders():
    assert names("select arr[1:2], arr[lo:hi], arr[:n]", POSTGRES_FLAVOR) == [("named", "n")]


def test_colon_equals_and_lonely_colon():
    assert names("select f(a := 1), ':' , x : y", POSTGRES_FLAVOR) == []


def test_dollar_quoted_strings():
    sql = "select $$ :x ? $$, $tag$ it's :y $tag$, :real"
    assert names(sql, POSTGRES_FLAVOR) == [("named", "real")]
    assert rewrite(sql, {"real": 1}, ND) == ("select $$ :x ? $$, $tag$ it's :y $tag$, $1", [1])
    assert names(sql, DUCKDB_FLAVOR) == [("named", "real")]


def test_dollar_tag_must_match():
    sql = "select $a$ :x $b$ :y $a$, :z"
    assert names(sql, POSTGRES_FLAVOR) == [("named", "z")]


def test_dollar_number_is_not_a_quote():
    # An existing $1 is left alone and does not open a quoted string.
    assert names("select $1, :a", POSTGRES_FLAVOR) == [("named", "a")]


def test_dollar_inside_identifier_is_not_a_quote():
    assert names("select foo$bar$ :a", POSTGRES_FLAVOR) == [("named", "a")]


def test_dollar_quotes_off_for_sqlite():
    # In SQLite $name is a (non-canonical) parameter; $$ is not a string.
    assert names("select $$ :a $$", SQLITE_FLAVOR) == [("named", "a")]


def test_nested_block_comments_postgres():
    sql = "select /* a /* b */ :x */ :y"
    assert names(sql, POSTGRES_FLAVOR) == [("named", "y")]
    # SQLite does not nest: the comment ends at the first */
    assert names(sql, SQLITE_FLAVOR) == [("named", "x"), ("named", "y")]


def test_escape_strings():
    sql = r"select E'it\'s :x', :y"
    assert names(sql, POSTGRES_FLAVOR) == [("named", "y")]
    # Without escape-string support the backslash is literal and the string
    # ends at the \' so :x looks real. SQLite has no E'' strings.
    assert ("named", "x") in names(sql, SQLITE_FLAVOR)


def test_escape_string_prefix_must_stand_alone():
    # "type'..." is not an E-string; the backslash is ordinary
    sql = r"select 'a\', :y"
    assert names(sql, POSTGRES_FLAVOR) == [("named", "y")]


def test_qmark_inside_strings_with_positional():
    sql = "select '?', \"?\", ? -- ?\n, ?"
    assert rewrite(sql, [1, 2], ND) == ("select '?', \"?\", $1 -- ?\n, $2", [1, 2])


def test_repeated_names_positional_styles():
    assert rewrite("select :a, :b, :a", {"a": 1, "b": 2}, QM) == ("select ?, ?, ?", [1, 2, 1])
    assert rewrite("select :a, :b, :a", {"a": 1, "b": 2}, ND) == ("select $1, $2, $1", [1, 2])


def test_cache_does_not_leak_params():
    sql = "select :a"
    assert rewrite(sql, {"a": 1}, ND)[1] == [1]
    assert rewrite(sql, {"a": 2}, ND)[1] == [2]


# -- leading_keyword ---------------------------------------------------------


@pytest.mark.parametrize(
    "sql,kw",
    [
        ("select 1", "SELECT"),
        ("  -- c\n /* x */ INSERT into t values (1)", "INSERT"),
        ("(select 1)", "SELECT"),
        ("with x as (select 1) insert into t select * from x", "INSERT"),
        ("with recursive c(n) as (select 1 union all select n+1 from c) select n from c", "SELECT"),
        ("with a as (select 1), b as (select 2) delete from t", "DELETE"),
        ("replace into t values (1)", "REPLACE"),
        ("create table t (x)", "CREATE"),
        ("", None),
        ("-- only a comment", None),
    ],
)
def test_leading_keyword(sql, kw):
    assert leading_keyword(sql) == kw
