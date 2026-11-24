"""Experiments that check the spec's DuckDB assumptions against the installed
engine. Run with ``uv run --extra duckdb python scripts/duckdb_verify.py``.

Each experiment prints one ``name: result`` line. Observed results on DuckDB
2.0.1 (macOS arm64, CPython 3.15) are recorded in ``OBSERVED`` at the bottom
of this file; re-running the script prints the live result next to them.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

import duckdb

RESULTS: dict[str, Any] = {}


def experiment(fn: Callable[[Path], Any]) -> Callable[[Path], Any]:
    def run(tmp: Path) -> Any:
        try:
            r = fn(tmp)
        except Exception as e:  # record, don't stop
            r = f"EXCEPTION {type(e).__name__}: {e}"
        RESULTS[fn.__name__] = r
        print(f"{fn.__name__}: {r}")
        print(f"    observed on 2.0.1: {OBSERVED.get(fn.__name__)}")
        return r

    run.__name__ = fn.__name__
    EXPERIMENTS.append(run)
    return run


EXPERIMENTS: list[Callable[[Path], Any]] = []


def err(f: Callable[[], Any]) -> str:
    try:
        r = f()
        return f"ok {r!r}" if r is not None else "ok"
    except Exception as e:
        return f"{type(e).__name__}: {str(e).splitlines()[0][:140]}"


@experiment
def mixed_modes_one_process(tmp: Path) -> Any:
    p = str(tmp / "mixed.duckdb")
    rw = duckdb.connect(p)
    rw.execute("create table t(a int)")
    out = {"ro_while_rw_open": err(lambda: duckdb.connect(p, read_only=True).close())}
    rw.close()
    ro = duckdb.connect(p, read_only=True)
    out["rw_while_ro_open"] = err(lambda: duckdb.connect(p).close())
    out["second_ro"] = err(lambda: duckdb.connect(p, read_only=True).close())
    ro.close()
    rw = duckdb.connect(p)
    out["second_rw_same_process"] = err(lambda: duckdb.connect(p).execute("select 1").fetchall())
    rw.close()
    return out


@experiment
def other_process_while_open(tmp: Path) -> Any:
    p = str(tmp / "proc.duckdb")
    con = duckdb.connect(p)
    con.execute("create table t(a int)")
    code = (
        "import duckdb,sys\n"
        "try:\n"
        f"  duckdb.connect({p!r}, read_only=sys.argv[1]=='ro').close(); print('ok')\n"
        "except Exception as e: print(type(e).__name__, str(e).splitlines()[0][:100])\n"
    )
    out = {}
    for mode in ("rw", "ro"):
        r = subprocess.run([sys.executable, "-c", code, mode], capture_output=True, text=True)
        out[f"subprocess_{mode}_while_open"] = r.stdout.strip() or r.stderr.strip()[-200:]
    con.close()
    r = subprocess.run([sys.executable, "-c", code, "rw"], capture_output=True, text=True)
    out["subprocess_rw_after_close"] = r.stdout.strip()
    # cursors keep the instance alive?
    con = duckdb.connect(p)
    cur = con.cursor()
    con.close()
    out["cursor_after_parent_close"] = err(lambda: cur.execute("select 1").fetchall())
    r = subprocess.run([sys.executable, "-c", code, "rw"], capture_output=True, text=True)
    out["subprocess_rw_after_parent_close_cursor_open"] = r.stdout.strip()
    con = duckdb.connect(p)
    cur = con.cursor()
    cur.close()
    r = subprocess.run([sys.executable, "-c", code, "rw"], capture_output=True, text=True)
    out["subprocess_rw_cursor_closed_parent_open"] = r.stdout.strip()
    con.close()
    return out


@experiment
def write_conflicts(tmp: Path) -> Any:
    con = duckdb.connect(str(tmp / "conflict.duckdb"))
    con.execute("create table t(id int primary key, v int)")
    con.execute("insert into t values (1, 0), (2, 0)")
    a, b = con.cursor(), con.cursor()
    out = {}
    # same row update
    a.execute("begin"); b.execute("begin")
    a.execute("update t set v = 1 where id = 1")
    out["second_update_same_row"] = err(lambda: b.execute("update t set v = 2 where id = 1"))
    a.execute("commit")
    out["b_commit_after_failure"] = err(lambda: b.execute("commit"))
    err(lambda: b.execute("rollback"))
    # different rows
    a.execute("begin"); b.execute("begin")
    a.execute("update t set v = 3 where id = 1")
    out["update_different_rows"] = err(lambda: b.execute("update t set v = 3 where id = 2"))
    out["commit_a"] = err(lambda: a.execute("commit"))
    out["commit_b"] = err(lambda: b.execute("commit"))
    # concurrent appends
    a.execute("begin"); b.execute("begin")
    a.execute("insert into t values (10, 0)")
    out["append_concurrent"] = err(lambda: b.execute("insert into t values (11, 0)"))
    a.execute("commit"); out["append_commit_b"] = err(lambda: b.execute("commit"))
    # same primary key insert
    a.execute("begin"); b.execute("begin")
    a.execute("insert into t values (20, 0)")
    out["insert_same_pk"] = err(lambda: b.execute("insert into t values (20, 1)"))
    a.execute("commit"); out["insert_same_pk_commit_b"] = err(lambda: b.execute("commit"))
    err(lambda: b.execute("rollback"))
    # update a row another tx updated & committed after b's snapshot
    b.execute("begin")
    b.execute("select * from t").fetchall()
    a.execute("update t set v = 9 where id = 1")  # autocommit
    out["update_row_changed_after_snapshot"] = err(lambda: b.execute("update t set v = 8 where id = 1"))
    err(lambda: b.execute("rollback"))
    # snapshot isolation of reads
    b.execute("begin")
    before = b.execute("select v from t where id=2").fetchone()
    a.execute("update t set v = 77 where id = 2")
    after = b.execute("select v from t where id=2").fetchone()
    b.execute("commit")
    out["snapshot_reads"] = f"before={before} after_other_commit={after}"
    # concurrent DDL
    a.execute("begin"); b.execute("begin")
    a.execute("create table x1(a int)")
    out["concurrent_create_other_table"] = err(lambda: b.execute("create table x2(a int)"))
    out["ddl_commit_a"] = err(lambda: a.execute("commit"))
    out["ddl_commit_b"] = err(lambda: b.execute("commit"))
    a.execute("begin"); b.execute("begin")
    a.execute("alter table t add column c1 int")
    out["concurrent_alter_and_insert"] = err(lambda: b.execute("insert into t values (30, 0)"))
    out["alter_commit_a"] = err(lambda: a.execute("commit"))
    out["alter_commit_b"] = err(lambda: b.execute("commit"))
    out["exception_classes"] = _conflict_class(con)
    con.close()
    return out


def _conflict_class(con: duckdb.DuckDBPyConnection) -> str:
    a, b = con.cursor(), con.cursor()
    a.execute("begin"); b.execute("begin")
    a.execute("update t set v = 100 where id = 1")
    try:
        b.execute("update t set v = 101 where id = 1")
    except Exception as e:
        a.execute("rollback")
        b.execute("rollback")
        return " > ".join(c.__name__ for c in type(e).__mro__[:-2])
    return "no conflict"


@experiment
def transactional_ddl(tmp: Path) -> Any:
    con = duckdb.connect(str(tmp / "ddl.duckdb"))
    con.execute("create table keep(a int)")
    con.execute("begin")
    con.execute("create table t(a int)")
    con.execute("insert into t values (1)")
    con.execute("alter table keep add column b int")
    con.execute("create index i on keep(a)")
    con.execute("create view v as select 1")
    con.execute("rollback")
    tables = [r[0] for r in con.execute("select table_name from duckdb_tables()").fetchall()]
    cols = [r[0] for r in con.execute("select column_name from duckdb_columns() where table_name='keep'").fetchall()]
    idx = con.execute("select count(*) from duckdb_indexes()").fetchone()
    views = con.execute("select count(*) from duckdb_views() where not internal").fetchone()
    con.close()
    return f"after rollback tables={tables} keep_columns={cols} indexes={idx[0]} views={views[0]}"


@experiment
def interrupt_latency(tmp: Path) -> Any:
    con = duckdb.connect(config={"threads": 2})
    cur = con.cursor()
    sql = "select count(*) from range(1000000000) a, range(100) b where (a.range * b.range) % 7 = 3"
    out = {}
    for delay in (0.05, 0.2):
        t0 = time.perf_counter()
        timer = threading.Timer(delay, cur.interrupt)
        timer.start()
        try:
            cur.execute(sql).fetchall()
            out[f"delay_{delay}"] = "finished"
        except Exception as e:
            lat = (time.perf_counter() - t0 - delay) * 1000
            out[f"delay_{delay}"] = f"{type(e).__name__} after +{lat:.1f}ms"
    # interrupt on idle cursor, then a query: is the interrupt remembered?
    cur.interrupt()
    out["interrupt_idle_then_query"] = err(lambda: cur.execute("select 42").fetchall())
    # cursor still usable after interrupt
    out["usable_after_interrupt"] = err(lambda: cur.execute("select 1").fetchall())
    # interrupt during fetch of a large materialised result
    cur.execute("select * from range(50000000)")
    timer = threading.Timer(0.05, cur.interrupt)
    timer.start()
    t0 = time.perf_counter()
    try:
        while cur.fetchmany(100000):
            pass
        out["interrupt_during_fetch"] = f"finished after {(time.perf_counter() - t0) * 1000:.0f}ms"
    except Exception as e:
        out["interrupt_during_fetch"] = f"{type(e).__name__} after {(time.perf_counter() - t0) * 1000:.0f}ms"
    # interrupt inside a transaction: is tx still alive?
    cur.execute("create table t(a int)")
    cur.execute("begin")
    cur.execute("insert into t values (1)")
    timer = threading.Timer(0.05, cur.interrupt)
    timer.start()
    try:
        cur.execute(sql).fetchall()
    except Exception:
        pass
    out["tx_after_interrupt"] = err(lambda: cur.execute("select count(*) from t").fetchall())
    out["commit_after_interrupt"] = err(lambda: cur.execute("commit"))
    err(lambda: cur.execute("rollback"))
    # interrupt of a cursor does not hit a sibling cursor
    sib = con.cursor()
    timer = threading.Timer(0.05, cur.interrupt)
    timer.start()
    t0 = time.perf_counter()
    res: dict[str, str] = {}

    def other() -> None:
        res["sib"] = err(lambda: sib.execute("select count(*) from range(300000000)").fetchall())

    th = threading.Thread(target=other)
    th.start()
    try:
        cur.execute(sql).fetchall()
    except Exception as e:
        res["cur"] = type(e).__name__
    th.join()
    out["sibling_unaffected"] = res
    con.close()
    return out


@experiment
def settings(tmp: Path) -> Any:
    con = duckdb.connect(str(tmp / "set.duckdb"), config={"memory_limit": "64MB", "threads": 1})
    out = {
        "config_memory_limit": con.execute("select current_setting('memory_limit')").fetchone()[0],
        "config_threads": con.execute("select current_setting('threads')").fetchone()[0],
    }
    cur = con.cursor()
    cur.execute("set memory_limit = '32MB'")
    out["set_on_cursor_seen_by_parent"] = con.execute("select current_setting('memory_limit')").fetchone()[0]
    cur.execute("set threads = 2")
    out["threads_after_set_on_cursor"] = con.execute("select current_setting('threads')").fetchone()[0]
    out["oom"] = err(
        lambda: con.execute(
            "select list(range) from (select range, range % 1000 as g from range(20000000)) group by g"
        ).fetchall()[:0]
    )
    out["oom_class_mro"] = ""
    try:
        con.execute("select string_agg(repeat('x', 100), ',') from range(10000000)").fetchall()
    except Exception as e:
        out["oom_class_mro"] = " > ".join(c.__name__ for c in type(e).__mro__[:-2])
    out["default_memory_limit_new_instance"] = duckdb.connect().execute(
        "select current_setting('memory_limit'), current_setting('threads')"
    ).fetchone()
    con.close()
    return out


@experiment
def alter_with_dependents(tmp: Path) -> Any:
    con = duckdb.connect(str(tmp / "alter.duckdb"))
    out = {}

    def fresh() -> None:
        for t in ("child", "t"):
            con.execute(f"drop table if exists {t}")
        con.execute("create table t(id integer primary key, a integer, b varchar unique, c integer)")
        con.execute("create index t_a on t(a)")
        con.execute("insert into t values (1, 1, 'x', 1)")

    cases = {
        "rename_indexed_col": "alter table t rename column a to a2",
        "rename_pk_col": "alter table t rename column id to id2",
        "rename_unique_col": "alter table t rename column b to b2",
        "rename_plain_col": "alter table t rename column c to c2",
        "drop_indexed_col": "alter table t drop column a",
        "drop_unique_col": "alter table t drop column b",
        "drop_pk_col": "alter table t drop column id",
        "drop_plain_col": "alter table t drop column c",
        "type_indexed_col": "alter table t alter column a type bigint",
        "type_plain_col": "alter table t alter column c type bigint",
        "type_pk_col": "alter table t alter column id type bigint",
        "add_column": "alter table t add column d integer",
        "add_column_default": "alter table t add column d integer default 5",
        "add_pk": "alter table t add primary key (c)",
        "set_not_null": "alter table t alter column c set not null",
        "rename_table": "alter table t rename to t2",
    }
    for name, sql in cases.items():
        fresh()
        out[name] = err(lambda: con.execute(sql))
    con.execute("drop table if exists t2")
    fresh()
    con.execute("create table child(id int, t_id int references t(id))")
    out["rename_referenced_table"] = err(lambda: con.execute("alter table t rename to t3"))
    out["drop_referenced_table"] = err(lambda: con.execute("drop table t"))
    out["rename_referenced_col"] = err(lambda: con.execute("alter table t rename column id to idx"))
    con.execute("drop table child")
    fresh()
    con.execute("create view v as select * from t")
    out["drop_table_with_view"] = err(lambda: con.execute("drop table t"))
    out["view_after_drop"] = err(lambda: con.execute("select * from v").fetchall())
    con.execute("drop view if exists v")
    fresh()
    con.execute("create view v as select * from t")
    out["rename_table_with_view"] = err(lambda: con.execute("alter table t rename to t4"))
    out["view_after_rename"] = err(lambda: con.execute("select * from v").fetchall())
    con.close()
    return out


@experiment
def definitions(tmp: Path) -> Any:
    con = duckdb.connect(str(tmp / "defs.duckdb"))
    con.execute(
        "create table t(id integer primary key, \"we ird\" varchar not null default 'x', "
        "c double check (c > 0), u int unique)"
    )
    con.execute("create index t_c on t(c)")
    con.execute("create view v as select id from t where id > 1")
    con.execute("create sequence s")
    out = {
        "table_sql": con.execute("select sql from duckdb_tables() where table_name='t'").fetchone()[0],
        "index_sql": con.execute("select sql from duckdb_indexes() where index_name='t_c'").fetchone()[0],
        "view_sql": con.execute("select sql from duckdb_views() where view_name='v'").fetchone()[0],
        "constraints": con.execute(
            "select constraint_type, constraint_column_names, constraint_name from duckdb_constraints() "
            "where table_name='t' order by constraint_index"
        ).fetchall(),
        "pk_index_listed": con.execute("select index_name, is_unique, is_primary, expressions from duckdb_indexes()").fetchall(),
        "column_defaults": con.execute(
            "select column_name, data_type, is_nullable, column_default from duckdb_columns() where table_name='t'"
        ).fetchall(),
    }
    con.execute("drop table t cascade") if False else None
    out["replay_table_sql"] = err(
        lambda: (con.execute("drop view v"), con.execute("drop table t"), con.execute(out["table_sql"]))[-1]
        and None
    )
    con.close()
    return out


@experiment
def params(tmp: Path) -> Any:
    con = duckdb.connect()
    out = {
        "dollar_name": err(lambda: con.execute("select $a, $b, $a", {"a": 1, "b": 2}).fetchall()),
        "qmark": err(lambda: con.execute("select ?, ?", [1, 2]).fetchall()),
        "dollar_num": err(lambda: con.execute("select $1, $1, $2", [1, 2]).fetchall()),
        "colon_name": err(lambda: con.execute("select :a", {"a": 1}).fetchall()),
        "extra_key": err(lambda: con.execute("select $a", {"a": 1, "zz": 2}).fetchall()),
        "case_insensitive": err(lambda: con.execute("select $A", {"a": 1}).fetchall()),
        "dollar_quote_string": err(lambda: con.execute("select $$it's $a$$").fetchall()),
        "tagged_dollar_quote": err(lambda: con.execute("select $t$x$t$").fetchall()),
        "e_string": err(lambda: con.execute("select E'a\\'b'").fetchall()),
        "backslash_in_plain_string": err(lambda: con.execute("select 'a\\'").fetchall()),
        "backtick_ident": err(lambda: con.execute("select 1 as `x`").fetchall()),
        "bracket_ident": err(lambda: con.execute("select [1,2,3][2:3]").fetchall()),
        "nested_comment": err(lambda: con.execute("select /* a /* b */ c */ 1").fetchall()),
        "cast": err(lambda: con.execute("select '1'::int").fetchall()),
        "struct_colon": err(lambda: con.execute("select {'a': 1}").fetchall()),
        "slice_vars": err(lambda: con.execute("select [1,2,3][$lo:$hi]", {"lo": 1, "hi": 2}).fetchall()),
        "prefix_named_param_ok": err(lambda: con.execute("select $a1", {"a1": 5}).fetchall()),
        "executemany_named": err(lambda: con.execute("create table e(a int)") and con.executemany("insert into e values ($a)", [{"a": 1}, {"a": 2}]).rowcount),
    }
    con.close()
    return out


@experiment
def wal_file(tmp: Path) -> Any:
    p = tmp / "wal.duckdb"
    con = duckdb.connect(str(p))
    con.execute("create table t(a int)")
    con.execute("insert into t values (1)")
    during = sorted(f.name for f in tmp.iterdir() if f.name.startswith("wal"))
    s1 = os.stat(p)
    con.execute("insert into t values (2)")
    files_after_insert = {f.name: f.stat().st_size for f in tmp.iterdir() if f.name.startswith("wal")}
    con.close()
    after = sorted(f.name for f in tmp.iterdir() if f.name.startswith("wal"))
    return {
        "while_open": during,
        "sizes_after_second_insert": files_after_insert,
        "main_file_size_before_second_insert": s1.st_size,
        "after_close": after,
    }


@experiment
def threads_and_cursors(tmp: Path) -> Any:
    con = duckdb.connect(str(tmp / "thr.duckdb"), config={"threads": 4})
    out = {}
    c1, c2 = con.cursor(), con.cursor()
    q = "select count(*) from range(300000000) where range % 7 = 1"
    t0 = time.perf_counter(); c1.execute(q).fetchall(); single = time.perf_counter() - t0
    res: list[Any] = []
    def run(c: Any) -> None:
        res.append(err(lambda: c.execute(q).fetchall()))
    t0 = time.perf_counter()
    ths = [threading.Thread(target=run, args=(c,)) for c in (c1, c2)]
    [t.start() for t in ths]; [t.join() for t in ths]
    out["two_cursors_two_threads"] = f"{res} single={single:.2f}s both={time.perf_counter() - t0:.2f}s"
    res.clear()
    ths = [threading.Thread(target=run, args=(c1,)) for _ in range(2)]
    [t.start() for t in ths]; [t.join() for t in ths]
    out["same_cursor_two_threads"] = res[:]
    out["cursor_used_from_other_thread_sequentially"] = err(
        lambda: [threading.Thread(target=lambda: c2.execute("select 1").fetchall()).start()]
        and None
    )
    # open result on cursor blocks it?
    c1.execute("select * from range(10)")
    out["new_execute_with_open_result"] = err(lambda: c1.execute("select 2").fetchall())
    con.close()
    return out


@experiment
def rowcount_and_returning(tmp: Path) -> Any:
    con = duckdb.connect()
    con.execute("create table t(id integer primary key, v int)")
    out = {}
    cur = con.execute("insert into t values (1, 1), (2, 2)")
    out["insert_rowcount"] = cur.rowcount
    out["insert_fetch"] = err(lambda: cur.fetchall())
    out["insert_description"] = cur.description
    cur = con.execute("update t set v = 5")
    out["update_rowcount"] = cur.rowcount
    cur = con.execute("insert into t values (3, 3) returning id")
    out["returning_rows"] = cur.fetchall()
    out["returning_rowcount"] = cur.rowcount
    cur = con.execute("with x as (select 4 as id) insert into t select id, 0 from x")
    out["cte_insert_rowcount"] = cur.rowcount
    cur = con.execute("insert into t values (4, 9) on conflict do nothing")
    out["on_conflict_nothing_rowcount"] = (cur.rowcount, err(cur.fetchall))
    cur = con.execute("insert into t values (4, 9) on conflict (id) do update set v = excluded.v")
    out["upsert_rowcount"] = (cur.rowcount, err(cur.fetchall))
    cur = con.execute("select 1")
    out["select_rowcount"] = cur.rowcount
    cur = con.executemany("insert into t values (?, ?)", [(10, 1), (11, 1)])
    out["executemany_rowcount"] = cur.rowcount
    out["rowid"] = err(lambda: con.execute("select rowid, id from t order by id limit 2").fetchall())
    out["types"] = con.execute("select 1::integer, 1::bigint, 1.5::double, 'a', 'a'::blob, true, null").description
    return out


@experiment
def catalog_version(tmp: Path) -> Any:
    con = duckdb.connect(str(tmp / "cv.duckdb"))
    out = {}
    fns = con.execute(
        "select distinct function_name from duckdb_functions() where function_name ilike '%catalog%' "
        "or function_name ilike '%version%' or function_name ilike '%schema%'"
    ).fetchall()
    out["candidate_functions"] = sorted(f[0] for f in fns)
    out["duckdb_databases_columns"] = [d[0] for d in con.execute("select * from duckdb_databases()").description]
    try:
        con.execute("create table a(x int)")
        out["pragma_database_size"] = con.execute("pragma database_size").fetchall()
    except Exception as e:
        out["pragma_database_size"] = str(e)
    con.close()
    return out


@experiment
def foreign_keys_and_errors(tmp: Path) -> Any:
    con = duckdb.connect()
    out: dict[str, Any] = {}
    con.execute("create table p(id int primary key)")
    out["on_delete_cascade"] = err(
        lambda: con.execute("create table c0(id int, p_id int references p(id) on delete cascade)")
    )
    con.execute("create table c(id int, p_id int references p(id))")
    out["fk_listing"] = con.execute(
        "select constraint_type, constraint_column_names, referenced_table, referenced_column_names "
        "from duckdb_constraints() where table_name = 'c' and constraint_type = 'FOREIGN KEY'"
    ).fetchall()
    con.execute("insert into p values (1)")
    out["fk_violation"] = err(lambda: con.execute("insert into c values (1, 2)"))
    con.execute("insert into c values (1, 1)")
    out["delete_referenced_row"] = err(lambda: con.execute("delete from p where id = 1"))
    out["drop_referenced_table"] = err(lambda: con.execute("drop table p"))
    out["unique_violation"] = err(lambda: con.execute("insert into p values (1)"))
    con.execute("create table nn(a int not null)")
    out["not_null_violation"] = err(lambda: con.execute("insert into nn values (null)"))
    out["missing_table"] = err(lambda: con.execute("select * from nope"))
    out["syntax"] = err(lambda: con.execute("selec 1"))
    out["self_ref_fk"] = err(lambda: con.execute("create table s(id int primary key, parent int references s(id))"))
    out["triggers"] = err(lambda: con.execute("create trigger x after insert on p begin select 1; end"))
    out["savepoint"] = err(lambda: (con.execute("begin"), con.execute("savepoint a")))
    err(lambda: con.execute("rollback"))
    out["names_case_insensitive"] = err(lambda: (con.execute('create table "Foo"(a int)'), con.execute("create table foo(a int)")))
    out["partial_index"] = err(lambda: con.execute("create index pi on p(id) where id > 1"))
    out["multi_statement_execute"] = err(lambda: con.execute("select 1; select 2").fetchall())
    out["real_type_is"] = (con.execute("create table r(a REAL, b TEXT, c BYTEA)"), con.execute(
        "select data_type from duckdb_columns() where table_name = 'r'").fetchall())[1]
    return out


@experiment
def alter_by_constraint_kind(tmp: Path) -> Any:
    """Which ALTER forms work when the table has only a PK / UNIQUE
    constraint, versus a CREATE INDEX index."""
    con = duckdb.connect()
    cases = {
        "rename_col": "alter table a rename column y to y2",
        "drop_col": "alter table a drop column z",
        "type": "alter table a alter column z type bigint",
        "rename_tbl": "alter table a rename to a2",
        "set_not_null": "alter table a alter column z set not null",
        "set_default": "alter table a alter column z set default 3",
        "add_col": "alter table a add column w int",
    }
    out = {}
    for label, create, idx in [
        ("plain", "create table a(x int, y int, z int)", None),
        ("pk", "create table a(x int primary key, y int, z int)", None),
        ("unique", "create table a(x int, y int unique, z int)", None),
        ("index", "create table a(x int, y int, z int)", "create index ai on a(x)"),
    ]:
        res = {}
        for k, sql in cases.items():
            con.execute("drop table if exists a"); con.execute("drop table if exists a2")
            con.execute(create)
            if idx:
                con.execute(idx)
            res[k] = "ok" if err(lambda: con.execute(sql)).startswith("ok") else "refused"
        out[label] = res
    con.execute("drop table if exists a"); con.execute("drop table if exists a2")
    con.execute("create table a(x int)"); con.execute("create view va as select * from a")
    out["rename_table_with_view"] = err(lambda: con.execute("alter table a rename to a2"))
    out["view_after_rename"] = err(lambda: con.execute("select * from va").fetchall())
    return out


@experiment
def aborted_transactions(tmp: Path) -> Any:
    con = duckdb.connect()
    c = con.cursor()
    c.execute("create table t(id int primary key)")
    c.execute("begin")
    c.execute("insert into t values (1)")
    out = {"dup_in_tx": err(lambda: c.execute("insert into t values (1)"))}
    out["next_statement"] = err(lambda: c.execute("select 1").fetchall())
    out["sql_commit_after_abort"] = err(lambda: c.execute("commit") and None)
    out["rows_after_commit"] = con.execute("select count(*) from t").fetchone()[0]
    c.execute("begin"); c.execute("insert into t values (1)")
    err(lambda: c.execute("insert into t values (1)"))
    out["py_commit_after_abort"] = err(lambda: c.commit() and None)
    out["rows_after_py_commit"] = con.execute("select count(*) from t").fetchone()[0]
    out["rollback_without_tx"] = err(lambda: c.execute("rollback"))
    c.execute("begin")
    out["nested_begin"] = err(lambda: c.execute("begin"))
    c.execute("rollback")
    c.execute("begin transaction read only")
    out["write_in_read_only_tx"] = err(lambda: c.execute("insert into t values (5)"))
    c.execute("rollback")
    c.execute("begin transaction read only")
    out["temp_table_in_read_only_tx"] = err(lambda: c.execute("create temp table z(a int)") and None)
    c.execute("rollback")
    return out


@experiment
def security_and_lock(tmp: Path) -> Any:
    (tmp / "x.csv").write_text("a\n1\n")
    con = duckdb.connect(
        str(tmp / "sec.duckdb"),
        config={"memory_limit": "64MB", "threads": 1, "enable_external_access": False},
    )
    out = {
        "read_csv": err(lambda: con.execute(f"select * from read_csv('{tmp}/x.csv')").fetchall()),
        "attach": err(lambda: con.execute(f"attach '{tmp}/other.duckdb'")),
    }
    con.execute("set lock_configuration = true")
    cur = con.cursor()
    out["set_memory_after_lock"] = err(lambda: cur.execute("set memory_limit = '10GB'"))
    out["pragma_threads_after_lock"] = err(lambda: cur.execute("pragma threads = 4"))
    out["set_search_path_after_lock"] = err(lambda: cur.execute("set search_path = 'main'") and None)
    con.close()
    return out


@experiment
def statement_types_and_costs(tmp: Path) -> Any:
    con = duckdb.connect()
    con.execute("create table t(a int)")
    sts = con.extract_statements(
        "select 1; insert into t values (1) returning a; pragma table_info('t'); describe t; "
        "checkpoint; set threads=1; copy t to 'x'; create table q as select 1; drop table q"
    )
    out: dict[str, Any] = {"types": [str(s.type).split(".")[1] for s in sts]}
    t0 = time.perf_counter()
    for _ in range(1000):
        con.extract_statements("select id, title from posts where author = $author order by id")
    out["extract_statements_us"] = round((time.perf_counter() - t0) * 1000, 1)
    cur = con.cursor()
    t0 = time.perf_counter()
    for _ in range(1000):
        cur.execute("begin transaction read only"); cur.execute("select 1").fetchall(); cur.execute("commit")
    out["select_in_read_only_tx_us"] = round((time.perf_counter() - t0) * 1000, 1)
    t0 = time.perf_counter()
    for _ in range(1000):
        cur.execute("select 1").fetchall()
    out["plain_select_us"] = round((time.perf_counter() - t0) * 1000, 1)
    out["ddl_description"] = con.execute("create table q2(a int)").description
    cur = con.execute("insert into t values (1), (2)")
    out["insert_result"] = (cur.rowcount, cur.fetchall())
    cur = con.execute("insert into t values (3) returning a")
    out["insert_returning"] = (cur.rowcount, cur.fetchall())
    return out


@experiment
def catalog_oids(tmp: Path) -> Any:
    con = duckdb.connect()
    con.execute("create table o(a int)")
    q = "select table_oid, sql from duckdb_tables() where table_name = 'o'"
    before = con.execute(q).fetchone()
    con.execute("alter table o add column b int")
    after = con.execute(q).fetchone()
    return {"oid_unchanged_by_alter": before[0] == after[0], "sql_changed": before[1] != after[1]}


OBSERVED: dict[str, Any] = {
    "mixed_modes_one_process": "ro while rw open and rw while ro open both refused "
    "(ConnectionException: different configuration); second rw / second ro connect share "
    "the cached instance",
    "other_process_while_open": "another process cannot open the file rw or ro while it is "
    "open (IOException: Could not set lock); closing the parent connection closes its "
    "cursors and releases the lock; closing only a cursor does not",
    "write_conflicts": "same-row UPDATE in two txs: second fails at once (TransactionException: "
    "Conflict on update!); different rows, concurrent appends and DDL on different tables do "
    "not conflict; same new PK in two txs fails at the second COMMIT (TransactionException: "
    "Failed to commit: PRIMARY KEY or UNIQUE constraint violation); UPDATE of a row committed "
    "by another tx after our snapshot conflicts; ALTER vs INSERT on one table conflicts; reads "
    "are snapshot isolated",
    "transactional_ddl": "yes: CREATE TABLE/INDEX/VIEW and ALTER all rolled back",
    "interrupt_latency": "interrupt() from another thread lands within ~5 ms; on an idle "
    "cursor it is a no-op (not remembered); it also stops a fetch in progress; it ABORTS the "
    "open transaction (later statements: 'Current transaction is aborted'); sibling cursors "
    "are unaffected",
    "settings": "memory_limit and threads via config or SET; SET on any cursor changes the "
    "whole instance; defaults on this Mac are 80% of RAM (102 GiB) and 18 threads; "
    "OutOfMemoryException is a subclass of OperationalError",
    "alter_with_dependents": "with a CREATE INDEX index on the table every ALTER except ADD "
    "COLUMN / SET DEFAULT fails (DependencyException), including RENAME TO and changes to "
    "unindexed columns; drop of PK/UNIQUE column refused; type change of PK/UNIQUE column "
    "refused; rename of a column referenced by a foreign key refused",
    "definitions": "duckdb_tables().sql, duckdb_indexes().sql and duckdb_views().sql give "
    "replayable CREATE statements (CHECK constraints kept); PK/UNIQUE are constraints, not "
    "listed in duckdb_indexes()",
    "params": "$name (case-insensitive, excess keys are an error), ? and $1 work; :name is a "
    "syntax error; $$...$$ and $tag$ quotes, E'' strings, nested /* */ comments and backtick "
    "identifiers are supported; [ ] is a list/slice, not an identifier quote",
    "wal_file": "<file>.wal next to the database; removed by checkpoint on close",
    "threads_and_cursors": "cursors of one instance run concurrently on different threads; "
    "the same cursor used from two threads at once raises ResourceInUseException (it does "
    "not wait)",
    "rowcount_and_returning": "INSERT/UPDATE/DELETE return a one-row 'Count' result and set "
    "rowcount; with RETURNING rowcount is -1; WITH ... INSERT rowcount is right; DDL also "
    "returns a 'Count' description with rowcount -1; rowid pseudo-column exists (0-based)",
    "catalog_version": "no catalog version counter is exposed (no such function/pragma)",
    "foreign_keys_and_errors": "FK ON DELETE/UPDATE actions other than NO ACTION are a "
    "parser error; FKs are enforced; a referenced table cannot be dropped; constraint "
    "violations are ConstraintException; no triggers; no savepoints; table names are "
    "case-insensitive; no partial indexes; execute() of 'select 1; select 2' runs both; "
    "REAL is FLOAT (4-byte)",
    "alter_by_constraint_kind": "tables with only PK/UNIQUE constraints: every ALTER works; "
    "with a CREATE INDEX index only ADD COLUMN and SET DEFAULT work; a table with a view can "
    "be renamed (the view then fails: views bind late by name)",
    "aborted_transactions": "after any error in a transaction the tx is aborted; COMMIT then "
    "SUCCEEDS SILENTLY and rolls back (both SQL COMMIT and Connection.commit()); ROLLBACK "
    "without a tx is an error; BEGIN TRANSACTION READ ONLY refuses writes but allows temp "
    "tables",
    "security_and_lock": "enable_external_access=false blocks read_csv/COPY/ATTACH; "
    "lock_configuration=true blocks SET/PRAGMA of memory_limit and threads",
    "statement_types_and_costs": "extract_statements gives the statement type in ~14 us; a "
    "read-only transaction wrapper costs ~40 us per statement",
    "catalog_oids": "table_oid does not change on ALTER; duckdb_tables().sql does",
}


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        for exp in EXPERIMENTS:
            exp(Path(d))


if __name__ == "__main__":
    print("duckdb", duckdb.__version__)
    main()
