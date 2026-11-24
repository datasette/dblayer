# dblayer build notes

Append-only log kept while building the prototype from `dblayer-prototype-spec.md`.
Each entry is tagged: **Found** (facts discovered), **Built** (what now exists),
**Decision** (a choice made, especially where it departs from the spec) or
**Idea** (something to discuss later, not acted on).

## 2026-11-23: before any code

### Found: environment

- Machine is macOS (Darwin 27.2, arm64). The spec assumes Linux in two places: `/proc/self/fd` and `RLIMIT_AS`.
- System `python3` is 3.9.6, below the spec's 3.11 floor. uv 0.13.0 has CPython 3.14.3 and 3.15.0 installed.
- PostgreSQL 18.3 (Homebrew) runs locally on the `/tmp:5432` socket. Role `simon` is a superuser. `asyncpg` 0.32.0 connects.
- The `duckdb` Python package is 2.0.1. There is no `duckdb` CLI. The spec's DuckDB section is written "from memory" and predates checking against 2.x, so every "to verify" there needs checking against this version.
- Docker is installed, so Linux-only tests can run in a container if wanted.
- `~/dev/datasette` is at `e245b406 Release 1.0a44` and is used as a read-only reference.

### Decision: how the work is run

- All Python runs through `uv run python` / `uv run pytest`.
- Opus 5.5 subagents write the code, one milestone each. Milestones 1 to 3 run in sequence because they share the core; 4 to 6 (worker, PostgreSQL, DuckDB) can run in parallel.
- Commits are made early and often, in sensible chunks, and pushed to `main` on GitHub (the repo is dedicated to this prototype). Subagents commit and push their own work whenever there is credible progress with passing tests; I commit notes and review fixes.

### Decision: departures from the spec so far

- **File-descriptor counting.** The spec says count entries in `/proc/self/fd`. A helper uses that path when it exists and `/dev/fd` otherwise, so the leak tests run natively on macOS and Linux.
- **PostgreSQL test server.** The spec says the fixture starts a server in a container. Tests instead use a dedicated database, `dblayer_test`, on the local server, with a fresh schema per test. This fits the spec's own model (one `Database` = one schema) and is much faster. The other databases on that server are never touched. A container fixture can be added later for CI.

### Idea: things noticed while reading the spec

- **`execute_write` has no `time_limit_ms`.** The Limits table says the time limit applies to "every read and write statement", but the method table gives `execute_write(sql, params, *, returning_limit)`. Writes probably want a time limit too, since a runaway `UPDATE` holds the only write slot.
- **Deadline expiry while a statement is running.** On in-process SQLite the transaction pins a connection, and a statement may be executing on a pool thread when the deadline fires. Rollback then needs `interrupt()` first, then `ROLLBACK` once the thread returns. The spec describes the deadline only between calls.
- **Cheaper, more exact SQLite change hint.** Size and mtime of the file and `-wal` can miss a change within one mtime tick, and they move on every data write, not just schema writes. Worth measuring how often the hint fires falsely under a write-heavy load, since each false hit costs a connection for `PRAGMA schema_version`.
- **`schema` message shape.** The wire-protocol table gives `schema` only a `tx` field, but the worked example sends `"table": "posts"`. Needs one definition: whole-database schema, with an optional table filter.
- **Python floor.** `sqlite3.Connection.autocommit` (3.12+) is the clean way to switch off implicit transactions. With a 3.11 floor the backend must use `isolation_level=None`. Raising the floor to 3.12 would simplify this; to discuss.

### Decision: Python floor raised to 3.12

Simon agreed to raise the floor from the spec's 3.11 to 3.12. The SQLite backend uses `sqlite3.connect(autocommit=True)` to switch off implicit transactions, in place of `isolation_level=None`. The milestone 1 subagent was told mid-run.
