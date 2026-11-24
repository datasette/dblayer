# dblayer DuckDB worker in Rust: a DuckDB worker process speaking
# docs/worker-protocol.md, linked against the official prebuilt libduckdb.
#
#   make -C duckdb-rs-worker            # target/release/dblayer-duckdb-rsworker
#   make -C duckdb-rs-worker debug      # target/debug/dblayer-duckdb-rsworker
#   make -C duckdb-rs-worker test       # cargo test (the Rust unit tests)
#
# libduckdb 2.0.1 (the version of the Python duckdb package) is downloaded
# once into vendor/libduckdb (git-ignored) and checked against a pinned
# SHA-256 (fetch-libduckdb.sh). Python finds the binary through an explicit
# path, then $DBLAYER_DUCKDB_RS_WORKER, then target/release/.

CARGO ?= cargo

release: vendor/libduckdb/STAMP
	$(CARGO) build --release --locked

debug: vendor/libduckdb/STAMP
	$(CARGO) build --locked

test: vendor/libduckdb/STAMP
	$(CARGO) test --locked

vendor/libduckdb/STAMP: fetch-libduckdb.sh
	./fetch-libduckdb.sh

clean:
	$(CARGO) clean

.PHONY: release debug test clean
