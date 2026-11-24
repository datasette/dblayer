# dblayer SQLite worker in Rust: a drop-in replacement for
# `cworker/build/dblayer-cworker` (single-tenant and --multi), speaking
# docs/worker-protocol.md.
#
#   make -C sqlite-rs-worker            # target/release/dblayer-sqlite-rsworker
#   make -C sqlite-rs-worker debug      # target/debug/dblayer-sqlite-rsworker
#   make -C sqlite-rs-worker test       # cargo test (unit tests and the fuzz)
#
# SQLite is the C worker's pinned, checksummed 3.53.1 amalgamation
# (downloaded once into cworker/vendor/ by cworker/Makefile), compiled by
# build.rs with the C worker's own options (SQLITE_DEFS, read from
# cworker/Makefile, and -O2 -g). Python finds the binary through an explicit
# path, then $DBLAYER_RSWORKER, then target/release/.

CARGO ?= cargo
AMALGAMATION := ../cworker/vendor/sqlite-amalgamation-3530100/sqlite3.c

release: $(AMALGAMATION)
	$(CARGO) build --release --locked

debug: $(AMALGAMATION)
	$(CARGO) build --locked

test: $(AMALGAMATION)
	$(CARGO) test --release --locked

$(AMALGAMATION):
	$(MAKE) -C ../cworker vendor/sqlite-amalgamation-3530100/sqlite3.c

clean:
	$(CARGO) clean

.PHONY: release debug test clean
