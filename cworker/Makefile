# dblayer C worker: a SQLite worker process speaking docs/worker-protocol.md.
#
#   make -C cworker                  # build/dblayer-cworker (SQLite amalgamation compiled in)
#   make -C cworker SQLITE=system    # link the system libsqlite3 instead
#   make -C cworker SANITIZE=1       # build-asan/dblayer-cworker with ASan + UBSan
#   make -C cworker wasm             # build-wasm/dblayer-sqlite.wasm (see Makefile.wasm)
#
# The amalgamation is downloaded once into vendor/ (git-ignored) and checked
# against a pinned SHA-256. Python finds the binary through an explicit
# path, then $DBLAYER_CWORKER, then cworker/build/dblayer-cworker.

SQLITE ?= amalgamation
SQLITE_VERSION ?= 3530100
SQLITE_YEAR ?= 2026
# SHA-256 of sqlite-amalgamation-3530100.zip (SQLite 3.53.1).
SQLITE_SHA256 ?= 36ad6e7f38540a3b21a2ac36340833f0a9e426bc1c752751c3ba669466827eae
SQLITE_DIR := vendor/sqlite-amalgamation-$(SQLITE_VERSION)
SQLITE_ZIP := vendor/sqlite-amalgamation-$(SQLITE_VERSION).zip

CC ?= cc
OPT ?= -O2 -g
WARN := -std=c11 -Wall -Wextra -Wshadow -Wstrict-prototypes -Wmissing-prototypes
LIBS := -lpthread -lm

ifeq ($(shell uname -s),Linux)
LIBS += -ldl
endif

ifdef SANITIZE
BUILD ?= build-asan
SAN := -fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=undefined
else
BUILD ?= build
SAN :=
endif

# The options python-build-standalone compiles its SQLite with, so that the
# C worker and Python's sqlite3 module behave alike.
SQLITE_DEFS := -DSQLITE_THREADSAFE=1 -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS3_PARENTHESIS \
	-DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_GEOPOLY \
	-DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_PERCENTILE -DSQLITE_ENABLE_DBSTAT_VTAB \
	-DSQLITE_DQS=3

DEFS :=
ifeq ($(SQLITE),system)
ifeq ($(shell uname -s),Darwin)
# Apple's libsqlite3 is built with SQLITE_OMIT_LOAD_EXTENSION.
DEFS += -DCWORKER_NO_LOAD_EXTENSION
endif
SQLITE_INC :=
SQLITE_OBJ :=
SQLITE_LIB := -lsqlite3
SQLITE_DEP :=
else
SQLITE_INC := -I$(SQLITE_DIR)
SQLITE_OBJ := $(BUILD)/sqlite3.o
SQLITE_LIB :=
SQLITE_DEP := $(SQLITE_DIR)/sqlite3.h
endif

SRC := src/worker.c src/codec.c src/sqlutil.c src/schema.c
HDR := src/codec.h src/sqlutil.h src/errnames.h
OBJ := $(patsubst src/%.c,$(BUILD)/%.o,$(SRC))
BIN := $(BUILD)/dblayer-cworker

all: $(BIN)

$(BIN): $(OBJ) $(SQLITE_OBJ)
	$(CC) $(OPT) $(SAN) -o $@ $^ $(SQLITE_LIB) $(LIBS)

$(BUILD)/%.o: src/%.c $(HDR) $(SQLITE_DEP) | $(BUILD)
	$(CC) $(OPT) $(SAN) $(WARN) $(DEFS) $(SQLITE_INC) -c -o $@ $<

$(BUILD)/sqlite3.o: $(SQLITE_DIR)/sqlite3.c | $(BUILD)
	$(CC) $(OPT) $(SAN) $(SQLITE_DEFS) -c -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

$(SQLITE_DIR)/sqlite3.c $(SQLITE_DIR)/sqlite3.h: $(SQLITE_ZIP)
	cd vendor && unzip -oq sqlite-amalgamation-$(SQLITE_VERSION).zip
	touch $(SQLITE_DIR)/sqlite3.c $(SQLITE_DIR)/sqlite3.h

$(SQLITE_ZIP):
	mkdir -p vendor
	curl -fsSL -o $@.tmp https://sqlite.org/$(SQLITE_YEAR)/sqlite-amalgamation-$(SQLITE_VERSION).zip
	@sum=$$( (sha256sum $@.tmp 2>/dev/null || shasum -a 256 $@.tmp) | cut -d' ' -f1); \
	if [ "$$sum" != "$(SQLITE_SHA256)" ]; then \
		echo "checksum mismatch for $@: got $$sum, expected $(SQLITE_SHA256)" >&2; rm -f $@.tmp; exit 1; \
	fi
	mv $@.tmp $@

clean:
	rm -rf build build-asan build-wasm

distclean: clean
	rm -rf vendor

# SQLite compiled to WebAssembly, for dblayer.backends.sqlite_wasm. Downloads
# a pinned wasi-sdk into vendor/ (checksummed) on first use.
wasm: $(SQLITE_DIR)/sqlite3.c
	$(MAKE) -f Makefile.wasm SQLITE_DIR=$(SQLITE_DIR)

.PHONY: all clean distclean wasm
