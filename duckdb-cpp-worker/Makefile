# dblayer DuckDB worker in C++: a DuckDB worker process speaking
# docs/worker-protocol.md, linked against the official prebuilt libduckdb
# through its C API.
#
#   make -C duckdb-cpp-worker             # build/dblayer-duckdb-worker
#   make -C duckdb-cpp-worker SANITIZE=1  # build-asan/dblayer-duckdb-worker (ASan + UBSan)
#
# The library is downloaded once into vendor/ (git-ignored) by
# fetch-libduckdb.sh and checked against a pinned SHA-256. The binary finds
# it through an rpath relative to itself, so no environment variable is
# needed. Python finds the binary through an explicit path, then
# $DBLAYER_DUCKDB_CPP_WORKER, then duckdb-cpp-worker/build/dblayer-duckdb-worker.

DUCKDB_VERSION := 2.0.1
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
PLATFORM := osx-universal
LIBFILE := libduckdb.dylib
RPATH := -Wl,-rpath,@executable_path/../vendor/libduckdb-$(DUCKDB_VERSION)-$(PLATFORM)
else
ifeq ($(UNAME_M),x86_64)
PLATFORM := linux-amd64
else
PLATFORM := linux-arm64
endif
LIBFILE := libduckdb.so
RPATH := -Wl,-rpath,'$$ORIGIN/../vendor/libduckdb-$(DUCKDB_VERSION)-$(PLATFORM)'
endif

LIBDIR := vendor/libduckdb-$(DUCKDB_VERSION)-$(PLATFORM)

CXX ?= c++
CC ?= cc
OPT ?= -O2 -g
WARN := -Wall -Wextra -Wshadow
CXXSTD := -std=c++17
LIBS := -L$(LIBDIR) -lduckdb -lpthread

ifdef SANITIZE
BUILD ?= build-asan
SAN := -fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=undefined
else
BUILD ?= build
SAN :=
endif

# The protocol codec is the C worker's (cworker/src/codec.c), compiled in.
CODEC_DIR := ../cworker/src
SRC := src/worker.cpp src/server.cpp src/exec.cpp src/values.cpp src/types.cpp src/schema.cpp src/errors.cpp src/wire.cpp
HDR := src/worker.hpp src/server.hpp src/wire.hpp $(CODEC_DIR)/codec.h
OBJ := $(patsubst src/%.cpp,$(BUILD)/%.o,$(SRC)) $(BUILD)/codec.o
BIN := $(BUILD)/dblayer-duckdb-worker

all: $(BIN)

$(BIN): $(OBJ) $(LIBDIR)/$(LIBFILE)
	$(CXX) $(OPT) $(SAN) -o $@ $(OBJ) $(LIBS) $(RPATH)

$(BUILD)/%.o: src/%.cpp $(HDR) $(LIBDIR)/duckdb.h | $(BUILD)
	$(CXX) $(CXXSTD) $(OPT) $(SAN) $(WARN) -I$(LIBDIR) -I$(CODEC_DIR) -c -o $@ $<

$(BUILD)/codec.o: $(CODEC_DIR)/codec.c $(CODEC_DIR)/codec.h | $(BUILD)
	$(CC) -std=c11 $(OPT) $(SAN) $(WARN) -c -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

$(LIBDIR)/duckdb.h $(LIBDIR)/$(LIBFILE):
	./fetch-libduckdb.sh

clean:
	rm -rf build build-asan

distclean: clean
	rm -rf vendor

.PHONY: all clean distclean
