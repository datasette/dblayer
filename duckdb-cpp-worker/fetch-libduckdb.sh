#!/bin/sh
# Download the official prebuilt libduckdb (shared library and C headers)
# for this platform into vendor/libduckdb-<version>-<platform>/, once, and
# check it against a pinned SHA-256 (the digests GitHub publishes for the
# release assets). The version must match the Python ``duckdb`` package so
# that database files are interchangeable.
#
#   ./fetch-libduckdb.sh          # prints the directory it filled (or found)
set -eu

VERSION=2.0.1
cd "$(dirname "$0")"

case "$(uname -s)-$(uname -m)" in
Darwin-*)
	PLATFORM=osx-universal
	SHA256=9a4be5a2a31ad5bb8c92bf37f18d59b9c10a79b392d7848cabd6eb2e05837449
	LIB=libduckdb.dylib
	;;
Linux-x86_64)
	PLATFORM=linux-amd64
	SHA256=e25502612fed533a243b61f23a2a574daad030e7e88906222a5f0ab30d06769e
	LIB=libduckdb.so
	;;
Linux-aarch64 | Linux-arm64)
	PLATFORM=linux-arm64
	SHA256=811e8b4d0226f5e531f583aadc798070ffb33b350e3823be563a2ebeb2c68593
	LIB=libduckdb.so
	;;
*)
	echo "fetch-libduckdb.sh: no prebuilt libduckdb for $(uname -s) $(uname -m)" >&2
	exit 1
	;;
esac

DEST="vendor/libduckdb-$VERSION-$PLATFORM"
if [ -f "$DEST/$LIB" ] && [ -f "$DEST/duckdb.h" ]; then
	echo "$DEST"
	exit 0
fi

ASSET="duckdb-shared-libs-$PLATFORM.tar.gz"
URL="https://github.com/duckdb/duckdb/releases/download/v$VERSION/$ASSET"
TMP="vendor/download-$$"
rm -rf "$TMP"
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

echo "downloading $URL" >&2
curl -fsSL --retry 3 -o "$TMP/$ASSET" "$URL"
SUM=$( (sha256sum "$TMP/$ASSET" 2>/dev/null || shasum -a 256 "$TMP/$ASSET") | cut -d' ' -f1)
if [ "$SUM" != "$SHA256" ]; then
	echo "fetch-libduckdb.sh: checksum mismatch for $ASSET: got $SUM, expected $SHA256" >&2
	exit 1
fi
mkdir -p "$TMP/x"
tar -xzf "$TMP/$ASSET" -C "$TMP/x" "$LIB" duckdb.h
rm -rf "$DEST"
mv "$TMP/x" "$DEST"
echo "$DEST"
