#!/bin/sh
# Download the official prebuilt libduckdb for this platform into
# vendor/libduckdb (git-ignored), pinned to the version the Python package
# uses and checked against a SHA-256 recorded here. Run by `make`; safe to
# run again (does nothing when the right library is already there).
#
#   ./fetch-libduckdb.sh            # this platform
#   ./fetch-libduckdb.sh linux-amd64
set -eu

VERSION=2.0.1
cd "$(dirname "$0")"

platform=${1:-}
if [ -z "$platform" ]; then
    case "$(uname -s)-$(uname -m)" in
        Darwin-arm64) platform=osx-arm64 ;;
        Darwin-x86_64) platform=osx-amd64 ;;
        Linux-x86_64) platform=linux-amd64 ;;
        Linux-aarch64 | Linux-arm64) platform=linux-arm64 ;;
        *) echo "fetch-libduckdb: no prebuilt libduckdb for $(uname -s)-$(uname -m)" >&2; exit 1 ;;
    esac
fi

# SHA-256 of duckdb-shared-libs-<platform>.tar.gz from the v2.0.1 release.
case "$platform" in
    osx-arm64) sha=d71e4800350f1135bc3f80a2e5cbcc01e199ca03759edf5a376db308b514b341 ;;
    osx-amd64) sha=e12f174ef01575590ec6e27a5d475308680a9e15292691623d9848c0fc5a9ba8 ;;
    linux-amd64) sha=e25502612fed533a243b61f23a2a574daad030e7e88906222a5f0ab30d06769e ;;
    linux-arm64) sha=811e8b4d0226f5e531f583aadc798070ffb33b350e3823be563a2ebeb2c68593 ;;
    *) echo "fetch-libduckdb: unknown platform $platform" >&2; exit 1 ;;
esac

dest=vendor/libduckdb
stamp="$VERSION $platform $sha"
if [ -f "$dest/STAMP" ] && [ "$(cat "$dest/STAMP")" = "$stamp" ]; then
    exit 0
fi

archive=duckdb-shared-libs-$platform.tar.gz
url=https://github.com/duckdb/duckdb/releases/download/v$VERSION/$archive
mkdir -p vendor
tmp=vendor/$archive.part
echo "fetch-libduckdb: downloading $url" >&2
curl -fsSL --retry 3 -o "$tmp" "$url"

if command -v sha256sum >/dev/null 2>&1; then
    got=$(sha256sum "$tmp" | cut -d' ' -f1)
else
    got=$(shasum -a 256 "$tmp" | cut -d' ' -f1)
fi
if [ "$got" != "$sha" ]; then
    rm -f "$tmp"
    echo "fetch-libduckdb: checksum mismatch for $archive: got $got, want $sha" >&2
    exit 1
fi

rm -rf "$dest" "$dest.new"
mkdir -p "$dest.new"
tar -xzf "$tmp" -C "$dest.new" duckdb.h $(tar -tzf "$tmp" | grep -E '^libduckdb\.(so|dylib)$')
rm -f "$tmp"
echo "$stamp" > "$dest.new/STAMP"
mv "$dest.new" "$dest"
echo "fetch-libduckdb: libduckdb $VERSION ($platform) in $dest" >&2
