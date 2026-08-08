#!/bin/sh
set -eu

# Build the exact upstream SQLite used by the guest acceptance image.  Keeping
# this outside libsqlite3-sys prevents a crate update from silently changing
# the database engine under the VFS tests.
SQLITE_VERSION=3.53.4
SQLITE_ARCHIVE_VERSION=3530400
SQLITE_ARCHIVE="sqlite-autoconf-${SQLITE_ARCHIVE_VERSION}.tar.gz"
SQLITE_ARCHIVE_SHA3=454e45f61c6bd75b7420e7190732dea03ce6639c63ada47bbc592f67fc340338

PREFIX=${1:?usage: build-sqlite.sh PREFIX}
CC=${CC:-cc}
AR=${AR:-ar}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT HUP INT TERM

curl -fsSL --retry 5 --retry-delay 2 \
  "https://www.sqlite.org/2026/${SQLITE_ARCHIVE}" \
  -o "$WORK/$SQLITE_ARCHIVE"
ACTUAL_SHA3=$(openssl dgst -sha3-256 "$WORK/$SQLITE_ARCHIVE" | awk '{print $NF}')
if [ "$ACTUAL_SHA3" != "$SQLITE_ARCHIVE_SHA3" ]; then
  echo "SQLite archive SHA3-256 mismatch" >&2
  exit 1
fi

tar -xzf "$WORK/$SQLITE_ARCHIVE" -C "$WORK"
SOURCE="$WORK/sqlite-autoconf-${SQLITE_ARCHIVE_VERSION}"
mkdir -p "$PREFIX/include" "$PREFIX/lib"

"$CC" -O2 -fPIC \
  -DSQLITE_CORE \
  -DSQLITE_DEFAULT_FOREIGN_KEYS=1 \
  -DSQLITE_ENABLE_API_ARMOR \
  -DSQLITE_ENABLE_COLUMN_METADATA \
  -DSQLITE_ENABLE_DBSTAT_VTAB \
  -DSQLITE_ENABLE_FTS3 \
  -DSQLITE_ENABLE_FTS3_PARENTHESIS \
  -DSQLITE_ENABLE_FTS5 \
  -DSQLITE_ENABLE_MEMORY_MANAGEMENT \
  -DSQLITE_ENABLE_RTREE \
  -DSQLITE_ENABLE_STAT4 \
  -DSQLITE_SOUNDEX \
  -DSQLITE_THREADSAFE=1 \
  -DSQLITE_USE_URI \
  -DHAVE_USLEEP=1 \
  -DHAVE_ISNAN \
  -D_POSIX_THREAD_SAFE_FUNCTIONS \
  -c "$SOURCE/sqlite3.c" \
  -o "$WORK/sqlite3.o"
"$AR" crs "$PREFIX/lib/libsqlite3.a" "$WORK/sqlite3.o"
cp "$SOURCE/sqlite3.h" "$SOURCE/sqlite3ext.h" "$PREFIX/include/"
printf '%s\n%s\n' "$SQLITE_VERSION" "$SQLITE_ARCHIVE_SHA3" > "$PREFIX/SOURCE"
