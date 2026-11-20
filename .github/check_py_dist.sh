#!/usr/bin/env bash
# Usage: check_py_dist.sh DIR
#
# Fails unless DIR holds exactly the 5 wheels and the sdist for the version in
# crates/redb-python/Cargo.toml, and none of them are corrupt.
# Run from the repository root.
set -euo pipefail
shopt -s nullglob

dir="$1"
version=$(sed -n 's/^version = "\(.*\)"$/\1/p' crates/redb-python/Cargo.toml | head -n 1)
expected=(
    "redb-$version-*manylinux_2_17_x86_64*.whl"
    "redb-$version-*manylinux_2_17_aarch64*.whl"
    "redb-$version-*-macosx_*_x86_64.whl"
    "redb-$version-*-macosx_*_arm64.whl"
    "redb-$version-*-win_amd64.whl"
    "redb-$version.tar.gz"
)

for pattern in "${expected[@]}"; do
    matches=("$dir"/$pattern)
    if [ "${#matches[@]}" -ne 1 ]; then
        echo "Expected exactly one $pattern in $dir, found ${#matches[@]}" >&2
        exit 1
    fi
done

all=("$dir"/*)
if [ "${#all[@]}" -ne "${#expected[@]}" ]; then
    echo "Expected ${#expected[@]} files in $dir, found ${#all[@]}:" >&2
    ls -l "$dir" >&2
    exit 1
fi

for wheel in "$dir"/*.whl; do
    # testzip() returns the first entry whose CRC doesn't match (`python3 -m zipfile -t` exits 0 even then)
    if ! python3 -c 'import sys, zipfile; sys.exit(zipfile.ZipFile(sys.argv[1]).testzip() is not None)' "$wheel" > /dev/null 2>&1; then
        echo "Corrupt wheel: $wheel" >&2
        exit 1
    fi
done

if ! tar -tzf "$dir/redb-$version.tar.gz" > /dev/null 2>&1; then
    echo "Corrupt sdist: $dir/redb-$version.tar.gz" >&2
    exit 1
fi

ls -l "$dir"
echo "OK: $dir has all release files for redb $version"
