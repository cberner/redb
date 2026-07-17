#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

readonly COMMAND="${1:-}"
if (( $# != 1 )) || [[ -z "$COMMAND" ]]; then
    echo "Usage: $0 COMMAND" >&2
    exit 2
fi

readonly IMAGE="localhost/redb-sandbox:latest"
readonly RUST_VERSION="$(<rust-toolchain)"
readonly TARGET_VOLUME="redb-sandbox-target"

if [[ ! "$RUST_VERSION" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
    echo "Invalid Rust version in rust-toolchain: $RUST_VERSION" >&2
    exit 1
fi

podman build \
    --file Containerfile.sandbox \
    --network=private \
    --pull=missing \
    --build-arg "RUST_VERSION=$RUST_VERSION" \
    --tag "$IMAGE" \
    .

podman volume create --ignore "$TARGET_VOLUME" >/dev/null

podman run --rm \
    --tty \
    --network=none \
    --read-only \
    --cap-drop=all \
    --security-opt=no-new-privileges \
    --pids-limit=2048 \
    --memory=8g \
    --userns=keep-id \
    --user="$(id -u):$(id -g)" \
    --http-proxy=false \
    --tmpfs=/tmp:rw,nodev,nosuid,size=2g \
    --volume="$TARGET_VOLUME:/target:rw" \
    --env=CARGO_BUILD_JOBS=4 \
    --env=CARGO_HOME=/tmp/cargo \
    --env=CARGO_NET_OFFLINE=true \
    --env=CARGO_TARGET_DIR=/target \
    --env=HOME=/tmp/home \
    "$IMAGE" \
    bash -c "$COMMAND"
