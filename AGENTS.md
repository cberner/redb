# Agent instructions for redb

This file tells coding agents how to work productively in this repository.

## Setup

The Rust toolchain is pinned by `rust-toolchain`, so `rustup`
will pick the right version automatically. Beyond that, install the following
tools before doing anything else:

- `just` (task runner): `cargo install --locked just`
- `cargo-deny` (license / advisory checks): `cargo install --locked cargo-deny`
- `cargo-fuzz` (fuzzing harness driver): `cargo install --locked cargo-fuzz`
- `rustfmt` and `clippy` components: `rustup component add rustfmt clippy`
- System packages: `libclang-dev` (required by transitive build deps on Linux)

The CI workflow pins specific versions; prefer those versions if you hit incompatibilities. See
`.github/workflows/ci.yml` for the exact list CI uses.

Python tests (`just test_py`) additionally need `maturin` and Python; only
install these if you are touching `crates/redb-python`.

## Before completing a task

**Always run `just test` and confirm it passes before telling the user you are done.**
This target runs the `pre` recipe first, which executes `cargo deny check licenses`,
`cargo fmt --check`, and `cargo clippy --all-targets --all-features`, and then
runs `cargo test --all-features` with `RUST_BACKTRACE=1`. If any of those fail,
fix the underlying issue — do not bypass checks.

If you are touching workspace crates beyond the main `redb` crate, run
`just test_all` instead, which also builds and tests the full workspace.

## Style guide
- Comments should be brief and focus on important invariants, architectural details, or other
  long-term relevant information. They should not contain minor implementation details of the current
  commit.

## Release notes

Changes that are significant to users should be documented in `CHANGELOG.md`. Entries should be
brief and focus on the user-facing impact of the change, not on implementation details.

## Fuzzing

`just fuzz` runs the libFuzzer-based harness under `fuzz/` (`fuzz_redb`) with
`-max_len=10000` and no sanitizer. Use it (with an appropriate time limit) when
changing on-disk format, B-tree logic, transaction/savepoint code, or anything else
where corner cases matter. Related targets: `just fuzz_ci` (60s
smoke run), `just fuzz_coverage` (HTML coverage report).

## Benchmarks

`just bench` runs `redb-bench`'s default benchmark (`redb_benchmark`). Pass a
different bench name to compare against other engines, e.g.
`just bench lmdb_benchmark`. `just bench_containerized` runs benchmarks inside
a memory-limited Docker container (see `Dockerfile.bench`).

## Design / architecture

The design doc lives at `docs/design.md` — read it before making non-trivial
changes to the storage format, transaction layer, or B-tree code. The main
crate source is in `src/`, additional workspace crates (bench harness, Python
bindings, derive macro) are under `crates/`, integration tests in `tests/`,
and fuzz targets in `fuzz/`.

## Other notes

- The repo enforces ASCII-only source: CI fails on non-ASCII characters in
  `*.rs` and `*.toml` files. Keep new code ASCII-only.
- `RUSTFLAGS=--deny warnings` is set in CI, so any new warning will break the
  build. Fix warnings rather than silencing them.
- The file format is stable; changes that affect on-disk layout require an
  upgrade path and usually a `CHANGELOG.md` entry.
