build: pre
	cargo build --all-targets

pre:
	cargo deny check licenses
	cargo fmt --all -- --check
	cargo clippy --all --all-targets

release: pre
	cargo build --release

flamegraph:
	cargo flamegraph --bench lmdb_benchmark
	firefox ./flamegraph.svg

release_native: pre
	RUSTFLAGS='-C target-cpu=native' cargo build --release

publish_py: test_py
	docker pull quay.io/pypa/manylinux2014_x86_64
	docker run -it --rm -v $(shell pwd):/redb quay.io/pypa/manylinux2014_x86_64 /redb/py_publish.sh

test_py: install_py
	python3 -m unittest discover

install_py: pre
	maturin develop

test: pre
	RUST_BACKTRACE=1 cargo test

bench: pre
	cargo bench

fuzz: pre
	# Nightly version selected from: https://rust-lang.github.io/rustup-components-history/
	rustup toolchain install nightly-2022-03-21
	cargo +nightly-2022-03-21 fuzz run fuzz_redb
