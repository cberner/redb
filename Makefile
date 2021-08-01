build: pre
	cargo build --all-targets

pre:
	cargo deny check licenses
	cargo fmt --all -- --check
	cargo clippy --all

release: pre
	cargo build --release

flamegraph:
	cargo flamegraph --bench lmdb_benchmark
	google-chrome ./flamegraph.svg

release_native: pre
	RUSTFLAGS='-C target-cpu=native' cargo build --release

publish_py: test_py
	docker pull quay.io/pypa/manylinux2014_x86_64
	docker run -it --rm -v $(shell pwd):/redb quay.io/pypa/manylinux2014_x86_64 /redb/py_publish.sh

test_py: install_py
	python3 -m unittest discover

install_py: pre
	maturin develop --cargo-extra-args="--features python"

test: pre
	cargo test

bench: pre
	cargo bench
