#!/bin/bash

PYTHON3=/opt/python/cp311-cp311/bin/python3

cp -r /redb-ro /redb
cd /redb
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain=1.61.0
source $HOME/.cargo/env

cd /tmp
$PYTHON3 -m venv venv
cd /redb
source /tmp/venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install maturin

python3 -m maturin publish
