#!/bin/bash

cd /redb
yum install -y python3-pip
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain=1.46.0
source $HOME/.cargo/env

pip3 install toml
pip3 install maturin

# xargs is just to merge the lines together into a single line
maturin publish --cargo-extra-args="--features python" \
 -i $(ls -1 /opt/python/*/bin/python3 | xargs | sed 's/ / -i /g')
