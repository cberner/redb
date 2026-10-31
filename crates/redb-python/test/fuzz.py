"""Loops the Hypothesis state machine in test_fuzz.py for a time budget; see `just fuzz_py`.

A failure is shrunk to a minimal sequence of steps and saved in the Hypothesis example
database, so the next run of `just test_py` replays it.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=600.0)
    args = parser.parse_args()
    os.environ.setdefault("HYPOTHESIS_PROFILE", "fuzz")
    sys.path.insert(0, str(Path(__file__).resolve().parent))

    from hypothesis import HealthCheck, settings
    from hypothesis.stateful import run_state_machine_as_test

    from test_fuzz import RedbMachine

    config = settings(suppress_health_check=list(HealthCheck))
    deadline = time.monotonic() + args.seconds
    rounds = 0
    while time.monotonic() < deadline:
        run_state_machine_as_test(RedbMachine, settings=config)
        rounds += 1
        print(f"round {rounds}: {config.max_examples} examples passed", flush=True)


if __name__ == "__main__":
    main()
