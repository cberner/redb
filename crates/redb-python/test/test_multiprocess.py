"""Multi-process tests for read-only concurrent access (issue #19)."""

from __future__ import annotations

import multiprocessing as mp
import sys
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.skip(reason="redb Python API not yet implemented"),
    pytest.mark.skipif(
        sys.platform == "win32",
        reason="File locking semantics differ on Windows",
    ),
]


# Worker functions must be module-level so they pickle under spawn.
def _reader_worker(path: str, result_queue: "mp.Queue[object]") -> None:
    raise NotImplementedError(
        "open ReadOnlyDatabase(path), read a known table, queue.put(rows)"
    )


def _spawn_readers(path: Path, count: int) -> list[tuple[str, object]]:
    ctx = mp.get_context("spawn")
    queue: "mp.Queue[object]" = ctx.Queue()
    procs = [
        ctx.Process(target=_reader_worker, args=(str(path), queue))
        for _ in range(count)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30)
        assert p.exitcode == 0, f"reader exited with {p.exitcode}"
    return [queue.get_nowait() for _ in procs]


def test_concurrent_read_only_opens(tmp_db_path: Path) -> None:
    pytest.skip("Depends on ReadOnlyDatabase open + table read in worker")


def test_second_writer_is_rejected(tmp_db_path: Path) -> None:
    pytest.skip("Depends on Database / DatabaseAlreadyOpen exception")


def test_reader_sees_consistent_snapshot(tmp_db_path: Path) -> None:
    pytest.skip("Depends on WriteTransaction and ReadTransaction")


def test_lock_released_after_writer_crash(tmp_db_path: Path) -> None:
    pytest.skip("Depends on Database open + SIGKILL recovery")
