"""Several processes sharing one file, in the concurrency modes that allow it."""

from __future__ import annotations

import multiprocessing as mp
import time
from pathlib import Path

import pytest

import redb

COUNTER = redb.TableDefinition("counter", redb.U64, redb.U64)
OWNED = redb.TableDefinition("owned", redb.U64, redb.U64)
SHARED_MODES = ["SINGLE_WRITER", "MULTI_WRITER"]


# Workers are module-level functions, and take the mode by name, so they pickle under spawn
def _follow(path: str, mode: str, target: int, result: mp.Queue) -> None:  # type: ignore[type-arg]
    db = redb.ReadOnlyDatabase.open(path, concurrency_mode=getattr(redb.ConcurrencyMode, mode))
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        value = db.begin_read().open_table(COUNTER).get(0)
        if value == target:
            result.put(value)
            return
    result.put(-1)


def _write_range(path: str, start: int, count: int) -> None:
    db = redb.Database.open(path, concurrency_mode=redb.ConcurrencyMode.MULTI_WRITER)
    for i in range(start, start + count):
        with db.begin_write() as txn:
            txn.open_table(OWNED)[i] = i


@pytest.mark.parametrize("mode", SHARED_MODES)
def test_readers_follow_the_writer(tmp_db_path: Path, mode: str) -> None:
    db = redb.Database.create(tmp_db_path, concurrency_mode=getattr(redb.ConcurrencyMode, mode))
    with db.begin_write() as txn:
        txn.open_table(COUNTER)[0] = 0
    ctx = mp.get_context("spawn")
    result = ctx.Queue()
    reader = ctx.Process(target=_follow, args=(str(tmp_db_path), mode, 50, result))
    reader.start()
    for i in range(1, 51):
        with db.begin_write() as txn:
            txn.open_table(COUNTER)[0] = i
    assert result.get(timeout=120) == 50
    reader.join(10)
    assert reader.exitcode == 0


def test_writers_in_several_processes(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path, concurrency_mode=redb.ConcurrencyMode.MULTI_WRITER)
    ctx = mp.get_context("spawn")
    writers = [
        ctx.Process(target=_write_range, args=(str(tmp_db_path), start, 25))
        for start in (0, 25, 50)
    ]
    for writer in writers:
        writer.start()
    for writer in writers:
        writer.join(120)
        assert writer.exitcode == 0
    assert [k for k, _ in db.begin_read().open_table(OWNED)] == list(range(75))
