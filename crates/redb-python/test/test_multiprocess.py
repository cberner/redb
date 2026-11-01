"""Checks across real processes, which is how Python programs mostly share a database."""

from __future__ import annotations

import multiprocessing
from collections.abc import Iterator
from multiprocessing.pool import Pool
from pathlib import Path

import pytest

import redb

USERS = redb.TableDefinition("users", redb.u64, redb.bytes)


def read(path: str, key: int) -> bytes | None:
    """In a child process: what a reader sees at `key`"""
    reader = redb.Database.open_read_only(path)
    return reader.begin_read().open_table(USERS).get(key)


def write(path: str, key: int, value: bytes) -> None:
    """In a child process: a MultiWriter writer storing `value` at `key`"""
    db = redb.Database.open(path, concurrency=redb.MultiWriter)
    with db.begin_write() as txn:
        txn.open_table(USERS)[key] = value


@pytest.fixture
def child() -> Iterator[Pool]:
    """One child process, started afresh rather than forked, as on every platform"""
    with multiprocessing.get_context("spawn").Pool(1) as pool:
        yield pool


def test_reader_process_follows_the_writer(tmp_db_path: Path, child: Pool) -> None:
    db = redb.Database.create(str(tmp_db_path))
    with db.begin_write() as txn:
        txn.open_table(USERS)[1] = b"alice"
    assert child.apply(read, (str(tmp_db_path),1)) == b"alice"
    assert child.apply(read, (str(tmp_db_path),2)) is None
    with db.begin_write() as txn:
        txn.open_table(USERS)[2] = b"bob"
    assert child.apply(read, (str(tmp_db_path),2)) == b"bob"


def test_writer_processes_take_turns(tmp_db_path: Path, child: Pool) -> None:
    db = redb.Database.create(str(tmp_db_path), concurrency=redb.MultiWriter)
    child.apply(write, (str(tmp_db_path), 1, b"alice"))
    assert db.begin_read().open_table(USERS)[1] == b"alice"
    # The other process's write transaction waits for this one to finish
    txn = db.begin_write()
    txn.open_table(USERS)[2] = b"bob"
    pending = child.apply_async(write, (str(tmp_db_path), 3, b"carol"))
    with pytest.raises(multiprocessing.TimeoutError):
        pending.get(timeout=1)
    txn.commit()
    pending.get(timeout=30)
    users = db.begin_read().open_table(USERS)
    assert (users[2], users[3]) == (b"bob", b"carol")
    assert child.apply(read, (str(tmp_db_path), 3)) == b"carol"
