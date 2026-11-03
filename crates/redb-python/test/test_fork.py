"""A forked child shares its parent's open database file, and the locks on it."""

from __future__ import annotations

import gc
import multiprocessing
import os
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

import redb

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")

TABLE = redb.TableDefinition("t", redb.u64, redb.u64)
MODES = [redb.SingleWriter, redb.MultiWriter]


def in_child(body: Callable[[], None]) -> None:
    """Runs `body` in a forked child, and fails if it raised"""
    pid = os.fork()
    if pid == 0:
        try:
            body()
        except BaseException:
            traceback.print_exc()
            os._exit(1)
        os._exit(0)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0


def create(path: Path, concurrency: redb.ConcurrencyMode) -> redb.Database:
    db = redb.Database.create(str(path), concurrency=concurrency)
    with db.begin_write() as txn:
        txn.open_table(TABLE)[1] = 1
    return db


@pytest.mark.parametrize("concurrency", MODES)
def test_a_child_cannot_use_what_it_inherited(
    tmp_db_path: Path, concurrency: redb.ConcurrencyMode
) -> None:
    db = create(tmp_db_path, concurrency)
    reader = redb.Database.open_read_only(str(tmp_db_path))
    read_txn = db.begin_read()
    table = read_txn.open_table(TABLE)
    entries = iter(table)
    write_txn = db.begin_write()
    write_table = write_txn.open_table(TABLE)
    write_entries = iter(write_table)
    uses: list[Callable[[], Any]] = [
        db.begin_write,
        db.begin_read,
        db.compact,
        db.check_integrity,
        reader.begin_read,
        read_txn.list_tables,
        lambda: read_txn.open_table(TABLE),
        read_txn.close,
        lambda: table[1],
        lambda: len(table),
        lambda: next(entries),
        write_txn.list_tables,
        lambda: write_txn.open_table(TABLE),
        lambda: next(write_entries),
        lambda: write_table.__setitem__(2, 2),
        write_txn.commit,
        write_txn.abort,
    ]

    def body() -> None:
        for use in uses:
            with pytest.raises(redb.UsedAfterFork):
                use()

    in_child(body)

    # The parent's own handles still work
    assert next(entries) == (1, 1)
    assert next(write_entries) == (1, 1)
    write_table[2] = 2
    write_txn.commit()
    assert table[1] == 1
    assert reader.begin_read().open_table(TABLE)[2] == 2
    del uses, entries, write_entries, table, read_txn, write_table
    assert db.check_integrity()


@pytest.mark.parametrize("concurrency", MODES)
def test_a_child_dropping_what_it_inherited_leaves_the_parent_alone(
    tmp_db_path: Path, concurrency: redb.ConcurrencyMode
) -> None:
    db = create(tmp_db_path, concurrency)
    inherited: dict[str, Any] = {"db": db, "read": db.begin_read()}
    inherited["table"] = inherited["read"].open_table(TABLE)
    inherited["entries"] = iter(inherited["table"])
    inherited["write"] = db.begin_write()
    del db

    def body() -> None:
        # The last references to them in the child, so each is dropped here
        inherited.clear()
        gc.collect()

    in_child(body)

    if concurrency == redb.SingleWriter:
        # The parent still holds the file's writer lock
        with pytest.raises(redb.DatabaseAlreadyOpen):
            redb.Database.open(str(tmp_db_path))
    inherited["write"].open_table(TABLE)[2] = 2
    inherited["write"].commit()
    db = inherited.pop("db")
    inherited.clear()
    assert db.check_integrity()
    table = db.begin_read().open_table(TABLE)
    assert (table[1], table[2]) == (1, 2)


def test_a_child_can_open_the_database_itself(tmp_db_path: Path) -> None:
    db = create(tmp_db_path, redb.SingleWriter)

    def body() -> None:
        # Its own handle sees the parent as another process
        with pytest.raises(redb.DatabaseAlreadyOpen):
            redb.Database.open(str(tmp_db_path))
        reader = redb.Database.open_read_only(str(tmp_db_path))
        assert reader.begin_read().open_table(TABLE)[1] == 1

    in_child(body)
    assert db.begin_read().open_table(TABLE)[1] == 1


# A module-level database, as a program sharing one with its worker processes would have
_inherited: redb.Database | None = None


def commit_inherited(key: int) -> None:
    """In a forked worker: a commit through the parent's database"""
    assert _inherited is not None
    with _inherited.begin_write() as txn:
        txn.open_table(TABLE)[key] = key


def test_a_fork_worker_is_refused_the_parent_database(
    tmp_db_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = create(tmp_db_path, redb.SingleWriter)
    monkeypatch.setitem(globals(), "_inherited", db)
    with multiprocessing.get_context("fork").Pool(1) as pool:
        with pytest.raises(redb.UsedAfterFork):
            pool.apply(commit_inherited, (2,))
    with db.begin_write() as txn:
        txn.open_table(TABLE)[3] = 3
    assert db.check_integrity()
    table = db.begin_read().open_table(TABLE)
    assert (table[1], table.get(2), table[3]) == (1, None, 3)
