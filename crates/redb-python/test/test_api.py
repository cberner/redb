"""Example-based checks of the bindings."""

from __future__ import annotations

from pathlib import Path

import pytest

import redb


def test_errors_form_a_flat_hierarchy() -> None:
    assert redb.Error.__bases__ == (Exception,)
    subclasses = [
        cls
        for cls in vars(redb).values()
        if isinstance(cls, type) and issubclass(cls, redb.Error)
    ]
    subclasses.remove(redb.Error)
    assert redb.Io in subclasses
    assert redb.TransactionCompleted in subclasses
    for cls in subclasses:
        assert cls.__bases__ == (redb.Error,)


def test_create_new_database_creates_file(tmp_db_path: Path) -> None:
    redb.Database.create(str(tmp_db_path))
    assert tmp_db_path.is_file()
    assert tmp_db_path.stat().st_size > 0


def test_create_on_non_database_file_raises(tmp_db_path: Path) -> None:
    tmp_db_path.write_bytes(b"not a redb database, just some bytes")
    with pytest.raises(redb.Io):
        redb.Database.create(str(tmp_db_path))


def test_create_on_open_database_raises(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    with pytest.raises(redb.DatabaseAlreadyOpen) as excinfo:
        redb.Database.create(str(tmp_db_path))
    assert "already open" in str(excinfo.value)
    del db


def test_begin_write_commit(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    with db.begin_write():
        pass


def test_begin_write_abort(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    with pytest.raises(RuntimeError, match="rollback"):
        with db.begin_write():
            raise RuntimeError("rollback")


def test_completed_transaction_releases_database(tmp_db_path: Path) -> None:
    # After the `with` block exits, the transaction must drop its reference
    # to the Database so the underlying file lock is released. Otherwise
    # reopening the same path while a finalized transaction is still alive
    # would fail with DatabaseAlreadyOpen.
    db = redb.Database.create(str(tmp_db_path))
    with db.begin_write() as txn:
        pass
    del db
    redb.Database.create(str(tmp_db_path))
    assert txn is not None  # keep the finalized txn alive past the reopen


def test_drop_unfinalized_transaction_does_not_deadlock(tmp_db_path: Path) -> None:
    # If an unfinalized transaction is the last owner of the Database,
    # dropping the transaction must release its writer slot before the
    # Database is finalized. Otherwise Database::drop's own begin_write()
    # would self-deadlock waiting for this writer.
    txn = redb.Database.create(str(tmp_db_path)).begin_write()
    del txn


def test_transaction_outlives_anonymous_database(tmp_db_path: Path) -> None:
    # The originating Database has no Python reference, only the
    # transaction does. The transaction must keep the Database alive,
    # otherwise Database.__del__ would block forever waiting for this
    # writer to finish.
    with redb.Database.create(str(tmp_db_path)).begin_write():
        pass


def test_reentering_finalized_transaction_raises(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    with txn:
        pass
    with pytest.raises(redb.TransactionCompleted):
        with txn:
            pass


def test_nested_with_block_raises(tmp_db_path: Path) -> None:
    # Re-entering a transaction that's already inside a `with` block must
    # raise rather than panic across the FFI boundary.
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    with pytest.raises(redb.TransactionCompleted):
        with txn:
            with txn:
                pass


def test_context_manager_aborts_on_exception(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    with pytest.raises(RuntimeError, match="boom"):
        with txn:
            raise RuntimeError("boom")
    # The transaction is finalized; re-entering must fail.
    with pytest.raises(redb.TransactionCompleted):
        with txn:
            pass
