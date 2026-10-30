"""Example-based checks of the bindings."""

from __future__ import annotations

import gc
import threading
from pathlib import Path

import pytest

import redb

USERS = redb.TableDefinition("users", redb.u64, redb.bytes)


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


def test_table_definition() -> None:
    users = redb.TableDefinition("users", redb.u64, redb.bytes)
    assert users.name == "users"
    assert users.key_type is redb.Type.u64
    assert users.value_type is redb.bytes
    assert redb.TableDefinition("t", bytes, bytes).key_type is redb.bytes
    assert repr(users) == 'TableDefinition("users", redb.u64, redb.bytes)'
    with pytest.raises(TypeError, match="explicit width"):
        redb.TableDefinition("t", int, bytes)
    with pytest.raises(TypeError):
        redb.TableDefinition("t", "u64", bytes)
    with pytest.raises(ValueError):
        redb.TableDefinition("", bytes, bytes)


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


def test_open_requires_an_existing_database(tmp_db_path: Path) -> None:
    with pytest.raises(redb.Io):
        redb.Database.open(str(tmp_db_path))
    redb.Database.create(str(tmp_db_path))
    redb.Database.open(str(tmp_db_path))


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


@pytest.mark.parametrize(
    "finish", [redb.WriteTransaction.commit, redb.WriteTransaction.abort]
)
def test_finished_transaction_is_completed(tmp_db_path: Path, finish) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    finish(txn)
    with pytest.raises(redb.TransactionCompleted):
        txn.commit()
    with pytest.raises(redb.TransactionCompleted):
        txn.abort()
    with pytest.raises(redb.TransactionCompleted):
        with txn:
            pass


def test_exit_after_explicit_finish_is_a_noop(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    with db.begin_write() as txn:
        txn.commit()
    with pytest.raises(RuntimeError, match="boom"):
        with db.begin_write() as txn:
            txn.abort()
            raise RuntimeError("boom")


def test_blocked_writer_releases_the_gil(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    done = threading.Event()

    def writer() -> None:
        with db.begin_write():
            pass
        done.set()

    thread = threading.Thread(target=writer)
    thread.start()
    # Blocked on our transaction; this would hang if it held the GIL
    assert not done.wait(0.05)
    txn.commit()
    thread.join(10)
    assert done.is_set()


def test_open_table(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    with db.begin_write() as txn:
        users = txn.open_table(USERS)
        assert users.name == "users"
        assert len(users) == 0
        assert len(txn.open_table(USERS)) == 0
        # The same name with other types, created earlier in this transaction
        with pytest.raises(redb.TableTypeMismatch):
            txn.open_table(redb.TableDefinition("users", redb.bytes, redb.bytes))
    with pytest.raises(redb.TransactionCompleted):
        len(users)
    # ... and once the table exists in the database
    with db.begin_write() as txn:
        with pytest.raises(redb.TableTypeMismatch):
            txn.open_table(redb.TableDefinition("users", redb.bytes, redb.bytes))
        assert len(txn.open_table(USERS)) == 0


def test_table_keeps_its_transaction_alive(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    users = db.begin_write().open_table(USERS)
    assert len(users) == 0
    del users
    # Dropping the table dropped the transaction, which released the writer slot
    with db.begin_write():
        pass


def test_get_and_insert(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    with db.begin_write() as txn:
        users = txn.open_table(USERS)
        assert users.get(1) is None
        assert users.insert(1, b"alice") is None
        assert users.insert(1, b"alicia") == b"alice"
        users[2] = bytearray(b"bob")
        assert users[2] == b"bob"
        assert len(users) == 2
        assert 1 in users
        assert 3 not in users
        with pytest.raises(KeyError):
            users[3]
        with pytest.raises(TypeError):
            users[b"1"] = b"x"
        with pytest.raises(TypeError):
            users[3] = "x"
        with pytest.raises(OverflowError):
            users[-1] = b"x"
    txn = db.begin_write()
    txn.open_table(USERS)[3] = b"carol"
    txn.abort()
    with db.begin_write() as txn:
        users = txn.open_table(USERS)
        assert users.get(1) == b"alicia"  # committed
        assert 3 not in users  # aborted


def test_read_transaction(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    with db.begin_write() as txn:
        txn.open_table(USERS)[1] = b"alice"
    read = db.begin_read()
    users = read.open_table(USERS)
    assert isinstance(users, redb.ReadOnlyTable)
    assert users.name == "users"
    assert len(users) == 1
    assert users.get(1) == users[1] == b"alice"
    assert 1 in users
    assert users.get(2) is None
    assert 2 not in users
    with pytest.raises(KeyError):
        users[2]
    with pytest.raises(AttributeError):
        users.insert(2, b"bob")
    with pytest.raises(redb.TableDoesNotExist):
        read.open_table(redb.TableDefinition("missing", redb.u64, redb.bytes))
    # A snapshot: a later commit is invisible to it, and visible to a later read
    with db.begin_write() as txn:
        txn.open_table(USERS)[2] = b"bob"
    assert 2 not in users
    assert 2 in db.begin_read().open_table(USERS)
    # Closing is refused while a table is alive, and the table stays usable
    with pytest.raises(redb.ReadTransactionStillInUse):
        read.close()
    assert users[1] == b"alice"
    del users
    gc.collect()
    read.close()
    with pytest.raises(redb.TransactionCompleted):
        read.open_table(USERS)
    with db.begin_read() as read:
        users = read.open_table(USERS)
    assert users[1] == b"alice"  # the table outlives the block
    # ... and the Database object, which it keeps open: the file stays locked until it goes
    del db, read
    gc.collect()
    assert users[1] == b"alice"
    with pytest.raises(redb.DatabaseAlreadyOpen):
        redb.Database.open(str(tmp_db_path))
    del users
    gc.collect()
    redb.Database.open(str(tmp_db_path))
