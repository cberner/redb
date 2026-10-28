"""Example-based checks of each binding. The state machine in test_fuzz.py covers behavior."""

from __future__ import annotations

import contextlib
import gc
import threading
import time
from pathlib import Path
from typing import Iterator

import pytest

import redb


@contextlib.contextmanager
def raises(kind: str) -> Iterator[None]:
    """Expects a redb.Error whose kind is `kind`"""
    with pytest.raises(redb.Error) as excinfo:
        yield
    assert excinfo.value.kind == kind, excinfo.value

USERS = redb.TableDefinition("users", redb.U64, redb.Str)
BLOBS = redb.TableDefinition("blobs", bytes, bytes)
TAGS = redb.MultimapTableDefinition("tags", redb.Str, redb.U64)


def test_definitions() -> None:
    assert USERS.key_type == redb.Type.U64
    assert redb.TableDefinition("t", str, float).value_type == redb.F64
    assert repr(BLOBS) == "TableDefinition(\"blobs\", redb.Bytes, redb.Bytes)"
    with pytest.raises(TypeError):
        redb.TableDefinition("t", float, bytes)
    with pytest.raises(TypeError):
        redb.MultimapTableDefinition("t", bytes, float)
    with pytest.raises(TypeError, match="explicit width"):
        redb.TableDefinition("t", int, bytes)
    with pytest.raises(ValueError):
        redb.TableDefinition("", bytes, bytes)


def test_write_then_read(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        users = txn.open_table(USERS)
        assert isinstance(users, redb.Table)
        assert users.insert(1, "alice") is None
        assert users.insert(1, "alicia") == "alice"
        users[2] = "bob"
        assert len(users) == 2
        assert 2 in users and 3 not in users
        assert users.first() == (1, "alicia")
        assert users.last() == (2, "bob")
    read = db.begin_read()
    users = read.open_table(USERS)
    assert isinstance(users, redb.ReadOnlyTable)
    assert users.get(1) == "alicia"
    assert users[2] == "bob"
    assert users.get(3) is None
    with pytest.raises(KeyError):
        users[3]
    with pytest.raises(AttributeError):
        users.insert(3, "carol")
    assert list(users) == [(1, "alicia"), (2, "bob")]


def test_value_conversion(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        users = txn.open_table(USERS)
        with pytest.raises(OverflowError):
            users.insert(-1, "negative")
        with pytest.raises(TypeError):
            users.insert("1", "string key")
        with pytest.raises(TypeError):
            users.insert(1, b"bytes value")
        wide = txn.open_table(redb.TableDefinition("wide", redb.U128, redb.I64))
        wide.insert(2**128 - 1, -(2**63))
        assert wide.get(2**128 - 1) == -(2**63)
        with pytest.raises(OverflowError):
            wide.insert(2**128, 0)
        flags = txn.open_table(redb.TableDefinition("flags", bytes, bool))
        flags[bytearray(b"k")] = True
        assert flags[b"k"] is True


def test_table_errors(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        txn.open_table(USERS)
    with raises("TableTypeMismatch"):
        with db.begin_write() as txn:
            txn.open_table(redb.TableDefinition("users", redb.Str, redb.Str))
    read = db.begin_read()
    with raises("TableDoesNotExist"):
        read.open_table(BLOBS)
    with raises("TableIsNotMultimap"):
        read.open_multimap_table(redb.MultimapTableDefinition("users", redb.U64, redb.Str))


def test_ranges(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    key = lambda i: i.to_bytes(2, "big")  # noqa: E731
    with db.begin_write() as txn:
        table = txn.open_table(BLOBS)
        for i in range(1000):
            table.insert(key(i), bytes([i % 256]))
        # Write-side iteration goes through the batched cursor, so cross batch boundaries
        assert [k for k, _ in table.range(key(5), key(512))] == [key(i) for i in range(5, 512)]
        assert [k for k, _ in table.range(key(768), reverse=True)] == [
            key(i) for i in range(999, 767, -1)
        ]
        with pytest.raises(TypeError):
            table.range("not bytes")
        # Changing a table under its iterator raises, as for a dict; other tables are fine
        iterator = table.range(key(990))
        assert next(iterator) == (key(990), bytes([990 % 256]))
        txn.open_table(USERS)[1] = "a"
        assert next(iterator) == (key(991), bytes([991 % 256]))
        table.insert(key(1000), b"")
        with pytest.raises(RuntimeError, match="changed during iteration"):
            next(iterator)
    read = db.begin_read()
    table = read.open_table(BLOBS)
    assert len(list(table.range())) == 1001
    assert [k for k, _ in table.range(end=key(3))] == [key(0), key(1), key(2)]
    assert [v for _, v in table.range(key(255), key(257), reverse=True)] == [b"\x00", b"\xff"]


def test_pop_retain_extract(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        t = txn.open_table(USERS)
        for i in range(10):
            t[i] = str(i)
        assert t.pop_first() == (0, "0")
        assert t.pop_last() == (9, "9")
        t.retain(lambda k, v: k % 2 == 0)
        assert [k for k, _ in t] == [2, 4, 6, 8]
        assert t.extract_if(lambda k, v: k > 5) == [(6, "6"), (8, "8")]
        t.retain_in(lambda k, v: False, 3, 5)
        assert [k for k, _ in t] == [2]
        assert t.extract_from_if(lambda k, v: True, 0, 2) == []
        assert t.remove(2) == "2"
        assert t.remove(2) is None
        with pytest.raises(KeyError):
            del t[2]
        assert t.pop_first() is None


def test_callbacks(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        t = txn.open_table(USERS)
        t[1] = "a"
        t[2] = "b"

        def boom(k: int, v: str) -> bool:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            t.retain(boom)
        assert len(t) == 2
        # The table is open for the duration of the callback, as in Rust
        with raises("TableAlreadyOpen"):
            t.retain(lambda k, v: t.get(k) is None)
        # Finalizing the transaction from inside a callback is refused, not deadlocked
        with raises("TableAlreadyOpen"):
            t.retain(lambda k, v: txn.commit())
        assert len(t) == 2
    assert db.begin_read().open_table(USERS).get(2) == "b"


def test_transaction_lifecycle(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    txn = db.begin_write()
    txn.open_table(USERS)[1] = "a"
    txn.abort()
    with raises("TransactionCompleted"):
        txn.commit()
    with raises("TransactionCompleted"):
        with txn:
            pass
    with pytest.raises(RuntimeError, match="rollback"):
        with db.begin_write() as txn:
            txn.open_table(USERS)[1] = "a"
            raise RuntimeError("rollback")
    assert db.begin_read().list_tables() == []
    with db.begin_write() as txn:
        txn.open_table(USERS)[1] = "a"
        txn.commit()  # an explicit commit inside the block is fine
    assert db.begin_read().open_table(USERS).get(1) == "a"


def test_handles_keep_their_owners_alive(tmp_db_path: Path) -> None:
    # A table keeps its transaction alive, and an unfinalized transaction that is the last
    # owner of the database must not deadlock the database's teardown
    table = redb.Database.create(tmp_db_path).begin_write().open_table(USERS)
    table[1] = "a"
    assert table.get(1) == "a"
    del table
    gc.collect()
    db = redb.Database.create(tmp_db_path)  # the file lock was released
    assert db.begin_read().list_tables() == []  # and the write was never committed


def test_read_transaction_close(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        txn.open_table(USERS)[1] = "a"
    read = db.begin_read()
    table = read.open_table(USERS)
    with raises("ReadTransactionStillInUse"):
        read.close()
    assert table.get(1) == "a"  # still usable after the refused close
    del table
    gc.collect()
    read.close()
    with raises("TransactionCompleted"):
        read.open_table(USERS)
    with db.begin_read() as read:
        table = read.open_table(USERS)
    assert table.get(1) == "a"  # the table outlives the block


def test_table_management(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        txn.open_table(USERS)[1] = "a"
        txn.open_multimap_table(TAGS).insert("x", 1)
        assert txn.list_tables() == ["users"]
        assert txn.list_multimap_tables() == ["tags"]
        txn.rename_table(USERS, "people")
        assert txn.list_tables() == ["people"]
        assert txn.delete_table("people")
        assert not txn.delete_table("people")
        txn.rename_multimap_table("tags", "labels")
        assert txn.delete_multimap_table("labels")
        assert txn.list_tables() == [] and txn.list_multimap_tables() == []
    with db.begin_write() as txn:
        table = txn.open_table(USERS)
        txn.delete_table(USERS)
        with raises("TableDoesNotExist"):
            table.get(1)


def test_multimap(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        tags = txn.open_multimap_table(TAGS)
        assert tags.insert("a", 2) is False
        assert tags.insert("a", 1) is False
        assert tags.insert("a", 1) is True
        tags.insert("b", 3)
        assert tags.get("a") == [1, 2]
        assert tags["c"] == []
        assert "a" in tags and "c" not in tags
        assert len(tags) == 3
        assert list(tags) == [("a", [1, 2]), ("b", [3])]
        assert list(tags.range("b")) == [("b", [3])]
        assert list(tags.range(end="b", reverse=True)) == [("a", [1, 2])]
        assert tags.remove("a", 2) is True
        assert tags.remove("a", 2) is False
        assert tags.remove_all("a") == [1]
        assert list(tags) == [("b", [3])]
        assert tags.stats().tree_height >= 1
    read = db.begin_read()
    tags = read.open_multimap_table(TAGS)
    assert isinstance(tags, redb.ReadOnlyMultimapTable) and tags.get("b") == [3]
    with pytest.raises(AttributeError):
        tags.insert("b", 4)


def test_savepoints(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        txn.open_table(USERS)[1] = "a"
    with db.begin_write() as txn:
        savepoint = txn.ephemeral_savepoint()
        t = txn.open_table(USERS)
        t[2] = "b"
        with raises("InvalidSavepoint"):
            txn.persistent_savepoint()  # only before the transaction's first table operation
        txn.restore_savepoint(savepoint)
        with raises("TableDoesNotExist"):
            t.get(1)  # handles from before the restore must be reopened
        assert list(txn.open_table(USERS)) == [(1, "a")]
    with db.begin_write() as txn:
        savepoint_id = txn.persistent_savepoint()
        assert txn.list_persistent_savepoints() == [savepoint_id]
        txn.open_table(USERS)[3] = "c"
    with db.begin_write() as txn:
        txn.open_table(USERS)[4] = "d"
        txn.restore_savepoint(txn.get_persistent_savepoint(savepoint_id))
        assert list(txn.open_table(USERS)) == [(1, "a")]
        assert txn.delete_persistent_savepoint(savepoint_id)
    with raises("InvalidSavepoint"):
        with db.begin_write() as txn:
            txn.get_persistent_savepoint(savepoint_id)


def test_durability_stats_and_maintenance(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path, cache_size=1 << 20)
    with db.begin_write() as txn:
        txn.set_durability(redb.Durability.NONE)
        txn.set_two_phase_commit(True)
        txn.set_quick_repair(True)
        txn.open_table(USERS)[1] = "a"
        assert txn.open_table(USERS).stats().stored_bytes > 0
        assert txn.stats().page_size > 0
        with raises("ImmediateDurabilityRequired"):
            txn.persistent_savepoint()
    assert db.check_integrity()
    assert db.compact() in (True, False)
    assert db.begin_read().open_table(USERS).get(1) == "a"


def test_open_and_read_only(tmp_db_path: Path) -> None:
    with raises("Io"):
        redb.Database.open(tmp_db_path)
    db = redb.Database.create(tmp_db_path)
    with db.begin_write() as txn:
        txn.open_table(USERS)[1] = "a"
    with raises("DatabaseAlreadyOpen"):
        redb.ReadOnlyDatabase.open(tmp_db_path)
    del db
    gc.collect()
    read_only = redb.ReadOnlyDatabase.open(tmp_db_path)
    assert read_only.begin_read().open_table(USERS).get(1) == "a"
    with raises("DatabaseAlreadyOpen"):
        redb.Database.open(tmp_db_path)


def test_blocked_writer_releases_the_gil(tmp_db_path: Path) -> None:
    db = redb.Database.create(tmp_db_path)
    txn = db.begin_write()
    done = threading.Event()

    def writer() -> None:
        with db.begin_write() as other:
            other.open_table(USERS)[2] = "b"
        done.set()

    thread = threading.Thread(target=writer)
    thread.start()
    time.sleep(0.05)
    assert not done.is_set()  # waiting on our transaction, without holding the GIL
    txn.open_table(USERS)[1] = "a"
    txn.commit()
    thread.join(10)
    assert done.is_set()
    assert len(db.begin_read().open_table(USERS)) == 2
