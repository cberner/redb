"""Model-based fuzzing of the bindings with Hypothesis.

The state machine drives redb through the Python API and a pure-Python model side by side,
and compares every result with the model. The `ci` profile runs a few short examples under
`just test_py`; fuzz.py loops the `fuzz` profile for a time budget, see `just fuzz_py`.
"""

from __future__ import annotations

import gc
import os
import tempfile
from pathlib import Path
from typing import Any

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import Bundle, RuleBasedStateMachine, consumes, precondition, rule

import redb

settings.register_profile("ci", max_examples=30, stateful_step_count=40, deadline=None)
# Long sequences in rounds short enough for fuzz.py to check its time budget between them
settings.register_profile(
    "fuzz", max_examples=25, stateful_step_count=150, deadline=None, print_blob=True
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))

TABLES = {
    definition.name: definition
    for definition in [
        redb.TableDefinition("bytes_bytes", redb.bytes, redb.bytes),
        redb.TableDefinition("bytes_u64", redb.bytes, redb.u64),
        redb.TableDefinition("u64_bytes", redb.u64, redb.bytes),
        redb.TableDefinition("u64_u64", redb.u64, redb.u64),
        redb.TableDefinition("str_str", redb.str, redb.str),
        redb.TableDefinition("i64_bool", redb.i64, redb.bool),
        redb.TableDefinition("bool_i64", redb.bool, redb.i64),
        redb.TableDefinition("u128_i128", redb.u128, redb.i128),
        redb.TableDefinition("i128_f64", redb.i128, redb.f64),
    ]
}

# Small key spaces, so that operations collide, mixed with the full range of each type.
# Values include ones larger than a page, which take redb's overflow path.
KEYS = {
    redb.bytes: st.binary(max_size=3),
    redb.u64: st.integers(0, 12) | st.integers(0, 2**64 - 1),
    redb.str: st.text(max_size=3),
    redb.i64: st.integers(-6, 6) | st.integers(-(2**63), 2**63 - 1),
    redb.bool: st.booleans(),
    redb.u128: st.integers(0, 12) | st.integers(0, 2**128 - 1),
    redb.i128: st.integers(-6, 6) | st.integers(-(2**127), 2**127 - 1),
}
VALUES = {
    redb.bytes: st.binary(max_size=8) | st.binary(min_size=4096, max_size=12000),
    redb.u64: st.integers(0, 2**64 - 1),
    redb.str: st.text(max_size=8),
    redb.i64: st.integers(-(2**63), 2**63 - 1),
    redb.bool: st.booleans(),
    redb.u128: st.integers(0, 2**128 - 1),
    redb.i128: st.integers(-(2**127), 2**127 - 1),
    redb.f64: st.floats(allow_nan=False),  # NaN is not equal to itself, which the model needs
}

Model = dict[str, dict[Any, Any]]


def writing(machine: RedbMachine) -> bool:
    return machine.write is not None


def in_range(key: Any, start: Any, end: Any) -> bool:
    return (start is None or key >= start) and (end is None or key < end)


class RedbMachine(RuleBasedStateMachine):
    names = st.sampled_from(sorted(TABLES))
    reads = Bundle("reads")

    def __init__(self) -> None:
        super().__init__()
        self.dir = tempfile.TemporaryDirectory()
        self.path = Path(self.dir.name) / "fuzz.redb"
        self.db: redb.Database | None = redb.Database.create(self.path)
        self.committed: Model = {}
        # The open write transaction, its view of the tables, and its handles on them
        self.write: redb.WriteTransaction | None = None
        self.pending: Model = {}
        self.handles: dict[str, redb.Table] = {}
        # A handle from a transaction that was committed or aborted
        self.stale: redb.Table | None = None
        self.open_reads = 0
        # The persistent savepoints, each the committed tables as they stood, and the open
        # write transaction's view of them
        self.savepoints: dict[int, Model] = {}
        self.pending_savepoints: dict[int, Model] = {}

    def teardown(self) -> None:
        self.handles.clear()
        self.stale = self.write = self.db = None
        gc.collect()
        try:
            self.dir.cleanup()
        except OSError:
            pass  # a read transaction still in the bundle keeps the file open on Windows

    def _table(self, name: str) -> tuple[redb.Table, dict[Any, Any]]:
        """The write transaction's handle on `name`, opened on first use, with its model"""
        if self.write is None:
            self.begin_write()
        assert self.write is not None
        if name not in self.handles:
            self.handles[name] = self.write.open_table(TABLES[name])
        return self.handles[name], self.pending.setdefault(name, {})

    def _check_size(self, table: Any, model: dict[Any, Any]) -> None:
        """The length and the ends of `table` against the model"""
        assert len(table) == len(model)
        entries = sorted(model.items())
        assert table.first() == (entries[0] if entries else None)
        assert table.last() == (entries[-1] if entries else None)

    def _bounds(self, name: str, data: st.DataObject) -> tuple[Any, Any]:
        """Optional start and end keys for a range over `name`"""
        keys = KEYS[TABLES[name].key_type]
        return data.draw(st.none() | keys), data.draw(st.none() | keys)

    def _check_range(
        self, table: Any, model: dict[Any, Any], data: st.DataObject, reverse: bool
    ) -> None:
        """A range with drawn bounds against the model"""
        start, end = self._bounds(table.name, data)
        expected = [(k, v) for k, v in sorted(model.items()) if in_range(k, start, end)]
        if reverse:
            expected.reverse()
        assert list(table.range(start, end, reverse=reverse)) == expected
        if start is None and end is None and not reverse:
            assert list(table) == expected

    def _end_write(self, finished: bool) -> None:
        # A handle keeps its transaction alive, so one is kept only once it has been finished
        self.stale = next(iter(self.handles.values()), None) if finished else None
        self.write = None
        # What an aborted transaction made or deleted is gone; a committed one's were taken over
        self.pending_savepoints = dict(self.savepoints)
        self.handles.clear()
        gc.collect()

    @precondition(lambda self: self.write is None)
    @rule()
    def begin_write(self) -> None:
        assert self.db is not None
        self.write = self.db.begin_write()
        self.pending = {name: dict(table) for name, table in self.committed.items()}
        self.pending_savepoints = dict(self.savepoints)

    @precondition(writing)
    @rule()
    def commit(self) -> None:
        assert self.write is not None
        self.write.commit()
        self.committed = self.pending
        self.savepoints = self.pending_savepoints
        self._end_write(finished=True)

    @precondition(writing)
    @rule(explicit=st.booleans())
    def abort(self, explicit: bool) -> None:
        assert self.write is not None
        if explicit:
            self.write.abort()
        # Dropping the last reference to the transaction aborts it as well
        self._end_write(finished=explicit)

    @rule(name=names, data=st.data(), setitem=st.booleans())
    def insert(self, name: str, data: st.DataObject, setitem: bool) -> None:
        table, model = self._table(name)
        key = data.draw(KEYS[TABLES[name].key_type])
        value = data.draw(VALUES[TABLES[name].value_type])
        if setitem:
            table[key] = value
        else:
            assert table.insert(key, value) == model.get(key)
        model[key] = value

    @rule(name=names, data=st.data())
    def get(self, name: str, data: st.DataObject) -> None:
        table, model = self._table(name)
        key = data.draw(KEYS[TABLES[name].key_type])
        assert table.get(key) == model.get(key)
        assert (key in table) == (key in model)
        if key in model:
            assert table[key] == model[key]
        else:
            with pytest.raises(KeyError):
                table[key]

    @rule(name=names)
    def length(self, name: str) -> None:
        table, model = self._table(name)
        self._check_size(table, model)

    @rule(name=names, data=st.data(), reverse=st.booleans())
    def range(self, name: str, data: st.DataObject, reverse: bool) -> None:
        table, model = self._table(name)
        self._check_range(table, model, data, reverse)

    @rule(name=names, data=st.data())
    def change_while_iterating(self, name: str, data: st.DataObject) -> None:
        table, model = self._table(name)
        entries = iter(table)
        first = next(entries, None)
        assert first == (min(model.items()) if model else None)
        key = data.draw(KEYS[TABLES[name].key_type])
        value = data.draw(VALUES[TABLES[name].value_type])
        table[key] = value
        model[key] = value
        if first is None:
            # Exhausted before the change, which stays final, as for a dict
            assert next(entries, None) is None
        else:
            with pytest.raises(RuntimeError):
                next(entries)

    @rule(name=names, data=st.data(), delitem=st.booleans())
    def remove(self, name: str, data: st.DataObject, delitem: bool) -> None:
        table, model = self._table(name)
        key = data.draw(KEYS[TABLES[name].key_type])
        if not delitem:
            assert table.remove(key) == model.get(key)
        elif key in model:
            del table[key]
        else:
            with pytest.raises(KeyError):
                del table[key]
        model.pop(key, None)

    @rule(name=names, last=st.booleans())
    def pop_end(self, name: str, last: bool) -> None:
        table, model = self._table(name)
        entries = sorted(model.items())
        expected = (entries[-1] if last else entries[0]) if entries else None
        assert (table.pop_last() if last else table.pop_first()) == expected
        if expected is not None:
            del model[expected[0]]

    @rule(name=names, data=st.data())
    def retain(self, name: str, data: st.DataObject) -> None:
        """Drops a drawn set of keys through the predicate, which sees every entry"""
        table, model = self._table(name)
        doomed = set(data.draw(st.lists(KEYS[TABLES[name].key_type], max_size=4)))
        seen = []

        def predicate(key: Any, value: Any) -> bool:
            seen.append((key, value))
            return key not in doomed

        table.retain(predicate)
        assert sorted(seen) == sorted(model.items())
        for key in doomed:
            model.pop(key, None)

    @rule(name=names, by_definition=st.booleans())
    def delete_table(self, name: str, by_definition: bool) -> None:
        if self.write is None:
            self.begin_write()
        assert self.write is not None
        table = TABLES[name] if by_definition else name
        assert self.write.delete_table(table) == (name in self.pending)
        self.pending.pop(name, None)
        self.handles.pop(name, None)
        assert self.write.list_tables() == sorted(self.pending)

    @rule(name=names)
    def rename_table(self, name: str) -> None:
        """Renames a table away and back; the data moves with it"""
        _, model = self._table(name)
        assert self.write is not None
        definition = TABLES[name]
        renamed = redb.TableDefinition("renamed", definition.key_type, definition.value_type)
        self.write.rename_table(definition, "renamed")
        assert self.write.list_tables() == sorted((set(self.pending) - {name}) | {"renamed"})
        assert dict(self.write.open_table(renamed)) == model
        self.write.rename_table("renamed", definition)
        assert self.write.list_tables() == sorted(self.pending)

    @precondition(lambda self: self.stale is not None)
    @rule()
    def stale_handle(self) -> None:
        with pytest.raises(redb.TransactionCompleted):
            len(self.stale)

    @precondition(lambda self: self.write is None and self.open_reads == 0)
    @rule(mode=st.sampled_from([redb.SingleWriter, redb.MultiWriter]))
    def reopen_database(self, mode: redb.ConcurrencyMode) -> None:
        """Closes and reopens the file, in either mode: what was committed is what is there"""
        self.db = self.stale = None
        gc.collect()
        self.db = redb.Database.open(self.path, concurrency=mode)
        with self.db.begin_write() as txn:
            assert txn.list_persistent_savepoints() == sorted(self.savepoints)
            for name, model in self.committed.items():
                assert len(txn.open_table(TABLES[name])) == len(model)

    @precondition(lambda self: self.write is None and self.open_reads == 0)
    @rule(check=st.booleans())
    def maintain(self, check: bool) -> None:
        """Checks or compacts the file between transactions; what was committed is what is there"""
        assert self.db is not None
        if check:
            assert self.db.check_integrity() is True
        elif self.savepoints:
            with pytest.raises(redb.PersistentSavepointExists):
                self.db.compact()
        else:
            self.db.compact()
        with self.db.begin_read() as txn:
            assert txn.list_tables() == sorted(self.committed)
            for name, model in self.committed.items():
                assert dict(txn.open_table(TABLES[name])) == model

    @precondition(lambda self: self.write is None)
    @rule()
    def savepoint(self) -> None:
        """Opens a write transaction with a savepoint of what is committed"""
        self.begin_write()
        assert self.write is not None
        savepoint = self.write.persistent_savepoint()
        assert savepoint not in self.pending_savepoints
        self.pending_savepoints[savepoint] = {
            name: dict(table) for name, table in self.committed.items()
        }
        assert self.write.list_persistent_savepoints() == sorted(self.pending_savepoints)

    @precondition(lambda self: bool(self.pending_savepoints))
    @rule(data=st.data())
    def restore_savepoint(self, data: st.DataObject) -> None:
        """Puts the tables back as they were at a savepoint, which drops the later savepoints"""
        if self.write is None:
            self.begin_write()
        assert self.write is not None
        savepoint = data.draw(st.sampled_from(sorted(self.pending_savepoints)))
        self.write.restore_savepoint(savepoint)
        self.pending = {
            name: dict(table) for name, table in self.pending_savepoints[savepoint].items()
        }
        self.pending_savepoints = {
            id: tables for id, tables in self.pending_savepoints.items() if id <= savepoint
        }
        assert self.write.list_persistent_savepoints() == sorted(self.pending_savepoints)
        for name in TABLES:
            if name in self.pending:
                assert dict(self.write.open_table(TABLES[name])) == self.pending[name]
        assert self.write.list_tables() == sorted(self.pending)

    @rule(data=st.data())
    def delete_savepoint(self, data: st.DataObject) -> None:
        if self.write is None:
            self.begin_write()
        assert self.write is not None
        known = sorted(self.pending_savepoints)
        savepoint = data.draw(st.sampled_from(known) if known else st.just(0)) + data.draw(
            st.sampled_from([0, 1000])
        )
        existed = self.write.delete_persistent_savepoint(savepoint)
        assert existed == (savepoint in self.pending_savepoints)
        self.pending_savepoints.pop(savepoint, None)
        with pytest.raises(redb.InvalidSavepoint):
            self.write.restore_savepoint(savepoint)

    @rule(target=reads)
    def begin_read(self) -> tuple[redb.ReadTransaction, Model]:
        """A snapshot of what is committed now, kept for later steps to read"""
        assert self.db is not None
        self.open_reads += 1
        snapshot = {name: dict(table) for name, table in self.committed.items()}
        txn = self.db.begin_read()
        assert txn.list_tables() == sorted(snapshot)
        return txn, snapshot

    @rule(read=reads, name=names, data=st.data())
    def read_get(
        self, read: tuple[redb.ReadTransaction, Model], name: str, data: st.DataObject
    ) -> None:
        txn, snapshot = read
        if name not in snapshot:
            with pytest.raises(redb.TableDoesNotExist):
                txn.open_table(TABLES[name])
            return
        table, model = txn.open_table(TABLES[name]), snapshot[name]
        key = data.draw(KEYS[TABLES[name].key_type])
        assert table.get(key) == model.get(key)
        self._check_size(table, model)

    @rule(read=reads, name=names, data=st.data(), reverse=st.booleans())
    def read_range(
        self,
        read: tuple[redb.ReadTransaction, Model],
        name: str,
        data: st.DataObject,
        reverse: bool,
    ) -> None:
        txn, snapshot = read
        if name not in snapshot:
            return
        table, model = txn.open_table(TABLES[name]), snapshot[name]
        self._check_range(table, model, data, reverse)

    @rule(read=consumes(reads))
    def close_read(self, read: tuple[redb.ReadTransaction, Model]) -> None:
        txn, _ = read
        txn.close()
        self.open_reads -= 1
        with pytest.raises(redb.TransactionCompleted):
            txn.open_table(TABLES["u64_u64"])

    @rule()
    def collect_garbage(self) -> None:
        gc.collect()


TestRedb = RedbMachine.TestCase
