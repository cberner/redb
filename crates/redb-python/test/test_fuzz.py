"""Model-based fuzzing of the bindings with Hypothesis.

The state machine drives redb through the Python API and a pure-Python model side by side. It
picks tables over the closed set of types, interleaves write transactions, read snapshots,
savepoints, reopens and object lifetimes, and compares every result with the model. The `ci`
profile runs a few short examples under `just test_py`; fuzz.py loops the `fuzz` profile for a
time budget.
"""

from __future__ import annotations

import copy
import gc
import os
import tempfile
from pathlib import Path
from typing import Any, Callable

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    invariant,
    precondition,
    rule,
)

import redb

settings.register_profile("ci", max_examples=30, stateful_step_count=40, deadline=None)
settings.register_profile(
    "fuzz", max_examples=200, stateful_step_count=150, deadline=None, print_blob=True
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))

TABLES = [
    redb.TableDefinition("bytes_bytes", bytes, bytes),
    redb.TableDefinition("u64_str", redb.U64, str),
    redb.TableDefinition("str_i64", str, redb.I64),
    redb.TableDefinition("u128_f64", redb.U128, float),
    redb.TableDefinition("i64_bool", redb.I64, bool),
]
MULTIMAPS = [redb.MultimapTableDefinition("u64_bytes", redb.U64, bytes)]
BY_NAME = {d.name: d for d in TABLES + MULTIMAPS}

# Small key spaces so operations collide, mixed with the full range of each type. Values
# include ones larger than a page, which take the overflow path.
KEYS = {
    redb.Bytes: st.binary(max_size=3),
    redb.U64: st.integers(0, 12) | st.integers(0, 2**64 - 1),
    redb.Str: st.text(alphabet="abé\U0001f600", max_size=3),
    redb.I64: st.integers(-6, 6) | st.integers(-(2**63), 2**63 - 1),
    redb.U128: st.integers(0, 12) | st.integers(0, 2**128 - 1),
}
VALUES = {
    **KEYS,
    redb.Bytes: st.binary(max_size=8) | st.binary(min_size=4096, max_size=12000),
    redb.F64: st.floats(allow_nan=False),
    redb.Bool: st.booleans(),
}

Model = dict[str, dict[Any, Any]]


def _is_multimap(name: str) -> bool:
    return isinstance(BY_NAME[name], redb.MultimapTableDefinition)


def _raises(kinds: str | set[str], call: Callable[[], object]) -> None:
    """Expects a redb.Error of one of the kinds"""
    expected = {kinds} if isinstance(kinds, str) else kinds
    try:
        call()
    except redb.Error as error:
        assert error.kind in expected, error
        return
    raise AssertionError(f"expected {expected}")


def _in_range(key: Any, start: Any, end: Any) -> bool:
    return (start is None or key >= start) and (end is None or key < end)


def _entries(name: str, model: dict[Any, Any]) -> list[tuple[Any, Any]]:
    if _is_multimap(name):
        return sorted((k, sorted(v)) for k, v in model.items())
    return sorted(model.items())


def writing(machine: RedbMachine) -> bool:
    return machine.write is not None


class RedbMachine(RuleBasedStateMachine):
    reads = Bundle("reads")
    tables = st.sampled_from([d.name for d in TABLES])
    multimaps = st.sampled_from([d.name for d in MULTIMAPS])
    any_table = st.sampled_from(sorted(BY_NAME))

    def __init__(self) -> None:
        super().__init__()
        self.dir = tempfile.TemporaryDirectory()
        self.path = Path(self.dir.name) / "fuzz.redb"
        self.db: redb.Database | None = redb.Database.create(self.path)
        self.committed: Model = {}
        # Persistent savepoints: id -> (creation order, snapshot of the model)
        self.persistent: dict[int, tuple[int, Model]] = {}
        self.sequence = 0
        self.open_reads = 0
        # State of the open write transaction, if any
        self.write: redb.WriteTransaction | None = None
        self.durable = True
        self.dirty = False  # redb allows savepoints only before a transaction's first table op
        self.pending: Model = {}
        self.pending_persistent: dict[int, tuple[int, Model]] = {}
        self.handles: dict[str, Any] = {}
        self.ephemeral: list[tuple[redb.Savepoint, int, Model]] = []

    def teardown(self) -> None:
        self.handles.clear()
        self.ephemeral.clear()
        self.write = None
        self.db = None
        gc.collect()
        try:
            self.dir.cleanup()
        except OSError:
            pass  # a read transaction still in a bundle keeps the file open on Windows

    # -- helpers -----------------------------------------------------------------------------

    def _write(self) -> redb.WriteTransaction:
        """The open write transaction, beginning one with default settings if there is none"""
        if self.write is None:
            self.begin_write(durable=True, two_phase=False, quick_repair=False)
        assert self.write is not None
        return self.write

    def _table(self, name: str) -> Any:
        """The write transaction's handle on `name`, opening (and so creating) it on first use"""
        if name not in self.handles:
            self._write()
            assert self.write is not None
            if _is_multimap(name):
                self.handles[name] = self.write.open_multimap_table(BY_NAME[name])
            else:
                self.handles[name] = self.write.open_table(BY_NAME[name])
            self.pending.setdefault(name, {})
            self.dirty = True
        return self.handles[name]

    def _bounds(self, name: str, data: st.DataObject) -> tuple[Any, Any]:
        keys = KEYS[BY_NAME[name].key_type]
        return data.draw(st.none() | keys), data.draw(st.none() | keys)

    @staticmethod
    def _check_range(
        name: str, table: Any, model: dict[Any, Any], start: Any, end: Any, reverse: bool
    ) -> None:
        expected = [(k, v) for k, v in _entries(name, model) if _in_range(k, start, end)]
        if reverse:
            expected.reverse()
        assert list(table.range(start, end, reverse=reverse)) == expected

    def _end_write(self) -> None:
        self.write = None
        self.handles.clear()
        self.ephemeral.clear()
        gc.collect()

    def _restored(self, sequence: int, snapshot: Model) -> None:
        """The model after rolling back to the savepoint created at `sequence`"""
        self.pending = copy.deepcopy(snapshot)
        # Savepoints created after the restored one are invalidated, and persistent ones removed
        self.ephemeral = [s for s in self.ephemeral if s[1] <= sequence]
        self.pending_persistent = {
            i: s for i, s in self.pending_persistent.items() if s[0] <= sequence
        }
        self.handles.clear()
        self.dirty = True

    # -- write transactions ------------------------------------------------------------------

    @precondition(lambda self: self.write is None)
    @rule(durable=st.booleans(), two_phase=st.booleans(), quick_repair=st.booleans())
    def begin_write_with(self, durable: bool, two_phase: bool, quick_repair: bool) -> None:
        self.begin_write(durable, two_phase, quick_repair)

    def begin_write(self, durable: bool, two_phase: bool, quick_repair: bool) -> None:
        assert self.db is not None and self.write is None
        self.write = self.db.begin_write()
        self.write.set_durability(
            redb.Durability.IMMEDIATE if durable else redb.Durability.NONE
        )
        self.write.set_two_phase_commit(two_phase)
        self.write.set_quick_repair(quick_repair)
        self.durable = durable
        self.dirty = False
        self.pending = copy.deepcopy(self.committed)
        self.pending_persistent = dict(self.persistent)

    @precondition(writing)
    @rule()
    def commit(self) -> None:
        assert self.write is not None
        self.write.commit()
        self.committed = self.pending
        self.persistent = self.pending_persistent
        self._end_write()

    @precondition(writing)
    @rule(explicit=st.booleans())
    def abort(self, explicit: bool) -> None:
        assert self.write is not None
        if explicit:
            self.write.abort()
        self._end_write()  # dropping the last reference aborts as well

    # -- tables ------------------------------------------------------------------------------

    @rule(name=tables, data=st.data())
    def insert(self, name: str, data: st.DataObject) -> None:
        table, model = self._table(name), self.pending[name]
        key = data.draw(KEYS[BY_NAME[name].key_type])
        value = data.draw(VALUES[BY_NAME[name].value_type])
        if data.draw(st.booleans()):
            assert table.insert(key, value) == model.get(key)
        else:
            table[key] = value
        model[key] = value

    @rule(name=tables, data=st.data())
    def remove(self, name: str, data: st.DataObject) -> None:
        table, model = self._table(name), self.pending[name]
        key = data.draw(KEYS[BY_NAME[name].key_type])
        assert table.remove(key) == model.pop(key, None)

    @rule(name=tables, data=st.data())
    def get(self, name: str, data: st.DataObject) -> None:
        table, model = self._table(name), self.pending[name]
        key = data.draw(KEYS[BY_NAME[name].key_type])
        assert table.get(key) == model.get(key)
        assert (key in table) == (key in model)

    @rule(name=tables, first=st.booleans())
    def pop(self, name: str, first: bool) -> None:
        table, model = self._table(name), self.pending[name]
        expected = None
        if model:
            key = min(model) if first else max(model)
            expected = (key, model.pop(key))
        assert (table.pop_first() if first else table.pop_last()) == expected

    @rule(name=tables, data=st.data(), modulus=st.integers(1, 4), extract=st.booleans())
    def retain_or_extract(
        self, name: str, data: st.DataObject, modulus: int, extract: bool
    ) -> None:
        table, model = self._table(name), self.pending[name]
        start, end = self._bounds(name, data)
        keep = {k for i, k in enumerate(sorted(model)) if i % modulus == 0}
        seen = []

        def predicate(key: Any, value: Any) -> bool:
            seen.append(key)
            assert model[key] == value
            return (key not in keep) if extract else (key in keep)

        dropped = [(k, v) for k, v in sorted(model.items()) if _in_range(k, start, end) and k not in keep]
        if extract:
            assert table.extract_from_if(predicate, start, end) == dropped
        else:
            table.retain_in(predicate, start, end)
        assert seen == [k for k in sorted(model) if _in_range(k, start, end)]
        for key, _ in dropped:
            del model[key]

    @rule(name=tables, data=st.data(), reverse=st.booleans())
    def range(self, name: str, data: st.DataObject, reverse: bool) -> None:
        table, model = self._table(name), self.pending[name]
        start, end = self._bounds(name, data)
        self._check_range(name, table, model, start, end, reverse)

    @rule(
        name=multimaps,
        data=st.data(),
        op=st.sampled_from(["insert", "remove", "remove_all", "get", "range"]),
    )
    def multimap(self, name: str, data: st.DataObject, op: str) -> None:
        table, model = self._table(name), self.pending[name]
        definition = BY_NAME[name]
        key = data.draw(KEYS[definition.key_type])
        values = model.setdefault(key, set())
        if op == "insert":
            value = data.draw(VALUES[definition.value_type])
            assert table.insert(key, value) == (value in values)
            values.add(value)
        elif op == "remove":
            value = data.draw(VALUES[definition.value_type])
            assert table.remove(key, value) == (value in values)
            values.discard(value)
        elif op == "remove_all":
            assert table.remove_all(key) == sorted(values)
            values.clear()
        elif op == "get":
            assert table.get(key) == sorted(values)
            assert (key in table) == bool(values)
        if not values:
            del model[key]
        if op == "range":
            start, end = self._bounds(name, data)
            self._check_range(name, table, model, start, end, data.draw(st.booleans()))

    @rule(name=any_table)
    def delete_table(self, name: str) -> None:
        write = self._write()
        delete = write.delete_multimap_table if _is_multimap(name) else write.delete_table
        assert delete(name) == (name in self.pending)
        self.pending.pop(name, None)
        self.handles.clear()  # a table tree change invalidates every handle
        self.dirty = True

    # -- savepoints --------------------------------------------------------------------------

    @precondition(lambda self: len(self.ephemeral) < 3)
    @rule()
    def ephemeral_savepoint(self) -> None:
        self._write()
        assert self.write is not None
        if self.dirty:
            _raises("InvalidSavepoint", self.write.ephemeral_savepoint)
            return
        self.sequence += 1
        savepoint = self.write.ephemeral_savepoint()
        self.ephemeral.append((savepoint, self.sequence, copy.deepcopy(self.pending)))

    @rule()
    def persistent_savepoint(self) -> None:
        self._write()
        assert self.write is not None
        if self.dirty or not self.durable:
            _raises({"InvalidSavepoint", "ImmediateDurabilityRequired"}, self.write.persistent_savepoint)
            return
        self.sequence += 1
        savepoint_id = self.write.persistent_savepoint()
        self.pending_persistent[savepoint_id] = (self.sequence, copy.deepcopy(self.pending))
        assert sorted(self.write.list_persistent_savepoints()) == sorted(self.pending_persistent)

    @precondition(lambda self: self.pending_persistent if writing(self) else self.persistent)
    @rule(data=st.data())
    def delete_persistent_savepoint(self, data: st.DataObject) -> None:
        self._write()
        assert self.write is not None
        savepoint_id = data.draw(st.sampled_from(sorted(self.pending_persistent)))
        if not self.durable:
            _raises("ImmediateDurabilityRequired",
                lambda: self.write.delete_persistent_savepoint(savepoint_id),
            )
            return
        assert self.write.delete_persistent_savepoint(savepoint_id)
        del self.pending_persistent[savepoint_id]

    @precondition(
        lambda self: self.ephemeral or (self.pending_persistent if writing(self) else self.persistent)
    )
    @rule(data=st.data())
    def restore_savepoint(self, data: st.DataObject) -> None:
        self._write()
        assert self.write is not None
        choices: list[tuple[int, Model, Callable[[], redb.Savepoint]]] = [
            (sequence, snapshot, lambda s=savepoint: s)
            for savepoint, sequence, snapshot in self.ephemeral
        ] + [
            (sequence, snapshot, lambda i=savepoint_id: self.write.get_persistent_savepoint(i))
            for savepoint_id, (sequence, snapshot) in self.pending_persistent.items()
        ]
        sequence, snapshot, savepoint = data.draw(st.sampled_from(choices))
        newer_persistent = any(s[0] > sequence for s in self.pending_persistent.values())
        if not self.durable and newer_persistent:
            _raises("ImmediateDurabilityRequired",
                lambda: self.write.restore_savepoint(savepoint()),
            )
            return
        self.write.restore_savepoint(savepoint())
        self._restored(sequence, snapshot)

    # -- read transactions -------------------------------------------------------------------

    @rule(target=reads)
    def begin_read(self) -> tuple[redb.ReadTransaction, Model]:
        assert self.db is not None
        self.open_reads += 1
        return self.db.begin_read(), copy.deepcopy(self.committed)

    @rule(read=reads, name=any_table, data=st.data(), reverse=st.booleans())
    def read_check(
        self, read: tuple[redb.ReadTransaction, Model], name: str, data: st.DataObject, reverse: bool
    ) -> None:
        txn, snapshot = read
        definition = BY_NAME[name]
        open_table = txn.open_multimap_table if _is_multimap(name) else txn.open_table
        if name not in snapshot:
            _raises("TableDoesNotExist", lambda: open_table(definition))
            return
        table, model = open_table(definition), snapshot[name]
        assert isinstance(table, (redb.ReadOnlyTable, redb.ReadOnlyMultimapTable))
        key = data.draw(KEYS[definition.key_type])
        if _is_multimap(name):
            assert len(table) == sum(len(v) for v in model.values())
            assert table.get(key) == sorted(model.get(key, ()))
        else:
            assert len(table) == len(model)
            assert table.get(key) == model.get(key)
        start, end = self._bounds(name, data)
        self._check_range(name, table, model, start, end, reverse)

    @rule(read=consumes(reads))
    def close_read(self, read: tuple[redb.ReadTransaction, Model]) -> None:
        txn, _ = read
        txn.close()  # nothing else references it, so this must not fail
        self.open_reads -= 1

    # -- the database ------------------------------------------------------------------------

    @precondition(lambda self: self.write is None and self.open_reads == 0)
    @rule(compact=st.booleans())
    def reopen(self, compact: bool) -> None:
        assert self.db is not None
        if compact:
            if self.persistent:
                _raises("PersistentSavepointExists", self.db.compact)
            else:
                self.db.compact()
        self.db = None
        gc.collect()
        self.db = redb.Database.open(self.path)
        assert self.db.check_integrity()
        with self.db.begin_read() as txn:
            assert sorted(txn.list_tables()) == sorted(
                n for n in self.committed if not _is_multimap(n)
            )
            assert sorted(txn.list_multimap_tables()) == sorted(
                n for n in self.committed if _is_multimap(n)
            )
            for name, model in self.committed.items():
                open_table = txn.open_multimap_table if _is_multimap(name) else txn.open_table
                assert list(open_table(BY_NAME[name])) == _entries(name, model)

    @rule()
    def collect_garbage(self) -> None:
        gc.collect()

    @invariant()
    def handles_match_the_model(self) -> None:
        for name, table in self.handles.items():
            entries = _entries(name, self.pending[name])
            if _is_multimap(name):
                assert len(table) == sum(len(v) for _, v in entries)
            else:
                assert len(table) == len(entries)
                assert table.first() == (entries[0] if entries else None)
                assert table.last() == (entries[-1] if entries else None)
        if self.write is not None:
            assert sorted(self.write.list_tables()) == sorted(
                n for n in self.pending if not _is_multimap(n)
            )


TestRedb = RedbMachine.TestCase
