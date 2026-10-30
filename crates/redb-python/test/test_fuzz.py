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
    ]
}

# Small key spaces, so that operations collide, mixed with the full range of each type.
# Values include ones larger than a page, which take redb's overflow path.
KEYS = {
    redb.bytes: st.binary(max_size=3),
    redb.u64: st.integers(0, 12) | st.integers(0, 2**64 - 1),
}
VALUES = {
    redb.bytes: st.binary(max_size=8) | st.binary(min_size=4096, max_size=12000),
    redb.u64: st.integers(0, 2**64 - 1),
}

Model = dict[str, dict[Any, Any]]


def writing(machine: RedbMachine) -> bool:
    return machine.write is not None


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

    def _end_write(self, finished: bool) -> None:
        # A handle keeps its transaction alive, so one is kept only once it has been finished
        self.stale = next(iter(self.handles.values()), None) if finished else None
        self.write = None
        self.handles.clear()
        gc.collect()

    @precondition(lambda self: self.write is None)
    @rule()
    def begin_write(self) -> None:
        assert self.db is not None
        self.write = self.db.begin_write()
        self.pending = {name: dict(table) for name, table in self.committed.items()}

    @precondition(writing)
    @rule()
    def commit(self) -> None:
        assert self.write is not None
        self.write.commit()
        self.committed = self.pending
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
        assert len(table) == len(model)

    @precondition(lambda self: self.stale is not None)
    @rule()
    def stale_handle(self) -> None:
        with pytest.raises(redb.TransactionCompleted):
            len(self.stale)

    @precondition(lambda self: self.write is None and self.open_reads == 0)
    @rule()
    def reopen_database(self) -> None:
        """Closes and reopens the file: what was committed is what is there"""
        self.db = self.stale = None
        gc.collect()
        self.db = redb.Database.open(self.path)
        with self.db.begin_write() as txn:
            for name, model in self.committed.items():
                assert len(txn.open_table(TABLES[name])) == len(model)

    @rule(target=reads)
    def begin_read(self) -> tuple[redb.ReadTransaction, Model]:
        """A snapshot of what is committed now, kept for later steps to read"""
        assert self.db is not None
        self.open_reads += 1
        snapshot = {name: dict(table) for name, table in self.committed.items()}
        return self.db.begin_read(), snapshot

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
        assert len(table) == len(model)

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
