"""Tests for redb write transactions."""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import redb


def test_begin_write_commit(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.commit()


def test_begin_write_abort(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.abort()


def test_completed_transaction_releases_database(tmp_db_path: Path) -> None:
    # After commit(), the transaction must drop its reference to the
    # Database so the underlying file lock is released. Otherwise reopening
    # the same path while a finalized transaction is still alive would fail
    # with DatabaseAlreadyOpen.
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.commit()
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
    txn = redb.Database.create(str(tmp_db_path)).begin_write()
    txn.commit()


def test_double_commit_raises(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.commit()
    with pytest.raises(redb.TransactionCompleted) as excinfo:
        txn.commit()
    # TransactionCompleted < TransactionError < Error in the hierarchy.
    assert isinstance(excinfo.value, redb.TransactionError)
    assert isinstance(excinfo.value, redb.Error)


@given(commits=st.lists(st.booleans(), min_size=1, max_size=20))
@settings(max_examples=25, deadline=None)
def test_sequential_transactions(tmp_path_factory, commits: list) -> None:
    path = tmp_path_factory.mktemp("redb") / "txn.redb"
    db = redb.Database.create(str(path))
    for commit in commits:
        txn = db.begin_write()
        if commit:
            txn.commit()
        else:
            txn.abort()
