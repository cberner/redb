"""Tests for WriteTransaction insert/get/remove table operations."""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import redb


def test_insert_and_get(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    assert txn.insert("t", b"key", b"value") is None
    assert txn.get("t", b"key") == b"value"
    txn.commit()


def test_insert_returns_previous_value(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.insert("t", b"k", b"first")
    old = txn.insert("t", b"k", b"second")
    assert old == b"first"
    assert txn.get("t", b"k") == b"second"
    txn.commit()


def test_get_missing_key_returns_none(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    assert txn.get("t", b"absent") is None
    txn.abort()


def test_remove_returns_previous_value(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.insert("t", b"k", b"v")
    old = txn.remove("t", b"k")
    assert old == b"v"
    assert txn.get("t", b"k") is None
    txn.commit()


def test_remove_missing_key_returns_none(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    assert txn.remove("t", b"absent") is None
    txn.abort()


def test_data_persists_across_transactions(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.insert("t", b"k", b"v")
    txn.commit()

    txn2 = db.begin_write()
    assert txn2.get("t", b"k") == b"v"
    txn2.abort()


def test_aborted_write_not_visible(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.insert("t", b"k", b"v")
    txn.abort()

    txn2 = db.begin_write()
    assert txn2.get("t", b"k") is None
    txn2.abort()


def test_ops_on_completed_transaction_raise(tmp_db_path: Path) -> None:
    db = redb.Database.create(str(tmp_db_path))
    txn = db.begin_write()
    txn.commit()
    with pytest.raises(redb.TransactionCompleted):
        txn.insert("t", b"k", b"v")
    with pytest.raises(redb.TransactionCompleted):
        txn.get("t", b"k")
    with pytest.raises(redb.TransactionCompleted):
        txn.remove("t", b"k")


_bytes_kv = st.binary(min_size=0, max_size=64)


@given(pairs=st.lists(st.tuples(_bytes_kv, _bytes_kv), min_size=1, max_size=30))
@settings(max_examples=50, deadline=None)
def test_insert_matches_dict(tmp_path_factory, pairs: list) -> None:
    path = tmp_path_factory.mktemp("redb") / "ops.redb"
    db = redb.Database.create(str(path))
    reference: dict[bytes, bytes] = {}

    txn = db.begin_write()
    for key, value in pairs:
        txn.insert("t", key, value)
        reference[key] = value
    txn.commit()

    txn2 = db.begin_write()
    for key, expected in reference.items():
        assert txn2.get("t", key) == expected
    txn2.abort()
