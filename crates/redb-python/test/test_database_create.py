"""Tests for redb.Database.create()."""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import redb


def test_create_new_database_creates_file(tmp_db_path: Path) -> None:
    redb.Database.create(str(tmp_db_path))
    assert tmp_db_path.is_file()
    assert tmp_db_path.stat().st_size > 0


def test_create_on_non_database_file_raises_error(tmp_db_path: Path) -> None:
    tmp_db_path.write_bytes(b"not a redb database, just some bytes")
    with pytest.raises(redb.Io) as excinfo:
        redb.Database.create(str(tmp_db_path))
    # redb.Io < redb.StorageError < redb.Error in the exception hierarchy.
    assert isinstance(excinfo.value, redb.StorageError)
    assert isinstance(excinfo.value, redb.Error)


# Windows reserves these device basenames (case-insensitive, also when
# followed by an extension), even though they contain only "safe" chars.
_WIN_RESERVED = frozenset(
    ["CON", "PRN", "AUX", "NUL"]
    + [f"COM{i}" for i in range(1, 10)]
    + [f"LPT{i}" for i in range(1, 10)]
)


def _is_portable_filename(s: str) -> bool:
    if s in (".", "..") or s.startswith(".") or s.strip() != s:
        return False
    stem = s.split(".", 1)[0]
    return stem.upper() not in _WIN_RESERVED


# Filenames: stick to a portable subset that's valid on Linux, macOS, and
# Windows. Avoid path separators, NULs, control chars, and the Windows-
# reserved punctuation (\ : * ? " < > |). Skip dotfiles and Windows device
# names to keep the on-disk layout boring.
_filename = st.text(
    alphabet=st.characters(
        whitelist_categories=("Lu", "Ll", "Lo", "Nd"),
        whitelist_characters="-_+= ",
    ),
    min_size=1,
    max_size=32,
).filter(_is_portable_filename)


@given(name=_filename)
@settings(max_examples=25, deadline=None)
def test_create_then_reopen_arbitrary_filenames(tmp_path_factory, name: str) -> None:
    path = tmp_path_factory.mktemp("redb") / name
    redb.Database.create(str(path))
    assert path.is_file()
    # Reopening the same file must succeed.
    redb.Database.create(str(path))
