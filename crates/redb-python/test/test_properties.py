"""Property tests modeled on fuzz/fuzz_targets/fuzz_redb.rs.

TODO: shadow redb against `self.reference` and add @rule()s mirroring
fuzz_redb.rs (insert/delete/get/range/commit/abort). Start with a
single table, u64 -> bytes, no savepoints, no multimap.
"""

from __future__ import annotations

import pytest
from hypothesis import settings
from hypothesis.stateful import RuleBasedStateMachine, rule

pytestmark = pytest.mark.skip(reason="redb Python API not yet implemented")


class RedbStateMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.reference: dict[int, bytes] = {}

    @rule()
    def placeholder(self) -> None:
        pass


TestRedbStateMachine = RedbStateMachine.TestCase
TestRedbStateMachine.settings = settings(max_examples=100, deadline=None)
