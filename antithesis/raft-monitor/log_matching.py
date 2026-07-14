"""Log-matching consumer.

Enforces the Log Matching invariant from Figure 3 of the Raft paper: if two
logs contain an entry at the same index with the same term, those entries
store the same command. A violation can lurk in uncommitted tails for many
cluster lifetimes before causing observable damage at the FSM layer, so the
check fires directly on log content as entries are stored.

Filter is `message == "appended entries to log store"` (raft.go, both
`dispatchLogs()` on the leader path and `appendEntries()` on the follower
path). That log carries `last-index`, `new-entries`, and an `entries`
field whose value is a JSON-encoded list of `{"t": term, "c": crc32}`
objects in append order, so each entry's index is recovered as
`last-index - new-entries + 1 + offset`.

State is per-source `log[idx] = (term, crc)` and is repaired by the
`"clearing log suffix"` truncation log. Without that repair, a truncated
old-term entry could be compared against a peer's newer entry at the same
index and yield a false positive — Log Matching is about what's currently
in the log, not what used to be.
"""

from __future__ import annotations

import json
from datetime import datetime

from antithesis.assertions import always_or_unreachable

from utils import coerce_int


_MSG_APPEND = "appended entries to log store"
_MSG_CLEAR = "clearing log suffix"


class LogMatchingProperty:
    def __init__(self) -> None:
        # source -> idx -> (term, crc32)
        self._log: dict[str, dict[int, tuple[int, int]]] = {}

    def consume(
        self,
        level: str,
        message: str,
        source: str,
        timestamp: datetime,
        fields: dict,
    ) -> None:
        if message == _MSG_APPEND:
            self._on_append(source, fields)
        elif message == _MSG_CLEAR:
            self._on_clear(source, fields)

    def _on_append(self, source: str, fields: dict) -> None:
        entries_json = fields.get("entries")
        last_index = coerce_int(fields.get("last-index"))
        new_entries = coerce_int(fields.get("new-entries"))
        if not isinstance(entries_json, str) or last_index is None or new_entries is None:
            return
        try:
            entries = json.loads(entries_json)
        except (json.JSONDecodeError, TypeError):
            return
        if not isinstance(entries, list) or len(entries) != new_entries:
            return
        start_index = last_index - new_entries + 1
        src_log = self._log.setdefault(source, {})
        for offset, raw in enumerate(entries):
            if not isinstance(raw, dict):
                continue
            term = coerce_int(raw.get("t"))
            crc = coerce_int(raw.get("c"))
            if term is None or crc is None:
                continue
            idx = start_index + offset
            self._check_peers(source, idx, term, crc)
            src_log[idx] = (term, crc)

    def _on_clear(self, source: str, fields: dict) -> None:
        from_idx = coerce_int(fields.get("from"))
        to_idx = coerce_int(fields.get("to"))
        if from_idx is None or to_idx is None:
            return
        src_log = self._log.get(source)
        if not src_log:
            return
        for idx in range(from_idx, to_idx + 1):
            src_log.pop(idx, None)

    def _check_peers(self, source: str, idx: int, term: int, crc: int) -> None:
        for peer, peer_log in self._log.items():
            if peer == source:
                continue
            existing = peer_log.get(idx)
            if existing is None:
                continue
            peer_term, peer_crc = existing
            if peer_term != term:
                # Different terms at the same index is permitted by the
                # protocol — the AE conflict-truncation rule fixes it.
                continue
            always_or_unreachable(
                peer_crc == crc,
                "[log-matching] entries agreeing on (index, term) agree on content",
                {
                    "slug": "log-matching",
                    "index": idx,
                    "term": term,
                    "observations": {
                        source: {"crc": crc},
                        peer: {"crc": peer_crc},
                    },
                },
            )
            return
