"""Leader-completeness consumer.

Enforces Leader Completeness from Figure 3 of the Raft paper: if a log entry
is committed in a given term, that entry must be present in the logs of
every leader for all higher terms. A violation means an acknowledged write
was lost from a client's perspective, silently — a new leader was allowed to
take over without already holding every previously-committed entry.

Three streams drive the check:

- ``"appended entries to log store"`` (raft.go ``dispatchLogs()`` and
  ``appendEntries()``) — builds per-node ``log[idx] = (term, crc)``,
  joining ``last-index`` / ``new-entries`` with the JSON-encoded ``entries``
  payload to recover per-entry indices.

- ``"clearing log suffix"`` (raft.go follower-side conflict truncation) —
  repairs per-node log state so a truncated entry isn't mistakenly compared
  later as if it were still in the log.

- ``"advanced commitIndex"`` (raft.go runLeader and the AE handler) — the
  leader-side advances fix the committed content at each freshly-committed
  index. First-writer-wins: the first leader to advance commit over an
  index pins that index's canonical content for all higher terms. The
  emission carries ``term`` (the leader's currentTerm at commit time);
  the consumer records this as the commit term and uses it to scope the
  Leader Completeness check.

- ``"election won"`` (raft.go ``runCandidate()``) — when a new leader wins,
  every committed entry must be present at its expected ``(term, crc)`` in
  the new leader's log, or covered by an earlier snapshot install on that
  node. A missing or divergent entry fires ``always_or_unreachable(False, ...)``; a clean
  check accumulates ``always_or_unreachable(True, ...)``.

- ``"Installed remote snapshot"`` (raft.go ``installSnapshot()``) — records
  the per-node snapshot watermark. Indices at or below that watermark are
  treated as present on the node when checking the election: the snapshot
  replaces the log prefix with restored FSM state, so per-entry
  ``(term, crc)`` is no longer recoverable for those indices, and the
  consumer trusts the snapshot's terminus rather than the append stream.

- ``"recovered log from disk"`` (raft.go ``NewRaft()``) — records the
  per-node recovery watermark: the terminus of the log a restarting node
  loaded from persistent storage. Indices at or below it are treated as
  present on the node for the same reason as the snapshot watermark — the
  persisted prefix is not re-emitted on the append stream, so per-entry
  ``(term, crc)`` is not recoverable for those indices.

Truncation is observed via the conflict-truncation log so that a follower
that briefly held an entry and was then forced to drop it doesn't
contribute false positives. Commit tracking deliberately uses only the
leader-side ``advanced commitIndex`` record because that's the canonical
point at which an entry becomes committed; follower-side advances are
echoes of that decision.

This consumer maintains its own per-node log state rather than sharing
with the log-matching consumer's; the dispatcher passes each record to every consumer
independently and the duplication cost is small relative to the
clarity it preserves.
"""

from __future__ import annotations

import json
from datetime import datetime

from antithesis.assertions import always_or_unreachable

from utils import coerce_int


_MSG_APPEND = "appended entries to log store"
_MSG_CLEAR = "clearing log suffix"
_MSG_COMMIT = "advanced commitIndex"
_MSG_ELECTION = "election won"
_MSG_SNAPSHOT_INSTALLED = "Installed remote snapshot"
_MSG_SNAPSHOT_RESTORED = "restored from snapshot"
_MSG_RECOVERED = "recovered log from disk"


class LeaderCompletenessProperty:
    def __init__(self) -> None:
        # source -> idx -> (term, crc)
        self._log: dict[str, dict[int, tuple[int, int]]] = {}
        # idx -> (term, crc); pinned on the first leader-side commit advance
        # that covers idx, then frozen. (term, crc) is the entry's intrinsic
        # creation term and content digest.
        self._committed: dict[int, tuple[int, int]] = {}
        # idx -> commit term (the leader's currentTerm at the moment of the
        # pinning commit advance). Leader Completeness only constrains
        # leaders at terms strictly greater than this — checks for leaders
        # at term <= commit_term are out of scope.
        self._commit_term: dict[int, int] = {}
        # source -> highest snapshot last-index installed on that node.
        # Indices <= this watermark are treated as present in the node's log
        # without a per-entry (term, crc) check, because the snapshot
        # replaces the log prefix with restored FSM state and the per-entry
        # digest stream does not cover snapshotted indices.
        self._snapshot_index: dict[str, int] = {}
        # source -> highest log terminus a restarting node loaded from disk.
        # Treated identically to the snapshot watermark: the persisted prefix
        # is not re-emitted on the append stream, so per-entry (term, crc) is
        # not recoverable for indices <= this watermark.
        self._recovered_index: dict[str, int] = {}

    def _covered_index(self, source: str) -> int:
        # Highest index whose presence on the node is trusted without a
        # per-entry (term, crc) check: covered by either an installed
        # snapshot or a recovered-from-disk prefix.
        return max(
            self._snapshot_index.get(source, 0),
            self._recovered_index.get(source, 0),
        )

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
        elif message == _MSG_COMMIT:
            self._on_commit(source, fields)
        elif message == _MSG_ELECTION:
            self._on_election(source, fields)
        elif message == _MSG_SNAPSHOT_INSTALLED or message == _MSG_SNAPSHOT_RESTORED:
            self._on_snapshot_installed(source, fields)
        elif message == _MSG_RECOVERED:
            self._on_recovered(source, fields)

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

    def _on_commit(self, source: str, fields: dict) -> None:
        # Only the leader's commit advance pins canonical committed content.
        # Followers learn commit status via the leader's heartbeat and emit
        # their own "advanced commitIndex" with role=follower — those echo
        # the leader's decision and would only add noise here.
        if fields.get("role") != "leader":
            return
        from_idx = coerce_int(fields.get("from"))
        to_idx = coerce_int(fields.get("to"))
        commit_term = coerce_int(fields.get("term"))
        if from_idx is None or to_idx is None or commit_term is None:
            return
        src_log = self._log.get(source, {})
        covered_idx = self._covered_index(source)
        for idx in range(from_idx + 1, to_idx + 1):
            if idx in self._committed:
                continue
            if idx <= covered_idx:
                # The leader holds the entry in a snapshot- or recovery-
                # covered prefix, but the per-entry (term, crc) is no longer
                # recoverable from the append stream. Skip pinning rather than
                # record an unverified value — a later leader that committed
                # via the append stream will pin canonical content for this
                # index.
                continue
            entry = src_log.get(idx)
            if entry is None:
                # The leader's append should have reached us before the
                # commit-advance for that index, but a race is possible.
                # Skip rather than pin an unverified value.
                continue
            self._committed[idx] = entry
            self._commit_term[idx] = commit_term

    def _on_election(self, source: str, fields: dict) -> None:
        term = coerce_int(fields.get("term"))
        if term is None:
            return
        new_leader_log = self._log.get(source, {})
        covered_idx = self._covered_index(source)
        verified = 0
        for idx, expected in self._committed.items():
            commit_term = self._commit_term.get(idx)
            if commit_term is not None and commit_term >= term:
                # Leader Completeness only constrains leaders at terms
                # strictly greater than the commit term. A candidate that
                # won at term <= commit_term cannot — and is not required
                # to — hold an entry committed at that or a higher term;
                # a leader's log only contains entries at terms <= its
                # own term. This is the stale-term-election case where
                # two candidates win concurrently at different terms.
                continue
            if idx <= covered_idx:
                # Index is covered by an installed snapshot or a recovered
                # on-disk prefix on this node. Those indices collapse into
                # restored/persisted state whose per-entry (term, crc) cannot
                # be verified from the append stream. Treat as present; Raft
                # guarantees the covered prefix matches the committed prefix
                # up to covered_idx.
                continue
            actual = new_leader_log.get(idx)
            if actual is None:
                always_or_unreachable(
                    False,
                    "[leader-completeness] new leader's log contains every previously-committed entry",
                    {
                        "slug": "leader-completeness",
                        "new_leader": source,
                        "new_leader_term": term,
                        "missing_index": idx,
                        "expected_term": expected[0],
                        "expected_crc": expected[1],
                        "commit_term": commit_term,
                        "covered_index": covered_idx,
                    },
                )
                return
            if actual != expected:
                always_or_unreachable(
                    False,
                    "[leader-completeness] new leader's log contains every previously-committed entry",
                    {
                        "slug": "leader-completeness",
                        "new_leader": source,
                        "new_leader_term": term,
                        "divergent_index": idx,
                        "expected": {"term": expected[0], "crc": expected[1]},
                        "actual": {"term": actual[0], "crc": actual[1]},
                        "commit_term": commit_term,
                        "covered_index": covered_idx,
                    },
                )
                return
            verified += 1
        always_or_unreachable(
            True,
            "[leader-completeness] new leader's log contains every previously-committed entry",
            {
                "slug": "leader-completeness",
                "new_leader": source,
                "new_leader_term": term,
                "verified_committed_entries": verified,
                "covered_index": covered_idx,
            },
        )

    def _on_snapshot_installed(self, source: str, fields: dict) -> None:
        last_index = coerce_int(fields.get("last-index"))
        if last_index is None:
            return
        prev = self._snapshot_index.get(source, 0)
        if last_index > prev:
            self._snapshot_index[source] = last_index

    def _on_recovered(self, source: str, fields: dict) -> None:
        last_index = coerce_int(fields.get("last-index"))
        if last_index is None:
            return
        prev = self._recovered_index.get(source, 0)
        if last_index > prev:
            self._recovered_index[source] = last_index
