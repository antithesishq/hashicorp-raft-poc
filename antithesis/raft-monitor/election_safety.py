"""Election-safety consumer.

Tracks the set of nodes that have logged a winning vote tally at each term
and fires the Always whenever any term accumulates more than one
distinct winner. Paper §5.2: at most one leader per term.

The filter is `message == "election won"` (raft.go runCandidate), which
carries the term the node was elected in directly in the `term` field,
captured from the vote response on the main goroutine before any further
state transitions. The downstream "entering leader state" log cannot be
substituted: it reads `r.getCurrentTerm()` after setState(Leader), and
an incoming heartbeat on the transport fast-path can bump the term
between those two steps — yielding a log line whose term is strictly
greater than the term the node was actually elected in.
"""

from __future__ import annotations

from datetime import datetime

from antithesis.assertions import always_or_unreachable

from utils import coerce_int


_MESSAGE = "election won"


class ElectionSafetyProperty:
    def __init__(self) -> None:
        # term -> set of node ids that emitted "election won" at that term.
        self._winners: dict[int, set[str]] = {}

    def consume(
        self,
        level: str,
        message: str,
        source: str,
        timestamp: datetime,
        fields: dict,
    ) -> None:
        if message != _MESSAGE:
            return
        term = coerce_int(fields.get("term"))
        if term is None:
            return
        winners = self._winners.setdefault(term, set())
        winners.add(source)
        always_or_unreachable(
            len(winners) <= 1,
            "[election-safety] at most one leader per term",
            {
                "slug": "election-safety",
                "term": term,
                "leaders": sorted(winners),
            },
        )
