"""State-machine-safety consumer.

Enforces the State Machine Safety guarantee from the Raft paper: if any two
nodes have applied a log entry at a given index, the entries must contain
the same command. Once an entry reaches the FSM it is, by definition,
committed and visible to clients; divergence at this layer is observable
as cluster data corruption.

Filter is `message == "applying entry to FSM"` (fsm.go, `runFSM()` in both
`applySingle` and `applyBatch` branches). The log carries `index`, `term`,
and `data` (the command bytes hex-encoded, so binary blocks survive JSON
transport losslessly). The consumer cross-checks `(term, data)` across nodes
at each shared index.

This is strictly weaker than log-matching — by the time an entry has been
applied it has also been appended, so a content divergence here implies a
content divergence in the log. The two assertions are kept separate because
log-matching catches divergences in uncommitted tails as well, while this
check directly witnesses the corruption that clients would observe.
"""

from __future__ import annotations

from datetime import datetime

from antithesis.assertions import always_or_unreachable

from utils import coerce_int


_MESSAGE = "applying entry to FSM"


class StateMachineSafetyProperty:
    def __init__(self) -> None:
        # source -> idx -> (term, data_string)
        self._applied: dict[str, dict[int, tuple[int, str]]] = {}

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
        idx = coerce_int(fields.get("index"))
        term = coerce_int(fields.get("term"))
        data = fields.get("data")
        if idx is None or term is None or not isinstance(data, str):
            return
        observation = (term, data)
        for peer, peer_applied in self._applied.items():
            if peer == source:
                continue
            existing = peer_applied.get(idx)
            if existing is None:
                continue
            always_or_unreachable(
                existing == observation,
                "[state-machine-safety] FSM apply of the same index agrees on (term, data)",
                {
                    "slug": "state-machine-safety",
                    "index": idx,
                    "observations": {
                        source: {"term": term, "data": data},
                        peer: {"term": existing[0], "data": existing[1]},
                    },
                },
            )
            if existing != observation:
                # Stop reporting against further peers — the violation is
                # already recorded and any additional peers would just
                # duplicate the finding.
                return
        self._applied.setdefault(source, {})[idx] = observation
        always_or_unreachable(
            True,
            "[state-machine-safety] FSM apply of the same index agrees on (term, data)",
            {
                "slug": "state-machine-safety",
                "index": idx,
                "node": source,
                "term": term,
            },
        )
