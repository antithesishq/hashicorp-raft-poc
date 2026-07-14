"""raft-monitor TCP server.

Accepts line-delimited JSON log records from raft-node containers (shipped by
the hclog InterceptLogger sink in raft-node/main.go) and dispatches each
decoded record to every registered Consumer.

A Consumer is any object exposing a single method
``consume(level, message, source, timestamp, fields)``:

- ``level``     — log level string (``@level``: ``debug``/``info``/``warn``/``error``).
- ``message``   — the human-readable text (``@message`` in hclog output).
- ``source``    — the emitting node id (``@module``).
- ``timestamp`` — ``datetime`` parsed from ``@timestamp`` (UTC, tz-aware).
- ``fields``    — the remaining structured key/value pairs the emitter added,
                  with the four ``@``-prefixed metadata fields removed (they are
                  already in the explicit arguments above).

The monitor process holds the union of all consumers; each consumer is the
home of one global-property check, allowing properties that need cross-node
visibility (e.g., state-machine safety, election safety, log matching) to be
expressed in plain Python against the stream of records.

The server is intentionally minimal: it does not buffer, persist, or
backpressure. Consumers must do their own filtering and bookkeeping. If a
consumer raises, the exception is logged and the next consumer still runs;
the connection itself stays open so a single misbehaving consumer can't
silence the rest.
"""

from __future__ import annotations

import argparse
import json
import socket
import socketserver
import sys
import threading
import traceback
from datetime import datetime
from typing import Protocol

from echo import StdoutLogger, FileLogger
from election_safety import ElectionSafetyProperty
from leader_completeness import LeaderCompletenessProperty
from state_machine_safety import StateMachineSafetyProperty
from log_matching import LogMatchingProperty


class Consumer(Protocol):
    """Receives one decoded log record at a time."""

    def consume(
        self,
        level: str,
        message: str,
        source: str,
        timestamp: datetime,
        fields: dict,
    ) -> None: ...


class Dispatcher:
    """Thread-safe registry of consumers + dispatch."""

    def __init__(self) -> None:
        self._consumers: list[Consumer] = []
        self._lock = threading.Lock()

    def register(self, consumer: Consumer) -> None:
        with self._lock:
            self._consumers.append(consumer)

    def dispatch(self, msg: dict) -> None:
        # Pop the @-prefixed metadata fields — they're surfaced as explicit
        # consume() arguments, no need to repeat them in `fields`.
        message = str(msg.pop("@message", ""))
        source = str(msg.pop("@module", ""))
        level = str(msg.pop("@level", ""))
        ts_raw = msg.pop("@timestamp", None)
        try:
            # hclog emits e.g. "2026-06-03T20:21:53.444433Z"; fromisoformat
            # in 3.11+ accepts the trailing 'Z'.
            timestamp = (
                datetime.fromisoformat(ts_raw) if isinstance(ts_raw, str) else None
            )
        except ValueError:
            timestamp = None
        if timestamp is None:
            print(
                f"raft-monitor: dropping record with missing/bad @timestamp: {ts_raw!r}",
                file=sys.stderr,
            )
            return

        with self._lock:
            consumers = list(self._consumers)
        for c in consumers:
            try:
                c.consume(level, message, source, timestamp, msg)
            except Exception:
                # One bad consumer must not knock out the rest. Print and
                # continue; the run as a whole still produces signal.
                print(
                    f"raft-monitor: consumer {type(c).__name__} raised:",
                    file=sys.stderr,
                )
                traceback.print_exc()


class LogHandler(socketserver.StreamRequestHandler):
    """One instance per accepted connection. Reads lines until EOF."""

    def handle(self) -> None:
        peer = self.client_address
        print(f"raft-monitor: connection from {peer}", file=sys.stderr)
        dispatcher: Dispatcher = self.server.dispatcher  # type: ignore[attr-defined]
        for raw in self.rfile:
            line = raw.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError as e:
                print(
                    f"raft-monitor: bad JSON from {peer}: {e}: {line!r}",
                    file=sys.stderr,
                )
                continue
            dispatcher.dispatch(msg)
        print(f"raft-monitor: connection closed from {peer}", file=sys.stderr)


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True
    address_family = socket.AF_INET


def build_dispatcher() -> Dispatcher:
    """Construct the dispatcher with the consumer set for this run.

    As global-property consumers are written they get registered here.
    """
    d = Dispatcher()
    # StdoutLogger is too verbose for normal runs; uncomment for local debugging.
    # d.register(StdoutLogger())
    d.register(FileLogger())
    d.register(ElectionSafetyProperty())
    d.register(LeaderCompletenessProperty())
    d.register(StateMachineSafetyProperty())
    d.register(LogMatchingProperty())
    return d


def main() -> int:
    parser = argparse.ArgumentParser(description="raft-monitor TCP server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    dispatcher = build_dispatcher()

    server = ThreadedTCPServer((args.host, args.port), LogHandler)
    server.dispatcher = dispatcher  # type: ignore[attr-defined]

    print(
        f"raft-monitor: listening on {args.host}:{args.port}",
        file=sys.stderr,
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
