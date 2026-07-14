"""Echo-style consumers for raft-monitor.

Both consumers render records in hclog's default text format
``<timestamp> [LEVEL] <source>: <message>: <args>`` — StdoutLogger writes to
stdout, FileLogger appends to a file. They share the formatting helpers
below so the two outputs never drift.
"""

from __future__ import annotations

import json
import sys
import threading
from datetime import datetime
from pathlib import Path


def _format_fields(fields: dict) -> str:
    """Render structured fields as ``key=value key=value``, hclog-style.

    Values go through ``json.dumps`` so strings get quoted and embedded
    spaces / specials are escaped — matches how hclog renders complex values
    in its default text format closely enough for human reading.
    """
    return " ".join(f"{k}={json.dumps(v)}" for k, v in fields.items())


def _format_record(
    level: str, message: str, source: str, timestamp: datetime, fields: dict
) -> str:
    """Render a record as ``<timestamp> [LEVEL] <source>: <message>: <args>``,
    matching hclog's default text format."""
    ts = timestamp.replace(tzinfo=None).isoformat()
    line = f"{ts} [{level.upper()}] {source}: {message}"
    if fields:
        line += ": " + _format_fields(fields)
    return line


class StdoutLogger:
    """Prints every record to stdout in hclog's default text format.
    Placeholder until real property consumers are registered."""

    def consume(
        self,
        level: str,
        message: str,
        source: str,
        timestamp: datetime,
        fields: dict,
    ) -> None:
        print(_format_record(level, message, source, timestamp, fields), flush=True)


class FileLogger:
    """Appends every record to a log file in hclog's default text format.

    The file is opened line-buffered so a `tail -F` from outside the container
    sees lines as they arrive. A lock serializes writes so concurrent
    raft-node connections can't interleave bytes within a single line.
    """

    def __init__(self, path: str = "raft-monitor.log") -> None:
        self._path = Path(path).resolve()
        # Line-buffered (buffering=1) flushes on every newline so tail -F works.
        self._file = open(self._path, "a", buffering=1, encoding="utf-8")
        self._lock = threading.Lock()
        print(f"raft-monitor: file log at {self._path}", file=sys.stderr, flush=True)

    def consume(
        self,
        level: str,
        message: str,
        source: str,
        timestamp: datetime,
        fields: dict,
    ) -> None:
        line = _format_record(level, message, source, timestamp, fields)
        with self._lock:
            self._file.write(line + "\n")
