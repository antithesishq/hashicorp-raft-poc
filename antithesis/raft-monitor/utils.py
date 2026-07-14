"""Shared helpers for raft-monitor consumers."""

from __future__ import annotations

from typing import Optional


def coerce_int(v) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        try:
            return int(v)
        except ValueError:
            return None
    if isinstance(v, float):
        return int(v)
    return None
