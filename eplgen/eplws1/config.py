from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

DEFAULT_SCHEMA_STREAMS: tuple[str, ...] = ("DetectMov", "BaseThermRead", "AlertSmoke", "ErrorEvt")
DEFAULT_FIELDS: tuple[str, ...] = ("camera", "therm", "temp", "humid", "x", "y", "sensor")

_AGGS = ["avg", "max", "min", "count"]

DEFAULT_WEIGHTS = {
    "where": 28,
    "r_filter": 46,
    "windows": 54,
    "timewin": 37,
    "join": 12,
    "pattern": 20,
    "followed_by": 20,
    "every": 22,
    "guards": 8,
    "aggregates": 30,
    "group_by": 11,
    "having": 9,
}

@dataclass(frozen=True)
class SchemaConfig:
    streams: Sequence[str] = DEFAULT_SCHEMA_STREAMS
    fields: Sequence[str] = DEFAULT_FIELDS
