from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

DEFAULT_SCHEMA_STREAMS: tuple[str, ...] = ("DetectMov", "BaseThermRead", "AlertSmoke", "ErrorEvt")
DEFAULT_FIELDS: tuple[str, ...] = ("camera", "therm", "temp", "humid", "x", "y", "sensor")

@dataclass(frozen=True)
class SchemaConfig:
    streams: Sequence[str] = DEFAULT_SCHEMA_STREAMS
    fields: Sequence[str] = DEFAULT_FIELDS
