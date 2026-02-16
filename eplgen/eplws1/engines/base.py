from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Protocol

Event = Dict[str, object]

class Engine(Protocol):
    def run(self, statements: List[str], events: Dict[str, List[Event]]) -> List[Event]:
        """Execute EPL statements against provided event streams; return output of the final statement."""
        ...
