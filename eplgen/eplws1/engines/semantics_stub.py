from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from .base import Engine, Event

@dataclass
class SemanticsStubEngine:
    """Replace this with your denotational-semantics interpreter adapter."""
    def run(self, statements: List[str], events: Dict[str, List[Event]]) -> List[Event]:
        raise NotImplementedError("Plug in your denotational semantics evaluator here.")
