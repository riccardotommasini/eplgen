from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Union, List

# ---- Source-level AST (what we parse / generate) ----

@dataclass(frozen=True)
class WindowSpec:
    """Inline window spec used in FROM, e.g., #time(20 sec)."""
    func: str  # e.g. "time(20 sec)" or "length(5)"


@dataclass(frozen=True)
class StreamSource:
    name: str                       # e.g., "DetectMov"
    filter_cond: Optional[str] = None  # e.g., "camera='R2'"
    window: Optional[WindowSpec] = None


@dataclass(frozen=True)
class PatternSource:
    pattern: str  # raw text inside PATTERN [...], including brackets if desired


FromSource = Union[StreamSource, PatternSource]


@dataclass(frozen=True)
class SelectQuery:
    """Minimal EPL query form for this workstream."""
    select: str                      # raw select list, e.g. "*", "camera, avg(temp)"
    from_sources: Sequence[FromSource]
    where: Optional[str] = None
    group_by: Optional[str] = None   # raw group-by list, e.g. "camera"
    having: Optional[str] = None     # raw having condition (will be rewritten)
    insert_into: Optional[str] = None  # if present, query is "INSERT INTO <name> SELECT ..."

    def is_join(self) -> bool:
        return len(self.from_sources) >= 2

# ---- Operator tree (what Algorithms 1–3 traverse) ----

@dataclass(frozen=True)
class OpNode:
    pass


@dataclass(frozen=True)
class OpSelect(OpNode):
    select: str
    group_by: Optional[str]
    child: OpNode


@dataclass(frozen=True)
class OpWhere(OpNode):
    cond: str
    child: OpNode


@dataclass(frozen=True)
class OpJoin(OpNode):
    left: OpNode
    right: OpNode


@dataclass(frozen=True)
class OpWindow(OpNode):
    child: OpNode
    window: WindowSpec


@dataclass(frozen=True)
class OpStream(OpNode):
    src: StreamSource


@dataclass(frozen=True)
class OpPattern(OpNode):
    src: PatternSource
