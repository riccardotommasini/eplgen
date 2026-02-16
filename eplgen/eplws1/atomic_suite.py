from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence, Optional
import random

from .ast import SelectQuery, StreamSource, PatternSource, WindowSpec
from .print_epl import query_to_epl
from .config import DEFAULT_SCHEMA_STREAMS


@dataclass(frozen=True)
class AtomicCase:
    name: str
    statements: List[str]
    input_streams: Sequence[str]
    notes: str = ""


def projection_case(stream: str, field: str = "camera") -> AtomicCase:
    q = SelectQuery(select=field, from_sources=[StreamSource(name=stream)])
    return AtomicCase("projection", [query_to_epl(q)], [stream], "Listing 7 style projection")

def selection_case(stream: str, cond: str = "camera = 'R2'") -> AtomicCase:
    q = SelectQuery(select="*", from_sources=[StreamSource(name=stream)], where=cond)
    return AtomicCase("selection", [query_to_epl(q)], [stream], "Listing 8 style WHERE selection")

def window_join_case(left: str, right: str, win: str = "time(20 seconds)") -> AtomicCase:
    q = SelectQuery(
        select="*",
        from_sources=[
            StreamSource(name=left, window=WindowSpec(win)),
            StreamSource(name=right, window=WindowSpec(win)),
        ],
    )
    return AtomicCase("window_join", [query_to_epl(q)], [left, right], "Listing 9 style join with inline windows")

def pattern_case(left: str, right: str, pattern: Optional[str] = None) -> AtomicCase:
    if pattern is None:
        pattern = f"[EVERY x={left} -> y={right}(temp>40 AND humid<20)]"
    q = SelectQuery(select="*", from_sources=[PatternSource(pattern=pattern)])
    return AtomicCase("pattern", [query_to_epl(q)], [left, right], "Listing 11 style PATTERN query")

def group_by_case(stream: str) -> AtomicCase:
    q = SelectQuery(select="therm, avg(temp)", from_sources=[StreamSource(name=stream)], group_by="therm")
    return AtomicCase("group_by", [query_to_epl(q)], [stream], "Listing 13 style GROUP BY + aggregate")

def having_case(stream: str) -> AtomicCase:
    s_agg = "AggregatedStream"
    q1 = SelectQuery(
        insert_into=s_agg,
        select="therm, avg(temp) as avgTemp",
        from_sources=[StreamSource(name=stream)],
        group_by="therm",
    )
    q2 = SelectQuery(select="*", from_sources=[StreamSource(name=s_agg)], where="avgTemp > 40")
    return AtomicCase("having", [query_to_epl(q1), query_to_epl(q2)], [stream], "Listing 14 style HAVING via post-aggregation WHERE")

def build_atomic_suite(seed: int = 0, *, streams: Sequence[str] = DEFAULT_SCHEMA_STREAMS) -> List[AtomicCase]:
    random.seed(seed)
    s = list(streams) if streams else list(DEFAULT_SCHEMA_STREAMS)
    a = s[0] if len(s) > 0 else "S0"
    b = s[1] if len(s) > 1 else a
    c = s[2] if len(s) > 2 else b
    return [
        projection_case(a),
        selection_case(a),
        window_join_case(c, b),
        pattern_case(a, b),
        group_by_case(b),
        having_case(b),
    ]
