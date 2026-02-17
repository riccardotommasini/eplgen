from __future__ import annotations

from typing import List, Sequence
import random

from .ast import SelectQuery, StreamSource, PatternSource, WindowSpec
from .print_epl import query_to_epl
from .config import DEFAULT_SCHEMA_STREAMS
from .config import _AGGS
from .config import DEFAULT_WEIGHTS

def _w(p: float) -> bool:
    return random.random() < p

def _rand_stream(streams: Sequence[str]) -> str:
    s = list(streams) if streams else list(DEFAULT_SCHEMA_STREAMS)
    return random.choice(s)

def _rand_cond() -> str:
    field = random.choice(["camera", "therm", "temp", "humid", "x", "y"])
    if field in ("camera", "therm"):
        val = random.choice(["'R1'","'R2'","'R3'"])
        op = random.choice(["=","!="])
        return f"{field} {op} {val}"
    if field in ("temp","humid"):
        op = random.choice([">",">=","<","<="])
        val = random.choice([20, 30, 40, 50])
        return f"{field} {op} {val}"
    op = random.choice([">",">=","<","<="])
    val = random.choice([0, 1, 2, 3, 4])
    return f"{field} {op} {val}"

def _rand_window() -> WindowSpec:
    if _w(0.7):
        n = random.choice([5, 10, 20, 60])
        unit = random.choice(["seconds", "sec"])
        return WindowSpec(func=f"time({n} {unit})")
    n = random.choice([5, 10, 100, 1000])
    return WindowSpec(func=f"length({n})")

def _pattern_expr(streams: Sequence[str], include_every: bool, include_followed_by: bool, include_guard: bool) -> str:
    s = list(streams) if streams else list(DEFAULT_SCHEMA_STREAMS)
    left_stream = s[0] if len(s) > 0 else "DetectMov"
    right_stream = s[1] if len(s) > 1 else left_stream

    left_var = random.choice(["a","x","m"])
    right_var = random.choice(["b","y","n"])
    left = f"{left_var}={left_stream}"
    if include_every:
        left = "EVERY " + left
    right = f"{right_var}={right_stream}"
    if include_guard:
        right += f"({_rand_cond()} AND {_rand_cond()})"
    if include_followed_by:
        return f"[{left} -> {right}]"
    return f"[{left}]"

def generate_query(weights=DEFAULT_WEIGHTS, *, streams: Sequence[str] = DEFAULT_SCHEMA_STREAMS) -> SelectQuery:
    p_pattern = weights["pattern"] / (weights["pattern"] + 80)
    is_pattern = _w(p_pattern)

    select_list = "*"
    group_by = None
    having = None

    include_aggs = _w(weights["aggregates"] / 100)
    include_group = include_aggs and _w(weights["group_by"] / 100)
    include_having = include_group and _w(weights["having"] / 100)

    if include_aggs:
        gfield = random.choice(["camera", "therm"])
        afield = random.choice(["temp", "humid", "*"])
        agg = random.choice(_AGGS)
        agg_expr = "count(*)" if agg == "count" else f"{agg}({afield})"
        if include_group:
            select_list = f"{gfield}, {agg_expr} as a1"
            group_by = gfield
            if include_having:
                having = "a1 > 1"
        else:
            select_list = agg_expr + " as a1"

    if is_pattern:
        include_every = _w(weights["every"] / 100)
        include_followed_by = _w(weights["followed_by"] / 100)
        include_guard = _w(weights["guards"] / 100)
        pat = _pattern_expr(streams, include_every, include_followed_by, include_guard)
        from_sources = [PatternSource(pattern=pat)]
    else:
        do_join = _w(weights["join"] / 100)
        nsrc = 2 if do_join else 1
        from_sources = []
        for _ in range(nsrc):
            st = _rand_stream(streams)
            filt = _rand_cond() if _w(weights["r_filter"] / 100) else None
            win = _rand_window() if _w(weights["windows"] / 100) else None
            from_sources.append(StreamSource(name=st, filter_cond=filt, window=win))

    where = _rand_cond() if _w(weights["where"] / 100) else None

    return SelectQuery(
        select=select_list,
        from_sources=from_sources,
        where=where,
        group_by=group_by,
        having=having,
        insert_into=None,
    )

def generate_workload(n: int, seed: int = 0, weights=DEFAULT_WEIGHTS, *, streams: Sequence[str] = DEFAULT_SCHEMA_STREAMS) -> List[str]:
    random.seed(seed)
    return [query_to_epl(generate_query(weights=weights, streams=streams)) for _ in range(n)]
