from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Union

from .ast import SelectQuery, StreamSource, PatternSource, WindowSpec, FromSource

def _src_to_str(src: FromSource) -> str:
    if isinstance(src, PatternSource):
        return f"PATTERN {src.pattern}"
    if isinstance(src, StreamSource):
        s = src.name
        if src.filter_cond:
            s += f"({src.filter_cond})"
        if src.window:
            s += f"#{src.window.func}"
        return s
    raise TypeError(src)

def query_to_epl(q: SelectQuery, *, trailing_semicolon: bool = True) -> str:
    parts: List[str] = []
    if q.insert_into:
        parts.append(f"INSERT INTO {q.insert_into}")
    parts.append(f"SELECT {q.select}")
    parts.append("FROM " + ", ".join(_src_to_str(s) for s in q.from_sources))
    if q.where:
        parts.append(f"WHERE {q.where}")
    if q.group_by:
        parts.append(f"GROUP BY {q.group_by}")
    if q.having:
        parts.append(f"HAVING {q.having}")
    s = "\n".join(parts)
    if trailing_semicolon:
        s += ";"
    return s
