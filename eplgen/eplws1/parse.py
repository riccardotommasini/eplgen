from __future__ import annotations

import re
from typing import List, Tuple, Optional

from .ast import SelectQuery, StreamSource, PatternSource, WindowSpec, FromSource

_KEYWORDS = [" where ", " group by ", " having "]

def _split_top_level(s: str, sep: str = ",") -> List[str]:
    """Split by sep, but ignore separators inside (), [] and quotes."""
    out: List[str] = []
    buf: List[str] = []
    depth_par = 0
    depth_br = 0
    in_squote = False
    in_dquote = False
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "'" and not in_dquote:
            in_squote = not in_squote
            buf.append(ch)
            i += 1
            continue
        if ch == '"' and not in_squote:
            in_dquote = not in_dquote
            buf.append(ch)
            i += 1
            continue
        if not in_squote and not in_dquote:
            if ch == "(":
                depth_par += 1
            elif ch == ")":
                depth_par = max(0, depth_par - 1)
            elif ch == "[":
                depth_br += 1
            elif ch == "]":
                depth_br = max(0, depth_br - 1)
            if ch == sep and depth_par == 0 and depth_br == 0:
                part = "".join(buf).strip()
                if part:
                    out.append(part)
                buf = []
                i += 1
                continue
        buf.append(ch)
        i += 1
    part = "".join(buf).strip()
    if part:
        out.append(part)
    return out


def _find_clause_boundaries(q: str) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
    """Return (select_list, from_clause, where, group_by, having)."""
    ql = q.strip()
    ql = re.sub(r"\s+", " ", ql)
    low = ql.lower()

    m = re.match(r"^select\s+(.+?)\s+from\s+(.+)$", ql, flags=re.I)
    if not m:
        raise ValueError(f"Not a SELECT-FROM query: {q}")

    select_list = m.group(1).strip()
    rest = m.group(2).strip()
    rest_low = rest.lower()

    where = group_by = having = None

    # locate keywords at top-level by scanning and tracking nesting
    def find_kw(rest_s: str, kw: str) -> int:
        # expects kw with surrounding spaces, e.g. ' where '
        depth_par = depth_br = 0
        in_squote = in_dquote = False
        rs = rest_s
        rslow = rs.lower()
        i = 0
        while i <= len(rs) - len(kw):
            ch = rs[i]
            if ch == "'" and not in_dquote:
                in_squote = not in_squote
                i += 1
                continue
            if ch == '"' and not in_squote:
                in_dquote = not in_dquote
                i += 1
                continue
            if not in_squote and not in_dquote:
                if ch == "(":
                    depth_par += 1
                elif ch == ")":
                    depth_par = max(0, depth_par-1)
                elif ch == "[":
                    depth_br += 1
                elif ch == "]":
                    depth_br = max(0, depth_br-1)
                if depth_par == 0 and depth_br == 0 and rslow.startswith(kw, i):
                    return i
            i += 1
        return -1

    idx_where = find_kw(rest, " where ")
    idx_group = find_kw(rest, " group by ")
    idx_having = find_kw(rest, " having ")

    # determine ordering among present clauses
    cutpoints = [(idx_where, "where"), (idx_group, "group"), (idx_having, "having")]
    cutpoints = [(i, k) for i,k in cutpoints if i != -1]
    cutpoints.sort()

    from_part_end = cutpoints[0][0] if cutpoints else len(rest)
    from_clause = rest[:from_part_end].strip()

    def slice_clause(start_idx: int, start_kw: str, end_idx: int) -> str:
        return rest[start_idx + len(start_kw): end_idx].strip()

    for pos, (idx, key) in enumerate(cutpoints):
        end = cutpoints[pos+1][0] if pos+1 < len(cutpoints) else len(rest)
        if key == "where":
            where = slice_clause(idx, " where ", end)
        elif key == "group":
            group_by = slice_clause(idx, " group by ", end)
        elif key == "having":
            having = slice_clause(idx, " having ", end)

    return select_list, from_clause, where, group_by, having


def _parse_source(src: str) -> FromSource:
    s = src.strip()
    low = s.lower()
    if low.startswith("pattern"):
        # allow: PATTERN [ ... ] or PATTERN[...] (tolerant)
        m = re.match(r"pattern\s*(\[.*\])\s*$", s, flags=re.I)
        if not m:
            raise ValueError(f"Bad PATTERN source: {src}")
        return PatternSource(pattern=m.group(1).strip())

    # Stream source: Name [ (cond) ] [ #win(...) ]
    # 1) window part (last #... if present at top level)
    window = None
    # Find top-level '#'
    # (ignore hashes inside quotes)
    depth_par = depth_br = 0
    in_squote = in_dquote = False
    hash_idx = -1
    for i,ch in enumerate(s):
        if ch == "'" and not in_dquote:
            in_squote = not in_squote
            continue
        if ch == '"' and not in_squote:
            in_dquote = not in_dquote
            continue
        if not in_squote and not in_dquote:
            if ch == "(":
                depth_par += 1
            elif ch == ")":
                depth_par = max(0, depth_par-1)
            elif ch == "[":
                depth_br += 1
            elif ch == "]":
                depth_br = max(0, depth_br-1)
            if ch == "#" and depth_par == 0 and depth_br == 0:
                hash_idx = i
                break
    base = s
    if hash_idx != -1:
        base = s[:hash_idx].strip()
        win = s[hash_idx+1:].strip()
        window = WindowSpec(func=win)

    # 2) filter args, e.g. DetectMov(camera="R2")
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(\((.*)\))?\s*$", base)
    if not m:
        raise ValueError(f"Bad stream source: {src}")
    name = m.group(1)
    filter_cond = None
    if m.group(3) is not None:
        filter_cond = m.group(3).strip()
    return StreamSource(name=name, filter_cond=filter_cond, window=window)


def parse_select_query(q: str) -> SelectQuery:
    """Parse a single EPL SELECT statement (optionally with INSERT INTO prefix)."""
    qs = q.strip().rstrip(";").strip()
    qs = re.sub(r"\s+", " ", qs)

    insert_into = None
    m_ins = re.match(r"^insert\s+into\s+([A-Za-z_][A-Za-z0-9_]*)\s+(select\s+.+)$", qs, flags=re.I)
    if m_ins:
        insert_into = m_ins.group(1)
        qs = m_ins.group(2)

    select_list, from_clause, where, group_by, having = _find_clause_boundaries(qs)
    sources = [_parse_source(p) for p in _split_top_level(from_clause, ",")]

    return SelectQuery(
        select=select_list,
        from_sources=sources,
        where=where,
        group_by=group_by,
        having=having,
        insert_into=insert_into,
    )
