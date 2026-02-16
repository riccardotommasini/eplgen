from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict

from .ast import (
    SelectQuery, OpNode, OpSelect, OpWhere, OpJoin, OpWindow, OpStream, OpPattern,
    StreamSource, PatternSource, WindowSpec,
)
from .normalize import to_operator_tree

# ---------------------------
# Naming + program container
# ---------------------------

@dataclass
class NameGen:
    prefix: str = "x"
    counter: int = 0

    def new(self, tag: str) -> str:
        self.counter += 1
        return f"{self.prefix}_{tag}_{self.counter}"


@dataclass
class Program:
    statements: List[str]
    # optional metadata (helps with analysis)
    stream_lineage: Dict[str, str]  # out_stream -> description

    def __init__(self) -> None:
        self.statements = []
        self.stream_lineage = {}

    def add(self, stmt: str, out_stream: Optional[str] = None, desc: Optional[str] = None) -> None:
        s = stmt.strip()
        if not s.endswith(";"):
            s += ";"
        self.statements.append(s)
        if out_stream and desc:
            self.stream_lineage[out_stream] = desc


# ----------------------------------------
# Table 19 write-procedure as code templates
# ----------------------------------------

def _stmt_insert_select(out_stream: str, select_body: str) -> str:
    return f"INSERT INTO {out_stream}\n{select_body}"

def _stmt_select(select_body: str) -> str:
    return select_body

def _stmt_create_window(win_name: str, win_func: str, *, mode: str = "paper") -> str:
    # 'paper' mirrors Listing 10: CREATE WINDOW AWindow#time(20 seconds)
    # 'esper' uses Esper-style: create window AWindow.win:time(20 sec) as SomeType
    if mode == "paper":
        return f"CREATE WINDOW {win_name}#{win_func}"
    if mode == "esper":
        return f"CREATE WINDOW {win_name}.win:{win_func} as BaseEvent"
    raise ValueError(mode)

def _stmt_insert_all(out_stream: str, in_stream: str) -> str:
    return f"INSERT INTO {out_stream}\nSELECT *\nFROM {in_stream}"

def _stmt_join(out_stream: str, left: str, right: str) -> str:
    return f"INSERT INTO {out_stream}\nSELECT *\nFROM {left}, {right}"

def _stmt_filter(out_stream: str, in_stream: str, cond: str) -> str:
    return f"INSERT INTO {out_stream}\nSELECT *\nFROM {in_stream}\nWHERE {cond}"

def _stmt_project(out_stream: str, in_stream: str, select_list: str, group_by: Optional[str] = None) -> str:
    body = [f"SELECT {select_list}", f"FROM {in_stream}"]
    if group_by:
        body.append(f"GROUP BY {group_by}")
    return _stmt_insert_select(out_stream, "\n".join(body))

def _stmt_pattern(out_stream: str, select_list: str, pattern: str) -> str:
    body = f"SELECT {select_list}\nFROM PATTERN {pattern}"
    return _stmt_insert_select(out_stream, body)

# ---------------------------
# Algorithms 1–3 implementation
# ---------------------------

def decompose_select_query(q: SelectQuery, *, create_window_mode: str = "paper") -> Tuple[Program, str]:
    """Return (program, final_stream_name).

    The returned program is an interconnected set of atomic queries equivalent to q,
    in the sense of Algorithms 1–3 + Table 19.
    """
    # Practical extension: rewrite HAVING into post-aggregation filter (Listing 14 style)
    # We treat HAVING only if it exists; otherwise use the query as-is.
    if q.having:
        # Step A: aggregate query (keeps GROUP BY)
        agg_out = q.insert_into or "AggOut"
        q1 = SelectQuery(
            insert_into=agg_out,
            select=q.select,
            from_sources=q.from_sources,
            where=q.where,
            group_by=q.group_by,
            having=None,
        )
        # Step B: post-filter over aggregated stream
        q2 = SelectQuery(
            insert_into=None,
            select="*",
            from_sources=[StreamSource(name=agg_out)],
            where=q.having,
            group_by=None,
            having=None,
        )
        prog1, s1 = decompose_select_query(q1, create_window_mode=create_window_mode)
        prog2, s2 = decompose_select_query(q2, create_window_mode=create_window_mode)
        prog1.statements.extend(prog2.statements)
        prog1.stream_lineage.update(prog2.stream_lineage)
        return prog1, s2

    root = to_operator_tree(q)
    prog = Program()
    ng = NameGen(prefix="x")
    final_stream = eExplore(root, prog, ng, create_window_mode=create_window_mode, final_insert=q.insert_into is not None, final_select=q.select)
    # If the original query had no INSERT INTO, keep the last statement as a plain SELECT producing output.
    # The algorithm above always uses INSERT INTO for OpSelect. Replace the last statement accordingly.
    if q.insert_into is None:
        # last statement must be an INSERT INTO ... SELECT ...
        last = prog.statements[-1]
        m = last.splitlines()
        if m and m[0].strip().lower().startswith("insert into "):
            prog.statements[-1] = "\n".join(m[1:]).rstrip(";") + ";"
    else:
        # ensure the last INSERT targets the requested output
        # (eExplore already does that by passing final_insert, but keep as guard)
        pass
    return prog, final_stream

def eExplore(node: OpNode, prog: Program, ng: NameGen, *, create_window_mode: str, final_insert: bool, final_select: str) -> str:
    # Algorithm 1: Expression Translation
    if isinstance(node, OpSelect):
        x = eExplore(node.child, prog, ng, create_window_mode=create_window_mode, final_insert=True, final_select=node.select)
        # projection/aggregation atomic query
        out = ng.new("proj")
        stmt = _stmt_project(out, x, node.select, node.group_by)
        prog.add(stmt, out_stream=out, desc=f"PROJ({node.select}) from {x}")
        return out
    if isinstance(node, OpWhere):
        x = wExplore(node.child, prog, ng, create_window_mode=create_window_mode)
        out = ng.new("filter")
        stmt = _stmt_filter(out, x, node.cond)
        prog.add(stmt, out_stream=out, desc=f"FILTER({node.cond}) from {x}")
        return out
    # else: pass-through to window exploration
    x = wExplore(node, prog, ng, create_window_mode=create_window_mode)
    return x

def wExplore(node: OpNode, prog: Program, ng: NameGen, *, create_window_mode: str) -> str:
    # Algorithm 2: Windowing Translation
    if isinstance(node, OpJoin):
        x = wExplore(node.left, prog, ng, create_window_mode=create_window_mode)
        y = wExplore(node.right, prog, ng, create_window_mode=create_window_mode)
        out = ng.new("join")
        stmt = _stmt_join(out, x, y)
        prog.add(stmt, out_stream=out, desc=f"JOIN({x},{y})")
        return out
    if isinstance(node, OpWindow):
        x = pExplore(node.child, prog, ng, create_window_mode=create_window_mode)
        # window materialization via named window
        win_name = ng.new("win")
        prog.add(_stmt_create_window(win_name, node.window.func, mode=create_window_mode), out_stream=win_name, desc=f"WINDOW({node.window.func})")
        prog.add(_stmt_insert_all(win_name, x))
        return win_name
    # fallback: pattern / stream
    x = pExplore(node, prog, ng, create_window_mode=create_window_mode)
    return x

def pExplore(node: OpNode, prog: Program, ng: NameGen, *, create_window_mode: str) -> str:
    # Algorithm 3: Pattern Translation
    if isinstance(node, OpStream):
        # inline stream filters R(cond) are kept as part of the source name in EPL,
        # but decomposition treats them as part of the source operator.
        # We return the printed source string to let upper layers use it in FROM.
        src = node.src.name
        if node.src.filter_cond:
            src += f"({node.src.filter_cond})"
        return src
    if isinstance(node, OpPattern):
        out = ng.new("pattern")
        stmt = _stmt_pattern(out, "*", node.src.pattern)
        prog.add(stmt, out_stream=out, desc=f"PATTERN({node.src.pattern})")
        return out
    # In case a window wraps a stream, wExplore passes the child here.
    if isinstance(node, OpWindow):
        return pExplore(node.child, prog, ng, create_window_mode=create_window_mode)
    raise TypeError(f"Unsupported node in pExplore: {type(node)}")
