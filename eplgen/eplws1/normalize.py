from __future__ import annotations

from .ast import (
    SelectQuery, OpNode, OpSelect, OpWhere, OpJoin, OpWindow, OpStream, OpPattern,
    StreamSource, PatternSource, WindowSpec,
)

def to_operator_tree(q: SelectQuery) -> OpNode:
    # FROM clause -> omega
    if len(q.from_sources) == 0:
        raise ValueError("FROM clause is empty")

    def source_to_op(src):
        if isinstance(src, StreamSource):
            base = OpStream(src=StreamSource(name=src.name, filter_cond=src.filter_cond, window=None))
            if src.window is not None:
                return OpWindow(child=base, window=src.window)
            return base
        if isinstance(src, PatternSource):
            return OpPattern(src=src)
        raise TypeError(src)

    omega: OpNode = source_to_op(q.from_sources[0])
    for src in q.from_sources[1:]:
        omega = OpJoin(left=omega, right=source_to_op(src))

    # WHERE
    node: OpNode = omega
    if q.where:
        node = OpWhere(cond=q.where, child=node)

    # SELECT (+ optional GROUP BY)
    node = OpSelect(select=q.select, group_by=q.group_by, child=node)
    return node
