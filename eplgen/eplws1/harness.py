from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import json

from .engines.base import Engine, Event
from .parse import parse_select_query
from .decompose import decompose_select_query

def _multiset_key(ev: Event) -> Tuple:
    # Stable comparison key: sort items; ignore engine-specific metadata keys if desired.
    return tuple(sorted((k, json.dumps(v, sort_keys=True, default=str)) for k,v in ev.items()))

def compare_outputs(a: List[Event], b: List[Event]) -> bool:
    # Multiset semantics (bag) by default.
    sa = sorted((_multiset_key(e) for e in a))
    sb = sorted((_multiset_key(e) for e in b))
    return sa == sb

@dataclass
class HarnessResult:
    ok: bool
    name: str
    details: str

def run_original_vs_decomposed(
    engine: Engine,
    query: str,
    events: Dict[str, List[Event]],
    *,
    create_window_mode: str = "paper",
) -> HarnessResult:
    q = parse_select_query(query)
    prog, _ = decompose_select_query(q, create_window_mode=create_window_mode)

    out_orig = engine.run([query], events)
    out_decomp = engine.run(prog.statements, events)

    ok = compare_outputs(out_orig, out_decomp)
    details = ""
    if not ok:
        details = (
            "Mismatch\n"
            f"original_out={out_orig}\n"
            f"decomposed_out={out_decomp}\n"
            f"decomposed_program={prog.statements}"
        )
    return HarnessResult(ok=ok, name="orig_vs_decomp", details=details)

def run_semantics_vs_esper(
    esper: Engine,
    semantics: Engine,
    statements: List[str],
    events: Dict[str, List[Event]],
) -> HarnessResult:
    out_e = esper.run(statements, events)
    out_s = semantics.run(statements, events)
    ok = compare_outputs(out_e, out_s)
    details = ""
    if not ok:
        details = f"Mismatch\nesper_out={out_e}\nsemantics_out={out_s}\nstatements={statements}"
    return HarnessResult(ok=ok, name="semantics_vs_esper", details=details)
