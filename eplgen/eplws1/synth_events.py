from __future__ import annotations

from typing import Dict, List, Sequence
import random

from .engines.base import Event
from .config import DEFAULT_SCHEMA_STREAMS

def generate_stream(n: int, seed: int = 0, *, stream_name: str) -> List[Event]:
    random.seed(seed)
    out: List[Event] = []
    t0 = 0
    for _ in range(n):
        t0 += random.choice([1, 1, 1, 2, 5])
        out.append({
            "camera": random.choice(["R1","R2","R3"]),
            "therm": random.choice(["R1","R2","R3"]),
            "temp": float(random.choice([18, 21, 23, 35, 42, 50])),
            "humid": float(random.choice([10, 20, 35, 40, 50])),
            "x": int(random.choice([0,1,2,3,4])),
            "y": int(random.choice([0,1,2,3,4])),
            "sensor": stream_name,
            "ts": int(t0),
        })
    return out

def generate_inputs(
    seed: int = 0,
    n_per_stream: int = 50,
    streams: Sequence[str] = DEFAULT_SCHEMA_STREAMS,
) -> Dict[str, List[Event]]:
    return {s: generate_stream(n_per_stream, seed=seed+idx, stream_name=s) for idx, s in enumerate(streams)}
