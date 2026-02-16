from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Sequence
import csv

from .engines.base import Event

DEFAULT_COLUMNS = ["EventType", "Timestamp", "camera", "therm", "temp", "humid", "x", "y", "sensor"]

def events_to_rows(events_by_type: Dict[str, List[Event]], *, columns: Sequence[str] = DEFAULT_COLUMNS) -> List[dict]:
    rows: List[dict] = []
    for etype, evs in events_by_type.items():
        for ev in evs:
            row = {c: "" for c in columns}
            row["EventType"] = etype
            ts = ev.get("ts", ev.get("Timestamp", ev.get("timestamp", "")))
            row["Timestamp"] = ts
            for k, v in ev.items():
                if k == "ts":
                    continue
                if k in row:
                    row[k] = v
            rows.append(row)

    def key(r):
        try:
            return (int(r["Timestamp"]), str(r["EventType"]))
        except Exception:
            return (0, str(r["EventType"]))
    rows.sort(key=key)
    return rows

def write_case_csv(out_csv: str | Path, events_by_type: Dict[str, List[Event]], *, columns: Sequence[str] = DEFAULT_COLUMNS) -> None:
    out_csv = Path(out_csv)
    rows = events_to_rows(events_by_type, columns=columns)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(columns))
        w.writeheader()
        for r in rows:
            w.writerow(r)
