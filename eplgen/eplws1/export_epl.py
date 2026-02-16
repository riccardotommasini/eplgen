from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from .parse import parse_select_query
from .decompose import decompose_select_query
from .synth_events import generate_inputs
from .export_data import write_case_csv
from .config import DEFAULT_SCHEMA_STREAMS


@dataclass(frozen=True)
class ExportConfig:
    create_window_mode: str = "esper"     # "paper" or "esper"
    tag_name: str = "CASE"                # aligns original/decomposed
    name_prefix: str = "Q"

    emit_schemas: bool = True
    schema_streams: Sequence[str] = DEFAULT_SCHEMA_STREAMS
    schema_name: str = "BaseEvent"

    emit_csv: bool = True
    n_per_stream: int = 200
    seed: int = 0


def _ensure_semicolon(stmt: str) -> str:
    s = stmt.strip()
    return s if s.endswith(";") else s + ";"


def _stmt_kind(stmt: str) -> str:
    s = stmt.strip().lower()
    return "DDL" if s.startswith("create ") else "DML"


def _statement_block(cfg: ExportConfig, tag_value: str, case_id: str, stmt_name: str, stmt: str) -> str:
    return "\n".join([
        f'@Tag(name="EPL", value="{tag_value}")',
        f'@Tag(name="{cfg.tag_name}", value="{case_id}")',
        f'@name("{stmt_name}")',
        _ensure_semicolon(stmt),
        "",
    ])


def _emit_basic_schemas(cfg: ExportConfig, case_id: str) -> List[str]:
    schema_fields = "camera string, therm string, temp double, humid double, x int, y int, sensor string, ts long"
    out: List[str] = []
    for s in cfg.schema_streams:
        out.append("\n".join([
            '@Tag(name="EPL", value="DDL")',
            f'@Tag(name="{cfg.tag_name}", value="{case_id}")',
            f'@name("Schema_{s}")',
            f'@EventRepresentation(map) create schema {s} ({schema_fields});',
            "",
        ]))
    out.append("\n".join([
        '@Tag(name="EPL", value="DDL")',
        f'@Tag(name="{cfg.tag_name}", value="{case_id}")',
        f'@name("Schema_{cfg.schema_name}")',
        f'@EventRepresentation(map) create schema {cfg.schema_name} ({schema_fields});',
        "",
    ]))
    return out


def export_queries_to_case_files(
    queries: Sequence[str],
    out_dir: str | Path,
    *,
    cfg: ExportConfig = ExportConfig(),
    start_index: int = 1,
) -> List[Tuple[Path, Optional[Path]]]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: List[Tuple[Path, Optional[Path]]] = []

    for idx0, q in enumerate(queries, start=start_index):
        case = f"{cfg.name_prefix}{idx0:04d}"
        epl_path = out_dir / f"{case}.epl"
        csv_path = out_dir / f"{case}.csv" if cfg.emit_csv else None

        blocks: List[str] = []
        if cfg.emit_schemas:
            blocks.extend(_emit_basic_schemas(cfg, case))

        blocks.append(_statement_block(cfg, "DML", case, f"{case}_Original", q.strip().rstrip(";")))

        parsed = parse_select_query(q)
        prog, _ = decompose_select_query(parsed, create_window_mode=cfg.create_window_mode)

        total = len(prog.statements)
        for j, stmt in enumerate(prog.statements, start=1):
            kind = _stmt_kind(stmt)
            name = f"{case}_Decomp_Final" if j == total else f"{case}_Decomp_{j:02d}"
            blocks.append(_statement_block(cfg, kind, case, name, stmt.strip().rstrip(";")))

        epl_path.write_text("\n".join(blocks).rstrip() + "\n", encoding="utf-8")

        if cfg.emit_csv and csv_path is not None:
            ev = generate_inputs(
                seed=cfg.seed + idx0,
                n_per_stream=cfg.n_per_stream,
                streams=list(cfg.schema_streams),
            )
            write_case_csv(csv_path, ev)

        written.append((epl_path, csv_path))

    return written


def export_jsonl_to_case_files(
    in_jsonl: str | Path,
    out_dir: str | Path,
    *,
    cfg: ExportConfig = ExportConfig(),
    limit: Optional[int] = None,
) -> List[Tuple[Path, Optional[Path]]]:
    in_jsonl = Path(in_jsonl)
    qs: List[str] = []
    with in_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            qs.append(obj["query"])
            if limit is not None and len(qs) >= limit:
                break
    return export_queries_to_case_files(qs, out_dir, cfg=cfg)
