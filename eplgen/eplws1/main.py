from __future__ import annotations

import argparse, json
from pathlib import Path

from .workload_gen import generate_workload
from .parse import parse_select_query
from .decompose import decompose_select_query
from .export_epl import export_jsonl_to_case_files, ExportConfig


def cmd_gen(args: argparse.Namespace) -> None:
    streams = None
    if args.streams:
        streams = [s.strip() for s in args.streams.split(",") if s.strip()]
    qs = generate_workload(args.n, seed=args.seed, streams=streams) if streams else generate_workload(args.n, seed=args.seed)

    outp = Path(args.out)
    with outp.open("w", encoding="utf-8") as f:
        for q in qs:
            f.write(json.dumps({"query": q}) + "\n")


def cmd_decompose(args: argparse.Namespace) -> None:
    inp = Path(args.inp)
    outp = Path(args.out)
    with inp.open("r", encoding="utf-8") as fin, outp.open("w", encoding="utf-8") as fout:
        for line in fin:
            obj = json.loads(line)
            q = obj["query"]
            parsed = parse_select_query(q)
            prog, _ = decompose_select_query(parsed, create_window_mode=args.create_window_mode)
            fout.write(json.dumps({
                "query": q,
                "decomposed": prog.statements,
                "lineage": prog.stream_lineage,
            }) + "\n")


def cmd_export_epl(args: argparse.Namespace) -> None:
    schema_streams = None
    if args.schema_streams:
        schema_streams = [s.strip() for s in args.schema_streams.split(",") if s.strip()]

    cfg = ExportConfig(
        create_window_mode=args.create_window_mode,
        tag_name=args.tag_name,
        name_prefix=args.name_prefix,
        emit_schemas=args.emit_schemas,
        schema_streams=schema_streams or ExportConfig().schema_streams,
        emit_csv=args.emit_csv,
        n_per_stream=args.n_per_stream,
        seed=args.seed,
        emit_decomposition=args.decompose,   # NEW
    )
    export_jsonl_to_case_files(args.inp, args.out_dir, cfg=cfg, limit=args.limit)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="eplws1")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("gen", help="Generate an EPL workload resembling Figure 1 frequencies.")
    g.add_argument("--n", type=int, required=True)
    g.add_argument("--seed", type=int, default=0)
    g.add_argument("--out", type=str, required=True)
    g.add_argument("--streams", type=str, default=None, help="Comma-separated stream/event type names")
    g.set_defaults(func=cmd_gen)


    d = sub.add_parser("decompose", help="Decompose EPL queries into atomic queries (Algorithms 1-3).")
    d.add_argument("--in", dest="inp", type=str, required=True)
    d.add_argument("--out", type=str, required=True)
    d.add_argument("--create-window-mode", choices=["paper","esper"], default="paper")
    d.set_defaults(func=cmd_decompose)
    

    e = sub.add_parser("export-epl", help="Export one .epl per query (original + decomposed network). Optionally generate a matching <case>.csv input dataset.")
    e.add_argument("--in", dest="inp", type=str, required=True)
    e.add_argument("--out-dir", dest="out_dir", type=str, required=True)
    e.add_argument("--create-window-mode", choices=["paper","esper"], default="esper")
    e.add_argument("--tag-name", type=str, default="CASE")
    e.add_argument("--name-prefix", type=str, default="Q")
    e.add_argument("--emit-schemas", action="store_true", default=True)
    e.add_argument("--no-schemas", action="store_false", dest="emit_schemas")
    e.add_argument("--schema-streams", type=str, default=None, help="Comma-separated schema stream/event types for schema + CSV generation")
    e.add_argument("--emit-csv", action="store_true", default=True)
    e.add_argument("--no-csv", action="store_false", dest="emit_csv")
    e.add_argument("--n-per-stream", type=int, default=200)
    e.add_argument("--seed", type=int, default=0)
    e.add_argument("--limit", type=int, default=None)
    e.add_argument("--no-decompose", action="store_false", dest="decompose", default=True)
    e.set_defaults(func=cmd_export_epl)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
