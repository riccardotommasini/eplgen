# Workstream 1 code: decomposition + atomic-suite harness + workload generator

This package implements:
- Algorithms 1–3 from the manuscript (expression/window/pattern translation) as executable Python.
- A minimal EPL AST + pretty-printer for the covered fragment.
- A test harness that can:
  - compare (original query) vs (decomposed atomic-query network) **inside the same engine** (useful for validating the decomposition);
  - compare (Esper output) vs (your denotational-semantics output) once you plug in your semantics interpreter.
- A workload generator that samples EPL queries with clause frequencies aligned to Figure 1 (defaults can be adjusted).

## Quick start (local)
```bash
python -m eplws1.main --help
python -m eplws1.main gen --n 50 --seed 1 --out workload.jsonl
python -m eplws1.main decompose --in workload.jsonl --out decomposed.jsonl
```

## Running tests against an engine
The harness expects an `Engine` implementation with:
- `run(statements: list[str], events: dict[str, list[dict]]) -> list[dict]`

Two adapters are provided:
- `eplws1.engines.esper_cmd.EsperCmdEngine`: calls an external command (typically `java -jar ...`) that accepts JSON on stdin and returns JSON on stdout.
- `eplws1.engines.semantics_stub.SemanticsStubEngine`: placeholder to be replaced by your semantics interpreter.

See `eplws1/engines/esper_cmd.py` for the JSON contract.

## Scope
The parser + decomposition cover the fragment used by Algorithms 1–3 and Table 19:
- SELECT ... FROM ...
- WHERE ...
- comma-joins in FROM
- inline windows `#time(..)`, `#length(..)` etc
- stream filters `R(cond)` in FROM
- PATTERN [ ... ] as an atomic source
- GROUP BY and HAVING are supported as a practical extension:
  - GROUP BY stays in the projection/aggregation atomic query
  - HAVING is rewritten into a post-aggregation filter atomic query (matching Listing 14 style)

You can extend the grammar/AST incrementally when you need more EPL features.

## Export .epl with @Tag/@name annotations
```bash
python -m eplws1.main export-epl --in workload.jsonl --out-dir epl_cases --create-window-mode esper
```

## Export one .epl per query
```bash
python -m eplws1.main export-epl --in workload.jsonl --out-dir epl_cases --create-window-mode esper
# writes epl_cases/Q0001.epl, Q0002.epl, ...
```

## Parameterizing event types

In the Python package:

1. **Schema names (event types)**

* `eplws1/config.py`

  * `DEFAULT_SCHEMA_STREAMS = ("DetectMov", "BaseThermRead", "AlertSmoke", "ErrorEvt")`

You can override at runtime:

* workload generation: `--streams ...`
* export (schemas + CSV generation): `--schema-streams ...`

2. **Schema fields and types used in exported EPL (create schema ...)**

* `eplws1/export_epl.py`

  * `_emit_basic_schemas(...)` contains the schema string:

    * `schema_fields = "camera string, therm string, temp double, humid double, x int, y int, sensor string, ts long"`

Edit that string to match your real schemas, or extend it to emit different field sets per stream.

3. **CSV column schema**

* `eplws1/export_data.py`

  * `DEFAULT_COLUMNS = ["EventType","Timestamp","camera","therm","temp","humid","x","y","sensor"]`

4. **Synthetic data generator field population**

* `eplws1/synth_events.py`

  * `generate_stream(...)` creates the actual per-field values (and `ts`).

If you want per-stream schemas (different fields per event type), the change is local: replace `_emit_basic_schemas` to look up a `dict {streamName: "field type, ..."}` and replace `DEFAULT_COLUMNS` with the union (or per-stream CSV files).


```bash
python -m eplws1.main gen --n 50 --seed 1 --out workload.jsonl --streams DetectMov,BaseThermRead,AlertSmoke
python -m eplws1.main export-epl --in workload.jsonl --out-dir epl_cases --schema-streams DetectMov,BaseThermRead,AlertSmoke --n-per-stream 500 --seed 1
```

Notes:
- The last decomposed statement is named `Qxxxx_Decomp_Final`.
- CSV header is `EventType,Timestamp,[schema fields...]`.

