# Architecture

## 1. What Core Is

fin123-core is a deterministic financial modeling engine with a local browser UI. It runs entirely on your machine — no database, no server, no account required.

**What you get:**
- A workbook engine that evaluates scalar and table computations deterministically.
- An Excel-like formula language (Lark LALR(1) parser) with cell references and named ranges.
- A local browser UI (FastAPI on localhost) with a canvas-based spreadsheet grid.
- Immutable, versioned builds with integrity verification.
- XLSX import with formula classification and review workflow.
- Templates for common financial model patterns.
- Batch builds across parameter sets.
- Garbage collection for bounded storage.

**Lifecycle:** Edit → Commit → Build → Verify.

Every build is reproducible. Identical local files always produce identical outputs.

---

## 2. Deterministic Lifecycle

### Edit

Modify your workbook through the browser UI or directly in `workbook.yaml`. The UI maintains an in-memory working copy with a dirty flag.

### Commit

`fin123 commit <dir>` (or Ctrl+S in the UI) writes the working copy to `workbook.yaml` and creates an immutable snapshot at `snapshots/workbook/vXXXX/workbook.yaml`. Versions are monotonic (v0001, v0002, ...).

### Build

`fin123 build <dir>` (or Ctrl+Enter in the UI) evaluates the latest committed snapshot. Rejects if uncommitted edits exist. Produces an immutable run directory under `runs/` with:
- `run_meta.json` — run_id, timestamp, workbook_spec_hash, input_hashes, effective_params, params_hash, engine_version, model_id, model_version_id.
- `outputs/scalars.json` — evaluated scalar values.
- `outputs/<table>.parquet` — materialized table outputs.

### Verify

`fin123 verify <run_id>` checks integrity: recomputes workbook spec hash, input file hashes, params hash, overlay hash, export hash. Detects any post-build tampering.

### Determinism Guarantees

- Scalar graph: evaluated in topological order, pure functions.
- Table graph: Polars LazyFrame plans are deterministic. `group_by` uses `maintain_order=True`.
- Tables without explicit sort get a deterministic secondary sort at export (all columns, alphabetical).
- Hashing uses deterministic JSON serialization (`sort_keys=True`, compact separators).
- Non-deterministic metadata (timestamps) does not affect computation.

---

## 3. Two-Graph Model

### Scalar Graph (`scalars.py`)

A directed acyclic graph of named scalar values.

**Node types:**
- **Literals** — constants from workbook params or output definitions.
- **Formulas** — function calls referencing other scalars via `$name` or `=expression` syntax.

Evaluation proceeds by iterative topological resolution: each pass evaluates formulas whose dependencies are all resolved. Continues until all resolved or a cycle is detected.

Scalar functions registered via `@register_scalar("name")`.

### Table Graph (`tables.py`)

A graph of named Polars LazyFrame plans.

**Node types:**
- **Sources** — tables loaded from CSV or Parquet files via `pl.scan_csv` / `pl.scan_parquet`.
- **Plans** — sequential chains of table functions applied to an upstream source or plan.

Table functions receive a `LazyFrame` and return a `LazyFrame`, allowing Polars to optimize the full query plan before materialization via `.collect()`.

Built-in table functions: `select`, `filter`, `group_agg`, `sort`, `with_column`, `join_left`.

### Orchestration (`workbook.py`)

`Workbook.run()`:
1. Resolve parameters (spec defaults + scenario overrides + CLI overrides).
2. Hash input files (with mtime/size-based caching).
3. Evaluate table graph (materializes DataFrames for lookup cache).
4. Evaluate scalar graph (with access to tables for `lookup_scalar`).
5. Persist results as an immutable run.

### Formula Engine (`formulas/`)

Lark LALR(1) parser supporting Excel-like syntax:

| Element | Example |
|---------|---------|
| References | `revenue`, `$tax_rate`, `A1`, `Sheet1!A1` |
| Arithmetic | `a * (1 - b)`, `-2^2` = `-4` |
| Functions | `SUM(1, 2, 3)`, `IF(x > 0, x, 0)` |
| Percent | `3%` = `0.03` |

Built-in functions: `SUM`, `AVERAGE`, `MIN`, `MAX`, `ABS`, `ROUND`, `IF`, `IFERROR`, `VLOOKUP`, `SUMIFS`, `COUNTIFS`, `PARAM`.

### CellGraph (`cell_graph.py`)

On-demand memoized evaluator for sheet cell formulas:
- Lazy evaluation — cells computed only when referenced.
- Memoization — results cached per evaluation pass.
- Cross-sheet references (`Sheet!Addr`).
- Named range expansion for aggregate functions.
- Cycle detection (`#CIRC!`), error containment (`#ERR!`).

### Lookup Semantics

**`join_left`** — Deterministic left joins with cardinality validation (default `many_to_one`), dtype compatibility checking, null key rejection.

**`lookup_scalar`** — VLOOKUP exact-match with configurable `on_missing` (error/none) and `on_duplicate` (error/first) policies.

---

## 4. Local UI Architecture

### Overview

A local-only FastAPI server serving vanilla HTML/JS/CSS. No React, no build step, no third-party grid libraries.

```
fin123 ui <dir> → FastAPI server (localhost) → ProjectService → Workbook engine
```

### ProjectService (`ui/service.py`)

Single service layer managing all UI operations:
- Sheet viewport retrieval (sparse cells + formatting).
- Batch cell edits with formula validation.
- Multi-sheet management (add, delete, rename).
- Named range CRUD.
- Row/column insert/delete with reference rewriting.
- Snapshot commit and workbook build.
- Build checks (assertions, verify report, timing, lookup violations).
- Model version browsing (read-only for historical versions).
- GC and cache clearing.
- XLSX import and import review.
- Project health aggregation.

### Grid Rendering

Canvas-based for performance:
- 22px row height, 90px column width.
- Sparse rendering: only non-empty cells drawn.
- Keyboard-first: arrows, Enter, Esc, Tab, type-to-edit.
- Shift+arrows for rectangular selection.
- Formula bar with live parse validation (debounced 300ms).

### Design

Dark, dense, keyboard-first — inspired by professional financial terminals:
- JetBrains Mono throughout.
- Deep navy-black backgrounds, cool gray borders.
- Blue accent for selection and formulas.
- Status colors: amber (dirty), green (success), red (error).

### Key Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+S` | Commit snapshot |
| `Ctrl+Enter` | Build workbook |
| `Ctrl+O` | XLSX import |
| `Ctrl+B` | Toggle side panel |
| `Ctrl+P` | Toggle dependency highlight |
| `Ctrl+C/V` | Copy/paste (TSV) |
| `?` | Keyboard shortcuts overlay |
| `E` | Toggle errors panel |

### Sheet Data Model

Sheets stored in `workbook.yaml` as sparse cell maps with optional formatting:

```yaml
sheets:
  - name: Revenue
    n_rows: 200
    n_cols: 40
    cells:
      A1: { value: 42 }
      B1: { formula: "=A1 * 2" }
    fmt:
      A1: { color: "#ff5c5c" }
```

---

## 5. Local Storage Layout

```
<project_dir>/
  workbook.yaml              # Workbook specification
  fin123.yaml                # Project configuration
  inputs/                    # Source data (CSV, Parquet)
  runs/                      # Immutable build records
    <timestamp>_run_<n>/
      run_meta.json
      outputs/
        scalars.json
        *.parquet
  artifacts/                 # Versioned workflow artifacts
    <name>/vXXXX/
      meta.json
      approval.json
      artifact.json
      table.parquet
  snapshots/                 # Workbook spec history
    workbook/
      index.json
      vXXXX/workbook.yaml
  import_reports/            # Versioned XLSX import reports
    index.json
    <timestamp>/
      import_report.json
      import_trace.log
      source_filename.txt
  cache/                     # Ephemeral (hashes.json)
  pins.yaml                  # Optional pinning file
```

All data is local files. No database. No network calls during builds.

---

## 6. Templates and Demos

fin123-core ships bundled project templates:

| Template | Description |
|----------|-------------|
| `single_company` | Single-company DCF with revenue, cost, and valuation sheets |
| `universe_batch` | Multi-ticker batch model with parameter CSV |

### Usage

```
fin123 init my_model --template single_company --set ticker=AAPL
fin123 template list
fin123 template show single_company
```

Templates use `{{placeholder}}` substitution in YAML files. Binary files (`.parquet`) are copied verbatim. Each new project gets a fresh `model_id` UUID and an initial v0001 snapshot.

---

## 7. What Core Explicitly Does NOT Include

- **No database** — no Postgres, no SQL queries during evaluation.
- **No sync** — no `fin123 sync` command. External data must be provided as local CSV/Parquet files.
- **No connectors** — no Bloomberg or other data vendor integrations.
- **No plugins** — no plugin marketplace, installation, or management.
- **No workflows** — no multi-step workflow orchestration or AI steps.
- **No headless runner** — no remote execution service.
- **No registry** — no centralized Postgres model registry, no push/pull.
- **No release system** — no `fin123 release` commands.
- **No production mode gates** — no `mode: prod` enforcement.

These features are available via [fin123-pod](https://github.com/reckoningmachines/fin123-pod), which extends fin123-core with enterprise/team capabilities.

---

## 8. Compatibility and Extension Points

### Pod Extension Model

fin123-core is designed to be extended by fin123-pod without modification:

- **CLI:** Pod imports Core's Click group and registers additional commands.
- **UI service:** Pod-only methods (`run_sync`, `run_workflow`, `registry_push_versions`) are guarded with `try/except ImportError` and return stub responses when Pod is not installed.
- **Registry:** `_get_registry()` returns `None` when `fin123_pod` is not installed. Core features degrade gracefully.
- **Python package:** Core ships as `fin123` (the import namespace). Pod ships as `fin123_pod` and depends on `fin123-core`.

### Stable Interfaces

The following are stable and safe for downstream consumption:

- `Workbook(project_dir, scenario_name?, overrides?).run() → WorkbookResult`
- `ProjectService(project_dir, model_version_id?)` — full service API
- `ScalarGraph`, `TableGraph` — graph evaluators
- `@register_scalar`, `@register_table` — function extension points
- `RunStore`, `ArtifactStore`, `SnapshotStore` — versioning stores
- `parse_formula()`, `evaluate_formula()` — formula engine
- All CLI commands exposed via Click groups

### Dependencies

```
polars>=1.0
pyyaml>=6.0
click>=8.0
lark>=1.1
fastapi>=0.100
uvicorn>=0.20
python-multipart>=0.0.6
```

Optional: `openpyxl>=3.1` for XLSX import.
