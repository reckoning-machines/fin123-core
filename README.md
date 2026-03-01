# fin123-core

Deterministic financial modeling engine with a local browser UI.

fin123-core is the standalone, open-source core of the fin123 platform.
It runs entirely on your machine — no database, no server, no account required.

## What You Get

- **Workbook engine** — Polars-backed scalar DAG + table LazyFrame evaluation.
- **Formula language** — Lark LALR(1) parser with Excel-like syntax (`=SUM(revenue * (1 - tax_rate))`).
- **Local browser UI** — FastAPI on localhost, canvas-based spreadsheet grid, keyboard-first.
- **Deterministic lifecycle** — Edit → Commit → Build → Verify. Identical inputs always produce identical outputs.
- **Versioning** — Immutable snapshots, builds, and artifacts with SHA-256 integrity.
- **XLSX import** — Best-effort import of Excel workbooks with formula classification.
- **Templates** — Pre-built starting points for common financial model patterns.
- **Offline-first** — `fin123-core build` reads only local files. Zero network calls.

## Quick Start

### Install from source (editable / development mode):

```bash
git clone https://github.com/reckoning-machines/fin123-core.git
cd fin123-core
pip install -e ".[dev]"
```

> **End-user binaries** (no Python required) are available on
> [GitHub Releases](https://github.com/reckoning-machines/fin123-core/releases)
> for macOS (arm64) and Windows (x86_64). PyPI is intended for developer
> and library installs.

### Usage

```bash
# Create a project from a template
fin123-core new my_model --template single_company --set ticker=AAPL

# Commit the workbook snapshot
fin123-core commit my_model

# Build (evaluate the workbook)
fin123-core build my_model
# => Build saved to: 20260227T120000_run_1

# Verify build integrity (requires a completed build)
fin123-core verify-build 20260227T120000_run_1 --project my_model

# Launch the browser UI
fin123-core ui my_model
```

> **Note:** `verify-build` requires a completed build run.
> Run `fin123-core build` first, then pass the run ID printed by `build`.

## Browser UI

```bash
fin123-core ui my_model
```

Opens a local spreadsheet editor at `http://localhost:<port>` with:

- Canvas grid with sparse rendering and keyboard-first navigation.
- Formula bar with live validation.
- Multi-sheet tabs, copy/paste (TSV), font color formatting.
- Commit (Ctrl+S), Build (Ctrl+Enter), dependency highlight (Ctrl+P).
- XLSX import via Ctrl+O or `fin123-core import-xlsx model.xlsx my_model`.
- Version browsing — select any historical snapshot (read-only).

## CLI Commands

| Command | Description |
|---------|-------------|
| `fin123-core new <dir>` | Scaffold a new project (optionally from template) |
| `fin123-core commit <dir>` | Snapshot current workbook |
| `fin123-core build <dir>` | Build workbook (evaluate graphs, persist outputs) |
| `fin123-core verify-build <run_id> --project <dir>` | Verify a build's integrity (requires completed build) |
| `fin123-core diff run <a> <b>` | Compare two builds |
| `fin123-core diff version <v1> <v2>` | Compare two workbook versions |
| `fin123-core batch build <dir>` | Run batch builds from a params CSV |
| `fin123-core artifact list <dir>` | List versioned artifacts |
| `fin123-core gc <dir>` | Garbage collect old builds and artifacts |
| `fin123-core export <dir>` | Export latest build outputs |
| `fin123-core import-xlsx <file> <dir>` | Import an Excel workbook |
| `fin123-core template list` | List available templates |
| `fin123-core template show <name>` | Show template details and file tree |
| `fin123-core artifact approve <name> <ver>` | Approve an artifact version |
| `fin123-core artifact reject <name> <ver>` | Reject an artifact version |
| `fin123-core artifact status <name> <ver>` | Show artifact approval status |
| `fin123-core events <dir>` | Show structured event log |
| `fin123-core run-log <dir> <run_id>` | Show event log for a specific run |
| `fin123-core ui <dir>` | Launch the browser UI |
| `fin123-core demo ai-governance` | Demo: plugin validation + compliance report |
| `fin123-core demo deterministic-build` | Demo: scaffold, build, verify with stable hashes |
| `fin123-core demo batch-sweep` | Demo: 3-scenario parameter grid with stable manifest |
| `fin123-core demo data-guardrails` | Demo: join validation failures + success |

## Demos

fin123-core ships with four built-in demos that exercise key capabilities
end-to-end. Each demo is self-contained and produces deterministic output.

```bash
# Run a single demo
fin123-core demo ai-governance
fin123-core demo deterministic-build
fin123-core demo batch-sweep
fin123-core demo data-guardrails
```

No external data or configuration required. Demos create temporary project
directories, run the full build lifecycle, and print results to stdout.

## Project Layout

```
my_model/
  workbook.yaml       # Workbook specification (sheets, params, tables, outputs)
  fin123.yaml         # Project config (GC limits, mode)
  inputs/             # Source data (CSV, Parquet)
  runs/               # Immutable build records
  artifacts/          # Versioned workflow artifacts
  snapshots/          # Workbook spec history (v0001, v0002, ...)
  cache/              # Ephemeral hash cache
```

## Build Artifacts

```bash
python -m build
```

Produces `dist/fin123_core-<version>.tar.gz` (sdist) and
`dist/fin123_core-<version>-py3-none-any.whl` (wheel).

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — Core architecture: two-graph model, formula engine, UI, storage, determinism.
- [POD_BOUNDARY.md](POD_BOUNDARY.md) — What the enterprise Pod layer adds and how it extends Core.
- [docs/formulas_and_views.md](docs/formulas_and_views.md) — Formula semantics, Excel compatibility, unsupported functions, view sort/filter.
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — Operational runbook: install, usage, troubleshooting, and release process.
- [CHANGELOG.md](CHANGELOG.md) — Release notes.

## Enterprise Features

For database-backed registries, headless runner services, connectors (Bloomberg),
plugin marketplace, workflow automation, and SQL sync, see
[fin123-pod](https://github.com/reckoning-machines/fin123-pod) (private, requires license).
Built by Reckoning Machines

## License

Apache-2.0 — see [LICENSE](LICENSE).
