# fin123-core

Deterministic financial modeling engine with a local browser UI.

fin123-core is the standalone, open-source core of the fin123 platform.
It runs entirely on your machine -- no database, no server, no account required.

## What You Get

- **Workbook engine** -- Polars-backed scalar DAG + table LazyFrame evaluation.
- **Formula language** -- Lark LALR(1) parser with Excel-like syntax (`=SUM(revenue * (1 - tax_rate))`).
- **Local browser UI** -- FastAPI on localhost, canvas-based spreadsheet grid, keyboard-first.
- **Deterministic lifecycle** -- Edit -> Commit -> Build -> Verify. Identical inputs always produce identical outputs.
- **Versioning** -- Immutable snapshots, builds, and artifacts with SHA-256 integrity.
- **XLSX import** -- Best-effort import of Excel workbooks with formula classification.
- **Templates** -- Pre-built starting points for common financial model patterns.
- **Offline-first** -- `fin123 build` reads only local files. Zero network calls.
- **Doctor** -- Deterministic preflight validation (`fin123 doctor`).

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
fin123 init my_model --template single_company --set ticker=AAPL

# Commit the workbook snapshot
fin123 commit my_model

# Build (evaluate the workbook)
fin123 build my_model
# => Build saved to: 20260227T120000_run_1

# Verify build integrity (requires a completed build)
fin123 verify 20260227T120000_run_1 --project my_model

# Preflight checks
fin123 doctor

# Launch the browser UI
fin123 ui my_model
```

> **Note:** `verify` requires a completed build run.
> Run `fin123 build` first, then pass the run ID printed by `build`.

## Browser UI

```bash
fin123 ui my_model
```

Opens a local spreadsheet editor at `http://localhost:<port>` with:

- Canvas grid with sparse rendering and keyboard-first navigation.
- Formula bar with live validation.
- Multi-sheet tabs, copy/paste (TSV), font color formatting.
- Commit (Ctrl+S), Build (Ctrl+Enter), dependency highlight (Ctrl+P).
- XLSX import via Ctrl+O or `fin123 import-xlsx model.xlsx my_model`.
- Version browsing -- select any historical snapshot (read-only).

## CLI Commands

The CLI executable is `fin123` in both core and pod.

### Core lifecycle

| Command | Description |
|---------|-------------|
| `fin123 init <dir>` | Scaffold a new project (optionally from template) |
| `fin123 commit <dir>` | Snapshot current workbook |
| `fin123 build <dir>` | Build workbook (evaluate graphs, persist outputs) |
| `fin123 verify <run_id> --project <dir>` | Verify a build's integrity |
| `fin123 diff run <a> <b>` | Compare two builds |
| `fin123 diff version <v1> <v2>` | Compare two workbook versions |
| `fin123 export <dir>` | Export latest build outputs |
| `fin123 doctor` | Preflight and compliance validation |

### Additional commands

| Command | Description |
|---------|-------------|
| `fin123 batch build <dir>` | Run batch builds from a params CSV |
| `fin123 artifact list <dir>` | List versioned artifacts |
| `fin123 gc <dir>` | Garbage collect old builds and artifacts |
| `fin123 import-xlsx <file> <dir>` | Import an Excel workbook |
| `fin123 template list` | List available templates |
| `fin123 events <dir>` | Show structured event log |
| `fin123 ui <dir>` | Launch the browser UI |
| `fin123 demo <name>` | Run a built-in demo |

### Enterprise commands (require fin123-pod)

| Command | Description |
|---------|-------------|
| `fin123 registry status` | Show registry status |
| `fin123 registry sync` | Sync with registry |
| `fin123 plugins list` | List installed plugins |
| `fin123 plugins run <name>` | Run a plugin |
| `fin123 server start` | Start runner service |
| `fin123 server status` | Show runner service status |

Enterprise commands exist in core but return exit code 4 with a clear
"Enterprise feature" message. Install fin123-pod for full functionality.

### Global flags

Every command supports:

| Flag | Description |
|------|-------------|
| `--json` | Machine-readable JSON output |
| `--quiet` | Suppress non-essential output |
| `--verbose` | Verbose diagnostic output |

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Generic error |
| 2 | Invalid usage / bad arguments |
| 3 | Verification failure (hash mismatch, non-determinism) |
| 4 | Enterprise-only feature (core) |
| 5 | Dependency / environment missing |

### JSON output contract

Every command with `--json` prints exactly one JSON object to stdout:

```json
{
  "ok": true,
  "cmd": "doctor",
  "version": "0.3.2",
  "data": { ... },
  "error": null
}
```

## Doctor

`fin123 doctor` runs deterministic preflight and compliance validation:

```
fin123 doctor

Runtime ...................... OK
Determinism engine ........... OK
Floating-point stability ..... OK
Filesystem ................... OK
Locale / encoding ............ OK
Timezone ..................... WARNING (EST)
Dependencies ................. OK
Registry connectivity ........ ENTERPRISE (core)
Plugin integrity ............. ENTERPRISE (core)
Server preflight ............. ENTERPRISE (core)

Overall: PASS (1 warning(s))
```

Checks (core):
1. Runtime integrity (Python version, package version)
2. Determinism engine self-test (hash stability)
3. Floating-point canonicalization (rounding, JSON, hash)
4. Filesystem permissions (temp write/read/cleanup)
5. Encoding / locale safety (UTF-8, decimal format, sort)
6. Timezone validation (warning if not UTC)
7. Dependency integrity (all required modules)

Enterprise checks (stubbed in core, implemented in pod):
8. Registry connectivity
9. Plugin integrity
10. Server preflight

CI usage: `fin123 doctor --json && echo "Preflight passed"`

## Demos

```bash
fin123 demo ai-governance
fin123 demo deterministic-build
fin123 demo batch-sweep
fin123 demo data-guardrails
```

No external data or configuration required.

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

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) -- Core architecture: two-graph model, formula engine, UI, storage, determinism.
- [POD_BOUNDARY.md](POD_BOUNDARY.md) -- What the enterprise Pod layer adds and how it extends Core.
- [docs/formulas_and_views.md](docs/formulas_and_views.md) -- Formula semantics, Excel compatibility, unsupported functions, view sort/filter.
- [docs/RUNBOOK.md](docs/RUNBOOK.md) -- Operational runbook: install, usage, troubleshooting, and release process.
- [CHANGELOG.md](CHANGELOG.md) -- Release notes.

## Enterprise Features

For database-backed registries, headless runner services, connectors (Bloomberg),
plugin marketplace, workflow automation, and SQL sync, see
[fin123-pod](https://github.com/reckoning-machines/fin123-pod) (private, requires license).
Built by Reckoning Machines

## License

Apache-2.0 -- see [LICENSE](LICENSE).
