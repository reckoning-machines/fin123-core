# Runbook — fin123-core

Operational guide for installing, running, and troubleshooting fin123-core.

## Prerequisites

- Python 3.11 or later.
- No database required. No network required.

## Install

### From PyPI

```bash
pip install fin123-core

# With XLSX import support
pip install "fin123-core[xlsx]"
```

### From source

```bash
git clone https://github.com/reckoning-machines/fin123-core.git
cd fin123-core
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Verify installation

```bash
fin123-core --version
fin123-core template list
```

## Create a Project

```bash
# From a template
fin123-core new my_model --template single_company --set ticker=AAPL

# From the demo template
fin123-core new demo --template demo_fin123
```

This creates a project directory with `workbook.yaml`, `fin123.yaml`, and sample input files.

## Build Lifecycle

### 1. Edit

Edit `workbook.yaml` directly or use the browser UI (`fin123-core ui <dir>`).

### 2. Commit

```bash
fin123-core commit my_model
```

Writes the current workbook to `workbook.yaml` and creates an immutable snapshot
(`snapshots/workbook/vXXXX/workbook.yaml`).

### 3. Build

```bash
fin123-core build my_model

# With parameter overrides
fin123-core build my_model --set tax_rate=0.25

# With a named scenario
fin123-core build my_model --scenario bear_case
```

Evaluates scalar and table graphs, writes outputs to `runs/<timestamp>_run_<n>/`.

### 4. Verify

```bash
fin123-core verify-build my_model
```

Recomputes hashes for the latest build. Reports pass/fail for spec hash, input hashes,
params hash, and export hashes.

## Browser UI

```bash
fin123-core ui my_model

# Specify port, skip auto-open
fin123-core ui my_model --port 8080 --no-open
```

The UI runs on localhost only. Key shortcuts:

| Shortcut | Action |
|----------|--------|
| Ctrl+S | Commit snapshot |
| Ctrl+Enter | Build workbook |
| Ctrl+O | Import XLSX |
| Ctrl+B | Toggle side panel |
| Ctrl+P | Toggle dependency highlight |
| ? | Keyboard help overlay |
| E | Toggle errors panel |

## Batch Builds

```bash
fin123-core batch build my_model --params-file params.csv

# With parallelism
fin123-core batch build my_model --params-file params.csv --max-workers 4
```

The CSV must have one column per parameter. One build per row.

## Diff

```bash
# Compare two builds
fin123-core diff run <run_a> <run_b> --project my_model

# Compare two workbook versions
fin123-core diff version v0001 v0002 --project my_model

# Machine-readable output
fin123-core diff run <run_a> <run_b> --project my_model --json
```

## XLSX Import

```bash
fin123-core import-xlsx model.xlsx my_model
```

Imports worksheets, cell values, formulas (as-is), and font colors. Writes an import
report to `import_reports/`. Formulas are classified as supported, unsupported, parse_error,
external_link, or plugin_formula.

## Garbage Collection

```bash
# Dry run (report only)
fin123-core gc my_model --dry-run

# Actually delete
fin123-core gc my_model

# Clear hash cache too
fin123-core clear-cache my_model
```

Configure limits in `fin123.yaml`:

```yaml
max_runs: 50
max_artifact_versions: 20
max_total_run_bytes: 2000000000   # 2 GB
ttl_days: 30
```

## Troubleshooting

### `fin123-core: command not found`

Ensure the package is installed and your PATH includes pip's script directory:

```bash
pip show fin123-core
python -m fin123.cli_core --help    # fallback
```

### Build fails with "uncommitted edits"

The UI has unsaved changes. Commit first:

```bash
fin123-core commit my_model
```

### Formula parse errors

Check the formula syntax. fin123-core uses a Lark LALR(1) parser, not Excel's parser.
Common issues:
- Range expressions (`A1:A10`) are not supported — use named ranges instead.
- `INDIRECT()`, `OFFSET()` are not supported.
- Ensure function names are uppercase (`SUM`, not `sum`).

### Large import (>20k cells) is slow

Configure limits in `fin123.yaml`:

```yaml
max_import_rows_per_sheet: 500
max_import_cols_per_sheet: 100
max_import_total_cells: 500000
```

### Build outputs differ across machines

Verify inputs are identical. Check `run_meta.json` → `input_hashes` for each build.
fin123-core guarantees deterministic outputs for identical inputs, but different input
file contents will produce different results.

### UI won't start (port in use)

```bash
fin123-core ui my_model --port 9999
```

### Polars version mismatch

fin123-core requires `polars>=1.0`. Check:

```bash
python -c "import polars; print(polars.__version__)"
```
