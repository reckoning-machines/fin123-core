# Worksheets

Deterministic, read-only projections of build outputs. A worksheet takes a
table from a completed build run, applies derived columns, sorts, and flags,
and produces an immutable compiled artifact.

Lineage: Lotus 1-2-3 deterministic worksheet, not BI dashboard.

## Core Concepts

### ViewTable

A typed, immutable tabular substrate. Wraps a Polars DataFrame with an
explicit column schema. Constructed from build run outputs (parquet files),
JSON records, or raw DataFrames.

```python
from fin123.worksheet.view_table import from_fin123_run

vt = from_fin123_run("./my_project", "priced_estimates")
# vt.columns -> ['ticker', 'date', 'px_last', 'eps_ntm', 'rev_ntm']
# vt.row_count -> 20
```

Key properties:
- Immutable after construction (raises on `setattr`/`delattr`).
- Schema must match the DataFrame exactly (column names, types).
- Optional `row_key` column (must be unique, non-null) for stable row identity.
- `source_label` recorded in provenance.

### WorksheetView

A declarative YAML spec that describes how to project a ViewTable into a
compiled worksheet. Authored as a YAML file in `<project>/worksheets/`.

```yaml
name: valuation_review
title: "Valuation Review"

columns:
  - source: ticker
    label: Ticker
  - source: px_last
    label: Price
    display_format: { type: currency, symbol: "$", places: 2 }
  - name: pe_ratio
    expression: "px_last / eps_ntm"
    label: "P/E Ratio"
    display_format: { type: decimal, places: 1 }

sorts:
  - column: pe_ratio

flags:
  - name: high_pe
    expression: "pe_ratio > 28"
    severity: warning
    message: "P/E above 28"

header_groups:
  - label: Valuation
    columns: [pe_ratio]
```

**Column types:**

| Kind | Fields | Description |
|------|--------|-------------|
| Source | `source`, `label?`, `display_format?`, `column_type?` | Column pulled from the ViewTable |
| Derived | `name`, `expression`, `label?`, `display_format?`, `column_type?` | Column computed via row-local formula |

**Display formats:** `decimal`, `percent`, `currency`, `integer`, `date`, `text`.

**Sorts:** Applied at compile time. Output rows are in sort order.

**Flags:** Row-local boolean expressions. Triggered flags attach to rows with
a severity (`info`, `warning`, `error`) and optional message.

**Header groups:** Single-level column grouping for rendering.

### CompiledWorksheet

The immutable output artifact. Row-oriented JSON, deterministic, diffable.

```
{
  "name": "valuation_review",
  "title": "Valuation Review",
  "columns": [...],
  "sorts": [...],
  "header_groups": [...],
  "rows": [
    {"ticker": "AAPL", "px_last": 180.0, "pe_ratio": 25.4, ...},
    ...
  ],
  "flags": [[], [{"name": "high_pe", "severity": "warning", ...}], ...],
  "provenance": {
    "view_table": {"source_label": "...", "row_key": "ticker", ...},
    "compiled_at": "2025-06-15T12:00:00+00:00",
    "fin123_version": "0.1.0",
    "spec_name": "valuation_review",
    "row_count": 20,
    "column_count": 7,
    "columns": {
      "ticker": {"type": "source", "source_column": "ticker"},
      "pe_ratio": {"type": "derived", "expression": "px_last / eps_ntm"}
    }
  },
  "error_summary": null
}
```

Key properties:
- Provenance is always present (source, version, spec, counts, per-column lineage).
- `content_hash_data()` excludes `compiled_at` for deterministic comparison.
- Inline error objects (`{"error": "#DIV/0!"}`) for failed derived expressions.
- `error_summary` aggregates error counts by column (null when no errors).

## CLI Workflow

```bash
# 1. List available worksheet specs
fin123 worksheet list --project .

# 2. Compile a worksheet from a spec + build table
fin123 worksheet compile worksheets/valuation_review.yaml \
  --table priced_estimates --project .

# 3. Verify artifact integrity
fin123 worksheet verify valuation_review.worksheet.json

# 4. Compare two compiled worksheets
fin123 worksheet diff v1.worksheet.json v2.worksheet.json
```

All commands support `--json` for machine-readable output and `--quiet` for
silent operation.

### compile

Reads a YAML spec and a table from the latest (or specified) build run.
Validates the spec against the table schema, evaluates derived columns in
dependency order, applies sorts and flags, and writes the compiled artifact.

Options: `--table` (required), `--project`, `--run`, `--output`.

### verify

Checks a compiled artifact: parses JSON, validates provenance, checks
column/row count consistency, verifies content roundtrip integrity.
Exit code 3 on failure.

### diff

Structural comparison of two artifacts: column changes, row count changes,
sort changes, error count changes, cell-level data diff. Uses `row_key` for
identity-based matching when available, falls back to positional.

### list

Discovers `*.yaml` files in `<project>/worksheets/` and reports spec name,
title, and column count.

## Local UI

The worksheet viewer is accessible as a tab in the fin123 local browser UI:

1. Click the **Worksheet** tab in the side panel.
2. Select a spec from the dropdown (populated from `worksheets/*.yaml`).
3. Select a table from the dropdown (populated from build output tables).
4. Click **Compile**.

The server compiles on demand via `POST /api/worksheet/compile` and the
DOM renderer (`worksheet_viewer.js`) mounts the result inline.

The renderer is read-only: semantic HTML table with sticky headers, grouped
headers, display formatting, inline error rendering, flag indicators, and
a keyboard-accessible provenance disclosure.

## Derived Expression Language

Derived columns and flags use a restricted subset of the fin123 formula
engine. Expressions are **row-local only** — each cell is evaluated
independently using values from the current row.

### Allowed functions

`IF`, `IFERROR`, `ISERROR`, `AND`, `OR`, `NOT`, `SUM`, `AVERAGE`, `MIN`,
`MAX`, `ABS`, `ROUND`, `DATE`, `YEAR`, `MONTH`, `DAY`, `EOMONTH`.

### Disallowed

Any function not in the allowlist is rejected at validation time. Notably:
`VLOOKUP`, `SUMIFS`, `COUNTIFS`, `XLOOKUP`, `MATCH`, `INDEX`, `NPV`, `IRR`,
`XNPV`, `XIRR`, `PARAM`. Cell references (`A1`, `Sheet1!A1`) and range
references are also rejected.

### Error handling

- Division by zero produces `{"error": "#DIV/0!"}`.
- Reference to undefined name produces `{"error": "#NAME?"}`.
- Type errors produce `{"error": "#ERR!"}`.
- `IFERROR(expr, fallback)` catches errors and returns the fallback.
- Errors propagate through dependent derived columns.

### Dependency ordering

Derived columns may reference other derived columns (including forward
references in the spec). The compiler builds a dependency graph and evaluates
in topological order. Cycles are a hard compile error.

Display order in the output always matches the spec's column order.
Evaluation order is determined by the dependency graph.

## v1 Boundaries

What worksheets are in v1:

- Deterministic projection of build output tables.
- Row-local derived expressions with restricted function allowlist.
- Compile-time sorts, flags, header groups.
- Immutable compiled artifacts with structured provenance.
- CLI commands: compile, verify, diff, list.
- Read-only DOM renderer in local UI.

What worksheets are **NOT** in v1:

- **Not a BI tool.** No drag-and-drop, no ad-hoc queries, no OLAP.
- **Not a dashboard builder.** No charts, no visualizations, no widgets.
- **No filtering.** WorksheetView has no filter clause. All source rows are projected.
- **No client-side sorting or filtering.** Sort order is compile-time only.
- **No scalar context injection.** Expressions cannot reference workbook params or scalars.
- **No cross-row expressions.** No running totals, no lag/lead, no window functions.
- **No worksheet store or registry.** Specs are plain YAML files. Artifacts are plain JSON files.
- **No multi-worksheet composition.** Each worksheet is compiled independently.
- **No client-side editing.** The rendered worksheet is read-only.
- **No remote or headless compilation.** Worksheets compile locally.
