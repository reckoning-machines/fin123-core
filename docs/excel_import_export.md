# Excel Import & Export

How fin123 converts Excel workbooks into fin123 projects (import) and
exports computed run results back out (export).

## Import

### Entry Points

**CLI:**

```bash
fin123 import-xlsx <xlsx_file> <directory> [--max-rows 500] [--max-cols 100]
```

**UI:**

```
POST /api/import/xlsx   (multipart file upload, max 10 MB)
```

The CLI creates a new fin123 project directory from an `.xlsx` file.
The UI endpoint does the same via file upload and derives the project name
from the filename (or an explicit parameter).

### Dependencies

`openpyxl` is an optional dependency.  Install with:

```bash
pip install 'fin123[xlsx]'
```

If missing, import raises `ImportError` (exit code 5 from CLI).

### Processing Pipeline

1. **Load workbook** -- `openpyxl.load_workbook(data_only=False)` preserves
   formulas rather than cached values.

2. **Detect unsupported features** -- VBA macros and chart sheets are
   detected and logged in `skipped_features` but not imported.
   Conditional formatting and data validations are noted per-sheet as
   warnings.

3. **Iterate sheets** -- Each worksheet is processed up to `max_rows` x
   `max_cols` (defaults: 500 rows, 100 columns).  If the sheet exceeds
   these limits the extra rows/columns are truncated and a warning is
   emitted.  A global `max_total_cells` limit (default 500k) halts
   processing if exceeded.

4. **Extract cells** -- For each non-empty cell:
   - **Formulas** (data_type `f` or value starting with `=`) are stored
     as-is with the leading `=`.
   - **Values** -- numbers, booleans, strings, and dates (converted to
     string) are stored directly.
   - **Font color** -- Non-black RGB font colors are extracted to
     `#rrggbb` hex and stored in the sheet's `fmt` map.

5. **Classify formulas** -- Every formula is classified into one of five
   categories (see below).

6. **Write project** -- Creates `workbook.yaml`, `fin123.yaml` (if new),
   and standard directories (`inputs/`, `runs/`, `artifacts/`,
   `snapshots/`, `cache/`).

7. **Snapshot** -- A versioned snapshot is saved
   (`snapshots/workbook/vXXXX/workbook.yaml`).

8. **Generate report** -- A detailed import report and trace log are
   written (see "Import Report" below).

### Formula Classification

Every imported formula is classified by `classify_formula()`:

| Classification | Meaning |
|---|---|
| `supported` | Parses and uses only fin123-supported functions (SUM, IF, VLOOKUP, etc.) |
| `unsupported_function` | Parses but calls functions fin123 doesn't implement (e.g. OFFSET, INDEX) |
| `parse_error` | Cannot be parsed -- syntax issues or special characters |
| `external_link` | References external workbooks, URLs, or UNC paths |
| `plugin_formula` | Contains vendor-specific functions (Bloomberg BDH/BDP/BDS, VA_* functions) |

Classification order: external link check -> plugin prefix check -> Lark
LALR(1) parse -> function-name validation against the fin123 function
registry.

### Unicode & Special Characters

Excel formulas sometimes contain non-ASCII characters (smart quotes, en
dashes, non-breaking spaces).  The importer detects these and records them
in the trace log.  A sanitized preview is generated for diagnostics
(e.g. U+2212 MINUS SIGN -> `-`, U+00A0 NBSP -> space).

### Import Report

Each import creates a versioned report directory:

```
import_reports/
  index.json                        # history of all imports
  20250302T120000Z_import_1/
    import_report.json              # full structured report
    import_trace.log                # human-readable diagnostics
    source_filename.txt             # original xlsx filename
```

A backward-compatible copy is also written at the project root as
`import_report.json`.

**Report contents:**

- `source` -- path to the original xlsx file
- `sheets_imported[]` -- per-sheet cell/formula/color counts and
  classification breakdown
- `classification_summary` -- aggregate formula classification counts
- `top_unsupported_functions` -- most common unsupported functions (max 20)
- `formula_classifications[]` -- per-formula detail including functions
  used, error messages, repr, non-ASCII detection, and sanitized preview
- `skipped_features` -- features detected but not imported (VBA, charts)
- `warnings` -- truncation notices, large import notices, etc.

### Limits & Configuration

Limits can be set in `fin123.yaml`:

```yaml
max_import_rows_per_sheet: 500      # default
max_import_cols_per_sheet: 100      # default
max_import_total_cells: 500000      # default
```

CLI flags `--max-rows` and `--max-cols` override the config values.

### What Is Not Imported

- Images, hyperlinks, comments
- Cell borders, styles (except font color)
- Merged cells, hidden rows/columns
- Print settings, pivot tables
- VBA macros and chart sheets (detected, skipped)

---

## Export

### Entry Point

```bash
fin123 export <directory> [--format json|csv|xlsx] [--out <path>]
```

Export reads outputs from the **latest run** in the project.

### Data Sources

After a `fin123 build`, results are stored in the run directory:

- `runs/<run_id>/outputs/scalars.json` -- scalar evaluation results
- `runs/<run_id>/outputs/<table>.parquet` -- table evaluation results
  (Polars Parquet format)

### Output Formats

**JSON (default):**

Scalars emitted directly, tables summarised by row count.  With `--json`
the output follows the standard contract envelope:

```json
{
  "ok": true,
  "cmd": "export",
  "version": "...",
  "data": {
    "run_id": "...",
    "timestamp": "...",
    "format": "json",
    "scalars": { "revenue": 1000000 },
    "tables": { "forecast": { "rows": 12 } }
  },
  "error": null
}
```

**CSV:**

Each table is written as a separate `.csv` file via Polars.

**XLSX:**

Tables written to sheets via Polars.

### Error Handling

| Condition | Behaviour |
|---|---|
| No runs found | Exit code 1, error message |
| Missing outputs directory | Empty data returned |
| Corrupted parquet | Exception propagated |
