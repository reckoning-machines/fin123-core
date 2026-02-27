# Formula Functions & View Sort/Filter

## Error Semantics

`ISERROR` and `IFERROR` both use the centralized `ENGINE_ERRORS` tuple
(`formulas/errors.py`) to decide which Python exceptions count as "error
values". The tuple includes:

- `FormulaError` (and subclasses: `FormulaRefError`, `FormulaFunctionError`)
- `ZeroDivisionError`
- `ValueError`
- `KeyError`
- `TypeError`

Any exception **not** in this tuple will propagate to the caller.

### Cell Display Codes

When a formula evaluation fails, `CellGraph.get_display_value()` maps the
stored error message to a canonical display code:

| Code | Trigger | Pattern matched |
|------|---------|-----------------|
| `#CIRC!` | Circular reference | `CellCycleError` (caught directly) |
| `#NAME?` | Unknown function | `"Unknown function"` in error message |
| `#REF!` | Unresolved reference | `"Unknown reference"` in error message |
| `#DIV/0!` | Division by zero | `"Division by zero"` in error message |
| `#NUM!` | Numeric / convergence error | `"did not converge"` in error message |
| `#ERR!` | Any other error | Fallback |

## Formula Functions

### Logical Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| AND | `AND(val1, val2, ...)` | TRUE if all arguments are truthy |
| OR | `OR(val1, val2, ...)` | TRUE if any argument is truthy |
| NOT | `NOT(val)` | Inverts a boolean value |

### Error Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| ISERROR | `ISERROR(expr)` | TRUE if the expression raises any error (division by zero, missing ref, etc.). Lazy — catches errors without propagating. |

### Date Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| DATE | `DATE(year, month, day)` | Construct a `datetime.date` |
| YEAR | `YEAR(date)` | Extract year from a date |
| MONTH | `MONTH(date)` | Extract month (1-12) |
| DAY | `DAY(date)` | Extract day of month |
| EOMONTH | `EOMONTH(start_date, months)` | End of month offset by N months |

**Date representation:** Dates are stored as Python `datetime.date` objects. The `_coerce_date()` helper accepts:
- `datetime.date` objects (passthrough)
- ISO format strings (`"YYYY-MM-DD"`)
- Excel serial numbers (int/float, where 1 = 1900-01-01)

**JSON serialization** is handled by the existing `default=str` in `versioning.py`.

**Display:** `CellGraph.get_display_value()` formats dates as ISO strings (`2024-03-15`).

**Differences from Excel:**
- No TIME component — dates only, no datetime
- EOMONTH returns a `datetime.date`, not a serial number
- Date arithmetic uses Python operators, not serial number math

### Lookup Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| MATCH | `MATCH(value, "table", "column")` | 1-based row index of first match |
| INDEX | `INDEX("table", "column", row_num)` | Value at 1-based row position |
| XLOOKUP | `XLOOKUP(value, "table", "lookup_col", "return_col" [, default])` | Search + return with optional default |

All lookup functions use the `table_cache` (materialized Polars DataFrames from the most recent build run).

**Duplicate key handling:** When the lookup column contains duplicate values,
`MATCH` and `XLOOKUP` always return the **first** matching row (lowest row
index). This is deterministic and matches Excel's default top-to-bottom scan.

**Differences from Excel:**
- MATCH searches a named table column, not a range
- INDEX takes a table name + column name, not a range reference
- XLOOKUP optional 5th argument is a literal default value (no match_mode/search_mode)

### Finance Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| NPV | `NPV(rate, cf1, cf2, ...)` | Net present value (discounts from t=1) |
| IRR | `IRR(cf0, cf1, cf2, ...)` | Internal rate of return |
| XNPV | `XNPV(rate, "table", "dates_col", "values_col")` | NPV with specific dates |
| XIRR | `XIRR("table", "dates_col", "values_col")` | IRR with specific dates |

**NPV semantics:** Matches Excel — discounts from t=1. The first cashflow is at t=1, not t=0. To include a t=0 investment, add it separately: `=NPV(rate, cf1, cf2) + cf0`.

**IRR solver:** Newton-Raphson with initial guess of 10%, falling back to bisection on [-0.99, 10.0] if Newton fails.

**Day-count convention:** XNPV and XIRR use Actual/365.

**No scipy dependency** — all numerical methods are implemented from scratch.

**IRR/XIRR convergence policy:** Newton-Raphson (guess = 0.1, max 100 iterations)
followed by bisection fallback on [-0.99, 10.0] (max 200 iterations). If neither
converges, `FormulaFunctionError("did not converge")` is raised. Use
`ISERROR(IRR(...))` or `IFERROR(IRR(...), default)` to handle gracefully.

### Unsupported Functions

The following Excel functions are intentionally **not** implemented:

| Function | Reason |
|----------|--------|
| `OFFSET` | Requires range semantics (volatile, non-deterministic shape) |
| `INDIRECT` | Requires runtime string-to-reference resolution |
| `NOW` | Non-deterministic (violates build reproducibility) |
| `TODAY` | Non-deterministic |
| `RAND` | Non-deterministic |
| `RANDBETWEEN` | Non-deterministic |

Calling any of these raises `FormulaFunctionError("Unknown function")`.

## View Sort/Filter

### Architecture

The view sort/filter system is a **FORMAT-layer** feature — it operates
exclusively on the display side and never modifies the underlying CALC-layer
data. All transforms produce new DataFrames via Polars operations; the
canonical table stored in the build run is never mutated.

**Components:**
- `view_transforms.py` — Pydantic models + `apply_view_transforms()` function
- `POST /api/outputs/table/view` — New endpoint (existing `GET /api/outputs/table` untouched)
- `app.js` — Client-side sort/filter UI state (in-memory, never persisted)

### Filter Types

| Type | Discriminator | Fields |
|------|--------------|--------|
| NumericFilter | `"numeric"` | `column`, `op` (=, <>, >, <, >=, <=), `value` |
| BetweenFilter | `"between"` | `column`, `low`, `high` |
| TextFilter | `"text"` | `column`, `op` (contains, starts_with, ends_with, equals), `value`, `case_sensitive` |
| ValueListFilter | `"value_list"` | `column`, `values` |
| BlanksFilter | `"blanks"` | `column`, `show_blanks` |

### Transform Order

1. Inject `__view_row_idx__` for deterministic tie-breaking
2. Apply filters (in order)
3. Apply sorts (with `nulls_last=True`, `maintain_order=True`)
4. Drop internal `__view_row_idx__` column

### UI Interactions

- **Sort:** Click column header cycles: ascending → descending → off
- **Filter:** Right-click column header opens floating filter panel
- **Toolbar:** Active sorts/filters shown as chips with × clear buttons
- **Clear All:** Single button resets all transforms

### Promote to Model Step

The `TableViewRequest` model maps directly to existing functions in `functions/table.py`:
- `SortSpec` → `table_sort(lf, by=[col], descending=[bool])`
- `NumericFilter` → `table_filter(lf, column, op, value)`

This enables a future "promote view to step" feature that converts ad-hoc view transforms into permanent model pipeline steps.
