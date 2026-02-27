# Changelog

All notable changes to this project will be documented in this file.

## [0.3.1] — 2026-02-27

### Added

- **Formula functions (16 new):**
  - Logical: `AND`, `OR`, `NOT`
  - Error handling: `ISERROR`
  - Date: `DATE`, `YEAR`, `MONTH`, `DAY`, `EOMONTH`
  - Lookup: `MATCH`, `INDEX`, `XLOOKUP`
  - Finance: `NPV`, `IRR`, `XNPV`, `XIRR`
- **View sort/filter** — read-only table transforms in the browser UI
  (`POST /api/outputs/table/view`). Sort by column header click, filter
  by right-click. Five filter types: numeric, between, text, value list,
  blanks.
- `ENGINE_ERRORS` tuple in `formulas/errors.py` — single source of truth
  for which Python exceptions ISERROR / IFERROR treat as error values.
- **Deterministic error display codes** — `#NAME?`, `#REF!`, `#DIV/0!`,
  `#NUM!` now shown in `CellGraph.get_display_value()` instead of a
  generic `#ERR!` for all non-circular errors.

### Fixed

- **IFERROR now catches `TypeError`** — previously IFERROR and ISERROR
  used different exception tuples; both now reference `ENGINE_ERRORS`.

### Documentation

- `docs/formulas_and_views.md` — formula semantics, date representation,
  lookup duplicate policy, IRR/XIRR convergence, view sort/filter design.
- `CHANGELOG.md` (this file).

### Unsupported Functions

The following Excel functions are intentionally unsupported and will raise
`FormulaFunctionError("Unknown function")`:

`OFFSET`, `INDIRECT`, `NOW`, `TODAY`, `RAND`, `RANDBETWEEN`

These are either non-deterministic or require range semantics that the
engine does not implement.

## [0.3.0] — 2026-02-20

### Added

- XLSX import with formula classification.
- Templates (`single_company`, `comparables`, `dcf`).
- Batch builds from parameter CSV.
- Artifact approval workflow.
- Structured event log and run-log commands.
