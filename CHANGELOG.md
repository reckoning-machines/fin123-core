# Changelog

All notable changes to this project will be documented in this file.

## [0.3.4] — 2026-03-03

### Fixed

- **Bundled demos as `fin123.demos`** — demos are now packaged inside the
  `fin123` package (`src/fin123/demos/`) so `fin123 demo <name>` works in
  installed environments without a source checkout. Resolves
  `ModuleNotFoundError: No module named 'demos'`.

## [0.3.3] — 2026-03-01

### Added

- **Unified CLI entrypoint** — single `fin123` command replaces `fin123-core`.
  All subcommands (`init`, `build`, `commit`, `verify`, `demo`, `doctor`, etc.)
  available under one executable.
- **`fin123 doctor`** — deterministic preflight validation with JSON contract
  output (`--json`), structured exit codes, and enterprise-stub checks.
- **CLI specification** — `docs/CLI_SPEC.md` documents the full command tree,
  exit codes, JSON output contract, and doctor checks.
- **CI contract drift check** — workflow step validates CLI spec stays in sync
  with implemented commands.

## [0.3.2] — 2026-02-28

### Added

- **Demo suite** — four built-in demos runnable via `fin123-core demo <name>`:
  - `ai-governance` — plugin validation and compliance report.
  - `deterministic-build` — scaffold, build, verify with stable hashes.
  - `batch-sweep` — 3-scenario parameter grid with stable manifest.
  - `data-guardrails` — join validation failures and success cases.
- **Demo test coverage** — `tests/test_demos.py` and
  `tests/test_ai_governance_demo.py` verify demo correctness and
  deterministic output (no timestamps or run IDs in expected values).

### Fixed

- **Deterministic demo outputs** — all demo scripts produce stable,
  hash-verifiable results with no non-deterministic fields in output.

### Documentation

- Updated `README.md` with demo commands and current install instructions.
- Updated `docs/RUNBOOK.md` with demo execution, determinism verification,
  installer build procedures, release publishing, and troubleshooting.

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
