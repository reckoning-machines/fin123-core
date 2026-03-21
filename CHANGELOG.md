# Changelog

All notable changes to this project will be documented in this file.

## [0.5.0] — 2026-03-20

### Added

- **Terminal Mode** — three UI modes (Spreadsheet, System, Terminal) with
  mode switcher. Terminal provides a deterministic runner shell.
- **`commit` as primary action** — persists state + builds in one step.
  Returns run_id, scalar/table summaries, timestamps.
- **Persistent scenarios** — `scenario save/load/list/show/delete/compare`.
  Stored in `.fin123/scenarios.json`. Scenarios label committed runs.
- **Parameter sweeps** — `sweep <input> <values...>` with `--outputs` selection,
  `range()` syntax, live progress, persisted results, CSV export.
- **Grid sweeps** — `grid <X> <valsX> vs <Y> <valsY> --output <name>`.
  2D parameter experiments with matrix rendering. 100-cell cap.
- **AI Workbench** — provider-backed `ai explain formula/output`,
  `ai draft addin`, `ai revise draft`. Draft → validate → apply lifecycle.
- **Plugin manager** (`fin123.plugins.manager`) — discovers, validates,
  imports, and registers plugins from `plugins/` during builds.
- **Production plugin validator** (`fin123.plugins.validator`) — AST-based
  policy scan: forbidden imports, eval/exec, network patterns, PLUGIN_META
  type checks, register() entrypoint.
- **Draft iteration** — `ai revise draft` creates new drafts linked via
  `derived_from`. `draft lineage` shows revision chains.
- **LLM provider** (`fin123.llm.provider`) — Anthropic + OpenAI via httpx.
  Config via environment variables.
- **Explanation truncation** — long AI explanations truncated to 8 lines.
  `show full last` to expand.
- **Draft code preview** — `draft show` truncates at 40 lines.
  `draft show full` for complete code.
- **Validation recall** — `draft validation <id>` recalls stored result
  without re-running validation.

### Changed

- Version bump to 0.5.0.
- README rewritten for current capabilities.
- Public site (`fin123_public/index.html`) created.
- `docs/concepts.md` and `docs/terminal.md` created.

## [0.4.1] — 2026-03-12

### Added

- **CI pipeline enforcement** — GitHub Actions workflow (`ci.yml`) with three
  jobs: pytest + lint, native bootstrap/smoke/acceptance scripts, and PyInstaller
  smoke build. Runs on every push and PR to `main`.
- **`fin123 doctor --environment`** — new flag to show only namespace and install
  diagnostics: pod presence, core-only status, namespace overlap detection.
- **Import-time namespace guard** — `import fin123` now emits a warning if both
  fin123-core and fin123-pod are installed in the same virtualenv.

### Fixed

- **`fin123 doctor` exit code** — version mismatch between source `__version__`
  and installed package metadata is now a warning, not a failure. Doctor only
  exits non-zero for real operational problems (determinism, dependencies,
  filesystem).

### Documentation

- `docs/CI.md` — CI pipeline contract and job descriptions.
- `docs/INSTALL.md` — namespace note, separate virtualenv guidance.
- `docs/OPERATIONS.md` — environment diagnostics section.

## [0.4.0] — 2026-03-06

### Added

- **Worksheet runtime** — deterministic, read-only projections of build
  output tables. Three new primitives:
  - **ViewTable** — typed, immutable tabular substrate wrapping a Polars
    DataFrame with explicit column schema. Constructed from build run
    parquet outputs via `from_fin123_run()`.
  - **WorksheetView** — declarative YAML spec defining source columns,
    derived columns (row-local expressions), sorts, flags, header groups,
    and display formats. Authored as `<project>/worksheets/*.yaml`.
  - **CompiledWorksheet** — immutable row-oriented JSON artifact with
    structured provenance, inline error objects (`#DIV/0!`, `#NAME?`,
    `#ERR!`), and deterministic `content_hash_data()`.

- **Worksheet CLI** — four new subcommands under `fin123 worksheet`:
  - `compile` — reads a YAML spec + build table, evaluates derived columns
    in dependency-graph order, applies sorts and flags, writes compiled
    artifact.
  - `verify` — validates compiled artifact integrity (provenance, counts,
    content roundtrip). Exit code 3 on failure.
  - `diff` — structural comparison of two compiled artifacts (columns, rows,
    sorts, errors, cell-level data). Uses `row_key` for identity-based
    matching when available.
  - `list` — discovers `*.yaml` specs in `<project>/worksheets/` and reports
    name, title, and column count.

- **Worksheet viewer in local UI** — new Worksheet tab in the browser side
  panel. Compile-on-demand via `POST /api/worksheet/compile`. Read-only DOM
  renderer with sticky headers, grouped headers, display formatting
  (currency, percent, decimal, integer, date), inline error rendering, flag
  indicators, and keyboard-accessible provenance disclosure.

- **Restricted row evaluator** — derived columns and flags use a restricted
  subset of the formula engine. Row-local only. Allowed functions: `IF`,
  `IFERROR`, `ISERROR`, `AND`, `OR`, `NOT`, `SUM`, `AVERAGE`, `MIN`, `MAX`,
  `ABS`, `ROUND`, `DATE`, `YEAR`, `MONTH`, `DAY`, `EOMONTH`. Cell
  references, range references, `VLOOKUP`, `SUMIFS`, `PARAM`, and cross-row
  operations are rejected at validation time.

- **Dependency-ordered evaluation** — derived columns may reference other
  derived columns. The compiler builds a dependency graph and evaluates in
  topological order. Cycles are a hard compile error. Display order always
  matches the spec's column order.

- **Demo worksheet spec** — `demo_fin123` template now includes
  `worksheets/valuation_review.yaml` (7 columns, sorts, flags, header
  groups) that compiles against the `priced_estimates` build output.

### Documentation

- `docs/worksheets.md` — full worksheet specification (ViewTable,
  WorksheetView, CompiledWorksheet, CLI workflow, expression language,
  v1 boundaries).
- `ARCHITECTURE.md` — new section covering worksheet runtime primitives,
  evaluation, CLI, UI, and key files.
- `docs/CLI_SPEC.md` — worksheet subcommands added to command tree.
- `demo_fin123/README.md` — worksheet quick-start steps added.

### v1 Constraints

Worksheets in v1 are deterministic projections only. No filtering, no
client-side sorting, no cross-row expressions, no scalar context injection,
no multi-worksheet composition, no client-side editing, no remote
compilation, no charts or visualizations.

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
