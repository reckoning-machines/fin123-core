# fin123 Demo/Templates Audit

## Purpose
This document inventories the existing demo/template set in the repo and defines the target demo set we want to ship, with minimal code changes and strict determinism.

---

## Definitions (Current vs Target)

### Template
- A `fin123-core new ... --template <name>` project scaffold.
- Ships with a minimal runnable model.
- Should be deterministic and reproducible.

### Demo
- A runnable, time-bounded (20-30 min) showcase with:
  - a deterministic workflow
  - predictable files/artifacts produced
  - CLI entrypoint(s) that run end-to-end
- May be backed by a template, but must have a scripted, deterministic "happy path."

---

## Current Repo Inventory

### Templates list (as implemented)
- Command to list templates:
  - `fin123-core template list` (text output)
  - `fin123-core template list --json` (JSON output)
  - `fin123-core template show <name>` (show template metadata and file tree)
- Templates found (3 bundled in core):
  - `single_company` -> `src/fin123/templates/single_company/` -> Single-company financial model with scenarios, assertions, and verify
  - `universe_batch` -> `src/fin123/templates/universe_batch/` -> Parameterized model with batch builds across a universe of tickers
  - `demo_fin123` -> `src/fin123/templates/demo_fin123/` -> Demo template showcasing the full fin123 lifecycle: build, verify, diff, release

For each template below, record:

#### Template: single_company
- Location: `src/fin123/templates/single_company/`
- Entry command: `fin123-core new <dir> --template single_company [--set ticker=AAPL]`
- Files:
  - `template.yaml` (removed after scaffold)
  - `workbook.yaml` (version 1; params: ticker, currency, tax_rate, revenue_growth)
  - `fin123.yaml` (project config: max_runs=50, mode=dev)
  - `inputs/assumptions.csv`
- Inputs (CSV/Parquet):
  - `inputs/assumptions.csv` (8 rows: category, item, amount; revenue/cost/opex categories)
- Builds produce:
  - `runs/<timestamp>_run_<n>/run_meta.json`
  - `runs/<timestamp>_run_<n>/outputs/scalars.json` (base_revenue, projected_revenue, net_income, margin)
  - `runs/<timestamp>_run_<n>/outputs/summary.parquet` (grouped by category)
  - Snapshots in `snapshots/workbook/v0001/workbook.yaml`
- Verify-build: `fin123-core verify-build <run_id> --project <dir>` -- checks workbook_spec_hash, input_hashes, export_hash, params_hash, row counts
- Scenarios: base (revenue_growth=0.05), bull (0.10), bear (-0.02)
- Assertions: net_income_positive (error), margin_reasonable (warn)
- Template params: ticker (string, default ACME), company_name (string, default ACME), currency (string, default USD)
- Notes on determinism issues:
  - Run directory names contain wall-clock timestamps (by design; not in export_hash)
  - group_agg uses `maintain_order=True` for determinism
  - Tables without explicit sort get deterministic secondary sort on all columns
  - JSON outputs use sort_keys=True, separators=(",",":")
- Gaps vs target demos:
  - No scripted end-to-end demo runner
  - No deterministic summary JSON (only run_meta.json which contains timestamps)

#### Template: universe_batch
- Location: `src/fin123/templates/universe_batch/`
- Entry command: `fin123-core new <dir> --template universe_batch [--set universe_name=sp5_demo]`
- Files:
  - `template.yaml` (removed after scaffold)
  - `workbook.yaml` (version 1; params: ticker, weight, universe)
  - `fin123.yaml` (project config: max_runs=200, mode=dev)
  - `inputs/universe.csv`
  - `inputs/params.csv`
- Inputs (CSV/Parquet):
  - `inputs/universe.csv` (5 rows: ticker, sector, weight; AAPL/MSFT/GOOGL/AMZN/META)
  - `inputs/params.csv` (5 rows: ticker, weight; for batch builds)
- Builds produce:
  - `runs/<timestamp>_run_<n>/run_meta.json`
  - `runs/<timestamp>_run_<n>/outputs/scalars.json` (ticker_label, weight_pct, universe_label)
  - `runs/<timestamp>_run_<n>/outputs/ticker_row.parquet` (filtered row for ticker)
  - Plans: ticker_row (filter universe by $ticker)
- Verify-build: `fin123-core verify-build <run_id> --project <dir>`
- Batch build: `fin123-core batch build <dir> --params-file inputs/params.csv`
- Template params: universe_name (string, default sp5_demo)
- Notes on determinism issues:
  - batch.py generates a UUID for build_batch_id (non-deterministic batch ID; not in export hashes)
  - Run directory timestamps are non-deterministic (by design)
- Gaps vs target demos:
  - No scripted batch sweep demo
  - No deterministic batch_manifest.json output

#### Template: demo_fin123
- Location: `src/fin123/templates/demo_fin123/`
- Entry command: `fin123-core new <dir> --template demo_fin123 [--set ticker=AAPL]`
- Files:
  - `template.yaml` (removed after scaffold)
  - `workbook.yaml` (version 1; params: ticker, multiple, discount_rate)
  - `fin123.yaml` (project config: max_runs=50, mode=dev)
  - `inputs/prices.parquet`
  - `inputs/estimates.parquet`
  - `workflows/build_and_verify.yaml`
  - `workflows/scenario_fail.yaml`
  - `README.md`
- Inputs (CSV/Parquet):
  - `inputs/prices.parquet` (parquet format)
  - `inputs/estimates.parquet` (parquet format)
- Builds produce:
  - `runs/<timestamp>_run_<n>/run_meta.json`
  - `runs/<timestamp>_run_<n>/outputs/scalars.json` (eps, implied_value, discounted_value)
  - `runs/<timestamp>_run_<n>/outputs/priced_estimates.parquet` (join_left prices+estimates, sorted by ticker,date)
  - Plans: priced_estimates (join_left + sort)
  - Sheets: Valuation (with PARAM() proxies)
- Verify-build: `fin123-core verify-build <run_id> --project <dir>`
- Assertions: eps_positive (error), discount_rate_sane (error), valuation_positive (warn)
- Workflows: build_and_verify (build then verify), scenario_fail (discount_rate=0.95 triggers assertion failure)
- Template params: ticker (string, default AAPL)
- Notes on determinism issues:
  - Parquet inputs are binary-stable across runs
  - join_left with validate=many_to_one ensures no surprise row multiplication
  - sort by [ticker, date] ensures stable row order
  - Run directory timestamps are non-deterministic (by design)
- Gaps vs target demos:
  - Has a README with manual workflow steps but no automated demo runner
  - Workflows require fin123-pod to execute (not available in core-only)
  - No data guardrails failure demonstration

---

## Existing "Demo" implementations (if any)
List any non-template demos (scripts / CLI commands / docs) already present.

- Name: demo_fin123 template README walkthrough
- Location: `src/fin123/templates/demo_fin123/README.md`
- How to run: Manual sequence: `fin123-core new`, `commit`, `build`, `verify-build`, `diff run`
- Outputs: Run directories with scalars.json, priced_estimates.parquet, verify_report.json
- Notes: Manual steps only; no single CLI entrypoint. `release` and `workflow run` steps require fin123-pod.

- Name: project.py scaffold_project() demo workbook
- Location: `src/fin123/project.py` (used by `fin123-core new <dir>` without --template)
- How to run: `fin123-core new <dir>` (creates a default demo project identical to demo_fin123 content)
- Outputs: Same as demo_fin123 template
- Notes: Legacy scaffold; duplicates demo_fin123 template functionality.

---

## Gaps Summary (Current)
### What exists but is too basic
- demo_fin123 template has a README walkthrough but no scripted, single-command demo
- universe_batch has batch build support but no packaged demo with deterministic manifest output
- single_company has scenario support but no automated demo runner

### What is missing entirely
- No `fin123-core demo <name>` CLI command group
- No AI governance / plugin validation demo
- No data guardrails demo with intentional failure fixtures
- No deterministic summary JSON outputs (without timestamps) for demos
- No batch_manifest.json for batch sweep results
- No compliance_report_output.json for AI governance
- No plugin validator module in core

### Determinism violations (if any)
- Run directory names embed wall-clock timestamps (e.g. `20260228_120000_run_1`) -- by design, not included in export_hash
- `batch.py` generates UUID4 for `build_batch_id` -- non-deterministic but not in export hashes
- `run_meta.json` contains `timestamp` and `elapsed_ms` fields -- non-deterministic metadata, excluded from integrity hashes
- All computation hashes (workbook_spec_hash, input_hashes, export_hash, params_hash, overlay_hash, plugin_hash) are fully deterministic

---

## Target Demo Set (New)

We want four repo demos that are implementation-focused and reproducible:

### Demo 1 -- AI Governance + Institutional Control
- Objective: simulated AI plugin generation + deterministic validation + deterministic build hash + artifact/registry record + compliance report JSON

### Demo 2 -- Deterministic Single-Company Build + Verify
- Objective: create model from template, commit/build/verify, show immutable run artifacts, show diffability and stable hashes

### Demo 3 -- Batch Parameter Sweep (Scenarios)
- Objective: run batch builds across a parameter grid, produce multiple run outputs, produce stable manifest describing the batch

### Demo 4 -- Data Guardrails (Join/Lookup Violations)
- Objective: demonstrate deterministic failure modes (duplicate keys / null keys / dtype mismatch) with structured error output and a "fixed input" path

For each demo, we require:
- Deterministic inputs
- No wall-clock timestamps in outputs
- Stable JSON output ordering
- Minimal CLI entrypoints:
  - `fin123-core demo <name>`
- Tests for demo determinism where possible

---

## Mapping: Existing Templates -> Target Demos
- Which existing template(s) will back each demo:
  - Demo 1 is standalone under `demos/ai_governance_demo/` (new module; no existing template)
  - Demo 2 uses: `single_company` template (scaffold + commit + build + verify)
  - Demo 3 uses: `universe_batch` template (scaffold + batch build with 3-scenario grid)
  - Demo 4 uses: `demo_fin123` template (join_left validation for dup/null/dtype errors)

---

## Implementation Notes / Constraints
- No refactors
- No new subsystems
- Use existing deterministic build pipeline and registry if available
- Core has `ArtifactStore` in `versioning.py` (versioned artifacts with approval workflow) -- use for Demo 1 artifact registration
- Plugin system in core is a graceful stub (actual implementation in fin123-pod) -- Demo 1 adds a minimal validator module used only by the demo path
- `utils/hash.py` provides `sha256_dict()` with `sort_keys=True, separators=(",",":")` -- reuse for deterministic build hash computation
- `verify.py` provides `verify_run()` -- reuse for Demo 2
- `batch.py` provides `run_batch()` -- reuse for Demo 3
- `functions/table.py` provides `join_left` with `_validate_join()` and `_check_join_key_dtypes()` -- reuse for Demo 4

---

## Acceptance Criteria
- Each demo runnable end-to-end on a clean checkout
- Produces the same outputs (byte-for-byte) on repeated runs
- Adds minimal code + minimal surface area
