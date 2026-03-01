# Runbook — fin123-core

Operational guide for installing, running, and troubleshooting fin123-core.
For the full CLI specification (command tree, exit codes, JSON contract),
see [CLI_SPEC.md](CLI_SPEC.md).

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
fin123 --version
fin123 template list
```

## Create a Project

```bash
# From a template
fin123 init my_model --template single_company --set ticker=AAPL

# From the demo template
fin123 init demo --template demo_fin123
```

This creates a project directory with `workbook.yaml`, `fin123.yaml`, and sample input files.

## Build Lifecycle

### 1. Edit

Edit `workbook.yaml` directly or use the browser UI (`fin123 ui <dir>`).

### 2. Commit

```bash
fin123 commit my_model
```

Writes the current workbook to `workbook.yaml` and creates an immutable snapshot
(`snapshots/workbook/vXXXX/workbook.yaml`).

### 3. Build

```bash
fin123 build my_model

# With parameter overrides
fin123 build my_model --set tax_rate=0.25

# With a named scenario
fin123 build my_model --scenario bear_case
```

Evaluates scalar and table graphs, writes outputs to `runs/<timestamp>_run_<n>/`.

### 4. Verify

```bash
# Verify the latest build
fin123 verify <run_id> --project my_model

# With JSON output
fin123 verify <run_id> --project my_model --json
```

Recomputes hashes for the specified build run. Reports pass/fail for spec hash, input hashes,
params hash, and export hashes.

> **Note:** `verify` requires a completed build run. Run `fin123 build` first, then pass the run ID printed by `build`.

## Browser UI

```bash
fin123 ui my_model

# Specify port, skip auto-open
fin123 ui my_model --port 8080 --no-open
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
fin123 batch build my_model --params-file params.csv

# With parallelism
fin123 batch build my_model --params-file params.csv --max-workers 4
```

The CSV must have one column per parameter. One build per row.

## Demos

fin123 includes four built-in demos. Each is self-contained, creates
a temporary project directory, runs the full build lifecycle, and prints
results to stdout. No external data or configuration is required.

```bash
# AI governance -- plugin validation + compliance report
fin123 demo ai-governance

# Deterministic build -- scaffold, build, verify with stable hashes
fin123 demo deterministic-build

# Batch sweep -- 3-scenario parameter grid with stable manifest
fin123 demo batch-sweep

# Data guardrails -- join validation failures + success cases
fin123 demo data-guardrails
```

All demos produce deterministic output. Running the same demo twice on the
same version of fin123 will produce identical results (no timestamps
or run IDs appear in output that would cause hash drift).

### Running all demos as a test suite

```bash
pytest tests/test_demos.py -v
```

This executes every demo in a subprocess and asserts a zero exit code.

## Verifying Determinism (Hash Stability)

fin123 guarantees that identical inputs produce identical outputs.
To verify this property after a build:

### Single-build verification

```bash
fin123 build my_model
# => Build saved to: <run_id>

fin123 verify <run_id> --project my_model
# => Status: PASS -- All checks passed.
```

`verify` recomputes SHA-256 hashes for the spec, inputs, params,
and exports, then compares them against the hashes stored at build time.
Any drift causes a FAIL.

### Cross-run determinism check

Build twice with the same inputs and compare:

```bash
fin123 build my_model
# => Build saved to: <run_a>

fin123 build my_model
# => Build saved to: <run_b>

fin123 diff run <run_a> <run_b> --project my_model
# => No differences (scalars match, tables match)
```

If `diff run` reports differences, inspect `run_meta.json` in each run
directory to identify which inputs diverged.

### Demo-level determinism

The `deterministic-build` demo exercises this workflow end-to-end:

```bash
fin123 demo deterministic-build
```

## Diff

```bash
# Compare two builds
fin123 diff run <run_a> <run_b> --project my_model

# Compare two workbook versions
fin123 diff version v0001 v0002 --project my_model

# Machine-readable output
fin123 diff run <run_a> <run_b> --project my_model --json
```

## XLSX Import

```bash
fin123 import-xlsx model.xlsx my_model
```

Imports worksheets, cell values, formulas (as-is), and font colors. Writes an import
report to `import_reports/`. Formulas are classified as supported, unsupported, parse_error,
external_link, or plugin_formula.

## Garbage Collection

```bash
# Dry run (report only)
fin123 gc my_model --dry-run

# Actually delete
fin123 gc my_model

# Clear hash cache too
fin123 clear-cache my_model
```

Configure limits in `fin123.yaml`:

```yaml
max_runs: 50
max_artifact_versions: 20
max_total_run_bytes: 2000000000   # 2 GB
ttl_days: 30
```

## Troubleshooting

### `fin123: command not found`

Ensure the package is installed and your PATH includes pip's script directory:

```bash
pip show fin123-core
fin123 --help
python -m fin123.cli_core --help    # fallback
```

### Build fails with "uncommitted edits"

The UI has unsaved changes. Commit first:

```bash
fin123 commit my_model
```

### Formula parse errors

Check the formula syntax. fin123 uses a Lark LALR(1) parser, not Excel's parser.
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
fin123 guarantees deterministic outputs for identical inputs, but different input
file contents will produce different results.

### UI won't start (port in use)

```bash
fin123 ui my_model --port 9999
```

### Polars version mismatch

fin123 requires `polars>=1.0`. Check:

```bash
python -c "import polars; print(polars.__version__)"
```

### Demo fails with `ModuleNotFoundError`

Demos import from the `demos/` package at the repo root. Ensure you are
running from a source checkout with the package installed in editable mode:

```bash
pip install -e ".[dev]"
```

### verify reports FAIL unexpectedly

Common causes:

- Input files were modified between the build and verification.
- The project was built with a different version of fin123-core.
- The hash cache is stale. Clear it and rebuild:

```bash
fin123 clear-cache my_model
fin123 build my_model
```

### PyInstaller build fails on macOS

Ensure you are using Python 3.12. Universal2 builds are not supported.
The build script targets the native architecture (`arm64` on Apple Silicon).

```bash
python --version   # must be 3.12.x
which python       # must point to a native arm64 Python
```

---

## Building Installers

fin123 produces standalone binaries via PyInstaller. CI builds both
macOS and Windows automatically on tag push; this section covers manual
local builds.

### Prerequisites

- Python 3.12
- PyInstaller: `pip install pyinstaller`
- The project installed in editable mode: `pip install -e .`

### macOS (arm64)

```bash
bash scripts/build_macos.sh
```

Produces `dist/fin123-core-<version>-macos-arm64.zip` and
`dist/SHA256SUMS.txt`.

### Windows (x86_64)

```powershell
pwsh scripts/build_windows.ps1
```

Produces `dist/fin123-core-<version>-windows-x86_64.zip` and
`dist/SHA256SUMS.txt`.

### Verifying checksums

After building, verify the ZIP integrity:

```bash
# macOS / Linux
cd dist
shasum -a 256 -c SHA256SUMS.txt

# Windows
certutil -hashfile fin123-core-<version>-windows-x86_64.zip SHA256
```

### Regenerating checksums only

If ZIPs already exist and you need to regenerate `SHA256SUMS.txt`:

```bash
python scripts/checksums.py
```

## Publishing a Release

### Automated (recommended)

Push a tag matching `core-vX.Y.Z` to trigger CI:

```bash
git tag core-vX.Y.Z
git push origin core-vX.Y.Z
```

This triggers two workflows:

1. `release.yml` -- runs tests, builds binaries on macOS and Windows,
   creates a GitHub Release with ZIPs and SHA256SUMS.txt.
2. `pypi-release.yml` -- builds sdist + wheel, publishes to PyPI via
   Trusted Publishing.

### Manual

If CI is unavailable, build locally and create the release manually:

```bash
# 1. Build macOS binary
bash scripts/build_macos.sh

# 2. Create release and upload (requires gh CLI)
gh release create core-vX.Y.Z \
  --title "core-vX.Y.Z" \
  --prerelease \
  dist/fin123-core-*-macos-arm64.zip \
  dist/SHA256SUMS.txt
```

Windows binaries must be built on a Windows machine or via CI.

---

## fin123 Release Runbook (v0.3+)

End-to-end procedure for publishing a new fin123 release. A single
`core-vX.Y.Z` tag triggers both PyPI and binary release workflows.

### Preconditions

Before tagging, confirm:

```bash
# 1. main branch is green (CI passing)
git checkout main && git pull

# 2. Version in pyproject.toml matches intended release
python -c "
import tomllib, pathlib
v = tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['version']
print(f'pyproject.toml version: {v}')
"

# 3. CLI prints correct version
pip install -e .
fin123 --version
# Expected: fin123, version X.Y.Z (core_api=0.3)

# 4. Tests pass
pytest --tb=short -q
# Expected: 702 passed, 0 failed (count may grow)
```

### Step 1 — Create and Push Tag

```bash
git tag core-vX.Y.Z
git push origin core-vX.Y.Z
```

This triggers two independent workflows:

| Workflow | File | Purpose |
|----------|------|---------|
| Release | `.github/workflows/release.yml` | PyInstaller binaries + GitHub Release |
| PyPI Release | `.github/workflows/pypi-release.yml` | sdist + wheel to PyPI |

### Step 2 — PyPI Publish Flow

`pypi-release.yml` runs two jobs:

1. **build** (ubuntu-latest):
   - Checks out code at the tag
   - Strips `core-v` prefix from tag and compares to `pyproject.toml` version
   - Runs `python -m build` (sdist + wheel)
   - Runs `twine check dist/*`
   - Uploads `dist/` as workflow artifact

2. **publish** (depends on build):
   - Downloads `dist/` artifact
   - Publishes to PyPI via Trusted Publishing (OIDC, `id-token: write`)
   - Uses environment `pypi` (can add manual approval rules in GitHub settings)

Verify the published package:

```bash
python -m venv /tmp/f123-verify
source /tmp/f123-verify/bin/activate
pip install fin123-core==X.Y.Z
fin123 --version
deactivate
rm -rf /tmp/f123-verify
```

### Step 3 — Binary Release Flow

`release.yml` runs:

1. **test** — full pytest on ubuntu
2. **build** — PyInstaller on `windows-latest` and `macos-14`
3. **release** — creates a GitHub Release (prerelease) with:
   - `fin123-core-<version>-windows-x86_64.zip`
   - `fin123-core-<version>-macos-<arch>.zip`
   - `SHA256SUMS.txt`

Verify at: `https://github.com/reckoning-machines/fin123-core/releases/tag/core-vX.Y.Z`

### Step 4 — Mirror to fin123_public

After both workflows succeed:

1. Go to the `fin123_public` repo → Actions → publish workflow.
2. Run workflow with input `CORE_TAG = core-vX.Y.Z`.
3. Confirm `index.html` download links point to the new release.

### Failure Modes

#### A) PyPI Trusted Publishing Failure

**Symptom:** publish job fails with OIDC / token exchange error.

**Cause:** Trusted Publisher not configured on PyPI for this repo/workflow.

**Fix:** Complete the one-time setup (documented in `pypi-release.yml` header):

1. Log in to https://pypi.org → project `fin123-core` → Settings → Publishing.
2. Add publisher: owner=`reckoning-machines`, repo=`fin123-core`,
   workflow=`pypi-release.yml`, environment=`pypi`.
3. Re-run the failed workflow.

#### B) Version Mismatch Failure

**Symptom:** build job exits 1 with "Version mismatch — tag says X but pyproject.toml says Y".

**Cause:** `pyproject.toml` version was not bumped before tagging, or the wrong tag was pushed.

**Fix:**

```bash
# Delete the bad tag (local + remote)
git tag -d core-vX.Y.Z
git push origin :refs/tags/core-vX.Y.Z

# Fix pyproject.toml version, commit, then re-tag
git tag core-vX.Y.Z
git push origin core-vX.Y.Z
```

#### C) Project Name Conflict on PyPI

**Symptom:** `twine check` passes but upload fails with HTTP 403 or name conflict.

**Cause:** Another project owns the `fin123-core` name on PyPI.

**Fix:** If the name is unavailable, change `[project].name` in `pyproject.toml`
(e.g. `fin123-core-engine`), update all references, bump version, and re-tag.

#### D) Windows Build Failure

**Symptom:** `release.yml` build job fails on `windows-latest`.

**Fix:** Inspect the workflow logs. Common causes: PyInstaller spec issues,
missing DLLs, path length limits. Do **not** delete and re-push the tag
unless the tag itself is wrong — re-run the failed job instead.

### Rollback Procedure

- **Do not delete published tags** unless the release has not been consumed.
  Deleted tags break auditability.
- **PyPI does not allow re-uploading** the same version. If a bad version was
  published to PyPI, you must increment the patch version:

  ```bash
  # In pyproject.toml: version = "X.Y.Z+1"
  git add pyproject.toml && git commit -m "fix: bump to X.Y.Z+1"
  git tag core-vX.Y.Z+1
  git push origin main core-vX.Y.Z+1
  ```

- **GitHub Releases** can be edited or deleted from the web UI if needed.

### Namespace Collision Note

`fin123-core` and `fin123-pod` share the Python package namespace `fin123`.
When both are installed in the same environment:

- The first one on `sys.path` wins; the other's modules may be invisible.
- `fin123-core` CLI prints a warning to stderr on startup if `fin123-pod` is detected.
- Pod must never overwrite core modules — it extends via its own subpackages.

**Recommendation:** Use separate virtualenvs for core-only development and
pod development. Do not install both into the same environment.

### Versioning Policy

| Element | Format | Example |
|---------|--------|---------|
| Git tag | `core-vX.Y.Z` | `core-v0.3.0` |
| pyproject.toml version | `X.Y.Z` | `0.3.0` |
| `core_api` version | integer | `1` |

- Tag version and `pyproject.toml` version **must** match (enforced by CI).
- `core_api` is incremented only when the internal API contract between core
  and pod changes. It is independent of the release version.
