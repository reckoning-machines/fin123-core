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

---

## fin123-core Release Runbook (v0.3+)

End-to-end procedure for publishing a new fin123-core release. A single
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
fin123-core --version
# Expected: fin123-core, version X.Y.Z (core_api=1)

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
fin123-core --version
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
