# Installation Guide -- fin123-core

fin123-core is the deterministic financial modeling and worksheet runtime.
It does not require a database, network access, or the fin123-pod platform.

## Prerequisites

- Python 3.11 or later
- pip (bundled with Python)
- git (to clone from source)

No database. No Docker. No external services.

## Quick Install (from PyPI)

```bash
pip install fin123-core

# With XLSX import support
pip install "fin123-core[xlsx]"
```

Verify:

```bash
fin123 --version
fin123 template list
```

## Install from Source (recommended for evaluation)

```bash
git clone https://github.com/reckoning-machines/fin123-core.git
cd fin123-core
bash scripts/bootstrap_venv.sh
source .venv/bin/activate
```

The bootstrap script:
1. Checks Python version (>=3.11)
2. Creates `.venv` if it does not exist
3. Installs the package in editable mode with dev dependencies
4. Verifies the installation with an import check

Alternatively, do it manually:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
fin123 --version
```

## Verify Installation

Run the smoke tests:

```bash
bash scripts/smoke_test.sh
```

This validates: package import, CLI availability, doctor checks, and
a deterministic build demo. Completes in under 30 seconds.

## Run Tests

```bash
# All core tests (default configuration)
pytest

# Verbose with short tracebacks
pytest -v --tb=short

# Acceptance suite only
bash scripts/run_acceptance_suite.sh
```

## What fin123-core Provides

- Deterministic financial workbook engine (scalars, tables, formulas)
- LALR(1) formula parser with 16+ financial/logical/date functions
- Worksheet runtime (ViewTable, WorksheetView, CompiledWorksheet)
- Build/verify/diff lifecycle with SHA-256 integrity
- Batch builds with parameter sweeps
- XLSX import with formula classification
- Project templates (single_company, universe_batch, demo_fin123)
- 4 built-in deterministic demos
- Local browser UI (FastAPI)
- CLI with structured JSON output

## What fin123-core Does NOT Provide

The following require fin123-pod (separate repository):

- Postgres-backed model registry
- Headless runner service
- SQL sync and data connectors
- Workflow orchestration
- Release governance
- Hosted worksheet delivery
- Plugin system

See the [fin123-pod repository](https://github.com/reckoning-machines/fin123-pod)
for platform/team capabilities. Pod depends on core; core does not depend on pod.
