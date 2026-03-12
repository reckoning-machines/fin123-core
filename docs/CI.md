# CI Pipeline -- fin123-core

Continuous integration for fin123-core runs on GitHub Actions.

## Workflow File

`.github/workflows/ci.yml`

## Triggers

- Push to `main`
- Pull requests targeting `main`
- Manual dispatch (`workflow_dispatch`)

## Jobs

### test

Standard pytest run with CLI contract check.

1. Checkout
2. Setup Python 3.12
3. `pip install -e ".[dev]"`
4. Lint: `python -m py_compile src/fin123/cli_core.py`
5. CLI contract check: `python scripts/cli_contract_check.py`
6. Test: `pytest --tb=short -q`

### native-scripts

Exercises the same bootstrap/smoke/acceptance path that an evaluator would use.

1. Checkout
2. Setup Python 3.12
3. `bash scripts/bootstrap_venv.sh`
4. `bash scripts/smoke_test.sh`
5. `bash scripts/run_acceptance_suite.sh`

This job validates that the documented evaluation path works end-to-end
on a clean Ubuntu runner.

### smoke-build

Verifies the PyInstaller binary builds without error.

1. Checkout
2. Setup Python 3.12
3. Install with PyInstaller
4. Build binary and verify `--version`

## No Database Required

fin123-core CI does not use any database. All tests run locally with
in-memory or filesystem state only.

## Relationship to fin123-pod CI

fin123-pod has its own CI workflow that additionally provisions a Postgres
service container for DB-backed tests. See the fin123-pod docs/CI.md.
