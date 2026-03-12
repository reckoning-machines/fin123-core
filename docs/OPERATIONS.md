# Operations Guide -- fin123-core

Day-to-day operational procedures for fin123-core. For installation, see
[INSTALL.md](INSTALL.md). For the CLI specification, see [CLI_SPEC.md](CLI_SPEC.md).

## Architecture Boundary

fin123-core is the deterministic modeling runtime. It operates entirely
locally -- no database, no network, no external services required.

fin123-pod is the separate DB-backed platform layer. If you are evaluating
core only, you do not need pod, Postgres, or any platform infrastructure.

## Smoke Testing

Fast validation that the installation is healthy:

```bash
bash scripts/smoke_test.sh
```

Checks:
1. Package imports successfully
2. CLI responds to --version, --help
3. Template list works
4. Doctor passes all preflight checks
5. Deterministic build demo produces correct output

Expected: all 6 checks pass. Time: under 30 seconds.

## Acceptance Testing

More thorough validation of deterministic guarantees:

```bash
bash scripts/run_acceptance_suite.sh
```

Runs `tests/acceptance/` and `tests/test_demos.py`. Exercises:
- Scaffold/build/verify lifecycle
- Cross-run export hash stability
- All 4 demo modules
- CLI JSON output contracts

Expected: all tests pass. Time: 1-3 minutes.

## Full Test Suite

```bash
pytest -v --tb=short
```

Runs all core tests (pod tests excluded by default via pyproject.toml).

## Doctor Checks

```bash
fin123 doctor
fin123 --json doctor
```

Validates:
- Python runtime version
- Determinism engine self-test
- Floating-point canonicalization
- Filesystem permissions
- UTF-8 encoding
- Timezone (warning if not UTC, not a failure)
- Dependency availability

## Deterministic Verification

### Single build

```bash
fin123 init my_model --template single_company
fin123 build my_model
# note the run_id printed
fin123 verify <run_id> --project my_model
```

### Cross-run comparison

```bash
fin123 build my_model
# => run_a
fin123 build my_model
# => run_b
fin123 diff run <run_a> <run_b> --project my_model
```

Identical inputs must produce identical outputs. Any difference indicates
environment or input drift.

### Built-in demos

```bash
fin123 demo deterministic-build
fin123 demo batch-sweep
fin123 demo data-guardrails
fin123 demo ai-governance
```

All demos produce deterministic output. Running the same demo twice on the
same fin123 version will produce byte-for-byte identical results.

## Troubleshooting

### fin123 command not found

```bash
pip show fin123-core
which fin123
python -m fin123.cli_core --help   # fallback
```

### Verify reports FAIL

Common causes:
- Input files modified between build and verification
- Different fin123-core version used for build vs verify
- Stale hash cache: `fin123 clear-cache <project>`

### Doctor check fails

Read the specific check name and detail. Most common:
- Python version too old (need >=3.11)
- Missing dependency (reinstall with `pip install -e ".[dev]"`)

### Demo fails with ModuleNotFoundError

Reinstall: `pip install -e ".[dev]"`. Demos are bundled as `fin123.demos`
since version 0.3.4.

## Release Verification

After a new release, validate:

```bash
bash scripts/smoke_test.sh
bash scripts/run_acceptance_suite.sh
pytest -v --tb=short
```

All three must pass before considering a release ready for external use.
