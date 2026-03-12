# Playbook: Run Smoke Tests -- fin123-core

## Objective

Execute the quick smoke test suite and report results. This is the
fastest way to validate that an installation is functional.

## Repo Assumptions

- fin123-core is installed (virtualenv active or package on PATH).
- No database or network required.

## Allowed Changes

- Create temporary directories for test output.

## Forbidden Changes

- Do not modify any files in the repository.

## Exact Steps

1. Verify the virtualenv is active:
   ```bash
   which fin123
   python -c "import fin123; print(fin123.__version__)"
   ```

2. Run smoke tests:
   ```bash
   bash scripts/smoke_test.sh
   ```

3. If any check fails, note the specific failure.

## Expected Pass Criteria

All 6 checks must pass:
1. Package import -- `import fin123` succeeds
2. CLI --version -- exits 0
3. CLI --help -- exits 0
4. Template list -- exits 0
5. Doctor checks -- all checks pass (no "fail" status)
6. Deterministic build demo -- produces summary with status "pass"

## Failure Interpretation

| Failed Check | Likely Cause |
|---|---|
| Package import | Package not installed, wrong virtualenv, or dependency missing |
| CLI --version/--help | Entry point not registered; reinstall package |
| Template list | Package installed but templates missing; reinstall from source |
| Doctor checks | Environment issue; read the specific failing check detail |
| Deterministic build demo | Engine regression; run `pytest tests/test_demos.py -v` for detail |

## Final Report Format

```
## fin123-core Smoke Test Report

- Passed: <N>/6
- Failed: <N>/6
- Failed checks: <list or "none">
- Overall: PASS / FAIL
```
