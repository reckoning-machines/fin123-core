# Playbook: Install and Verify fin123-core

## Objective

Install fin123-core from source on a clean machine, run smoke tests, and
produce a pass/fail verification report.

## Repo Assumptions

- Working directory is the fin123-core repository root.
- No database or network services required.
- Python 3.11+ is available on the system.

## Allowed Changes

- Create `.venv/` directory.
- Install Python packages into the virtualenv.
- Create temporary directories for demo/test output.

## Forbidden Changes

- Do not modify source code, tests, or configuration.
- Do not install system-level packages.
- Do not modify files outside the repository.

## Exact Steps

1. Verify Python version:
   ```bash
   python3 --version
   ```
   Must be 3.11 or later. If not, stop and report.

2. Run the bootstrap script:
   ```bash
   bash scripts/bootstrap_venv.sh
   ```
   Confirm exit code 0 and "Bootstrap complete" message.

3. Activate the virtualenv:
   ```bash
   source .venv/bin/activate
   ```

4. Verify the CLI:
   ```bash
   fin123 --version
   fin123 --json doctor
   ```
   Confirm version output. Confirm all doctor checks pass.

5. Run smoke tests:
   ```bash
   bash scripts/smoke_test.sh
   ```
   Confirm exit code 0 and "ALL SMOKE TESTS PASSED".

6. Run the acceptance suite:
   ```bash
   bash scripts/run_acceptance_suite.sh
   ```
   Confirm exit code 0 and "ACCEPTANCE SUITE PASSED".

7. Run the full test suite:
   ```bash
   pytest -v --tb=short -q
   ```
   Note the pass/fail count.

## Acceptance Criteria

- Bootstrap completes without error.
- `fin123 --version` prints the expected version.
- All doctor checks pass (timezone warning is acceptable).
- Smoke tests: 6/6 passed.
- Acceptance suite: all tests passed.
- Full test suite: zero failures.

## Final Report Format

```
## fin123-core Install & Verify Report

- Python version: <version>
- Package version: <fin123 --version output>
- Bootstrap: PASS / FAIL
- Doctor: PASS / FAIL (detail any failures)
- Smoke tests: <N>/<N> passed
- Acceptance suite: <passed>/<total> passed
- Full test suite: <passed>/<total> passed, <failed> failed
- Overall: PASS / FAIL
```
