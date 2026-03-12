# Playbook: Release Readiness Review -- fin123-core

## Objective

Evaluate whether the current state of fin123-core is ready for external
release or evaluation. Produce a structured readiness report.

## Repo Assumptions

- Working directory is the fin123-core repository root.
- On the branch intended for release (typically main).

## Allowed Changes

- None. This is a read-only review.

## Forbidden Changes

- Do not modify any files.

## Exact Steps

1. Check version consistency:
   ```bash
   python -c "
   import tomllib, pathlib
   v = tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['version']
   print(f'pyproject.toml: {v}')
   "
   python -c "import fin123; print(f'__version__: {fin123.__version__}')"
   fin123 --version
   ```
   All three must agree.

2. Check CHANGELOG:
   ```bash
   head -30 CHANGELOG.md
   ```
   Confirm the latest entry matches the current version.

3. Run bootstrap:
   ```bash
   bash scripts/bootstrap_venv.sh
   source .venv/bin/activate
   ```

4. Run smoke tests:
   ```bash
   bash scripts/smoke_test.sh
   ```

5. Run acceptance suite:
   ```bash
   bash scripts/run_acceptance_suite.sh
   ```

6. Run full test suite:
   ```bash
   pytest -v --tb=short -q
   ```

7. Check documentation accuracy:
   - `docs/INSTALL.md` commands work as documented
   - `docs/OPERATIONS.md` procedures are current
   - `docs/CLI_SPEC.md` matches actual CLI output
   - `README.md` quick start works

8. Check CLI contract:
   ```bash
   python scripts/cli_contract_check.py
   ```

9. Check for uncommitted changes:
   ```bash
   git status
   git diff --stat
   ```

10. Review git log for unreleased changes:
    ```bash
    git log --oneline -20
    ```

## Acceptance Criteria

- Version strings consistent across pyproject.toml, __version__, CLI
- CHANGELOG updated for current version
- Bootstrap succeeds
- Smoke tests: 6/6 pass
- Acceptance suite: all pass
- Full test suite: zero failures
- Documentation matches reality
- CLI contract check passes
- No uncommitted changes on release branch

## Final Report Format

```
## fin123-core Release Readiness Report

- Version: <version>
- Version consistent: YES / NO
- CHANGELOG current: YES / NO
- Bootstrap: PASS / FAIL
- Smoke tests: <N>/<N>
- Acceptance suite: PASS / FAIL (<N> tests)
- Full test suite: <passed>/<total>, <failed> failures
- CLI contract: PASS / FAIL
- Docs accurate: YES / NO (detail issues)
- Clean tree: YES / NO
- RELEASE READY: YES / NO
- Blockers: <list or "none">
```
