# Playbook: Triage Failed Verification -- fin123-core

## Objective

Reproduce a verification failure deterministically, isolate the root cause,
and collect artifacts for diagnosis. Do not apply speculative fixes.

## Repo Assumptions

- fin123-core is installed and the failing project directory is accessible.
- A specific run_id is known to have a failing `fin123 verify` result.

## Allowed Changes

- Create temporary directories for reproduction.
- Read files and run commands. No edits to source or project files.

## Forbidden Changes

- Do not edit source code, workbook specs, or test files.
- Do not delete or modify run directories.
- Do not apply speculative broad fixes.

## Exact Steps

1. Reproduce the failure:
   ```bash
   fin123 --json verify <run_id> --project <project_dir>
   ```
   Capture the full JSON output. Note which checks failed.

2. Inspect run metadata:
   ```bash
   cat <project_dir>/runs/<run_id>/run_meta.json | python -m json.tool
   ```
   Record: workbook_spec_hash, input_hashes, params_hash, export_hash,
   engine_version, timestamp.

3. Check for input drift:
   Compare current input file hashes against `run_meta.json` input_hashes:
   ```bash
   python -c "
   from fin123.utils.hash import sha256_dict
   import json, hashlib
   from pathlib import Path
   # hash current inputs and compare to stored hashes
   "
   ```

4. Check for environment drift:
   ```bash
   fin123 --version
   python -c "import fin123; print(fin123.__version__)"
   fin123 --json doctor
   ```
   Compare engine_version in run_meta.json with current version.

5. Check for package/version drift:
   ```bash
   pip show fin123-core
   pip show polars
   ```
   Compare installed versions with those at build time (if recorded).

6. Attempt a fresh rebuild and verify:
   ```bash
   fin123 build <project_dir>
   # => new_run_id
   fin123 verify <new_run_id> --project <project_dir>
   ```
   If the new build verifies, the failure is due to drift since the
   original build. If it also fails, there is an engine or config issue.

7. If both old and new verify fail, run the diff:
   ```bash
   fin123 diff run <original_run_id> <new_run_id> --project <project_dir>
   ```

## Likely Sources

| Category | Symptoms |
|---|---|
| Input drift | input_hashes in run_meta differ from current files |
| Environment drift | engine_version mismatch, Python version changed |
| Package/version drift | polars or other dependency version changed |
| Output/hash mismatch | export_hash differs; inputs same; engine regression |
| Stale hash cache | `fin123 clear-cache <project>` then rebuild |

## Final Report Format

```
## fin123-core Verify Triage Report

- Run ID: <run_id>
- Project: <project_dir>
- Failure type: <spec_hash / input_hash / params_hash / export_hash>
- Root cause category: <input drift / env drift / version drift / unknown>
- Evidence: <specific hashes or version mismatches>
- Fresh rebuild verifies: YES / NO
- Recommended action: <specific next step>
```
