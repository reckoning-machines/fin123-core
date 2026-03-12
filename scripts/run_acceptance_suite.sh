#!/usr/bin/env bash
# Run the fin123-core acceptance test suite.
#
# Usage:
#   bash scripts/run_acceptance_suite.sh
#
# Runs tests/acceptance/ via pytest. These tests exercise full
# deterministic lifecycle paths: scaffold, build, verify, diff, demos.
# Exit 0 on success, non-zero on any failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

log() { printf '[acceptance] %s\n' "$*"; }

# Activate venv if present
if [ -f "${REPO_ROOT}/.venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/.venv/bin/activate"
fi

log "fin123-core acceptance suite starting"

# Run acceptance tests plus the existing demo stability tests
pytest "${REPO_ROOT}/tests/acceptance/" "${REPO_ROOT}/tests/test_demos.py" \
    -v --tb=short -m "not pod" 2>&1

EXIT_CODE=$?

if [ "$EXIT_CODE" -eq 0 ]; then
    log "ACCEPTANCE SUITE PASSED"
else
    log "ACCEPTANCE SUITE FAILED (exit code ${EXIT_CODE})"
fi

exit "$EXIT_CODE"
