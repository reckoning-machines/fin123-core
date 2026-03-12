#!/usr/bin/env bash
# Quick smoke tests for fin123-core.
#
# Usage:
#   bash scripts/smoke_test.sh
#
# Validates package import, CLI availability, doctor checks, and runs
# a single deterministic demo. Designed to complete in under 30 seconds.
# Exit 0 on success, non-zero on any failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PASS=0
FAIL=0

log()      { printf '[smoke] %s\n' "$*"; }
pass()     { PASS=$((PASS + 1)); log "PASS: $*"; }
fail()     { FAIL=$((FAIL + 1)); log "FAIL: $*"; }

# Activate venv if present
if [ -f "${REPO_ROOT}/.venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/.venv/bin/activate"
fi

log "fin123-core smoke tests starting"

# ── 1. Package import ────────────────────────────────────────────────

if python -c "import fin123; print(f'version={fin123.__version__}')" 2>/dev/null; then
    pass "package import"
else
    fail "package import"
fi

# ── 2. CLI --version ─────────────────────────────────────────────────

if fin123 --version >/dev/null 2>&1; then
    pass "CLI --version"
else
    fail "CLI --version"
fi

# ── 3. CLI --help ────────────────────────────────────────────────────

if fin123 --help >/dev/null 2>&1; then
    pass "CLI --help"
else
    fail "CLI --help"
fi

# ── 4. Template list ─────────────────────────────────────────────────

if fin123 template list >/dev/null 2>&1; then
    pass "template list"
else
    fail "template list"
fi

# ── 5. Doctor ────────────────────────────────────────────────────────

if fin123 --json doctor 2>/dev/null | python -c "
import sys, json
data = json.load(sys.stdin)
checks = data.get('data', {}).get('checks', [])
failures = [c for c in checks if c.get('status') == 'fail']
if failures:
    for f in failures:
        print(f'  doctor FAIL: {f[\"name\"]}: {f.get(\"detail\", \"\")}', file=sys.stderr)
    sys.exit(1)
"; then
    pass "doctor checks"
else
    fail "doctor checks"
fi

# ── 6. Deterministic build demo ──────────────────────────────────────

DEMO_DIR=$(mktemp -d)
trap 'rm -rf "$DEMO_DIR"' EXIT

if python -c "
from fin123.demos.deterministic_build_demo.run import run_demo
from pathlib import Path
import json
out = Path('${DEMO_DIR}')
run_demo(output_dir=out)
summary = json.loads((out / 'deterministic_build_summary.json').read_text())
assert summary.get('verify_status') == 'pass', f'Demo status: {summary.get(\"verify_status\")}'
print('deterministic-build demo passed')
" 2>/dev/null; then
    pass "deterministic-build demo"
else
    fail "deterministic-build demo"
fi

# ── Summary ──────────────────────────────────────────────────────────

TOTAL=$((PASS + FAIL))
log "Results: ${PASS}/${TOTAL} passed, ${FAIL} failed"

if [ "$FAIL" -gt 0 ]; then
    log "SMOKE TESTS FAILED"
    exit 1
fi

log "ALL SMOKE TESTS PASSED"
exit 0
