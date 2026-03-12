#!/usr/bin/env bash
# Bootstrap a Python virtual environment for fin123-core development.
#
# Usage:
#   bash scripts/bootstrap_venv.sh
#
# Creates .venv in the repository root, installs the package in editable
# mode with dev extras, and runs a minimal import sanity check.
# Idempotent: re-running reuses the existing .venv.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="${REPO_ROOT}/.venv"
MIN_PYTHON_MAJOR=3
MIN_PYTHON_MINOR=11

# ── Helpers ──────────────────────────────────────────────────────────

log()  { printf '[bootstrap] %s\n' "$*"; }
die()  { printf '[bootstrap] ERROR: %s\n' "$*" >&2; exit 1; }

check_python() {
    local py
    for py in python3 python; do
        if command -v "$py" >/dev/null 2>&1; then
            local ver
            ver="$("$py" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")"
            local major minor
            major="${ver%%.*}"
            minor="${ver##*.}"
            if [ "$major" -ge "$MIN_PYTHON_MAJOR" ] && [ "$minor" -ge "$MIN_PYTHON_MINOR" ]; then
                PYTHON="$py"
                log "Found Python ${ver} at $(command -v "$py")"
                return 0
            fi
        fi
    done
    die "Python >=${MIN_PYTHON_MAJOR}.${MIN_PYTHON_MINOR} is required but not found on PATH."
}

# ── Main ─────────────────────────────────────────────────────────────

log "fin123-core bootstrap starting"

check_python

if [ -d "$VENV_DIR" ]; then
    log "Reusing existing virtualenv at ${VENV_DIR}"
else
    log "Creating virtualenv at ${VENV_DIR}"
    "$PYTHON" -m venv "$VENV_DIR"
fi

# Activate
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

log "Upgrading pip"
pip install --upgrade pip --quiet

log "Installing fin123-core in editable mode with dev extras"
pip install -e "${REPO_ROOT}[dev]" --quiet

log "Verifying installation"
python -c "import fin123; print(f'fin123 {fin123.__version__} imported successfully')"
fin123 --version

log "Bootstrap complete. Activate with: source .venv/bin/activate"
