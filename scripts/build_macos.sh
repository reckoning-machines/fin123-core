#!/usr/bin/env bash
# Build fin123-core macOS binary and package into a release ZIP.
# Usage: scripts/build_macos.sh [VERSION_OVERRIDE]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Resolve version
if [[ -n "${1:-}" ]]; then
    VERSION="$1"
else
    VERSION="$(python -c "from importlib.metadata import version; print(version('fin123-core'))" 2>/dev/null \
        || python -c "import re, pathlib; print(re.search(r'version\s*=\s*\"([^\"]+)\"', pathlib.Path('pyproject.toml').read_text()).group(1))")"
fi

# Determine architecture
ARCH="$(uname -m)"  # arm64 or x86_64

echo "==> Building fin123-core ${VERSION} for macOS-${ARCH}"

# Clean previous build artifacts
rm -rf build/fin123-core dist/fin123-core

# Run PyInstaller
python -m PyInstaller \
    --clean \
    --noconfirm \
    packaging/fin123_core.spec

# Verify the binary works
echo "==> Verifying binary..."
BINARY="dist/fin123-core"
if [[ ! -f "$BINARY" ]]; then
    echo "ERROR: Binary not found at ${BINARY}" >&2
    exit 1
fi
"$BINARY" --version

# Package into ZIP
ZIP_NAME="fin123-core-${VERSION}-macos-${ARCH}.zip"
echo "==> Packaging ${ZIP_NAME}"
cd dist
zip -j "$ZIP_NAME" fin123-core
cd "$REPO_ROOT"

echo "==> Built: dist/${ZIP_NAME}"

# Generate checksums
python scripts/checksums.py

echo "==> Done."
