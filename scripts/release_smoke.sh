#!/bin/sh
# release_smoke.sh -- Smoke-test a GitHub Release of fin123-core.
#
# Downloads the release zips, verifies SHA256 checksums, unpacks the macOS
# binary, and exercises basic CLI commands (--version, --help, doctor --json).
#
# Usage:
#   sh scripts/release_smoke.sh
#   RELEASE_TAG=core-v0.4.0 sh scripts/release_smoke.sh
#
# Requirements: curl, unzip, shasum or sha256sum, python3.
# Runs on macOS and Linux (/bin/sh, POSIX).

set -e

RELEASE_TAG="${RELEASE_TAG:-core-v0.3.3}"
VERSION="$(echo "$RELEASE_TAG" | sed 's/^core-v//')"

REPO="reckoning-machines/fin123-core"
BASE_URL="https://github.com/${REPO}/releases/download/${RELEASE_TAG}"

MAC_ZIP="fin123-core-${VERSION}-macos-arm64.zip"
WIN_ZIP="fin123-core-${VERSION}-windows-x86_64.zip"
SUMS="SHA256SUMS.txt"

PASS_COUNT=0
FAIL_COUNT=0

pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    echo "  PASS: $1"
}

fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo "  FAIL: $1" >&2
}

# -------------------------------------------------------------------
# Temp directory with cleanup
# -------------------------------------------------------------------
TMPDIR_SMOKE="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_SMOKE"' EXIT
echo "Working directory: ${TMPDIR_SMOKE}"
echo ""

# -------------------------------------------------------------------
# Step 1 -- Download release assets
# -------------------------------------------------------------------
echo "== Step 1: Download release assets (tag: ${RELEASE_TAG}) =="

for asset in "$MAC_ZIP" "$WIN_ZIP" "$SUMS"; do
    url="${BASE_URL}/${asset}"
    echo "  Downloading ${asset} ..."
    if curl -L -f -s -o "${TMPDIR_SMOKE}/${asset}" "$url"; then
        pass "Downloaded ${asset}"
    else
        fail "Failed to download ${asset} from ${url}"
    fi
done
echo ""

# -------------------------------------------------------------------
# Step 2 -- Verify SHA256 checksums
# -------------------------------------------------------------------
echo "== Step 2: Verify SHA256 checksums =="

cd "$TMPDIR_SMOKE"

if command -v shasum >/dev/null 2>&1; then
    SHA_CMD="shasum -a 256 -c"
elif command -v sha256sum >/dev/null 2>&1; then
    SHA_CMD="sha256sum -c"
else
    fail "No shasum or sha256sum found"
    SHA_CMD=""
fi

if [ -n "$SHA_CMD" ] && [ -f "$SUMS" ]; then
    if $SHA_CMD "$SUMS"; then
        pass "SHA256 checksums verified"
    else
        fail "SHA256 checksum mismatch"
    fi
fi
echo ""

# -------------------------------------------------------------------
# Step 3 -- Unzip and locate macOS binary
# -------------------------------------------------------------------
echo "== Step 3: Unzip macOS binary =="

UNPACK_DIR="${TMPDIR_SMOKE}/macos"
mkdir -p "$UNPACK_DIR"

if unzip -o -q "${TMPDIR_SMOKE}/${MAC_ZIP}" -d "$UNPACK_DIR"; then
    pass "Unzipped ${MAC_ZIP}"
else
    fail "Failed to unzip ${MAC_ZIP}"
fi

BINARY="${UNPACK_DIR}/fin123-core"
if [ -f "$BINARY" ]; then
    chmod +x "$BINARY"
    pass "Found binary: fin123-core"
else
    fail "Binary fin123-core not found in zip"
    echo ""
    echo "== OVERALL: FAIL (binary missing, cannot continue) =="
    exit 1
fi
echo ""

# -------------------------------------------------------------------
# Step 4 -- Exercise CLI commands
# -------------------------------------------------------------------
echo "== Step 4: CLI smoke tests =="

# 4a: --version
echo "  Running: fin123-core --version"
VERSION_OUT="$("$BINARY" --version 2>&1)" || true
echo "  Output:  ${VERSION_OUT}"
if echo "$VERSION_OUT" | grep -q "$VERSION"; then
    pass "--version reports ${VERSION}"
else
    fail "--version did not contain ${VERSION}"
fi

# 4b: --help
echo "  Running: fin123-core --help"
HELP_OUT="$("$BINARY" --help 2>&1)" || true
if echo "$HELP_OUT" | grep -q "Usage"; then
    pass "--help contains Usage"
else
    fail "--help did not contain Usage"
fi

# 4c: --json doctor (--json is a global flag, must precede subcommand)
echo "  Running: fin123-core --json doctor"
DOCTOR_OUT="$("$BINARY" --json doctor 2>&1)" || true
echo "  Output:  ${DOCTOR_OUT}"

DOCTOR_VALID="$(python3 -c "
import json, sys
try:
    d = json.loads(sys.argv[1])
    required = ['ok', 'cmd', 'version', 'data', 'error']
    missing = [k for k in required if k not in d]
    if missing:
        print('MISSING:' + ','.join(missing))
        sys.exit(1)
    print('VALID')
except Exception as e:
    print('PARSE_ERROR:' + str(e))
    sys.exit(1)
" "$DOCTOR_OUT" 2>&1)" || true

if [ "$DOCTOR_VALID" = "VALID" ]; then
    pass "doctor --json has required keys (ok, cmd, version, data, error)"
else
    fail "doctor --json validation: ${DOCTOR_VALID}"
fi
echo ""

# -------------------------------------------------------------------
# Summary
# -------------------------------------------------------------------
TOTAL=$((PASS_COUNT + FAIL_COUNT))
echo "== Results: ${PASS_COUNT}/${TOTAL} passed, ${FAIL_COUNT} failed =="

if [ "$FAIL_COUNT" -eq 0 ]; then
    echo "== OVERALL: PASS =="
    exit 0
else
    echo "== OVERALL: FAIL =="
    exit 1
fi
