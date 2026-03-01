#!/usr/bin/env python3
"""CLI contract check -- detect help-text drift.

Compares a normalized SHA-256 hash of `fin123 --help` against a
checked-in expected hash in docs/CLI_HELP_SHA256.txt.

Usage:
    python scripts/cli_contract_check.py          # verify (exit non-zero on mismatch)
    python scripts/cli_contract_check.py --write   # update expected hash file
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
from pathlib import Path

HASH_FILE = Path(__file__).resolve().parent.parent / "docs" / "CLI_HELP_SHA256.txt"


def get_help_output() -> str:
    result = subprocess.run(
        ["fin123", "--help"],
        capture_output=True,
        text=True,
    )
    return result.stdout


def normalize(text: str) -> str:
    """Normalize help output for deterministic hashing.

    - Strip ANSI escape sequences.
    - Collapse runs of whitespace to single spaces.
    - Strip leading/trailing whitespace per line.
    - Remove blank lines.
    - Lowercase for case-insensitive comparison.
    """
    # Strip ANSI
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)
    lines = []
    for line in text.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line.lower())
    return "\n".join(lines) + "\n"


def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="CLI contract check")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Update the expected hash file instead of checking.",
    )
    args = parser.parse_args()

    raw = get_help_output()
    if not raw.strip():
        print("ERROR: fin123 --help produced no output.", file=sys.stderr)
        return 1

    normalized = normalize(raw)
    current_hash = compute_hash(normalized)

    if args.write:
        HASH_FILE.parent.mkdir(parents=True, exist_ok=True)
        HASH_FILE.write_text(current_hash + "\n")
        print(f"Updated {HASH_FILE}")
        print(f"Hash: {current_hash}")
        return 0

    if not HASH_FILE.exists():
        print(f"ERROR: Expected hash file not found: {HASH_FILE}", file=sys.stderr)
        print("Run with --write to create it:", file=sys.stderr)
        print(f"  python {__file__} --write", file=sys.stderr)
        return 1

    expected_hash = HASH_FILE.read_text().strip()

    if current_hash == expected_hash:
        print(f"CLI contract check passed. Hash: {current_hash}")
        return 0

    print("CLI contract check FAILED.", file=sys.stderr)
    print(f"  Expected: {expected_hash}", file=sys.stderr)
    print(f"  Actual:   {current_hash}", file=sys.stderr)
    print("", file=sys.stderr)
    print("If the change is intentional, update the hash:", file=sys.stderr)
    print(f"  python {__file__} --write", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
