#!/usr/bin/env python3
"""CLI contract check -- detect CLI surface drift.

Runs `fin123 __contract` to get a deterministic JSON representation of the
CLI surface (commands, options, arguments, global flags), hashes it with
SHA-256, and compares against a checked-in expected hash.

Usage:
    python scripts/cli_contract_check.py          # verify (exit non-zero on mismatch)
    python scripts/cli_contract_check.py --write   # update expected hash file
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

HASH_FILE = Path(__file__).resolve().parent.parent / "docs" / "CLI_CONTRACT_SHA256.txt"


def get_contract() -> dict:
    """Run `fin123 __contract` and return parsed JSON."""
    result = subprocess.run(
        ["fin123", "__contract"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"ERROR: fin123 __contract exited {result.returncode}", file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def canonicalize(contract: dict) -> bytes:
    """Produce deterministic canonical JSON bytes."""
    return json.dumps(contract, indent=2, sort_keys=True, ensure_ascii=True).encode("utf-8")


def compute_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="CLI contract check")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Update the expected hash file instead of checking.",
    )
    args = parser.parse_args()

    contract = get_contract()
    canonical = canonicalize(contract)
    current_hash = compute_hash(canonical)

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
