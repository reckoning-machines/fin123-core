#!/usr/bin/env python3
"""Generate SHA256SUMS.txt for all release ZIP files in dist/."""

import hashlib
import sys
from pathlib import Path


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    dist_dir = Path("dist")
    if not dist_dir.exists():
        print("dist/ directory not found", file=sys.stderr)
        sys.exit(1)

    zips = sorted(dist_dir.glob("fin123-core-*.zip"))
    if not zips:
        print("No fin123-core-*.zip files found in dist/", file=sys.stderr)
        sys.exit(1)

    lines = []
    for z in zips:
        digest = sha256_file(z)
        lines.append(f"{digest}  {z.name}")
        print(f"{digest}  {z.name}")

    sums_path = dist_dir / "SHA256SUMS.txt"
    sums_path.write_text("\n".join(lines) + "\n")
    print(f"\nWritten: {sums_path}")


if __name__ == "__main__":
    main()
