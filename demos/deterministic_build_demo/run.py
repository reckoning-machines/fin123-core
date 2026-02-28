"""Deterministic Build demo runner.

Scaffolds a project from the single_company template, commits, builds,
verifies, and writes a deterministic summary JSON with stable hashes.
No timestamps or run_ids are printed.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

_DEMO_DIR = Path(__file__).parent


def run_demo(output_dir: Path | None = None) -> dict[str, Any]:
    """Execute the deterministic build demo end-to-end.

    Args:
        output_dir: Directory to write output files. Defaults to the demo directory.

    Returns:
        Summary dict with stable hashes.
    """
    from fin123.template_engine import scaffold_from_template
    from fin123.verify import verify_run
    from fin123.workbook import Workbook

    out = output_dir or _DEMO_DIR

    # Use a fixed subdirectory for the demo project
    project_dir = out / "_demo_project"
    if project_dir.exists():
        shutil.rmtree(project_dir)

    # 1. Scaffold from single_company template
    scaffold_from_template(target_dir=project_dir, name="single_company")

    # 2. Build (commit happens inside Workbook.run)
    wb = Workbook(project_dir)
    result = wb.run()
    run_id = result.run_dir.name

    # 3. Read run_meta for hashes
    meta = json.loads((result.run_dir / "run_meta.json").read_text())

    # 4. Verify
    report = verify_run(project_dir, run_id)

    # 5. Write deterministic summary (no timestamps, no run_id, no model_id-dependent hashes)
    summary = {
        "demo": "deterministic_build",
        "export_hash": meta.get("export_hash", ""),
        "params_hash": meta.get("params_hash", ""),
        "scalars": sorted(result.scalars.keys()),
        "tables": sorted(result.tables.keys()),
        "template": "single_company",
        "verify_status": report["status"],
    }

    summary_path = out / "deterministic_build_summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )

    # Print only stable values (no run_id, no timestamps)
    print(f"Params Hash: {summary['params_hash']}")
    print(f"Export Hash: {summary['export_hash']}")
    print(f"Verify: {report['status'].upper()}")

    # Cleanup demo project to avoid leftover state
    shutil.rmtree(project_dir)

    return summary
