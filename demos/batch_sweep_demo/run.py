"""Batch Sweep demo runner.

Scaffolds a project from the single_company template and runs its
three built-in scenarios (base, bull, bear), producing materially
different outputs with distinct export hashes.  Writes a deterministic
batch_manifest.json with stable hashes per scenario.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

_DEMO_DIR = Path(__file__).parent

# Fixed 3-scenario names matching single_company template
_SCENARIOS = ["base", "bull", "bear"]


def run_demo(output_dir: Path | None = None) -> dict[str, Any]:
    """Execute the batch sweep demo end-to-end.

    Args:
        output_dir: Directory to write output files. Defaults to the demo directory.

    Returns:
        Batch manifest dict.
    """
    from fin123.template_engine import scaffold_from_template
    from fin123.workbook import Workbook

    out = output_dir or _DEMO_DIR

    # Use a fixed subdirectory for the demo project
    project_dir = out / "_demo_project"
    if project_dir.exists():
        shutil.rmtree(project_dir)

    # 1. Scaffold from single_company template
    scaffold_from_template(target_dir=project_dir, name="single_company")

    # 2. Run scenario builds sequentially (deterministic order)
    scenarios: list[dict[str, Any]] = []
    for idx, scenario_name in enumerate(_SCENARIOS):
        wb = Workbook(project_dir, scenario_name=scenario_name)
        result = wb.run()
        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        scenarios.append({
            "export_hash": meta.get("export_hash", ""),
            "index": idx,
            "params_hash": meta.get("params_hash", ""),
            "scenario": scenario_name,
        })
        print(f"  [{idx}] {scenario_name} -> export_hash={meta.get('export_hash', '')[:16]}...")

    # 3. Write deterministic batch manifest (no timestamps, no UUIDs)
    manifest = {
        "demo": "batch_sweep",
        "scenario_count": len(scenarios),
        "scenarios": scenarios,
        "template": "single_company",
    }

    manifest_path = out / "batch_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )

    print(f"Batch manifest written: {len(scenarios)} scenario(s)")

    # Cleanup demo project
    shutil.rmtree(project_dir)

    return manifest
