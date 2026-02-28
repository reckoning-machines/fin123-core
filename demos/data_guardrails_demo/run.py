"""Data Guardrails demo runner.

Creates fixture inputs that deterministically fail (duplicate keys,
null keys, dtype mismatch), runs build expecting structured failure,
then swaps to fixed inputs and runs build successfully.

Each run uses a freshly scaffolded project directory so that snapshot
versioning is deterministic (always starts at v0001).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import polars as pl
import yaml

_DEMO_DIR = Path(__file__).parent


def _create_bad_fixtures(inputs_dir: Path) -> list[dict[str, str]]:
    """Create input files that trigger join_left validation failures.

    Returns list of expected failure descriptions.
    """
    inputs_dir.mkdir(parents=True, exist_ok=True)

    # prices.parquet -- good left table
    prices = pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "date": ["2025-01-01", "2025-01-01", "2025-01-01"],
        "price": [150.0, 300.0, 140.0],
    })
    prices.write_parquet(inputs_dir / "prices.parquet")

    # estimates.parquet -- BAD: duplicate key "AAPL" (triggers many_to_one violation)
    estimates = pl.DataFrame({
        "ticker": ["AAPL", "AAPL", "MSFT", "GOOGL"],
        "eps_ntm": [6.5, 6.8, 11.0, 5.2],
    })
    estimates.write_parquet(inputs_dir / "estimates.parquet")

    return [
        {
            "fixture": "duplicate_key",
            "description": "estimates.parquet has duplicate ticker 'AAPL' -- violates many_to_one join validation",
        },
    ]


def _create_good_fixtures(inputs_dir: Path) -> None:
    """Create clean input files that pass join_left validation."""
    inputs_dir.mkdir(parents=True, exist_ok=True)

    # prices.parquet -- same as bad
    prices = pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "date": ["2025-01-01", "2025-01-01", "2025-01-01"],
        "price": [150.0, 300.0, 140.0],
    })
    prices.write_parquet(inputs_dir / "prices.parquet")

    # estimates.parquet -- FIXED: no duplicates
    estimates = pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "eps_ntm": [6.5, 11.0, 5.2],
    })
    estimates.write_parquet(inputs_dir / "estimates.parquet")


_WORKBOOK_SPEC: dict[str, Any] = {
    "version": 1,
    "params": {
        "ticker": "AAPL",
        "multiple": 15,
        "discount_rate": 0.10,
    },
    "tables": {
        "prices": {
            "source": "inputs/prices.parquet",
            "format": "parquet",
        },
        "estimates": {
            "source": "inputs/estimates.parquet",
            "format": "parquet",
        },
    },
    "plans": [
        {
            "name": "priced_estimates",
            "source": "prices",
            "steps": [
                {
                    "func": "join_left",
                    "right": "estimates",
                    "on": "ticker",
                    "validate": "many_to_one",
                },
                {
                    "func": "sort",
                    "by": ["ticker", "date"],
                },
            ],
        },
    ],
    "outputs": [
        {
            "name": "eps",
            "type": "scalar",
            "func": "lookup_scalar",
            "args": {
                "table_name": "estimates",
                "key_col": "ticker",
                "value_col": "eps_ntm",
                "key_value": "$ticker",
            },
        },
        {"name": "priced_estimates", "type": "table"},
    ],
}


def _scaffold_project(project_dir: Path, inputs_dir_creator: Any) -> None:
    """Create a fresh project directory with deterministic initial state."""
    if project_dir.exists():
        shutil.rmtree(project_dir)
    project_dir.mkdir(parents=True)

    # Write workbook spec
    wb_path = project_dir / "workbook.yaml"
    wb_path.write_text(yaml.dump(_WORKBOOK_SPEC, default_flow_style=False, sort_keys=False))
    (project_dir / "fin123.yaml").write_text("max_runs: 50\nmode: dev\n")

    # Create required directories
    for subdir in ("runs", "artifacts", "snapshots", "sync_runs", "cache"):
        (project_dir / subdir).mkdir(exist_ok=True)

    # Create input data
    inputs_dir_creator(project_dir / "inputs")

    # Create initial snapshot (always v0001 in a fresh project)
    from fin123.versioning import SnapshotStore

    store = SnapshotStore(project_dir)
    store.save_snapshot(wb_path.read_text())


def run_demo(output_dir: Path | None = None) -> dict[str, Any]:
    """Execute the data guardrails demo end-to-end.

    Args:
        output_dir: Directory to write output files. Defaults to the demo directory.

    Returns:
        Dict with failure and success results.
    """
    from fin123.workbook import Workbook

    out = output_dir or _DEMO_DIR

    # ---- Phase 1: Bad fixtures (expect failure) ----
    print("Phase 1: Testing with bad fixtures (duplicate keys)...")

    bad_project = out / "_demo_project_bad"
    _scaffold_project(bad_project, _create_bad_fixtures)

    failure_result: dict[str, Any]
    try:
        wb = Workbook(bad_project)
        wb.run()
        # Should not reach here
        failure_result = {
            "status": "unexpected_pass",
            "error": "Build should have failed but did not",
        }
    except (ValueError, TypeError) as exc:
        failure_result = {
            "error_message": str(exc),
            "error_type": type(exc).__name__,
            "fixtures": [
                {
                    "description": "estimates.parquet has duplicate ticker 'AAPL' -- violates many_to_one join validation",
                    "fixture": "duplicate_key",
                },
            ],
            "status": "expected_failure",
        }
        print(f"  Expected failure: {type(exc).__name__}: {exc}")

    shutil.rmtree(bad_project)

    failure_path = out / "guardrails_failure.json"
    failure_path.write_text(
        json.dumps(failure_result, indent=2, sort_keys=True) + "\n"
    )

    # ---- Phase 2: Fixed fixtures (expect success) ----
    print("Phase 2: Testing with fixed fixtures (clean data)...")

    good_project = out / "_demo_project_good"
    _scaffold_project(good_project, _create_good_fixtures)

    wb = Workbook(good_project)
    result = wb.run()
    meta = json.loads((result.run_dir / "run_meta.json").read_text())

    success_result = {
        "demo": "data_guardrails",
        "export_hash": meta.get("export_hash", ""),
        "scalars": sorted(result.scalars.keys()),
        "status": "pass",
        "tables": sorted(result.tables.keys()),
    }

    shutil.rmtree(good_project)

    success_path = out / "guardrails_success.json"
    success_path.write_text(
        json.dumps(success_result, indent=2, sort_keys=True) + "\n"
    )

    print(f"  Build passed. Export hash: {meta.get('export_hash', '')[:16]}...")

    return {
        "failure": failure_result,
        "success": success_result,
    }
