"""Batch build orchestration for fin123.

Runs a workbook multiple times with different parameter sets,
loaded from a CSV file.
"""

from __future__ import annotations

import csv
import json
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any
from uuid import uuid4


def load_params_csv(path: Path) -> list[dict[str, Any]]:
    """Load parameter sets from a CSV file.

    Each row becomes a dict of param_name -> value. Values are
    converted to float where possible.

    Args:
        path: Path to the CSV file.

    Returns:
        List of parameter dicts.
    """
    rows: list[dict[str, Any]] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed: dict[str, Any] = {}
            for k, v in row.items():
                try:
                    parsed[k] = float(v)
                except (ValueError, TypeError):
                    parsed[k] = v
            rows.append(parsed)
    return rows


def run_batch(
    project_dir: Path,
    params_rows: list[dict[str, Any]],
    scenario_name: str | None = None,
    max_workers: int = 1,
) -> dict[str, Any]:
    """Run a batch of workbook builds with different parameter sets.

    Args:
        project_dir: Root of the fin123 project.
        params_rows: List of parameter dicts (one per build).
        scenario_name: Optional scenario to apply to each build.
        max_workers: Number of parallel workers (1 = sequential).

    Returns:
        Summary dict with batch_id, results list, and counts.
    """
    batch_id = str(uuid4())

    # Emit batch_started event
    _emit_batch_event(
        project_dir, batch_id, "started",
        total=len(params_rows), scenario_name=scenario_name,
    )

    if max_workers <= 1:
        results = _run_sequential(project_dir, params_rows, scenario_name, batch_id)
    else:
        results = _run_parallel(project_dir, params_rows, scenario_name, batch_id, max_workers)

    ok_count = sum(1 for r in results if r["status"] == "ok")
    fail_count = sum(1 for r in results if r["status"] == "error")

    # Emit batch_completed event
    _emit_batch_event(
        project_dir, batch_id, "completed",
        total=len(results), ok=ok_count, failed=fail_count,
    )

    return {
        "build_batch_id": batch_id,
        "total": len(results),
        "ok": ok_count,
        "failed": fail_count,
        "results": results,
    }


def _run_sequential(
    project_dir: Path,
    params_rows: list[dict[str, Any]],
    scenario_name: str | None,
    batch_id: str,
) -> list[dict[str, Any]]:
    """Run builds sequentially."""
    results: list[dict[str, Any]] = []
    for idx, params in enumerate(params_rows):
        result = _run_single_build(project_dir, params, scenario_name, batch_id, idx)
        results.append(result)
    return results


def _run_single_build(
    project_dir: Path,
    params: dict[str, Any],
    scenario_name: str | None,
    batch_id: str,
    index: int,
) -> dict[str, Any]:
    """Run a single build within a batch."""
    try:
        from fin123.workbook import Workbook

        wb = Workbook(project_dir, overrides=params, scenario_name=scenario_name)
        result = wb.run()

        # Amend batch metadata
        _amend_batch_meta(result.run_dir, batch_id, index)

        return {
            "index": index,
            "status": "ok",
            "run_id": result.run_dir.name,
            "params": params,
        }
    except Exception as exc:
        return {
            "index": index,
            "status": "error",
            "error": str(exc),
            "params": params,
        }


def _run_single_args(args: tuple) -> dict[str, Any]:
    """Top-level picklable function for ProcessPoolExecutor."""
    project_dir, params, scenario_name, batch_id, index = args
    return _run_single_build(Path(project_dir), params, scenario_name, batch_id, index)


def _run_parallel(
    project_dir: Path,
    params_rows: list[dict[str, Any]],
    scenario_name: str | None,
    batch_id: str,
    max_workers: int,
) -> list[dict[str, Any]]:
    """Run builds in parallel using ProcessPoolExecutor."""
    args_list = [
        (str(project_dir), params, scenario_name, batch_id, idx)
        for idx, params in enumerate(params_rows)
    ]
    results: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(_run_single_args, args_list):
            results.append(result)
    return results


def _emit_batch_event(
    project_dir: Path,
    batch_id: str,
    phase: str,
    *,
    total: int = 0,
    ok: int = 0,
    failed: int = 0,
    scenario_name: str | None = None,
) -> None:
    """Emit a batch_started or batch_completed event."""
    try:
        from fin123.logging.events import (
            EventLevel,
            EventType,
            emit,
            make_run_event,
            set_project_dir,
        )

        set_project_dir(project_dir)
        if phase == "started":
            extra: dict[str, Any] = {"build_batch_id": batch_id, "total": total}
            if scenario_name:
                extra["scenario_name"] = scenario_name
            emit(make_run_event(
                EventType.batch_started,
                EventLevel.info,
                f"Batch started: {total} build(s), batch_id={batch_id[:8]}",
                extra=extra,
            ))
        else:
            emit(make_run_event(
                EventType.batch_completed,
                EventLevel.info,
                f"Batch completed: {ok} ok, {failed} failed",
                extra={
                    "build_batch_id": batch_id,
                    "total": total,
                    "ok": ok,
                    "failed": failed,
                },
            ))
    except Exception:
        pass


def _amend_batch_meta(run_dir: Path, batch_id: str, index: int) -> None:
    """Patch run_meta.json with batch metadata.

    Args:
        run_dir: Path to the run directory.
        batch_id: The batch UUID.
        index: The row index within the batch.
    """
    meta_path = run_dir / "run_meta.json"
    if not meta_path.exists():
        return
    meta = json.loads(meta_path.read_text())
    meta["build_batch_id"] = batch_id
    meta["batch_index"] = index
    meta_path.write_text(json.dumps(meta, indent=2))
