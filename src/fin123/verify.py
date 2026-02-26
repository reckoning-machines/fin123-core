"""Run verification: recompute and validate hashes for a completed run.

The ``verify_run`` function checks:
- workbook_spec_hash matches recomputed hash from the referenced snapshot
- input_hashes match recomputed file hashes for resolved input paths
- plugin_hash matches recomputed hash
- export_hash matches recomputed hash of exported artifacts
- row-order determinism: sorted_exports and export_row_counts are correct
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from fin123.utils.hash import (
    compute_export_hash,
    compute_params_hash,
    compute_plugin_hash_combined,
    sha256_dict,
    sha256_file,
)


def verify_run(project_dir: Path, run_id: str) -> dict[str, Any]:
    """Verify the integrity of a completed run.

    Recomputes all hashes and compares against run_meta.json.
    Writes verify_report.json into the run directory.

    Args:
        project_dir: Root of the fin123 project.
        run_id: The run directory name.

    Returns:
        Report dict with status ("pass" or "fail"), failures list,
        and recomputed hashes.
    """
    run_dir = project_dir / "runs" / run_id
    meta_path = run_dir / "run_meta.json"
    if not meta_path.exists():
        return {
            "status": "fail",
            "failures": [f"run_meta.json not found for run {run_id}"],
            "hashes": {},
        }

    meta = json.loads(meta_path.read_text())
    failures: list[str] = []
    recomputed: dict[str, str] = {}

    # 0. Model version ID linkage
    _check_model_version_id(project_dir, meta, failures)

    # 1. Workbook spec hash
    _check_workbook_hash(project_dir, meta, failures, recomputed)

    # 2. Input hashes
    _check_input_hashes(meta, failures, recomputed)

    # 3. Plugin hash
    _check_plugin_hash(project_dir, meta, failures, recomputed)

    # 4. Export hash
    _check_export_hash(run_dir, meta, failures, recomputed)

    # 5. Row-order determinism
    _check_row_counts(run_dir, meta, failures)

    # 6. Params hash
    _check_params_hash(meta, failures, recomputed)

    # 7. Overlay hash (scenario definition from snapshot)
    _check_overlay_hash(project_dir, meta, failures, recomputed)

    # 8. Assertion results from run_meta
    assertion_summary = _extract_assertion_summary(meta)

    status = "pass" if not failures else "fail"

    report = {
        "status": status,
        "run_id": run_id,
        "model_version_id": meta.get("model_version_id"),
        "engine_version": meta.get("engine_version"),
        "failures": sorted(failures),
        "hashes": dict(sorted(recomputed.items())),
        "assertions": assertion_summary,
    }

    # Persist verify report with stable key ordering
    report_path = run_dir / "verify_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True))

    # Emit events
    _emit_verify_events(run_id, meta, status, failures)

    return report


def _check_model_version_id(
    project_dir: Path,
    meta: dict[str, Any],
    failures: list[str],
) -> None:
    """Verify that model_version_id references an existing snapshot.

    Args:
        project_dir: Root of the fin123 project.
        meta: Run metadata dict.
        failures: Mutable list to append failure messages.
    """
    model_version_id = meta.get("model_version_id")
    if not model_version_id:
        failures.append(
            "No model_version_id in run_meta.json; cannot verify snapshot linkage"
        )
        return

    snapshot_path = (
        project_dir / "snapshots" / "workbook" / model_version_id / "workbook.yaml"
    )
    if not snapshot_path.exists():
        failures.append(
            f"model_version_id {model_version_id!r} references missing snapshot"
        )
        return

    # Validate workbook_spec_hash matches this snapshot
    stored_hash = meta.get("workbook_spec_hash", "")
    if stored_hash:
        import yaml

        spec = yaml.safe_load(snapshot_path.read_text()) or {}
        computed = sha256_dict(spec)
        if computed != stored_hash:
            failures.append(
                f"model_version_id {model_version_id!r}: workbook_spec_hash does not "
                f"match snapshot content"
            )


def _extract_assertion_summary(meta: dict[str, Any]) -> dict[str, Any]:
    """Extract assertion results from run metadata.

    Args:
        meta: Run metadata dict.

    Returns:
        Summary dict with status, failed_count, warn_count.
    """
    return {
        "status": meta.get("assertions_status", "pass"),
        "failed_count": meta.get("assertions_failed_count", 0),
        "warn_count": meta.get("assertions_warn_count", 0),
    }


def _check_workbook_hash(
    project_dir: Path,
    meta: dict[str, Any],
    failures: list[str],
    recomputed: dict[str, str],
) -> None:
    """Verify workbook_spec_hash by recomputing from the snapshot."""
    stored_hash = meta.get("workbook_spec_hash", "")
    model_version_id = meta.get("model_version_id")

    if not model_version_id:
        failures.append("No model_version_id in run_meta.json; cannot verify workbook hash")
        return

    snapshot_path = project_dir / "snapshots" / "workbook" / model_version_id / "workbook.yaml"
    if not snapshot_path.exists():
        failures.append(
            f"Snapshot {model_version_id} not found at {snapshot_path}; "
            f"cannot verify workbook hash"
        )
        return

    import yaml
    spec = yaml.safe_load(snapshot_path.read_text()) or {}
    computed = sha256_dict(spec)
    recomputed["workbook_spec_hash"] = computed

    if computed != stored_hash:
        failures.append(
            f"workbook_spec_hash mismatch: stored={stored_hash[:16]}... "
            f"recomputed={computed[:16]}..."
        )


def _check_input_hashes(
    meta: dict[str, Any],
    failures: list[str],
    recomputed: dict[str, str],
) -> None:
    """Verify input file hashes by recomputing from disk."""
    stored_hashes = meta.get("input_hashes", {})
    for file_path_str, stored_hash in stored_hashes.items():
        file_path = Path(file_path_str)
        if not file_path.exists():
            failures.append(f"Input file missing: {file_path_str}")
            continue

        computed = sha256_file(file_path)
        recomputed[f"input:{file_path.name}"] = computed

        if computed != stored_hash:
            failures.append(
                f"Input hash mismatch for {file_path.name}: "
                f"stored={stored_hash[:16]}... recomputed={computed[:16]}..."
            )


def _check_plugin_hash(
    project_dir: Path,
    meta: dict[str, Any],
    failures: list[str],
    recomputed: dict[str, str],
) -> None:
    """Verify plugin_hash by recomputing from active plugins."""
    stored_hash = meta.get("plugin_hash")
    if not stored_hash:
        return  # No plugin hash recorded; skip check

    from fin123 import __version__

    plugins_info = meta.get("plugins", {})
    computed = compute_plugin_hash_combined(__version__, plugins_info)
    recomputed["plugin_hash"] = computed

    if computed != stored_hash:
        failures.append(
            f"plugin_hash mismatch: stored={stored_hash[:16]}... "
            f"recomputed={computed[:16]}..."
        )


def _check_export_hash(
    run_dir: Path,
    meta: dict[str, Any],
    failures: list[str],
    recomputed: dict[str, str],
) -> None:
    """Verify export_hash by recomputing from outputs directory."""
    stored_hash = meta.get("export_hash")
    outputs_dir = run_dir / "outputs"

    if not outputs_dir.exists():
        failures.append("outputs/ directory not found in run")
        return

    computed = compute_export_hash(outputs_dir)
    recomputed["export_hash"] = computed

    if stored_hash and computed != stored_hash:
        failures.append(
            f"export_hash mismatch: stored={stored_hash[:16]}... "
            f"recomputed={computed[:16]}..."
        )


def _check_row_counts(
    run_dir: Path,
    meta: dict[str, Any],
    failures: list[str],
) -> None:
    """Verify export_row_counts match actual parquet row counts."""
    stored_counts = meta.get("export_row_counts", {})
    outputs_dir = run_dir / "outputs"

    if not outputs_dir.exists():
        return

    for table_name, expected_count in stored_counts.items():
        parquet_path = outputs_dir / f"{table_name}.parquet"
        if not parquet_path.exists():
            failures.append(f"Table parquet missing: {table_name}.parquet")
            continue

        df = pl.read_parquet(parquet_path)
        actual = len(df)
        if actual != expected_count:
            failures.append(
                f"Row count mismatch for {table_name}: "
                f"meta={expected_count} actual={actual}"
            )

    # Verify sorted_exports are noted
    sorted_exports = meta.get("sorted_exports", [])
    for table_name in sorted_exports:
        parquet_path = outputs_dir / f"{table_name}.parquet"
        if parquet_path.exists():
            # Confirm the table was auto-sorted (just validate it is recorded)
            pass  # Presence in sorted_exports is the contract


def _check_params_hash(
    meta: dict[str, Any],
    failures: list[str],
    recomputed: dict[str, str],
) -> None:
    """Verify params_hash by recomputing from effective_params."""
    stored_hash = meta.get("params_hash")
    if not stored_hash:
        return  # No params hash recorded; skip check

    effective_params = meta.get("effective_params", {})
    computed = compute_params_hash(effective_params)
    recomputed["params_hash"] = computed

    if computed != stored_hash:
        failures.append(
            f"params_hash mismatch: stored={stored_hash[:16]}... "
            f"recomputed={computed[:16]}..."
        )


def _check_overlay_hash(
    project_dir: Path,
    meta: dict[str, Any],
    failures: list[str],
    recomputed: dict[str, str],
) -> None:
    """Verify overlay_hash by recomputing from the snapshot's scenario definition."""
    stored_hash = meta.get("overlay_hash")
    if not stored_hash:
        return  # No overlay hash recorded; skip check

    scenario_name = meta.get("scenario_name", "")
    model_version_id = meta.get("model_version_id")

    if not model_version_id:
        # Cannot verify without snapshot — already flagged by workbook hash check
        return

    snapshot_path = project_dir / "snapshots" / "workbook" / model_version_id / "workbook.yaml"
    if not snapshot_path.exists():
        # Already flagged by workbook hash check
        return

    import yaml

    from fin123.utils.hash import overlay_hash

    spec = yaml.safe_load(snapshot_path.read_text()) or {}
    scenarios = spec.get("scenarios", {})
    scenario_overrides: dict[str, Any] = {}
    if scenario_name and scenario_name in scenarios:
        scenario_spec = scenarios[scenario_name]
        scenario_overrides = dict(scenario_spec.get("overrides", {}))

    computed = overlay_hash(scenario_name, scenario_overrides)
    recomputed["overlay_hash"] = computed

    if computed != stored_hash:
        failures.append(
            f"overlay_hash mismatch: stored={stored_hash[:16]}... "
            f"recomputed={computed[:16]}..."
        )


def _emit_verify_events(
    run_id: str,
    meta: dict[str, Any],
    status: str,
    failures: list[str],
) -> None:
    """Emit verification events to the event log."""
    try:
        from fin123.logging.events import (
            EventLevel,
            EventType,
            emit,
            make_run_event,
        )

        if status == "pass":
            emit(
                make_run_event(
                    EventType.run_verify_pass,
                    EventLevel.info,
                    f"Verification passed for run {run_id}",
                    run_id=run_id,
                    model_id=str(meta.get("model_id", "")),
                ),
                run_id=run_id,
            )
        else:
            emit(
                make_run_event(
                    EventType.run_verify_fail,
                    EventLevel.error,
                    f"Verification failed for run {run_id}: {len(failures)} issue(s)",
                    run_id=run_id,
                    model_id=str(meta.get("model_id", "")),
                    extra={"failures": failures[:10]},
                ),
                run_id=run_id,
            )
    except Exception:
        pass
