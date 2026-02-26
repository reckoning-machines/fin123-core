"""Diff v1: deterministic comparison of runs and workbook snapshot versions.

Read-only — never mutates project state.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from fin123.utils.hash import sha256_file

_MAX_ROW_LEVEL_DIFF = 1_000_000
_MAX_SAMPLE_CHANGES = 20
_MAX_TABLES = 50


# ---------------------------------------------------------------------------
# diff run
# ---------------------------------------------------------------------------


def diff_runs(
    project_dir: Path,
    run_id_a: str,
    run_id_b: str,
) -> dict[str, Any]:
    """Compare two runs and return a structured diff.

    Args:
        project_dir: Root of the fin123 project.
        run_id_a: First run ID.
        run_id_b: Second run ID.

    Returns:
        Structured diff dict.
    """
    project_dir = project_dir.resolve()
    run_dir_a = project_dir / "runs" / run_id_a
    run_dir_b = project_dir / "runs" / run_id_b

    meta_a = _load_run_meta(run_dir_a, run_id_a)
    meta_b = _load_run_meta(run_dir_b, run_id_b)

    result: dict[str, Any] = {
        "type": "run_diff",
        "run_a": run_id_a,
        "run_b": run_id_b,
    }

    # Fast path: export_hash match
    export_hash_a = meta_a.get("export_hash", "")
    export_hash_b = meta_b.get("export_hash", "")

    if export_hash_a and export_hash_b and export_hash_a == export_hash_b:
        result["status"] = "identical"
        result["meta_diff"] = _build_meta_diff(meta_a, meta_b)
        return result

    result["status"] = "different"

    # Meta diff
    result["meta_diff"] = _build_meta_diff(meta_a, meta_b)

    # Scalar diff
    scalars_a = _load_scalars(run_dir_a)
    scalars_b = _load_scalars(run_dir_b)
    result["scalar_diff"] = _diff_scalars(scalars_a, scalars_b)

    # Table diffs
    pk_map = _load_primary_keys(project_dir)
    result["table_diffs"] = _diff_tables(run_dir_a, run_dir_b, pk_map)

    return result


def _load_run_meta(run_dir: Path, run_id: str) -> dict[str, Any]:
    meta_path = run_dir / "run_meta.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"Run {run_id!r} not found: {meta_path}")
    return json.loads(meta_path.read_text())


def _build_meta_diff(meta_a: dict, meta_b: dict) -> dict[str, Any]:
    md: dict[str, Any] = {
        "workbook_spec_hash_match": meta_a.get("workbook_spec_hash") == meta_b.get("workbook_spec_hash"),
        "params_hash_match": meta_a.get("params_hash") == meta_b.get("params_hash"),
        "input_hashes_match": meta_a.get("input_hashes") == meta_b.get("input_hashes"),
        "model_version_id_a": meta_a.get("model_version_id"),
        "model_version_id_b": meta_b.get("model_version_id"),
    }

    # Input hashes diff
    ih_a = meta_a.get("input_hashes", {})
    ih_b = meta_b.get("input_hashes", {})
    changed_inputs: list[dict[str, str]] = []
    all_paths = sorted(set(ih_a) | set(ih_b))
    for p in all_paths:
        ha = ih_a.get(p)
        hb = ih_b.get(p)
        if ha != hb:
            changed_inputs.append({"path": p, "a_hash": ha, "b_hash": hb})
    if changed_inputs:
        md["input_hash_changes"] = changed_inputs

    # Effective params diff
    ep_a = meta_a.get("effective_params", {})
    ep_b = meta_b.get("effective_params", {})
    param_changes: list[dict[str, Any]] = []
    all_keys = sorted(set(ep_a) | set(ep_b))
    for k in all_keys:
        va = ep_a.get(k)
        vb = ep_b.get(k)
        if va != vb:
            param_changes.append({"key": k, "a": va, "b": vb})
    if param_changes:
        md["param_changes"] = param_changes

    return md


def _load_scalars(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "outputs" / "scalars.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _diff_scalars(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    keys_a = set(a)
    keys_b = set(b)

    added = sorted(keys_b - keys_a)
    removed = sorted(keys_a - keys_b)
    common = sorted(keys_a & keys_b)

    changed: list[dict[str, Any]] = []
    for k in common:
        va, vb = a[k], b[k]
        if va != vb:
            entry: dict[str, Any] = {"name": k, "a_value": va, "b_value": vb}
            if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
                entry["delta"] = vb - va
                if va != 0:
                    entry["pct_change"] = round((vb - va) / va, 6)
            changed.append(entry)

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
    }


def _load_primary_keys(project_dir: Path) -> dict[str, str | list[str]]:
    """Load primary_key declarations from workbook.yaml tables spec."""
    wb_path = project_dir / "workbook.yaml"
    if not wb_path.exists():
        return {}
    spec = yaml.safe_load(wb_path.read_text()) or {}
    pk_map: dict[str, str | list[str]] = {}
    for name, tspec in spec.get("tables", {}).items():
        pk = tspec.get("primary_key")
        if pk:
            pk_map[name] = pk
    return pk_map


def _diff_tables(
    run_dir_a: Path,
    run_dir_b: Path,
    pk_map: dict[str, str | list[str]],
) -> list[dict[str, Any]]:
    out_a = run_dir_a / "outputs"
    out_b = run_dir_b / "outputs"

    tables_a = {p.stem: p for p in sorted(out_a.glob("*.parquet"))} if out_a.exists() else {}
    tables_b = {p.stem: p for p in sorted(out_b.glob("*.parquet"))} if out_b.exists() else {}

    all_tables = sorted(set(tables_a) | set(tables_b))
    truncated = len(all_tables) > _MAX_TABLES
    if truncated:
        all_tables = all_tables[:_MAX_TABLES]

    diffs: list[dict[str, Any]] = []

    for tname in all_tables:
        path_a = tables_a.get(tname)
        path_b = tables_b.get(tname)

        td: dict[str, Any] = {"table": tname}

        if path_a is None:
            td["status"] = "added"
            td["b_rowcount"] = _parquet_rowcount(path_b)
            diffs.append(td)
            continue
        if path_b is None:
            td["status"] = "removed"
            td["a_rowcount"] = _parquet_rowcount(path_a)
            diffs.append(td)
            continue

        # Both present
        checksum_a = sha256_file(path_a)
        checksum_b = sha256_file(path_b)
        td["checksum_a"] = checksum_a
        td["checksum_b"] = checksum_b
        td["content_match"] = checksum_a == checksum_b

        schema_a = _parquet_schema(path_a)
        schema_b = _parquet_schema(path_b)
        if schema_a != schema_b:
            td["schema_a"] = schema_a
            td["schema_b"] = schema_b

        rc_a = _parquet_rowcount(path_a)
        rc_b = _parquet_rowcount(path_b)
        td["a_rowcount"] = rc_a
        td["b_rowcount"] = rc_b

        if not td["content_match"]:
            td["status"] = "changed"
            pk = pk_map.get(tname)
            if pk is None:
                td["row_level_diff"] = "skipped"
                td["row_level_diff_reason"] = "no primary_key declared"
            elif rc_a > _MAX_ROW_LEVEL_DIFF or rc_b > _MAX_ROW_LEVEL_DIFF:
                td["row_level_diff"] = "skipped"
                td["row_level_diff_reason"] = f"rowcount exceeds {_MAX_ROW_LEVEL_DIFF}"
            else:
                try:
                    td["row_level_diff"] = _row_level_diff(path_a, path_b, pk)
                except Exception as exc:
                    td["row_level_diff"] = "skipped"
                    td["row_level_diff_reason"] = f"runtime_error: {type(exc).__name__}"
        else:
            td["status"] = "identical"

        diffs.append(td)

    if truncated:
        diffs.append({"_tables_truncated": True})

    return diffs


def _parquet_rowcount(path: Path) -> int:
    import polars as pl
    return pl.scan_parquet(path).select(pl.len()).collect().item()


def _parquet_schema(path: Path) -> dict[str, str]:
    import polars as pl
    lf = pl.scan_parquet(path)
    return {name: str(dtype) for name, dtype in lf.collect_schema().items()}


def _row_level_diff(
    path_a: Path,
    path_b: Path,
    pk: str | list[str],
) -> dict[str, Any]:
    import polars as pl

    pk_cols = [pk] if isinstance(pk, str) else list(pk)

    df_a = pl.read_parquet(path_a)
    df_b = pl.read_parquet(path_b)

    # Index by PK
    keys_a = set(df_a.select(pk_cols).iter_rows())
    keys_b = set(df_b.select(pk_cols).iter_rows())

    added_keys = keys_b - keys_a
    removed_keys = keys_a - keys_b
    common_keys = keys_a & keys_b

    # For common keys, find changed rows
    value_cols = sorted(set(df_a.columns) & set(df_b.columns) - set(pk_cols))

    changed_count = 0
    sample_changes: list[dict[str, Any]] = []

    if common_keys and value_cols:
        # Join on PK to compare
        joined = df_a.join(df_b, on=pk_cols, suffix="_b")
        for col in value_cols:
            col_b = f"{col}_b"
            if col_b not in joined.columns:
                continue
            # Mark rows where col differs
            joined = joined.with_columns(
                (pl.col(col).ne(pl.col(col_b))).alias(f"_diff_{col}")
            )

        diff_flag_cols = [c for c in joined.columns if c.startswith("_diff_")]
        if diff_flag_cols:
            any_diff = joined.select(
                pk_cols + diff_flag_cols
            ).filter(
                pl.any_horizontal(*[pl.col(c) for c in diff_flag_cols])
            )
            changed_count = len(any_diff)

            # Collect sample changes
            for row in any_diff.head(_MAX_SAMPLE_CHANGES).iter_rows(named=True):
                pk_val = {c: row[c] for c in pk_cols}
                changed_cols: dict[str, dict[str, Any]] = {}
                for col in value_cols:
                    flag = f"_diff_{col}"
                    if flag in row and row[flag]:
                        # Look up values
                        a_row = joined.filter(
                            pl.all_horizontal(*[pl.col(c) == row[c] for c in pk_cols])
                        ).head(1)
                        if len(a_row) > 0:
                            changed_cols[col] = {
                                "a": a_row[col][0],
                                "b": a_row[f"{col}_b"][0],
                            }
                if changed_cols:
                    sample_changes.append({"key": pk_val, "changes": changed_cols})

    return {
        "rows_added": len(added_keys),
        "rows_removed": len(removed_keys),
        "rows_changed": changed_count,
        "sample_changes": sample_changes,
    }


# ---------------------------------------------------------------------------
# diff version (snapshot)
# ---------------------------------------------------------------------------


def diff_versions(
    project_dir: Path,
    version_a: str,
    version_b: str,
) -> dict[str, Any]:
    """Compare two workbook snapshot versions.

    Args:
        project_dir: Root of the fin123 project.
        version_a: First snapshot version (e.g. 'v0001').
        version_b: Second snapshot version (e.g. 'v0002').

    Returns:
        Structured diff dict.
    """
    project_dir = project_dir.resolve()
    from fin123.versioning import SnapshotStore

    store = SnapshotStore(project_dir)
    spec_a = store.load_version(version_a)
    spec_b = store.load_version(version_b)

    result: dict[str, Any] = {
        "type": "version_diff",
        "version_a": version_a,
        "version_b": version_b,
    }

    # Params diff
    params_a = spec_a.get("params", {})
    params_b = spec_b.get("params", {})
    result["params_diff"] = _diff_dicts_flat(params_a, params_b)

    # Tables diff
    tables_a = spec_a.get("tables", {})
    tables_b = spec_b.get("tables", {})
    result["tables_diff"] = _diff_dicts_flat(tables_a, tables_b)

    # Outputs diff
    outputs_a = _outputs_by_name(spec_a.get("outputs", []))
    outputs_b = _outputs_by_name(spec_b.get("outputs", []))
    result["outputs_diff"] = _diff_outputs(outputs_a, outputs_b)

    # Scenarios diff
    scenarios_a = spec_a.get("scenarios", {})
    scenarios_b = spec_b.get("scenarios", {})
    result["scenarios_diff"] = _diff_dicts_flat(scenarios_a, scenarios_b)

    # Plans diff
    plans_a = _plans_by_name(spec_a.get("plans", []))
    plans_b = _plans_by_name(spec_b.get("plans", []))
    result["plans_diff"] = _diff_dicts_flat(plans_a, plans_b)

    # Assertions diff
    assertions_a = _assertions_by_name(spec_a.get("assertions", []))
    assertions_b = _assertions_by_name(spec_b.get("assertions", []))
    result["assertions_diff"] = _diff_dicts_flat(assertions_a, assertions_b)

    return result


def _outputs_by_name(outputs: list[dict]) -> dict[str, dict]:
    return {o["name"]: o for o in outputs if "name" in o}


def _plans_by_name(plans: list[dict]) -> dict[str, dict]:
    return {p["name"]: p for p in plans if "name" in p}


def _assertions_by_name(assertions: list[dict]) -> dict[str, dict]:
    return {a["name"]: a for a in assertions if "name" in a}


def _diff_dicts_flat(a: dict, b: dict) -> dict[str, Any]:
    """Diff two dicts: report added/removed/changed keys with stable ordering."""
    keys_a = set(a)
    keys_b = set(b)

    added = sorted(keys_b - keys_a)
    removed = sorted(keys_a - keys_b)
    changed: list[dict[str, Any]] = []

    for k in sorted(keys_a & keys_b):
        va = a[k]
        vb = b[k]
        if va != vb:
            entry: dict[str, Any] = {"key": k, "a": va, "b": vb}
            # Distinguish formula vs value for outputs
            if isinstance(va, dict) and isinstance(vb, dict):
                if va.get("formula") != vb.get("formula"):
                    entry["change_type"] = "formula"
                elif va.get("value") != vb.get("value"):
                    entry["change_type"] = "value"
                else:
                    entry["change_type"] = "other"
            changed.append(entry)

    return {"added": added, "removed": removed, "changed": changed}


def _diff_outputs(a: dict[str, dict], b: dict[str, dict]) -> dict[str, Any]:
    """Diff outputs with formula vs value distinction."""
    keys_a = set(a)
    keys_b = set(b)

    added = sorted(keys_b - keys_a)
    removed = sorted(keys_a - keys_b)
    changed: list[dict[str, Any]] = []

    for k in sorted(keys_a & keys_b):
        oa = a[k]
        ob = b[k]
        if oa != ob:
            entry: dict[str, Any] = {"name": k, "a": oa, "b": ob}
            # Determine change type
            fa = oa.get("formula")
            fb = ob.get("formula")
            va = oa.get("value")
            vb = ob.get("value")
            if fa != fb:
                entry["change_type"] = "formula"
            elif va != vb:
                entry["change_type"] = "value"
            else:
                entry["change_type"] = "other"
            changed.append(entry)

    return {"added": added, "removed": removed, "changed": changed}


# ---------------------------------------------------------------------------
# Human-readable formatting
# ---------------------------------------------------------------------------


def format_run_diff(d: dict[str, Any]) -> str:
    """Format a run diff dict as human-readable text."""
    lines: list[str] = []
    lines.append(f"Run diff: {d['run_a']} vs {d['run_b']}")
    lines.append(f"Status: {d['status']}")

    md = d.get("meta_diff", {})
    lines.append(f"  workbook_spec_hash_match: {md.get('workbook_spec_hash_match')}")
    lines.append(f"  params_hash_match: {md.get('params_hash_match')}")
    lines.append(f"  input_hashes_match: {md.get('input_hashes_match')}")

    if d["status"] == "identical":
        return "\n".join(lines)

    for pc in md.get("param_changes", []):
        lines.append(f"  param {pc['key']}: {pc['a']} → {pc['b']}")

    for ic in md.get("input_hash_changes", []):
        lines.append(f"  input {ic['path']}: {ic['a_hash'][:12]}… → {ic['b_hash'][:12]}…")

    sd = d.get("scalar_diff", {})
    if sd.get("added"):
        lines.append(f"Scalars added: {', '.join(sd['added'])}")
    if sd.get("removed"):
        lines.append(f"Scalars removed: {', '.join(sd['removed'])}")
    for c in sd.get("changed", []):
        delta_str = ""
        if "delta" in c:
            delta_str = f" (delta={c['delta']}"
            if "pct_change" in c:
                delta_str += f", pct={c['pct_change']:.4%}"
            delta_str += ")"
        lines.append(f"  {c['name']}: {c['a_value']} → {c['b_value']}{delta_str}")

    for td in d.get("table_diffs", []):
        if "_tables_truncated" in td:
            lines.append("(tables truncated to 50)")
            continue
        tname = td["table"]
        status = td.get("status", "unknown")
        if status == "added":
            lines.append(f"Table {tname}: added ({td.get('b_rowcount', '?')} rows)")
        elif status == "removed":
            lines.append(f"Table {tname}: removed ({td.get('a_rowcount', '?')} rows)")
        elif status == "identical":
            lines.append(f"Table {tname}: identical ({td.get('a_rowcount', '?')} rows)")
        else:
            lines.append(f"Table {tname}: changed (rows {td.get('a_rowcount')}→{td.get('b_rowcount')})")
            if td.get("row_level_diff") == "skipped":
                lines.append(f"  row-level diff skipped: {td.get('row_level_diff_reason', 'unknown')}")
            elif isinstance(td.get("row_level_diff"), dict):
                rld = td["row_level_diff"]
                lines.append(f"  rows added={rld['rows_added']} removed={rld['rows_removed']} changed={rld['rows_changed']}")

    return "\n".join(lines)


def format_version_diff(d: dict[str, Any]) -> str:
    """Format a version diff dict as human-readable text."""
    lines: list[str] = []
    lines.append(f"Version diff: {d['version_a']} vs {d['version_b']}")

    for section in ("params_diff", "tables_diff", "outputs_diff", "scenarios_diff", "plans_diff", "assertions_diff"):
        sd = d.get(section, {})
        label = section.replace("_diff", "")
        if sd.get("added") or sd.get("removed") or sd.get("changed"):
            lines.append(f"\n{label}:")
            for k in sd.get("added", []):
                lines.append(f"  + {k}")
            for k in sd.get("removed", []):
                lines.append(f"  - {k}")
            for c in sd.get("changed", []):
                key = c.get("key") or c.get("name", "?")
                ct = c.get("change_type", "")
                ct_str = f" [{ct}]" if ct else ""
                lines.append(f"  ~ {key}{ct_str}: {c.get('a', '')} → {c.get('b', '')}")

    if len(lines) == 1:
        lines.append("No differences found.")

    return "\n".join(lines)
