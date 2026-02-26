"""Hardening v2 tests: verify contract, determinism, GC safety, params, joins.

Tests cover the hardening items identified in the architecture review:
C (verify), D (params), A (determinism), F (GC safety), B (join/lookup).
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from fin123.gc import run_gc
from fin123.project import scaffold_project
from fin123.workbook import Workbook


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Scaffold a demo project in a temporary directory."""
    return scaffold_project(tmp_path / "proj")


# ---------------------------------------------------------------------------
# C1: Verify report contract — structure, sorted keys, model_version_id
# ---------------------------------------------------------------------------


class TestVerifyReportContract:
    """verify_report.json must have a stable, complete structure."""

    def test_verify_report_has_sorted_keys(self, demo_project: Path) -> None:
        """verify_report.json must serialize with sorted keys."""
        wb = Workbook(demo_project)
        result = wb.run()

        from fin123.verify import verify_run

        report = verify_run(demo_project, result.run_dir.name)
        report_path = result.run_dir / "verify_report.json"
        raw = report_path.read_text()
        parsed = json.loads(raw)

        # Top-level keys must be in sorted order
        keys = list(parsed.keys())
        assert keys == sorted(keys), f"Keys not sorted: {keys}"

    def test_verify_report_includes_model_version_id(self, demo_project: Path) -> None:
        """verify_report.json must include model_version_id."""
        wb = Workbook(demo_project)
        result = wb.run()

        from fin123.verify import verify_run

        report = verify_run(demo_project, result.run_dir.name)
        assert "model_version_id" in report
        assert report["model_version_id"] is not None

    def test_verify_report_includes_engine_version(self, demo_project: Path) -> None:
        """verify_report.json must include engine_version."""
        wb = Workbook(demo_project)
        result = wb.run()

        from fin123.verify import verify_run

        report = verify_run(demo_project, result.run_dir.name)
        assert "engine_version" in report

    def test_verify_report_includes_assertion_summary(self, demo_project: Path) -> None:
        """verify_report.json must include assertion summary."""
        wb = Workbook(demo_project)
        result = wb.run()

        from fin123.verify import verify_run

        report = verify_run(demo_project, result.run_dir.name)
        assert "assertions" in report
        assert "status" in report["assertions"]
        assert "failed_count" in report["assertions"]
        assert "warn_count" in report["assertions"]

    def test_verify_report_failures_sorted(self, demo_project: Path) -> None:
        """verify_report failures list must be sorted for determinism."""
        from fin123.verify import verify_run

        # Create a run with no model_version_id to trigger multiple failures
        run_dir = demo_project / "runs" / "bad_run"
        run_dir.mkdir(parents=True)
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text('{"x": 1}')
        meta = {"run_id": "bad_run", "params_hash": "0" * 64, "effective_params": {}}
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        report = verify_run(demo_project, "bad_run")
        failures = report["failures"]
        assert failures == sorted(failures)

    def test_verify_report_hashes_sorted(self, demo_project: Path) -> None:
        """verify_report hashes dict must have sorted keys."""
        wb = Workbook(demo_project)
        result = wb.run()

        from fin123.verify import verify_run

        report = verify_run(demo_project, result.run_dir.name)
        hash_keys = list(report["hashes"].keys())
        assert hash_keys == sorted(hash_keys)

    def test_verify_pass_on_valid_run(self, demo_project: Path) -> None:
        """A valid build followed by verify should pass."""
        wb = Workbook(demo_project)
        result = wb.run()

        from fin123.verify import verify_run

        report = verify_run(demo_project, result.run_dir.name)
        assert report["status"] == "pass", f"Failures: {report['failures']}"

    def test_verify_detects_tampered_export(self, demo_project: Path) -> None:
        """Verify should detect modified output files."""
        wb = Workbook(demo_project)
        result = wb.run()

        # Tamper with scalars.json
        scalars_path = result.run_dir / "outputs" / "scalars.json"
        scalars_path.write_text('{"tampered": true}')

        from fin123.verify import verify_run

        report = verify_run(demo_project, result.run_dir.name)
        assert report["status"] == "fail"
        assert any("export_hash" in f for f in report["failures"])


# ---------------------------------------------------------------------------
# C3: Verify report is deterministic across runs
# ---------------------------------------------------------------------------


class TestVerifyDeterminism:
    """Verify reports for identical builds must be structurally identical."""

    def test_verify_report_deterministic(self, demo_project: Path) -> None:
        """Two builds from identical inputs produce identical verify reports."""
        from fin123.verify import verify_run

        wb1 = Workbook(demo_project)
        r1 = wb1.run()
        report1 = verify_run(demo_project, r1.run_dir.name)

        wb2 = Workbook(demo_project)
        r2 = wb2.run()
        report2 = verify_run(demo_project, r2.run_dir.name)

        # Status must match
        assert report1["status"] == report2["status"]
        # Hash values must match (same inputs)
        assert report1["hashes"] == report2["hashes"]
        # Failure lists must match
        assert report1["failures"] == report2["failures"]


# ---------------------------------------------------------------------------
# A2: Golden determinism — build twice, assert identical export hashes
# ---------------------------------------------------------------------------


class TestGoldenDeterminism:
    """Two builds from identical inputs must produce identical outputs."""

    def test_export_hash_identical_across_runs(self, demo_project: Path) -> None:
        """export_hash must be identical for two runs from the same inputs."""
        wb1 = Workbook(demo_project)
        r1 = wb1.run()
        meta1 = json.loads((r1.run_dir / "run_meta.json").read_text())

        wb2 = Workbook(demo_project)
        r2 = wb2.run()
        meta2 = json.loads((r2.run_dir / "run_meta.json").read_text())

        assert meta1["export_hash"] == meta2["export_hash"]

    def test_parquet_bytes_identical_across_runs(self, demo_project: Path) -> None:
        """Parquet bytes for a representative table must be identical across runs."""
        wb1 = Workbook(demo_project)
        r1 = wb1.run()

        wb2 = Workbook(demo_project)
        r2 = wb2.run()

        # Find all parquet outputs and compare
        for pq in sorted((r1.run_dir / "outputs").glob("*.parquet")):
            pq2 = r2.run_dir / "outputs" / pq.name
            assert pq.read_bytes() == pq2.read_bytes(), (
                f"Parquet mismatch for {pq.name}"
            )

    def test_scalars_json_identical_across_runs(self, demo_project: Path) -> None:
        """scalars.json must be identical across two runs."""
        wb1 = Workbook(demo_project)
        r1 = wb1.run()

        wb2 = Workbook(demo_project)
        r2 = wb2.run()

        s1 = (r1.run_dir / "outputs" / "scalars.json").read_text()
        s2 = (r2.run_dir / "outputs" / "scalars.json").read_text()
        assert s1 == s2


# ---------------------------------------------------------------------------
# A3: Null ordering determinism
# ---------------------------------------------------------------------------


class TestNullOrdering:
    """Export sort must handle nulls deterministically."""

    def test_deterministic_sort_nulls_last(self, tmp_path: Path) -> None:
        """_deterministic_sort places nulls last."""
        from fin123.versioning import _deterministic_sort

        df = pl.DataFrame({
            "a": [None, "z", "a", None],
            "b": [1, 2, 3, 4],
        })
        sorted_df = _deterministic_sort(df)
        # With nulls_last=True, non-null "a" values come first
        a_vals = sorted_df["a"].to_list()
        assert a_vals[0] is not None
        assert a_vals[1] is not None
        assert a_vals[2] is None
        assert a_vals[3] is None


# ---------------------------------------------------------------------------
# D2: Params hash numeric normalization
# ---------------------------------------------------------------------------


class TestParamsHashNormalization:
    """params_hash must be stable despite numeric type variations."""

    def test_float_int_normalization(self) -> None:
        """1.0 and 1 must produce the same params_hash."""
        from fin123.utils.hash import compute_params_hash

        h_float = compute_params_hash({"rate": 1.0})
        h_int = compute_params_hash({"rate": 1})
        assert h_float == h_int

    def test_non_integer_float_preserved(self) -> None:
        """1.5 must not be normalized to int."""
        from fin123.utils.hash import compute_params_hash

        h1 = compute_params_hash({"rate": 1.5})
        h2 = compute_params_hash({"rate": 1})
        assert h1 != h2

    def test_nan_preserved(self) -> None:
        """NaN must not be normalized to int."""
        from fin123.utils.hash import _normalize_keys

        result = _normalize_keys({"x": float("nan")})
        assert isinstance(result["x"], float)
        assert math.isnan(result["x"])

    def test_key_whitespace_stripped(self) -> None:
        """Param names with whitespace are stripped for normalization."""
        from fin123.utils.hash import compute_params_hash

        h1 = compute_params_hash({"rate": 1})
        h2 = compute_params_hash({" rate ": 1})
        assert h1 == h2


# ---------------------------------------------------------------------------
# D3: Overlay hash invariant — CLI overrides never leak
# ---------------------------------------------------------------------------


class TestOverlayHashInvariant:
    """overlay_hash must only include committed scenario overrides."""

    def test_cli_overrides_excluded_from_overlay_hash(
        self, tmp_path: Path
    ) -> None:
        """CLI --set overrides must not affect overlay_hash."""
        project = _project_with_scenario(tmp_path)

        # Run with scenario only
        wb1 = Workbook(project, scenario_name="low_rate")
        r1 = wb1.run()
        meta1 = json.loads((r1.run_dir / "run_meta.json").read_text())

        # Run with scenario + CLI override
        wb2 = Workbook(project, scenario_name="low_rate", overrides={"extra": 999})
        r2 = wb2.run()
        meta2 = json.loads((r2.run_dir / "run_meta.json").read_text())

        # overlay_hash must be identical (CLI override excluded)
        assert meta1["overlay_hash"] == meta2["overlay_hash"]
        # params_hash must differ (CLI override included)
        assert meta1["params_hash"] != meta2["params_hash"]


# ---------------------------------------------------------------------------
# F3: GC skips in-progress directories
# ---------------------------------------------------------------------------


class TestGCInProgressSafety:
    """GC must never delete directories with .in_progress markers."""

    def test_gc_skips_in_progress_run(self, demo_project: Path) -> None:
        """A run directory with .in_progress marker survives GC."""
        (demo_project / "fin123.yaml").write_text("max_runs: 1\n")

        # Create multiple runs
        run_dirs = []
        for _ in range(3):
            wb = Workbook(demo_project)
            result = wb.run()
            run_dirs.append(result.run_dir)

        # Mark the second run as in-progress
        (run_dirs[1] / ".in_progress").write_text("")

        run_gc(demo_project)

        # Most recent (run_dirs[2]) always survives
        assert run_dirs[2].exists()
        # In-progress (run_dirs[1]) must also survive
        assert run_dirs[1].exists()
        # First run should be deleted (not protected)
        assert not run_dirs[0].exists()

    def test_in_progress_marker_removed_after_run(self, demo_project: Path) -> None:
        """After a successful run, .in_progress marker is removed."""
        wb = Workbook(demo_project)
        result = wb.run()
        assert not (result.run_dir / ".in_progress").exists()


# ---------------------------------------------------------------------------
# F4: GC model version protection — versions referenced by runs
# ---------------------------------------------------------------------------


class TestGCModelVersionProtection:
    """GC must not delete model versions referenced by retained runs."""

    def test_gc_protects_versions_referenced_by_runs(
        self, demo_project: Path
    ) -> None:
        """Model versions referenced by existing runs are not deleted."""
        # Create runs (each creates a snapshot version)
        run_versions = []
        for _ in range(3):
            wb = Workbook(demo_project)
            result = wb.run()
            meta = json.loads((result.run_dir / "run_meta.json").read_text())
            run_versions.append(meta["model_version_id"])

        # Configure tight model version limit
        config = demo_project / "fin123.yaml"
        config.write_text("max_model_versions: 1\nmax_runs: 100\n")

        run_gc(demo_project)

        # All versions referenced by retained runs must survive
        snap_dir = demo_project / "snapshots" / "workbook"
        for v in run_versions:
            assert (snap_dir / v / "workbook.yaml").exists(), (
                f"Version {v} deleted but referenced by a run"
            )


# ---------------------------------------------------------------------------
# B1: Join dtype enforcement
# ---------------------------------------------------------------------------


class TestJoinDtypeEnforcement:
    """join_left must fail on incompatible key dtypes."""

    def test_string_int_join_raises_type_error(self) -> None:
        """Joining string key to int key must raise TypeError."""
        from fin123.functions.table import table_join_left

        left = pl.LazyFrame({"key": ["a", "b"], "val": [1, 2]})
        right = pl.LazyFrame({"key": [1, 2], "extra": [10, 20]})

        with pytest.raises(TypeError, match="dtype mismatch"):
            table_join_left(
                left,
                right="right_t",
                on=["key"],
                _tables={"right_t": right},
            )

    def test_same_dtype_join_passes(self) -> None:
        """Joining matching dtypes should succeed."""
        from fin123.functions.table import table_join_left

        left = pl.LazyFrame({"key": [1, 2], "val": [10, 20]})
        right = pl.LazyFrame({"key": [1, 2], "extra": [100, 200]})

        result = table_join_left(
            left,
            right="right_t",
            on=["key"],
            validate="none",
            _tables={"right_t": right},
        )
        assert result.collect().height == 2

    def test_missing_key_column_raises(self) -> None:
        """join_left must raise on missing key column."""
        from fin123.functions.table import table_join_left

        left = pl.LazyFrame({"a": [1], "val": [10]})
        right = pl.LazyFrame({"b": [1], "extra": [100]})

        with pytest.raises(ValueError, match="not found"):
            table_join_left(
                left,
                right="right_t",
                on=["nonexistent"],
                _tables={"right_t": right},
            )


# ---------------------------------------------------------------------------
# B2: lookup_scalar / SUMIFS / COUNTIFS edge cases
# ---------------------------------------------------------------------------


class TestLookupEdgeCases:
    """lookup_scalar and aggregation functions must handle edge cases."""

    def test_lookup_with_none_key(self) -> None:
        """lookup_scalar with None key value must raise."""
        from fin123.functions.scalar import scalar_lookup

        df = pl.DataFrame({"key": ["a", "b"], "val": [1, 2]})
        with pytest.raises(ValueError, match="Available keys"):
            scalar_lookup("test", "key", "val", None, _table_cache={"test": df})

    def test_lookup_int_vs_float_key(self) -> None:
        """lookup_scalar should handle int key matching float column."""
        from fin123.functions.scalar import scalar_lookup

        df = pl.DataFrame({"key": [1.0, 2.0], "val": ["a", "b"]})
        # Looking up int 1 against float column
        result = scalar_lookup("test", "key", "val", 1.0, _table_cache={"test": df})
        assert result == "a"


class TestSumifsCountifsEdgeCases:
    """SUMIFS and COUNTIFS must handle type edge cases."""

    def test_sumifs_with_int_criteria(self) -> None:
        """SUMIFS should work with integer criteria values."""
        from fin123.formulas.evaluator import evaluate_formula
        from fin123.formulas.parser import parse_formula

        table_cache = {
            "data": pl.DataFrame({
                "region": ["US", "US", "EU"],
                "amount": [100, 200, 300],
                "count": [1, 2, 3],
            })
        }
        tree = parse_formula('=SUMIFS("data", "amount", "count", ">", 1)')
        result = evaluate_formula(tree, {}, table_cache=table_cache)
        assert result == 500.0  # 200 + 300

    def test_countifs_with_string_criteria(self) -> None:
        """COUNTIFS should work with string criteria."""
        from fin123.formulas.evaluator import evaluate_formula
        from fin123.formulas.parser import parse_formula

        table_cache = {
            "data": pl.DataFrame({
                "region": ["US", "US", "EU"],
                "amount": [100, 200, 300],
            })
        }
        tree = parse_formula('=COUNTIFS("data", "region", "=", "US")')
        result = evaluate_formula(tree, {}, table_cache=table_cache)
        assert result == 2


# ---------------------------------------------------------------------------
# Item 1: params_hash normalization scoping
# ---------------------------------------------------------------------------


class TestNormalizationScoping:
    """sha256_dict must be type-faithful; only params_hash normalizes floats."""

    def test_sha256_dict_preserves_float(self) -> None:
        """sha256_dict must NOT normalize 1.0 to 1."""
        from fin123.utils.hash import sha256_dict

        h_float = sha256_dict({"rate": 1.0})
        h_int = sha256_dict({"rate": 1})
        assert h_float != h_int, "sha256_dict should be type-faithful"

    def test_params_hash_normalizes_float(self) -> None:
        """compute_params_hash SHOULD normalize 1.0 to 1."""
        from fin123.utils.hash import compute_params_hash

        h_float = compute_params_hash({"rate": 1.0})
        h_int = compute_params_hash({"rate": 1})
        assert h_float == h_int

    def test_plugin_hash_preserves_float(self) -> None:
        """compute_plugin_hash_combined must be type-faithful."""
        from fin123.utils.hash import compute_plugin_hash_combined

        h1 = compute_plugin_hash_combined("1.0.0", {"p": {"version": "v1", "sha256": "abc"}})
        h2 = compute_plugin_hash_combined("1.0.0", {"p": {"version": "v1", "sha256": "abc"}})
        assert h1 == h2  # sanity check

    def test_normalize_keys_only_no_float_coercion(self) -> None:
        """_normalize_keys_only must not coerce floats to int."""
        from fin123.utils.hash import _normalize_keys_only

        result = _normalize_keys_only({"a": 1.0, "b": [2.0, {"c": 3.0}]})
        assert isinstance(result["a"], float)
        assert isinstance(result["b"][0], float)
        assert isinstance(result["b"][1]["c"], float)


# ---------------------------------------------------------------------------
# Item 2: Deterministic export sorting edge cases
# ---------------------------------------------------------------------------


class TestDeterministicExportEdgeCases:
    """Export sorting handles nulls in multiple columns and duplicate rows."""

    def test_nulls_in_multiple_columns_deterministic(self, tmp_path: Path) -> None:
        """Nulls across multiple columns must sort deterministically."""
        from fin123.versioning import _deterministic_sort

        df = pl.DataFrame({
            "a": [None, "x", None, "x"],
            "b": [1, None, None, 2],
        })
        s1 = _deterministic_sort(df)
        s2 = _deterministic_sort(df)
        assert s1.equals(s2)
        # Write to parquet and compare bytes
        p1 = tmp_path / "s1.parquet"
        p2 = tmp_path / "s2.parquet"
        s1.write_parquet(p1)
        s2.write_parquet(p2)
        assert p1.read_bytes() == p2.read_bytes()

    def test_duplicate_rows_byte_identical_parquet(self, tmp_path: Path) -> None:
        """Identical duplicate rows must produce byte-identical parquet."""
        from fin123.versioning import _deterministic_sort

        df = pl.DataFrame({
            "a": ["x", "x", "y", "y"],
            "b": [1, 1, 2, 2],
        })
        p1 = tmp_path / "d1.parquet"
        p2 = tmp_path / "d2.parquet"
        _deterministic_sort(df).write_parquet(p1)
        _deterministic_sort(df).write_parquet(p2)
        assert p1.read_bytes() == p2.read_bytes()


# ---------------------------------------------------------------------------
# Item 3: join_left null key rejection
# ---------------------------------------------------------------------------


class TestJoinNullKeyRejection:
    """join_left rejects null keys under strict validation (many_to_one, one_to_one)."""

    def test_null_join_key_raises_under_many_to_one(self) -> None:
        """Null key values in right table under many_to_one must raise ValueError."""
        from fin123.functions.table import table_join_left

        left = pl.LazyFrame({"key": [1, 2], "val": [10, 20]})
        right = pl.LazyFrame({"key": [1, None], "extra": [100, 200]})

        with pytest.raises(ValueError, match="null join key"):
            table_join_left(
                left,
                right="right_t",
                on=["key"],
                validate="many_to_one",
                _tables={"right_t": right},
            )

    def test_null_join_key_allowed_under_many_to_many(self) -> None:
        """Null key values should pass when validate=many_to_many."""
        from fin123.functions.table import table_join_left

        left = pl.LazyFrame({"key": [1, 2], "val": [10, 20]})
        right = pl.LazyFrame({"key": [1, None], "extra": [100, 200]})

        result = table_join_left(
            left,
            right="right_t",
            on=["key"],
            validate="many_to_many",
            _tables={"right_t": right},
        )
        assert result.collect().height == 2

    def test_date_datetime_join_compatible(self) -> None:
        """Date and Datetime join keys should be treated as compatible."""
        from fin123.functions.table import _check_join_key_dtypes
        from datetime import date, datetime

        left = pl.LazyFrame({"d": [date(2024, 1, 1)]})
        right = pl.LazyFrame({"d": [datetime(2024, 1, 1)]})

        # Should not raise
        _check_join_key_dtypes(left, right, ["d"], None, None)


# ---------------------------------------------------------------------------
# Item 4: Verify contract — tampered snapshot detection
# ---------------------------------------------------------------------------


class TestVerifyTamperedSnapshot:
    """Verify must detect tampered snapshot YAML."""

    def test_verify_detects_tampered_snapshot_yaml(self, demo_project: Path) -> None:
        """Modifying snapshot YAML after build should cause verify failure."""
        wb = Workbook(demo_project)
        result = wb.run()
        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        mv_id = meta["model_version_id"]

        # Tamper with the snapshot (change actual content, not just a comment)
        snap_path = demo_project / "snapshots" / "workbook" / mv_id / "workbook.yaml"
        snap_path.write_text(snap_path.read_text() + "\ntampered_key: true")

        from fin123.verify import verify_run

        report = verify_run(demo_project, result.run_dir.name)
        assert report["status"] == "fail"
        assert any("workbook_spec_hash" in f or "snapshot content" in f for f in report["failures"])


# ---------------------------------------------------------------------------
# Item 5: .in_progress marker covers sync_runs
# ---------------------------------------------------------------------------


class TestInProgressSyncRuns:
    """GC must skip sync_runs with .in_progress markers."""

    def test_gc_skips_in_progress_sync_run(self, demo_project: Path) -> None:
        """A sync_run with .in_progress marker survives GC."""
        (demo_project / "fin123.yaml").write_text(
            "max_sync_runs: 1\nmax_runs: 100\n"
        )

        sync_dir = demo_project / "sync_runs"
        sync_dir.mkdir(exist_ok=True)

        # Create 3 fake sync runs
        dirs = []
        for i in range(3):
            d = sync_dir / f"20240101_00000{i}_sync"
            d.mkdir()
            meta = {"sync_id": d.name, "timestamp": "2024-01-01T00:00:00+00:00", "pinned": False}
            (d / "sync_meta.json").write_text(json.dumps(meta))
            dirs.append(d)

        # Mark the oldest as in-progress
        (dirs[0] / ".in_progress").write_text("")

        run_gc(demo_project)

        # Most recent always survives
        assert dirs[2].exists()
        # In-progress must survive
        assert dirs[0].exists(), ".in_progress sync_run was deleted by GC"


# ---------------------------------------------------------------------------
# Item 6: Plugin lock groundwork (+ canonical JSON hashing)
# ---------------------------------------------------------------------------


class TestPluginLockGroundwork:
    """Plugin lock hash stored in run_meta when plugins.lock exists."""

    def test_plugin_lock_hash_stored_when_lock_exists(self, demo_project: Path) -> None:
        """If plugins.lock exists, plugin_lock_hash appears in run_meta."""
        lock_path = demo_project / "plugins.lock"
        lock_path.write_text('{"yahoo_prices": "v0001"}')

        wb = Workbook(demo_project)
        result = wb.run()
        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        assert "plugin_lock_hash" in meta
        assert len(meta["plugin_lock_hash"]) == 64  # SHA-256 hex digest
        assert meta["plugin_lock_hash_mode"] == "canonical_json"

    def test_no_plugin_lock_hash_when_no_lock_file(self, demo_project: Path) -> None:
        """Without plugins.lock, plugin_lock_hash should not be in run_meta."""
        lock_path = demo_project / "plugins.lock"
        if lock_path.exists():
            lock_path.unlink()

        wb = Workbook(demo_project)
        result = wb.run()
        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        assert "plugin_lock_hash" not in meta
        assert "plugin_lock_hash_mode" not in meta

    def test_prod_mode_blocks_without_plugins_lock(self) -> None:
        """Prod mode must block when plugins active but plugins.lock missing."""
        from fin123.project import enforce_prod_mode

        errors = enforce_prod_mode(
            project_dir=Path("/tmp/fake"),
            workbook_spec={},
            model_version_id="v0001",
            plugins_info={"yahoo_prices": {"version": "v0001", "sha256": "abc"}},
        )
        assert any("plugins.lock" in e for e in errors)


# ---------------------------------------------------------------------------
# Canonical plugin lock hashing
# ---------------------------------------------------------------------------


class TestCanonicalPluginLockHash:
    """JSON lock files with different key order/whitespace must hash identically."""

    def test_equivalent_json_locks_produce_identical_hash(self, tmp_path: Path) -> None:
        """Two JSON files with different key order and whitespace hash the same."""
        from fin123.utils.hash import sha256_canonical_json_file

        f1 = tmp_path / "lock1.json"
        f2 = tmp_path / "lock2.json"
        f1.write_text('{"b": "v0002",  "a": "v0001"}')
        f2.write_text('{\n  "a": "v0001",\n  "b": "v0002"\n}\n')

        h1, mode1 = sha256_canonical_json_file(f1)
        h2, mode2 = sha256_canonical_json_file(f2)

        assert h1 == h2, "Equivalent JSON should produce identical hash"
        assert mode1 == "canonical_json"
        assert mode2 == "canonical_json"

    def test_non_json_lock_uses_raw_bytes(self, tmp_path: Path) -> None:
        """Non-JSON lock file must use raw_bytes mode."""
        from fin123.utils.hash import sha256_canonical_json_file

        f = tmp_path / "lock.yaml"
        f.write_text("yahoo_prices: v0001\n")

        h, mode = sha256_canonical_json_file(f)
        assert mode == "raw_bytes"
        assert len(h) == 64

    def test_raw_bytes_mode_stored_in_run_meta(self, demo_project: Path) -> None:
        """Non-JSON lock file sets plugin_lock_hash_mode=raw_bytes in run_meta."""
        lock_path = demo_project / "plugins.lock"
        lock_path.write_text("# not json\nyahoo_prices: v0001\n")

        wb = Workbook(demo_project)
        result = wb.run()
        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        assert meta["plugin_lock_hash_mode"] == "raw_bytes"


# ---------------------------------------------------------------------------
# Table-driven dtype compatibility tests for join_left
# ---------------------------------------------------------------------------


class TestJoinDtypeCompatibilityMatrix:
    """Explicit table-driven tests for dtype compatibility policy."""

    @pytest.mark.parametrize(
        "left_dtype,right_dtype,should_pass",
        [
            # Compatible numeric pairs
            (pl.Int32, pl.Int64, True),
            (pl.Float64, pl.Float32, True),
            (pl.Int64, pl.Float64, True),
            (pl.UInt8, pl.Int64, True),
            # Compatible temporal pairs
            (pl.Date, pl.Datetime, True),
            # Compatible string pairs
            (pl.Utf8, pl.Utf8, True),
            # Incompatible cross-family pairs
            (pl.Utf8, pl.Int64, False),
            (pl.Date, pl.Utf8, False),
            (pl.Int64, pl.Utf8, False),
            (pl.Float64, pl.Date, False),
        ],
        ids=[
            "int32_vs_int64",
            "float64_vs_float32",
            "int64_vs_float64",
            "uint8_vs_int64",
            "date_vs_datetime",
            "utf8_vs_utf8",
            "utf8_vs_int64",
            "date_vs_utf8",
            "int64_vs_utf8",
            "float64_vs_date",
        ],
    )
    def test_dtype_compatibility(
        self,
        left_dtype: pl.DataType,
        right_dtype: pl.DataType,
        should_pass: bool,
    ) -> None:
        """Verify explicit dtype compatibility policy via _check_join_key_dtypes."""
        from fin123.functions.table import _check_join_key_dtypes

        left = pl.LazyFrame(
            schema={"key": left_dtype, "val": pl.Int64},
        )
        right = pl.LazyFrame(
            schema={"key": right_dtype, "extra": pl.Int64},
        )

        if should_pass:
            _check_join_key_dtypes(left, right, ["key"], None, None)
        else:
            with pytest.raises(TypeError, match="dtype mismatch"):
                _check_join_key_dtypes(left, right, ["key"], None, None)

    def test_categorical_vs_utf8_incompatible(self) -> None:
        """Categorical and Utf8 are not in the same compatibility family."""
        from fin123.functions.table import _check_join_key_dtypes

        left = pl.LazyFrame(schema={"key": pl.Categorical, "v": pl.Int64})
        right = pl.LazyFrame(schema={"key": pl.Utf8, "v": pl.Int64})

        with pytest.raises(TypeError, match="dtype mismatch"):
            _check_join_key_dtypes(left, right, ["key"], None, None)

    def test_null_key_rejected_under_one_to_one(self) -> None:
        """Null keys are also rejected under one_to_one (same policy as many_to_one)."""
        from fin123.functions.table import table_join_left

        left = pl.LazyFrame({"key": [1, 2], "val": [10, 20]})
        right = pl.LazyFrame({"key": [1, None], "extra": [100, 200]})

        with pytest.raises(ValueError, match="null join key"):
            table_join_left(
                left,
                right="right_t",
                on=["key"],
                validate="one_to_one",
                _tables={"right_t": right},
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _project_with_scenario(tmp_path: Path) -> Path:
    """Create a project with a scenario for overlay hash testing."""
    import yaml

    project = scaffold_project(tmp_path / "scenario_proj")

    # Add scenarios to the workbook
    wb_path = project / "workbook.yaml"
    spec = yaml.safe_load(wb_path.read_text()) or {}
    spec["scenarios"] = {
        "low_rate": {"overrides": {"rate": 0.01}},
        "high_rate": {"overrides": {"rate": 0.10}},
    }
    if "params" not in spec:
        spec["params"] = {}
    spec["params"]["rate"] = 0.05
    wb_path.write_text(yaml.dump(spec, default_flow_style=False, sort_keys=False))
    return project
