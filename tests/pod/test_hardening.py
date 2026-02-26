"""Phase 2 hardening tests: deterministic exports, join validation, lookup errors,
SQL schema guard, GC safety, and dry-run behavior."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pytest

from fin123.gc import run_gc
from fin123.project import scaffold_project
from fin123.sync import run_sync
from fin123.workbook import Workbook


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Scaffold a demo project in a temporary directory."""
    project_dir = tmp_path / "test_project"
    scaffold_project(project_dir)
    return project_dir


# ---------------------------------------------------------------------------
# 1) Deterministic table exports
# ---------------------------------------------------------------------------


class TestDeterministicExports:
    """Tables without explicit sort get a deterministic secondary sort at export."""

    def test_unsorted_table_gets_deterministic_sort(self, demo_project: Path) -> None:
        """filtered_prices (no explicit sort) is exported sorted by all columns."""
        wb = Workbook(demo_project)
        result = wb.run()

        # Read the exported parquet
        pq_path = result.run_dir / "outputs" / "filtered_prices.parquet"
        df = pl.read_parquet(pq_path)

        # Manually sort by all columns alphabetically and compare
        sort_cols = sorted(df.columns)
        expected = df.sort(sort_cols)
        assert df.equals(expected), "Unsorted table should be deterministically sorted"

    def test_sorted_table_preserves_plan_order(self, demo_project: Path) -> None:
        """summary_by_category (has sort step) keeps plan's ordering."""
        wb = Workbook(demo_project)
        result = wb.run()

        pq_path = result.run_dir / "outputs" / "summary_by_category.parquet"
        df = pl.read_parquet(pq_path)

        # The plan sorts by total_revenue descending
        assert df["total_revenue"].to_list() == sorted(
            df["total_revenue"].to_list(), reverse=True
        )

    def test_run_meta_records_sorted_exports(self, demo_project: Path) -> None:
        """run_meta.json records which tables were auto-sorted."""
        wb = Workbook(demo_project)
        result = wb.run()

        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        assert "sorted_exports" in meta
        # summary_by_category has explicit sort, so shouldn't appear
        assert "summary_by_category" not in meta["sorted_exports"]
        # filtered_prices lacks explicit sort, so should appear
        assert "filtered_prices" in meta["sorted_exports"]

    def test_run_meta_records_export_row_counts(self, demo_project: Path) -> None:
        """run_meta.json records row counts for each exported table."""
        wb = Workbook(demo_project)
        result = wb.run()

        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        assert "export_row_counts" in meta
        assert "filtered_prices" in meta["export_row_counts"]
        assert meta["export_row_counts"]["filtered_prices"] > 0

    def test_deterministic_sort_is_reproducible(self, demo_project: Path) -> None:
        """Two runs produce identical parquet bytes for unsorted tables."""
        wb1 = Workbook(demo_project)
        r1 = wb1.run()
        wb2 = Workbook(demo_project)
        r2 = wb2.run()

        pq1 = (r1.run_dir / "outputs" / "filtered_prices.parquet").read_bytes()
        pq2 = (r2.run_dir / "outputs" / "filtered_prices.parquet").read_bytes()
        assert pq1 == pq2


# ---------------------------------------------------------------------------
# 2) join_left duplicate detection with sample keys
# ---------------------------------------------------------------------------


class TestJoinLeftValidation:
    """join_left validation includes sample duplicate keys in error messages."""

    def test_duplicate_error_includes_sample_keys(self, tmp_path: Path) -> None:
        """Error message from many_to_one violation includes sample key values."""
        project = _project_with_join(tmp_path, right_has_dupes=True, validate="many_to_one")
        wb = Workbook(project)
        with pytest.raises(ValueError, match="Sample duplicates"):
            wb.run()

    def test_duplicate_error_mentions_key_column(self, tmp_path: Path) -> None:
        """Error message names the key column(s)."""
        project = _project_with_join(tmp_path, right_has_dupes=True, validate="many_to_one")
        wb = Workbook(project)
        with pytest.raises(ValueError, match="key"):
            wb.run()


# ---------------------------------------------------------------------------
# 3) lookup_scalar error diagnostics
# ---------------------------------------------------------------------------


class TestLookupScalarErrors:
    """lookup_scalar produces diagnostic error messages."""

    def test_missing_key_shows_available_keys(self, tmp_path: Path) -> None:
        """Error for missing key lists available keys from the table."""
        project = _minimal_lookup_project(tmp_path, on_missing="error")
        wb = Workbook(project, overrides={"ticker": "NONEXISTENT"})
        with pytest.raises(ValueError, match="Available keys"):
            wb.run()

    def test_missing_key_col_shows_available_columns(self, tmp_path: Path) -> None:
        """Error for invalid key_col lists available columns."""
        project = _minimal_lookup_project(tmp_path, key_col="bad_col")
        wb = Workbook(project)
        with pytest.raises(ValueError, match="Available columns"):
            wb.run()

    def test_missing_value_col_shows_available_columns(self, tmp_path: Path) -> None:
        """Error for invalid value_col lists available columns."""
        project = _minimal_lookup_project(tmp_path, value_col="bad_col")
        wb = Workbook(project)
        with pytest.raises(ValueError, match="Available columns"):
            wb.run()


# ---------------------------------------------------------------------------
# 4) Primary key enforcement
# ---------------------------------------------------------------------------


class TestPrimaryKeyEnforcement:
    """Tables with primary_key declared are validated for uniqueness."""

    def test_duplicate_primary_key_raises(self, tmp_path: Path) -> None:
        """Workbook run fails when a primary_key column has duplicates."""
        project = _project_with_primary_key(tmp_path, has_dupes=True)
        wb = Workbook(project)
        with pytest.raises(ValueError, match="primary_key"):
            wb.run()

    def test_unique_primary_key_passes(self, tmp_path: Path) -> None:
        """Workbook run succeeds when primary_key column is unique."""
        project = _project_with_primary_key(tmp_path, has_dupes=False)
        wb = Workbook(project)
        result = wb.run()
        assert "data" in result.tables


# ---------------------------------------------------------------------------
# 5) SQL schema guard (extra column warnings)
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_db(tmp_path: Path) -> str:
    """Create a temporary SQLite database with test data."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE analyst_estimates "
        "(ticker TEXT, eps REAL, revenue_estimate REAL, pe_ratio REAL)"
    )
    conn.executemany(
        "INSERT INTO analyst_estimates VALUES (?, ?, ?, ?)",
        [
            ("AAPL", 6.75, 420000.0, 28.5),
            ("MSFT", 12.10, 265000.0, 34.2),
        ],
    )
    conn.commit()
    conn.close()
    return f"sqlite:///{db_path}"


class TestSQLSchemaGuard:
    """SQL schema guard: fail on missing columns, warn on extra."""

    def test_extra_columns_produce_warnings(
        self, demo_project: Path, sqlite_db: str
    ) -> None:
        """Extra columns in query result produce warnings, not errors."""
        # expected_columns has only ticker and eps; query returns more
        _write_sync_workbook(
            demo_project,
            sqlite_db,
            expected_columns=["ticker", "eps"],
        )
        with patch.dict(os.environ, {"TEST_DB_URL": sqlite_db}):
            result = run_sync(demo_project, table_name="test_table", force=True)

        assert "test_table" in result["synced"]
        assert len(result["errors"]) == 0
        # Should have a warning about extra columns
        assert len(result["warnings"]) > 0
        assert any("extra columns" in w.lower() for w in result["warnings"])

    def test_missing_columns_produce_errors(
        self, demo_project: Path, sqlite_db: str
    ) -> None:
        """Missing expected columns cause sync to fail."""
        _write_sync_workbook(
            demo_project,
            sqlite_db,
            expected_columns=["ticker", "eps", "nonexistent"],
        )
        with patch.dict(os.environ, {"TEST_DB_URL": sqlite_db}):
            result = run_sync(demo_project, table_name="test_table", force=True)

        assert len(result["errors"]) == 1
        assert "nonexistent" in result["errors"][0]

    def test_sync_provenance_includes_schema(
        self, demo_project: Path, sqlite_db: str
    ) -> None:
        """Sync provenance records column schema for successful syncs."""
        _write_sync_workbook(demo_project, sqlite_db)
        with patch.dict(os.environ, {"TEST_DB_URL": sqlite_db}):
            run_sync(demo_project, table_name="test_table", force=True)

        sync_dirs = sorted((demo_project / "sync_runs").iterdir())
        meta = json.loads((sync_dirs[-1] / "sync_meta.json").read_text())
        table_detail = meta["tables"][0]
        assert "schema" in table_detail
        assert "ticker" in table_detail["schema"]


# ---------------------------------------------------------------------------
# 6) GC safety: never delete most recent, dry-run
# ---------------------------------------------------------------------------


class TestGCSafety:
    """GC safety: most recent always protected, dry-run doesn't delete."""

    def test_gc_never_deletes_most_recent_run(self, demo_project: Path) -> None:
        """Even with max_runs=1, the most recent run survives."""
        (demo_project / "fin123.yaml").write_text("max_runs: 1\n")

        run_dirs = []
        for _ in range(3):
            wb = Workbook(demo_project)
            result = wb.run()
            run_dirs.append(result.run_dir)

        run_gc(demo_project)

        # Most recent run must survive
        assert run_dirs[-1].exists()
        remaining = [d for d in (demo_project / "runs").iterdir() if d.is_dir()]
        assert len(remaining) == 1
        assert remaining[0].name == run_dirs[-1].name

    def test_gc_never_deletes_most_recent_artifact_version(
        self, demo_project: Path
    ) -> None:
        """Most recent artifact version survives even when over limit."""
        (demo_project / "fin123.yaml").write_text(
            "max_runs: 100\nmax_artifact_versions: 1\n"
        )

        from fin123.workflows.runner import run_workflow

        for _ in range(3):
            run_workflow("scenario_sweep", demo_project)

        run_gc(demo_project)

        art_dir = demo_project / "artifacts" / "scenario_sweep_results"
        versions = sorted(d for d in art_dir.iterdir() if d.is_dir())
        assert len(versions) == 1
        assert versions[0].name == "v0003"

    def test_gc_never_deletes_most_recent_sync_run(self, demo_project: Path) -> None:
        """Most recent sync run survives even when over limit."""
        (demo_project / "fin123.yaml").write_text("max_sync_runs: 1\nmax_runs: 100\n")

        sync_dir = demo_project / "sync_runs"
        sync_dir.mkdir(exist_ok=True)
        for i in range(3):
            sd = sync_dir / f"2026010{i}_sync_{i + 1}"
            sd.mkdir()
            (sd / "sync_meta.json").write_text(
                json.dumps({
                    "sync_id": sd.name,
                    "timestamp": f"2026-01-0{i + 1}T00:00:00+00:00",
                    "tables_updated": [],
                    "tables": [],
                    "pinned": False,
                })
            )

        run_gc(demo_project)

        remaining = sorted(d for d in sync_dir.iterdir() if d.is_dir())
        assert len(remaining) == 1
        # Most recent (20260102_sync_3) should survive
        assert remaining[0].name == "20260102_sync_3"

    def test_gc_dry_run_does_not_delete(self, demo_project: Path) -> None:
        """GC dry-run reports what would be deleted but doesn't remove anything."""
        (demo_project / "fin123.yaml").write_text("max_runs: 1\n")

        for _ in range(3):
            wb = Workbook(demo_project)
            wb.run()

        runs_before = list((demo_project / "runs").iterdir())
        assert len(runs_before) == 3

        summary = run_gc(demo_project, dry_run=True)

        # Nothing actually deleted
        runs_after = list((demo_project / "runs").iterdir())
        assert len(runs_after) == 3

        # But summary reports what would be deleted
        assert summary["runs_deleted"] == 2
        assert summary["dry_run"] is True

    def test_gc_dry_run_reports_bytes(self, demo_project: Path) -> None:
        """GC dry-run reports bytes that would be freed."""
        (demo_project / "fin123.yaml").write_text("max_runs: 1\n")

        for _ in range(3):
            wb = Workbook(demo_project)
            wb.run()

        summary = run_gc(demo_project, dry_run=True)
        assert summary["bytes_freed"] > 0


# ---------------------------------------------------------------------------
# 7) CLI --dry-run flag
# ---------------------------------------------------------------------------


class TestCLIDryRun:
    """Test CLI gc --dry-run flag."""

    def test_cli_gc_dry_run(self, demo_project: Path) -> None:
        """CLI gc --dry-run outputs dry-run label."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["gc", "--dry-run", str(demo_project)])
        assert result.exit_code == 0
        assert "dry-run" in result.output

    def test_cli_gc_without_dry_run(self, demo_project: Path) -> None:
        """CLI gc without --dry-run outputs complete label."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["gc", str(demo_project)])
        assert result.exit_code == 0
        assert "GC complete" in result.output

    def test_cli_gc_shows_sync_runs_deleted(self, demo_project: Path) -> None:
        """CLI gc output includes sync runs deleted count."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["gc", str(demo_project)])
        assert result.exit_code == 0
        assert "Sync runs deleted" in result.output


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _write_sync_workbook(
    project_dir: Path,
    conn_string: str,
    refresh: str = "manual",
    expected_columns: list[str] | None = None,
) -> None:
    """Write a workbook.yaml with a SQL table for sync testing."""
    cols_str = ""
    if expected_columns:
        cols_str = f"    expected_columns: {expected_columns}\n"

    workbook = f"""\
version: 1
connections:
  test_db:
    driver: sqlite
    env: TEST_DB_URL
params:
  x: 1
tables:
  test_table:
    source: sql
    connection: test_db
    query: "SELECT * FROM analyst_estimates ORDER BY ticker"
    cache: inputs/test_sync.parquet
    refresh: {refresh}
{cols_str}
outputs:
  - name: x
    type: scalar
    value: 1
"""
    (project_dir / "workbook.yaml").write_text(workbook)


def _minimal_lookup_project(
    tmp_path: Path,
    on_missing: str = "error",
    on_duplicate: str = "error",
    key_col: str = "ticker",
    value_col: str = "eps",
) -> Path:
    """Create a minimal project with a lookup_scalar output."""
    project = tmp_path / "lookup_project"
    project.mkdir(exist_ok=True)

    inputs = project / "inputs"
    inputs.mkdir(exist_ok=True)
    pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "eps": [6.75, 12.10, 7.50],
    }).write_parquet(inputs / "data.parquet")

    workbook = f"""\
version: 1
params:
  ticker: AAPL
tables:
  data:
    source: inputs/data.parquet
    format: parquet
outputs:
  - name: lookup_result
    type: scalar
    func: lookup_scalar
    args:
      table_name: data
      key_col: "{key_col}"
      value_col: "{value_col}"
      key_value: "$ticker"
      on_missing: "{on_missing}"
      on_duplicate: "{on_duplicate}"
"""
    (project / "workbook.yaml").write_text(workbook)
    return project


def _project_with_join(
    tmp_path: Path,
    right_has_dupes: bool = False,
    validate: str = "many_to_one",
) -> Path:
    """Create a project with a join_left plan."""
    project = tmp_path / "join_project"
    project.mkdir(exist_ok=True)

    inputs = project / "inputs"
    inputs.mkdir(exist_ok=True)

    pl.DataFrame({
        "id": [1, 2, 3],
        "key": ["A", "B", "C"],
        "value": [10, 20, 30],
    }).write_parquet(inputs / "left.parquet")

    right_rows: dict[str, list[Any]] = {
        "key": ["A", "B", "C"],
        "extra": [100, 200, 300],
    }
    if right_has_dupes:
        right_rows["key"].append("A")
        right_rows["extra"].append(150)
    pl.DataFrame(right_rows).write_parquet(inputs / "right.parquet")

    workbook = f"""\
version: 1
params: {{}}
tables:
  left_t:
    source: inputs/left.parquet
    format: parquet
  right_t:
    source: inputs/right.parquet
    format: parquet
plans:
  - name: joined
    source: left_t
    steps:
      - func: join_left
        right: right_t
        "on": key
        validate: {validate}
outputs:
  - name: joined
    type: table
"""
    (project / "workbook.yaml").write_text(workbook)
    return project


def _project_with_primary_key(tmp_path: Path, has_dupes: bool) -> Path:
    """Create a project with a table that declares a primary_key."""
    project = tmp_path / "pk_project"
    project.mkdir(exist_ok=True)

    inputs = project / "inputs"
    inputs.mkdir(exist_ok=True)

    rows: dict[str, list[Any]] = {
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "eps": [6.75, 12.10, 7.50],
    }
    if has_dupes:
        rows["ticker"].append("AAPL")
        rows["eps"].append(7.00)
    pl.DataFrame(rows).write_parquet(inputs / "data.parquet")

    workbook = """\
version: 1
params: {}
tables:
  data:
    source: inputs/data.parquet
    format: parquet
    primary_key: ticker
outputs:
  - name: data
    type: table
"""
    (project / "workbook.yaml").write_text(workbook)
    return project
