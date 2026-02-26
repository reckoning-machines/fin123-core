"""Phase 2 tests: SQL sync, lookup semantics, join validation, GC sync_runs."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pytest

from fin123.gc import run_gc
from fin123.project import scaffold_project
from fin123.sync import execute_sql, run_sync
from fin123.workbook import Workbook


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Scaffold a demo project in a temporary directory."""
    project_dir = tmp_path / "test_project"
    scaffold_project(project_dir)
    return project_dir


@pytest.fixture
def sqlite_db(tmp_path: Path) -> str:
    """Create a temporary SQLite database with test data.

    Returns:
        SQLAlchemy connection string for the database.
    """
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
            ("GOOGL", 7.50, 380000.0, 22.8),
        ],
    )
    conn.commit()
    conn.close()
    return f"sqlite:///{db_path}"


class TestSQLSync:
    """Test that fin123 sync writes parquet cache and provenance."""

    def test_execute_sql_with_sqlite(self, sqlite_db: str) -> None:
        """execute_sql returns a Polars DataFrame from a SQLite query."""
        df = execute_sql(sqlite_db, "SELECT * FROM analyst_estimates ORDER BY ticker")
        assert len(df) == 3
        assert set(df.columns) == {"ticker", "eps", "revenue_estimate", "pe_ratio"}
        assert df["ticker"].to_list() == ["AAPL", "GOOGL", "MSFT"]

    def test_sync_writes_parquet_and_provenance(
        self, demo_project: Path, sqlite_db: str
    ) -> None:
        """sync writes parquet cache file and sync_meta.json."""
        # Patch the workbook to use sqlite connection
        _write_sync_workbook(demo_project, sqlite_db)

        with patch.dict(os.environ, {"TEST_DB_URL": sqlite_db}):
            result = run_sync(demo_project, table_name="test_table", force=True)

        assert "test_table" in result["synced"]
        assert len(result["errors"]) == 0

        # Check parquet was written
        cache_path = demo_project / "inputs" / "test_sync.parquet"
        assert cache_path.exists()
        df = pl.read_parquet(cache_path)
        assert len(df) == 3

        # Check sync provenance
        sync_runs = list((demo_project / "sync_runs").iterdir())
        assert len(sync_runs) >= 1
        meta_path = sync_runs[-1] / "sync_meta.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert meta["tables_updated"] == ["test_table"]
        assert meta["tables"][0]["status"] == "ok"
        assert meta["tables"][0]["rowcount"] == 3
        assert meta["tables"][0]["query_hash"] != ""
        assert meta["tables"][0]["output_file_hash"] != ""

    def test_sync_skips_when_cache_fresh(
        self, demo_project: Path, sqlite_db: str
    ) -> None:
        """sync skips tables when cache file is within TTL."""
        _write_sync_workbook(demo_project, sqlite_db, refresh="manual")

        with patch.dict(os.environ, {"TEST_DB_URL": sqlite_db}):
            # First sync
            run_sync(demo_project, table_name="test_table", force=True)
            # Second sync with TTL -- should skip because cache exists and
            # refresh is manual (no TTL forcing re-sync)
            result = run_sync(
                demo_project, table_name="test_table", ttl_hours=24.0
            )

        assert "test_table" in result["skipped"]

    def test_sync_errors_on_missing_env_var(self, demo_project: Path) -> None:
        """sync reports error when env var for connection string is not set."""
        _write_sync_workbook(demo_project, "fake://")

        # Ensure the env var is NOT set
        env = dict(os.environ)
        env.pop("TEST_DB_URL", None)
        with patch.dict(os.environ, env, clear=True):
            result = run_sync(demo_project, table_name="test_table")

        assert len(result["errors"]) == 1
        assert "TEST_DB_URL" in result["errors"][0]

    def test_sync_validates_expected_columns(
        self, demo_project: Path, sqlite_db: str
    ) -> None:
        """sync fails when expected columns are missing from query results."""
        _write_sync_workbook(
            demo_project,
            sqlite_db,
            expected_columns=["ticker", "eps", "nonexistent_col"],
        )

        with patch.dict(os.environ, {"TEST_DB_URL": sqlite_db}):
            result = run_sync(demo_project, table_name="test_table", force=True)

        assert len(result["errors"]) == 1
        assert "nonexistent_col" in result["errors"][0]

    def test_sync_updates_hash_cache(
        self, demo_project: Path, sqlite_db: str
    ) -> None:
        """sync updates cache/hashes.json for the written file."""
        _write_sync_workbook(demo_project, sqlite_db)

        with patch.dict(os.environ, {"TEST_DB_URL": sqlite_db}):
            run_sync(demo_project, table_name="test_table", force=True)

        cache_path = demo_project / "cache" / "hashes.json"
        assert cache_path.exists()
        hashes = json.loads(cache_path.read_text())
        # Should contain an entry for the parquet file
        assert any("test_sync.parquet" in k for k in hashes)


class TestLookupScalar:
    """Test lookup_scalar with missing key, duplicate key, and policies."""

    def test_lookup_scalar_basic(self, demo_project: Path) -> None:
        """lookup_scalar retrieves a value from a table."""
        wb = Workbook(demo_project)
        result = wb.run()
        # Default ticker is AAPL, EPS is 6.75
        assert result.scalars["ticker_eps"] == 6.75

    def test_lookup_scalar_with_override(self, demo_project: Path) -> None:
        """lookup_scalar works with overridden ticker parameter."""
        wb = Workbook(demo_project, overrides={"ticker": "MSFT"})
        result = wb.run()
        assert result.scalars["ticker_eps"] == 12.10

    def test_lookup_scalar_missing_key_error(self, tmp_path: Path) -> None:
        """lookup_scalar raises on missing key by default."""
        project = _minimal_lookup_project(tmp_path, on_missing="error")
        wb = Workbook(project, overrides={"ticker": "NONEXISTENT"})
        with pytest.raises(ValueError, match="no row found"):
            wb.run()

    def test_lookup_scalar_missing_key_none(self, tmp_path: Path) -> None:
        """lookup_scalar returns None when on_missing='none'."""
        project = _minimal_lookup_project(tmp_path, on_missing="none")
        wb = Workbook(project, overrides={"ticker": "NONEXISTENT"})
        result = wb.run()
        assert result.scalars["lookup_result"] is None

    def test_lookup_scalar_duplicate_key_error(self, tmp_path: Path) -> None:
        """lookup_scalar raises on duplicate keys by default."""
        project = _minimal_lookup_project(tmp_path, on_duplicate="error", with_dupes=True)
        wb = Workbook(project, overrides={"ticker": "AAPL"})
        with pytest.raises(ValueError, match="rows found"):
            wb.run()

    def test_lookup_scalar_duplicate_key_first(self, tmp_path: Path) -> None:
        """lookup_scalar takes first match when on_duplicate='first'."""
        project = _minimal_lookup_project(tmp_path, on_duplicate="first", with_dupes=True)
        wb = Workbook(project, overrides={"ticker": "AAPL"})
        result = wb.run()
        # First AAPL row has eps=6.75
        assert result.scalars["lookup_result"] == 6.75


class TestJoinLeft:
    """Test join_left with duplicate validation."""

    def test_join_left_succeeds_unique_right(self, demo_project: Path) -> None:
        """join_left works when right table has unique keys."""
        wb = Workbook(demo_project)
        result = wb.run()
        df = result.tables["prices_with_estimates"]
        assert "eps" in df.columns
        assert len(df) == 10  # All 10 price rows preserved

    def test_join_left_raises_on_right_duplicates(self, tmp_path: Path) -> None:
        """join_left raises when right table has duplicate join keys."""
        project = _project_with_join(tmp_path, right_has_dupes=True, validate="many_to_one")
        wb = Workbook(project)
        with pytest.raises(ValueError, match="duplicate key group"):
            wb.run()

    def test_join_left_allows_dupes_with_many_to_many(self, tmp_path: Path) -> None:
        """join_left allows right duplicates when validate='many_to_many'."""
        project = _project_with_join(tmp_path, right_has_dupes=True, validate="many_to_many")
        wb = Workbook(project)
        result = wb.run()
        # Should succeed without error
        assert "joined" in result.tables

    def test_join_left_no_validate(self, tmp_path: Path) -> None:
        """join_left skips validation when validate='none'."""
        project = _project_with_join(tmp_path, right_has_dupes=True, validate="none")
        wb = Workbook(project)
        result = wb.run()
        assert "joined" in result.tables


class TestGCSyncRuns:
    """Test that GC cleans sync_runs under caps."""

    def test_gc_deletes_excess_sync_runs(self, demo_project: Path) -> None:
        """GC deletes oldest unpinned sync runs when max exceeded."""
        # Set low limit
        (demo_project / "fin123.yaml").write_text("max_sync_runs: 2\nmax_runs: 100\n")

        # Create 4 sync run dirs
        sync_dir = demo_project / "sync_runs"
        sync_dir.mkdir(exist_ok=True)
        for i in range(4):
            sd = sync_dir / f"2026010{i}_sync_{i+1}"
            sd.mkdir()
            (sd / "sync_meta.json").write_text(json.dumps({
                "sync_id": sd.name,
                "timestamp": f"2026-01-0{i+1}T00:00:00+00:00",
                "tables_updated": [],
                "tables": [],
                "pinned": False,
            }))

        summary = run_gc(demo_project)

        remaining = [d for d in sync_dir.iterdir() if d.is_dir()]
        assert len(remaining) == 2
        assert summary["sync_runs_deleted"] == 2

    def test_gc_preserves_pinned_sync_runs(self, demo_project: Path) -> None:
        """GC preserves pinned sync runs even when over limit."""
        (demo_project / "fin123.yaml").write_text("max_sync_runs: 1\nmax_runs: 100\n")

        sync_dir = demo_project / "sync_runs"
        sync_dir.mkdir(exist_ok=True)

        # Create 3 sync runs, pin the first
        for i in range(3):
            sd = sync_dir / f"2026010{i}_sync_{i+1}"
            sd.mkdir()
            (sd / "sync_meta.json").write_text(json.dumps({
                "sync_id": sd.name,
                "timestamp": f"2026-01-0{i+1}T00:00:00+00:00",
                "tables_updated": [],
                "tables": [],
                "pinned": i == 0,
            }))

        summary = run_gc(demo_project)

        remaining = [d for d in sync_dir.iterdir() if d.is_dir()]
        # Pinned one survives + 1 from limit
        assert len(remaining) <= 2
        # The pinned one (first) should exist
        assert (sync_dir / "20260100_sync_1").exists()


class TestCLISync:
    """Test CLI sync command."""

    def test_cli_sync_no_sql_tables(self, demo_project: Path) -> None:
        """CLI sync with no SQL tables reports nothing to sync."""
        from click.testing import CliRunner
        from fin123.cli import main

        # Remove SQL table from workbook
        _write_simple_workbook(demo_project)

        runner = CliRunner()
        result = runner.invoke(main, ["sync", str(demo_project)])
        assert result.exit_code == 0
        assert "No sync targets found" in result.output

    def test_cli_sync_help(self) -> None:
        """CLI sync --help works."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["sync", "--help"])
        assert result.exit_code == 0
        assert "--table" in result.output
        assert "--force" in result.output


# --- Helper functions ---


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


def _write_simple_workbook(project_dir: Path) -> None:
    """Write a minimal workbook.yaml without SQL tables."""
    workbook = """\
version: 1
params:
  x: 1
tables:
  prices:
    source: inputs/prices.csv
    format: csv
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
    with_dupes: bool = False,
) -> Path:
    """Create a minimal project with a lookup_scalar output."""
    project = tmp_path / "lookup_project"
    project.mkdir()

    # Write data parquet
    inputs = project / "inputs"
    inputs.mkdir()
    rows = {
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "eps": [6.75, 12.10, 7.50],
    }
    if with_dupes:
        rows["ticker"].append("AAPL")
        rows["eps"].append(7.00)
    pl.DataFrame(rows).write_parquet(inputs / "data.parquet")

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
      key_col: ticker
      value_col: eps
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
    """Create a project with a join_left plan for testing validation."""
    project = tmp_path / "join_project"
    project.mkdir()

    inputs = project / "inputs"
    inputs.mkdir()

    # Left table
    pl.DataFrame({
        "id": [1, 2, 3],
        "key": ["A", "B", "C"],
        "value": [10, 20, 30],
    }).write_parquet(inputs / "left.parquet")

    # Right table
    right_rows: dict[str, list] = {
        "key": ["A", "B", "C"],
        "extra": [100, 200, 300],
    }
    if right_has_dupes:
        right_rows["key"].append("A")
        right_rows["extra"].append(150)
    pl.DataFrame(right_rows).write_parquet(inputs / "right.parquet")

    # Use quoted "on" in YAML to avoid boolean coercion
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
