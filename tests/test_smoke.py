"""Smoke tests for fin123 core functionality."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from fin123.project import scaffold_project
from fin123.workbook import Workbook
from fin123.workflows.runner import run_workflow
from fin123.gc import run_gc


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Scaffold a demo project in a temporary directory."""
    project_dir = tmp_path / "test_project"
    scaffold_project(project_dir)
    return project_dir


class TestNewAndRun:
    """Test that scaffolding a project and running it succeeds."""

    def test_scaffold_creates_workbook(self, demo_project: Path) -> None:
        """Scaffold creates workbook.yaml and required directories."""
        assert (demo_project / "workbook.yaml").exists()
        assert (demo_project / "fin123.yaml").exists()
        assert (demo_project / "inputs" / "prices.csv").exists()
        assert (demo_project / "workflows" / "scenario_sweep.yaml").exists()

    def test_run_succeeds(self, demo_project: Path) -> None:
        """Running the workbook produces scalar and table outputs."""
        wb = Workbook(demo_project)
        result = wb.run()

        # Scalar outputs
        assert "total_revenue" in result.scalars
        assert "gross_revenue" in result.scalars
        assert result.scalars["gross_revenue"] == 125000.0
        # total_revenue = 125000 * (1 - 0.15) = 106250.0
        assert result.scalars["total_revenue"] == 106250.0

        # Table outputs
        assert "filtered_prices" in result.tables
        assert "summary_by_category" in result.tables
        assert len(result.tables["filtered_prices"]) > 0
        assert len(result.tables["summary_by_category"]) > 0

        # Run directory created
        assert result.run_dir.exists()
        assert (result.run_dir / "run_meta.json").exists()
        assert (result.run_dir / "outputs" / "scalars.json").exists()

    def test_run_with_overrides(self, demo_project: Path) -> None:
        """Parameter overrides affect scalar outputs."""
        wb = Workbook(demo_project, overrides={"tax_rate": 0.20})
        result = wb.run()

        # total_revenue = 125000 * (1 - 0.20) = 100000.0
        assert result.scalars["total_revenue"] == 100000.0

    def test_run_creates_snapshot(self, demo_project: Path) -> None:
        """Each run creates a workbook snapshot (v0001 from scaffold, v0002 from run)."""
        wb = Workbook(demo_project)
        wb.run()

        snapshot_dir = demo_project / "snapshots" / "workbook"
        assert snapshot_dir.exists()
        versions = sorted(p for p in snapshot_dir.iterdir() if p.is_dir())
        # v0001 from scaffold_project, v0002 from run
        assert len(versions) == 2
        assert versions[0].name == "v0001"
        assert versions[1].name == "v0002"
        assert (versions[1] / "workbook.yaml").exists()

    def test_run_meta_contents(self, demo_project: Path) -> None:
        """Run metadata contains required fields."""
        wb = Workbook(demo_project)
        result = wb.run()

        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        assert "run_id" in meta
        assert "timestamp" in meta
        assert "workbook_spec_hash" in meta
        assert "input_hashes" in meta
        assert "engine_version" in meta
        assert meta["pinned"] is False

    def test_multiple_runs_create_separate_dirs(self, demo_project: Path) -> None:
        """Multiple runs create distinct run directories."""
        wb1 = Workbook(demo_project)
        r1 = wb1.run()
        wb2 = Workbook(demo_project)
        r2 = wb2.run()

        assert r1.run_dir != r2.run_dir
        runs = list((demo_project / "runs").iterdir())
        assert len(runs) == 2


class TestScenarioSweep:
    """Test that the scenario sweep workflow creates artifacts."""

    def test_scenario_sweep_creates_artifact(self, demo_project: Path) -> None:
        """Scenario sweep produces a versioned artifact with results table."""
        result = run_workflow("scenario_sweep", demo_project)

        assert result["workflow"] == "scenario_sweep"
        assert result["scenario_count"] == 3
        assert result["artifact_version"] == "v0001"

        # Check artifact on disk
        artifact_dir = Path(result["artifact_dir"])
        assert artifact_dir.exists()
        assert (artifact_dir / "meta.json").exists()
        assert (artifact_dir / "table.parquet").exists()
        assert (artifact_dir / "artifact.json").exists()

        # Check results contain expected scenarios
        scenarios = {r["scenario"] for r in result["results"]}
        assert scenarios == {"low_tax", "medium_tax", "high_tax"}

        # Check scalar collection
        for row in result["results"]:
            assert "total_revenue" in row
            assert row["total_revenue"] is not None

    def test_scenario_sweep_values_correct(self, demo_project: Path) -> None:
        """Scenario sweep scalar values match expected calculations."""
        result = run_workflow("scenario_sweep", demo_project)

        for row in result["results"]:
            expected = 125000.0 * (1 - row["tax_rate"])
            assert row["total_revenue"] == pytest.approx(expected)

    def test_second_sweep_increments_version(self, demo_project: Path) -> None:
        """Running the sweep twice creates v0001 and v0002."""
        r1 = run_workflow("scenario_sweep", demo_project)
        r2 = run_workflow("scenario_sweep", demo_project)

        assert r1["artifact_version"] == "v0001"
        assert r2["artifact_version"] == "v0002"


class TestGarbageCollection:
    """Test that GC deletes unpinned old runs when limit exceeded."""

    def test_gc_deletes_excess_runs(self, demo_project: Path) -> None:
        """GC deletes oldest unpinned runs when max_runs is exceeded."""
        # Set very low limit
        (demo_project / "fin123.yaml").write_text("max_runs: 3\n")

        # Create 5 runs
        for i in range(5):
            wb = Workbook(demo_project)
            wb.run()

        runs_before = list((demo_project / "runs").iterdir())
        assert len(runs_before) == 5

        # Run GC
        summary = run_gc(demo_project)

        runs_after = list((demo_project / "runs").iterdir())
        assert len(runs_after) == 3
        assert summary["runs_deleted"] == 2

    def test_gc_preserves_pinned_runs(self, demo_project: Path) -> None:
        """GC never deletes pinned runs even when limit is exceeded."""
        (demo_project / "fin123.yaml").write_text("max_runs: 2\n")

        # Create 4 runs
        run_dirs = []
        for _ in range(4):
            wb = Workbook(demo_project)
            result = wb.run()
            run_dirs.append(result.run_dir)

        # Pin the oldest run
        meta_path = run_dirs[0] / "run_meta.json"
        meta = json.loads(meta_path.read_text())
        meta["pinned"] = True
        meta_path.write_text(json.dumps(meta, indent=2))

        summary = run_gc(demo_project)

        # The oldest (pinned) run should survive
        assert run_dirs[0].exists()
        # Should still have at most 3 total (pinned + 2 from limit)
        remaining = [d for d in (demo_project / "runs").iterdir() if d.is_dir()]
        assert len(remaining) <= 3

    def test_gc_on_clean_project(self, demo_project: Path) -> None:
        """GC on a project with no runs does nothing."""
        summary = run_gc(demo_project)
        assert summary["runs_deleted"] == 0
        assert summary["artifact_versions_deleted"] == 0

    def test_gc_deletes_excess_artifact_versions(self, demo_project: Path) -> None:
        """GC deletes oldest artifact versions when limit exceeded."""
        (demo_project / "fin123.yaml").write_text(
            "max_runs: 100\nmax_artifact_versions: 2\n"
        )

        # Create 4 artifact versions via workflow
        for _ in range(4):
            run_workflow("scenario_sweep", demo_project)

        artifact_dir = demo_project / "artifacts" / "scenario_sweep_results"
        versions_before = [d for d in artifact_dir.iterdir() if d.is_dir()]
        assert len(versions_before) == 4

        summary = run_gc(demo_project)

        versions_after = [d for d in artifact_dir.iterdir() if d.is_dir()]
        assert len(versions_after) == 2
        assert summary["artifact_versions_deleted"] == 2


class TestCLI:
    """Test CLI commands via Click's testing utilities."""

    def test_cli_help(self) -> None:
        """CLI --help works."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "fin123" in result.output

    def test_cli_new_and_run(self, tmp_path: Path) -> None:
        """CLI new + run integration test."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        project = tmp_path / "cli_test"

        result = runner.invoke(main, ["new", str(project)])
        assert result.exit_code == 0

        result = runner.invoke(main, ["run", str(project)])
        assert result.exit_code == 0
        assert "total_revenue" in result.output

    def test_cli_export(self, demo_project: Path) -> None:
        """CLI export shows run outputs."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()

        # Run first
        runner.invoke(main, ["run", str(demo_project)])

        # Export
        result = runner.invoke(main, ["export", str(demo_project)])
        assert result.exit_code == 0
        assert "Exporting run" in result.output

    def test_cli_gc(self, demo_project: Path) -> None:
        """CLI gc command runs successfully."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["gc", str(demo_project)])
        assert result.exit_code == 0
        assert "GC complete" in result.output
