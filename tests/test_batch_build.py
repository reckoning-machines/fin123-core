"""Tests for batch build orchestration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# A) load_params_csv
# ---------------------------------------------------------------------------


class TestLoadParamsCsv:
    def test_basic_load(self, tmp_path):
        from fin123.batch import load_params_csv

        csv_path = tmp_path / "params.csv"
        csv_path.write_text("ticker,rate\nAAPL,0.05\nMSFT,0.08\n")

        rows = load_params_csv(csv_path)
        assert len(rows) == 2
        assert rows[0]["ticker"] == "AAPL"
        assert rows[0]["rate"] == 0.05
        assert rows[1]["ticker"] == "MSFT"
        assert rows[1]["rate"] == 0.08

    def test_empty_csv(self, tmp_path):
        from fin123.batch import load_params_csv

        csv_path = tmp_path / "params.csv"
        csv_path.write_text("ticker,rate\n")

        rows = load_params_csv(csv_path)
        assert rows == []

    def test_string_values_kept(self, tmp_path):
        from fin123.batch import load_params_csv

        csv_path = tmp_path / "params.csv"
        csv_path.write_text("name,value\nalpha,hello\nbeta,world\n")

        rows = load_params_csv(csv_path)
        assert rows[0]["value"] == "hello"
        assert rows[1]["value"] == "world"


# ---------------------------------------------------------------------------
# B) Sequential batch
# ---------------------------------------------------------------------------


class TestSequentialBatch:
    @pytest.fixture
    def project_dir(self, tmp_path: Path) -> Path:
        from fin123.project import scaffold_project

        return scaffold_project(tmp_path / "proj")

    def test_basic_batch(self, project_dir):
        from fin123.batch import run_batch

        rows = [
            {"discount_rate": 0.1},
            {"discount_rate": 0.2},
        ]

        summary = run_batch(project_dir, rows)
        assert summary["total"] == 2
        assert summary["ok"] == 2
        assert summary["failed"] == 0
        assert len(summary["build_batch_id"]) > 0

        # Verify batch_id in run_meta
        for r in summary["results"]:
            assert r["status"] == "ok"
            run_dir = project_dir / "runs" / r["run_id"]
            meta = json.loads((run_dir / "run_meta.json").read_text())
            assert meta["build_batch_id"] == summary["build_batch_id"]
            assert "batch_index" in meta

    def test_batch_with_scenario(self, project_dir):
        import yaml

        # Add a scenario to the workbook
        spec_path = project_dir / "workbook.yaml"
        spec = yaml.safe_load(spec_path.read_text()) or {}
        spec["scenarios"] = {
            "low": {"overrides": {"discount_rate": 0.01}},
        }
        spec_path.write_text(yaml.dump(spec, default_flow_style=False))

        from fin123.batch import run_batch

        rows = [{"discount_rate": 0.1}]
        summary = run_batch(project_dir, rows, scenario_name="low")
        assert summary["ok"] == 1

    def test_batch_run_meta_has_params_hash(self, project_dir):
        from fin123.batch import run_batch

        rows = [{"discount_rate": 0.15}]
        summary = run_batch(project_dir, rows)
        assert summary["ok"] == 1

        run_dir = project_dir / "runs" / summary["results"][0]["run_id"]
        meta = json.loads((run_dir / "run_meta.json").read_text())
        assert "params_hash" in meta
        assert len(meta["params_hash"]) == 64
        assert "effective_params" in meta


# ---------------------------------------------------------------------------
# C) Batch build CLI
# ---------------------------------------------------------------------------


@pytest.mark.pod
class TestBatchBuildCLI:
    @pytest.fixture
    def project_dir(self, tmp_path: Path) -> Path:
        from fin123.project import scaffold_project

        return scaffold_project(tmp_path / "proj")

    def test_cli_batch_build(self, project_dir, tmp_path):
        from click.testing import CliRunner

        from fin123.cli import main

        csv_path = tmp_path / "params.csv"
        csv_path.write_text("discount_rate\n0.1\n0.2\n")

        runner = CliRunner()
        result = runner.invoke(main, [
            "batch", "build", str(project_dir),
            "--params-file", str(csv_path),
        ])
        assert result.exit_code == 0
        assert "Batch ID:" in result.output
        assert "Total: 2" in result.output
        assert "OK: 2" in result.output
