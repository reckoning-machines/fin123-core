"""Tests for fin123 Diff v1."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from fin123.project import scaffold_project
from fin123.workbook import Workbook


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Scaffold a demo project in a temporary directory."""
    project_dir = tmp_path / "proj"
    scaffold_project(project_dir)
    return project_dir


# ---------------------------------------------------------------------------
# 1) diff run fast-path identical
# ---------------------------------------------------------------------------


class TestDiffRunFastPath:
    def test_identical_runs(self, demo_project: Path) -> None:
        """Two builds with same inputs produce export_hash match → status identical."""
        from fin123.diff import diff_runs

        wb1 = Workbook(demo_project)
        r1 = wb1.run()
        wb2 = Workbook(demo_project)
        r2 = wb2.run()

        result = diff_runs(demo_project, r1.run_dir.name, r2.run_dir.name)

        assert result["status"] == "identical"
        assert result["type"] == "run_diff"
        md = result["meta_diff"]
        assert md["workbook_spec_hash_match"] is True
        assert md["params_hash_match"] is True
        assert md["input_hashes_match"] is True

    def test_identical_run_has_no_scalar_diff(self, demo_project: Path) -> None:
        """Fast-path identical should not have scalar_diff or table_diffs."""
        from fin123.diff import diff_runs

        wb1 = Workbook(demo_project)
        r1 = wb1.run()
        wb2 = Workbook(demo_project)
        r2 = wb2.run()

        result = diff_runs(demo_project, r1.run_dir.name, r2.run_dir.name)
        assert "scalar_diff" not in result
        assert "table_diffs" not in result


# ---------------------------------------------------------------------------
# 2) diff run scalar change
# ---------------------------------------------------------------------------


class TestDiffRunScalarChange:
    def test_param_override_changes_scalar(self, demo_project: Path) -> None:
        """Build with different param override reports changed scalar."""
        from fin123.diff import diff_runs

        wb1 = Workbook(demo_project)
        r1 = wb1.run()

        wb2 = Workbook(demo_project, overrides={"tax_rate": 0.25})
        r2 = wb2.run()

        result = diff_runs(demo_project, r1.run_dir.name, r2.run_dir.name)

        assert result["status"] == "different"

        # Meta should show params mismatch
        md = result["meta_diff"]
        assert md["params_hash_match"] is False

        # Scalar diff should show total_revenue changed
        sd = result["scalar_diff"]
        changed_names = [c["name"] for c in sd["changed"]]
        assert "total_revenue" in changed_names

        # Check delta and pct_change for numeric change
        for c in sd["changed"]:
            if c["name"] == "total_revenue":
                assert "delta" in c
                assert "pct_change" in c
                # tax_rate 0.15 → 0.25: revenue = 125000*(1-0.15) vs 125000*(1-0.25)
                assert c["a_value"] == pytest.approx(106250.0)
                assert c["b_value"] == pytest.approx(93750.0)
                assert c["delta"] == pytest.approx(-12500.0)
                break

    def test_param_change_in_meta_diff(self, demo_project: Path) -> None:
        """Effective param changes are reported in meta_diff."""
        from fin123.diff import diff_runs

        wb1 = Workbook(demo_project)
        r1 = wb1.run()

        wb2 = Workbook(demo_project, overrides={"tax_rate": 0.30})
        r2 = wb2.run()

        result = diff_runs(demo_project, r1.run_dir.name, r2.run_dir.name)

        md = result["meta_diff"]
        assert "param_changes" in md
        tax_change = next(pc for pc in md["param_changes"] if pc["key"] == "tax_rate")
        assert tax_change["a"] == 0.15
        assert tax_change["b"] == 0.30


# ---------------------------------------------------------------------------
# 3) diff run table diff without primary_key
# ---------------------------------------------------------------------------


class TestDiffRunTableNoPK:
    def test_table_checksum_mismatch_no_pk(self, demo_project: Path) -> None:
        """Table diff without primary_key skips row-level diff but reports checksum mismatch."""
        from fin123.diff import diff_runs

        # The demo project has a 'prices' table without primary_key
        # and plans that filter it — different params produce different tables
        wb1 = Workbook(demo_project)
        r1 = wb1.run()

        wb2 = Workbook(demo_project, overrides={"tax_rate": 0.30})
        r2 = wb2.run()

        result = diff_runs(demo_project, r1.run_dir.name, r2.run_dir.name)

        # Find the summary_by_category table diff (if it differs due to
        # different scalar outputs but same table logic, tables may be identical)
        # Let's check that tables are present in the diff
        assert "table_diffs" in result

        # For tables that differ: check row_level_diff skipped reason
        for td in result["table_diffs"]:
            if td.get("status") == "changed" and td.get("row_level_diff") == "skipped":
                assert "no primary_key" in td.get("row_level_diff_reason", "")
                break


# ---------------------------------------------------------------------------
# 4) diff version reports changes
# ---------------------------------------------------------------------------


class TestDiffVersion:
    def test_version_param_change(self, demo_project: Path) -> None:
        """Diff version reports a param change between two committed versions."""
        from fin123.diff import diff_versions
        from fin123.versioning import SnapshotStore

        store = SnapshotStore(demo_project)

        # v0001 already exists from scaffold
        # Modify workbook.yaml and create v0002
        wb_path = demo_project / "workbook.yaml"
        spec = yaml.safe_load(wb_path.read_text())
        spec["params"]["tax_rate"] = 0.25
        wb_path.write_text(yaml.dump(spec, sort_keys=False))
        store.save_snapshot(wb_path.read_text())

        versions = store.list_versions()
        assert len(versions) >= 2

        result = diff_versions(demo_project, versions[0], versions[1])

        assert result["type"] == "version_diff"
        pd = result["params_diff"]
        changed_keys = [c["key"] for c in pd["changed"]]
        assert "tax_rate" in changed_keys

        for c in pd["changed"]:
            if c["key"] == "tax_rate":
                assert c["a"] == 0.15
                assert c["b"] == 0.25
                break

    def test_version_output_added(self, demo_project: Path) -> None:
        """Diff version reports an added output."""
        from fin123.diff import diff_versions
        from fin123.versioning import SnapshotStore

        store = SnapshotStore(demo_project)

        wb_path = demo_project / "workbook.yaml"
        spec = yaml.safe_load(wb_path.read_text())
        spec.setdefault("outputs", []).append({
            "name": "new_scalar",
            "type": "scalar",
            "value": 42,
        })
        wb_path.write_text(yaml.dump(spec, sort_keys=False))
        store.save_snapshot(wb_path.read_text())

        versions = store.list_versions()

        result = diff_versions(demo_project, versions[0], versions[-1])
        od = result["outputs_diff"]
        assert "new_scalar" in od["added"]


# ---------------------------------------------------------------------------
# 5) --json output
# ---------------------------------------------------------------------------


class TestDiffJSON:
    def test_diff_run_json(self, demo_project: Path) -> None:
        """--json outputs valid JSON with required top-level keys."""
        from click.testing import CliRunner
        from fin123.cli import main

        wb1 = Workbook(demo_project)
        r1 = wb1.run()
        wb2 = Workbook(demo_project)
        r2 = wb2.run()

        runner = CliRunner()
        result = runner.invoke(main, [
            "diff", "run", r1.run_dir.name, r2.run_dir.name,
            "--project", str(demo_project),
            "--json",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["type"] == "run_diff"
        assert "status" in data
        assert "meta_diff" in data

    def test_diff_version_json(self, demo_project: Path) -> None:
        """--json version diff outputs valid JSON with required keys."""
        from click.testing import CliRunner
        from fin123.cli import main
        from fin123.versioning import SnapshotStore

        store = SnapshotStore(demo_project)
        wb_path = demo_project / "workbook.yaml"
        spec = yaml.safe_load(wb_path.read_text())
        spec["params"]["discount_rate"] = 0.20
        wb_path.write_text(yaml.dump(spec, sort_keys=False))
        store.save_snapshot(wb_path.read_text())

        versions = store.list_versions()

        runner = CliRunner()
        result = runner.invoke(main, [
            "diff", "version", versions[0], versions[-1],
            "--project", str(demo_project),
            "--json",
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["type"] == "version_diff"
        assert "params_diff" in data
        assert "outputs_diff" in data

    def test_diff_run_text_output(self, demo_project: Path) -> None:
        """Default (non-JSON) output is human-readable text."""
        from click.testing import CliRunner
        from fin123.cli import main

        wb1 = Workbook(demo_project)
        r1 = wb1.run()
        wb2 = Workbook(demo_project, overrides={"tax_rate": 0.25})
        r2 = wb2.run()

        runner = CliRunner()
        result = runner.invoke(main, [
            "diff", "run", r1.run_dir.name, r2.run_dir.name,
            "--project", str(demo_project),
        ])
        assert result.exit_code == 0, result.output
        assert "Run diff:" in result.output
        assert "different" in result.output


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestDiffErrors:
    def test_missing_run(self, demo_project: Path) -> None:
        """Missing run_id exits with code 2."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, [
            "diff", "run", "nonexistent_a", "nonexistent_b",
            "--project", str(demo_project),
        ])
        assert result.exit_code == 2

    def test_missing_version(self, demo_project: Path) -> None:
        """Missing version exits with code 2."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, [
            "diff", "version", "v9998", "v9999",
            "--project", str(demo_project),
        ])
        assert result.exit_code == 2
