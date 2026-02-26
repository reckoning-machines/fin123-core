"""Tests for assertions, verify, scenarios, and production mode enforcement."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Scaffold a demo project for testing."""
    from fin123.project import scaffold_project

    return scaffold_project(tmp_path / "proj")


# ---------------------------------------------------------------------------
# A) Assertions
# ---------------------------------------------------------------------------


class TestAssertions:
    def test_all_pass(self):
        from fin123.assertions import evaluate_assertions

        specs = [
            {"name": "revenue_positive", "expr": "$revenue > 0", "severity": "error"},
            {"name": "margin_valid", "expr": "NOT(ISNAN($margin))", "severity": "warn"},
        ]
        scalars = {"revenue": 125000.0, "margin": 0.15}
        report = evaluate_assertions(specs, scalars)
        assert report["status"] == "pass"
        assert report["failed_count"] == 0
        assert report["warn_count"] == 0
        assert len(report["results"]) == 2
        assert all(r["ok"] for r in report["results"])

    def test_error_failure(self):
        from fin123.assertions import evaluate_assertions

        specs = [
            {"name": "revenue_positive", "expr": "$revenue > 0", "severity": "error"},
        ]
        scalars = {"revenue": -100.0}
        report = evaluate_assertions(specs, scalars)
        assert report["status"] == "fail"
        assert report["failed_count"] == 1

    def test_warn_only(self):
        from fin123.assertions import evaluate_assertions

        specs = [
            {"name": "margin_check", "expr": "$margin > 0.5", "severity": "warn"},
        ]
        scalars = {"margin": 0.1}
        report = evaluate_assertions(specs, scalars)
        assert report["status"] == "warn"
        assert report["warn_count"] == 1
        assert report["failed_count"] == 0

    def test_comparison_operators(self):
        from fin123.assertions import evaluate_assertions

        specs = [
            {"name": "ge", "expr": "$x >= 10", "severity": "error"},
            {"name": "le", "expr": "$x <= 10", "severity": "error"},
            {"name": "eq", "expr": "$x == 10", "severity": "error"},
            {"name": "ne", "expr": "$x != 0", "severity": "error"},
        ]
        scalars = {"x": 10}
        report = evaluate_assertions(specs, scalars)
        assert report["status"] == "pass"
        assert all(r["ok"] for r in report["results"])

    def test_isnan_check(self):
        import math

        from fin123.assertions import evaluate_assertions

        specs = [
            {"name": "is_nan", "expr": "ISNAN($bad)", "severity": "error"},
        ]
        scalars = {"bad": float("nan")}
        report = evaluate_assertions(specs, scalars)
        assert report["status"] == "pass"
        assert report["results"][0]["ok"]

    def test_not_isnan_with_nan_fails(self):
        from fin123.assertions import evaluate_assertions

        specs = [
            {"name": "not_nan", "expr": "NOT(ISNAN($bad))", "severity": "error"},
        ]
        scalars = {"bad": float("nan")}
        report = evaluate_assertions(specs, scalars)
        assert report["status"] == "fail"

    def test_missing_variable(self):
        from fin123.assertions import evaluate_assertions

        specs = [
            {"name": "check", "expr": "$missing > 0", "severity": "error"},
        ]
        report = evaluate_assertions(specs, {})
        assert report["status"] == "fail"

    def test_empty_assertions(self):
        from fin123.assertions import evaluate_assertions

        report = evaluate_assertions([], {"x": 1})
        assert report["status"] == "pass"
        assert report["failed_count"] == 0
        assert report["warn_count"] == 0
        assert report["results"] == []


# ---------------------------------------------------------------------------
# A.2) Event display mapping
# ---------------------------------------------------------------------------


class TestDisplayEventType:
    def test_run_maps_to_build(self):
        from fin123.logging.events import display_event_type

        assert display_event_type("run_started") == "build_started"
        assert display_event_type("run_completed") == "build_completed"
        assert display_event_type("run_verify_pass") == "build_verify_pass"
        assert display_event_type("run_timing") == "build_timing"

    def test_non_run_unchanged(self):
        from fin123.logging.events import display_event_type

        assert display_event_type("sync_started") == "sync_started"
        assert display_event_type("assertion_pass") == "assertion_pass"
        assert display_event_type("mode_block") == "mode_block"

    def test_enum_input(self):
        from fin123.logging.events import EventType, display_event_type

        assert display_event_type(EventType.run_started) == "build_started"
        assert display_event_type(EventType.sync_started) == "sync_started"


# ---------------------------------------------------------------------------
# B) Hash helpers
# ---------------------------------------------------------------------------


class TestHashHelpers:
    def test_overlay_hash_deterministic(self):
        from fin123.utils.hash import overlay_hash

        h1 = overlay_hash("base", {"tax_rate": 0.1, "discount": 0.05})
        h2 = overlay_hash("base", {"discount": 0.05, "tax_rate": 0.1})
        assert h1 == h2
        assert len(h1) == 64

    def test_overlay_hash_different_scenarios(self):
        from fin123.utils.hash import overlay_hash

        h1 = overlay_hash("low_tax", {"tax_rate": 0.1})
        h2 = overlay_hash("high_tax", {"tax_rate": 0.3})
        assert h1 != h2

    def test_compute_export_hash(self, tmp_path):
        from fin123.utils.hash import compute_export_hash

        outputs = tmp_path / "outputs"
        outputs.mkdir()
        (outputs / "scalars.json").write_text('{"x": 1}')
        (outputs / "table.parquet").write_bytes(b"fake parquet data")
        (outputs / "ignored.txt").write_text("not included")

        h = compute_export_hash(outputs)
        assert len(h) == 64

    def test_compute_export_hash_deterministic(self, tmp_path):
        from fin123.utils.hash import compute_export_hash

        outputs = tmp_path / "outputs"
        outputs.mkdir()
        (outputs / "scalars.json").write_text('{"x": 1}')

        h1 = compute_export_hash(outputs)
        h2 = compute_export_hash(outputs)
        assert h1 == h2

    def test_compute_plugin_hash_combined(self):
        from fin123.utils.hash import compute_plugin_hash_combined

        h = compute_plugin_hash_combined("1.0.0", {"myplugin": {"sha256": "abc123"}})
        assert len(h) == 64

    def test_plugin_hash_changes_with_version(self):
        from fin123.utils.hash import compute_plugin_hash_combined

        h1 = compute_plugin_hash_combined("1.0.0", {})
        h2 = compute_plugin_hash_combined("2.0.0", {})
        assert h1 != h2


# ---------------------------------------------------------------------------
# C) Verify
# ---------------------------------------------------------------------------


class TestVerify:
    def test_verify_missing_run(self, project_dir):
        from fin123.verify import verify_run

        report = verify_run(project_dir, "nonexistent_run")
        assert report["status"] == "fail"
        assert "not found" in report["failures"][0]

    def test_verify_run_no_model_version(self, project_dir):
        """Run without model_version_id should report failure."""
        from fin123.verify import verify_run

        run_dir = project_dir / "runs" / "test_run"
        run_dir.mkdir(parents=True)
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text('{"x": 1}')

        meta = {"run_id": "test_run", "timestamp": "2025-01-01"}
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        report = verify_run(project_dir, "test_run")
        assert report["status"] == "fail"
        assert any("model_version_id" in f for f in report["failures"])

    def test_verify_writes_report(self, project_dir):
        """verify_run should write verify_report.json."""
        from fin123.verify import verify_run

        run_dir = project_dir / "runs" / "test_run"
        run_dir.mkdir(parents=True)
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text('{"x": 1}')

        meta = {"run_id": "test_run"}
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        verify_run(project_dir, "test_run")

        report_path = run_dir / "verify_report.json"
        assert report_path.exists()
        report = json.loads(report_path.read_text())
        assert "status" in report
        assert "failures" in report

    def test_verify_checks_params_hash(self, project_dir):
        """verify_run should detect params_hash mismatch."""
        from fin123.verify import verify_run

        run_dir = project_dir / "runs" / "test_run_ph"
        run_dir.mkdir(parents=True)
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text('{"x": 1}')

        meta = {
            "run_id": "test_run_ph",
            "params_hash": "0" * 64,
            "effective_params": {"ticker": "AAPL"},
        }
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        report = verify_run(project_dir, "test_run_ph")
        assert any("params_hash" in f for f in report["failures"])

    def test_verify_passes_correct_params_hash(self, project_dir):
        """verify_run should pass when params_hash matches."""
        from fin123.utils.hash import compute_params_hash
        from fin123.verify import verify_run

        effective_params = {"ticker": "AAPL", "rate": 0.05}
        params_hash = compute_params_hash(effective_params)

        run_dir = project_dir / "runs" / "test_run_ph_ok"
        run_dir.mkdir(parents=True)
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text('{"x": 1}')

        meta = {
            "run_id": "test_run_ph_ok",
            "params_hash": params_hash,
            "effective_params": effective_params,
        }
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        report = verify_run(project_dir, "test_run_ph_ok")
        assert not any("params_hash" in f for f in report["failures"])


# ---------------------------------------------------------------------------
# D) Production mode enforcement
# ---------------------------------------------------------------------------


class TestProdMode:
    def test_default_mode_is_dev(self, project_dir):
        from fin123.project import get_project_mode

        mode = get_project_mode(project_dir)
        assert mode == "dev"

    def test_prod_mode_from_config(self, project_dir):
        import yaml

        from fin123.project import get_project_mode

        config_path = project_dir / "fin123.yaml"
        config = yaml.safe_load(config_path.read_text()) or {}
        config["mode"] = "prod"
        config_path.write_text(yaml.dump(config))

        mode = get_project_mode(project_dir)
        assert mode == "prod"

    def test_enforce_prod_no_version(self, project_dir):
        from fin123.project import enforce_prod_mode

        errors = enforce_prod_mode(
            project_dir,
            workbook_spec={"tables": {}},
            model_version_id=None,
            plugins_info={},
        )
        assert any("model_version_id" in e or "snapshot" in e for e in errors)

    def test_enforce_prod_with_version_passes(self, project_dir):
        from fin123.project import enforce_prod_mode

        errors = enforce_prod_mode(
            project_dir,
            workbook_spec={"tables": {}},
            model_version_id="v0001",
            plugins_info={},
        )
        # Should pass (no blocking errors for this basic case)
        assert not any("snapshot" in e for e in errors)

    def test_enforce_prod_assertion_failure(self, project_dir):
        from fin123.project import enforce_prod_mode

        errors = enforce_prod_mode(
            project_dir,
            workbook_spec={"tables": {}},
            model_version_id="v0001",
            plugins_info={},
            assertion_report={"failed_count": 2},
        )
        assert any("assertion" in e for e in errors)

    def test_enforce_prod_unpinned_plugin(self, project_dir):
        from fin123.project import enforce_prod_mode

        errors = enforce_prod_mode(
            project_dir,
            workbook_spec={"tables": {}},
            model_version_id="v0001",
            plugins_info={"myplugin": {"sha256": "abc"}},
        )
        assert any("plugin" in e.lower() or "pin" in e.lower() for e in errors)

    def test_enforce_prod_import_parse_errors(self, project_dir):
        from fin123.project import enforce_prod_mode

        # Write import_report.json with parse errors
        report = {
            "formula_classifications": [
                {"classification": "parse_error", "sheet": "Sheet1", "addr": "A1"},
            ]
        }
        (project_dir / "import_report.json").write_text(json.dumps(report))

        errors = enforce_prod_mode(
            project_dir,
            workbook_spec={"tables": {}},
            model_version_id="v0001",
            plugins_info={},
        )
        assert any("parse error" in e for e in errors)


# ---------------------------------------------------------------------------
# E) CLI: verify-run command
# ---------------------------------------------------------------------------


@pytest.mark.pod
class TestVerifyBuildCLI:
    def test_verify_build_missing(self, project_dir):
        from click.testing import CliRunner

        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["verify-build", "nonexistent", "--project", str(project_dir)])
        assert result.exit_code == 2

    def test_verify_build_json_output(self, project_dir):
        from click.testing import CliRunner

        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["verify-build", "nonexistent", "--project", str(project_dir), "--json"],
        )
        assert result.exit_code == 2
        output = json.loads(result.output)
        assert output["status"] == "fail"

    def test_verify_run_deprecated_alias(self, project_dir):
        from click.testing import CliRunner

        from fin123.cli import main

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(main, ["verify-run", "nonexistent", "--project", str(project_dir)])
        assert result.exit_code == 2
        assert "deprecated" in result.stderr_bytes.decode().lower()


# ---------------------------------------------------------------------------
# F) CLI: scenario flags
# ---------------------------------------------------------------------------


@pytest.mark.pod
class TestCommitCLI:
    def test_commit_creates_snapshot(self, project_dir):
        from click.testing import CliRunner

        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["commit", str(project_dir)])
        assert result.exit_code == 0
        assert "Committed snapshot:" in result.output


@pytest.mark.pod
class TestScenarioCLI:
    def test_all_scenarios_no_scenarios(self, project_dir):
        from click.testing import CliRunner

        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["build", str(project_dir), "--all-scenarios"])
        assert result.exit_code == 0
        assert "No scenarios" in result.output
