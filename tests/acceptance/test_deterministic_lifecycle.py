"""Acceptance tests: full deterministic lifecycle for fin123-core.

These tests exercise the end-to-end build/verify/diff workflow using
real project scaffolding and the built-in templates. They confirm that
the deterministic guarantees hold across runs and that the verification
pipeline catches drift.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fin123.project import scaffold_project
from fin123.workbook import Workbook


class TestScaffoldBuildVerify:
    """End-to-end: scaffold a project, build it, verify the build."""

    def test_scaffold_build_verify_passes(self, tmp_path: Path) -> None:
        """A fresh scaffold followed by build and verify produces a passing result."""
        project = tmp_path / "acceptance_model"
        scaffold_project(project)

        wb = Workbook(project)
        result = wb.run()

        assert result.run_dir.exists()
        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        assert "run_id" in meta
        assert "workbook_spec_hash" in meta

        # Verify the build
        from fin123.verify import verify_run

        run_id = result.run_dir.name
        report = verify_run(project, run_id)
        assert report["status"] == "pass", f"Verify failed: {report}"

    def test_two_builds_produce_identical_exports(self, tmp_path: Path) -> None:
        """Two builds from the same inputs produce identical export hashes."""
        project = tmp_path / "determinism_model"
        scaffold_project(project)

        wb1 = Workbook(project)
        r1 = wb1.run()
        meta1 = json.loads((r1.run_dir / "run_meta.json").read_text())

        wb2 = Workbook(project)
        r2 = wb2.run()
        meta2 = json.loads((r2.run_dir / "run_meta.json").read_text())

        assert meta1["export_hash"] == meta2["export_hash"], (
            f"Export hash drift: {meta1['export_hash']} != {meta2['export_hash']}"
        )


class TestDemoStability:
    """All four built-in demos produce stable output across runs."""

    def test_all_demos_importable(self) -> None:
        """All demo modules can be imported without error."""
        from fin123.demos.ai_governance_demo.run import run_demo as _ag
        from fin123.demos.deterministic_build_demo.run import run_demo as _db
        from fin123.demos.batch_sweep_demo.run import run_demo as _bs
        from fin123.demos.data_guardrails_demo.run import run_demo as _dg

    def test_deterministic_build_demo_stable(self, tmp_path: Path) -> None:
        """Deterministic build demo produces identical output across two runs."""
        from fin123.demos.deterministic_build_demo.run import run_demo

        out1 = tmp_path / "r1"
        out1.mkdir()
        run_demo(output_dir=out1)

        out2 = tmp_path / "r2"
        out2.mkdir()
        run_demo(output_dir=out2)

        assert (out1 / "deterministic_build_summary.json").read_bytes() == (
            out2 / "deterministic_build_summary.json"
        ).read_bytes()

    def test_batch_sweep_demo_distinct_hashes(self, tmp_path: Path) -> None:
        """Batch sweep demo produces 3 distinct scenario export hashes."""
        from fin123.demos.batch_sweep_demo.run import run_demo

        manifest = run_demo(output_dir=tmp_path)
        hashes = [s["export_hash"] for s in manifest["scenarios"]]
        assert len(set(hashes)) == 3


class TestCLISanity:
    """CLI commands execute without error via Click test runner."""

    def test_version_output(self) -> None:
        """fin123 --version prints version string."""
        from click.testing import CliRunner
        from fin123.cli_core import main

        result = CliRunner().invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "fin123" in result.output

    def test_doctor_json(self) -> None:
        """fin123 --json doctor returns valid JSON with checks array."""
        from click.testing import CliRunner
        from fin123.cli_core import main

        result = CliRunner().invoke(main, ["--json", "doctor"])
        # Doctor may exit non-zero if optional dependencies are missing;
        # the important thing is that it returns valid structured JSON.
        data = json.loads(result.output)
        assert "checks" in data.get("data", {})

    def test_template_list_json(self) -> None:
        """fin123 --json template list returns valid JSON."""
        from click.testing import CliRunner
        from fin123.cli_core import main

        result = CliRunner().invoke(main, ["--json", "template", "list"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data.get("ok") is True
