"""Tests for Demos 2, 3, and 4."""

from __future__ import annotations

import json
from pathlib import Path


class TestDeterministicBuildDemo:
    """Test Demo 2 -- deterministic build."""

    def test_output_no_run_id(self, tmp_path: Path, capsys: ...) -> None:
        """Console output does not contain run_id or 'Build saved to'."""
        from demos.deterministic_build_demo.run import run_demo

        run_demo(output_dir=tmp_path)
        captured = capsys.readouterr().out
        assert "_run_" not in captured
        assert "Build saved to" not in captured

    def test_summary_no_timestamps(self, tmp_path: Path) -> None:
        """Summary JSON contains no timestamps or run_ids."""
        from demos.deterministic_build_demo.run import run_demo

        run_demo(output_dir=tmp_path)
        text = (tmp_path / "deterministic_build_summary.json").read_text()
        assert "_run_" not in text
        assert "timestamp" not in text
        assert "model_version_id" not in text

    def test_summary_stable_across_runs(self, tmp_path: Path) -> None:
        """Summary JSON is byte-for-byte identical across two runs."""
        from demos.deterministic_build_demo.run import run_demo

        out1 = tmp_path / "r1"
        out1.mkdir()
        run_demo(output_dir=out1)

        out2 = tmp_path / "r2"
        out2.mkdir()
        run_demo(output_dir=out2)

        assert (out1 / "deterministic_build_summary.json").read_bytes() == (
            out2 / "deterministic_build_summary.json"
        ).read_bytes()


class TestBatchSweepDemo:
    """Test Demo 3 -- batch sweep."""

    def test_all_export_hashes_distinct(self, tmp_path: Path) -> None:
        """The three scenarios produce distinct export hashes."""
        from demos.batch_sweep_demo.run import run_demo

        manifest = run_demo(output_dir=tmp_path)
        hashes = [s["export_hash"] for s in manifest["scenarios"]]
        assert len(set(hashes)) == 3, f"Expected 3 distinct hashes, got {hashes}"

    def test_manifest_stable_across_runs(self, tmp_path: Path) -> None:
        """Manifest JSON is byte-for-byte identical across two runs."""
        from demos.batch_sweep_demo.run import run_demo

        out1 = tmp_path / "r1"
        out1.mkdir()
        run_demo(output_dir=out1)

        out2 = tmp_path / "r2"
        out2.mkdir()
        run_demo(output_dir=out2)

        assert (out1 / "batch_manifest.json").read_bytes() == (
            out2 / "batch_manifest.json"
        ).read_bytes()


class TestDataGuardrailsDemo:
    """Test Demo 4 -- data guardrails."""

    def test_failure_and_success_stable(self, tmp_path: Path) -> None:
        """Both failure and success JSON outputs are stable across two runs."""
        from demos.data_guardrails_demo.run import run_demo

        out1 = tmp_path / "r1"
        out1.mkdir()
        run_demo(output_dir=out1)

        out2 = tmp_path / "r2"
        out2.mkdir()
        run_demo(output_dir=out2)

        assert (out1 / "guardrails_failure.json").read_bytes() == (
            out2 / "guardrails_failure.json"
        ).read_bytes()
        assert (out1 / "guardrails_success.json").read_bytes() == (
            out2 / "guardrails_success.json"
        ).read_bytes()

    def test_failure_has_expected_structure(self, tmp_path: Path) -> None:
        """Failure JSON has the expected error structure."""
        from demos.data_guardrails_demo.run import run_demo

        result = run_demo(output_dir=tmp_path)
        failure = result["failure"]
        assert failure["status"] == "expected_failure"
        assert failure["error_type"] == "ValueError"
        assert "many_to_one" in failure["error_message"]

    def test_success_has_export_hash(self, tmp_path: Path) -> None:
        """Success JSON has a non-empty export hash."""
        from demos.data_guardrails_demo.run import run_demo

        result = run_demo(output_dir=tmp_path)
        success = result["success"]
        assert success["status"] == "pass"
        assert len(success["export_hash"]) == 64
