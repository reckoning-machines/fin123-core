"""Tests for Demo 1 -- AI Governance."""

from __future__ import annotations

import json
from pathlib import Path

from demos.ai_governance_demo.run import compute_deterministic_hash, run_demo


class TestAIGovernanceDemo:
    """Test AI governance demo determinism and correctness."""

    def test_compliance_report_fields(self, tmp_path: Path) -> None:
        """Demo produces a compliance report with required fields."""
        report = run_demo(output_dir=tmp_path)

        assert report["validation_status"] == "PASS"
        assert report["ai_generated"] is True
        assert report["compliance_status"] == "APPROVED"
        assert report["artifact_version"] == "v1"
        assert report["plugin_version"] == 1
        assert report["generated_by"] == "ai"
        assert report["model"] == "demo-llm"
        assert report["prompt_hash"] == "demo_prompt_hash_v1"
        assert isinstance(report["deterministic_build_hash"], str)
        assert len(report["deterministic_build_hash"]) == 64  # SHA-256 hex

    def test_plugin_actually_executed(self, tmp_path: Path) -> None:
        """Plugin function is executed via engine build and produces correct output."""
        report = run_demo(output_dir=tmp_path)

        # ai_scale_revenue(100000.0, 1.08) = 108000.0
        assert report["plugin_output_value"] == 108000.0
        assert report["export_hash"] != ""

        # Verify the engine_build_executed policy check is present
        checks = {c["check"]: c["status"] for c in report["policy_checks"]}
        assert checks.get("engine_build_executed") == "PASS"

    def test_deterministic_hash_stable_across_runs(self, tmp_path: Path) -> None:
        """Two runs produce identical deterministic build hashes."""
        out1 = tmp_path / "run1"
        out1.mkdir()
        report1 = run_demo(output_dir=out1)

        out2 = tmp_path / "run2"
        out2.mkdir()
        report2 = run_demo(output_dir=out2)

        assert report1["deterministic_build_hash"] == report2["deterministic_build_hash"]
        assert report1["export_hash"] == report2["export_hash"]

    def test_output_files_written(self, tmp_path: Path) -> None:
        """Demo writes all expected output files."""
        run_demo(output_dir=tmp_path)

        assert (tmp_path / "ai_generated_plugin_example.py").exists()
        assert (tmp_path / "compliance_report_output.json").exists()
        assert (tmp_path / "artifact_manifest.json").exists()

    def test_output_json_stable(self, tmp_path: Path) -> None:
        """Output JSON files are byte-for-byte identical across runs."""
        out1 = tmp_path / "run1"
        out1.mkdir()
        run_demo(output_dir=out1)

        out2 = tmp_path / "run2"
        out2.mkdir()
        run_demo(output_dir=out2)

        for name in ("compliance_report_output.json", "artifact_manifest.json"):
            assert (out1 / name).read_bytes() == (out2 / name).read_bytes(), (
                f"{name} not stable across runs"
            )

    def test_policy_checks_all_pass(self, tmp_path: Path) -> None:
        """All policy checks in the compliance report are PASS."""
        report = run_demo(output_dir=tmp_path)

        for check in report["policy_checks"]:
            assert check["status"] == "PASS", f"Check {check['check']} failed"

    def test_compute_deterministic_hash_stable(self) -> None:
        """compute_deterministic_hash is stable for identical input."""
        d = {"a": 1, "b": "hello", "nested": {"x": True}}
        h1 = compute_deterministic_hash(d)
        h2 = compute_deterministic_hash(d)
        assert h1 == h2
        assert len(h1) == 64
