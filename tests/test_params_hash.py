"""Tests for params_hash computation, determinism, and verification."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# A) Determinism
# ---------------------------------------------------------------------------


class TestParamsHash:
    def test_deterministic(self):
        from fin123.utils.hash import compute_params_hash

        h1 = compute_params_hash({"ticker": "AAPL", "rate": 0.05})
        h2 = compute_params_hash({"rate": 0.05, "ticker": "AAPL"})
        assert h1 == h2
        assert len(h1) == 64

    def test_different_params_different_hash(self):
        from fin123.utils.hash import compute_params_hash

        h1 = compute_params_hash({"ticker": "AAPL"})
        h2 = compute_params_hash({"ticker": "MSFT"})
        assert h1 != h2

    def test_empty_params(self):
        from fin123.utils.hash import compute_params_hash

        h = compute_params_hash({})
        assert len(h) == 64

    def test_differs_from_overlay_hash(self):
        from fin123.utils.hash import compute_params_hash, overlay_hash

        params = {"ticker": "AAPL", "rate": 0.05}
        ph = compute_params_hash(params)
        oh = overlay_hash("", params)
        assert ph != oh


# ---------------------------------------------------------------------------
# B) Verify checks params_hash
# ---------------------------------------------------------------------------


class TestVerifyParamsHash:
    @pytest.fixture
    def project_dir(self, tmp_path: Path) -> Path:
        from fin123.project import scaffold_project

        return scaffold_project(tmp_path / "proj")

    def test_verify_passes_with_correct_params_hash(self, project_dir):
        from fin123.utils.hash import compute_params_hash
        from fin123.verify import verify_run

        effective_params = {"ticker": "AAPL", "rate": 0.05}
        params_hash = compute_params_hash(effective_params)

        run_dir = project_dir / "runs" / "test_run"
        run_dir.mkdir(parents=True)
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text('{"x": 1}')

        meta = {
            "run_id": "test_run",
            "params_hash": params_hash,
            "effective_params": effective_params,
        }
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        report = verify_run(project_dir, "test_run")
        # Should not have params_hash mismatch
        assert not any("params_hash" in f for f in report["failures"])

    def test_verify_fails_with_tampered_params_hash(self, project_dir):
        from fin123.verify import verify_run

        run_dir = project_dir / "runs" / "test_run"
        run_dir.mkdir(parents=True)
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text('{"x": 1}')

        meta = {
            "run_id": "test_run",
            "params_hash": "0000000000000000000000000000000000000000000000000000000000000000",
            "effective_params": {"ticker": "AAPL"},
        }
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        report = verify_run(project_dir, "test_run")
        assert any("params_hash" in f for f in report["failures"])

    def test_verify_skips_when_no_params_hash(self, project_dir):
        from fin123.verify import verify_run

        run_dir = project_dir / "runs" / "test_run"
        run_dir.mkdir(parents=True)
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text('{"x": 1}')

        meta = {"run_id": "test_run"}
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        report = verify_run(project_dir, "test_run")
        # Should not mention params_hash in failures
        assert not any("params_hash" in f for f in report["failures"])
