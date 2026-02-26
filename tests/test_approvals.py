"""Tests for fin123 Approvals v1."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    from fin123.project import scaffold_project

    return scaffold_project(tmp_path / "proj")


def _create_run(project_dir: Path, run_id: str, **extra_meta: Any) -> Path:
    """Helper to create a minimal run directory."""
    run_dir = project_dir / "runs" / run_id
    run_dir.mkdir(parents=True)
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir()
    (outputs_dir / "scalars.json").write_text('{"x": 1}')

    meta: dict[str, Any] = {
        "run_id": run_id,
        "timestamp": "2026-01-01T00:00:00Z",
        "model_id": "test-model",
        "model_version_id": "v0001",
        "scenario_name": "",
        "overlay_hash": "abc",
        "params_hash": "def",
        "effective_params": {"ticker": "AAPL"},
        "plugin_hash": "ghi",
        "input_hashes": {},
    }
    meta.update(extra_meta)
    (run_dir / "run_meta.json").write_text(json.dumps(meta, indent=2))
    return run_dir


def _create_artifact(project_dir: Path, name: str, version: str = "v0001") -> Path:
    """Helper to create a minimal artifact with meta.json."""
    version_dir = project_dir / "artifacts" / name / version
    version_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "artifact_name": name,
        "version": version,
        "created_at": "2026-01-01T00:00:00Z",
        "input_hash": "abc123",
        "workflow_name": "test",
        "status": "completed",
        "model": None,
        "pinned": False,
    }
    (version_dir / "meta.json").write_text(json.dumps(meta, indent=2))
    return version_dir


# ---------------------------------------------------------------------------
# A) Approval object on artifact meta.json
# ---------------------------------------------------------------------------


class TestArtifactApproval:
    """Test approve/reject/status on artifact meta.json."""

    def test_no_approval_key_is_approved(self, project_dir: Path) -> None:
        """Artifact without approval key is treated as approved (backward compat)."""
        from fin123.versioning import ArtifactStore

        _create_artifact(project_dir, "my_art", "v0001")
        store = ArtifactStore(project_dir)
        approval = store.get_artifact_approval("my_art", "v0001")
        assert approval["status"] == "approved"
        assert approval["approved_by"] is None
        assert approval["approved_at"] is None

    def test_approve_artifact(self, project_dir: Path) -> None:
        """Approving an artifact sets status=approved and records metadata."""
        from fin123.versioning import ArtifactStore

        _create_artifact(project_dir, "my_art", "v0001")
        store = ArtifactStore(project_dir)

        result = store.approve_artifact("my_art", "v0001", approved_by="alice", note="LGTM")
        assert result["status"] == "approved"
        assert result["approved_by"] == "alice"
        assert result["approved_at"] is not None
        assert result["note"] == "LGTM"
        assert result["reason_code"] is None

        # Persisted on disk
        meta = json.loads(
            (project_dir / "artifacts" / "my_art" / "v0001" / "meta.json").read_text()
        )
        assert meta["approval"]["status"] == "approved"

    def test_reject_artifact(self, project_dir: Path) -> None:
        """Rejecting an artifact sets status=rejected with reason_code."""
        from fin123.versioning import ArtifactStore

        _create_artifact(project_dir, "my_art", "v0001")
        store = ArtifactStore(project_dir)

        result = store.reject_artifact(
            "my_art", "v0001",
            approved_by="bob",
            note="Data stale",
            reason_code="STALE_DATA",
        )
        assert result["status"] == "rejected"
        assert result["approved_by"] == "bob"
        assert result["reason_code"] == "STALE_DATA"

    def test_approve_idempotent_preserves_timestamp(self, project_dir: Path) -> None:
        """Approving an already-approved artifact preserves original timestamp."""
        from fin123.versioning import ArtifactStore

        _create_artifact(project_dir, "my_art", "v0001")
        store = ArtifactStore(project_dir)

        first = store.approve_artifact("my_art", "v0001", approved_by="alice")
        first_ts = first["approved_at"]

        time.sleep(0.01)  # Ensure clock advances

        second = store.approve_artifact("my_art", "v0001", approved_by="charlie")
        assert second["approved_at"] == first_ts
        assert second["approved_by"] == "alice"  # Original approver preserved

    def test_reject_idempotent_preserves_timestamp(self, project_dir: Path) -> None:
        """Rejecting an already-rejected artifact preserves original timestamp."""
        from fin123.versioning import ArtifactStore

        _create_artifact(project_dir, "my_art", "v0001")
        store = ArtifactStore(project_dir)

        first = store.reject_artifact("my_art", "v0001", approved_by="bob")
        first_ts = first["approved_at"]

        time.sleep(0.01)

        second = store.reject_artifact("my_art", "v0001", approved_by="dave")
        assert second["approved_at"] == first_ts
        assert second["approved_by"] == "bob"

    def test_approve_on_rejected_updates(self, project_dir: Path) -> None:
        """Approving a rejected artifact updates status with a new timestamp."""
        from fin123.versioning import ArtifactStore

        _create_artifact(project_dir, "my_art", "v0001")
        store = ArtifactStore(project_dir)

        rejected = store.reject_artifact("my_art", "v0001", approved_by="bob")
        rejected_ts = rejected["approved_at"]

        time.sleep(0.01)

        approved = store.approve_artifact("my_art", "v0001", approved_by="alice")
        assert approved["status"] == "approved"
        assert approved["approved_at"] != rejected_ts
        assert approved["approved_by"] == "alice"

    def test_reject_on_approved_updates(self, project_dir: Path) -> None:
        """Rejecting an approved artifact updates status with a new timestamp."""
        from fin123.versioning import ArtifactStore

        _create_artifact(project_dir, "my_art", "v0001")
        store = ArtifactStore(project_dir)

        approved = store.approve_artifact("my_art", "v0001", approved_by="alice")
        approved_ts = approved["approved_at"]

        time.sleep(0.01)

        rejected = store.reject_artifact("my_art", "v0001", approved_by="bob", reason_code="BAD")
        assert rejected["status"] == "rejected"
        assert rejected["approved_at"] != approved_ts
        assert rejected["reason_code"] == "BAD"

    def test_nonexistent_artifact_raises(self, project_dir: Path) -> None:
        """Operations on nonexistent artifact raise FileNotFoundError."""
        from fin123.versioning import ArtifactStore

        store = ArtifactStore(project_dir)
        with pytest.raises(FileNotFoundError, match="not found"):
            store.approve_artifact("nope", "v9999")
        with pytest.raises(FileNotFoundError, match="not found"):
            store.reject_artifact("nope", "v9999")
        with pytest.raises(FileNotFoundError, match="not found"):
            store.get_artifact_approval("nope", "v9999")


# ---------------------------------------------------------------------------
# B) Prod-mode release gate
# ---------------------------------------------------------------------------


@pytest.mark.pod
class TestProdModeApprovalGate:
    """Test that prod-mode release blocks on unapproved artifacts."""

    def _enable_prod_mode(self, project_dir: Path) -> None:
        """Set the project to prod mode."""
        (project_dir / "fin123.yaml").write_text("mode: prod\n")

    def _create_run_with_artifacts(
        self,
        project_dir: Path,
        run_id: str,
        artifact_versions_used: dict[str, str],
    ) -> Path:
        """Create a run that references specific artifact versions."""
        run_dir = _create_run(
            project_dir,
            run_id,
            artifact_versions_used=artifact_versions_used,
        )
        # Create verify_report.json with status=pass (required for prod release)
        (run_dir / "verify_report.json").write_text(
            json.dumps({"status": "pass"})
        )
        return run_dir

    def test_no_artifacts_referenced_allows_release(self, project_dir: Path) -> None:
        """Prod release with no artifact_versions_used is allowed."""
        from fin123.releases import ReleaseStore

        self._enable_prod_mode(project_dir)
        run_dir = _create_run(project_dir, "run_001")
        (run_dir / "verify_report.json").write_text(json.dumps({"status": "pass"}))

        store = ReleaseStore(project_dir)
        release = store.create_build_release("run_001")
        assert release["run_id"] == "run_001"

    def test_no_approval_key_allows_release(self, project_dir: Path) -> None:
        """Artifact without approval key (backward compat) allows prod release."""
        from fin123.releases import ReleaseStore

        self._enable_prod_mode(project_dir)
        _create_artifact(project_dir, "estimates", "v0001")
        self._create_run_with_artifacts(
            project_dir, "run_002",
            artifact_versions_used={"estimates": "v0001"},
        )

        store = ReleaseStore(project_dir)
        release = store.create_build_release("run_002")
        assert release["run_id"] == "run_002"

    def test_pending_blocks_release(self, project_dir: Path) -> None:
        """Artifact with approval.status=pending blocks prod release."""
        from fin123.releases import ReleaseStore

        self._enable_prod_mode(project_dir)
        art_dir = _create_artifact(project_dir, "estimates", "v0001")

        # Set approval to pending
        meta_path = art_dir / "meta.json"
        meta = json.loads(meta_path.read_text())
        meta["approval"] = {
            "status": "pending",
            "approved_by": None,
            "approved_at": None,
            "note": "",
            "reason_code": None,
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        self._create_run_with_artifacts(
            project_dir, "run_003",
            artifact_versions_used={"estimates": "v0001"},
        )

        store = ReleaseStore(project_dir)
        with pytest.raises(ValueError, match=r"estimates.*v0001.*pending"):
            store.create_build_release("run_003")

    def test_approved_unblocks_release(self, project_dir: Path) -> None:
        """Approving a pending artifact unblocks prod release."""
        from fin123.releases import ReleaseStore
        from fin123.versioning import ArtifactStore

        self._enable_prod_mode(project_dir)
        art_dir = _create_artifact(project_dir, "estimates", "v0001")

        # Set to pending
        meta_path = art_dir / "meta.json"
        meta = json.loads(meta_path.read_text())
        meta["approval"] = {
            "status": "pending",
            "approved_by": None,
            "approved_at": None,
            "note": "",
            "reason_code": None,
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        self._create_run_with_artifacts(
            project_dir, "run_004",
            artifact_versions_used={"estimates": "v0001"},
        )

        # Blocked before approval
        rel_store = ReleaseStore(project_dir)
        with pytest.raises(ValueError, match="pending"):
            rel_store.create_build_release("run_004")

        # Approve
        art_store = ArtifactStore(project_dir)
        art_store.approve_artifact("estimates", "v0001", approved_by="alice")

        # Now release succeeds
        release = rel_store.create_build_release("run_004")
        assert release["run_id"] == "run_004"

    def test_rejected_blocks_release(self, project_dir: Path) -> None:
        """Artifact with approval.status=rejected blocks prod release."""
        from fin123.releases import ReleaseStore
        from fin123.versioning import ArtifactStore

        self._enable_prod_mode(project_dir)
        _create_artifact(project_dir, "estimates", "v0001")

        art_store = ArtifactStore(project_dir)
        art_store.reject_artifact("estimates", "v0001", approved_by="bob", reason_code="STALE")

        self._create_run_with_artifacts(
            project_dir, "run_005",
            artifact_versions_used={"estimates": "v0001"},
        )

        rel_store = ReleaseStore(project_dir)
        with pytest.raises(ValueError, match=r"estimates.*v0001.*rejected"):
            rel_store.create_build_release("run_005")

    def test_dev_mode_ignores_approval(self, project_dir: Path) -> None:
        """Dev mode does not enforce artifact approval."""
        from fin123.releases import ReleaseStore

        # Default mode is dev
        art_dir = _create_artifact(project_dir, "estimates", "v0001")
        meta_path = art_dir / "meta.json"
        meta = json.loads(meta_path.read_text())
        meta["approval"] = {"status": "pending", "approved_by": None, "approved_at": None, "note": "", "reason_code": None}
        meta_path.write_text(json.dumps(meta, indent=2))

        _create_run(
            project_dir, "run_006",
            artifact_versions_used={"estimates": "v0001"},
        )

        store = ReleaseStore(project_dir)
        release = store.create_build_release("run_006")
        assert release["run_id"] == "run_006"


# ---------------------------------------------------------------------------
# C) CLI commands
# ---------------------------------------------------------------------------


@pytest.mark.pod
class TestApprovalCLI:
    """Test artifact approve/reject/status CLI commands."""

    def test_cli_approve(self, project_dir: Path) -> None:
        from click.testing import CliRunner
        from fin123.cli import main

        _create_artifact(project_dir, "my_art", "v0001")

        runner = CliRunner()
        result = runner.invoke(main, [
            "artifact", "approve", "my_art", "v0001",
            "--project", str(project_dir),
            "--by", "analyst",
            "--note", "Looks good",
        ])
        assert result.exit_code == 0, result.output
        assert "approved" in result.output

    def test_cli_reject(self, project_dir: Path) -> None:
        from click.testing import CliRunner
        from fin123.cli import main

        _create_artifact(project_dir, "my_art", "v0001")

        runner = CliRunner()
        result = runner.invoke(main, [
            "artifact", "reject", "my_art", "v0001",
            "--project", str(project_dir),
            "--by", "reviewer",
            "--note", "Data issue",
            "--reason-code", "DATA_QUALITY",
        ])
        assert result.exit_code == 0, result.output
        assert "rejected" in result.output

    def test_cli_status_no_approval(self, project_dir: Path) -> None:
        from click.testing import CliRunner
        from fin123.cli import main

        _create_artifact(project_dir, "my_art", "v0001")

        runner = CliRunner()
        result = runner.invoke(main, [
            "artifact", "status", "my_art", "v0001",
            "--project", str(project_dir),
        ])
        assert result.exit_code == 0, result.output
        assert "approved" in result.output

    def test_cli_status_after_reject(self, project_dir: Path) -> None:
        from click.testing import CliRunner
        from fin123.cli import main

        _create_artifact(project_dir, "my_art", "v0001")

        runner = CliRunner()
        runner.invoke(main, [
            "artifact", "reject", "my_art", "v0001",
            "--project", str(project_dir),
            "--reason-code", "BAD",
        ])
        result = runner.invoke(main, [
            "artifact", "status", "my_art", "v0001",
            "--project", str(project_dir),
        ])
        assert result.exit_code == 0
        assert "rejected" in result.output
        assert "BAD" in result.output

    def test_cli_nonexistent_artifact(self, project_dir: Path) -> None:
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, [
            "artifact", "approve", "nope", "v9999",
            "--project", str(project_dir),
        ])
        assert result.exit_code != 0
        assert "not found" in result.output
