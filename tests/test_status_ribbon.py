"""Tests for model status ribbon and latest table output endpoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from fin123.project import scaffold_project
from fin123.ui.service import ProjectService


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Scaffold a demo project for testing."""
    project_dir = tmp_path / "test_project"
    scaffold_project(project_dir)
    return project_dir


@pytest.fixture
def service(demo_project: Path) -> ProjectService:
    """Create a ProjectService for testing."""
    return ProjectService(project_dir=demo_project)


# ────────────────────────────────────────────────────────────────
# get_model_status — structure & base cases
# ────────────────────────────────────────────────────────────────


class TestGetModelStatus:
    """Test the get_model_status() service method."""

    def test_returns_all_sections(self, service: ProjectService) -> None:
        """Status response contains project, datasheets, build, verify."""
        result = service.get_model_status()
        assert "project" in result
        assert "datasheets" in result
        assert "build" in result
        assert "verify" in result

    def test_project_section_fields(self, service: ProjectService) -> None:
        """Project section has dirty, model_id, model_version_id, read_only."""
        result = service.get_model_status()
        p = result["project"]
        assert "dirty" in p
        assert "model_id" in p
        assert "model_version_id" in p
        assert "read_only" in p
        assert p["dirty"] is False
        assert p["read_only"] is False

    def test_dirty_after_edit(self, service: ProjectService) -> None:
        """After editing a cell, project section reports dirty=True."""
        service.update_cells("Sheet1", [{"addr": "A1", "value": "42"}])
        result = service.get_model_status()
        assert result["project"]["dirty"] is True

    def test_no_builds_section(self, service: ProjectService) -> None:
        """Before any build, build section reports has_build=False."""
        result = service.get_model_status()
        b = result["build"]
        assert b["has_build"] is False
        assert b["run_id"] is None
        assert b["status"] is None

    def test_verify_unknown_no_builds(self, service: ProjectService) -> None:
        """Before any build, verify section reports status=unknown."""
        result = service.get_model_status()
        v = result["verify"]
        assert v["status"] == "unknown"

    def test_datasheets_section_counts(self, service: ProjectService) -> None:
        """Datasheets section has summary_status, counts, stale_tables."""
        result = service.get_model_status()
        ds = result["datasheets"]
        assert "summary_status" in ds
        assert "counts" in ds
        assert "stale_tables" in ds
        assert isinstance(ds["counts"], dict)
        assert isinstance(ds["stale_tables"], list)

    def test_build_with_run_meta(self, service: ProjectService) -> None:
        """A run_meta.json makes build section report has_build=True."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "status-run-001"
        run_dir.mkdir(parents=True, exist_ok=True)

        meta = {
            "run_id": "status-run-001",
            "timestamp": "2024-06-15T10:30:00Z",
            "timings_ms": {"build": 500, "write": 200},
            "assertions_status": "pass",
            "assertions_failed_count": 0,
        }
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        result = service.get_model_status()
        b = result["build"]
        assert b["has_build"] is True
        assert b["run_id"] == "status-run-001"
        assert b["built_at"] == "2024-06-15T10:30:00Z"
        assert b["duration_ms"] == 700
        assert b["status"] == "ok"

    def test_build_failed_assertions(self, service: ProjectService) -> None:
        """Failed assertions make build status=fail."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "status-run-002"
        run_dir.mkdir(parents=True, exist_ok=True)

        meta = {
            "run_id": "status-run-002",
            "assertions_status": "fail",
            "assertions_failed_count": 2,
        }
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        result = service.get_model_status()
        assert result["build"]["status"] == "fail"

    def test_verify_pass(self, service: ProjectService) -> None:
        """A verify_report.json with status=pass reports correctly."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "status-run-003"
        run_dir.mkdir(parents=True, exist_ok=True)

        (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "status-run-003"}))
        (run_dir / "verify_report.json").write_text(
            json.dumps({"status": "pass", "checked_at": "2024-06-15T11:00:00Z"})
        )

        result = service.get_model_status()
        v = result["verify"]
        assert v["status"] == "pass"
        assert v["checked_at"] == "2024-06-15T11:00:00Z"

    def test_verify_fail(self, service: ProjectService) -> None:
        """A verify_report.json with status=fail reports correctly."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "status-run-004"
        run_dir.mkdir(parents=True, exist_ok=True)

        (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "status-run-004"}))
        (run_dir / "verify_report.json").write_text(
            json.dumps({"status": "fail", "failures": ["hash mismatch"]})
        )

        result = service.get_model_status()
        assert result["verify"]["status"] == "fail"


# ────────────────────────────────────────────────────────────────
# get_latest_table_output_name
# ────────────────────────────────────────────────────────────────


class TestGetLatestTableOutput:
    """Test the get_latest_table_output_name() service method."""

    def test_no_runs_returns_error(self, service: ProjectService) -> None:
        """With no runs, returns error dict."""
        result = service.get_latest_table_output_name()
        assert "error" in result

    def test_no_outputs_dir_returns_error(self, service: ProjectService) -> None:
        """A run without outputs directory returns error."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "table-run-001"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "table-run-001"}))

        result = service.get_latest_table_output_name(run_id="table-run-001")
        assert "error" in result

    def test_default_selection(self, service: ProjectService) -> None:
        """Without primary_table hint, picks first alphabetically."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "table-run-002"
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)

        # Create fake parquet files
        (outputs_dir / "beta_output.parquet").write_bytes(b"fake")
        (outputs_dir / "alpha_output.parquet").write_bytes(b"fake")
        (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "table-run-002"}))

        result = service.get_latest_table_output_name(run_id="table-run-002")
        assert result["table_name"] == "alpha_output"
        assert result["run_id"] == "table-run-002"
        assert "download_url" in result

    def test_primary_table_hint(self, service: ProjectService) -> None:
        """When ui.primary_table is set, that table is preferred."""
        import yaml

        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "table-run-003"
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)

        (outputs_dir / "alpha.parquet").write_bytes(b"fake")
        (outputs_dir / "beta.parquet").write_bytes(b"fake")
        (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "table-run-003"}))

        # Set primary_table hint in workbook.yaml
        spec_path = service.project_dir / "workbook.yaml"
        spec = yaml.safe_load(spec_path.read_text()) or {}
        spec.setdefault("ui", {})["primary_table"] = "beta"
        spec_path.write_text(yaml.dump(spec, default_flow_style=False))

        # Reload service to pick up spec change
        svc = ProjectService(project_dir=service.project_dir)
        result = svc.get_latest_table_output_name(run_id="table-run-003")
        assert result["table_name"] == "beta"

    def test_ignores_internal_files(self, service: ProjectService) -> None:
        """Files starting with _ are excluded from output list."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "table-run-004"
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)

        (outputs_dir / "_internal.parquet").write_bytes(b"fake")
        (outputs_dir / "visible.parquet").write_bytes(b"fake")
        (run_dir / "run_meta.json").write_text(json.dumps({"run_id": "table-run-004"}))

        result = service.get_latest_table_output_name(run_id="table-run-004")
        assert result["table_name"] == "visible"


# ────────────────────────────────────────────────────────────────
# API endpoint tests
# ────────────────────────────────────────────────────────────────


class TestStatusAPI:
    """Test the /api/status endpoint."""

    @pytest.fixture
    def client(self, demo_project: Path):
        from fastapi.testclient import TestClient

        from fin123.ui.server import create_app

        app = create_app(demo_project)
        return TestClient(app)

    def test_get_status_returns_200(self, client) -> None:
        """GET /api/status returns 200 with all sections."""
        resp = client.get("/api/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "project" in data
        assert "datasheets" in data
        assert "build" in data
        assert "verify" in data

    def test_status_dirty_after_edit(self, client) -> None:
        """GET /api/status reflects dirty state after cell edit."""
        client.post("/api/sheet/cells", json={
            "sheet": "Sheet1",
            "edits": [{"addr": "A1", "value": "test"}],
        })
        resp = client.get("/api/status")
        assert resp.json()["project"]["dirty"] is True


class TestLatestTableAPI:
    """Test the /api/run/latest/table endpoint."""

    @pytest.fixture
    def client(self, demo_project: Path):
        from fastapi.testclient import TestClient

        from fin123.ui.server import create_app

        app = create_app(demo_project)
        return TestClient(app)

    def test_no_runs_returns_404(self, client) -> None:
        """GET /api/run/latest/table returns 404 when no runs exist."""
        resp = client.get("/api/run/latest/table")
        assert resp.status_code == 404

    def test_nonexistent_run_id_returns_404(self, client) -> None:
        """GET /api/run/latest/table?run_id=fake returns 404."""
        resp = client.get("/api/run/latest/table?run_id=fake-nonexistent")
        assert resp.status_code == 404
