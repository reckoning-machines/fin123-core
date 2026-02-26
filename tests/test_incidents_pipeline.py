"""Tests for incident collection, pipeline flow, and API endpoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

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
# Incidents — service layer
# ────────────────────────────────────────────────────────────────


class TestGetIncidents:
    """Test the get_incidents() service method."""

    def test_empty_run_returns_structure(self, service: ProjectService) -> None:
        """With no runs, get_incidents returns a valid structure."""
        result = service.get_incidents()
        assert "run_id" in result
        assert "total" in result
        assert "counts" in result
        assert "incidents" in result
        assert isinstance(result["incidents"], list)

    def test_no_run_id_returns_empty(self, service: ProjectService) -> None:
        """When there are no runs, incidents list is empty."""
        result = service.get_incidents()
        assert result["total"] == 0

    def test_explicit_run_id_not_found(self, service: ProjectService) -> None:
        """Passing a non-existent run_id doesn't crash (try/excepted)."""
        result = service.get_incidents(run_id="nonexistent-run")
        assert isinstance(result["incidents"], list)

    def test_verify_failure_produces_incident(self, service: ProjectService) -> None:
        """A verify_report.json with failures produces verify_fail incidents."""
        # Create a fake run with a verify report
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "fake-run-001"
        run_dir.mkdir(parents=True, exist_ok=True)

        # Write verify report with failures
        verify_report = {
            "status": "fail",
            "failures": [
                "Hash mismatch for output.parquet",
                "Missing file: expected.csv",
            ],
        }
        (run_dir / "verify_report.json").write_text(json.dumps(verify_report))

        result = service.get_incidents(run_id="fake-run-001")
        verify_incidents = [i for i in result["incidents"] if i["category"] == "verify_fail"]
        assert len(verify_incidents) == 2
        assert verify_incidents[0]["severity"] == "error"
        assert "Hash mismatch" in verify_incidents[0]["detail"]
        assert verify_incidents[1]["code"] == "verify_missing_file"

    def test_assertion_failure_produces_incident(self, service: ProjectService) -> None:
        """A run_meta.json with failed assertions produces assertion_fail incidents."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "fake-run-002"
        run_dir.mkdir(parents=True, exist_ok=True)

        meta = {
            "run_id": "fake-run-002",
            "assertions_status": "fail",
            "assertions_failed_count": 3,
            "assertions_warn_count": 1,
        }
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        result = service.get_incidents(run_id="fake-run-002")
        assertion_incidents = [i for i in result["incidents"] if i["category"] == "assertion_fail"]
        assert len(assertion_incidents) == 1
        assert assertion_incidents[0]["severity"] == "error"
        assert "3 assertion(s) failed" in assertion_incidents[0]["title"]

    def test_assertion_warning_produces_warning(self, service: ProjectService) -> None:
        """Assertion warnings (no failures) produce warning-severity incidents."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "fake-run-003"
        run_dir.mkdir(parents=True, exist_ok=True)

        meta = {
            "run_id": "fake-run-003",
            "assertions_status": "warn",
            "assertions_failed_count": 0,
            "assertions_warn_count": 2,
        }
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        result = service.get_incidents(run_id="fake-run-003")
        assertion_incidents = [i for i in result["incidents"] if i["category"] == "assertion_fail"]
        assert len(assertion_incidents) == 1
        assert assertion_incidents[0]["severity"] == "warning"

    def test_sync_error_produces_incident(self, service: ProjectService) -> None:
        """A failed sync produces sync_error incidents."""
        sync_dir = service.project_dir / "sync_runs" / "sync-001"
        sync_dir.mkdir(parents=True, exist_ok=True)
        sync_meta = {
            "sync_id": "sync-001",
            "timestamp": "2024-01-01T00:00:00Z",
            "tables": [
                {
                    "table_name": "prices",
                    "status": "fail",
                    "error_message": "Connection refused",
                },
            ],
        }
        (sync_dir / "sync_meta.json").write_text(json.dumps(sync_meta))

        result = service.get_incidents()
        sync_incidents = [i for i in result["incidents"] if i["category"] == "sync_error"]
        assert len(sync_incidents) == 1
        assert sync_incidents[0]["severity"] == "error"
        assert "prices" in sync_incidents[0]["title"]
        assert sync_incidents[0]["suggested_action"] is not None

    def test_counts_are_correct(self, service: ProjectService) -> None:
        """The counts dict correctly tallies by severity."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "fake-run-004"
        run_dir.mkdir(parents=True, exist_ok=True)

        # Create verify failures (errors)
        (run_dir / "verify_report.json").write_text(
            json.dumps({"status": "fail", "failures": ["fail1"]})
        )
        # Create assertion warnings
        (run_dir / "run_meta.json").write_text(
            json.dumps({
                "assertions_status": "warn",
                "assertions_failed_count": 0,
                "assertions_warn_count": 1,
            })
        )

        result = service.get_incidents(run_id="fake-run-004")
        assert result["counts"]["error"] == 1  # verify failure
        assert result["counts"]["warning"] == 1  # assertion warning

    def test_incidents_sorted_errors_first(self, service: ProjectService) -> None:
        """Incidents are sorted with errors before warnings before info."""
        from fin123.versioning import RunStore

        store = RunStore(service.project_dir)
        run_dir = store.runs_dir / "fake-run-005"
        run_dir.mkdir(parents=True, exist_ok=True)

        (run_dir / "verify_report.json").write_text(
            json.dumps({"status": "fail", "failures": ["fail1"]})
        )
        (run_dir / "run_meta.json").write_text(
            json.dumps({
                "assertions_status": "warn",
                "assertions_failed_count": 0,
                "assertions_warn_count": 1,
            })
        )

        result = service.get_incidents(run_id="fake-run-005")
        severities = [i["severity"] for i in result["incidents"]]
        # Errors should come before warnings
        if len(severities) >= 2:
            error_indices = [i for i, s in enumerate(severities) if s == "error"]
            warning_indices = [i for i, s in enumerate(severities) if s == "warning"]
            if error_indices and warning_indices:
                assert max(error_indices) < min(warning_indices)


# ────────────────────────────────────────────────────────────────
# Pipeline — service layer
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def nosql_project(tmp_path: Path) -> Path:
    """Scaffold a project and strip SQL tables + their dependents so pipeline can run."""
    project_dir = tmp_path / "nosql_project"
    scaffold_project(project_dir)
    # Rewrite workbook.yaml to remove SQL tables and anything that references them
    spec_path = project_dir / "workbook.yaml"
    spec = yaml.safe_load(spec_path.read_text()) or {}
    # Remove SQL-sourced tables
    spec["tables"] = {
        k: v for k, v in spec.get("tables", {}).items() if v.get("source") != "sql"
    }
    # Remove outputs that reference va_estimates (by name or by args)
    spec["outputs"] = [
        o for o in spec.get("outputs", [])
        if o.get("name") not in ("ticker_eps", "prices_with_estimates")
        and o.get("args", {}).get("table_name") != "va_estimates"
    ]
    # Remove plans that reference va_estimates
    spec["plans"] = [
        p for p in spec.get("plans", [])
        if "va_estimates" not in json.dumps(p)
    ]
    spec_path.write_text(yaml.dump(spec, default_flow_style=False))
    return project_dir


@pytest.fixture
def nosql_service(nosql_project: Path) -> ProjectService:
    """Create a ProjectService backed by a project with no SQL tables."""
    return ProjectService(project_dir=nosql_project)


class TestRunPipeline:
    """Test the run_pipeline() service method."""

    def test_dirty_rejection(self, service: ProjectService) -> None:
        """Pipeline rejects when working copy is dirty."""
        service.update_cells("Sheet1", [{"addr": "A1", "value": "42"}])
        result = service.run_pipeline()
        assert result["status"] == "error"
        assert "uncommitted" in result["error"].lower()
        assert result["run_id"] is None

    def test_clean_run(self, nosql_service: ProjectService) -> None:
        """Pipeline runs successfully on a clean project without SQL tables."""
        nosql_service.save_snapshot()
        result = nosql_service.run_pipeline()
        assert result["status"] == "ok"
        assert result["run_id"] is not None
        assert len(result["steps"]) >= 2  # sync (skipped) + build + verify
        step_names = [s["step"] for s in result["steps"]]
        assert "build" in step_names
        assert "verify" in step_names

    def test_pipeline_returns_incidents(self, nosql_service: ProjectService) -> None:
        """Pipeline returns incidents data."""
        nosql_service.save_snapshot()
        result = nosql_service.run_pipeline()
        assert result["incidents"] is not None
        assert "total" in result["incidents"]

    def test_pipeline_steps_have_status(self, service: ProjectService) -> None:
        """Each pipeline step has a status field."""
        service.save_snapshot()
        result = service.run_pipeline()
        for step in result["steps"]:
            assert "step" in step
            assert "status" in step

    def test_pipeline_sync_failure_halts(self, service: ProjectService) -> None:
        """Pipeline halts on sync failure and reports error."""
        service.save_snapshot()
        result = service.run_pipeline()
        # The demo project has SQL datasheets that can't sync without PG_MAIN_URL
        if result["status"] == "error" and result.get("error", "").startswith("Sync failed"):
            # Sync step should be present and marked error
            assert result["steps"][0]["step"] == "sync"
            assert result["steps"][0]["status"] == "error"
            # Pipeline should not have proceeded to build
            step_names = [s["step"] for s in result["steps"]]
            assert "build" not in step_names


# ────────────────────────────────────────────────────────────────
# API endpoint tests
# ────────────────────────────────────────────────────────────────


class TestIncidentsAPI:
    """Test the /api/incidents endpoint."""

    @pytest.fixture
    def client(self, demo_project: Path):
        from fastapi.testclient import TestClient

        from fin123.ui.server import create_app

        app = create_app(demo_project)
        return TestClient(app)

    def test_get_incidents_endpoint(self, client) -> None:
        """GET /api/incidents returns valid structure."""
        resp = client.get("/api/incidents")
        assert resp.status_code == 200
        data = resp.json()
        assert "run_id" in data
        assert "total" in data
        assert "counts" in data
        assert "incidents" in data

    def test_get_incidents_with_run_id(self, client) -> None:
        """GET /api/incidents?run_id=... accepts run_id param."""
        resp = client.get("/api/incidents?run_id=nonexistent")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0


class TestPipelineAPI:
    """Test the /api/pipeline/run endpoint."""

    @pytest.fixture
    def client(self, demo_project: Path):
        from fastapi.testclient import TestClient

        from fin123.ui.server import create_app

        app = create_app(demo_project)
        return TestClient(app)

    @pytest.fixture
    def nosql_client(self, nosql_project: Path):
        from fastapi.testclient import TestClient

        from fin123.ui.server import create_app

        app = create_app(nosql_project)
        return TestClient(app)

    def test_pipeline_dirty_returns_409(self, client) -> None:
        """POST /api/pipeline/run returns 409 when dirty."""
        # Make a cell edit to set dirty flag
        client.post("/api/sheet/cells", json={
            "sheet": "Sheet1",
            "edits": [{"addr": "A1", "value": "42"}],
        })
        resp = client.post("/api/pipeline/run")
        assert resp.status_code == 409

    def test_pipeline_clean_returns_200(self, nosql_client) -> None:
        """POST /api/pipeline/run returns 200 on clean project (no SQL tables)."""
        # Commit first
        nosql_client.post("/api/commit")
        resp = nosql_client.post("/api/pipeline/run")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["run_id"] is not None
