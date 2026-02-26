"""Phase 4 tests: local browser UI service layer and API endpoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from fin123.project import scaffold_project
from fin123.ui.service import (
    ProjectService,
    col_letter_to_index,
    index_to_col_letter,
    make_addr,
    parse_addr,
)


# ────────────────────────────────────────────────────────────────
# Address helpers
# ────────────────────────────────────────────────────────────────


class TestAddressHelpers:
    def test_col_letter_to_index(self) -> None:
        assert col_letter_to_index("A") == 0
        assert col_letter_to_index("B") == 1
        assert col_letter_to_index("Z") == 25
        assert col_letter_to_index("AA") == 26
        assert col_letter_to_index("AZ") == 51

    def test_index_to_col_letter(self) -> None:
        assert index_to_col_letter(0) == "A"
        assert index_to_col_letter(25) == "Z"
        assert index_to_col_letter(26) == "AA"

    def test_roundtrip(self) -> None:
        for i in range(100):
            assert col_letter_to_index(index_to_col_letter(i)) == i

    def test_parse_addr(self) -> None:
        assert parse_addr("A1") == (0, 0)
        assert parse_addr("B3") == (2, 1)
        assert parse_addr("AA10") == (9, 26)

    def test_make_addr(self) -> None:
        assert make_addr(0, 0) == "A1"
        assert make_addr(2, 1) == "B3"

    def test_parse_addr_invalid(self) -> None:
        with pytest.raises(ValueError):
            parse_addr("123")


# ────────────────────────────────────────────────────────────────
# Service layer
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


class TestServiceBasics:
    def test_get_project_info(self, service: ProjectService) -> None:
        info = service.get_project_info()
        assert "project_dir" in info
        assert info["engine_version"] == "0.1.0"
        assert "Sheet1" in info["sheets"]
        assert isinstance(info["params"], dict)
        assert isinstance(info["dirty"], bool)

    def test_model_version_id_requires_project_dir(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="project_dir"):
            ProjectService(model_version_id="v1234")

    def test_no_project_dir_raises(self) -> None:
        with pytest.raises(ValueError, match="project_dir"):
            ProjectService()

    def test_get_sheet_viewport(self, service: ProjectService) -> None:
        vp = service.get_sheet_viewport("Sheet1", 0, 0, 10, 5)
        assert vp["sheet"] == "Sheet1"
        assert vp["n_rows"] == 200
        assert vp["n_cols"] == 40
        assert isinstance(vp["cells"], list)


class TestCellEditing:
    def test_update_cells_value(self, service: ProjectService) -> None:
        result = service.update_cells("Sheet1", [{"addr": "A1", "value": "42"}])
        assert result["ok"] is True
        assert result["dirty"] is True

        vp = service.get_sheet_viewport("Sheet1", 0, 0, 1, 1)
        cells = {c["addr"]: c for c in vp["cells"]}
        assert "A1" in cells

    def test_update_cells_formula(self, service: ProjectService) -> None:
        service.update_cells("Sheet1", [{"addr": "A1", "value": "10"}])
        result = service.update_cells("Sheet1", [{"addr": "B1", "formula": "=A1 * 2"}])
        assert result["ok"] is True

    def test_update_cells_clear(self, service: ProjectService) -> None:
        service.update_cells("Sheet1", [{"addr": "A1", "value": "10"}])
        service.update_cells("Sheet1", [{"addr": "A1", "value": ""}])
        vp = service.get_sheet_viewport("Sheet1", 0, 0, 1, 1)
        cells = {c["addr"]: c for c in vp["cells"]}
        assert "A1" not in cells

    def test_update_cells_bad_formula(self, service: ProjectService) -> None:
        result = service.update_cells("Sheet1", [{"addr": "A1", "formula": "=+!!bad"}])
        assert result["ok"] is False
        assert len(result["errors"]) == 1

    def test_update_cells_invalid_addr(self, service: ProjectService) -> None:
        result = service.update_cells("Sheet1", [{"addr": "999", "value": "x"}])
        assert result["ok"] is False

    def test_batch_update(self, service: ProjectService) -> None:
        edits = [
            {"addr": "A1", "value": "100"},
            {"addr": "A2", "value": "200"},
            {"addr": "A3", "value": "300"},
        ]
        result = service.update_cells("Sheet1", edits)
        assert result["ok"] is True
        vp = service.get_sheet_viewport("Sheet1", 0, 0, 3, 1)
        assert len(vp["cells"]) == 3

    def test_number_parsing(self, service: ProjectService) -> None:
        """Integer values are stored as int, floats as float."""
        service.update_cells("Sheet1", [
            {"addr": "A1", "value": "42"},
            {"addr": "A2", "value": "3.14"},
            {"addr": "A3", "value": "hello"},
        ])
        sheet = service._get_sheet("Sheet1")
        assert sheet["cells"]["A1"]["value"] == 42
        assert sheet["cells"]["A2"]["value"] == 3.14
        assert sheet["cells"]["A3"]["value"] == "hello"


class TestSnapshotOnSave:
    def test_save_creates_snapshot(self, service: ProjectService) -> None:
        service.update_cells("Sheet1", [{"addr": "A1", "value": "test"}])
        result = service.save_snapshot()
        assert "snapshot_version" in result
        assert result["snapshot_version"].startswith("v")
        assert result["dirty"] is False

    def test_dirty_flag_after_save(self, service: ProjectService) -> None:
        service.update_cells("Sheet1", [{"addr": "A1", "value": "test"}])
        assert service._dirty is True
        service.save_snapshot()
        assert service._dirty is False

    def test_save_updates_workbook_yaml(self, service: ProjectService) -> None:
        service.update_cells("Sheet1", [{"addr": "A1", "value": "42"}])
        service.save_snapshot()
        # Re-read workbook.yaml
        spec = yaml.safe_load((service.project_dir / "workbook.yaml").read_text())
        assert "sheets" in spec
        assert spec["sheets"][0]["cells"]["A1"]["value"] == 42

    def test_snapshot_version_increments(self, service: ProjectService) -> None:
        service.update_cells("Sheet1", [{"addr": "A1", "value": "v1"}])
        r1 = service.save_snapshot()
        service.update_cells("Sheet1", [{"addr": "A1", "value": "v2"}])
        r2 = service.save_snapshot()
        v1 = int(r1["snapshot_version"][1:])
        v2 = int(r2["snapshot_version"][1:])
        assert v2 > v1


class TestRunIntegration:
    def test_run_requires_save(self, service: ProjectService) -> None:
        service.update_cells("Sheet1", [{"addr": "A1", "value": "test"}])
        result = service.run_workbook()
        assert "error" in result

    def test_run_after_save_succeeds(self, service: ProjectService) -> None:
        service.save_snapshot()
        result = service.run_workbook()
        assert "run_id" in result
        assert "scalars" in result

    def test_run_uses_saved_snapshot(self, service: ProjectService) -> None:
        """Run uses the saved snapshot, not the dirty working copy."""
        service.save_snapshot()
        result = service.run_workbook()
        assert result["snapshot_version"] == service._snapshot_version

    def test_full_cycle_edit_save_run_outputs(self, service: ProjectService) -> None:
        """Full cycle: edit -> save -> run -> fetch outputs."""
        service.update_cells("Sheet1", [{"addr": "A1", "value": "hello"}])
        service.save_snapshot()
        run_result = service.run_workbook()
        assert "run_id" in run_result

        scalars = service.get_scalar_outputs()
        assert "scalars" in scalars

        runs = service.list_runs()
        assert len(runs) >= 1


class TestSyncAndWorkflow:
    def test_sync_returns_summary(self, service: ProjectService) -> None:
        result = service.run_sync()
        # Demo project has SQL tables but no DB connection set, so expect errors or skips
        assert isinstance(result, dict)
        assert "synced" in result

    def test_workflow_run(self, service: ProjectService) -> None:
        """Running scenario_sweep workflow should succeed on demo project."""
        service.save_snapshot()
        service.run_workbook()  # Need at least one run first
        result = service.run_workflow("scenario_sweep")
        assert result["status"] == "completed"


class TestOutputFetching:
    def test_list_runs_empty(self, service: ProjectService) -> None:
        runs = service.list_runs()
        # Demo project may have 0 runs initially
        assert isinstance(runs, list)

    def test_list_snapshots(self, service: ProjectService) -> None:
        snaps = service.list_snapshots()
        assert isinstance(snaps, list)

    def test_list_artifacts(self, service: ProjectService) -> None:
        artifacts = service.list_artifacts()
        assert isinstance(artifacts, dict)

    def test_get_table_output_after_run(self, service: ProjectService) -> None:
        service.save_snapshot()
        service.run_workbook()
        result = service.get_table_output("filtered_prices")
        assert "columns" in result
        assert "rows" in result
        assert len(result["rows"]) > 0

    def test_get_table_download_path(self, service: ProjectService) -> None:
        service.save_snapshot()
        service.run_workbook()
        path = service.get_table_download_path("filtered_prices")
        assert path is not None
        assert path.exists()

    def test_get_scalar_outputs_after_run(self, service: ProjectService) -> None:
        service.save_snapshot()
        service.run_workbook()
        result = service.get_scalar_outputs()
        assert "gross_revenue" in result["scalars"]

    def test_no_runs_returns_error(self, service: ProjectService) -> None:
        result = service.get_scalar_outputs()
        # No runs yet
        assert "scalars" in result  # empty dict or error


class TestExistingProjectUnchanged:
    def test_demo_project_still_runs(self) -> None:
        """Existing demo project runs unchanged with Phase 4 code."""
        import tempfile

        from fin123.workbook import Workbook

        with tempfile.TemporaryDirectory() as td:
            proj = Path(td) / "demo"
            scaffold_project(proj)
            wb = Workbook(proj)
            result = wb.run()
            assert result.scalars["gross_revenue"] == 125000.0


# ────────────────────────────────────────────────────────────────
# FastAPI endpoint tests
# ────────────────────────────────────────────────────────────────


class TestAPIEndpoints:
    """Test FastAPI endpoints using TestClient."""

    @pytest.fixture
    def client(self, demo_project: Path):
        from fastapi.testclient import TestClient

        from fin123.ui.server import create_app

        app = create_app(demo_project)
        return TestClient(app)

    def test_root_returns_html(self, client) -> None:
        resp = client.get("/")
        assert resp.status_code == 200
        assert "fin123" in resp.text

    def test_get_project(self, client) -> None:
        resp = client.get("/api/project")
        assert resp.status_code == 200
        data = resp.json()
        assert "sheets" in data
        assert "engine_version" in data

    def test_get_sheet(self, client) -> None:
        resp = client.get("/api/sheet?sheet=Sheet1&r0=0&c0=0&rows=10&cols=5")
        assert resp.status_code == 200
        data = resp.json()
        assert data["sheet"] == "Sheet1"

    def test_update_cells(self, client) -> None:
        resp = client.post("/api/sheet/cells", json={
            "sheet": "Sheet1",
            "edits": [{"addr": "A1", "value": "42"}],
        })
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        assert resp.json()["dirty"] is True

    def test_save(self, client) -> None:
        client.post("/api/sheet/cells", json={
            "sheet": "Sheet1",
            "edits": [{"addr": "A1", "value": "test"}],
        })
        resp = client.post("/api/save")
        assert resp.status_code == 200
        assert "snapshot_version" in resp.json()

    def test_run_requires_save(self, client) -> None:
        client.post("/api/sheet/cells", json={
            "sheet": "Sheet1",
            "edits": [{"addr": "A1", "value": "test"}],
        })
        resp = client.post("/api/run")
        assert resp.status_code == 409

    def test_save_then_run(self, client) -> None:
        client.post("/api/save")
        resp = client.post("/api/run")
        assert resp.status_code == 200
        assert "run_id" in resp.json()

    def test_list_runs(self, client) -> None:
        resp = client.get("/api/runs?limit=5")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_get_scalars_after_run(self, client) -> None:
        client.post("/api/save")
        client.post("/api/run")
        resp = client.get("/api/outputs/scalars")
        assert resp.status_code == 200
        assert "scalars" in resp.json()

    def test_get_table_after_run(self, client) -> None:
        client.post("/api/save")
        client.post("/api/run")
        resp = client.get("/api/outputs/table?name=filtered_prices")
        assert resp.status_code == 200
        data = resp.json()
        assert "columns" in data
        assert "rows" in data

    def test_get_snapshots(self, client) -> None:
        resp = client.get("/api/snapshots")
        assert resp.status_code == 200

    def test_get_artifacts(self, client) -> None:
        resp = client.get("/api/artifacts")
        assert resp.status_code == 200

    def test_sync(self, client) -> None:
        resp = client.post("/api/sync")
        assert resp.status_code == 200

    def test_workflow_run(self, client) -> None:
        client.post("/api/save")
        client.post("/api/run")
        resp = client.post("/api/workflow/run", json={"workflow_name": "scenario_sweep"})
        assert resp.status_code == 200

    def test_static_files_served(self, client) -> None:
        resp = client.get("/static/styles.css")
        assert resp.status_code == 200
        assert "var(--bg)" in resp.text

    def test_table_not_found(self, client) -> None:
        client.post("/api/save")
        client.post("/api/run")
        resp = client.get("/api/outputs/table?name=no_such_table")
        assert resp.status_code == 404
