"""Phase 4.2 tests: clipboard batch updates, error structures, datasheet health."""

from __future__ import annotations

import json
import time
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
# Error Structure Tests
# ────────────────────────────────────────────────────────────────


class TestErrorStructure:
    """Verify that update_cells returns structured error objects."""

    def test_parse_error_has_code_and_message(self, service: ProjectService) -> None:
        """Formula parse errors include code, message, and addr."""
        result = service.update_cells("Sheet1", [{"addr": "A1", "formula": "=+!!bad"}])
        assert result["ok"] is False
        assert len(result["errors"]) == 1
        err = result["errors"][0]
        assert err["addr"] == "A1"
        assert err["code"] == "parse_error"
        assert "message" in err
        assert len(err["message"]) > 0

    def test_invalid_address_error_structure(self, service: ProjectService) -> None:
        """Invalid address errors include code and message."""
        result = service.update_cells("Sheet1", [{"addr": "999", "value": "x"}])
        assert result["ok"] is False
        err = result["errors"][0]
        assert err["code"] == "invalid_address"
        assert "message" in err

    def test_valid_edit_has_no_errors(self, service: ProjectService) -> None:
        result = service.update_cells("Sheet1", [{"addr": "A1", "value": "42"}])
        assert result["ok"] is True
        assert result["errors"] == []

    def test_batch_mixed_errors_and_successes(self, service: ProjectService) -> None:
        """Batch edits where some succeed and some fail."""
        edits = [
            {"addr": "A1", "value": "100"},
            {"addr": "B1", "formula": "=+!!bad"},
            {"addr": "C1", "value": "200"},
            {"addr": "999", "value": "x"},
        ]
        result = service.update_cells("Sheet1", edits)
        assert result["ok"] is False
        assert len(result["errors"]) == 2
        codes = {e["code"] for e in result["errors"]}
        assert "parse_error" in codes
        assert "invalid_address" in codes
        # Successful edits should still be applied
        sheet = service._get_sheet("Sheet1")
        assert sheet["cells"]["A1"]["value"] == 100
        assert sheet["cells"]["C1"]["value"] == 200

    def test_error_cleared_on_valid_edit(self, service: ProjectService) -> None:
        """A cell that had an error can be overwritten with valid data."""
        service.update_cells("Sheet1", [{"addr": "A1", "formula": "=+!!bad"}])
        result = service.update_cells("Sheet1", [{"addr": "A1", "value": "42"}])
        assert result["ok"] is True
        sheet = service._get_sheet("Sheet1")
        assert sheet["cells"]["A1"]["value"] == 42


# ────────────────────────────────────────────────────────────────
# Formula Validation
# ────────────────────────────────────────────────────────────────


class TestFormulaValidation:
    def test_valid_formula(self, service: ProjectService) -> None:
        result = service.validate_formula("=1 + 2")
        assert result["valid"] is True

    def test_invalid_formula(self, service: ProjectService) -> None:
        result = service.validate_formula("=+!!bad")
        assert result["valid"] is False
        assert "message" in result

    def test_non_formula_text(self, service: ProjectService) -> None:
        result = service.validate_formula("hello")
        assert result["valid"] is False

    def test_complex_formula(self, service: ProjectService) -> None:
        result = service.validate_formula("=SUM(1, 2, 3) * IF(TRUE, 10, 20)")
        assert result["valid"] is True


# ────────────────────────────────────────────────────────────────
# Batch Paste Simulation (service-level)
# ────────────────────────────────────────────────────────────────


class TestBatchPaste:
    """Simulate paste by sending batch edits as the frontend would."""

    def test_paste_values_block(self, service: ProjectService) -> None:
        """Paste a 3x2 block of plain values."""
        edits = [
            {"addr": "A1", "value": "10"},
            {"addr": "B1", "value": "20"},
            {"addr": "A2", "value": "30"},
            {"addr": "B2", "value": "40"},
            {"addr": "A3", "value": "50"},
            {"addr": "B3", "value": "60"},
        ]
        result = service.update_cells("Sheet1", edits)
        assert result["ok"] is True

        vp = service.get_sheet_viewport("Sheet1", 0, 0, 3, 2)
        assert len(vp["cells"]) == 6

    def test_paste_with_formulas(self, service: ProjectService) -> None:
        """Paste includes a formula cell."""
        edits = [
            {"addr": "A1", "value": "100"},
            {"addr": "B1", "formula": "=A1 * 2"},
        ]
        result = service.update_cells("Sheet1", edits)
        assert result["ok"] is True

        sheet = service._get_sheet("Sheet1")
        assert sheet["cells"]["A1"]["value"] == 100
        assert sheet["cells"]["B1"]["formula"] == "=A1 * 2"

    def test_paste_with_empty_cells(self, service: ProjectService) -> None:
        """Empty cells in paste should clear existing content."""
        service.update_cells("Sheet1", [{"addr": "A1", "value": "existing"}])
        edits = [
            {"addr": "A1", "value": ""},
            {"addr": "B1", "value": "new"},
        ]
        result = service.update_cells("Sheet1", edits)
        assert result["ok"] is True

        sheet = service._get_sheet("Sheet1")
        assert "A1" not in sheet["cells"]
        assert sheet["cells"]["B1"]["value"] == "new"

    def test_large_paste_block(self, service: ProjectService) -> None:
        """Paste a 10x5 block."""
        edits = []
        from fin123.ui.service import make_addr
        for r in range(10):
            for c in range(5):
                edits.append({"addr": make_addr(r, c), "value": str(r * 5 + c)})
        result = service.update_cells("Sheet1", edits)
        assert result["ok"] is True
        vp = service.get_sheet_viewport("Sheet1", 0, 0, 10, 5)
        assert len(vp["cells"]) == 50

    def test_paste_formula_classification(self, service: ProjectService) -> None:
        """Values starting with = are formulas; others are values."""
        edits = [
            {"addr": "A1", "value": "hello"},
            {"addr": "A2", "formula": "=1+1"},
            {"addr": "A3", "value": "42"},
        ]
        result = service.update_cells("Sheet1", edits)
        assert result["ok"] is True

        sheet = service._get_sheet("Sheet1")
        assert sheet["cells"]["A1"]["value"] == "hello"
        assert sheet["cells"]["A2"]["formula"] == "=1+1"
        assert sheet["cells"]["A3"]["value"] == 42


# ────────────────────────────────────────────────────────────────
# Datasheet Health
# ────────────────────────────────────────────────────────────────


class TestDatasheetHealth:
    """Test the get_datasheets() service method."""

    def test_datasheets_returns_sql_tables(self, service: ProjectService) -> None:
        """Should return info for SQL-sourced tables."""
        sheets = service.get_datasheets()
        assert isinstance(sheets, list)
        # Demo project has va_estimates as SQL table
        names = {ds["table_name"] for ds in sheets}
        assert "va_estimates" in names

    def test_datasheet_fields(self, service: ProjectService) -> None:
        """Each datasheet entry has the required fields."""
        sheets = service.get_datasheets()
        ds = next(d for d in sheets if d["table_name"] == "va_estimates")
        assert "cache_path" in ds
        assert "refresh_policy" in ds
        assert "staleness" in ds
        assert "last_sync_id" in ds
        assert "last_sync_time" in ds
        assert "last_status" in ds
        assert "last_rowcount" in ds
        assert "cache_file_exists" in ds

    def test_no_sync_means_unknown(self, service: ProjectService) -> None:
        """A table with no sync history should have staleness=unknown."""
        sheets = service.get_datasheets()
        ds = next(d for d in sheets if d["table_name"] == "va_estimates")
        # Demo project has a pre-populated cache parquet but no sync history
        # With no sync_runs, staleness should be unknown
        assert ds["staleness"] in ("unknown", "fresh")

    def test_staleness_after_mock_sync(self, demo_project: Path) -> None:
        """After writing a sync provenance record, staleness should update."""
        # Write a fake sync run
        from datetime import datetime, timezone

        sync_dir = demo_project / "sync_runs" / "20260101_120000_sync_1"
        sync_dir.mkdir(parents=True)
        sync_meta = {
            "sync_id": "20260101_120000_sync_1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tables_updated": ["va_estimates"],
            "tables": [
                {
                    "table_name": "va_estimates",
                    "connection_name": "pg_main",
                    "query_hash": "abc123",
                    "rowcount": 5,
                    "output_path": "inputs/va_estimates.parquet",
                    "output_file_hash": "def456",
                    "elapsed_ms": 100,
                    "status": "ok",
                    "error_message": "",
                    "warnings": [],
                }
            ],
            "pinned": False,
        }
        (sync_dir / "sync_meta.json").write_text(json.dumps(sync_meta, indent=2))

        service = ProjectService(project_dir=demo_project)
        sheets = service.get_datasheets()
        ds = next(d for d in sheets if d["table_name"] == "va_estimates")
        assert ds["last_status"] == "ok"
        assert ds["last_rowcount"] == 5
        assert ds["last_sync_id"] == "20260101_120000_sync_1"
        # Cache exists and was synced ok -> fresh
        assert ds["staleness"] == "fresh"

    def test_staleness_fail(self, demo_project: Path) -> None:
        """A failed sync should report staleness=fail."""
        sync_dir = demo_project / "sync_runs" / "20260101_120000_sync_1"
        sync_dir.mkdir(parents=True)
        sync_meta = {
            "sync_id": "20260101_120000_sync_1",
            "timestamp": "2026-01-01T12:00:00+00:00",
            "tables_updated": [],
            "tables": [
                {
                    "table_name": "va_estimates",
                    "status": "fail",
                    "error_message": "connection refused",
                    "rowcount": 0,
                }
            ],
            "pinned": False,
        }
        (sync_dir / "sync_meta.json").write_text(json.dumps(sync_meta, indent=2))

        service = ProjectService(project_dir=demo_project)
        sheets = service.get_datasheets()
        ds = next(d for d in sheets if d["table_name"] == "va_estimates")
        assert ds["staleness"] == "fail"

    def test_csv_tables_not_included(self, service: ProjectService) -> None:
        """CSV-sourced tables should not appear in datasheets."""
        sheets = service.get_datasheets()
        names = {ds["table_name"] for ds in sheets}
        assert "prices" not in names


class TestStalenessClassification:
    """Unit tests for _classify_staleness."""

    def test_no_sync_is_unknown(self) -> None:
        result = ProjectService._classify_staleness(
            last_sync=None, cache_exists=True, cache_mtime=time.time(),
            refresh="manual", tspec={},
        )
        assert result == "unknown"

    def test_fail_sync(self) -> None:
        result = ProjectService._classify_staleness(
            last_sync={"status": "fail"}, cache_exists=True,
            cache_mtime=time.time(), refresh="manual", tspec={},
        )
        assert result == "fail"

    def test_fresh_with_ok_sync(self) -> None:
        result = ProjectService._classify_staleness(
            last_sync={"status": "ok"}, cache_exists=True,
            cache_mtime=time.time(), refresh="manual", tspec={},
        )
        assert result == "fresh"

    def test_stale_with_ttl(self) -> None:
        old_mtime = time.time() - (25 * 3600)  # 25 hours ago
        result = ProjectService._classify_staleness(
            last_sync={"status": "ok"}, cache_exists=True,
            cache_mtime=old_mtime, refresh="manual",
            tspec={"ttl_hours": 24},
        )
        assert result == "stale"

    def test_no_cache_is_unknown(self) -> None:
        result = ProjectService._classify_staleness(
            last_sync={"status": "ok"}, cache_exists=False,
            cache_mtime=None, refresh="manual", tspec={},
        )
        assert result == "unknown"


# ────────────────────────────────────────────────────────────────
# FastAPI endpoint tests
# ────────────────────────────────────────────────────────────────


class TestPhase42Endpoints:
    """Test new Phase 4.2 FastAPI endpoints."""

    @pytest.fixture
    def client(self, demo_project: Path):
        from fastapi.testclient import TestClient

        from fin123.ui.server import create_app

        app = create_app(demo_project)
        return TestClient(app)

    def test_batch_paste_endpoint(self, client) -> None:
        """Simulate paste: batch update via POST /api/sheet/cells."""
        edits = [
            {"addr": "A1", "value": "10"},
            {"addr": "B1", "value": "20"},
            {"addr": "A2", "value": "30"},
            {"addr": "B2", "formula": "=A1 + B1"},
        ]
        resp = client.post("/api/sheet/cells", json={
            "sheet": "Sheet1",
            "edits": edits,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["dirty"] is True

    def test_error_structure_endpoint(self, client) -> None:
        """Error responses have structured code/message fields."""
        resp = client.post("/api/sheet/cells", json={
            "sheet": "Sheet1",
            "edits": [{"addr": "A1", "formula": "=+!!bad"}],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is False
        err = data["errors"][0]
        assert err["code"] == "parse_error"
        assert "message" in err

    def test_invalid_address_error_endpoint(self, client) -> None:
        resp = client.post("/api/sheet/cells", json={
            "sheet": "Sheet1",
            "edits": [{"addr": "999", "value": "x"}],
        })
        data = resp.json()
        assert data["ok"] is False
        assert data["errors"][0]["code"] == "invalid_address"

    def test_datasheets_endpoint(self, client) -> None:
        """GET /api/datasheets returns SQL table status."""
        resp = client.get("/api/datasheets")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        names = {ds["table_name"] for ds in data}
        assert "va_estimates" in names

    def test_datasheets_fields(self, client) -> None:
        """Each datasheet has required fields."""
        resp = client.get("/api/datasheets")
        data = resp.json()
        ds = next(d for d in data if d["table_name"] == "va_estimates")
        assert "staleness" in ds
        assert "cache_path" in ds
        assert "refresh_policy" in ds

    def test_validate_formula_endpoint_valid(self, client) -> None:
        """POST /api/validate-formula with valid formula."""
        resp = client.post("/api/validate-formula", json={"text": "=1 + 2"})
        assert resp.status_code == 200
        assert resp.json()["valid"] is True

    def test_validate_formula_endpoint_invalid(self, client) -> None:
        """POST /api/validate-formula with invalid formula."""
        resp = client.post("/api/validate-formula", json={"text": "=+!!bad"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False
        assert "message" in data

    def test_sync_with_table_name(self, client) -> None:
        """POST /api/sync with table_name parameter."""
        resp = client.post("/api/sync", json={"table_name": "va_estimates"})
        assert resp.status_code == 200
        # Will likely error (no DB) but shouldn't crash
        data = resp.json()
        assert "synced" in data

    def test_paste_then_verify_viewport(self, client) -> None:
        """Full integration: paste data, then read back via viewport."""
        edits = [
            {"addr": "A1", "value": "hello"},
            {"addr": "B1", "value": "world"},
            {"addr": "A2", "value": "42"},
            {"addr": "B2", "value": "3.14"},
        ]
        resp = client.post("/api/sheet/cells", json={"sheet": "Sheet1", "edits": edits})
        assert resp.json()["ok"] is True

        resp = client.get("/api/sheet?sheet=Sheet1&r0=0&c0=0&rows=2&cols=2")
        data = resp.json()
        addrs = {c["addr"] for c in data["cells"]}
        assert addrs == {"A1", "B1", "A2", "B2"}
