"""Tests for the XLSX import upload endpoint and service function."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def make_xlsx(tmp_path: Path):
    """Factory for creating XLSX files with openpyxl."""
    openpyxl = pytest.importorskip("openpyxl")

    def _make(sheets: dict[str, dict[str, Any]], filename: str = "test.xlsx") -> Path:
        wb = openpyxl.Workbook()
        first = True
        for sheet_name, cells in sheets.items():
            if first:
                ws = wb.active
                ws.title = sheet_name
                first = False
            else:
                ws = wb.create_sheet(sheet_name)
            for addr, val in cells.items():
                ws[addr] = val
        path = tmp_path / filename
        wb.save(str(path))
        wb.close()
        return path

    return _make


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Scaffold a demo project for the TestClient."""
    from fin123.project import scaffold_project

    project_dir = tmp_path / "test_project"
    scaffold_project(project_dir)
    return project_dir


@pytest.fixture
def client(demo_project: Path):
    from fastapi.testclient import TestClient

    from fin123.ui.server import create_app

    app = create_app(demo_project)
    return TestClient(app)


class TestImportUploadEndpoint:
    """Test POST /api/import/xlsx."""

    def test_upload_creates_project(self, client, make_xlsx, tmp_path) -> None:
        xlsx_path = make_xlsx({"Sheet1": {"A1": 42, "B1": "hello"}})
        base_dir = tmp_path / "projects"

        with open(xlsx_path, "rb") as f:
            # We need to pass base_dir through the service function directly
            # since the endpoint doesn't expose base_dir.
            pass

        # Test via the service function directly to control base_dir
        from fin123.ui.service import import_xlsx_upload

        data = xlsx_path.read_bytes()
        result = import_xlsx_upload(
            file_bytes=data,
            filename="test.xlsx",
            project_name="test_proj",
            base_dir=base_dir,
        )
        assert result["ok"] is True
        assert (Path(result["project_dir"]) / "workbook.yaml").exists()

    def test_upload_returns_report(self, client, make_xlsx, tmp_path) -> None:
        xlsx_path = make_xlsx({
            "Sheet1": {"A1": 10, "B1": 20, "A2": "=A1+B1"},
        })
        from fin123.ui.service import import_xlsx_upload

        result = import_xlsx_upload(
            file_bytes=xlsx_path.read_bytes(),
            filename="model.xlsx",
            base_dir=tmp_path / "projects",
        )
        assert result["ok"] is True
        assert result["report"]["cells_imported"] > 0

    def test_upload_creates_import_report_index(self, make_xlsx, tmp_path) -> None:
        xlsx_path = make_xlsx({"Sheet1": {"A1": 1}})
        from fin123.ui.service import import_xlsx_upload

        result = import_xlsx_upload(
            file_bytes=xlsx_path.read_bytes(),
            filename="idx_test.xlsx",
            base_dir=tmp_path / "projects",
        )
        project_dir = Path(result["project_dir"])
        report_json = project_dir / "import_report.json"
        assert report_json.exists()

    def test_upload_rejects_non_xlsx(self, client) -> None:
        resp = client.post(
            "/api/import/xlsx",
            files={"file": ("data.csv", b"a,b,c\n1,2,3", "text/csv")},
        )
        assert resp.status_code == 400
        assert "xlsx" in resp.json()["detail"].lower()

    def test_upload_rejects_empty_file(self, client) -> None:
        resp = client.post(
            "/api/import/xlsx",
            files={"file": ("empty.xlsx", b"", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        )
        assert resp.status_code == 400
        assert "empty" in resp.json()["detail"].lower()

    def test_upload_duplicate_name_errors(self, make_xlsx, tmp_path) -> None:
        xlsx_path = make_xlsx({"Sheet1": {"A1": 1}})
        from fin123.ui.service import import_xlsx_upload

        data = xlsx_path.read_bytes()
        base_dir = tmp_path / "projects"

        # First import succeeds
        import_xlsx_upload(
            file_bytes=data, filename="dup.xlsx",
            project_name="dup_proj", base_dir=base_dir,
        )

        # Second import with same name fails
        with pytest.raises(ValueError, match="already exists"):
            import_xlsx_upload(
                file_bytes=data, filename="dup.xlsx",
                project_name="dup_proj", base_dir=base_dir,
            )

    def test_upload_project_name_from_filename(self, make_xlsx, tmp_path) -> None:
        xlsx_path = make_xlsx(
            {"Sheet1": {"A1": 1}},
            filename="My Cool Model.xlsx",
        )
        from fin123.ui.service import import_xlsx_upload

        result = import_xlsx_upload(
            file_bytes=xlsx_path.read_bytes(),
            filename="My Cool Model.xlsx",
            base_dir=tmp_path / "projects",
        )
        assert result["project_name"] == "my_cool_model"
        assert result["ok"] is True


class TestImportUploadViaHTTP:
    """Test the upload endpoint through the full HTTP stack."""

    def test_endpoint_upload_success(self, client, make_xlsx, tmp_path, monkeypatch) -> None:
        """Upload via the HTTP endpoint with monkeypatched base_dir."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": 99, "B1": "data"}})
        base_dir = tmp_path / "http_projects"

        import fin123.ui.service as svc_mod

        orig = svc_mod.import_xlsx_upload

        def patched(file_bytes, filename, project_name=None, base_dir=None):
            return orig(file_bytes, filename, project_name, base_dir=base_dir or (tmp_path / "http_projects"))

        monkeypatch.setattr(svc_mod, "import_xlsx_upload", patched)
        # Also patch the reference in server module
        import fin123.ui.server as srv_mod
        monkeypatch.setattr(srv_mod, "import_xlsx_upload", patched)

        with open(xlsx_path, "rb") as f:
            resp = client.post(
                "/api/import/xlsx",
                files={"file": ("test.xlsx", f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                data={"project_name": "http_test"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["project_name"] == "http_test"
        assert data["report"]["cells_imported"] > 0
