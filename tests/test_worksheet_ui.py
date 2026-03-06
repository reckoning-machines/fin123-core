"""Tests for worksheet UI integration (Stage 7).

Tests both the service layer methods and the FastAPI endpoints.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
import yaml

from fin123.project import scaffold_project


# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────


def _setup_project_with_run_and_spec(tmp_path: Path) -> Path:
    """Create a project with a build run and a worksheet spec."""
    project = scaffold_project(tmp_path / "proj")

    # Create a fake run with parquet output
    run_dir = project / "runs" / "run_001"
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True)
    (run_dir / "run_meta.json").write_text(json.dumps({
        "run_id": "run_001",
        "status": "success",
    }))
    df = pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOG"],
        "px_last": [180.0, 370.0, 175.0],
        "eps_ntm": [7.1, 12.5, 6.8],
    })
    df.write_parquet(outputs_dir / "estimates.parquet")

    # Create worksheet spec
    ws_dir = project / "worksheets"
    ws_dir.mkdir()
    spec = {
        "name": "valuation",
        "title": "Valuation Review",
        "columns": [
            {"source": "ticker", "label": "Ticker"},
            {"source": "px_last", "label": "Price"},
            {"source": "eps_ntm", "label": "EPS"},
            {
                "name": "pe_ratio",
                "expression": "px_last / eps_ntm",
                "label": "P/E",
            },
        ],
        "sorts": [{"column": "pe_ratio"}],
    }
    (ws_dir / "valuation.yaml").write_text(yaml.dump(spec))

    return project


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    return _setup_project_with_run_and_spec(tmp_path)


@pytest.fixture
def service(project_dir: Path):
    from fin123.ui.service import ProjectService
    return ProjectService(project_dir=project_dir)


@pytest.fixture
def client(project_dir: Path):
    from fastapi.testclient import TestClient
    from fin123.ui.server import create_app

    app = create_app(project_dir)
    return TestClient(app)


# ────────────────────────────────────────────────────────────────
# Service layer tests
# ────────────────────────────────────────────────────────────────


class TestServiceWorksheetSpecs:
    def test_list_specs(self, service) -> None:
        specs = service.list_worksheet_specs()
        assert len(specs) == 1
        assert specs[0]["name"] == "valuation"
        assert specs[0]["title"] == "Valuation Review"
        assert specs[0]["columns"] == 4

    def test_list_specs_no_dir(self, tmp_path: Path) -> None:
        from fin123.ui.service import ProjectService

        project = scaffold_project(tmp_path / "empty")
        svc = ProjectService(project_dir=project)
        specs = svc.list_worksheet_specs()
        assert specs == []

    def test_list_specs_invalid_yaml(self, project_dir: Path, service) -> None:
        """Invalid YAML spec shows up with name='?'."""
        (project_dir / "worksheets" / "bad.yaml").write_text("not: [valid")
        specs = service.list_worksheet_specs()
        bad = [s for s in specs if s["name"] == "?"]
        assert len(bad) == 1


class TestServiceWorksheetCompile:
    def test_compile_success(self, service) -> None:
        result = service.compile_worksheet_from_run(
            spec_file="worksheets/valuation.yaml",
            table_name="estimates",
        )
        assert result["name"] == "valuation"
        assert len(result["rows"]) == 3
        assert len(result["columns"]) == 4
        # Check a derived value exists
        tickers = [r["ticker"] for r in result["rows"]]
        assert "AAPL" in tickers

    def test_compile_missing_spec(self, service) -> None:
        with pytest.raises(FileNotFoundError, match="not found"):
            service.compile_worksheet_from_run(
                spec_file="worksheets/nonexistent.yaml",
                table_name="estimates",
            )

    def test_compile_missing_table(self, service) -> None:
        with pytest.raises(FileNotFoundError):
            service.compile_worksheet_from_run(
                spec_file="worksheets/valuation.yaml",
                table_name="nonexistent",
            )

    def test_compile_returns_provenance(self, service) -> None:
        result = service.compile_worksheet_from_run(
            spec_file="worksheets/valuation.yaml",
            table_name="estimates",
        )
        assert "provenance" in result
        assert result["provenance"]["spec_name"] == "valuation"
        assert result["provenance"]["row_count"] == 3

    def test_compile_returns_sorts(self, service) -> None:
        result = service.compile_worksheet_from_run(
            spec_file="worksheets/valuation.yaml",
            table_name="estimates",
        )
        assert len(result["sorts"]) == 1
        assert result["sorts"][0]["column"] == "pe_ratio"


# ────────────────────────────────────────────────────────────────
# FastAPI endpoint tests
# ────────────────────────────────────────────────────────────────


class TestAPIWorksheetSpecs:
    def test_get_specs(self, client) -> None:
        resp = client.get("/api/worksheet/specs")
        assert resp.status_code == 200
        specs = resp.json()
        assert len(specs) == 1
        assert specs[0]["name"] == "valuation"

    def test_get_specs_empty(self, tmp_path: Path) -> None:
        from fastapi.testclient import TestClient
        from fin123.ui.server import create_app

        project = scaffold_project(tmp_path / "empty")
        app = create_app(project)
        c = TestClient(app)
        resp = c.get("/api/worksheet/specs")
        assert resp.status_code == 200
        assert resp.json() == []


class TestAPIWorksheetCompile:
    def test_compile_success(self, client) -> None:
        resp = client.post("/api/worksheet/compile", json={
            "spec_file": "worksheets/valuation.yaml",
            "table_name": "estimates",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "valuation"
        assert len(data["rows"]) == 3
        assert "provenance" in data

    def test_compile_missing_spec_404(self, client) -> None:
        resp = client.post("/api/worksheet/compile", json={
            "spec_file": "worksheets/nonexistent.yaml",
            "table_name": "estimates",
        })
        assert resp.status_code == 404

    def test_compile_missing_table_404(self, client) -> None:
        resp = client.post("/api/worksheet/compile", json={
            "spec_file": "worksheets/valuation.yaml",
            "table_name": "nonexistent",
        })
        assert resp.status_code == 404

    def test_compile_bad_spec_400(self, client, project_dir: Path) -> None:
        """Spec that references nonexistent column returns 400."""
        bad_spec = {
            "name": "bad",
            "columns": [{"source": "nonexistent_column"}],
        }
        (project_dir / "worksheets" / "bad.yaml").write_text(yaml.dump(bad_spec))

        resp = client.post("/api/worksheet/compile", json={
            "spec_file": "worksheets/bad.yaml",
            "table_name": "estimates",
        })
        assert resp.status_code == 400

    def test_compile_returns_full_artifact_shape(self, client) -> None:
        """Response matches the CompiledWorksheet canonical shape."""
        resp = client.post("/api/worksheet/compile", json={
            "spec_file": "worksheets/valuation.yaml",
            "table_name": "estimates",
        })
        data = resp.json()
        # All required top-level keys present
        assert "name" in data
        assert "columns" in data
        assert "rows" in data
        assert "flags" in data
        assert "provenance" in data
        assert "sorts" in data
        assert "header_groups" in data
        # Provenance substructure
        prov = data["provenance"]
        assert "view_table" in prov
        assert "compiled_at" in prov
        assert "fin123_version" in prov
        assert "columns" in prov


# ────────────────────────────────────────────────────────────────
# Static file serving
# ────────────────────────────────────────────────────────────────


class TestStaticFiles:
    def test_index_html_includes_worksheet_tab(self, client) -> None:
        resp = client.get("/")
        assert resp.status_code == 200
        html = resp.text
        assert 'data-tab="worksheet"' in html
        assert "ws-viewer-mount" in html

    def test_worksheet_viewer_js_served(self, client) -> None:
        resp = client.get("/static/worksheet_viewer.js")
        assert resp.status_code == 200
        assert "WorksheetViewer" in resp.text

    def test_worksheet_viewer_css_served(self, client) -> None:
        resp = client.get("/static/worksheet_viewer.css")
        assert resp.status_code == 200
        assert "ws-viewer" in resp.text
