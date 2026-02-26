"""Tests for row/column insertion, deletion, and formula reference rewriting."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from fin123.project import scaffold_project
from fin123.ui.service import (
    ProjectService,
    _remap_addresses,
    col_letter_to_index,
    index_to_col_letter,
    make_addr,
    parse_addr,
    rewrite_formula_refs,
)


# ────────────────────────────────────────────────────────────────
# rewrite_formula_refs — pure function tests
# ────────────────────────────────────────────────────────────────


class TestRewriteFormulaRefs:
    """Test the regex-based formula reference rewriting engine."""

    # -- Row insertion --

    def test_insert_row_shifts_refs_at_or_past(self) -> None:
        """Refs at or past the insert index shift forward."""
        result = rewrite_formula_refs("=A5+A10", "Sheet1", "Sheet1", "row", 4, 2)
        assert result == "=A7+A12"

    def test_insert_row_leaves_refs_before(self) -> None:
        """Refs before the insert index are unchanged."""
        result = rewrite_formula_refs("=A1+A3", "Sheet1", "Sheet1", "row", 4, 1)
        assert result == "=A1+A3"

    def test_insert_row_at_exact_index(self) -> None:
        """Ref at exactly the insert index shifts."""
        result = rewrite_formula_refs("=B5", "Sheet1", "Sheet1", "row", 4, 1)
        assert result == "=B6"

    def test_insert_row_multi_col_ref(self) -> None:
        """Multi-letter column refs are preserved."""
        result = rewrite_formula_refs("=AA10", "Sheet1", "Sheet1", "row", 5, 3)
        assert result == "=AA13"

    # -- Row deletion --

    def test_delete_row_ref_in_deleted_range(self) -> None:
        """Refs in the deleted range become #REF!."""
        result = rewrite_formula_refs("=A5", "Sheet1", "Sheet1", "row", 4, -1)
        assert result == "=#REF!"

    def test_delete_row_ref_past_range_shifts(self) -> None:
        """Refs past the deleted range shift backward."""
        result = rewrite_formula_refs("=A10", "Sheet1", "Sheet1", "row", 4, -2)
        assert result == "=A8"

    def test_delete_row_ref_before_range_unchanged(self) -> None:
        """Refs before the deleted range are unchanged."""
        result = rewrite_formula_refs("=A3", "Sheet1", "Sheet1", "row", 4, -1)
        assert result == "=A3"

    def test_delete_multiple_rows_ref(self) -> None:
        """Delete 3 rows: ref in range → #REF!, ref past → shifts."""
        formula = "=A5+A6+A10"
        result = rewrite_formula_refs(formula, "Sheet1", "Sheet1", "row", 4, -3)
        assert "#REF!" in result
        # A10 (row 9, 0-based) shifts by -3 → A7
        assert "A7" in result

    # -- Column insertion --

    def test_insert_col_shifts_refs(self) -> None:
        """Column insert shifts column letters."""
        result = rewrite_formula_refs("=C5", "Sheet1", "Sheet1", "col", 2, 1)
        assert result == "=D5"

    def test_insert_col_leaves_earlier_cols(self) -> None:
        """Columns before the insert point are unchanged."""
        result = rewrite_formula_refs("=A5+B5", "Sheet1", "Sheet1", "col", 2, 1)
        assert result == "=A5+B5"

    def test_insert_col_multi_letter(self) -> None:
        """Column insert past Z wraps to multi-letter columns."""
        # Insert 1 col at Z (index 25): Z5 → AA5
        result = rewrite_formula_refs("=Z5", "Sheet1", "Sheet1", "col", 25, 1)
        assert result == "=AA5"

    # -- Column deletion --

    def test_delete_col_ref_in_range(self) -> None:
        """Ref in deleted column range → #REF!."""
        result = rewrite_formula_refs("=C5", "Sheet1", "Sheet1", "col", 2, -1)
        assert result == "=#REF!"

    def test_delete_col_ref_past_range(self) -> None:
        """Ref past deleted range shifts back."""
        result = rewrite_formula_refs("=D5", "Sheet1", "Sheet1", "col", 1, -1)
        assert result == "=C5"

    # -- Sheet references --

    def test_quoted_sheet_ref(self) -> None:
        """Quoted sheet references are rewritten."""
        result = rewrite_formula_refs("='My Sheet'!A5", "My Sheet", "Sheet1", "row", 4, 1)
        assert result == "='My Sheet'!A6"

    def test_unquoted_sheet_ref(self) -> None:
        """Unquoted sheet references are rewritten."""
        result = rewrite_formula_refs("=Sheet2!A5", "Sheet2", "Sheet1", "row", 4, 1)
        assert result == "=Sheet2!A6"

    def test_different_sheet_not_affected(self) -> None:
        """Refs to a different sheet are not changed."""
        result = rewrite_formula_refs("=Sheet2!A5", "Sheet1", "Sheet1", "row", 4, 1)
        assert result == "=Sheet2!A5"

    def test_bare_ref_resolves_to_current_sheet(self) -> None:
        """Bare refs resolve to current_sheet; only rewrite if it matches affected_sheet."""
        # current_sheet is Sheet2, affected is Sheet2 → should rewrite
        result = rewrite_formula_refs("=A5", "Sheet2", "Sheet2", "row", 4, 1)
        assert result == "=A6"

    def test_bare_ref_different_current_sheet(self) -> None:
        """Bare ref on different sheet than affected → no change."""
        result = rewrite_formula_refs("=A5", "Sheet1", "Sheet2", "row", 4, 1)
        assert result == "=A5"

    # -- String literals --

    def test_string_literal_not_rewritten(self) -> None:
        """Refs inside string literals are not rewritten."""
        result = rewrite_formula_refs('=CONCAT("A5", A5)', "Sheet1", "Sheet1", "row", 4, 1)
        # The bare A5 after the string should shift, but "A5" in string should not
        assert '"A5"' in result
        assert "A6" in result

    # -- Edge cases --

    def test_non_formula_unchanged(self) -> None:
        """Non-formula strings are returned as-is."""
        assert rewrite_formula_refs("hello", "Sheet1", "Sheet1", "row", 0, 1) == "hello"

    def test_empty_formula(self) -> None:
        """Empty strings are returned as-is."""
        assert rewrite_formula_refs("", "Sheet1", "Sheet1", "row", 0, 1) == ""

    def test_formula_no_refs(self) -> None:
        """Formula with no cell references is unchanged."""
        assert rewrite_formula_refs("=42+3", "Sheet1", "Sheet1", "row", 0, 1) == "=42+3"

    def test_mixed_refs_partial_shift(self) -> None:
        """Only refs at/past the index shift; others unchanged."""
        result = rewrite_formula_refs("=A1+A5+A10", "Sheet1", "Sheet1", "row", 4, 2)
        assert result == "=A1+A7+A12"


# ────────────────────────────────────────────────────────────────
# _remap_addresses — pure function tests
# ────────────────────────────────────────────────────────────────


class TestRemapAddresses:
    """Test address key remapping for cells/fmt dicts."""

    def test_insert_row_shifts_keys(self) -> None:
        """Keys at or past the insert index shift forward."""
        d = {"A1": 1, "A5": 5, "A10": 10}
        result = _remap_addresses(d, "row", 4, 2)
        assert "A1" in result
        assert "A7" in result  # A5 → A7
        assert "A12" in result  # A10 → A12
        assert "A5" not in result

    def test_delete_row_drops_deleted(self) -> None:
        """Keys in the deleted range are dropped."""
        d = {"A5": 5, "A6": 6, "A10": 10}
        result = _remap_addresses(d, "row", 4, -2)
        assert "A5" not in result  # deleted
        assert "A6" not in result  # deleted
        assert "A8" in result  # A10 → A8

    def test_insert_col_shifts_keys(self) -> None:
        """Column insertion shifts column keys."""
        d = {"A1": 1, "C1": 3, "E1": 5}
        result = _remap_addresses(d, "col", 2, 1)
        assert "A1" in result  # before insert
        assert "D1" in result  # C1 → D1
        assert "F1" in result  # E1 → F1
        assert "C1" not in result

    def test_delete_col_drops_deleted(self) -> None:
        """Column keys in deleted range are dropped."""
        d = {"A1": 1, "C1": 3, "D1": 4}
        result = _remap_addresses(d, "col", 2, -1)
        assert "A1" in result
        assert "C1" in result  # D1 → C1
        assert len(result) == 2  # C1 was deleted


# ────────────────────────────────────────────────────────────────
# Service integration tests
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


class TestServiceRowColInsert:
    """Test insert_rows, delete_rows, insert_cols, delete_cols on ProjectService."""

    def test_insert_rows_basic(self, service: ProjectService) -> None:
        """Insert rows increases n_rows and marks dirty."""
        service.update_cells("Sheet1", [{"addr": "A5", "value": "hello"}])
        service._dirty = False  # reset
        result = service.insert_rows("Sheet1", 4, 2)
        assert result["ok"] is True
        assert result["n_rows"] == 202
        assert result["dirty"] is True

    def test_insert_rows_shifts_cell(self, service: ProjectService) -> None:
        """Cells at/past the insert point shift down."""
        service.update_cells("Sheet1", [{"addr": "A5", "value": "hello"}])
        service.insert_rows("Sheet1", 4, 2)
        sheet = service._get_sheet("Sheet1")
        # A5 (row 4) should have moved to A7
        assert "A7" in sheet["cells"]
        assert "A5" not in sheet["cells"]

    def test_delete_rows_basic(self, service: ProjectService) -> None:
        """Delete rows decreases n_rows."""
        result = service.delete_rows("Sheet1", 0, 5)
        assert result["ok"] is True
        assert result["n_rows"] == 195

    def test_delete_rows_removes_cells(self, service: ProjectService) -> None:
        """Cells in deleted range are removed."""
        service.update_cells("Sheet1", [
            {"addr": "A5", "value": "deleted"},
            {"addr": "A10", "value": "kept"},
        ])
        service.delete_rows("Sheet1", 4, 2)
        sheet = service._get_sheet("Sheet1")
        assert "A5" not in sheet["cells"]
        # A10 (row 9) → shifts by -2 → A8
        assert "A8" in sheet["cells"]

    def test_insert_cols_basic(self, service: ProjectService) -> None:
        """Insert cols increases n_cols."""
        result = service.insert_cols("Sheet1", 2, 3)
        assert result["ok"] is True
        assert result["n_cols"] == 43

    def test_delete_cols_basic(self, service: ProjectService) -> None:
        """Delete cols decreases n_cols."""
        result = service.delete_cols("Sheet1", 0, 2)
        assert result["ok"] is True
        assert result["n_cols"] == 38

    def test_insert_rows_invalid_index(self, service: ProjectService) -> None:
        """Out-of-range index raises ValueError."""
        with pytest.raises(ValueError, match="out of range"):
            service.insert_rows("Sheet1", -1, 1)

    def test_delete_rows_invalid_count(self, service: ProjectService) -> None:
        """count < 1 raises ValueError."""
        with pytest.raises(ValueError, match="count must be >= 1"):
            service.delete_rows("Sheet1", 0, 0)

    def test_insert_shifts_formulas(self, service: ProjectService) -> None:
        """Formulas referencing shifted cells are updated."""
        service.update_cells("Sheet1", [
            {"addr": "A1", "value": "10"},
            {"addr": "A5", "value": "20"},
            {"addr": "B1", "formula": "=A1+A5"},
        ])
        service.insert_rows("Sheet1", 4, 1)
        sheet = service._get_sheet("Sheet1")
        # B1 formula should now reference A1+A6
        assert sheet["cells"]["B1"]["formula"] == "=A1+A6"

    def test_cross_sheet_formula_rewrite(self, service: ProjectService) -> None:
        """Formulas on other sheets referencing the affected sheet are rewritten."""
        service.add_sheet("Sheet2")
        service.update_cells("Sheet1", [{"addr": "A5", "value": "100"}])
        service.update_cells("Sheet2", [{"addr": "A1", "formula": "=Sheet1!A5"}])
        service.insert_rows("Sheet1", 4, 1)
        sheet2 = service._get_sheet("Sheet2")
        assert sheet2["cells"]["A1"]["formula"] == "=Sheet1!A6"

    def test_insert_shifts_named_range(self, service: ProjectService) -> None:
        """Named ranges on the affected sheet shift."""
        service.set_name("MyRange", "Sheet1", "A5", "C10")
        service.insert_rows("Sheet1", 4, 2)
        defn = service.get_name("MyRange")
        assert defn["start"] == "A7"
        assert defn["end"] == "C12"

    def test_delete_with_ref_produces_ref_error(self, service: ProjectService) -> None:
        """Deleting a row containing a referenced cell produces #REF! in formulas."""
        service.update_cells("Sheet1", [
            {"addr": "A5", "value": "100"},
            {"addr": "B1", "formula": "=A5"},
        ])
        service.delete_rows("Sheet1", 4, 1)
        sheet = service._get_sheet("Sheet1")
        assert sheet["cells"]["B1"]["formula"] == "=#REF!"

    def test_insert_cols_shifts_cell_cols(self, service: ProjectService) -> None:
        """Column insertion shifts cell column keys."""
        service.update_cells("Sheet1", [{"addr": "C5", "value": "data"}])
        service.insert_cols("Sheet1", 2, 1)
        sheet = service._get_sheet("Sheet1")
        assert "D5" in sheet["cells"]
        assert "C5" not in sheet["cells"]

    def test_read_only_rejects_insert(self, service: ProjectService) -> None:
        """Read-only mode rejects insert operations."""
        service._read_only = True
        with pytest.raises(ValueError, match="read-only"):
            service.insert_rows("Sheet1", 0, 1)


# ────────────────────────────────────────────────────────────────
# API endpoint tests (via TestClient)
# ────────────────────────────────────────────────────────────────


class TestRowColAPI:
    """Test the row/col API endpoints via FastAPI TestClient."""

    @pytest.fixture
    def client(self, demo_project: Path):
        from fastapi.testclient import TestClient

        from fin123.ui.server import create_app

        app = create_app(demo_project)
        return TestClient(app)

    def test_insert_rows_endpoint(self, client) -> None:
        """POST /api/sheet/rows/insert returns ok."""
        resp = client.post("/api/sheet/rows/insert", json={"sheet": "Sheet1", "row_idx": 5, "count": 2})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["n_rows"] == 202

    def test_delete_rows_endpoint(self, client) -> None:
        """POST /api/sheet/rows/delete returns ok."""
        resp = client.post("/api/sheet/rows/delete", json={"sheet": "Sheet1", "row_idx": 0, "count": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["n_rows"] == 199

    def test_insert_cols_endpoint(self, client) -> None:
        """POST /api/sheet/cols/insert returns ok."""
        resp = client.post("/api/sheet/cols/insert", json={"sheet": "Sheet1", "col_idx": 3, "count": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["n_cols"] == 41

    def test_delete_cols_endpoint(self, client) -> None:
        """POST /api/sheet/cols/delete returns ok."""
        resp = client.post("/api/sheet/cols/delete", json={"sheet": "Sheet1", "col_idx": 0, "count": 2})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["n_cols"] == 38

    def test_invalid_row_idx_returns_400(self, client) -> None:
        """Invalid row_idx returns 400."""
        resp = client.post("/api/sheet/rows/insert", json={"sheet": "Sheet1", "row_idx": -1, "count": 1})
        assert resp.status_code == 400

    def test_missing_sheet_returns_400(self, client) -> None:
        """Non-existent sheet returns 400."""
        resp = client.post("/api/sheet/rows/insert", json={"sheet": "NoSheet", "row_idx": 0, "count": 1})
        assert resp.status_code == 400
