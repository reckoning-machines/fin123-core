"""Phase 6 tests: Cross-sheet semantics and named ranges."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from fin123.cell_graph import CellGraph, CellCycleError, _expand_rect
from fin123.formulas import parse_formula, extract_all_refs, extract_refs, parse_sheet_ref
from fin123.formulas.evaluator import evaluate_formula
from fin123.formulas.errors import FormulaRefError
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
def cross_sheet_project(tmp_path: Path) -> Path:
    """Create a project with cross-sheet formulas and named ranges."""
    d = tmp_path / "cross"
    d.mkdir()
    spec = {
        "sheets": [
            {
                "name": "Inputs",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {
                    "A1": {"value": "Revenue"},
                    "B1": {"value": 1000},
                    "A2": {"value": "Cost"},
                    "B2": {"value": 400},
                    "A3": {"value": "Tax"},
                    "B3": {"value": 0.25},
                },
            },
            {
                "name": "Calc",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {
                    "A1": {"value": "Gross Profit"},
                    "B1": {"formula": "=Inputs!B1 - Inputs!B2"},
                    "A2": {"value": "Net Profit"},
                    "B2": {"formula": "=Calc!B1 * (1 - Inputs!B3)"},
                },
            },
        ],
        "names": {
            "revenues": {"sheet": "Inputs", "start": "B1", "end": "B1"},
            "costs": {"sheet": "Inputs", "start": "B2", "end": "B2"},
        },
        "params": {},
        "tables": {},
        "plans": [],
        "outputs": [],
    }
    (d / "workbook.yaml").write_text(yaml.dump(spec, default_flow_style=False))
    (d / "inputs").mkdir()
    (d / "cache").mkdir()
    return d


@pytest.fixture
def service(demo_project: Path) -> ProjectService:
    return ProjectService(project_dir=demo_project)


@pytest.fixture
def cross_service(cross_sheet_project: Path) -> ProjectService:
    return ProjectService(project_dir=cross_sheet_project)


# ────────────────────────────────────────────────────────────────
# Parser: cross-sheet ref tokens
# ────────────────────────────────────────────────────────────────


class TestParserCrossSheet:
    def test_parse_unquoted_sheet_ref(self):
        tree = parse_formula("=Sheet1!A1")
        scalar_refs, cell_refs = extract_all_refs(tree)
        assert cell_refs == {("Sheet1", "A1")}
        assert scalar_refs == set()

    def test_parse_quoted_sheet_ref(self):
        tree = parse_formula("='My Sheet'!B2")
        scalar_refs, cell_refs = extract_all_refs(tree)
        assert cell_refs == {("My Sheet", "B2")}

    def test_parse_sheet_ref_in_expression(self):
        tree = parse_formula("=Sheet1!A1 + Sheet2!B2 * 2")
        scalar_refs, cell_refs = extract_all_refs(tree)
        assert cell_refs == {("Sheet1", "A1"), ("Sheet2", "B2")}

    def test_parse_sheet_ref_in_function(self):
        tree = parse_formula("=SUM(Sheet1!A1, Sheet2!B2)")
        _, cell_refs = extract_all_refs(tree)
        assert cell_refs == {("Sheet1", "A1"), ("Sheet2", "B2")}

    def test_parse_sheet_ref_helper_unquoted(self):
        sheet, addr = parse_sheet_ref("Sheet1!A1")
        assert sheet == "Sheet1"
        assert addr == "A1"

    def test_parse_sheet_ref_helper_quoted(self):
        sheet, addr = parse_sheet_ref("'My Sheet'!B2")
        assert sheet == "My Sheet"
        assert addr == "B2"

    def test_parse_sheet_ref_uppercase_addr(self):
        sheet, addr = parse_sheet_ref("Data!a1")
        assert addr == "A1"

    def test_mixed_scalar_and_cell_refs(self):
        tree = parse_formula("=tax_rate + Sheet1!A1")
        scalar_refs, cell_refs = extract_all_refs(tree)
        assert "tax_rate" in scalar_refs
        assert ("Sheet1", "A1") in cell_refs

    def test_extract_refs_still_works_for_scalar_only(self):
        """extract_refs (old API) still returns scalar refs only."""
        tree = parse_formula("=revenue + Sheet1!A1")
        refs = extract_refs(tree)
        assert "revenue" in refs
        # extract_refs should not include sheet refs
        assert all(isinstance(r, str) for r in refs)


# ────────────────────────────────────────────────────────────────
# Evaluator: cross-sheet + named ranges via resolver
# ────────────────────────────────────────────────────────────────


class MockResolver:
    """Simple mock resolver for testing evaluator directly."""

    def __init__(self, cells, names=None):
        self._cells = cells  # {(sheet, addr): value}
        self._names = names or {}  # {name: [values]}

    def resolve_cell(self, sheet, addr):
        key = (sheet, addr.upper())
        if key not in self._cells:
            raise FormulaRefError(f"{sheet}!{addr}")
        return self._cells[key]

    def resolve_range(self, name):
        if name not in self._names:
            raise FormulaRefError(name)
        return self._names[name]

    def has_named_range(self, name):
        return name in self._names


class TestEvaluatorCrossSheet:
    def test_cross_sheet_ref(self):
        resolver = MockResolver({("Sheet1", "A1"): 42})
        tree = parse_formula("=Sheet1!A1")
        result = evaluate_formula(tree, {}, resolver=resolver)
        assert result == 42

    def test_cross_sheet_arithmetic(self):
        resolver = MockResolver({("Sheet1", "A1"): 100, ("Sheet2", "B2"): 50})
        tree = parse_formula("=Sheet1!A1 - Sheet2!B2")
        result = evaluate_formula(tree, {}, resolver=resolver)
        assert result == 50

    def test_cross_sheet_in_function(self):
        resolver = MockResolver({("Data", "A1"): 10, ("Data", "A2"): 20})
        tree = parse_formula("=SUM(Data!A1, Data!A2)")
        result = evaluate_formula(tree, {}, resolver=resolver)
        assert result == 30

    def test_no_resolver_raises(self):
        tree = parse_formula("=Sheet1!A1")
        with pytest.raises(FormulaRefError):
            evaluate_formula(tree, {}, resolver=None)

    def test_named_range_in_sum(self):
        resolver = MockResolver({}, names={"revenues": [10, 20, 30]})
        tree = parse_formula("=SUM(revenues)")
        result = evaluate_formula(tree, {}, resolver=resolver)
        assert result == 60

    def test_named_range_in_average(self):
        resolver = MockResolver({}, names={"data": [10, 20, 30]})
        tree = parse_formula("=AVERAGE(data)")
        result = evaluate_formula(tree, {}, resolver=resolver)
        assert result == 20

    def test_named_range_in_min_max(self):
        resolver = MockResolver({}, names={"vals": [5, 15, 10]})
        tree = parse_formula("=MIN(vals)")
        assert evaluate_formula(tree, {}, resolver=resolver) == 5
        tree2 = parse_formula("=MAX(vals)")
        assert evaluate_formula(tree2, {}, resolver=resolver) == 15

    def test_named_range_as_scalar_raises(self):
        """Named range used outside aggregate function raises error."""
        resolver = MockResolver({}, names={"myrange": [1, 2, 3]})
        tree = parse_formula("=myrange + 1")
        with pytest.raises(FormulaRefError):
            evaluate_formula(tree, {}, resolver=resolver)

    def test_scalar_context_takes_priority(self):
        """Scalar context name wins over named range of same name."""
        resolver = MockResolver({}, names={"x": [1, 2, 3]})
        tree = parse_formula("=x + 1")
        result = evaluate_formula(tree, {"x": 10}, resolver=resolver)
        assert result == 11


# ────────────────────────────────────────────────────────────────
# CellGraph: basic evaluation
# ────────────────────────────────────────────────────────────────


class TestCellGraph:
    def test_literal_values(self):
        sheets = {
            "Sheet1": {"A1": {"value": 10}, "A2": {"value": "hello"}},
        }
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet1", "A1") == 10
        assert cg.evaluate_cell("Sheet1", "A2") == "hello"

    def test_empty_cell(self):
        sheets = {"Sheet1": {}}
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet1", "A1") is None

    def test_simple_formula(self):
        sheets = {
            "Sheet1": {
                "A1": {"value": 10},
                "A2": {"value": 20},
                "A3": {"formula": "=Sheet1!A1 + Sheet1!A2"},
            },
        }
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet1", "A3") == 30

    def test_cross_sheet_formula(self):
        sheets = {
            "Sheet1": {"A1": {"value": 100}},
            "Sheet2": {"B1": {"formula": "=Sheet1!A1 * 2"}},
        }
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet2", "B1") == 200

    def test_chain_formula(self):
        sheets = {
            "Sheet1": {
                "A1": {"value": 10},
                "A2": {"formula": "=Sheet1!A1 * 2"},
                "A3": {"formula": "=Sheet1!A2 + 5"},
            },
        }
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet1", "A3") == 25

    def test_memoization(self):
        """Results are cached — second call returns same value."""
        sheets = {"Sheet1": {"A1": {"value": 42}}}
        cg = CellGraph(sheets)
        v1 = cg.evaluate_cell("Sheet1", "A1")
        v2 = cg.evaluate_cell("Sheet1", "A1")
        assert v1 is v2

    def test_missing_sheet_raises(self):
        cg = CellGraph({})
        with pytest.raises(ValueError, match="not found"):
            cg.evaluate_cell("NoSuch", "A1")

    def test_evaluate_all(self):
        sheets = {
            "Sheet1": {"A1": {"value": 10}, "A2": {"formula": "=Sheet1!A1 + 5"}},
            "Sheet2": {"B1": {"value": "hi"}},
        }
        cg = CellGraph(sheets)
        results = cg.evaluate_all()
        assert results["Sheet1"]["A1"] == 10
        assert results["Sheet1"]["A2"] == 15
        assert results["Sheet2"]["B1"] == "hi"


# ────────────────────────────────────────────────────────────────
# CellGraph: cycle detection
# ────────────────────────────────────────────────────────────────


class TestCellGraphCycles:
    def test_self_reference(self):
        sheets = {"Sheet1": {"A1": {"formula": "=Sheet1!A1"}}}
        cg = CellGraph(sheets)
        with pytest.raises(CellCycleError) as exc_info:
            cg.evaluate_cell("Sheet1", "A1")
        assert ("Sheet1", "A1") in exc_info.value.cycle_path

    def test_two_cell_cycle(self):
        sheets = {
            "Sheet1": {
                "A1": {"formula": "=Sheet1!A2"},
                "A2": {"formula": "=Sheet1!A1"},
            },
        }
        cg = CellGraph(sheets)
        with pytest.raises(CellCycleError):
            cg.evaluate_cell("Sheet1", "A1")

    def test_cross_sheet_cycle(self):
        sheets = {
            "Sheet1": {"A1": {"formula": "=Sheet2!A1"}},
            "Sheet2": {"A1": {"formula": "=Sheet1!A1"}},
        }
        cg = CellGraph(sheets)
        with pytest.raises(CellCycleError) as exc_info:
            cg.evaluate_cell("Sheet1", "A1")
        path = exc_info.value.cycle_path
        assert len(path) >= 3  # A -> B -> A

    def test_cycle_error_message(self):
        sheets = {
            "Sheet1": {
                "A1": {"formula": "=Sheet1!B1"},
                "B1": {"formula": "=Sheet1!A1"},
            },
        }
        cg = CellGraph(sheets)
        with pytest.raises(CellCycleError, match="Circular cell reference"):
            cg.evaluate_cell("Sheet1", "A1")


# ────────────────────────────────────────────────────────────────
# CellGraph: named ranges
# ────────────────────────────────────────────────────────────────


class TestCellGraphNamedRanges:
    def test_named_range_sum(self):
        sheets = {
            "Data": {
                "B2": {"value": 10},
                "B3": {"value": 20},
                "B4": {"value": 30},
            },
            "Calc": {
                "A1": {"formula": "=SUM(revenues)"},
            },
        }
        names = {"revenues": {"sheet": "Data", "start": "B2", "end": "B4"}}
        cg = CellGraph(sheets, names)
        assert cg.evaluate_cell("Calc", "A1") == 60

    def test_named_range_average(self):
        sheets = {
            "Data": {"A1": {"value": 10}, "A2": {"value": 30}},
            "Calc": {"A1": {"formula": "=AVERAGE(vals)"}},
        }
        names = {"vals": {"sheet": "Data", "start": "A1", "end": "A2"}}
        cg = CellGraph(sheets, names)
        assert cg.evaluate_cell("Calc", "A1") == 20

    def test_named_range_rect(self):
        """Named range spanning a rectangle (multiple cols and rows)."""
        sheets = {
            "Data": {
                "A1": {"value": 1},
                "B1": {"value": 2},
                "A2": {"value": 3},
                "B2": {"value": 4},
            },
            "Calc": {"A1": {"formula": "=SUM(block)"}},
        }
        names = {"block": {"sheet": "Data", "start": "A1", "end": "B2"}}
        cg = CellGraph(sheets, names)
        assert cg.evaluate_cell("Calc", "A1") == 10

    def test_named_range_skips_empty(self):
        """Empty cells in named range are skipped."""
        sheets = {
            "Data": {
                "A1": {"value": 10},
                # A2 is empty
                "A3": {"value": 20},
            },
            "Calc": {"A1": {"formula": "=SUM(vals)"}},
        }
        names = {"vals": {"sheet": "Data", "start": "A1", "end": "A3"}}
        cg = CellGraph(sheets, names)
        assert cg.evaluate_cell("Calc", "A1") == 30

    def test_named_range_with_formulas(self):
        """Named range cells that contain formulas are evaluated."""
        sheets = {
            "Data": {
                "A1": {"value": 10},
                "A2": {"formula": "=Data!A1 * 2"},
            },
            "Calc": {"A1": {"formula": "=SUM(vals)"}},
        }
        names = {"vals": {"sheet": "Data", "start": "A1", "end": "A2"}}
        cg = CellGraph(sheets, names)
        # A1=10, A2=20, SUM=30
        assert cg.evaluate_cell("Calc", "A1") == 30


# ────────────────────────────────────────────────────────────────
# expand_rect helper
# ────────────────────────────────────────────────────────────────


class TestExpandRect:
    def test_single_cell(self):
        assert _expand_rect("A1", "A1") == ["A1"]

    def test_column_range(self):
        result = _expand_rect("A1", "A3")
        assert result == ["A1", "A2", "A3"]

    def test_row_range(self):
        result = _expand_rect("A1", "C1")
        assert result == ["A1", "B1", "C1"]

    def test_rect_range(self):
        result = _expand_rect("A1", "B2")
        assert result == ["A1", "B1", "A2", "B2"]

    def test_reversed_order(self):
        """Start > end is normalized."""
        result = _expand_rect("B2", "A1")
        assert result == ["A1", "B1", "A2", "B2"]


# ────────────────────────────────────────────────────────────────
# CellGraph: display values
# ────────────────────────────────────────────────────────────────


class TestDisplayValues:
    def test_display_number(self):
        cg = CellGraph({"S1": {"A1": {"value": 42}}})
        assert cg.get_display_value("S1", "A1") == "42"

    def test_display_float(self):
        cg = CellGraph({"S1": {"A1": {"value": 3.14}}})
        assert cg.get_display_value("S1", "A1") == "3.14"

    def test_display_empty(self):
        cg = CellGraph({"S1": {}})
        assert cg.get_display_value("S1", "A1") == ""

    def test_display_string(self):
        cg = CellGraph({"S1": {"A1": {"value": "hello"}}})
        assert cg.get_display_value("S1", "A1") == "hello"

    def test_display_formula_result(self):
        cg = CellGraph({"S1": {"A1": {"value": 5}, "A2": {"formula": "=S1!A1 * 2"}}})
        assert cg.get_display_value("S1", "A2") == "10"

    def test_display_cycle_shows_circ(self):
        cg = CellGraph({"S1": {"A1": {"formula": "=S1!A1"}}})
        assert cg.get_display_value("S1", "A1") == "#CIRC!"

    def test_display_bool(self):
        cg = CellGraph({"S1": {"A1": {"formula": "=1 > 0"}}})
        assert cg.get_display_value("S1", "A1") == "TRUE"


# ────────────────────────────────────────────────────────────────
# CellGraph: invalidation
# ────────────────────────────────────────────────────────────────


class TestCellGraphInvalidation:
    def test_invalidate_clears_cache(self):
        sheets = {"S1": {"A1": {"value": 10}}}
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("S1", "A1") == 10
        cg.invalidate()
        # Should re-evaluate (same result but not from cache)
        assert cg.evaluate_cell("S1", "A1") == 10


# ────────────────────────────────────────────────────────────────
# Service: names CRUD
# ────────────────────────────────────────────────────────────────


class TestServiceNamesCrud:
    def test_list_names_empty(self, service: ProjectService):
        assert service.list_names() == {}

    def test_set_and_get_name(self, cross_service: ProjectService):
        result = cross_service.set_name("profit", "Inputs", "B1", "B3")
        assert result["name"] == "profit"
        assert result["sheet"] == "Inputs"
        defn = cross_service.get_name("profit")
        assert defn["start"] == "B1"
        assert defn["end"] == "B3"

    def test_list_names(self, cross_service: ProjectService):
        names = cross_service.list_names()
        assert "revenues" in names
        assert "costs" in names

    def test_update_name(self, cross_service: ProjectService):
        cross_service.update_name("revenues", start="B1", end="B5")
        defn = cross_service.get_name("revenues")
        assert defn["end"] == "B5"

    def test_delete_name(self, cross_service: ProjectService):
        cross_service.delete_name("revenues")
        assert "revenues" not in cross_service.list_names()

    def test_delete_nonexistent_name(self, cross_service: ProjectService):
        with pytest.raises(KeyError):
            cross_service.delete_name("nosuch")

    def test_get_nonexistent_name(self, cross_service: ProjectService):
        with pytest.raises(KeyError):
            cross_service.get_name("nosuch")

    def test_set_name_invalid_addr(self, cross_service: ProjectService):
        with pytest.raises(ValueError):
            cross_service.set_name("bad", "Inputs", "ZZZZZ", "B1")

    def test_set_name_invalid_sheet(self, cross_service: ProjectService):
        with pytest.raises(ValueError):
            cross_service.set_name("bad", "NoSuchSheet", "A1", "B1")


# ────────────────────────────────────────────────────────────────
# Service: computed viewport (CellGraph display values)
# ────────────────────────────────────────────────────────────────


class TestServiceComputedViewport:
    def test_literal_display(self, cross_service: ProjectService):
        vp = cross_service.get_sheet_viewport("Inputs", 0, 0, 5, 5)
        cells = {c["addr"]: c for c in vp["cells"]}
        assert cells["B1"]["display"] == "1000"
        assert cells["B2"]["display"] == "400"

    def test_formula_display(self, cross_service: ProjectService):
        vp = cross_service.get_sheet_viewport("Calc", 0, 0, 5, 5)
        cells = {c["addr"]: c for c in vp["cells"]}
        # B1 = Inputs!B1 - Inputs!B2 = 1000 - 400 = 600
        assert cells["B1"]["display"] == "600"
        assert cells["B1"]["raw"] == "=Inputs!B1 - Inputs!B2"

    def test_chain_formula_display(self, cross_service: ProjectService):
        vp = cross_service.get_sheet_viewport("Calc", 0, 0, 5, 5)
        cells = {c["addr"]: c for c in vp["cells"]}
        # B2 = Calc!B1 * (1 - Inputs!B3) = 600 * 0.75 = 450
        assert cells["B2"]["display"] == "450"

    def test_cell_edit_invalidates_graph(self, cross_service: ProjectService):
        # Edit Inputs!B1 to 2000
        cross_service.update_cells("Inputs", [{"addr": "B1", "value": "2000"}])
        vp = cross_service.get_sheet_viewport("Calc", 0, 0, 5, 5)
        cells = {c["addr"]: c for c in vp["cells"]}
        # B1 = 2000 - 400 = 1600
        assert cells["B1"]["display"] == "1600"


# ────────────────────────────────────────────────────────────────
# Service: names persisted in save
# ────────────────────────────────────────────────────────────────


class TestServiceNamesPersistence:
    def test_names_saved_in_snapshot(self, cross_service: ProjectService):
        cross_service.set_name("test_range", "Inputs", "A1", "A3")
        cross_service.save_snapshot()
        # Read back workbook.yaml
        spec = yaml.safe_load(
            (cross_service.project_dir / "workbook.yaml").read_text()
        )
        assert "names" in spec
        assert "test_range" in spec["names"]

    def test_names_in_project_info(self, cross_service: ProjectService):
        info = cross_service.get_project_info()
        assert "names" in info
        assert "revenues" in info["names"]


# ────────────────────────────────────────────────────────────────
# API endpoints (via TestClient)
# ────────────────────────────────────────────────────────────────


class TestAPINames:
    @pytest.fixture
    def client(self, cross_sheet_project: Path):
        from fastapi.testclient import TestClient
        from fin123.ui.server import create_app
        app = create_app(cross_sheet_project)
        return TestClient(app)

    def test_get_names(self, client):
        resp = client.get("/api/names")
        assert resp.status_code == 200
        data = resp.json()
        assert "revenues" in data
        assert "costs" in data

    def test_create_name(self, client):
        resp = client.post("/api/names", json={
            "name": "new_range",
            "sheet": "Inputs",
            "start": "A1",
            "end": "A5",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "new_range"
        # Verify it shows up in list
        resp2 = client.get("/api/names")
        assert "new_range" in resp2.json()

    def test_update_name(self, client):
        resp = client.patch("/api/names/revenues", json={"end": "B5"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["end"] == "B5"

    def test_delete_name(self, client):
        resp = client.delete("/api/names/costs")
        assert resp.status_code == 200
        assert resp.json()["deleted"] == "costs"
        # Gone from list
        resp2 = client.get("/api/names")
        assert "costs" not in resp2.json()

    def test_delete_nonexistent(self, client):
        resp = client.delete("/api/names/nosuch")
        assert resp.status_code == 404

    def test_update_nonexistent(self, client):
        resp = client.patch("/api/names/nosuch", json={"start": "A1"})
        assert resp.status_code == 404

    def test_viewport_has_computed_values(self, client):
        resp = client.get("/api/sheet?sheet=Calc&r0=0&c0=0&rows=5&cols=5")
        assert resp.status_code == 200
        cells = {c["addr"]: c for c in resp.json()["cells"]}
        # Calc!B1 = Inputs!B1 - Inputs!B2 = 1000 - 400 = 600
        assert cells["B1"]["display"] == "600"


# ────────────────────────────────────────────────────────────────
# Integration: existing tests still pass
# ────────────────────────────────────────────────────────────────


class TestExistingIntegration:
    def test_demo_project_still_runs(self, demo_project: Path):
        from fin123.workbook import Workbook
        wb = Workbook(demo_project)
        result = wb.run()
        assert result.scalars
        assert result.run_dir.exists()

    def test_demo_service_viewport(self, service: ProjectService):
        vp = service.get_sheet_viewport()
        assert vp["sheet"] == "Sheet1"
        assert isinstance(vp["cells"], list)
