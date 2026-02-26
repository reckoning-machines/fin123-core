"""Phase 8.1 tests: In-sheet A1 cell references in formulas."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from fin123.cell_graph import CellGraph, CellCycleError
from fin123.formulas import parse_formula, extract_all_refs, extract_refs
from fin123.formulas.evaluator import evaluate_formula
from fin123.formulas.errors import FormulaRefError
from fin123.ui.service import ProjectService
from fin123.xlsx_import import classify_formula


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def insheet_project(tmp_path: Path) -> Path:
    """Create a project with in-sheet cell references."""
    d = tmp_path / "insheet"
    d.mkdir()
    spec = {
        "sheets": [
            {
                "name": "Sheet1",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {
                    "A1": {"value": "Price"},
                    "B1": {"value": 10},
                    "A2": {"value": "Qty"},
                    "B2": {"value": 5},
                    "A3": {"value": "Total"},
                    "B3": {"formula": "=B1 * B2"},
                    "A4": {"value": "Tax"},
                    "B4": {"formula": "=B3 * 0.1"},
                    "F2": {"value": 2},
                    "G2": {"formula": "=F2 * 3.5"},
                },
            },
            {
                "name": "Summary",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {
                    "A1": {"formula": "=Sheet1!B3"},
                    "A2": {"formula": "=Sheet1!B3 + A1"},
                },
            },
        ],
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
def insheet_service(insheet_project: Path) -> ProjectService:
    return ProjectService(project_dir=insheet_project)


# ────────────────────────────────────────────────────────────────
# Parser: CELL_REF tokenization
# ────────────────────────────────────────────────────────────────


class TestParserCellRef:
    def test_f2_tokenizes_as_cell_ref(self):
        """=F2 should parse as cell_ref, not ref_bare."""
        tree = parse_formula("=F2")
        # The tree should have a cell_ref node
        assert tree.children[0].data == "cell_ref"

    def test_aa10_tokenizes_as_cell_ref(self):
        tree = parse_formula("=AA10")
        assert tree.children[0].data == "cell_ref"

    def test_bbb3_tokenizes_as_cell_ref(self):
        tree = parse_formula("=BBB3")
        assert tree.children[0].data == "cell_ref"

    def test_a1_tokenizes_as_cell_ref(self):
        tree = parse_formula("=A1")
        assert tree.children[0].data == "cell_ref"

    def test_max_f2_1_parses(self):
        """=MAX(F2,1) should parse without error."""
        tree = parse_formula("=MAX(F2,1)")
        assert tree is not None

    def test_aa10_plus_bbb3_parses(self):
        """=AA10+BBB3 should parse."""
        tree = parse_formula("=AA10+BBB3")
        assert tree is not None

    def test_identifier_still_parses_as_name(self):
        """revenue (contains lowercase) should still be ref_bare."""
        tree = parse_formula("=revenue")
        assert tree.children[0].data == "ref_bare"

    def test_mixed_case_identifier_is_name(self):
        """tax_rate should still be ref_bare."""
        tree = parse_formula("=tax_rate")
        assert tree.children[0].data == "ref_bare"

    def test_underscore_start_is_name(self):
        """_foo should still be ref_bare."""
        tree = parse_formula("=_foo")
        assert tree.children[0].data == "ref_bare"

    def test_max_negf2_neg3_f2_times_3point5(self):
        """=MAX(-F2,-3,F2*3.5) should parse."""
        tree = parse_formula("=MAX(-F2,-3,F2*3.5)")
        assert tree is not None

    def test_f2_times_3point5(self):
        """=F2*3.5 should parse."""
        tree = parse_formula("=F2*3.5")
        assert tree is not None

    def test_sum_with_cell_refs(self):
        """=SUM(A1,B2,C3) should parse."""
        tree = parse_formula("=SUM(A1,B2,C3)")
        assert tree is not None

    def test_cell_ref_in_if(self):
        """=IF(A1>0,B1,C1) should parse."""
        tree = parse_formula("=IF(A1>0,B1,C1)")
        assert tree is not None


# ────────────────────────────────────────────────────────────────
# Parser: reference extraction
# ────────────────────────────────────────────────────────────────


class TestRefExtractionCellRef:
    def test_bare_ref_extracted(self):
        tree = parse_formula("=F2")
        scalar_refs, cell_refs = extract_all_refs(tree)
        assert scalar_refs == set()
        assert (None, "F2") in cell_refs

    def test_bare_ref_with_cross_sheet(self):
        tree = parse_formula("=Sheet1!A1 + F2")
        scalar_refs, cell_refs = extract_all_refs(tree)
        assert (None, "F2") in cell_refs
        assert ("Sheet1", "A1") in cell_refs
        assert scalar_refs == set()

    def test_bare_ref_with_scalar(self):
        tree = parse_formula("=revenue + F2")
        scalar_refs, cell_refs = extract_all_refs(tree)
        assert "revenue" in scalar_refs
        assert (None, "F2") in cell_refs

    def test_multiple_bare_refs(self):
        tree = parse_formula("=A1 + B2 + C3")
        _, cell_refs = extract_all_refs(tree)
        assert (None, "A1") in cell_refs
        assert (None, "B2") in cell_refs
        assert (None, "C3") in cell_refs

    def test_extract_refs_still_returns_scalar_only(self):
        """extract_refs (old API) returns only scalar refs, not cell refs."""
        tree = parse_formula("=revenue + F2")
        refs = extract_refs(tree)
        assert "revenue" in refs
        assert len(refs) == 1


# ────────────────────────────────────────────────────────────────
# Evaluator: bare cell refs via resolver
# ────────────────────────────────────────────────────────────────


class MockResolver:
    """Simple mock resolver for testing evaluator directly."""

    def __init__(self, cells, names=None):
        self._cells = cells  # {(sheet, addr): value}
        self._names = names or {}

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


class TestEvaluatorBareRef:
    def test_bare_ref_resolves(self):
        resolver = MockResolver({("Sheet1", "F2"): 42})
        tree = parse_formula("=F2")
        result = evaluate_formula(tree, {}, resolver=resolver, current_sheet="Sheet1")
        assert result == 42

    def test_bare_ref_arithmetic(self):
        resolver = MockResolver({("Sheet1", "A1"): 10, ("Sheet1", "B1"): 5})
        tree = parse_formula("=A1 + B1")
        result = evaluate_formula(tree, {}, resolver=resolver, current_sheet="Sheet1")
        assert result == 15

    def test_bare_ref_in_function(self):
        resolver = MockResolver({("S1", "A1"): 1, ("S1", "B1"): 2, ("S1", "C1"): 3})
        tree = parse_formula("=SUM(A1,B1,C1)")
        result = evaluate_formula(tree, {}, resolver=resolver, current_sheet="S1")
        assert result == 6

    def test_bare_ref_no_resolver_raises(self):
        tree = parse_formula("=F2")
        with pytest.raises(FormulaRefError):
            evaluate_formula(tree, {}, resolver=None, current_sheet=None)

    def test_bare_ref_no_current_sheet_raises(self):
        resolver = MockResolver({})
        tree = parse_formula("=F2")
        with pytest.raises(FormulaRefError):
            evaluate_formula(tree, {}, resolver=resolver, current_sheet=None)

    def test_mixed_bare_and_cross_sheet(self):
        resolver = MockResolver({("S1", "A1"): 10, ("Other", "A1"): 20})
        tree = parse_formula("=A1 + Other!A1")
        result = evaluate_formula(tree, {}, resolver=resolver, current_sheet="S1")
        assert result == 30

    def test_bare_ref_in_if(self):
        resolver = MockResolver({("S1", "A1"): 5, ("S1", "B1"): 10, ("S1", "C1"): 20})
        tree = parse_formula("=IF(A1>0,B1,C1)")
        result = evaluate_formula(tree, {}, resolver=resolver, current_sheet="S1")
        assert result == 10

    def test_bare_ref_in_iferror(self):
        resolver = MockResolver({("S1", "A1"): 42})
        tree = parse_formula("=IFERROR(A1, 0)")
        result = evaluate_formula(tree, {}, resolver=resolver, current_sheet="S1")
        assert result == 42


# ────────────────────────────────────────────────────────────────
# CellGraph: bare A1 refs
# ────────────────────────────────────────────────────────────────


class TestCellGraphBareRef:
    def test_simple_bare_ref(self):
        """=F2 resolves to the value of F2 in the same sheet."""
        sheets = {
            "Sheet1": {
                "F2": {"value": 2},
                "G2": {"formula": "=F2 * 3.5"},
            },
        }
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet1", "G2") == 7.0

    def test_chain_bare_refs(self):
        """F2 depends on F1, both bare refs."""
        sheets = {
            "Sheet1": {
                "F1": {"value": 10},
                "F2": {"formula": "=F1 * 2"},
                "F3": {"formula": "=F2 + 5"},
            },
        }
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet1", "F2") == 20
        assert cg.evaluate_cell("Sheet1", "F3") == 25

    def test_bare_ref_with_cross_sheet(self):
        """Formula mixes bare ref and cross-sheet ref."""
        sheets = {
            "Sheet1": {
                "A1": {"value": 100},
            },
            "Sheet2": {
                "B1": {"value": 50},
                "B2": {"formula": "=Sheet1!A1 + B1"},
            },
        }
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet2", "B2") == 150

    def test_bare_ref_in_function(self):
        sheets = {
            "Sheet1": {
                "A1": {"value": 1},
                "A2": {"value": 2},
                "A3": {"value": 3},
                "B1": {"formula": "=SUM(A1,A2,A3)"},
            },
        }
        cg = CellGraph(sheets)
        assert cg.evaluate_cell("Sheet1", "B1") == 6

    def test_max_neg_bare_ref(self):
        """=MAX(-F2,-3,F2*3.5) evaluates correctly."""
        sheets = {
            "Sheet1": {
                "F2": {"value": 2},
                "G2": {"formula": "=MAX(-F2,-3,F2*3.5)"},
            },
        }
        cg = CellGraph(sheets)
        # MAX(-2, -3, 7.0) = 7.0
        assert cg.evaluate_cell("Sheet1", "G2") == 7.0

    def test_evaluate_all_with_bare_refs(self):
        sheets = {
            "Sheet1": {
                "A1": {"value": 10},
                "A2": {"formula": "=A1 + 5"},
            },
        }
        cg = CellGraph(sheets)
        results = cg.evaluate_all()
        assert results["Sheet1"]["A1"] == 10
        assert results["Sheet1"]["A2"] == 15

    def test_display_value_with_bare_ref(self):
        sheets = {
            "Sheet1": {
                "A1": {"value": 5},
                "A2": {"formula": "=A1 * 2"},
            },
        }
        cg = CellGraph(sheets)
        assert cg.get_display_value("Sheet1", "A2") == "10"


# ────────────────────────────────────────────────────────────────
# CellGraph: cycle detection with bare refs
# ────────────────────────────────────────────────────────────────


class TestCellGraphCyclesBareRef:
    def test_self_reference_bare(self):
        sheets = {"Sheet1": {"A1": {"formula": "=A1"}}}
        cg = CellGraph(sheets)
        with pytest.raises(CellCycleError):
            cg.evaluate_cell("Sheet1", "A1")

    def test_two_cell_cycle_bare(self):
        sheets = {
            "Sheet1": {
                "A1": {"formula": "=A2"},
                "A2": {"formula": "=A1"},
            },
        }
        cg = CellGraph(sheets)
        with pytest.raises(CellCycleError):
            cg.evaluate_cell("Sheet1", "A1")

    def test_cycle_mixed_bare_and_cross_sheet(self):
        """Cycle detection works when bare refs and cross-sheet refs are mixed."""
        sheets = {
            "Sheet1": {"A1": {"formula": "=Sheet2!A1"}},
            "Sheet2": {"A1": {"formula": "=A1"}},  # self-ref via bare
        }
        cg = CellGraph(sheets)
        with pytest.raises(CellCycleError):
            cg.evaluate_cell("Sheet2", "A1")

    def test_display_value_cycle_bare_ref(self):
        sheets = {"Sheet1": {"A1": {"formula": "=A1"}}}
        cg = CellGraph(sheets)
        assert cg.get_display_value("Sheet1", "A1") == "#CIRC!"


# ────────────────────────────────────────────────────────────────
# Import classification: cell refs no longer cause parse_error
# ────────────────────────────────────────────────────────────────


class TestImportClassificationCellRef:
    def test_max_neg_f2_neg3_f2_times_3point5_supported(self):
        result = classify_formula("=MAX(-F2,-3,F2*3.5)")
        assert result["classification"] != "parse_error", (
            f"Expected supported, got parse_error: {result['error_message']}"
        )
        assert result["classification"] == "supported"

    def test_f2_times_3point5_supported(self):
        result = classify_formula("=F2*3.5")
        assert result["classification"] != "parse_error"
        assert result["classification"] == "supported"

    def test_simple_cell_ref_supported(self):
        result = classify_formula("=F2")
        assert result["classification"] == "supported"

    def test_sum_cell_refs_supported(self):
        result = classify_formula("=SUM(A1,B2,C3)")
        assert result["classification"] == "supported"

    def test_if_with_cell_refs_supported(self):
        result = classify_formula("=IF(A1>0,B1,C1)")
        assert result["classification"] == "supported"

    def test_cell_ref_arithmetic_supported(self):
        result = classify_formula("=A1+B2*C3-D4/E5")
        assert result["classification"] == "supported"

    def test_unary_plus_cell_ref_supported(self):
        result = classify_formula("=+F2*3.5")
        assert result["classification"] != "parse_error"

    def test_mixed_cross_sheet_and_bare_supported(self):
        result = classify_formula("=Sheet1!A1 + F2")
        assert result["classification"] == "supported"


# ────────────────────────────────────────────────────────────────
# UI Service: computed viewport with bare A1 refs
# ────────────────────────────────────────────────────────────────


class TestServiceViewportBareRef:
    def test_bare_ref_formula_display(self, insheet_service: ProjectService):
        """B3 = B1 * B2 = 10 * 5 = 50."""
        vp = insheet_service.get_sheet_viewport("Sheet1", 0, 0, 10, 10)
        cells = {c["addr"]: c for c in vp["cells"]}
        assert cells["B3"]["display"] == "50"
        assert cells["B3"]["raw"] == "=B1 * B2"

    def test_chain_bare_ref_display(self, insheet_service: ProjectService):
        """B4 = B3 * 0.1 = 50 * 0.1 = 5."""
        vp = insheet_service.get_sheet_viewport("Sheet1", 0, 0, 10, 10)
        cells = {c["addr"]: c for c in vp["cells"]}
        assert cells["B4"]["display"] == "5"

    def test_g2_formula_display(self, insheet_service: ProjectService):
        """G2 = F2 * 3.5 = 2 * 3.5 = 7."""
        vp = insheet_service.get_sheet_viewport("Sheet1", 0, 0, 10, 10)
        cells = {c["addr"]: c for c in vp["cells"]}
        assert cells["G2"]["display"] == "7"

    def test_cross_sheet_plus_bare_ref(self, insheet_service: ProjectService):
        """Summary!A2 = Sheet1!B3 + A1 where A1=Sheet1!B3=50, so 50+50=100."""
        vp = insheet_service.get_sheet_viewport("Summary", 0, 0, 10, 10)
        cells = {c["addr"]: c for c in vp["cells"]}
        assert cells["A1"]["display"] == "50"
        assert cells["A2"]["display"] == "100"

    def test_cell_edit_invalidates_bare_refs(self, insheet_service: ProjectService):
        """Editing B1 should propagate through B3 and B4."""
        insheet_service.update_cells("Sheet1", [{"addr": "B1", "value": "20"}])
        vp = insheet_service.get_sheet_viewport("Sheet1", 0, 0, 10, 10)
        cells = {c["addr"]: c for c in vp["cells"]}
        # B3 = 20 * 5 = 100
        assert cells["B3"]["display"] == "100"
        # B4 = 100 * 0.1 = 10
        assert cells["B4"]["display"] == "10"
