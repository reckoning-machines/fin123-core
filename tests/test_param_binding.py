"""Tests for PARAM() proxy binding in CellGraph and UI service."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


# ---------------------------------------------------------------------------
# A) PARAM() evaluation in CellGraph
# ---------------------------------------------------------------------------


class TestParamEval:
    def test_param_resolves_from_context(self):
        from fin123.cell_graph import CellGraph

        sheets = {
            "Sheet1": {
                "A1": {"formula": '=PARAM("ticker")'},
            },
        }
        cg = CellGraph(sheets, params={"ticker": "AAPL"})
        assert cg.evaluate_cell("Sheet1", "A1") == "AAPL"

    def test_param_numeric(self):
        from fin123.cell_graph import CellGraph

        sheets = {
            "Sheet1": {
                "A1": {"formula": '=PARAM("rate")'},
            },
        }
        cg = CellGraph(sheets, params={"rate": 0.05})
        assert cg.evaluate_cell("Sheet1", "A1") == 0.05

    def test_param_in_expression(self):
        from fin123.cell_graph import CellGraph

        sheets = {
            "Sheet1": {
                "A1": {"formula": '=PARAM("price") * 2'},
            },
        }
        cg = CellGraph(sheets, params={"price": 100.0})
        assert cg.evaluate_cell("Sheet1", "A1") == 200.0

    def test_param_unknown_returns_none_with_error(self):
        from fin123.cell_graph import CellGraph

        sheets = {
            "Sheet1": {
                "A1": {"formula": '=PARAM("missing")'},
            },
        }
        cg = CellGraph(sheets, params={"ticker": "AAPL"})
        result = cg.evaluate_cell("Sheet1", "A1")
        assert result is None
        errors = cg.get_errors()
        assert len(errors) == 1
        assert "missing" in str(list(errors.values())[0])

    def test_dollar_ref_resolves_from_params(self):
        from fin123.cell_graph import CellGraph

        sheets = {
            "Sheet1": {
                "A1": {"formula": "=$ticker"},
            },
        }
        cg = CellGraph(sheets, params={"ticker": "MSFT"})
        assert cg.evaluate_cell("Sheet1", "A1") == "MSFT"

    def test_param_no_params_gives_error(self):
        from fin123.cell_graph import CellGraph

        sheets = {
            "Sheet1": {
                "A1": {"formula": '=PARAM("x")'},
            },
        }
        cg = CellGraph(sheets)
        result = cg.evaluate_cell("Sheet1", "A1")
        assert result is None
        assert cg.get_errors()


# ---------------------------------------------------------------------------
# B) scan_param_bindings
# ---------------------------------------------------------------------------


class TestScanParamBindings:
    def test_basic_scan(self):
        from fin123.cell_graph import scan_param_bindings

        sheets = [
            {
                "name": "Sheet1",
                "cells": {
                    "A1": {"formula": '=PARAM("ticker")'},
                    "B1": {"value": 42},
                },
            },
        ]
        bindings, errors = scan_param_bindings(sheets)
        assert errors == []
        assert bindings == {"ticker": ("Sheet1", "A1")}

    def test_duplicate_param_error(self):
        from fin123.cell_graph import scan_param_bindings

        sheets = [
            {
                "name": "Sheet1",
                "cells": {
                    "A1": {"formula": '=PARAM("ticker")'},
                },
            },
            {
                "name": "Sheet2",
                "cells": {
                    "B2": {"formula": '=PARAM("ticker")'},
                },
            },
        ]
        bindings, errors = scan_param_bindings(sheets)
        assert len(errors) == 1
        assert "Duplicate" in errors[0]

    def test_no_param_cells(self):
        from fin123.cell_graph import scan_param_bindings

        sheets = [
            {
                "name": "Sheet1",
                "cells": {
                    "A1": {"formula": "=1+2"},
                    "B1": {"value": "hello"},
                },
            },
        ]
        bindings, errors = scan_param_bindings(sheets)
        assert bindings == {}
        assert errors == []

    def test_case_insensitive_match(self):
        from fin123.cell_graph import scan_param_bindings

        sheets = [
            {
                "name": "Sheet1",
                "cells": {
                    "A1": {"formula": '=param("rate")'},
                },
            },
        ]
        bindings, errors = scan_param_bindings(sheets)
        assert "rate" in bindings

    def test_multiple_params(self):
        from fin123.cell_graph import scan_param_bindings

        sheets = [
            {
                "name": "Data",
                "cells": {
                    "A1": {"formula": '=PARAM("ticker")'},
                    "A2": {"formula": '=PARAM("rate")'},
                    "A3": {"formula": '=PARAM("period")'},
                },
            },
        ]
        bindings, errors = scan_param_bindings(sheets)
        assert len(bindings) == 3
        assert errors == []


# ---------------------------------------------------------------------------
# C) unbind_param
# ---------------------------------------------------------------------------


class TestUnbindParam:
    @pytest.fixture
    def project_dir(self, tmp_path: Path) -> Path:
        from fin123.project import scaffold_project

        return scaffold_project(tmp_path / "proj")

    def test_unbind_replaces_formula(self, project_dir):
        import yaml
        from fin123.ui.service import ProjectService

        # Set up workbook with PARAM cell and params
        spec_path = project_dir / "workbook.yaml"
        spec = yaml.safe_load(spec_path.read_text()) or {}
        spec["params"] = {"ticker": "AAPL"}
        spec["sheets"] = [
            {
                "name": "Sheet1",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {"A1": {"formula": '=PARAM("ticker")'}},
            },
        ]
        spec_path.write_text(yaml.dump(spec, default_flow_style=False))

        svc = ProjectService(project_dir=project_dir)
        result = svc.unbind_param("Sheet1", "A1")
        assert result["ok"] is True
        assert result["value"] == "AAPL"

    def test_unbind_non_param_raises(self, project_dir):
        import yaml
        from fin123.ui.service import ProjectService

        spec_path = project_dir / "workbook.yaml"
        spec = yaml.safe_load(spec_path.read_text()) or {}
        spec["sheets"] = [
            {
                "name": "Sheet1",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {"A1": {"formula": "=1+2"}},
            },
        ]
        spec_path.write_text(yaml.dump(spec, default_flow_style=False))

        svc = ProjectService(project_dir=project_dir)
        with pytest.raises(ValueError, match="not a PARAM formula"):
            svc.unbind_param("Sheet1", "A1")


# ---------------------------------------------------------------------------
# D) Auto-declare on commit
# ---------------------------------------------------------------------------


class TestAutoDeclarOnCommit:
    @pytest.fixture
    def project_dir(self, tmp_path: Path) -> Path:
        from fin123.project import scaffold_project

        return scaffold_project(tmp_path / "proj")

    def test_auto_declare_new_param(self, project_dir):
        import yaml
        from fin123.ui.service import ProjectService

        spec_path = project_dir / "workbook.yaml"
        spec = yaml.safe_load(spec_path.read_text()) or {}
        # Start with no params
        spec.pop("params", None)
        spec["sheets"] = [
            {
                "name": "Sheet1",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {"A1": {"formula": '=PARAM("new_param")'}},
            },
        ]
        spec_path.write_text(yaml.dump(spec, default_flow_style=False))

        svc = ProjectService(project_dir=project_dir)
        # The cell won't eval properly (PARAM("new_param") fails since
        # new_param is not in params yet), but we still test the commit flow
        svc.save_snapshot()

        # Re-read the spec
        updated = yaml.safe_load(spec_path.read_text()) or {}
        assert "new_param" in updated.get("params", {})
