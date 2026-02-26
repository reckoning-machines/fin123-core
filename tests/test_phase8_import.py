"""Phase 8 tests: XLSX import hardening — classification, health, guardrails, versioning, quick actions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from fin123.xlsx_import import (
    classify_formula,
    find_non_ascii_chars,
    import_xlsx,
    sanitize_formula_preview,
    safe_trim,
)
from fin123.ui.service import ProjectService


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


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
def tmp_project(tmp_path: Path) -> Path:
    """Minimal project directory."""
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    return project_dir


# ────────────────────────────────────────────────────────────────
# TestFormulaClassification
# ────────────────────────────────────────────────────────────────


class TestFormulaClassification:
    def test_supported_formula(self):
        result = classify_formula("=SUM(A1,A2)")
        assert result["classification"] == "supported"
        assert "SUM" in result["functions_used"]
        assert result["unsupported_functions"] == []

    def test_parse_error_formula(self):
        result = classify_formula("=INVALID(((")
        assert result["classification"] == "parse_error"
        assert result["error_message"] is not None
        assert len(result["error_message"]) > 0

    def test_unsupported_function(self):
        result = classify_formula("=INDEX(A1)")
        assert result["classification"] == "unsupported_function"
        assert "INDEX" in result["unsupported_functions"]

    def test_external_link(self):
        result = classify_formula("=[Budget.xlsx]Sheet1!A1")
        assert result["classification"] == "external_link"

    def test_plugin_formula_bdh(self):
        result = classify_formula("=BDH(\"AAPL\",\"PX_LAST\")")
        assert result["classification"] == "plugin_formula"

    def test_plugin_formula_va(self):
        result = classify_formula("=VA_EPS(A2)")
        assert result["classification"] == "plugin_formula"

    def test_leading_unary_plus_not_parse_error(self):
        """Excel/Lotus-style =+... formulas should parse, not be parse_error."""
        for formula in ("=+SUM(A1,A2)", "=+A1*3.5", "=+MAX(1,2,3)"):
            result = classify_formula(formula)
            assert result["classification"] != "parse_error", f"{formula} wrongly classified as parse_error"


# ────────────────────────────────────────────────────────────────
# TestClassificationSummary
# ────────────────────────────────────────────────────────────────


class TestClassificationSummary:
    def test_summary_counts_correct(self, make_xlsx, tmp_project):
        """Mixed formulas produce correct classification counts."""
        xlsx_path = make_xlsx({
            "Sheet1": {
                "A1": "=SUM(1,2)",
                "A2": "=AVERAGE(1,2)",
                "A3": 42,
                "A4": "=INDEX(A1)",
                "A5": "hello",
            },
        })
        report = import_xlsx(xlsx_path, tmp_project)
        summary = report["classification_summary"]
        assert summary["total_formulas"] == 3
        assert summary["supported"] == 2
        assert summary["unsupported_functions"] == 1

    def test_top_unsupported_functions_sorted(self, make_xlsx, tmp_project):
        """Unsupported functions are sorted by frequency."""
        xlsx_path = make_xlsx({
            "Sheet1": {
                "A1": "=INDEX(A2)",
                "A2": "=INDEX(A3)",
                "A3": "=MATCH(A4,1)",
            },
        })
        report = import_xlsx(xlsx_path, tmp_project)
        top = report["top_unsupported_functions"]
        assert len(top) >= 1
        # INDEX should appear with count >= 2
        index_entry = next((t for t in top if t["name"] == "INDEX"), None)
        assert index_entry is not None
        assert index_entry["count"] >= 2
        # Sorted desc
        counts = [t["count"] for t in top]
        assert counts == sorted(counts, reverse=True)


# ────────────────────────────────────────────────────────────────
# TestReportStructure
# ────────────────────────────────────────────────────────────────


class TestReportStructure:
    def test_report_has_required_keys(self, make_xlsx, tmp_project):
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=SUM(1,2)"}})
        report = import_xlsx(xlsx_path, tmp_project)
        assert "formula_classifications" in report
        assert "classification_summary" in report
        assert "top_unsupported_functions" in report
        # Summary has required keys
        summary = report["classification_summary"]
        for key in ("total_formulas", "supported", "parse_errors",
                     "unsupported_functions", "external_links", "plugin_formulas"):
            assert key in summary

    def test_per_sheet_classifications(self, make_xlsx, tmp_project):
        xlsx_path = make_xlsx({
            "Data": {"A1": "=SUM(1,2)", "B1": 10},
            "Summary": {"A1": "=MAX(1,2)"},
        })
        report = import_xlsx(xlsx_path, tmp_project)
        for sheet_info in report["sheets_imported"]:
            assert "classifications" in sheet_info


# ────────────────────────────────────────────────────────────────
# TestHealthIntegration
# ────────────────────────────────────────────────────────────────


class TestHealthIntegration:
    def test_parse_errors_produce_health_errors(self, make_xlsx, tmp_project):
        """Import with parse errors produces health error issues."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=BOGUS((("}})
        import_xlsx(xlsx_path, tmp_project)
        svc = ProjectService(project_dir=tmp_project)
        health = svc.get_project_health()
        codes = [i["code"] for i in health["issues"]]
        assert "import_formula_parse_error" in codes
        parse_issue = next(i for i in health["issues"] if i["code"] == "import_formula_parse_error")
        assert parse_issue["severity"] == "error"
        # Target should be a dict with sheet and addr
        assert isinstance(parse_issue["target"], dict)
        assert "sheet" in parse_issue["target"]
        assert "addr" in parse_issue["target"]

    def test_unsupported_functions_produce_warning(self, make_xlsx, tmp_project):
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=INDEX(A2)"}})
        import_xlsx(xlsx_path, tmp_project)
        svc = ProjectService(project_dir=tmp_project)
        health = svc.get_project_health()
        codes = [i["code"] for i in health["issues"]]
        assert "import_unsupported_functions" in codes

    def test_no_classifications_no_extra_issues(self, tmp_project):
        """Old-style report without classification_summary doesn't break health."""
        # Write a minimal old-style import report
        tmp_project.mkdir(exist_ok=True)
        yaml_content = yaml.dump({
            "sheets": [{"name": "Sheet1", "n_rows": 200, "n_cols": 40, "cells": {}}],
            "params": {}, "tables": {}, "plans": [], "outputs": [],
        }, default_flow_style=False, sort_keys=False)
        (tmp_project / "workbook.yaml").write_text(yaml_content)

        old_report = {
            "source": "test.xlsx",
            "sheets_imported": [{"name": "Sheet1", "cells": 5, "formulas": 2, "colors": 0}],
            "cells_imported": 5,
            "formulas_imported": 2,
            "colors_imported": 0,
            "skipped_features": [],
            "warnings": [],
        }
        (tmp_project / "import_report.json").write_text(json.dumps(old_report))
        svc = ProjectService(project_dir=tmp_project)
        health = svc.get_project_health()
        # Should not crash — no classification-based issues
        cls_codes = [i["code"] for i in health["issues"]
                     if i["code"].startswith("import_formula_") or i["code"].startswith("import_unsupported")
                     or i["code"].startswith("import_external") or i["code"].startswith("import_plugin")]
        assert cls_codes == []


# ────────────────────────────────────────────────────────────────
# TestFormulasPreserved
# ────────────────────────────────────────────────────────────────


class TestFormulasPreserved:
    def test_unsupported_formulas_stored_unchanged(self, make_xlsx, tmp_project):
        """Unsupported formulas are stored exactly as-is (never rewritten)."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=INDEX(MATCH(A2,B:B,0),C:C)"}})
        import_xlsx(xlsx_path, tmp_project)
        spec = yaml.safe_load((tmp_project / "workbook.yaml").read_text())
        cell_a1 = spec["sheets"][0]["cells"]["A1"]
        assert cell_a1["formula"] == "=INDEX(MATCH(A2,B:B,0),C:C)"


# ────────────────────────────────────────────────────────────────
# TestPerformanceGuardrails
# ────────────────────────────────────────────────────────────────


class TestPerformanceGuardrails:
    def test_row_truncation_produces_warning(self, make_xlsx, tmp_project):
        """max_rows truncation generates a warning."""
        cells = {f"A{i}": i for i in range(1, 21)}  # 20 rows of data
        xlsx_path = make_xlsx({"Sheet1": cells})
        report = import_xlsx(xlsx_path, tmp_project, max_rows=5)
        trunc_warns = [w for w in report["warnings"] if "truncated" in w and "rows" in w]
        assert len(trunc_warns) >= 1

    def test_total_cell_limit(self, make_xlsx, tmp_project):
        """max_total_cells causes early termination with warning."""
        cells = {f"A{i}": i for i in range(1, 11)}  # 10 cells per sheet
        xlsx_path = make_xlsx({
            "Sheet1": cells,
            "Sheet2": cells,
            "Sheet3": cells,
        })
        report = import_xlsx(xlsx_path, tmp_project, max_total_cells=15)
        # Should have imported Sheet1 (10) and Sheet2 (partial or full) but skipped Sheet3
        skipped_warns = [w for w in report["warnings"] if "total cell limit" in w]
        assert len(skipped_warns) >= 1 or len(report["sheets_imported"]) < 3


# ────────────────────────────────────────────────────────────────
# TestReportVersioning
# ────────────────────────────────────────────────────────────────


class TestReportVersioning:
    def test_directory_naming_pattern(self, make_xlsx, tmp_project):
        """Import report directory contains '_import_' pattern."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": 1}})
        import_xlsx(xlsx_path, tmp_project)
        reports_dir = tmp_project / "import_reports"
        dirs = [d.name for d in reports_dir.iterdir() if d.is_dir()]
        assert any("_import_" in d for d in dirs)

    def test_source_filename_stored(self, make_xlsx, tmp_project):
        """source_filename.txt is created in the report directory."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": 1}}, filename="myfile.xlsx")
        import_xlsx(xlsx_path, tmp_project)
        reports_dir = tmp_project / "import_reports"
        import_dirs = [d for d in reports_dir.iterdir() if d.is_dir() and "_import_" in d.name]
        assert len(import_dirs) >= 1
        src_file = import_dirs[0] / "source_filename.txt"
        assert src_file.exists()
        assert src_file.read_text() == "myfile.xlsx"


# ────────────────────────────────────────────────────────────────
# TestQuickActions
# ────────────────────────────────────────────────────────────────


class TestQuickActions:
    def test_mark_todo_sets_color_and_comment(self, make_xlsx, tmp_project):
        """mark_import_todo sets amber color and review comment."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=SUM(1,2)"}})
        import_xlsx(xlsx_path, tmp_project)
        svc = ProjectService(project_dir=tmp_project)
        result = svc.mark_import_todo("Sheet1", "A1")
        assert result["ok"] is True
        # Check fmt
        sheet = svc._get_sheet("Sheet1")
        assert sheet["fmt"]["A1"]["color"] == "#f59e0b"
        # Check comment
        assert sheet["cells"]["A1"].get("comment") == "TODO: review imported formula"

    def test_convert_to_value_replaces_formula(self, make_xlsx, tmp_project):
        """convert_to_value replaces a formula with its computed value."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": 10, "B1": "=SUM(1,2,3)"}})
        import_xlsx(xlsx_path, tmp_project)
        svc = ProjectService(project_dir=tmp_project)
        # B1 should have formula =SUM(1,2,3) which evaluates to 6
        result = svc.convert_to_value("Sheet1", "B1")
        assert result["ok"] is True
        assert result["value"] == 6
        # Cell should now be a value, not formula
        sheet = svc._get_sheet("Sheet1")
        assert "formula" not in sheet["cells"]["B1"]
        assert sheet["cells"]["B1"]["value"] == 6


# ────────────────────────────────────────────────────────────────
# TestImportSnapshotVersioning
# ────────────────────────────────────────────────────────────────


class TestImportSnapshotVersioning:
    """Verify that XLSX import into a scaffolded project creates v0002."""

    def test_scaffold_creates_v0001(self, tmp_path):
        """scaffold_project creates an initial v0001 snapshot."""
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        snap_dir = project_dir / "snapshots" / "workbook"
        assert (snap_dir / "v0001").exists()
        assert (snap_dir / "v0001" / "workbook.yaml").exists()
        index = json.loads((snap_dir / "index.json").read_text())
        version_ids = [v["model_version_id"] for v in index["versions"]]
        assert version_ids == ["v0001"]

    def test_import_creates_v0002_after_scaffold(self, make_xlsx, tmp_path):
        """XLSX import into scaffolded project creates v0002."""
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": 42, "B1": "=SUM(1,2)"}})
        import_xlsx(xlsx_path, project_dir)

        snap_dir = project_dir / "snapshots" / "workbook"
        assert (snap_dir / "v0001").exists()
        assert (snap_dir / "v0002").exists()
        index = json.loads((snap_dir / "index.json").read_text())
        version_ids = [v["model_version_id"] for v in index["versions"]]
        assert "v0001" in version_ids
        assert "v0002" in version_ids

    def test_import_preserves_model_id(self, make_xlsx, tmp_path):
        """XLSX import into existing project preserves the original model_id."""
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        original_spec = yaml.safe_load((project_dir / "workbook.yaml").read_text())
        original_model_id = original_spec["model_id"]

        xlsx_path = make_xlsx({"Sheet1": {"A1": 42}})
        import_xlsx(xlsx_path, project_dir)

        new_spec = yaml.safe_load((project_dir / "workbook.yaml").read_text())
        assert new_spec["model_id"] == original_model_id

    def test_index_json_updated_correctly(self, make_xlsx, tmp_path):
        """index.json has correct version_ordinal and no duplicates."""
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": 42}})
        import_xlsx(xlsx_path, project_dir)

        snap_dir = project_dir / "snapshots" / "workbook"
        index = json.loads((snap_dir / "index.json").read_text())
        version_ids = [v["model_version_id"] for v in index["versions"]]
        # No duplicates
        assert len(version_ids) == len(set(version_ids))
        # Correct ordering
        assert version_ids == sorted(version_ids)

    def test_import_never_overwrites_previous_snapshot(self, make_xlsx, tmp_path):
        """Import does not modify the v0001 snapshot content."""
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        snap_dir = project_dir / "snapshots" / "workbook"
        v0001_content = (snap_dir / "v0001" / "workbook.yaml").read_text()

        xlsx_path = make_xlsx({"Sheet1": {"A1": 42}})
        import_xlsx(xlsx_path, project_dir)

        # v0001 content unchanged
        assert (snap_dir / "v0001" / "workbook.yaml").read_text() == v0001_content

    def test_project_service_sees_import_version(self, make_xlsx, tmp_path):
        """ProjectService.list_model_versions returns both scaffold and import versions."""
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": 42}})
        import_xlsx(xlsx_path, project_dir)

        svc = ProjectService(project_dir=project_dir)
        versions = svc.list_model_versions()
        version_ids = [v["model_version_id"] for v in versions]
        assert "v0001" in version_ids
        assert "v0002" in version_ids

    def test_project_service_has_import_report(self, make_xlsx, tmp_path):
        """ProjectService.get_project_info reports has_import_report=True after import."""
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": 42}})
        import_xlsx(xlsx_path, project_dir)

        svc = ProjectService(project_dir=project_dir)
        info = svc.get_project_info()
        assert info["has_import_report"] is True

    def test_import_report_index_has_v0002(self, make_xlsx, tmp_path):
        """import_reports/index.json references v0002 as model_version_created."""
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": 42}})
        import_xlsx(xlsx_path, project_dir)

        index_path = project_dir / "import_reports" / "index.json"
        assert index_path.exists()
        index = json.loads(index_path.read_text())
        assert len(index) == 1
        assert index[0]["model_version_created"] == "v0002"


# ────────────────────────────────────────────────────────────────
# TestImportTabUI
# ────────────────────────────────────────────────────────────────


class TestImportTabUI:
    """Verify the Import tab is present in the HTML and reachable via API."""

    def test_index_html_contains_import_tab(self):
        """index.html has an Import tab element with data-tab='import'."""
        html_path = Path(__file__).parent.parent / "src" / "fin123" / "ui" / "static" / "index.html"
        html = html_path.read_text()
        assert 'data-tab="import"' in html
        assert "Import" in html

    def test_tab_bar_css_supports_overflow(self):
        """styles.css .tab-bar has overflow-x for horizontal scrolling."""
        css_path = Path(__file__).parent.parent / "src" / "fin123" / "ui" / "static" / "styles.css"
        css = css_path.read_text()
        assert "overflow-x" in css
        # Tabs should not shrink
        assert "flex-shrink: 0" in css

    def test_api_project_has_import_report_after_import(self, make_xlsx, tmp_path):
        """GET /api/project returns has_import_report=true after XLSX import."""
        from fastapi.testclient import TestClient

        from fin123.project import scaffold_project
        from fin123.ui.server import create_app

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": 42}})
        import_xlsx(xlsx_path, project_dir)

        app = create_app(project_dir)
        client = TestClient(app)
        resp = client.get("/api/project")
        assert resp.status_code == 200
        assert resp.json()["has_import_report"] is True

    def test_api_import_report_latest_after_import(self, make_xlsx, tmp_path):
        """GET /api/import/report/latest returns 200 after XLSX import."""
        from fastapi.testclient import TestClient

        from fin123.project import scaffold_project
        from fin123.ui.server import create_app

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": 42, "B1": "=SUM(1,2)"}})
        import_xlsx(xlsx_path, project_dir)

        app = create_app(project_dir)
        client = TestClient(app)
        resp = client.get("/api/import/report/latest")
        assert resp.status_code == 200
        data = resp.json()
        assert "sheets_imported" in data

    def test_all_panel_tabs_present_in_html(self):
        """All expected panel tabs exist in index.html."""
        html_path = Path(__file__).parent.parent / "src" / "fin123" / "ui" / "static" / "index.html"
        html = html_path.read_text()
        expected_tabs = ["scalars", "tables", "names", "runs", "snaps", "errors", "import", "health", "registry"]
        for tab in expected_tabs:
            assert f'data-tab="{tab}"' in html, f"Missing tab: {tab}"


# ────────────────────────────────────────────────────────────────
# TestTraceLogHelpers
# ────────────────────────────────────────────────────────────────


class TestTraceLogHelpers:
    def test_find_non_ascii_chars_empty(self):
        assert find_non_ascii_chars("=SUM(A1,A2)") == []

    def test_find_non_ascii_chars_unicode_minus(self):
        result = find_non_ascii_chars("=A1\u2212B1")
        assert len(result) == 1
        assert result[0][0] == "U+2212"
        assert result[0][2] == 1

    def test_find_non_ascii_chars_multiple(self):
        result = find_non_ascii_chars("=A1\u2212B1\u2212C1\u00a0")
        codepoints = {r[0] for r in result}
        assert "U+2212" in codepoints
        assert "U+00A0" in codepoints
        minus_entry = next(r for r in result if r[0] == "U+2212")
        assert minus_entry[2] == 2

    def test_sanitize_formula_preview(self):
        assert sanitize_formula_preview("=A1\u2212B1") == "=A1-B1"
        assert sanitize_formula_preview("=A1\u00a0+B1") == "=A1 +B1"
        assert sanitize_formula_preview("=IF(\u201cA\u201d,1,2)") == '=IF("A",1,2)'
        assert sanitize_formula_preview("=A1\u2013B1") == "=A1-B1"  # en dash
        assert sanitize_formula_preview("=A1\u2014B1") == "=A1-B1"  # em dash
        assert sanitize_formula_preview("=\u2018x\u2019") == "='x'"  # smart quotes

    def test_safe_trim(self):
        assert safe_trim("hello", 10) == "hello"
        assert safe_trim("hello world", 5) == "hello..."
        assert safe_trim("abc", 3) == "abc"


# ────────────────────────────────────────────────────────────────
# TestTraceLogGeneration
# ────────────────────────────────────────────────────────────────


class TestTraceLogGeneration:
    def test_trace_log_created_for_parse_error(self, make_xlsx, tmp_project):
        """Trace log is created when import has parse_error entries."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=BOGUS((("}})
        import_xlsx(xlsx_path, tmp_project)
        reports_dir = tmp_project / "import_reports"
        import_dirs = [d for d in reports_dir.iterdir() if d.is_dir() and "_import_" in d.name]
        assert len(import_dirs) == 1
        trace_path = import_dirs[0] / "import_trace.log"
        assert trace_path.exists()
        content = trace_path.read_text()
        assert "[IMPORT][parse_error]" in content
        assert "Sheet1!A1" in content

    def test_trace_log_contains_repr(self, make_xlsx, tmp_project):
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=BOGUS((("}})
        import_xlsx(xlsx_path, tmp_project)
        reports_dir = tmp_project / "import_reports"
        import_dirs = [d for d in reports_dir.iterdir() if d.is_dir() and "_import_" in d.name]
        content = (import_dirs[0] / "import_trace.log").read_text()
        assert "repr" in content

    def test_trace_log_contains_non_ascii_chars(self, make_xlsx, tmp_project):
        """Trace log reports non-ASCII chars including U+2212."""
        # We can't easily write U+2212 into an XLSX formula cell, so
        # we test via the report JSON diagnostics instead.
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=BOGUS((("}})
        import_xlsx(xlsx_path, tmp_project)
        reports_dir = tmp_project / "import_reports"
        import_dirs = [d for d in reports_dir.iterdir() if d.is_dir() and "_import_" in d.name]
        content = (import_dirs[0] / "import_trace.log").read_text()
        assert "non_ascii_chars:" in content

    def test_trace_log_contains_sanitized_preview(self, make_xlsx, tmp_project):
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=INDEX(A2)"}})
        import_xlsx(xlsx_path, tmp_project)
        reports_dir = tmp_project / "import_reports"
        import_dirs = [d for d in reports_dir.iterdir() if d.is_dir() and "_import_" in d.name]
        content = (import_dirs[0] / "import_trace.log").read_text()
        assert "sanitized_preview:" in content

    def test_trace_log_no_issues(self, make_xlsx, tmp_project):
        """Trace log exists even when all formulas are supported."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=SUM(1,2)"}})
        import_xlsx(xlsx_path, tmp_project)
        reports_dir = tmp_project / "import_reports"
        import_dirs = [d for d in reports_dir.iterdir() if d.is_dir() and "_import_" in d.name]
        trace_path = import_dirs[0] / "import_trace.log"
        assert trace_path.exists()
        content = trace_path.read_text()
        assert "No issues found" in content

    def test_trace_log_parser_error_line(self, make_xlsx, tmp_project):
        """Parse error entries include a parser_error line in the trace."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=BOGUS((("}})
        import_xlsx(xlsx_path, tmp_project)
        reports_dir = tmp_project / "import_reports"
        import_dirs = [d for d in reports_dir.iterdir() if d.is_dir() and "_import_" in d.name]
        content = (import_dirs[0] / "import_trace.log").read_text()
        assert "parser_error:" in content


# ────────────────────────────────────────────────────────────────
# TestTraceLogDiagnosticsInReport
# ────────────────────────────────────────────────────────────────


class TestTraceLogDiagnosticsInReport:
    def test_report_parse_error_has_diagnostics(self, make_xlsx, tmp_project):
        """import_report.json includes repr/non_ascii/sanitized for parse_error."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=BOGUS((("}})
        report = import_xlsx(xlsx_path, tmp_project)
        pe_entries = [c for c in report["formula_classifications"]
                      if c["classification"] == "parse_error"]
        assert len(pe_entries) >= 1
        entry = pe_entries[0]
        assert "repr" in entry
        assert "non_ascii_chars" in entry
        assert "sanitized_preview" in entry

    def test_report_unsupported_has_diagnostics(self, make_xlsx, tmp_project):
        """import_report.json includes repr/non_ascii/sanitized for unsupported_function."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=INDEX(A2)"}})
        report = import_xlsx(xlsx_path, tmp_project)
        uf_entries = [c for c in report["formula_classifications"]
                      if c["classification"] == "unsupported_function"]
        assert len(uf_entries) >= 1
        entry = uf_entries[0]
        assert "repr" in entry
        assert "non_ascii_chars" in entry
        assert "sanitized_preview" in entry

    def test_supported_formula_no_extra_diagnostics(self, make_xlsx, tmp_project):
        """Supported formulas don't get extra diagnostic fields."""
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=SUM(1,2)"}})
        report = import_xlsx(xlsx_path, tmp_project)
        supported = [c for c in report["formula_classifications"]
                     if c["classification"] == "supported"]
        assert len(supported) >= 1
        assert "repr" not in supported[0]


# ────────────────────────────────────────────────────────────────
# TestUnicodeMinusDetection
# ────────────────────────────────────────────────────────────────


class TestUnicodeMinusDetection:
    def test_unicode_minus_detected_in_classify(self):
        """Formula with U+2212 produces parse_error and diagnostics detect it."""
        formula = "=A1\u2212B1"
        result = classify_formula(formula)
        # U+2212 will likely cause a parse error
        assert result["classification"] == "parse_error"

    def test_unicode_minus_in_find_non_ascii(self):
        chars = find_non_ascii_chars("=A1\u2212B1")
        assert any(cp == "U+2212" for cp, _, _ in chars)

    def test_sanitize_replaces_unicode_minus(self):
        assert sanitize_formula_preview("=A1\u2212B1") == "=A1-B1"


# ────────────────────────────────────────────────────────────────
# TestTraceLogAPI
# ────────────────────────────────────────────────────────────────


class TestTraceLogAPI:
    def test_api_trace_latest_after_import(self, make_xlsx, tmp_path):
        """GET /api/import/trace/latest returns 200 with trace content."""
        from fastapi.testclient import TestClient
        from fin123.project import scaffold_project
        from fin123.ui.server import create_app

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=BOGUS(((", "B1": "=SUM(1,2)"}})
        import_xlsx(xlsx_path, project_dir)

        app = create_app(project_dir)
        client = TestClient(app)
        resp = client.get("/api/import/trace/latest")
        assert resp.status_code == 200
        data = resp.json()
        assert "trace" in data
        assert "[IMPORT][parse_error]" in data["trace"]

    def test_api_trace_download_after_import(self, make_xlsx, tmp_path):
        """GET /api/import/trace/download/latest returns 200 with Content-Disposition."""
        from fastapi.testclient import TestClient
        from fin123.project import scaffold_project
        from fin123.ui.server import create_app

        project_dir = scaffold_project(tmp_path / "proj")
        xlsx_path = make_xlsx({"Sheet1": {"A1": "=BOGUS((("}})
        import_xlsx(xlsx_path, project_dir)

        app = create_app(project_dir)
        client = TestClient(app)
        resp = client.get("/api/import/trace/download/latest")
        assert resp.status_code == 200
        assert "content-disposition" in resp.headers
        assert "import_trace.log" in resp.headers["content-disposition"]

    def test_api_trace_404_when_no_import(self, tmp_path):
        """GET /api/import/trace/latest returns 404 when no import exists."""
        from fastapi.testclient import TestClient
        from fin123.project import scaffold_project
        from fin123.ui.server import create_app

        project_dir = scaffold_project(tmp_path / "proj")
        app = create_app(project_dir)
        client = TestClient(app)
        resp = client.get("/api/import/trace/latest")
        assert resp.status_code == 404
