"""Tests for worksheet compilation pipeline (Stage 3)."""

from __future__ import annotations

import json

import polars as pl
import pytest

from fin123.worksheet.compiled import CompiledWorksheet
from fin123.worksheet.compiler import compile_worksheet
from fin123.worksheet.spec import parse_worksheet_view
from fin123.worksheet.types import ColumnSchema, ColumnType
from fin123.worksheet.view_table import ViewTable, from_polars, suggest_schema

FROZEN_TIME = "2025-06-15T12:00:00+00:00"


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def simple_vt() -> ViewTable:
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "revenue": [100.0, 200.0, 50.0],
        "cost": [40.0, 180.0, 30.0],
    })
    schema = [
        ColumnSchema(name="id", dtype=ColumnType.INT64),
        ColumnSchema(name="name", dtype=ColumnType.STRING),
        ColumnSchema(name="revenue", dtype=ColumnType.FLOAT64),
        ColumnSchema(name="cost", dtype=ColumnType.FLOAT64),
    ]
    return from_polars(df, schema, row_key="id", source_label="test data")


# ────────────────────────────────────────────────────────────────
# Source column projection
# ────────────────────────────────────────────────────────────────


class TestSourceProjection:
    def test_projects_source_columns(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "name"}, {"source": "revenue"}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert len(ws.rows) == 3
        assert ws.rows[0] == {"name": "Alice", "revenue": 100.0}
        assert ws.rows[1] == {"name": "Bob", "revenue": 200.0}

    def test_column_metadata(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "revenue", "label": "Rev"}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert ws.columns[0].name == "revenue"
        assert ws.columns[0].label == "Rev"
        assert ws.columns[0].source == "revenue"
        assert ws.columns[0].column_type.value == "float64"


# ────────────────────────────────────────────────────────────────
# Derived columns
# ────────────────────────────────────────────────────────────────


class TestDerivedColumns:
    def test_basic_derived(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "profit", "expression": "revenue - cost"},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["profit"] == 60.0
        assert ws.rows[1]["profit"] == 20.0
        assert ws.rows[2]["profit"] == 20.0

    def test_derived_on_derived(self, simple_vt: ViewTable) -> None:
        """Derived column references another derived column."""
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "profit", "expression": "revenue - cost"},
                {"name": "margin", "expression": "profit / revenue"},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["margin"] == pytest.approx(0.6)
        assert ws.rows[1]["margin"] == pytest.approx(0.1)

    def test_forward_reference_derived(self, simple_vt: ViewTable) -> None:
        """Derived column defined BEFORE the column it references (forward ref).

        Display order: margin appears before profit.
        Evaluation order: profit is evaluated first (dependency).
        """
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "margin", "expression": "profit / revenue"},
                {"name": "profit", "expression": "revenue - cost"},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        # margin appears first in output (spec order)
        assert list(ws.rows[0].keys()) == ["revenue", "cost", "margin", "profit"]
        # Values are correct despite forward reference
        assert ws.rows[0]["profit"] == 60.0
        assert ws.rows[0]["margin"] == pytest.approx(0.6)

    def test_derived_references_non_projected_column(self, simple_vt: ViewTable) -> None:
        """Derived expression references a ViewTable column not in the output."""
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "name"},
                {"name": "profit", "expression": "revenue - cost"},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        # revenue and cost are not in the output, but profit is computed
        assert "revenue" not in ws.rows[0]
        assert "cost" not in ws.rows[0]
        assert ws.rows[0]["profit"] == 60.0

    def test_derived_column_metadata(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"name": "doubled", "expression": "revenue * 2", "label": "2x Rev"},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        col = ws.columns[1]
        assert col.name == "doubled"
        assert col.label == "2x Rev"
        assert col.expression == "revenue * 2"
        assert col.source is None


# ────────────────────────────────────────────────────────────────
# Dependency cycle detection
# ────────────────────────────────────────────────────────────────


class TestCycleDetection:
    def test_cycle_raises(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"name": "a", "expression": "b + 1"},
                {"name": "b", "expression": "a + 1"},
            ],
        })
        with pytest.raises(ValueError, match="Cycle"):
            compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)

    def test_self_reference_cycle(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"name": "x", "expression": "x + 1"},
            ],
        })
        with pytest.raises(ValueError, match="Cycle"):
            compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)

    def test_three_way_cycle(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"name": "a", "expression": "c + 1"},
                {"name": "b", "expression": "a + 1"},
                {"name": "c", "expression": "b + 1"},
            ],
        })
        with pytest.raises(ValueError, match="Cycle"):
            compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)


# ────────────────────────────────────────────────────────────────
# Sorts
# ────────────────────────────────────────────────────────────────


class TestSorts:
    def test_sort_ascending(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "name"}, {"source": "revenue"}],
            "sorts": [{"column": "revenue"}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        revenues = [r["revenue"] for r in ws.rows]
        assert revenues == [50.0, 100.0, 200.0]

    def test_sort_descending(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "name"}, {"source": "revenue"}],
            "sorts": [{"column": "revenue", "descending": True}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        revenues = [r["revenue"] for r in ws.rows]
        assert revenues == [200.0, 100.0, 50.0]

    def test_sort_on_derived_column(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "name"},
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "profit", "expression": "revenue - cost"},
            ],
            "sorts": [{"column": "profit", "descending": True}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        profits = [r["profit"] for r in ws.rows]
        assert profits == [60.0, 20.0, 20.0]

    def test_sort_metadata_in_artifact(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "revenue"}],
            "sorts": [{"column": "revenue", "descending": True}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert len(ws.sorts) == 1
        assert ws.sorts[0].column == "revenue"
        assert ws.sorts[0].descending is True


# ────────────────────────────────────────────────────────────────
# Flags
# ────────────────────────────────────────────────────────────────


class TestFlags:
    def test_basic_flag(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "name"}, {"source": "revenue"}],
            "flags": [
                {"name": "high_rev", "expression": "revenue > 100", "severity": "info", "message": "High"},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        # Only Bob (200) triggers the flag
        flagged_rows = [i for i, fl in enumerate(ws.flags) if fl]
        assert len(flagged_rows) == 1
        assert ws.rows[flagged_rows[0]]["name"] == "Bob"
        assert ws.flags[flagged_rows[0]][0].name == "high_rev"

    def test_flag_references_derived(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "profit", "expression": "revenue - cost"},
            ],
            "flags": [
                {"name": "low_profit", "expression": "profit < 25", "severity": "warning"},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        # profit values: 60, 20, 20 — rows with profit < 25 get flagged
        flagged_count = sum(1 for fl in ws.flags if fl)
        assert flagged_count == 2


# ────────────────────────────────────────────────────────────────
# Inline errors
# ────────────────────────────────────────────────────────────────


class TestInlineErrors:
    def test_division_by_zero(self) -> None:
        df = pl.DataFrame({"a": [10.0, 20.0], "b": [5.0, 0.0]})
        schema = [
            ColumnSchema(name="a", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="b", dtype=ColumnType.FLOAT64),
        ]
        vt = from_polars(df, schema, source_label="test")
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "a"},
                {"source": "b"},
                {"name": "ratio", "expression": "a / b"},
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["ratio"] == 2.0
        assert ws.rows[1]["ratio"] == {"error": "#DIV/0!"}

    def test_error_summary(self) -> None:
        df = pl.DataFrame({"a": [10.0, 20.0, 30.0], "b": [0.0, 0.0, 5.0]})
        schema = [
            ColumnSchema(name="a", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="b", dtype=ColumnType.FLOAT64),
        ]
        vt = from_polars(df, schema, source_label="test")
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "a"},
                {"name": "ratio", "expression": "a / b"},
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.error_summary is not None
        assert ws.error_summary.total_errors == 2
        assert ws.error_summary.by_column == {"ratio": 2}

    def test_no_errors_no_summary(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "revenue"}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert ws.error_summary is None

    def test_error_in_derived_propagates_to_dependent(self) -> None:
        """If derived col A errors, derived col B that depends on A also errors."""
        df = pl.DataFrame({"x": [10.0], "y": [0.0]})
        schema = [
            ColumnSchema(name="x", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="y", dtype=ColumnType.FLOAT64),
        ]
        vt = from_polars(df, schema, source_label="test")
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "x"},
                {"name": "ratio", "expression": "x / y"},
                {"name": "doubled_ratio", "expression": "ratio * 2"},
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["ratio"] == {"error": "#DIV/0!"}
        # doubled_ratio tries to multiply an error dict by 2 → error
        assert isinstance(ws.rows[0]["doubled_ratio"], dict)
        assert "error" in ws.rows[0]["doubled_ratio"]


# ────────────────────────────────────────────────────────────────
# Provenance
# ────────────────────────────────────────────────────────────────


class TestProvenance:
    def test_provenance_structure(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "prov_test",
            "columns": [
                {"source": "revenue"},
                {"name": "doubled", "expression": "revenue * 2"},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        prov = ws.provenance

        # View table block
        assert prov.view_table.source_label == "test data"
        assert prov.view_table.row_key == "id"
        assert prov.view_table.input_row_count == 3
        assert "revenue" in prov.view_table.input_columns

        # Metadata
        assert prov.compiled_at == FROZEN_TIME
        assert prov.spec_name == "prov_test"
        assert prov.row_count == 3
        assert prov.column_count == 2

        # Per-column provenance
        assert prov.columns["revenue"].type == "source"
        assert prov.columns["revenue"].source_column == "revenue"
        assert prov.columns["doubled"].type == "derived"
        assert prov.columns["doubled"].expression == "revenue * 2"

    def test_provenance_always_present(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "minimal",
            "columns": [{"source": "id"}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert ws.provenance is not None
        assert ws.provenance.fin123_version != ""

    def test_frozen_compiled_at(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "id"}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert ws.provenance.compiled_at == FROZEN_TIME


# ────────────────────────────────────────────────────────────────
# Header groups
# ────────────────────────────────────────────────────────────────


class TestHeaderGroups:
    def test_preserved_in_artifact(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "profit", "expression": "revenue - cost"},
            ],
            "header_groups": [
                {"label": "Financials", "columns": ["revenue", "cost", "profit"]},
            ],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert len(ws.header_groups) == 1
        assert ws.header_groups[0].label == "Financials"
        assert ws.header_groups[0].columns == ["revenue", "cost", "profit"]


# ────────────────────────────────────────────────────────────────
# JSON roundtrip of compiled worksheet
# ────────────────────────────────────────────────────────────────


class TestCompilerJsonRoundtrip:
    def test_roundtrip(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "rt_test",
            "columns": [
                {"source": "name"},
                {"source": "revenue"},
                {"name": "doubled", "expression": "revenue * 2"},
            ],
            "sorts": [{"column": "revenue", "descending": True}],
        })
        ws = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        json_str = ws.to_json()
        restored = CompiledWorksheet.from_json(json_str)
        assert restored.rows == ws.rows
        assert restored.provenance.spec_name == "rt_test"


# ────────────────────────────────────────────────────────────────
# Deterministic output
# ────────────────────────────────────────────────────────────────


class TestDeterminism:
    def test_same_inputs_same_json(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "det_test",
            "columns": [
                {"source": "revenue"},
                {"name": "profit", "expression": "revenue - cost"},
            ],
            "sorts": [{"column": "profit"}],
        })
        ws1 = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        ws2 = compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)
        assert ws1.to_json() == ws2.to_json()

    def test_content_hash_stable(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "hash_test",
            "columns": [{"source": "revenue"}],
        })
        ws1 = compile_worksheet(simple_vt, spec, compiled_at="2025-01-01T00:00:00")
        ws2 = compile_worksheet(simple_vt, spec, compiled_at="2099-12-31T23:59:59")
        assert ws1.content_hash_data() == ws2.content_hash_data()


# ────────────────────────────────────────────────────────────────
# Validation errors
# ────────────────────────────────────────────────────────────────


class TestValidationErrors:
    def test_missing_source_column(self, simple_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [{"source": "nonexistent"}],
        })
        with pytest.raises(ValueError, match="validation failed"):
            compile_worksheet(simple_vt, spec, compiled_at=FROZEN_TIME)


# ────────────────────────────────────────────────────────────────
# PoC: demo_fin123 priced_estimates
# ────────────────────────────────────────────────────────────────


class TestPocPricedEstimates:
    """End-to-end proof of concept using the demo data shape."""

    @pytest.fixture
    def priced_estimates_vt(self) -> ViewTable:
        """Simulate the priced_estimates table from demo_fin123."""
        df = pl.DataFrame({
            "ticker": ["AAPL", "MSFT", "GOOG"],
            "date": ["2025-01-06", "2025-01-06", "2025-01-06"],
            "px_last": [180.0, 370.0, 175.0],
            "eps_ntm": [7.1, 12.5, 6.8],
            "rev_ntm": [400.0, 250.0, 350.0],
        })
        schema = [
            ColumnSchema(name="ticker", dtype=ColumnType.STRING),
            ColumnSchema(name="date", dtype=ColumnType.STRING),
            ColumnSchema(name="px_last", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="eps_ntm", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="rev_ntm", dtype=ColumnType.FLOAT64),
        ]
        return from_polars(df, schema, row_key="ticker", source_label="demo priced_estimates")

    def test_poc_compile(self, priced_estimates_vt: ViewTable) -> None:
        spec = parse_worksheet_view({
            "name": "poc_priced_estimates",
            "title": "PoC - Priced Estimates Review",
            "columns": [
                {"source": "ticker", "label": "Ticker"},
                {"source": "px_last", "label": "Price",
                 "display_format": {"type": "currency", "symbol": "$", "places": 2}},
                {"source": "eps_ntm", "label": "EPS (NTM)",
                 "display_format": {"type": "decimal", "places": 2}},
                {"name": "pe_ratio", "expression": "px_last / eps_ntm",
                 "label": "P/E Ratio",
                 "display_format": {"type": "decimal", "places": 1}},
                {"name": "earnings_yield", "expression": "eps_ntm / px_last",
                 "label": "Earnings Yield",
                 "display_format": {"type": "percent", "places": 2}},
            ],
            "sorts": [{"column": "pe_ratio"}],
            "flags": [
                {"name": "high_pe", "expression": "pe_ratio > 28",
                 "severity": "warning", "message": "P/E above 28"},
            ],
            "header_groups": [
                {"label": "Valuation", "columns": ["pe_ratio", "earnings_yield"]},
            ],
        })
        ws = compile_worksheet(priced_estimates_vt, spec, compiled_at=FROZEN_TIME)

        # Row count
        assert len(ws.rows) == 3

        # Derived values correct
        aapl_row = next(r for r in ws.rows if r["ticker"] == "AAPL")
        assert aapl_row["pe_ratio"] == pytest.approx(180.0 / 7.1, rel=1e-6)
        assert aapl_row["earnings_yield"] == pytest.approx(7.1 / 180.0, rel=1e-6)

        # Sorted by pe_ratio ascending
        pe_values = [r["pe_ratio"] for r in ws.rows]
        assert pe_values == sorted(pe_values)

        # Flag: MSFT P/E = 370/12.5 = 29.6 → should trigger
        msft_idx = next(i for i, r in enumerate(ws.rows) if r["ticker"] == "MSFT")
        assert any(f.name == "high_pe" for f in ws.flags[msft_idx])

        # Provenance
        assert ws.provenance.view_table.source_label == "demo priced_estimates"
        assert ws.provenance.view_table.row_key == "ticker"
        assert ws.provenance.spec_name == "poc_priced_estimates"
        assert ws.provenance.columns["pe_ratio"].type == "derived"
        assert ws.provenance.columns["ticker"].type == "source"

        # Header groups
        assert len(ws.header_groups) == 1
        assert ws.header_groups[0].label == "Valuation"

        # No errors
        assert ws.error_summary is None

        # Sort metadata
        assert len(ws.sorts) == 1
        assert ws.sorts[0].column == "pe_ratio"

        # JSON roundtrip
        json_str = ws.to_json()
        restored = CompiledWorksheet.from_json(json_str)
        assert restored.rows == ws.rows
        assert restored.provenance.columns == ws.provenance.columns

        # Deterministic
        ws2 = compile_worksheet(priced_estimates_vt, spec, compiled_at=FROZEN_TIME)
        assert ws.to_json() == ws2.to_json()

    def test_poc_with_division_by_zero(self) -> None:
        """Edge case: EPS = 0 causes P/E division by zero."""
        df = pl.DataFrame({
            "ticker": ["ZERO"],
            "px_last": [100.0],
            "eps_ntm": [0.0],
        })
        schema = [
            ColumnSchema(name="ticker", dtype=ColumnType.STRING),
            ColumnSchema(name="px_last", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="eps_ntm", dtype=ColumnType.FLOAT64),
        ]
        vt = from_polars(df, schema, source_label="zero test")
        spec = parse_worksheet_view({
            "name": "test",
            "columns": [
                {"source": "ticker"},
                {"name": "pe_ratio", "expression": "px_last / eps_ntm"},
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["pe_ratio"] == {"error": "#DIV/0!"}
        assert ws.error_summary is not None
        assert ws.error_summary.total_errors == 1
