"""Tests for WorksheetView spec parsing and validation (Stage 2)."""

from __future__ import annotations

import textwrap

import pytest
import yaml

from fin123.worksheet.spec import (
    DerivedColumnSpec,
    FlagSpec,
    HeaderGroup,
    SortSpec,
    SourceColumnSpec,
    WorksheetView,
    load_worksheet_view,
    parse_worksheet_view,
    validate_worksheet_view,
)
from fin123.worksheet.types import ColumnType, DisplayFormat


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────

VALID_SPEC = {
    "name": "margin_review",
    "title": "Margin Review — Q4",
    "columns": [
        {"source": "item_code", "label": "Item Code"},
        {"source": "revenue", "display_format": {"type": "currency", "symbol": "$", "places": 0}},
        {"source": "cost"},
        {
            "name": "margin",
            "expression": "(revenue - cost) / revenue",
            "label": "Margin %",
            "display_format": {"type": "percent", "places": 1},
        },
    ],
    "sorts": [{"column": "margin", "descending": True}],
    "flags": [
        {
            "name": "low_margin",
            "expression": "(revenue - cost) / revenue < 0.1",
            "severity": "warning",
            "message": "Margin below 10%",
        }
    ],
    "header_groups": [
        {"label": "Financials", "columns": ["revenue", "cost", "margin"]},
    ],
}

AVAILABLE_COLUMNS = ["item_code", "revenue", "cost", "description"]


# ────────────────────────────────────────────────────────────────
# Basic parsing
# ────────────────────────────────────────────────────────────────


class TestParsing:
    def test_valid_spec(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        assert spec.name == "margin_review"
        assert spec.title == "Margin Review — Q4"
        assert len(spec.columns) == 4
        assert len(spec.sorts) == 1
        assert len(spec.flags) == 1
        assert len(spec.header_groups) == 1

    def test_source_column(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        col = spec.columns[0]
        assert isinstance(col, SourceColumnSpec)
        assert col.source == "item_code"
        assert col.label == "Item Code"
        assert col.canonical_name == "item_code"

    def test_derived_column(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        col = spec.columns[3]
        assert isinstance(col, DerivedColumnSpec)
        assert col.name == "margin"
        assert col.expression == "(revenue - cost) / revenue"
        assert col.label == "Margin %"
        assert col.canonical_name == "margin"

    def test_display_format_parsed(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        col = spec.columns[1]
        assert isinstance(col, SourceColumnSpec)
        assert col.display_format is not None
        assert col.display_format.type == "currency"
        assert col.display_format.symbol == "$"
        assert col.display_format.places == 0

    def test_sort_parsed(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        assert spec.sorts[0].column == "margin"
        assert spec.sorts[0].descending is True

    def test_flag_parsed(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        flag = spec.flags[0]
        assert flag.name == "low_margin"
        assert flag.severity == "warning"
        assert flag.message == "Margin below 10%"

    def test_header_group_parsed(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        group = spec.header_groups[0]
        assert group.label == "Financials"
        assert group.columns == ["revenue", "cost", "margin"]

    def test_minimal_spec(self) -> None:
        spec = parse_worksheet_view({
            "name": "simple",
            "columns": [{"source": "x"}],
        })
        assert spec.name == "simple"
        assert spec.title is None
        assert spec.sorts == []
        assert spec.flags == []
        assert spec.header_groups == []

    def test_canonical_names(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        assert spec.canonical_names() == ["item_code", "revenue", "cost", "margin"]


# ────────────────────────────────────────────────────────────────
# YAML loading
# ────────────────────────────────────────────────────────────────


class TestYamlLoading:
    def test_load_from_file(self, tmp_path) -> None:
        yaml_text = textwrap.dedent("""\
            name: test_ws
            title: Test Worksheet
            columns:
              - source: id
                label: ID
              - source: value
              - name: doubled
                expression: "value * 2"
            sorts:
              - column: doubled
                descending: true
        """)
        path = tmp_path / "ws.yaml"
        path.write_text(yaml_text)

        spec = load_worksheet_view(path)
        assert spec.name == "test_ws"
        assert len(spec.columns) == 3
        assert spec.sorts[0].column == "doubled"

    def test_file_not_found(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError):
            load_worksheet_view(tmp_path / "nonexistent.yaml")


# ────────────────────────────────────────────────────────────────
# Parse errors
# ────────────────────────────────────────────────────────────────


class TestParseErrors:
    def test_not_a_dict(self) -> None:
        with pytest.raises(ValueError, match="must be a dict"):
            parse_worksheet_view("not a dict")  # type: ignore[arg-type]

    def test_columns_not_a_list(self) -> None:
        with pytest.raises(ValueError, match="must be a list"):
            parse_worksheet_view({"name": "x", "columns": "bad"})

    def test_empty_columns(self) -> None:
        with pytest.raises(Exception, match="must not be empty"):
            parse_worksheet_view({"name": "x", "columns": []})

    def test_column_missing_source_and_name(self) -> None:
        with pytest.raises(ValueError, match="must have 'source'"):
            parse_worksheet_view({
                "name": "x",
                "columns": [{"label": "orphan"}],
            })

    def test_derived_without_expression(self) -> None:
        with pytest.raises(ValueError, match="must have 'source'"):
            parse_worksheet_view({
                "name": "x",
                "columns": [{"name": "bad"}],
            })


# ────────────────────────────────────────────────────────────────
# Default factories (no mutable default sharing)
# ────────────────────────────────────────────────────────────────


class TestDefaults:
    def test_sorts_default_independent(self) -> None:
        a = parse_worksheet_view({"name": "a", "columns": [{"source": "x"}]})
        b = parse_worksheet_view({"name": "b", "columns": [{"source": "x"}]})
        a.sorts.append(SortSpec(column="x"))
        assert b.sorts == []

    def test_flags_default_independent(self) -> None:
        a = parse_worksheet_view({"name": "a", "columns": [{"source": "x"}]})
        b = parse_worksheet_view({"name": "b", "columns": [{"source": "x"}]})
        a.flags.append(FlagSpec(name="f", expression="x > 0"))
        assert b.flags == []

    def test_header_groups_default_independent(self) -> None:
        a = parse_worksheet_view({"name": "a", "columns": [{"source": "x"}]})
        b = parse_worksheet_view({"name": "b", "columns": [{"source": "x"}]})
        a.header_groups.append(HeaderGroup(label="g", columns=["x"]))
        assert b.header_groups == []


# ────────────────────────────────────────────────────────────────
# Validation against ViewTable columns
# ────────────────────────────────────────────────────────────────


class TestValidation:
    def test_valid_spec_no_errors(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert errors == []

    def test_missing_source_column(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "nonexistent"}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("nonexistent" in e and "not found" in e for e in errors)

    def test_duplicate_output_names(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"name": "revenue", "expression": "cost * 2"},
            ],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("Duplicate" in e for e in errors)

    def test_invalid_sort_column(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "revenue"}],
            "sorts": [{"column": "nonexistent"}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("Sort column" in e and "nonexistent" in e for e in errors)

    def test_sort_on_valid_output_column(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"name": "doubled", "expression": "revenue * 2"},
            ],
            "sorts": [{"column": "doubled"}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert errors == []

    def test_bad_derived_expression(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"name": "bad", "expression": "(((("},
            ],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("bad" in e and "Parse error" in e for e in errors)

    def test_disallowed_function_in_derived(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"name": "lookup", "expression": 'VLOOKUP(revenue, "t", "c")'},
            ],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("VLOOKUP" in e for e in errors)

    def test_derived_references_unknown_column(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"name": "calc", "expression": "nonexistent * 2"},
            ],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("nonexistent" in e for e in errors)

    def test_derived_references_source_column(self) -> None:
        """Derived columns may reference source columns from ViewTable."""
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"name": "doubled", "expression": "revenue * 2"},
            ],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert errors == []

    def test_derived_references_earlier_derived(self) -> None:
        """Derived columns may reference previously-defined derived columns."""
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "profit", "expression": "revenue - cost"},
                {"name": "margin", "expression": "profit / revenue"},
            ],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert errors == []

    def test_derived_forward_reference_allowed(self) -> None:
        """Derived columns may reference later-defined derived columns.

        Cycle detection is the compiler's job, not the validator's.
        """
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "margin", "expression": "profit / revenue"},
                {"name": "profit", "expression": "revenue - cost"},
            ],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert errors == []

    def test_header_group_valid(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "revenue"}, {"source": "cost"}],
            "header_groups": [{"label": "Money", "columns": ["revenue", "cost"]}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert errors == []

    def test_header_group_unknown_column(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "revenue"}],
            "header_groups": [{"label": "Bad", "columns": ["nonexistent"]}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("nonexistent" in e and "canonical column names" in e for e in errors)

    def test_header_group_must_use_canonical_names_not_labels(self) -> None:
        """Header groups reference canonical names, not display labels."""
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "revenue", "label": "Rev"}],
            "header_groups": [{"label": "G", "columns": ["Rev"]}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        # "Rev" is a label, not the canonical name "revenue"
        assert any("Rev" in e for e in errors)

    def test_flag_valid(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "revenue"}],
            "flags": [{"name": "big", "expression": "revenue > 1000"}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert errors == []

    def test_flag_bad_expression(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "revenue"}],
            "flags": [{"name": "bad", "expression": "(((("}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("Flag" in e and "bad" in e for e in errors)

    def test_flag_disallowed_function(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "revenue"}],
            "flags": [{"name": "f", "expression": 'VLOOKUP(revenue, "t", "c") > 0'}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert any("VLOOKUP" in e for e in errors)

    def test_flag_references_derived_column(self) -> None:
        """Flags can reference derived columns."""
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "margin", "expression": "(revenue - cost) / revenue"},
            ],
            "flags": [{"name": "low", "expression": "margin < 0.1"}],
        })
        errors = validate_worksheet_view(spec, AVAILABLE_COLUMNS)
        assert errors == []


# ────────────────────────────────────────────────────────────────
# No row_key on WorksheetView
# ────────────────────────────────────────────────────────────────


class TestNoRowKeyOverride:
    def test_no_row_key_field(self) -> None:
        """WorksheetView has no row_key field."""
        spec = parse_worksheet_view({"name": "x", "columns": [{"source": "a"}]})
        assert not hasattr(spec, "row_key")


# ────────────────────────────────────────────────────────────────
# Column type override
# ────────────────────────────────────────────────────────────────


class TestColumnTypeOverride:
    def test_source_with_type_override(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [{"source": "revenue", "column_type": "float64"}],
        })
        col = spec.columns[0]
        assert isinstance(col, SourceColumnSpec)
        assert col.column_type == ColumnType.FLOAT64

    def test_derived_with_type(self) -> None:
        spec = parse_worksheet_view({
            "name": "x",
            "columns": [
                {"source": "revenue"},
                {"name": "d", "expression": "revenue * 2", "column_type": "float64"},
            ],
        })
        col = spec.columns[1]
        assert isinstance(col, DerivedColumnSpec)
        assert col.column_type == ColumnType.FLOAT64


# ────────────────────────────────────────────────────────────────
# Pydantic model_dump roundtrip
# ────────────────────────────────────────────────────────────────


class TestRoundtrip:
    def test_model_dump_and_reparse(self) -> None:
        spec = parse_worksheet_view(VALID_SPEC)
        dumped = spec.model_dump()
        # Re-parse — need to handle the column kind discriminator
        restored = parse_worksheet_view(dumped)
        assert restored.name == spec.name
        assert restored.canonical_names() == spec.canonical_names()
        assert len(restored.sorts) == len(spec.sorts)
        assert len(restored.flags) == len(spec.flags)
        assert len(restored.header_groups) == len(spec.header_groups)
