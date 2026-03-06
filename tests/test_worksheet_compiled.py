"""Tests for CompiledWorksheet serialization (Stage 3)."""

from __future__ import annotations

import json

import pytest

from fin123.worksheet.compiled import (
    ColumnProvenance,
    CompiledColumn,
    CompiledFlag,
    CompiledHeaderGroup,
    CompiledWorksheet,
    ErrorSummary,
    Provenance,
    SortEntry,
    ViewTableProvenance,
)
from fin123.worksheet.types import ColumnType, DisplayFormat


@pytest.fixture
def sample_worksheet() -> CompiledWorksheet:
    return CompiledWorksheet(
        name="test",
        title="Test Worksheet",
        columns=[
            CompiledColumn(
                name="id", label="ID", column_type=ColumnType.INT64, source="id"
            ),
            CompiledColumn(
                name="revenue", label="Revenue", column_type=ColumnType.FLOAT64,
                display_format=DisplayFormat(type="currency", symbol="$", places=0),
                source="revenue",
            ),
            CompiledColumn(
                name="doubled", label="Doubled", column_type=ColumnType.FLOAT64,
                expression="revenue * 2",
            ),
        ],
        sorts=[SortEntry(column="revenue", descending=True)],
        header_groups=[
            CompiledHeaderGroup(label="Numbers", columns=["revenue", "doubled"]),
        ],
        rows=[
            {"id": 1, "revenue": 100.0, "doubled": 200.0},
            {"id": 2, "revenue": 50.0, "doubled": 100.0},
        ],
        flags=[
            [],
            [CompiledFlag(name="low", severity="warning", message="Low revenue")],
        ],
        provenance=Provenance(
            view_table=ViewTableProvenance(
                source_label="test data",
                row_key=None,
                input_row_count=2,
                input_columns=["id", "revenue"],
            ),
            compiled_at="2025-01-01T00:00:00+00:00",
            fin123_version="0.3.4",
            spec_name="test",
            row_count=2,
            column_count=3,
            columns={
                "id": ColumnProvenance(type="source", source_column="id"),
                "revenue": ColumnProvenance(type="source", source_column="revenue"),
                "doubled": ColumnProvenance(type="derived", expression="revenue * 2"),
            },
        ),
        error_summary=None,
    )


class TestJsonRoundtrip:
    def test_roundtrip(self, sample_worksheet: CompiledWorksheet) -> None:
        json_str = sample_worksheet.to_json()
        restored = CompiledWorksheet.from_json(json_str)
        assert restored.name == sample_worksheet.name
        assert restored.title == sample_worksheet.title
        assert len(restored.columns) == len(sample_worksheet.columns)
        assert len(restored.rows) == len(sample_worksheet.rows)
        assert restored.rows == sample_worksheet.rows
        assert restored.provenance.compiled_at == sample_worksheet.provenance.compiled_at

    def test_roundtrip_preserves_columns(self, sample_worksheet: CompiledWorksheet) -> None:
        restored = CompiledWorksheet.from_json(sample_worksheet.to_json())
        for orig, rest in zip(sample_worksheet.columns, restored.columns):
            assert orig.name == rest.name
            assert orig.column_type == rest.column_type

    def test_roundtrip_preserves_sorts(self, sample_worksheet: CompiledWorksheet) -> None:
        restored = CompiledWorksheet.from_json(sample_worksheet.to_json())
        assert len(restored.sorts) == 1
        assert restored.sorts[0].column == "revenue"
        assert restored.sorts[0].descending is True

    def test_roundtrip_preserves_header_groups(self, sample_worksheet: CompiledWorksheet) -> None:
        restored = CompiledWorksheet.from_json(sample_worksheet.to_json())
        assert len(restored.header_groups) == 1
        assert restored.header_groups[0].label == "Numbers"

    def test_roundtrip_preserves_flags(self, sample_worksheet: CompiledWorksheet) -> None:
        restored = CompiledWorksheet.from_json(sample_worksheet.to_json())
        assert restored.flags[0] == []
        assert len(restored.flags[1]) == 1
        assert restored.flags[1][0].name == "low"

    def test_roundtrip_preserves_provenance(self, sample_worksheet: CompiledWorksheet) -> None:
        restored = CompiledWorksheet.from_json(sample_worksheet.to_json())
        assert restored.provenance.view_table.source_label == "test data"
        assert restored.provenance.columns["doubled"].type == "derived"

    def test_roundtrip_with_errors(self) -> None:
        ws = CompiledWorksheet(
            name="err",
            columns=[
                CompiledColumn(name="x", label="X", column_type=ColumnType.FLOAT64, source="x"),
            ],
            rows=[{"x": {"error": "#DIV/0!"}}],
            flags=[[]],
            provenance=Provenance(
                view_table=ViewTableProvenance(
                    source_label="t", input_row_count=1, input_columns=["x"],
                ),
                compiled_at="2025-01-01T00:00:00",
                fin123_version="0.3.4",
                spec_name="err",
                row_count=1,
                column_count=1,
                columns={"x": ColumnProvenance(type="source", source_column="x")},
            ),
            error_summary=ErrorSummary(total_errors=1, by_column={"x": 1}),
        )
        restored = CompiledWorksheet.from_json(ws.to_json())
        assert restored.rows[0]["x"] == {"error": "#DIV/0!"}
        assert restored.error_summary is not None
        assert restored.error_summary.total_errors == 1


class TestDeterminism:
    def test_same_input_same_json(self, sample_worksheet: CompiledWorksheet) -> None:
        json1 = sample_worksheet.to_json()
        json2 = sample_worksheet.to_json()
        assert json1 == json2

    def test_content_hash_excludes_compiled_at(self) -> None:
        ws1 = CompiledWorksheet(
            name="t",
            columns=[CompiledColumn(name="x", label="X", column_type=ColumnType.INT64, source="x")],
            rows=[{"x": 1}],
            flags=[[]],
            provenance=Provenance(
                view_table=ViewTableProvenance(
                    source_label="s", input_row_count=1, input_columns=["x"],
                ),
                compiled_at="2025-01-01T00:00:00",
                fin123_version="0.3.4",
                spec_name="t",
                row_count=1,
                column_count=1,
                columns={"x": ColumnProvenance(type="source", source_column="x")},
            ),
        )
        ws2 = CompiledWorksheet(
            name="t",
            columns=[CompiledColumn(name="x", label="X", column_type=ColumnType.INT64, source="x")],
            rows=[{"x": 1}],
            flags=[[]],
            provenance=Provenance(
                view_table=ViewTableProvenance(
                    source_label="s", input_row_count=1, input_columns=["x"],
                ),
                compiled_at="2099-12-31T23:59:59",
                fin123_version="0.3.4",
                spec_name="t",
                row_count=1,
                column_count=1,
                columns={"x": ColumnProvenance(type="source", source_column="x")},
            ),
        )
        assert ws1.content_hash_data() == ws2.content_hash_data()
        assert ws1.to_json() != ws2.to_json()  # full JSON differs


class TestFileIO:
    def test_to_and_from_file(self, sample_worksheet: CompiledWorksheet, tmp_path) -> None:
        path = tmp_path / "ws.json"
        sample_worksheet.to_file(path)
        restored = CompiledWorksheet.from_file(path)
        assert restored.name == sample_worksheet.name
        assert restored.rows == sample_worksheet.rows
