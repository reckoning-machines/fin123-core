"""Stage 4: Hardening — type coverage, null handling, display format,
large table sanity, and default_factory verification."""

from __future__ import annotations

import datetime
import time

import polars as pl
import pytest

from fin123.worksheet.compiled import (
    CompiledHeaderGroup,
    CompiledWorksheet,
    SortEntry,
)
from fin123.worksheet.compiler import compile_worksheet
from fin123.worksheet.spec import (
    FlagSpec,
    HeaderGroup,
    SortSpec,
    WorksheetView,
    parse_worksheet_view,
)
from fin123.worksheet.types import ColumnSchema, ColumnType, DisplayFormat
from fin123.worksheet.view_table import ViewTable, from_polars

FROZEN_TIME = "2025-06-15T12:00:00+00:00"


# ────────────────────────────────────────────────────────────────
# Helper
# ────────────────────────────────────────────────────────────────


def _vt(df: pl.DataFrame, schema: list[ColumnSchema], **kw) -> ViewTable:
    return from_polars(df, schema, source_label="test", **kw)


# ────────────────────────────────────────────────────────────────
# All logical column types
# ────────────────────────────────────────────────────────────────


class TestColumnTypes:
    def test_string_type(self) -> None:
        df = pl.DataFrame({"s": ["hello", "world"]})
        vt = _vt(df, [ColumnSchema(name="s", dtype=ColumnType.STRING)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "s"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["s"] == "hello"
        assert ws.columns[0].column_type == ColumnType.STRING

    def test_int64_type(self) -> None:
        df = pl.DataFrame({"i": [1, 2, 3]})
        vt = _vt(df, [ColumnSchema(name="i", dtype=ColumnType.INT64)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "i"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["i"] == 1
        assert isinstance(ws.rows[0]["i"], int)
        assert ws.columns[0].column_type == ColumnType.INT64

    def test_float64_type(self) -> None:
        df = pl.DataFrame({"f": [1.5, 2.5]})
        vt = _vt(df, [ColumnSchema(name="f", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "f"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["f"] == 1.5
        assert isinstance(ws.rows[0]["f"], float)
        assert ws.columns[0].column_type == ColumnType.FLOAT64

    def test_bool_type(self) -> None:
        df = pl.DataFrame({"b": [True, False]})
        vt = _vt(df, [ColumnSchema(name="b", dtype=ColumnType.BOOL)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "b"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["b"] is True
        assert ws.rows[1]["b"] is False
        assert ws.columns[0].column_type == ColumnType.BOOL

    def test_date_type(self) -> None:
        df = pl.DataFrame({"d": [datetime.date(2025, 1, 15), datetime.date(2025, 6, 30)]})
        vt = _vt(df, [ColumnSchema(name="d", dtype=ColumnType.DATE)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "d"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["d"] == datetime.date(2025, 1, 15)
        assert ws.columns[0].column_type == ColumnType.DATE

    def test_datetime_type(self) -> None:
        dt1 = datetime.datetime(2025, 1, 15, 10, 30, 0)
        dt2 = datetime.datetime(2025, 6, 30, 14, 0, 0)
        df = pl.DataFrame({"dt": [dt1, dt2]})
        vt = _vt(df, [ColumnSchema(name="dt", dtype=ColumnType.DATETIME)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "dt"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["dt"] == dt1
        assert ws.columns[0].column_type == ColumnType.DATETIME

    def test_mixed_types_in_one_worksheet(self) -> None:
        df = pl.DataFrame({
            "name": ["A"],
            "count": [42],
            "price": [9.99],
            "active": [True],
            "created": [datetime.date(2025, 3, 1)],
        })
        schema = [
            ColumnSchema(name="name", dtype=ColumnType.STRING),
            ColumnSchema(name="count", dtype=ColumnType.INT64),
            ColumnSchema(name="price", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="active", dtype=ColumnType.BOOL),
            ColumnSchema(name="created", dtype=ColumnType.DATE),
        ]
        vt = _vt(df, schema)
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [
                {"source": "name"},
                {"source": "count"},
                {"source": "price"},
                {"source": "active"},
                {"source": "created"},
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        row = ws.rows[0]
        assert isinstance(row["name"], str)
        assert isinstance(row["count"], int)
        assert isinstance(row["price"], float)
        assert isinstance(row["active"], bool)
        assert isinstance(row["created"], datetime.date)


# ────────────────────────────────────────────────────────────────
# Null handling
# ────────────────────────────────────────────────────────────────


class TestNullHandling:
    def test_null_source_column_passes_through(self) -> None:
        df = pl.DataFrame({"x": [1.0, None, 3.0]})
        vt = _vt(df, [ColumnSchema(name="x", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "x"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["x"] == 1.0
        assert ws.rows[1]["x"] is None
        assert ws.rows[2]["x"] == 3.0

    def test_null_string_passes_through(self) -> None:
        df = pl.DataFrame({"s": ["hello", None, "world"]})
        vt = _vt(df, [ColumnSchema(name="s", dtype=ColumnType.STRING)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "s"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[1]["s"] is None

    def test_null_int_passes_through(self) -> None:
        df = pl.DataFrame({"i": [1, None, 3]})
        vt = _vt(df, [ColumnSchema(name="i", dtype=ColumnType.INT64)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "i"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[1]["i"] is None

    def test_null_in_derived_expression_produces_error(self) -> None:
        """Arithmetic with null produces an inline error."""
        df = pl.DataFrame({"a": [10.0, None], "b": [5.0, 5.0]})
        schema = [
            ColumnSchema(name="a", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="b", dtype=ColumnType.FLOAT64),
        ]
        vt = _vt(df, schema)
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [
                {"source": "a"},
                {"source": "b"},
                {"name": "sum", "expression": "a + b"},
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        # Row 0: 10 + 5 = 15
        assert ws.rows[0]["sum"] == 15.0
        # Row 1: None + 5 → error (TypeError in Python)
        assert isinstance(ws.rows[1]["sum"], dict)
        assert "error" in ws.rows[1]["sum"]

    def test_null_in_iferror_caught(self) -> None:
        """IFERROR can catch null-induced errors."""
        df = pl.DataFrame({"a": [10.0, None], "b": [5.0, 5.0]})
        schema = [
            ColumnSchema(name="a", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="b", dtype=ColumnType.FLOAT64),
        ]
        vt = _vt(df, schema)
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [
                {"source": "a"},
                {"name": "safe_sum", "expression": "IFERROR(a + b, 0)"},
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["safe_sum"] == 15.0
        assert ws.rows[1]["safe_sum"] == 0

    def test_null_sorts_last_ascending(self) -> None:
        df = pl.DataFrame({"x": [3.0, None, 1.0]})
        vt = _vt(df, [ColumnSchema(name="x", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [{"source": "x"}],
            "sorts": [{"column": "x"}],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["x"] == 1.0
        assert ws.rows[1]["x"] == 3.0
        assert ws.rows[2]["x"] is None

    def test_null_sorts_last_descending(self) -> None:
        df = pl.DataFrame({"x": [3.0, None, 1.0]})
        vt = _vt(df, [ColumnSchema(name="x", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [{"source": "x"}],
            "sorts": [{"column": "x", "descending": True}],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.rows[0]["x"] == 3.0
        assert ws.rows[1]["x"] == 1.0
        assert ws.rows[2]["x"] is None

    def test_null_in_json_roundtrip(self) -> None:
        df = pl.DataFrame({"x": [1.0, None]})
        vt = _vt(df, [ColumnSchema(name="x", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "x"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        restored = CompiledWorksheet.from_json(ws.to_json())
        assert restored.rows[0]["x"] == 1.0
        assert restored.rows[1]["x"] is None


# ────────────────────────────────────────────────────────────────
# DisplayFormat propagation
# ────────────────────────────────────────────────────────────────


class TestDisplayFormat:
    def test_currency_format_propagates(self) -> None:
        df = pl.DataFrame({"rev": [1000.0]})
        vt = _vt(df, [ColumnSchema(name="rev", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [{
                "source": "rev",
                "display_format": {"type": "currency", "symbol": "$", "places": 0},
            }],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        fmt = ws.columns[0].display_format
        assert fmt is not None
        assert fmt.type == "currency"
        assert fmt.symbol == "$"
        assert fmt.places == 0

    def test_percent_format_propagates(self) -> None:
        df = pl.DataFrame({"r": [0.15]})
        vt = _vt(df, [ColumnSchema(name="r", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [{
                "source": "r",
                "display_format": {"type": "percent", "places": 1},
            }],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        fmt = ws.columns[0].display_format
        assert fmt.type == "percent"
        assert fmt.places == 1

    def test_decimal_format_propagates(self) -> None:
        df = pl.DataFrame({"v": [3.14159]})
        vt = _vt(df, [ColumnSchema(name="v", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [{
                "source": "v",
                "display_format": {"type": "decimal", "places": 2},
            }],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        fmt = ws.columns[0].display_format
        assert fmt.type == "decimal"
        assert fmt.places == 2

    def test_date_format_propagates(self) -> None:
        df = pl.DataFrame({"d": [datetime.date(2025, 1, 1)]})
        vt = _vt(df, [ColumnSchema(name="d", dtype=ColumnType.DATE)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [{
                "source": "d",
                "display_format": {"type": "date", "date_format": "%Y-%m-%d"},
            }],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        fmt = ws.columns[0].display_format
        assert fmt.type == "date"
        assert fmt.date_format == "%Y-%m-%d"

    def test_integer_format_propagates(self) -> None:
        df = pl.DataFrame({"n": [42]})
        vt = _vt(df, [ColumnSchema(name="n", dtype=ColumnType.INT64)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [{
                "source": "n",
                "display_format": {"type": "integer"},
            }],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        fmt = ws.columns[0].display_format
        assert fmt.type == "integer"

    def test_text_format_propagates(self) -> None:
        df = pl.DataFrame({"s": ["hello"]})
        vt = _vt(df, [ColumnSchema(name="s", dtype=ColumnType.STRING)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [{
                "source": "s",
                "display_format": {"type": "text"},
            }],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        fmt = ws.columns[0].display_format
        assert fmt.type == "text"

    def test_no_format_is_none(self) -> None:
        df = pl.DataFrame({"x": [1]})
        vt = _vt(df, [ColumnSchema(name="x", dtype=ColumnType.INT64)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "x"}]})
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws.columns[0].display_format is None

    def test_derived_column_format_propagates(self) -> None:
        df = pl.DataFrame({"a": [100.0], "b": [40.0]})
        schema = [
            ColumnSchema(name="a", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="b", dtype=ColumnType.FLOAT64),
        ]
        vt = _vt(df, schema)
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [
                {"source": "a"},
                {"source": "b"},
                {
                    "name": "margin",
                    "expression": "(a - b) / a",
                    "display_format": {"type": "percent", "places": 1},
                },
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        fmt = ws.columns[2].display_format
        assert fmt is not None
        assert fmt.type == "percent"
        assert fmt.places == 1

    def test_display_format_survives_json_roundtrip(self) -> None:
        df = pl.DataFrame({"rev": [1000.0], "d": [datetime.date(2025, 1, 1)]})
        schema = [
            ColumnSchema(name="rev", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="d", dtype=ColumnType.DATE),
        ]
        vt = _vt(df, schema)
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [
                {"source": "rev", "display_format": {"type": "currency", "symbol": "€", "places": 2}},
                {"source": "d", "display_format": {"type": "date", "date_format": "%d/%m/%Y"}},
            ],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        restored = CompiledWorksheet.from_json(ws.to_json())
        assert restored.columns[0].display_format.type == "currency"
        assert restored.columns[0].display_format.symbol == "€"
        assert restored.columns[0].display_format.places == 2
        assert restored.columns[1].display_format.type == "date"
        assert restored.columns[1].display_format.date_format == "%d/%m/%Y"


# ────────────────────────────────────────────────────────────────
# row_key uniqueness still validated
# ────────────────────────────────────────────────────────────────


class TestRowKeyValidation:
    def test_unique_row_key_passes(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.INT64),
        ]
        vt = _vt(df, schema, row_key="id")
        assert vt.row_key == "id"

    def test_duplicate_row_key_rejected(self) -> None:
        df = pl.DataFrame({"id": [1, 1, 2], "val": [10, 20, 30]})
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.INT64),
        ]
        with pytest.raises(ValueError, match="duplicate"):
            _vt(df, schema, row_key="id")


# ────────────────────────────────────────────────────────────────
# Deterministic serialization / content hash
# ────────────────────────────────────────────────────────────────


class TestDeterministicSerialization:
    def test_same_inputs_same_json(self) -> None:
        df = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
        vt = _vt(df, [ColumnSchema(name="x", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({
            "name": "t",
            "columns": [
                {"source": "x"},
                {"name": "d", "expression": "x * 2"},
            ],
        })
        ws1 = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        ws2 = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        assert ws1.to_json() == ws2.to_json()

    def test_content_hash_ignores_compiled_at(self) -> None:
        df = pl.DataFrame({"x": [1.0]})
        vt = _vt(df, [ColumnSchema(name="x", dtype=ColumnType.FLOAT64)])
        spec = parse_worksheet_view({"name": "t", "columns": [{"source": "x"}]})
        ws1 = compile_worksheet(vt, spec, compiled_at="2025-01-01T00:00:00")
        ws2 = compile_worksheet(vt, spec, compiled_at="2099-12-31T23:59:59")
        assert ws1.content_hash_data() == ws2.content_hash_data()


# ────────────────────────────────────────────────────────────────
# Field(default_factory=list) verification
# ────────────────────────────────────────────────────────────────


class TestDefaultFactory:
    def test_worksheet_view_sorts_independent(self) -> None:
        a = parse_worksheet_view({"name": "a", "columns": [{"source": "x"}]})
        b = parse_worksheet_view({"name": "b", "columns": [{"source": "x"}]})
        a.sorts.append(SortSpec(column="x"))
        assert b.sorts == []

    def test_worksheet_view_flags_independent(self) -> None:
        a = parse_worksheet_view({"name": "a", "columns": [{"source": "x"}]})
        b = parse_worksheet_view({"name": "b", "columns": [{"source": "x"}]})
        a.flags.append(FlagSpec(name="f", expression="x > 0"))
        assert b.flags == []

    def test_worksheet_view_header_groups_independent(self) -> None:
        a = parse_worksheet_view({"name": "a", "columns": [{"source": "x"}]})
        b = parse_worksheet_view({"name": "b", "columns": [{"source": "x"}]})
        a.header_groups.append(HeaderGroup(label="g", columns=["x"]))
        assert b.header_groups == []

    def test_compiled_worksheet_sorts_independent(self) -> None:
        from fin123.worksheet.compiled import (
            ColumnProvenance, CompiledColumn, Provenance, ViewTableProvenance,
        )
        def _make():
            return CompiledWorksheet(
                name="t",
                columns=[CompiledColumn(name="x", label="X", column_type=ColumnType.INT64, source="x")],
                rows=[{"x": 1}],
                flags=[[]],
                provenance=Provenance(
                    view_table=ViewTableProvenance(source_label="s", input_row_count=1, input_columns=["x"]),
                    compiled_at="2025-01-01", fin123_version="0.3.4", spec_name="t",
                    row_count=1, column_count=1,
                    columns={"x": ColumnProvenance(type="source", source_column="x")},
                ),
            )
        a = _make()
        b = _make()
        a.sorts.append(SortEntry(column="x"))
        assert b.sorts == []

    def test_compiled_worksheet_header_groups_independent(self) -> None:
        from fin123.worksheet.compiled import (
            ColumnProvenance, CompiledColumn, Provenance, ViewTableProvenance,
        )
        def _make():
            return CompiledWorksheet(
                name="t",
                columns=[CompiledColumn(name="x", label="X", column_type=ColumnType.INT64, source="x")],
                rows=[{"x": 1}],
                flags=[[]],
                provenance=Provenance(
                    view_table=ViewTableProvenance(source_label="s", input_row_count=1, input_columns=["x"]),
                    compiled_at="2025-01-01", fin123_version="0.3.4", spec_name="t",
                    row_count=1, column_count=1,
                    columns={"x": ColumnProvenance(type="source", source_column="x")},
                ),
            )
        a = _make()
        b = _make()
        a.header_groups.append(CompiledHeaderGroup(label="g", columns=["x"]))
        assert b.header_groups == []


# ────────────────────────────────────────────────────────────────
# Large-table sanity check
# ────────────────────────────────────────────────────────────────


class TestLargeTable:
    def test_10k_rows_compiles_in_reasonable_time(self) -> None:
        n = 10_000
        df = pl.DataFrame({
            "id": list(range(n)),
            "revenue": [float(i * 10) for i in range(n)],
            "cost": [float(i * 4) for i in range(n)],
            "name": [f"item_{i}" for i in range(n)],
        })
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="revenue", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="cost", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="name", dtype=ColumnType.STRING),
        ]
        vt = _vt(df, schema, row_key="id")
        spec = parse_worksheet_view({
            "name": "large",
            "columns": [
                {"source": "id"},
                {"source": "name"},
                {"source": "revenue"},
                {"source": "cost"},
                {"name": "profit", "expression": "revenue - cost"},
                {"name": "margin", "expression": "IF(revenue > 0, profit / revenue, 0)"},
            ],
            "sorts": [{"column": "margin", "descending": True}],
            "flags": [
                {"name": "low_margin", "expression": "margin < 0.5", "severity": "warning"},
            ],
        })

        start = time.monotonic()
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        elapsed = time.monotonic() - start

        assert len(ws.rows) == n
        # Sanity: margin should be 0.6 for all rows (cost = 0.4 * revenue)
        assert ws.rows[0]["margin"] == pytest.approx(0.6)
        # Should complete in under 30 seconds (very generous — typically ~2-5s)
        assert elapsed < 30, f"10k rows took {elapsed:.1f}s"

    def test_10k_rows_json_roundtrip(self) -> None:
        n = 10_000
        df = pl.DataFrame({
            "id": list(range(n)),
            "val": [float(i) for i in range(n)],
        })
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.FLOAT64),
        ]
        vt = _vt(df, schema)
        spec = parse_worksheet_view({
            "name": "large",
            "columns": [{"source": "id"}, {"source": "val"}],
        })
        ws = compile_worksheet(vt, spec, compiled_at=FROZEN_TIME)
        json_str = ws.to_json()
        restored = CompiledWorksheet.from_json(json_str)
        assert len(restored.rows) == n
        assert restored.rows[0] == ws.rows[0]
        assert restored.rows[-1] == ws.rows[-1]
