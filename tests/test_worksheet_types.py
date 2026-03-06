"""Tests for worksheet type definitions (Stage 1)."""

from __future__ import annotations

from fin123.worksheet.types import ColumnSchema, ColumnType, DisplayFormat


class TestColumnType:
    def test_values(self) -> None:
        assert ColumnType.STRING.value == "string"
        assert ColumnType.INT64.value == "int64"
        assert ColumnType.FLOAT64.value == "float64"
        assert ColumnType.BOOL.value == "bool"
        assert ColumnType.DATE.value == "date"
        assert ColumnType.DATETIME.value == "datetime"

    def test_from_string(self) -> None:
        assert ColumnType("string") == ColumnType.STRING
        assert ColumnType("int64") == ColumnType.INT64
        assert ColumnType("float64") == ColumnType.FLOAT64


class TestColumnSchema:
    def test_basic(self) -> None:
        cs = ColumnSchema(name="revenue", dtype=ColumnType.FLOAT64)
        assert cs.name == "revenue"
        assert cs.dtype == ColumnType.FLOAT64
        assert cs.nullable is True

    def test_non_nullable(self) -> None:
        cs = ColumnSchema(name="id", dtype=ColumnType.INT64, nullable=False)
        assert cs.nullable is False

    def test_serialization_roundtrip(self) -> None:
        cs = ColumnSchema(name="x", dtype=ColumnType.DATE, nullable=False)
        data = cs.model_dump()
        restored = ColumnSchema.model_validate(data)
        assert restored == cs


class TestDisplayFormat:
    def test_decimal(self) -> None:
        fmt = DisplayFormat(type="decimal", places=2)
        assert fmt.type == "decimal"
        assert fmt.places == 2

    def test_currency(self) -> None:
        fmt = DisplayFormat(type="currency", symbol="$", places=0)
        assert fmt.symbol == "$"

    def test_percent(self) -> None:
        fmt = DisplayFormat(type="percent", places=1)
        assert fmt.type == "percent"

    def test_date(self) -> None:
        fmt = DisplayFormat(type="date", date_format="%Y-%m-%d")
        assert fmt.date_format == "%Y-%m-%d"

    def test_defaults(self) -> None:
        fmt = DisplayFormat(type="text")
        assert fmt.places is None
        assert fmt.symbol is None
        assert fmt.date_format is None
