"""Tests for ViewTable construction, validation, and adapters (Stage 1)."""

from __future__ import annotations

import json
import datetime

import polars as pl
import pytest

from fin123.worksheet.types import ColumnSchema, ColumnType
from fin123.worksheet.view_table import (
    ViewTable,
    from_fin123_run,
    from_json_file,
    from_json_records,
    from_polars,
    suggest_schema,
)


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "revenue": [100.0, 200.0, 300.0],
            "active": [True, False, True],
        }
    )


@pytest.fixture
def sample_schema() -> list[ColumnSchema]:
    return [
        ColumnSchema(name="id", dtype=ColumnType.INT64),
        ColumnSchema(name="name", dtype=ColumnType.STRING),
        ColumnSchema(name="revenue", dtype=ColumnType.FLOAT64),
        ColumnSchema(name="active", dtype=ColumnType.BOOL),
    ]


# ────────────────────────────────────────────────────────────────
# ViewTable construction
# ────────────────────────────────────────────────────────────────


class TestViewTableConstruction:
    def test_basic(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        assert vt.row_count == 3
        assert vt.columns == ["id", "name", "revenue", "active"]
        assert vt.source_label == ""

    def test_with_row_key(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema, row_key="id")
        assert vt.row_key == "id"

    def test_with_source_label(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema, source_label="test data")
        assert vt.source_label == "test data"

    def test_schema_property(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        assert len(vt.schema) == 4
        assert isinstance(vt.schema, tuple)

    def test_column_schema_lookup(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        cs = vt.column_schema("revenue")
        assert cs.dtype == ColumnType.FLOAT64

    def test_column_schema_missing(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        with pytest.raises(KeyError, match="nonexistent"):
            vt.column_schema("nonexistent")

    def test_df_accessible(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        assert vt.df.shape == (3, 4)


# ────────────────────────────────────────────────────────────────
# Immutability
# ────────────────────────────────────────────────────────────────


class TestImmutability:
    def test_cannot_set_attribute(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        with pytest.raises(AttributeError, match="immutable"):
            vt.foo = "bar"  # type: ignore[attr-defined]

    def test_cannot_delete_attribute(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        with pytest.raises(AttributeError, match="immutable"):
            del vt._df  # type: ignore[attr-defined]

    def test_cannot_set_df(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        with pytest.raises(AttributeError, match="immutable"):
            vt._df = pl.DataFrame()  # type: ignore[misc]


# ────────────────────────────────────────────────────────────────
# Deterministic row order
# ────────────────────────────────────────────────────────────────


class TestRowOrder:
    def test_order_preserved(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        assert vt.df["name"].to_list() == ["Alice", "Bob", "Charlie"]

    def test_reverse_order_preserved(self, sample_schema: list[ColumnSchema]) -> None:
        df = pl.DataFrame(
            {
                "id": [3, 2, 1],
                "name": ["Charlie", "Bob", "Alice"],
                "revenue": [300.0, 200.0, 100.0],
                "active": [True, False, True],
            }
        )
        vt = ViewTable(df, sample_schema)
        assert vt.df["name"].to_list() == ["Charlie", "Bob", "Alice"]


# ────────────────────────────────────────────────────────────────
# Schema validation
# ────────────────────────────────────────────────────────────────


class TestSchemaValidation:
    def test_empty_schema_rejected(self, sample_df: pl.DataFrame) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            ViewTable(sample_df, [])

    def test_missing_column_in_df(self) -> None:
        df = pl.DataFrame({"a": [1]})
        schema = [
            ColumnSchema(name="a", dtype=ColumnType.INT64),
            ColumnSchema(name="b", dtype=ColumnType.INT64),
        ]
        with pytest.raises(ValueError, match="not in DataFrame"):
            ViewTable(df, schema)

    def test_extra_column_in_df(self) -> None:
        df = pl.DataFrame({"a": [1], "b": [2]})
        schema = [ColumnSchema(name="a", dtype=ColumnType.INT64)]
        with pytest.raises(ValueError, match="not in schema"):
            ViewTable(df, schema)

    def test_type_mismatch(self) -> None:
        df = pl.DataFrame({"x": ["hello"]})
        schema = [ColumnSchema(name="x", dtype=ColumnType.FLOAT64)]
        with pytest.raises(ValueError, match="incompatible"):
            ViewTable(df, schema)

    def test_int_compatible_with_float64(self) -> None:
        """Integer columns are compatible with float64 declared type."""
        df = pl.DataFrame({"x": [1, 2, 3]})
        schema = [ColumnSchema(name="x", dtype=ColumnType.FLOAT64)]
        vt = ViewTable(df, schema)
        assert vt.row_count == 3

    def test_duplicate_schema_names(self) -> None:
        df = pl.DataFrame({"a": [1]})
        schema = [
            ColumnSchema(name="a", dtype=ColumnType.INT64),
            ColumnSchema(name="a", dtype=ColumnType.INT64),
        ]
        with pytest.raises(ValueError, match="Duplicate"):
            ViewTable(df, schema)

    def test_bool_dtype_validated(self) -> None:
        df = pl.DataFrame({"flag": [True, False]})
        schema = [ColumnSchema(name="flag", dtype=ColumnType.BOOL)]
        vt = ViewTable(df, schema)
        assert vt.row_count == 2

    def test_date_dtype_validated(self) -> None:
        df = pl.DataFrame({"d": [datetime.date(2025, 1, 1)]})
        schema = [ColumnSchema(name="d", dtype=ColumnType.DATE)]
        vt = ViewTable(df, schema)
        assert vt.row_count == 1


# ────────────────────────────────────────────────────────────────
# row_key validation
# ────────────────────────────────────────────────────────────────


class TestRowKey:
    def test_valid_row_key(self) -> None:
        df = pl.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.INT64),
        ]
        vt = ViewTable(df, schema, row_key="id")
        assert vt.row_key == "id"

    def test_duplicate_key_values_rejected(self) -> None:
        df = pl.DataFrame({"id": [1, 1, 2], "val": [10, 20, 30]})
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.INT64),
        ]
        with pytest.raises(ValueError, match="duplicate"):
            ViewTable(df, schema, row_key="id")

    def test_null_key_values_rejected(self) -> None:
        df = pl.DataFrame({"id": [1, None, 3], "val": [10, 20, 30]})
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.INT64),
        ]
        with pytest.raises(ValueError, match="null"):
            ViewTable(df, schema, row_key="id")

    def test_nonexistent_key_rejected(self) -> None:
        df = pl.DataFrame({"id": [1], "val": [10]})
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.INT64),
        ]
        with pytest.raises(ValueError, match="not found"):
            ViewTable(df, schema, row_key="missing")

    def test_no_row_key_is_default(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = ViewTable(sample_df, sample_schema)
        assert vt.row_key is None


# ────────────────────────────────────────────────────────────────
# suggest_schema helper
# ────────────────────────────────────────────────────────────────


class TestSuggestSchema:
    def test_basic(self, sample_df: pl.DataFrame) -> None:
        schema = suggest_schema(sample_df)
        assert len(schema) == 4
        names = [cs.name for cs in schema]
        assert names == ["id", "name", "revenue", "active"]

    def test_types_inferred(self, sample_df: pl.DataFrame) -> None:
        schema = suggest_schema(sample_df)
        by_name = {cs.name: cs for cs in schema}
        assert by_name["id"].dtype == ColumnType.INT64
        assert by_name["name"].dtype == ColumnType.STRING
        assert by_name["revenue"].dtype == ColumnType.FLOAT64
        assert by_name["active"].dtype == ColumnType.BOOL

    def test_nullable_detected(self) -> None:
        df = pl.DataFrame({"x": [1, None, 3], "y": [1, 2, 3]})
        schema = suggest_schema(df)
        by_name = {cs.name: cs for cs in schema}
        assert by_name["x"].nullable is True
        assert by_name["y"].nullable is False

    def test_not_a_constructor(self, sample_df: pl.DataFrame) -> None:
        """suggest_schema returns a list, not a ViewTable."""
        result = suggest_schema(sample_df)
        assert isinstance(result, list)
        assert all(isinstance(cs, ColumnSchema) for cs in result)


# ────────────────────────────────────────────────────────────────
# from_polars adapter
# ────────────────────────────────────────────────────────────────


class TestFromPolars:
    def test_basic(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = from_polars(sample_df, sample_schema)
        assert vt.row_count == 3
        assert vt.columns == ["id", "name", "revenue", "active"]

    def test_with_row_key(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = from_polars(sample_df, sample_schema, row_key="id")
        assert vt.row_key == "id"

    def test_with_source_label(self, sample_df: pl.DataFrame, sample_schema: list[ColumnSchema]) -> None:
        vt = from_polars(sample_df, sample_schema, source_label="test")
        assert vt.source_label == "test"

    def test_schema_required(self, sample_df: pl.DataFrame) -> None:
        """Schema is a required argument — no inference."""
        with pytest.raises(TypeError):
            from_polars(sample_df)  # type: ignore[call-arg]


# ────────────────────────────────────────────────────────────────
# from_json_records adapter
# ────────────────────────────────────────────────────────────────


class TestFromJsonRecords:
    def test_basic(self) -> None:
        records = [
            {"id": 1, "name": "Alice", "value": 10.0},
            {"id": 2, "name": "Bob", "value": 20.0},
        ]
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="name", dtype=ColumnType.STRING),
            ColumnSchema(name="value", dtype=ColumnType.FLOAT64),
        ]
        vt = from_json_records(records, schema)
        assert vt.row_count == 2
        assert vt.df["name"].to_list() == ["Alice", "Bob"]

    def test_with_row_key(self) -> None:
        records = [{"id": "a", "val": 1}, {"id": "b", "val": 2}]
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.STRING),
            ColumnSchema(name="val", dtype=ColumnType.INT64),
        ]
        vt = from_json_records(records, schema, row_key="id")
        assert vt.row_key == "id"

    def test_empty_records(self) -> None:
        schema = [
            ColumnSchema(name="x", dtype=ColumnType.INT64),
            ColumnSchema(name="y", dtype=ColumnType.STRING),
        ]
        vt = from_json_records([], schema)
        assert vt.row_count == 0
        assert vt.columns == ["x", "y"]

    def test_order_preserved(self) -> None:
        records = [
            {"id": 3, "name": "C"},
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
        ]
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="name", dtype=ColumnType.STRING),
        ]
        vt = from_json_records(records, schema)
        assert vt.df["name"].to_list() == ["C", "A", "B"]

    def test_schema_required(self) -> None:
        with pytest.raises(TypeError):
            from_json_records([{"a": 1}])  # type: ignore[call-arg]


# ────────────────────────────────────────────────────────────────
# from_json_file adapter
# ────────────────────────────────────────────────────────────────


class TestFromJsonFile:
    def test_basic(self, tmp_path) -> None:
        records = [{"id": 1, "val": 10.5}, {"id": 2, "val": 20.5}]
        path = tmp_path / "data.json"
        path.write_text(json.dumps(records))

        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.FLOAT64),
        ]
        vt = from_json_file(path, schema)
        assert vt.row_count == 2
        assert "data.json" in vt.source_label

    def test_custom_source_label(self, tmp_path) -> None:
        path = tmp_path / "d.json"
        path.write_text(json.dumps([{"x": 1}]))
        schema = [ColumnSchema(name="x", dtype=ColumnType.INT64)]
        vt = from_json_file(path, schema, source_label="custom")
        assert vt.source_label == "custom"


# ────────────────────────────────────────────────────────────────
# from_fin123_run adapter
# ────────────────────────────────────────────────────────────────


class TestFromFin123Run:
    def _create_run(self, project_dir, table_name="test_table", run_name="20250101_120000_run_1"):
        """Helper: create a minimal fin123 run structure."""
        run_dir = project_dir / "runs" / run_name
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir(parents=True)

        df = pl.DataFrame({"item": ["A", "B"], "amount": [100.0, 200.0]})
        df.write_parquet(outputs_dir / f"{table_name}.parquet")

        meta = {"run_id": run_name, "timestamp": "2025-01-01T12:00:00"}
        (run_dir / "run_meta.json").write_text(json.dumps(meta))

        return run_dir

    def test_latest_run(self, tmp_path) -> None:
        self._create_run(tmp_path, run_name="20250101_120000_run_1")
        self._create_run(tmp_path, run_name="20250102_120000_run_2")

        vt = from_fin123_run(tmp_path, "test_table")
        assert vt.row_count == 2
        assert "run_2" in vt.source_label

    def test_specific_run(self, tmp_path) -> None:
        self._create_run(tmp_path, run_name="20250101_120000_run_1")
        self._create_run(tmp_path, run_name="20250102_120000_run_2")

        vt = from_fin123_run(tmp_path, "test_table", run_id="20250101_120000_run_1")
        assert "run_1" in vt.source_label

    def test_with_explicit_schema(self, tmp_path) -> None:
        self._create_run(tmp_path)
        schema = [
            ColumnSchema(name="item", dtype=ColumnType.STRING),
            ColumnSchema(name="amount", dtype=ColumnType.FLOAT64),
        ]
        vt = from_fin123_run(tmp_path, "test_table", schema=schema)
        assert vt.column_schema("amount").dtype == ColumnType.FLOAT64

    def test_schema_inferred_from_parquet(self, tmp_path) -> None:
        self._create_run(tmp_path)
        vt = from_fin123_run(tmp_path, "test_table")
        assert vt.column_schema("item").dtype == ColumnType.STRING
        assert vt.column_schema("amount").dtype == ColumnType.FLOAT64

    def test_with_row_key(self, tmp_path) -> None:
        self._create_run(tmp_path)
        vt = from_fin123_run(tmp_path, "test_table", row_key="item")
        assert vt.row_key == "item"

    def test_missing_table(self, tmp_path) -> None:
        self._create_run(tmp_path)
        with pytest.raises(FileNotFoundError, match="nonexistent"):
            from_fin123_run(tmp_path, "nonexistent")

    def test_no_runs_dir(self, tmp_path) -> None:
        with pytest.raises(FileNotFoundError, match="No runs"):
            from_fin123_run(tmp_path, "test_table")

    def test_no_completed_runs(self, tmp_path) -> None:
        (tmp_path / "runs").mkdir()
        with pytest.raises(FileNotFoundError, match="No completed runs"):
            from_fin123_run(tmp_path, "test_table")
