"""ViewTable: typed tabular substrate for worksheet compilation.

A ViewTable is an immutable, schema-validated wrapper around a Polars
DataFrame. It guarantees deterministic row order and optional stable
row identity via row_key.

Construction requires an explicit schema — no implicit inference.
Use suggest_schema() as a helper to generate a starting schema from
a DataFrame, then pass it to a constructor.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from fin123.worksheet.types import ColumnSchema, ColumnType

# ────────────────────────────────────────────────────────────────
# Polars dtype → ColumnType mapping
# ────────────────────────────────────────────────────────────────

_POLARS_TO_COLUMN_TYPE: dict[type, ColumnType] = {
    pl.Utf8: ColumnType.STRING,
    pl.String: ColumnType.STRING,
    pl.Categorical: ColumnType.STRING,
    pl.Int8: ColumnType.INT64,
    pl.Int16: ColumnType.INT64,
    pl.Int32: ColumnType.INT64,
    pl.Int64: ColumnType.INT64,
    pl.UInt8: ColumnType.INT64,
    pl.UInt16: ColumnType.INT64,
    pl.UInt32: ColumnType.INT64,
    pl.UInt64: ColumnType.INT64,
    pl.Float32: ColumnType.FLOAT64,
    pl.Float64: ColumnType.FLOAT64,
    pl.Boolean: ColumnType.BOOL,
    pl.Date: ColumnType.DATE,
    pl.Datetime: ColumnType.DATETIME,
}

# Which ColumnTypes accept which Polars dtype families
_COMPATIBLE_TYPES: dict[ColumnType, set[ColumnType]] = {
    ColumnType.FLOAT64: {ColumnType.FLOAT64, ColumnType.INT64},
    ColumnType.INT64: {ColumnType.INT64},
    ColumnType.STRING: {ColumnType.STRING},
    ColumnType.BOOL: {ColumnType.BOOL},
    ColumnType.DATE: {ColumnType.DATE},
    ColumnType.DATETIME: {ColumnType.DATETIME},
}


def _polars_dtype_to_column_type(dtype: pl.DataType) -> ColumnType:
    """Map a Polars dtype to the nearest ColumnType."""
    dtype_class = type(dtype)
    if dtype_class in _POLARS_TO_COLUMN_TYPE:
        return _POLARS_TO_COLUMN_TYPE[dtype_class]
    return ColumnType.STRING


def _check_type_compatible(
    declared: ColumnType, actual: ColumnType, col_name: str
) -> str | None:
    """Check if the actual Polars-inferred type is compatible with the declared type.

    Returns an error message if incompatible, None if compatible.
    """
    compatible = _COMPATIBLE_TYPES.get(declared, {declared})
    if actual in compatible:
        return None
    return (
        f"Column '{col_name}': declared type '{declared.value}' "
        f"is incompatible with actual type '{actual.value}'"
    )


# ────────────────────────────────────────────────────────────────
# ViewTable
# ────────────────────────────────────────────────────────────────


class ViewTable:
    """Immutable typed table that a WorksheetView compiles against.

    Construction requires an explicit schema. Row order is deterministic
    (preserved from the source). An optional row_key column provides
    stable row identity for diffing.

    No mutable methods. Once constructed, the ViewTable is fixed.
    """

    __slots__ = ("_df", "_schema", "_row_key", "_source_label", "_schema_map")

    def __init__(
        self,
        df: pl.DataFrame,
        schema: list[ColumnSchema],
        row_key: str | None = None,
        source_label: str = "",
    ) -> None:
        _validate_schema(df, schema, row_key)
        object.__setattr__(self, "_df", df)
        object.__setattr__(self, "_schema", tuple(schema))
        object.__setattr__(self, "_row_key", row_key)
        object.__setattr__(self, "_source_label", source_label)
        object.__setattr__(
            self, "_schema_map", {cs.name: cs for cs in schema}
        )

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("ViewTable is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("ViewTable is immutable")

    @property
    def df(self) -> pl.DataFrame:
        return self._df

    @property
    def schema(self) -> tuple[ColumnSchema, ...]:
        return self._schema

    @property
    def row_key(self) -> str | None:
        return self._row_key

    @property
    def source_label(self) -> str:
        return self._source_label

    @property
    def columns(self) -> list[str]:
        return list(self._df.columns)

    @property
    def row_count(self) -> int:
        return len(self._df)

    def column_schema(self, name: str) -> ColumnSchema:
        """Look up the schema for a column by name.

        Raises:
            KeyError: If the column does not exist.
        """
        if name not in self._schema_map:
            raise KeyError(f"Column '{name}' not in schema")
        return self._schema_map[name]


# ────────────────────────────────────────────────────────────────
# Validation
# ────────────────────────────────────────────────────────────────


def _validate_schema(
    df: pl.DataFrame,
    schema: list[ColumnSchema],
    row_key: str | None,
) -> None:
    """Validate schema against DataFrame. Raises ValueError on mismatch."""
    errors: list[str] = []

    if not schema:
        errors.append("Schema must not be empty")

    schema_names = [cs.name for cs in schema]
    df_columns = df.columns

    # Check for duplicate schema names
    seen: set[str] = set()
    for name in schema_names:
        if name in seen:
            errors.append(f"Duplicate column in schema: '{name}'")
        seen.add(name)

    # Schema columns must match DataFrame columns exactly
    schema_set = set(schema_names)
    df_set = set(df_columns)

    missing_in_df = schema_set - df_set
    extra_in_df = df_set - schema_set

    if missing_in_df:
        errors.append(
            f"Schema columns not in DataFrame: {sorted(missing_in_df)}"
        )
    if extra_in_df:
        errors.append(
            f"DataFrame columns not in schema: {sorted(extra_in_df)}"
        )

    # Type compatibility check (only for columns present in both)
    for cs in schema:
        if cs.name in df_set:
            actual = _polars_dtype_to_column_type(df[cs.name].dtype)
            err = _check_type_compatible(cs.dtype, actual, cs.name)
            if err:
                errors.append(err)

    # row_key validation
    if row_key is not None:
        if row_key not in schema_set:
            errors.append(f"row_key '{row_key}' not found in schema")
        elif row_key in df_set:
            col = df[row_key]
            if col.n_unique() != len(col):
                n_dupes = len(col) - col.n_unique()
                errors.append(
                    f"row_key '{row_key}' has {n_dupes} duplicate value(s)"
                )
            if col.null_count() > 0:
                errors.append(f"row_key '{row_key}' contains null values")

    if errors:
        raise ValueError(
            "ViewTable validation failed:\n  - " + "\n  - ".join(errors)
        )


# ────────────────────────────────────────────────────────────────
# suggest_schema helper
# ────────────────────────────────────────────────────────────────


def suggest_schema(df: pl.DataFrame) -> list[ColumnSchema]:
    """Suggest a schema from a DataFrame's Polars dtypes.

    This is a convenience helper — not a constructor. The returned
    schema should be reviewed and passed to a ViewTable constructor.
    """
    return [
        ColumnSchema(
            name=col,
            dtype=_polars_dtype_to_column_type(df[col].dtype),
            nullable=df[col].null_count() > 0,
        )
        for col in df.columns
    ]


# ────────────────────────────────────────────────────────────────
# Adapters
# ────────────────────────────────────────────────────────────────


def from_polars(
    df: pl.DataFrame,
    schema: list[ColumnSchema],
    row_key: str | None = None,
    source_label: str = "",
) -> ViewTable:
    """Build a ViewTable from a Polars DataFrame with an explicit schema.

    Args:
        df: Source DataFrame. Row order is preserved.
        schema: Explicit column schema (required).
        row_key: Optional column providing stable row identity.
        source_label: Human-readable provenance string.

    Returns:
        Validated, immutable ViewTable.
    """
    return ViewTable(df, schema, row_key=row_key, source_label=source_label)


def from_json_records(
    records: list[dict[str, Any]],
    schema: list[ColumnSchema],
    row_key: str | None = None,
    source_label: str = "",
) -> ViewTable:
    """Build a ViewTable from in-memory row-oriented records.

    Args:
        records: List of dicts, each dict is one row.
        schema: Explicit column schema (required).
        row_key: Optional column providing stable row identity.
        source_label: Human-readable provenance string.

    Returns:
        Validated, immutable ViewTable.
    """
    if not records:
        # Empty table — build from schema column names
        df = pl.DataFrame(
            {cs.name: pl.Series([], dtype=_column_type_to_polars(cs.dtype)) for cs in schema}
        )
    else:
        df = pl.DataFrame(records)

    return ViewTable(df, schema, row_key=row_key, source_label=source_label)


def from_json_file(
    path: str | Path,
    schema: list[ColumnSchema],
    row_key: str | None = None,
    source_label: str | None = None,
) -> ViewTable:
    """Build a ViewTable from a JSON file containing row-oriented records.

    Thin wrapper around from_json_records.
    """
    path = Path(path)
    with open(path) as f:
        records = json.load(f)
    label = source_label if source_label is not None else f"json:{path.name}"
    return from_json_records(records, schema, row_key=row_key, source_label=label)


def from_fin123_run(
    project_dir: str | Path,
    table_name: str,
    run_id: str | None = None,
    schema: list[ColumnSchema] | None = None,
    row_key: str | None = None,
) -> ViewTable:
    """Build a ViewTable from a fin123 build run's output table.

    Reads the parquet file from the run's outputs directory. If no schema
    is provided, one is inferred from the parquet file's typed columns
    (parquet has an explicit type system, so this is not guessing).

    Args:
        project_dir: Path to the fin123 project root.
        table_name: Name of the output table.
        run_id: Specific run directory name. If None, uses the latest run.
        schema: Explicit column schema. If None, inferred from parquet.
        row_key: Optional column providing stable row identity.

    Returns:
        Validated, immutable ViewTable.

    Raises:
        FileNotFoundError: If the run or table does not exist.
    """
    project_dir = Path(project_dir)
    runs_dir = project_dir / "runs"

    if not runs_dir.exists():
        raise FileNotFoundError(f"No runs directory at {runs_dir}")

    if run_id is not None:
        run_dir = runs_dir / run_id
    else:
        # Latest run: sort by directory name (includes timestamp)
        run_dirs = sorted(
            d for d in runs_dir.iterdir()
            if d.is_dir() and (d / "run_meta.json").exists()
        )
        if not run_dirs:
            raise FileNotFoundError("No completed runs found")
        run_dir = run_dirs[-1]

    parquet_path = run_dir / "outputs" / f"{table_name}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Table '{table_name}' not found in run {run_dir.name}"
        )

    df = pl.read_parquet(parquet_path)

    if schema is None:
        schema = suggest_schema(df)

    source_label = f"fin123 run {run_dir.name} / {table_name}"
    return ViewTable(df, schema, row_key=row_key, source_label=source_label)


# ────────────────────────────────────────────────────────────────
# ColumnType → Polars dtype (for empty DataFrame construction)
# ────────────────────────────────────────────────────────────────

_COLUMN_TYPE_TO_POLARS: dict[ColumnType, pl.DataType] = {
    ColumnType.STRING: pl.Utf8,
    ColumnType.INT64: pl.Int64,
    ColumnType.FLOAT64: pl.Float64,
    ColumnType.BOOL: pl.Boolean,
    ColumnType.DATE: pl.Date,
    ColumnType.DATETIME: pl.Datetime,
}


def _column_type_to_polars(ct: ColumnType) -> pl.DataType:
    return _COLUMN_TYPE_TO_POLARS.get(ct, pl.Utf8)
