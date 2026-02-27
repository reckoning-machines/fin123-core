"""View-only table sort/filter transforms for the browser UI.

Provides Pydantic models and a pure Polars transform function.
The underlying data is never mutated — transforms produce new DataFrames.
"""

from __future__ import annotations

from typing import Any, Literal

import polars as pl
from pydantic import BaseModel


# ────────────────────────────────────────────────────────────────
# Sort spec
# ────────────────────────────────────────────────────────────────


class SortSpec(BaseModel):
    column: str
    descending: bool = False


# ────────────────────────────────────────────────────────────────
# Filter specs (discriminated union)
# ────────────────────────────────────────────────────────────────


class NumericFilter(BaseModel):
    type: Literal["numeric"] = "numeric"
    column: str
    op: Literal["=", "<>", ">", "<", ">=", "<="]
    value: float


class BetweenFilter(BaseModel):
    type: Literal["between"] = "between"
    column: str
    low: float
    high: float


class TextFilter(BaseModel):
    type: Literal["text"] = "text"
    column: str
    op: Literal["contains", "starts_with", "ends_with", "equals"]
    value: str
    case_sensitive: bool = False


class ValueListFilter(BaseModel):
    type: Literal["value_list"] = "value_list"
    column: str
    values: list[Any]


class BlanksFilter(BaseModel):
    type: Literal["blanks"] = "blanks"
    column: str
    show_blanks: bool = True


FilterSpec = NumericFilter | BetweenFilter | TextFilter | ValueListFilter | BlanksFilter


# ────────────────────────────────────────────────────────────────
# Request model
# ────────────────────────────────────────────────────────────────


class TableViewRequest(BaseModel):
    name: str
    run_id: str | None = None
    limit: int = 5000
    sorts: list[SortSpec] = []
    filters: list[FilterSpec] = []


# ────────────────────────────────────────────────────────────────
# Transform engine
# ────────────────────────────────────────────────────────────────

_ROW_IDX_COL = "__view_row_idx__"


def apply_view_transforms(
    df: pl.DataFrame,
    sorts: list[SortSpec] | None = None,
    filters: list[FilterSpec] | None = None,
) -> pl.DataFrame:
    """Apply view-only sort/filter transforms to a DataFrame.

    Transform order: inject row index -> filter -> sort.
    The row index ensures stable tie-breaking for sorts.

    Args:
        df: Source DataFrame (not mutated).
        sorts: Sort specifications (applied in order).
        filters: Filter specifications (applied in order).

    Returns:
        Transformed DataFrame (without the internal row index column).
    """
    sorts = sorts or []
    filters = filters or []

    # Inject deterministic row index for stable tie-breaking
    result = df.with_row_index(_ROW_IDX_COL)

    # Apply filters first (matches Excel behavior)
    for f in filters:
        result = _apply_filter(result, f)

    # Apply sorts
    if sorts:
        by_cols = [s.column for s in sorts] + [_ROW_IDX_COL]
        descending = [s.descending for s in sorts] + [False]
        result = result.sort(
            by=by_cols,
            descending=descending,
            nulls_last=True,
            maintain_order=True,
        )

    # Drop internal column
    return result.drop(_ROW_IDX_COL)


def _apply_filter(df: pl.DataFrame, spec: FilterSpec) -> pl.DataFrame:
    """Apply a single filter spec to a DataFrame."""
    if isinstance(spec, NumericFilter):
        ops = {
            "=": lambda c, v: pl.col(c) == v,
            "<>": lambda c, v: pl.col(c) != v,
            ">": lambda c, v: pl.col(c) > v,
            "<": lambda c, v: pl.col(c) < v,
            ">=": lambda c, v: pl.col(c) >= v,
            "<=": lambda c, v: pl.col(c) <= v,
        }
        return df.filter(ops[spec.op](spec.column, spec.value))

    if isinstance(spec, BetweenFilter):
        return df.filter(
            (pl.col(spec.column) >= spec.low) & (pl.col(spec.column) <= spec.high)
        )

    if isinstance(spec, TextFilter):
        col = pl.col(spec.column).cast(pl.Utf8)
        val = spec.value
        if not spec.case_sensitive:
            col = col.str.to_lowercase()
            val = val.lower()
        if spec.op == "contains":
            return df.filter(col.str.contains(val, literal=True))
        if spec.op == "starts_with":
            return df.filter(col.str.starts_with(val))
        if spec.op == "ends_with":
            return df.filter(col.str.ends_with(val))
        if spec.op == "equals":
            return df.filter(col == val)
        return df  # Unreachable, but defensive

    if isinstance(spec, ValueListFilter):
        return df.filter(pl.col(spec.column).is_in(spec.values))

    if isinstance(spec, BlanksFilter):
        if spec.show_blanks:
            return df.filter(pl.col(spec.column).is_null())
        else:
            return df.filter(pl.col(spec.column).is_not_null())

    return df
