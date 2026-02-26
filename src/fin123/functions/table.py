"""Built-in table functions operating on Polars LazyFrames."""

from __future__ import annotations

from typing import Any

import polars as pl

from fin123.functions.registry import register_table


@register_table("select")
def table_select(lf: pl.LazyFrame, columns: list[str], **_: Any) -> pl.LazyFrame:
    """Select specific columns from a LazyFrame.

    Args:
        lf: Input LazyFrame.
        columns: Column names to keep.

    Returns:
        LazyFrame with only the specified columns.
    """
    return lf.select(columns)


@register_table("filter")
def table_filter(lf: pl.LazyFrame, column: str, op: str, value: Any, **_: Any) -> pl.LazyFrame:
    """Filter rows based on a comparison.

    Args:
        lf: Input LazyFrame.
        column: Column to filter on.
        op: Comparison operator (``>``, ``>=``, ``<``, ``<=``, ``==``, ``!=``).
        value: Value to compare against.

    Returns:
        Filtered LazyFrame.
    """
    col = pl.col(column)
    ops = {
        ">": col > value,
        ">=": col >= value,
        "<": col < value,
        "<=": col <= value,
        "==": col == value,
        "!=": col != value,
    }
    if op not in ops:
        raise ValueError(f"Unsupported filter operator: {op!r}")
    return lf.filter(ops[op])


@register_table("group_agg")
def table_group_agg(
    lf: pl.LazyFrame,
    group_by: list[str],
    aggs: dict[str, str],
    **_: Any,
) -> pl.LazyFrame:
    """Group by columns and aggregate.

    Args:
        lf: Input LazyFrame.
        group_by: Columns to group by.
        aggs: Mapping of ``output_col`` to ``func(source_col)`` strings.
              Supported funcs: ``sum``, ``mean``, ``min``, ``max``, ``count``.

    Returns:
        Aggregated LazyFrame.
    """
    agg_exprs = []
    for out_name, spec in aggs.items():
        func_name, col_name = _parse_agg_spec(spec)
        base = pl.col(col_name)
        agg_fn = {
            "sum": base.sum,
            "mean": base.mean,
            "min": base.min,
            "max": base.max,
            "count": base.count,
        }
        if func_name not in agg_fn:
            raise ValueError(f"Unsupported agg function: {func_name!r}")
        agg_exprs.append(agg_fn[func_name]().alias(out_name))
    return lf.group_by(group_by, maintain_order=True).agg(agg_exprs)


@register_table("sort")
def table_sort(
    lf: pl.LazyFrame,
    by: list[str],
    descending: bool | list[bool] = False,
    **_: Any,
) -> pl.LazyFrame:
    """Sort a LazyFrame by one or more columns.

    Args:
        lf: Input LazyFrame.
        by: Columns to sort by.
        descending: Sort order(s).

    Returns:
        Sorted LazyFrame.
    """
    return lf.sort(by, descending=descending, nulls_last=True)


@register_table("with_column")
def table_with_column(
    lf: pl.LazyFrame,
    name: str,
    expression: str,
    **_: Any,
) -> pl.LazyFrame:
    """Add or replace a column using a Polars expression string.

    Supports simple column arithmetic like ``col("a") * col("b")``.
    For safety, only ``pl.col`` and basic arithmetic are allowed.

    Args:
        lf: Input LazyFrame.
        name: Name for the new column.
        expression: Polars expression string.

    Returns:
        LazyFrame with the new column.
    """
    expr = _safe_eval_expr(expression)
    return lf.with_columns(expr.alias(name))


@register_table("join_left")
def table_join_left(
    lf: pl.LazyFrame,
    right: str = "",
    on: str | list[str] = "",
    left_on: str | list[str] | None = None,
    right_on: str | list[str] | None = None,
    validate: str = "many_to_one",
    _tables: dict[str, pl.LazyFrame] | None = None,
    **_: Any,
) -> pl.LazyFrame:
    """Left join with deterministic duplicate validation.

    For finance-grade VLOOKUP-like joins, the right side is validated against
    duplicates by default (``validate="many_to_one"``).

    Args:
        lf: Left LazyFrame.
        right: Name of the right table (resolved from ``_tables``).
        on: Join column(s) when left and right column names match.
        left_on: Left join column(s) (when names differ).
        right_on: Right join column(s) (when names differ).
        validate: Join cardinality validation.  One of ``one_to_one``,
                  ``many_to_one``, ``one_to_many``, ``many_to_many``, ``none``.
                  Default is ``many_to_one`` which raises on right-side
                  duplicates in the join key.
        _tables: Dict of available tables (injected by the table graph evaluator).

    Returns:
        Joined LazyFrame.

    Raises:
        ValueError: If the right table is not found, or if validation fails.
    """
    if _tables is None or right not in _tables:
        raise ValueError(
            f"join_left: right table {right!r} not found. "
            f"Available: {list((_tables or {}).keys())}"
        )
    right_lf = _tables[right]

    # Normalize join keys
    join_on = [on] if isinstance(on, str) and on else (on if isinstance(on, list) else [])
    l_on = [left_on] if isinstance(left_on, str) else left_on
    r_on = [right_on] if isinstance(right_on, str) else right_on

    # Determine effective key columns for validation
    effective_join_on = join_on or []
    effective_l_on = l_on
    effective_r_on = r_on

    if not effective_join_on and not (effective_l_on and effective_r_on):
        raise ValueError("join_left: must specify 'on' or both 'left_on' and 'right_on'")

    # Dtype compatibility check on join keys
    _check_join_key_dtypes(lf, right_lf, effective_join_on, effective_l_on, effective_r_on)

    # Validate right-side duplicates (must collect to check)
    if validate != "none":
        _validate_join(right_lf, effective_join_on or effective_r_on or [], validate)

    if effective_join_on:
        return lf.join(right_lf, on=effective_join_on, how="left")
    else:
        return lf.join(right_lf, left_on=effective_l_on, right_on=effective_r_on, how="left")


def _check_join_key_dtypes(
    left_lf: pl.LazyFrame,
    right_lf: pl.LazyFrame,
    on: list[str],
    left_on: list[str] | None,
    right_on: list[str] | None,
) -> None:
    """Validate that join key columns have compatible dtypes.

    Raises a hard error if the left and right key columns have incompatible
    types (e.g. joining a string column to an integer column).

    Args:
        left_lf: Left LazyFrame.
        right_lf: Right LazyFrame.
        on: Shared join key columns (when names match).
        left_on: Left key columns (when names differ).
        right_on: Right key columns (when names differ).

    Raises:
        TypeError: If any join key pair has incompatible dtypes.
    """
    left_schema = left_lf.collect_schema()
    right_schema = right_lf.collect_schema()

    if on:
        pairs = [(c, c) for c in on]
    elif left_on and right_on:
        pairs = list(zip(left_on, right_on))
    else:
        return

    # Explicit compatible dtype families
    _COMPATIBLE_FAMILIES: dict[str, set[type]] = {
        "numeric": {
            pl.Int8, pl.Int16, pl.Int32, pl.Int64,
            pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
            pl.Float32, pl.Float64,
        },
        "temporal_date": {pl.Date, pl.Datetime},
        "string": {pl.Utf8, pl.String},
    }

    def _compatible(dt_a: pl.DataType, dt_b: pl.DataType) -> bool:
        for family in _COMPATIBLE_FAMILIES.values():
            if type(dt_a) in family and type(dt_b) in family:
                return True
        return False

    for l_col, r_col in pairs:
        if l_col not in left_schema:
            raise ValueError(
                f"join_left: left key column {l_col!r} not found. "
                f"Available: {list(left_schema.names())}"
            )
        if r_col not in right_schema:
            raise ValueError(
                f"join_left: right key column {r_col!r} not found. "
                f"Available: {list(right_schema.names())}"
            )
        l_dtype = left_schema[l_col]
        r_dtype = right_schema[r_col]
        if l_dtype != r_dtype and not _compatible(l_dtype, r_dtype):
            raise TypeError(
                f"join_left: dtype mismatch on key columns "
                f"{l_col!r} ({l_dtype}) vs {r_col!r} ({r_dtype}). "
                f"Cast columns to compatible types before joining."
            )


def _validate_join(
    right_lf: pl.LazyFrame, key_cols: list[str], validate: str
) -> None:
    """Check join cardinality constraints on the right side.

    Uses a lazy group-by aggregation to detect duplicates without
    materializing the full right table up-front.

    Args:
        right_lf: Right-side LazyFrame.
        key_cols: Join key column(s).
        validate: Validation mode.

    Raises:
        ValueError: If validation fails.  The error message includes up to
            5 sample duplicate key values.
    """
    if not key_cols or validate in ("many_to_many", "one_to_many"):
        return

    # Reject null join keys under strict validation (many_to_one, one_to_one)
    if validate in ("many_to_one", "one_to_one"):
        null_filter = pl.lit(False)
        for kc in key_cols:
            null_filter = null_filter | pl.col(kc).is_null()
        null_count = right_lf.filter(null_filter).select(pl.len()).collect().item()
        if null_count > 0:
            raise ValueError(
                f"join_left validate={validate!r}: right table has "
                f"{null_count} row(s) with null join key(s) on {key_cols}. "
                f"Clean nulls or use validate='many_to_many'."
            )

    # Use lazy aggregation: group by key cols, count, filter count > 1
    dup_df = (
        right_lf
        .group_by(key_cols)
        .agg(pl.len().alias("_dup_count"))
        .filter(pl.col("_dup_count") > 1)
        .collect()
    )

    if len(dup_df) > 0:
        sample_keys = dup_df.select(key_cols).head(5).to_dicts()
        sample_str = ", ".join(str(row) for row in sample_keys)
        raise ValueError(
            f"join_left validate={validate!r}: right table has "
            f"{len(dup_df)} duplicate key group(s) on {key_cols}. "
            f"Sample duplicates: {sample_str}. "
            f"Use validate='many_to_many' to allow duplicates."
        )


def _parse_agg_spec(spec: str) -> tuple[str, str]:
    """Parse an aggregation spec like ``sum(revenue)`` into (func, col).

    Args:
        spec: String in the form ``func(column)``.

    Returns:
        Tuple of (function_name, column_name).
    """
    spec = spec.strip()
    if "(" not in spec or not spec.endswith(")"):
        raise ValueError(f"Invalid agg spec: {spec!r}. Expected format: func(column)")
    func_name = spec[: spec.index("(")]
    col_name = spec[spec.index("(") + 1 : -1]
    return func_name.strip(), col_name.strip()


def _safe_eval_expr(expression: str) -> pl.Expr:
    """Evaluate a restricted Polars expression string.

    Only ``pl.col``, ``pl.lit``, and basic arithmetic operators are allowed.

    Args:
        expression: The expression string.

    Returns:
        A Polars Expr object.
    """
    namespace = {"pl": pl, "col": pl.col, "lit": pl.lit}
    return eval(expression, {"__builtins__": {}}, namespace)  # noqa: S307
