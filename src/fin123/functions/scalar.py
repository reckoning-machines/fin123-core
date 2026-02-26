"""Built-in scalar functions for the scalar graph."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from fin123.functions.registry import register_scalar


@register_scalar("sum")
def scalar_sum(values: list[float | int]) -> float:
    """Sum a list of numeric values.

    Args:
        values: Numbers to sum.

    Returns:
        The total.
    """
    return float(sum(values))


@register_scalar("mean")
def scalar_mean(values: list[float | int]) -> float:
    """Compute the arithmetic mean of numeric values.

    Args:
        values: Numbers to average.

    Returns:
        The mean.
    """
    if not values:
        return 0.0
    return float(sum(values) / len(values))


@register_scalar("multiply")
def scalar_multiply(a: float | int, b: float | int) -> float:
    """Multiply two numbers.

    Args:
        a: First operand.
        b: Second operand.

    Returns:
        Product of a and b.
    """
    return float(a * b)


@register_scalar("subtract")
def scalar_subtract(a: float | int, b: float | int) -> float:
    """Subtract b from a.

    Args:
        a: Minuend.
        b: Subtrahend.

    Returns:
        Difference.
    """
    return float(a - b)


@register_scalar("divide")
def scalar_divide(a: float | int, b: float | int) -> float:
    """Divide a by b.

    Args:
        a: Numerator.
        b: Denominator.

    Returns:
        Quotient.

    Raises:
        ZeroDivisionError: If b is zero.
    """
    return float(a / b)


@register_scalar("if")
def scalar_if(
    condition: bool, then_value: Any, else_value: Any = None
) -> Any:
    """Conditional: return then_value if condition is truthy, else else_value.

    Args:
        condition: Boolean condition.
        then_value: Value when true.
        else_value: Value when false (default None).

    Returns:
        The chosen value.
    """
    return then_value if condition else else_value


@register_scalar("min")
def scalar_min(values: list[float | int]) -> float:
    """Return the minimum of a list of numeric values.

    Args:
        values: Numbers to compare.

    Returns:
        The minimum value.
    """
    return float(min(values))


@register_scalar("max")
def scalar_max(values: list[float | int]) -> float:
    """Return the maximum of a list of numeric values.

    Args:
        values: Numbers to compare.

    Returns:
        The maximum value.
    """
    return float(max(values))


@register_scalar("abs")
def scalar_abs(value: float | int) -> float:
    """Return the absolute value.

    Args:
        value: A number.

    Returns:
        The absolute value.
    """
    return float(abs(value))


@register_scalar("round")
def scalar_round(value: float | int, digits: int = 0) -> float:
    """Round a number to the given number of decimal places.

    Args:
        value: The number to round.
        digits: Number of decimal places (default 0).

    Returns:
        The rounded value.
    """
    return float(round(value, digits))


@register_scalar("expr")
def scalar_expr(expression: str, variables: dict[str, Any] | None = None) -> float:
    """Evaluate a simple arithmetic expression with optional variables.

    Only supports basic arithmetic operators (+, -, *, /, parentheses) and
    numeric literals.  Variables are substituted before evaluation.

    Args:
        expression: The arithmetic expression string.
        variables: Optional mapping of variable names to numeric values.

    Returns:
        Result of the expression.
    """
    if variables:
        for name, val in variables.items():
            expression = expression.replace(name, str(val))
    # Restrict to safe characters
    allowed = set("0123456789.+-*/() \t")
    if not all(c in allowed for c in expression):
        raise ValueError(f"Unsafe expression: {expression!r}")
    return float(eval(expression))  # noqa: S307


@register_scalar("lookup_scalar")
def scalar_lookup(
    table_name: str,
    key_col: str,
    value_col: str,
    key_value: Any,
    on_missing: str = "error",
    on_duplicate: str = "error",
    _table_cache: dict[str, pl.DataFrame] | None = None,
    _project_dir: str | Path | None = None,
) -> Any:
    """Look up a scalar value from a table (VLOOKUP exact-match semantics).

    Searches *key_col* for *key_value* and returns the corresponding value
    from *value_col*.  The table must exist as a local cached file; this
    function never executes SQL.

    Args:
        table_name: Logical table name (must be available in the run-time
            table cache or resolvable from the project directory).
        key_col: Column to match against.
        value_col: Column to return the value from.
        key_value: The exact key to search for.
        on_missing: Policy when no matching row is found.
            ``"error"`` (default) raises; ``"none"`` returns None.
        on_duplicate: Policy when multiple rows match.
            ``"error"`` (default) raises; ``"first"`` takes the first match.
        _table_cache: In-run table materialization cache (injected by the
            workbook engine).
        _project_dir: Project directory for resolving table files (injected
            by the workbook engine).

    Returns:
        The scalar value from *value_col*, or None if on_missing="none".

    Raises:
        ValueError: If the table is not found, or if missing/duplicate
            policies are violated.
    """
    df = _resolve_table(table_name, _table_cache, _project_dir)

    if key_col not in df.columns:
        raise ValueError(
            f"lookup_scalar: key column {key_col!r} not found in table "
            f"{table_name!r}. Available columns: {df.columns}"
        )
    if value_col not in df.columns:
        raise ValueError(
            f"lookup_scalar: value column {value_col!r} not found in table "
            f"{table_name!r}. Available columns: {df.columns}"
        )

    matches = df.filter(pl.col(key_col) == key_value)

    if len(matches) == 0:
        if on_missing == "none":
            return None
        available = df[key_col].unique().sort().head(10).to_list()
        raise ValueError(
            f"lookup_scalar: no row found in table {table_name!r} where "
            f"{key_col}=={key_value!r}. "
            f"Available keys (up to 10): {available}"
        )

    if len(matches) > 1:
        if on_duplicate == "first":
            pass  # take first below
        else:
            raise ValueError(
                f"lookup_scalar: {len(matches)} rows found in table "
                f"{table_name!r} where {key_col}=={key_value!r}. "
                f"Use on_duplicate='first' to take the first match."
            )

    value = matches[value_col][0]
    # Convert Polars types to Python natives
    if isinstance(value, (int, float, str, bool, type(None))):
        return value
    return float(value)


def _resolve_table(
    table_name: str,
    table_cache: dict[str, pl.DataFrame] | None,
    project_dir: str | Path | None,
) -> pl.DataFrame:
    """Resolve a table by name, using the in-run cache or loading from disk.

    Args:
        table_name: Logical table name.
        table_cache: In-run table materialization cache.
        project_dir: Project directory for file-based resolution.

    Returns:
        The resolved DataFrame.

    Raises:
        ValueError: If the table cannot be found.
    """
    if table_cache and table_name in table_cache:
        return table_cache[table_name]

    raise ValueError(
        f"lookup_scalar: table {table_name!r} not found in run cache. "
        f"Ensure it is defined in the workbook tables section."
    )
