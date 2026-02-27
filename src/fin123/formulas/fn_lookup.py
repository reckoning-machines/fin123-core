"""Lookup formula functions: MATCH, INDEX, XLOOKUP."""

from __future__ import annotations

from typing import Any

import polars as pl

from fin123.formulas.errors import FormulaFunctionError


def _get_table(tc: dict[str, pl.DataFrame], table_name: str, func_name: str) -> pl.DataFrame:
    """Resolve a table from the cache, raising on miss."""
    if not tc:
        raise FormulaFunctionError(func_name, f"{func_name}: no table cache available")
    if table_name not in tc:
        raise FormulaFunctionError(
            func_name, f"{func_name}: table {table_name!r} not found in table cache"
        )
    return tc[table_name]


def _fn_match(args: list, ctx: dict, tc: dict, resolver: Any) -> int:
    """MATCH(value, "table", "column") — return 1-based row index of first match."""
    if len(args) != 3:
        raise FormulaFunctionError("MATCH", "MATCH requires 3 arguments (value, table, column)")
    value, table_name, col_name = args[0], args[1], args[2]
    df = _get_table(tc, table_name, "MATCH")
    if col_name not in df.columns:
        raise FormulaFunctionError(
            "MATCH", f"MATCH: column {col_name!r} not found in table {table_name!r}"
        )
    series = df[col_name]
    for i in range(len(series)):
        if series[i] == value:
            return i + 1  # 1-based
    raise FormulaFunctionError(
        "MATCH", f"MATCH: value {value!r} not found in {table_name!r}.{col_name!r}"
    )


def _fn_index(args: list, ctx: dict, tc: dict, resolver: Any) -> Any:
    """INDEX("table", "column", row_num) — return value at 1-based row position."""
    if len(args) != 3:
        raise FormulaFunctionError("INDEX", "INDEX requires 3 arguments (table, column, row_num)")
    table_name, col_name, row_num = args[0], args[1], int(args[2])
    df = _get_table(tc, table_name, "INDEX")
    if col_name not in df.columns:
        raise FormulaFunctionError(
            "INDEX", f"INDEX: column {col_name!r} not found in table {table_name!r}"
        )
    if row_num < 1 or row_num > len(df):
        raise FormulaFunctionError(
            "INDEX", f"INDEX: row {row_num} out of range (table has {len(df)} rows)"
        )
    val = df[col_name][row_num - 1]
    if isinstance(val, (int, float, str, bool, type(None))):
        return val
    return float(val)


def _fn_xlookup(args: list, ctx: dict, tc: dict, resolver: Any) -> Any:
    """XLOOKUP(value, "table", "lookup_col", "return_col" [, if_not_found]).

    Searches lookup_col for value and returns the corresponding value from return_col.
    Optional 5th argument is returned when no match is found.
    """
    if len(args) < 4 or len(args) > 5:
        raise FormulaFunctionError(
            "XLOOKUP",
            "XLOOKUP requires 4-5 arguments (value, table, lookup_col, return_col [, if_not_found])",
        )
    value, table_name, lookup_col, return_col = args[0], args[1], args[2], args[3]
    if_not_found = args[4] if len(args) == 5 else None
    has_default = len(args) == 5

    df = _get_table(tc, table_name, "XLOOKUP")
    for col in (lookup_col, return_col):
        if col not in df.columns:
            raise FormulaFunctionError(
                "XLOOKUP", f"XLOOKUP: column {col!r} not found in table {table_name!r}"
            )

    matches = df.filter(pl.col(lookup_col) == value)
    if len(matches) == 0:
        if has_default:
            return if_not_found
        raise FormulaFunctionError(
            "XLOOKUP",
            f"XLOOKUP: value {value!r} not found in {table_name!r}.{lookup_col!r}",
        )

    val = matches[return_col][0]
    if isinstance(val, (int, float, str, bool, type(None))):
        return val
    return float(val)


LOOKUP_FUNCTIONS: dict[str, Any] = {
    "MATCH": _fn_match,
    "INDEX": _fn_index,
    "XLOOKUP": _fn_xlookup,
}
