"""Tree-walking evaluator for parsed formula expressions.

Supports:
- Scalar context (existing)
- Cross-sheet cell references via a resolver callback
- Named range references that expand to value lists in aggregate functions
"""

from __future__ import annotations

from typing import Any, Protocol

import polars as pl
from lark import Tree, Token

from fin123.formulas.errors import (
    FormulaError,
    FormulaFunctionError,
    FormulaRefError,
)
from fin123.formulas.parser import parse_sheet_ref
from fin123.functions.scalar import scalar_lookup


# ---------------------------------------------------------------------------
# Resolver protocol — optional callback for cross-sheet + named range support
# ---------------------------------------------------------------------------


class CellResolver(Protocol):
    """Protocol for resolving cross-sheet cell refs and named ranges."""

    def resolve_cell(self, sheet: str, addr: str) -> Any:
        """Resolve a cell value (may trigger recursive evaluation)."""
        ...

    def resolve_range(self, name: str) -> list[Any]:
        """Resolve a named range to a flat list of values (row-major)."""
        ...

    def has_named_range(self, name: str) -> bool:
        """Check if a name is a defined named range."""
        ...


def evaluate_formula(
    tree: Tree,
    context: dict[str, Any],
    table_cache: dict[str, pl.DataFrame] | None = None,
    resolver: CellResolver | None = None,
    current_sheet: str | None = None,
) -> Any:
    """Evaluate a parsed formula tree against a scalar context.

    Args:
        tree: Parse tree from ``parse_formula()``.
        context: Mapping of scalar names to their resolved values.
        table_cache: Materialized table DataFrames for VLOOKUP/SUMIFS/COUNTIFS.
        resolver: Optional resolver for cross-sheet cell refs and named ranges.
        current_sheet: Sheet name for resolving bare A1 cell references.

    Returns:
        The computed scalar value.
    """
    return _eval(tree, context, table_cache or {}, resolver, current_sheet)


def _eval(
    node: Tree | Token,
    ctx: dict[str, Any],
    tc: dict[str, pl.DataFrame],
    resolver: CellResolver | None,
    cs: str | None = None,
) -> Any:
    """Recursively evaluate a tree node.

    Args:
        cs: Current sheet name for resolving bare A1 cell references.
    """
    if isinstance(node, Token):
        return _eval_token(node)

    rule = node.data

    # Start rule just wraps expr
    if rule == "start":
        return _eval(node.children[0], ctx, tc, resolver, cs)

    # Arithmetic
    if rule == "add":
        return _eval(node.children[0], ctx, tc, resolver, cs) + _eval(node.children[1], ctx, tc, resolver, cs)
    if rule == "sub":
        return _eval(node.children[0], ctx, tc, resolver, cs) - _eval(node.children[1], ctx, tc, resolver, cs)
    if rule == "mul":
        return _eval(node.children[0], ctx, tc, resolver, cs) * _eval(node.children[1], ctx, tc, resolver, cs)
    if rule == "div":
        left = _eval(node.children[0], ctx, tc, resolver, cs)
        right = _eval(node.children[1], ctx, tc, resolver, cs)
        if right == 0:
            raise ZeroDivisionError("Division by zero in formula")
        return left / right
    if rule == "neg":
        return -_eval(node.children[0], ctx, tc, resolver, cs)
    if rule == "pos":
        return _eval(node.children[0], ctx, tc, resolver, cs)
    if rule == "pow":
        base = _eval(node.children[0], ctx, tc, resolver, cs)
        exp = _eval(node.children[1], ctx, tc, resolver, cs)
        return base ** exp
    if rule == "percent":
        return _eval(node.children[0], ctx, tc, resolver, cs) / 100

    # Comparison
    if rule == "gt":
        return _eval(node.children[0], ctx, tc, resolver, cs) > _eval(node.children[1], ctx, tc, resolver, cs)
    if rule == "lt":
        return _eval(node.children[0], ctx, tc, resolver, cs) < _eval(node.children[1], ctx, tc, resolver, cs)
    if rule == "gte":
        return _eval(node.children[0], ctx, tc, resolver, cs) >= _eval(node.children[1], ctx, tc, resolver, cs)
    if rule == "lte":
        return _eval(node.children[0], ctx, tc, resolver, cs) <= _eval(node.children[1], ctx, tc, resolver, cs)
    if rule == "eq":
        return _eval(node.children[0], ctx, tc, resolver, cs) == _eval(node.children[1], ctx, tc, resolver, cs)
    if rule == "neq":
        return _eval(node.children[0], ctx, tc, resolver, cs) != _eval(node.children[1], ctx, tc, resolver, cs)

    # Literals
    if rule == "number":
        return _parse_number(node.children[0])
    if rule == "boolean":
        return str(node.children[0]) == "TRUE"
    if rule == "string":
        raw = str(node.children[0])
        return raw[1:-1].replace('\\"', '"').replace("\\\\", "\\")

    # Bare in-sheet cell reference (e.g. F2, AA10)
    if rule == "cell_ref":
        token = node.children[0]
        cell_addr = str(token).upper()
        if resolver is None or cs is None:
            raise FormulaRefError(
                cell_addr,
                available=sorted(ctx.keys()),
            )
        return resolver.resolve_cell(cs, cell_addr)

    # Cross-sheet cell reference
    if rule == "sheet_cell_ref":
        token = node.children[0]
        sheet_name, cell_addr = parse_sheet_ref(str(token))
        if resolver is None:
            raise FormulaRefError(
                f"{sheet_name}!{cell_addr}",
                available=sorted(ctx.keys()),
            )
        return resolver.resolve_cell(sheet_name, cell_addr)

    # References (scalar or named range)
    if rule in ("ref_bare", "ref_dollar"):
        name = str(node.children[0])
        # Priority: scalar context > named range (error if range used as scalar)
        if name in ctx:
            return ctx[name]
        if resolver is not None and resolver.has_named_range(name):
            raise FormulaRefError(
                name,
                available=sorted(ctx.keys()),
            )
        raise FormulaRefError(name, available=sorted(ctx.keys()))

    # Function call
    if rule == "func_call":
        return _eval_func(node, ctx, tc, resolver, cs)

    # args — should not be evaluated directly
    if rule == "args":
        return [_eval(child, ctx, tc, resolver, cs) for child in node.children]

    raise FormulaError(f"Unknown node type: {rule}")


def _eval_token(token: Token) -> Any:
    """Evaluate a bare token (shouldn't normally happen at top level)."""
    if token.type == "NUMBER":
        return _parse_number(token)
    if token.type == "BOOL":
        return str(token) == "TRUE"
    if token.type == "ESCAPED_STRING":
        raw = str(token)
        return raw[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    return str(token)


def _parse_number(token: Token) -> int | float:
    """Parse a NUMBER token to int or float."""
    s = str(token)
    if "." in s:
        return float(s)
    return int(s)


# ---------- Function dispatch ----------

_LAZY_FUNCTIONS = {"IF", "IFERROR"}
_AGGREGATE_FUNCTIONS = {"SUM", "AVERAGE", "MIN", "MAX"}


def _resolve_func_arg(
    arg_node: Tree | Token,
    ctx: dict[str, Any],
    tc: dict[str, pl.DataFrame],
    resolver: CellResolver | None,
    allow_range: bool = False,
    cs: str | None = None,
) -> Any:
    """Evaluate a function argument, optionally allowing named range expansion.

    For aggregate functions, bare ref_bare nodes that match named ranges
    are expanded to their value lists.
    """
    if allow_range and isinstance(arg_node, Tree) and arg_node.data in ("ref_bare", "ref_dollar"):
        name = str(arg_node.children[0])
        # Check if it's a named range
        if name not in ctx and resolver is not None and resolver.has_named_range(name):
            return resolver.resolve_range(name)
    return _eval(arg_node, ctx, tc, resolver, cs)


def _flatten_args(args: list) -> list:
    """Flatten one level of lists in argument list."""
    result = []
    for a in args:
        if isinstance(a, list):
            result.extend(a)
        else:
            result.append(a)
    return result


def _eval_func(
    node: Tree,
    ctx: dict[str, Any],
    tc: dict[str, pl.DataFrame],
    resolver: CellResolver | None,
    cs: str | None = None,
) -> Any:
    """Evaluate a function call node."""
    func_name = str(node.children[0]).upper()
    args_node = node.children[1]
    raw_args = args_node.children if args_node.children else []

    # Lazy functions receive unevaluated AST nodes
    if func_name in _LAZY_FUNCTIONS:
        return _FUNC_TABLE[func_name](raw_args, ctx, tc, resolver, cs)

    # Aggregate functions allow named range expansion
    if func_name in _AGGREGATE_FUNCTIONS:
        evaluated_args = [
            _resolve_func_arg(arg, ctx, tc, resolver, allow_range=True, cs=cs)
            for arg in raw_args
        ]
        # Flatten one level (named ranges expand to lists)
        evaluated_args = _flatten_args(evaluated_args)
        if func_name not in _FUNC_TABLE:
            raise FormulaFunctionError(func_name)
        return _FUNC_TABLE[func_name](evaluated_args, ctx, tc, resolver)

    # Eager functions receive pre-evaluated values
    evaluated_args = [_eval(arg, ctx, tc, resolver, cs) for arg in raw_args]

    if func_name not in _FUNC_TABLE:
        raise FormulaFunctionError(func_name)

    return _FUNC_TABLE[func_name](evaluated_args, ctx, tc, resolver)


def _fn_sum(args: list, ctx: dict, tc: dict, resolver) -> float:
    if len(args) < 1:
        raise FormulaFunctionError("SUM", "SUM requires at least 1 argument")
    return sum(args)


def _fn_average(args: list, ctx: dict, tc: dict, resolver) -> float:
    if len(args) < 1:
        raise FormulaFunctionError("AVERAGE", "AVERAGE requires at least 1 argument")
    return sum(args) / len(args)


def _fn_min(args: list, ctx: dict, tc: dict, resolver) -> Any:
    if len(args) < 1:
        raise FormulaFunctionError("MIN", "MIN requires at least 1 argument")
    return min(args)


def _fn_max(args: list, ctx: dict, tc: dict, resolver) -> Any:
    if len(args) < 1:
        raise FormulaFunctionError("MAX", "MAX requires at least 1 argument")
    return max(args)


def _fn_abs(args: list, ctx: dict, tc: dict, resolver) -> float:
    if len(args) != 1:
        raise FormulaFunctionError("ABS", "ABS requires exactly 1 argument")
    return abs(args[0])


def _fn_round(args: list, ctx: dict, tc: dict, resolver) -> float:
    if len(args) < 1 or len(args) > 2:
        raise FormulaFunctionError("ROUND", "ROUND requires 1-2 arguments")
    digits = int(args[1]) if len(args) == 2 else 0
    return round(args[0], digits)


def _fn_if(raw_args: list, ctx: dict, tc: dict, resolver, cs=None) -> Any:
    """IF(condition, then_value [, else_value]) — lazy evaluation."""
    if len(raw_args) < 2 or len(raw_args) > 3:
        raise FormulaFunctionError("IF", "IF requires 2-3 arguments")
    condition = _eval(raw_args[0], ctx, tc, resolver, cs)
    if condition:
        return _eval(raw_args[1], ctx, tc, resolver, cs)
    if len(raw_args) == 3:
        return _eval(raw_args[2], ctx, tc, resolver, cs)
    return False


def _fn_iferror(raw_args: list, ctx: dict, tc: dict, resolver, cs=None) -> Any:
    """IFERROR(value, fallback) — catches errors in first arg."""
    if len(raw_args) != 2:
        raise FormulaFunctionError("IFERROR", "IFERROR requires exactly 2 arguments")
    try:
        return _eval(raw_args[0], ctx, tc, resolver, cs)
    except (FormulaError, ZeroDivisionError, ValueError, KeyError):
        return _eval(raw_args[1], ctx, tc, resolver, cs)


def _fn_vlookup(args: list, ctx: dict, tc: dict, resolver) -> Any:
    """VLOOKUP(key, "table", "key_col", "value_col") or VLOOKUP(key, "table", "value_col")."""
    if len(args) < 3 or len(args) > 4:
        raise FormulaFunctionError("VLOOKUP", "VLOOKUP requires 3-4 arguments")

    key_value = args[0]
    table_name = args[1]

    if not tc:
        raise FormulaFunctionError(
            "VLOOKUP", f"VLOOKUP: table {table_name!r} not found (no table cache)"
        )
    if table_name not in tc:
        raise FormulaFunctionError(
            "VLOOKUP", f"VLOOKUP: table {table_name!r} not found in table cache"
        )

    if len(args) == 3:
        df = tc[table_name]
        key_col = df.columns[0]
        value_col = args[2]
    else:
        key_col = args[2]
        value_col = args[3]

    return scalar_lookup(
        table_name=table_name,
        key_col=key_col,
        value_col=value_col,
        key_value=key_value,
        _table_cache=tc,
    )


def _fn_sumifs(args: list, ctx: dict, tc: dict, resolver) -> float:
    """SUMIFS("table", "sum_col", "crit_col", "op", crit_val, ...)."""
    if len(args) < 5 or (len(args) - 2) % 3 != 0:
        raise FormulaFunctionError(
            "SUMIFS",
            "SUMIFS requires (table, sum_col, crit_col, op, val, ...) — "
            f"got {len(args)} arguments",
        )

    table_name = args[0]
    sum_col = args[1]

    if table_name not in tc:
        raise FormulaFunctionError(
            "SUMIFS", f"SUMIFS: table {table_name!r} not found in table cache"
        )

    df = tc[table_name]
    for i in range(2, len(args), 3):
        crit_col = args[i]
        op = args[i + 1]
        crit_val = args[i + 2]
        df = _apply_filter(df, crit_col, op, crit_val)

    return float(df[sum_col].sum())


def _fn_countifs(args: list, ctx: dict, tc: dict, resolver) -> int:
    """COUNTIFS("table", "crit_col", "op", crit_val, ...)."""
    if len(args) < 4 or (len(args) - 1) % 3 != 0:
        raise FormulaFunctionError(
            "COUNTIFS",
            "COUNTIFS requires (table, crit_col, op, val, ...) — "
            f"got {len(args)} arguments",
        )

    table_name = args[0]

    if table_name not in tc:
        raise FormulaFunctionError(
            "COUNTIFS", f"COUNTIFS: table {table_name!r} not found in table cache"
        )

    df = tc[table_name]
    for i in range(1, len(args), 3):
        crit_col = args[i]
        op = args[i + 1]
        crit_val = args[i + 2]
        df = _apply_filter(df, crit_col, op, crit_val)

    return len(df)


def _apply_filter(
    df: pl.DataFrame, col: str, op: str, val: Any
) -> pl.DataFrame:
    """Apply a single filter criterion to a DataFrame."""
    ops = {
        "=": lambda c, v: pl.col(c) == v,
        "<>": lambda c, v: pl.col(c) != v,
        ">": lambda c, v: pl.col(c) > v,
        "<": lambda c, v: pl.col(c) < v,
        ">=": lambda c, v: pl.col(c) >= v,
        "<=": lambda c, v: pl.col(c) <= v,
    }
    if op not in ops:
        raise FormulaFunctionError("filter", f"Unknown comparison operator: {op!r}")
    return df.filter(ops[op](col, val))


def _fn_param(args: list, ctx: dict, tc: dict, resolver) -> Any:
    """PARAM("name") — resolve a parameter from the evaluation context."""
    if len(args) != 1 or not isinstance(args[0], str):
        raise FormulaFunctionError("PARAM", "PARAM requires exactly 1 string argument")
    name = args[0]
    if name not in ctx:
        raise FormulaFunctionError(
            "PARAM",
            f"PARAM: parameter {name!r} not found. "
            f"Available: {sorted(ctx.keys())}",
        )
    return ctx[name]


_FUNC_TABLE: dict[str, Any] = {
    "SUM": _fn_sum,
    "AVERAGE": _fn_average,
    "MIN": _fn_min,
    "MAX": _fn_max,
    "ABS": _fn_abs,
    "ROUND": _fn_round,
    "IF": _fn_if,
    "IFERROR": _fn_iferror,
    "VLOOKUP": _fn_vlookup,
    "SUMIFS": _fn_sumifs,
    "COUNTIFS": _fn_countifs,
    "PARAM": _fn_param,
}
