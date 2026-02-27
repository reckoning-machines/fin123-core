"""Logical formula functions: AND, OR, NOT."""

from __future__ import annotations

from typing import Any

from fin123.formulas.errors import FormulaFunctionError


def _fn_and(args: list, ctx: dict, tc: dict, resolver: Any) -> bool:
    """AND(val1, val2, ...) — TRUE if all arguments are truthy."""
    if len(args) < 1:
        raise FormulaFunctionError("AND", "AND requires at least 1 argument")
    return all(bool(a) for a in args)


def _fn_or(args: list, ctx: dict, tc: dict, resolver: Any) -> bool:
    """OR(val1, val2, ...) — TRUE if any argument is truthy."""
    if len(args) < 1:
        raise FormulaFunctionError("OR", "OR requires at least 1 argument")
    return any(bool(a) for a in args)


def _fn_not(args: list, ctx: dict, tc: dict, resolver: Any) -> bool:
    """NOT(val) — inverts a boolean value."""
    if len(args) != 1:
        raise FormulaFunctionError("NOT", "NOT requires exactly 1 argument")
    return not bool(args[0])


LOGICAL_FUNCTIONS: dict[str, Any] = {
    "AND": _fn_and,
    "OR": _fn_or,
    "NOT": _fn_not,
}
