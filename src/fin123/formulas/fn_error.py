"""Error-handling formula functions: ISERROR."""

from __future__ import annotations

from typing import Any

from fin123.formulas.errors import ENGINE_ERRORS, FormulaFunctionError


def _fn_iserror(raw_args: list, ctx: dict, tc: dict, resolver: Any, cs: str | None = None) -> bool:
    """ISERROR(expr) — TRUE if the expression raises an error.

    This is a lazy function: it receives unevaluated AST nodes.
    """
    if len(raw_args) != 1:
        raise FormulaFunctionError("ISERROR", "ISERROR requires exactly 1 argument")
    # Local import to avoid circular dependency
    from fin123.formulas.evaluator import _eval

    try:
        _eval(raw_args[0], ctx, tc, resolver, cs)
        return False
    except ENGINE_ERRORS:
        return True


ERROR_FUNCTIONS: dict[str, Any] = {
    "ISERROR": _fn_iserror,
}

ERROR_LAZY_FUNCTIONS: set[str] = {"ISERROR"}
