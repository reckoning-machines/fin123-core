"""Restricted row-local formula evaluator for worksheet derived columns.

Wraps the existing fin123.formulas parser and evaluator with:
- An explicit function allowlist (no table-access or lookup functions)
- No table_cache, no resolver, no cross-sheet refs, no cell refs
- Inline error objects for evaluation failures

Three-level API:
- parse_row_expression(): parse + validate (once per expression)
- evaluate_row_tree(): evaluate a pre-parsed tree (once per row)
- evaluate_row_expression(): all-in-one convenience
"""

from __future__ import annotations

from typing import Any

from lark import Token, Tree

from fin123.formulas.errors import (
    ENGINE_ERRORS,
    FormulaError,
    FormulaFunctionError,
    FormulaParseError,
    FormulaRefError,
)
from fin123.formulas.evaluator import evaluate_formula
from fin123.formulas.parser import parse_formula

# ────────────────────────────────────────────────────────────────
# Function allowlist — row-local safe only
# ────────────────────────────────────────────────────────────────

ROW_LOCAL_ALLOWLIST: frozenset[str] = frozenset({
    # Logic / control flow
    "IF",
    "IFERROR",
    "ISERROR",
    "AND",
    "OR",
    "NOT",
    # Math
    "SUM",
    "AVERAGE",
    "MIN",
    "MAX",
    "ABS",
    "ROUND",
    # Date
    "DATE",
    "YEAR",
    "MONTH",
    "DAY",
    "EOMONTH",
})

# ────────────────────────────────────────────────────────────────
# Error code mapping
# ────────────────────────────────────────────────────────────────

_ERROR_CODES: dict[type, str] = {
    ZeroDivisionError: "#DIV/0!",
    FormulaRefError: "#REF!",
    FormulaFunctionError: "#NAME?",
    FormulaParseError: "#ERR!",
}


def _error_code(exc: BaseException) -> str:
    """Map an exception to an Excel-style error code."""
    for exc_type, code in _ERROR_CODES.items():
        if isinstance(exc, exc_type):
            return code
    return "#ERR!"


# ────────────────────────────────────────────────────────────────
# Static validation
# ────────────────────────────────────────────────────────────────


def validate_row_local(
    expression: str,
    available_columns: list[str] | None = None,
) -> list[str]:
    """Statically validate a row-local expression.

    Checks:
    - Parseable
    - No cell references (A1, Sheet1!A1)
    - All function calls use allowlisted functions
    - All column references exist in available_columns (if provided)

    Returns:
        List of error messages. Empty list means valid.
    """
    errors: list[str] = []
    normalized = expression.strip()
    if not normalized.startswith("="):
        normalized = "=" + normalized

    try:
        tree = parse_formula(normalized)
    except FormulaParseError as exc:
        errors.append(f"Parse error: {exc}")
        return errors

    _walk_validate(tree, available_columns, errors)
    return errors


def _walk_validate(
    node: Tree | Token,
    available_columns: list[str] | None,
    errors: list[str],
) -> None:
    """Recursively walk a parse tree checking row-local constraints."""
    if isinstance(node, Token):
        return

    rule = node.data

    if rule == "cell_ref":
        addr = str(node.children[0])
        errors.append(f"Cell reference '{addr}' not allowed in row-local expressions")
        return

    if rule == "sheet_cell_ref":
        ref = str(node.children[0])
        errors.append(f"Sheet reference '{ref}' not allowed in row-local expressions")
        return

    if rule == "func_call":
        func_name = str(node.children[0]).upper()
        if func_name not in ROW_LOCAL_ALLOWLIST:
            errors.append(
                f"Function '{func_name}' not allowed in row-local expressions"
            )

    if rule in ("ref_bare", "ref_dollar"):
        name = str(node.children[0])
        if available_columns is not None and name not in available_columns:
            errors.append(f"Unknown column reference: '{name}'")

    for child in node.children:
        if isinstance(child, (Tree, Token)):
            _walk_validate(child, available_columns, errors)


# ────────────────────────────────────────────────────────────────
# Parse (with validation)
# ────────────────────────────────────────────────────────────────


def parse_row_expression(expression: str) -> Tree:
    """Parse and validate a row-local expression.

    Prepends '=' if not present (worksheet expressions omit it).
    Raises on parse failure or disallowed constructs.

    Args:
        expression: Formula string, e.g. "revenue - cost" or "=revenue - cost".

    Returns:
        Parsed Lark tree, ready for evaluate_row_tree().

    Raises:
        FormulaParseError: On syntax errors.
        FormulaError: On disallowed constructs (cell refs, blocked functions).
    """
    normalized = expression.strip()
    if not normalized.startswith("="):
        normalized = "=" + normalized

    tree = parse_formula(normalized)

    errors = _walk_validate_collect(tree)
    if errors:
        raise FormulaError(
            f"Row-local validation failed: {'; '.join(errors)}"
        )

    return tree


def _walk_validate_collect(node: Tree | Token) -> list[str]:
    """Walk tree and collect validation errors (no column check)."""
    errors: list[str] = []
    _walk_validate(node, None, errors)
    return errors


# ────────────────────────────────────────────────────────────────
# Evaluate
# ────────────────────────────────────────────────────────────────


def evaluate_row_tree(tree: Tree, row: dict[str, Any]) -> Any:
    """Evaluate a pre-parsed row-local expression against a row.

    Args:
        tree: Parse tree from parse_row_expression().
        row: Column name -> value mapping for the current row.

    Returns:
        Computed value (not wrapped in error dict).

    Raises:
        FormulaError subclasses, ZeroDivisionError, etc. on failure.
    """
    return evaluate_formula(tree, context=row, table_cache=None, resolver=None)


def evaluate_row_expression(expression: str, row: dict[str, Any]) -> Any:
    """Parse, validate, and evaluate a row-local expression.

    Convenience function that combines parse + validate + evaluate.
    Returns the computed value on success, or an error dict on failure.

    Args:
        expression: Formula string, e.g. "revenue - cost".
        row: Column name -> value mapping for the current row.

    Returns:
        Computed value on success, or {"error": "#CODE!"} on failure.
    """
    try:
        tree = parse_row_expression(expression)
        return evaluate_row_tree(tree, row)
    except ENGINE_ERRORS as exc:
        return {"error": _error_code(exc)}
