"""Assertion evaluation for deterministic workbook runs.

Assertions are declared in workbook.yaml under an ``assertions:`` key.
Each assertion has a name, an expression (using the formula engine), and
a severity (error or warn).  Assertions are evaluated after scalars are
computed and produce a structured report.
"""

from __future__ import annotations

import math
from typing import Any


def evaluate_assertions(
    assertion_specs: list[dict[str, Any]],
    scalar_values: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate all assertions against computed scalar values.

    Each assertion spec must have:
    - name: str
    - expr: str (e.g. "$revenue > 0" or "NOT(ISNAN($margin))")
    - severity: "error" | "warn"

    Args:
        assertion_specs: List of assertion dicts from workbook.yaml.
        scalar_values: Computed scalar values from the run.

    Returns:
        Report dict with keys:
        - status: "pass" | "warn" | "fail"
        - results: list of per-assertion result dicts
        - failed_count: int
        - warn_count: int
    """
    results: list[dict[str, Any]] = []
    failed_count = 0
    warn_count = 0

    for spec in assertion_specs:
        name = spec.get("name", "unnamed")
        expr = spec.get("expr", "")
        severity = spec.get("severity", "error")

        result = _evaluate_single(name, expr, severity, scalar_values)
        results.append(result)

        if not result["ok"]:
            if severity == "error":
                failed_count += 1
            else:
                warn_count += 1

    if failed_count > 0:
        status = "fail"
    elif warn_count > 0:
        status = "warn"
    else:
        status = "pass"

    return {
        "status": status,
        "results": results,
        "failed_count": failed_count,
        "warn_count": warn_count,
    }


def _evaluate_single(
    name: str,
    expr: str,
    severity: str,
    scalars: dict[str, Any],
) -> dict[str, Any]:
    """Evaluate a single assertion expression.

    Supports simple expressions:
    - $var > N, $var < N, $var >= N, $var <= N, $var == N
    - NOT(ISNAN($var))
    - ISNAN($var)

    Falls back to the formula engine for complex expressions.

    Args:
        name: Assertion name.
        expr: Expression string.
        severity: "error" or "warn".
        scalars: Scalar values dict.

    Returns:
        Result dict with name, ok, severity, message.
    """
    try:
        resolved = _resolve_vars(expr, scalars)
        ok = _eval_expr(resolved, scalars)
        message = "" if ok else f"Assertion failed: {expr}"
        return {"name": name, "ok": ok, "severity": severity, "message": message}
    except Exception as exc:
        return {
            "name": name,
            "ok": False,
            "severity": severity,
            "message": f"Evaluation error: {exc}",
        }


def _resolve_vars(expr: str, scalars: dict[str, Any]) -> str:
    """Replace $var references with their scalar values."""
    import re

    def _replace(m: re.Match) -> str:
        var_name = m.group(1)
        val = scalars.get(var_name)
        if val is None:
            return "None"
        return repr(val)

    return re.sub(r"\$([a-zA-Z_][a-zA-Z0-9_]*)", _replace, expr)


def _eval_expr(resolved: str, scalars: dict[str, Any]) -> bool:
    """Evaluate a resolved expression to a boolean.

    Handles common patterns safely without using eval().

    Args:
        resolved: Expression with variables replaced by values.
        scalars: Original scalar values (for ISNAN lookups).

    Returns:
        Boolean result.
    """
    expr = resolved.strip()

    # NOT(ISNAN(...))
    if expr.upper().startswith("NOT(ISNAN(") and expr.endswith("))"):
        inner = expr[10:-2]
        val = _parse_value(inner)
        return not (isinstance(val, float) and math.isnan(val))

    # ISNAN(...)
    if expr.upper().startswith("ISNAN(") and expr.endswith(")"):
        inner = expr[6:-1]
        val = _parse_value(inner)
        return isinstance(val, float) and math.isnan(val)

    # Comparison operators: val op val
    import re
    m = re.match(r"^(.+?)\s*(>=|<=|!=|==|>|<)\s*(.+)$", expr)
    if m:
        left = _parse_value(m.group(1).strip())
        op = m.group(2)
        right = _parse_value(m.group(3).strip())
        return _compare(left, op, right)

    # Try as a bare truthy value
    val = _parse_value(expr)
    return bool(val)


def _parse_value(s: str) -> Any:
    """Parse a string value into a Python type."""
    s = s.strip()
    if s == "None":
        return None
    if s == "True":
        return True
    if s == "False":
        return False
    # Try numeric
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    # Strip quotes
    if len(s) >= 2 and s[0] in ("'", '"') and s[-1] == s[0]:
        return s[1:-1]
    return s


def _compare(left: Any, op: str, right: Any) -> bool:
    """Perform a comparison between two values."""
    if op == ">":
        return left > right
    if op == "<":
        return left < right
    if op == ">=":
        return left >= right
    if op == "<=":
        return left <= right
    if op == "==":
        return left == right
    if op == "!=":
        return left != right
    return False
