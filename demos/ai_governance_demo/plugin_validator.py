"""Minimal AI plugin validator for governance demo.

Validates that AI-generated plugins conform to policy rules:
- Required PLUGIN_META fields present with correct values
- No forbidden imports (random, datetime, network, subprocess, etc.)
- Correct register() entrypoint
- At least one deterministic transform callable
"""

from __future__ import annotations

import ast
import re
from typing import Any


class ValidationError(Exception):
    """Structured validation error with code, message, and violations list."""

    def __init__(self, code: str, message: str, violations: list[dict[str, str]]) -> None:
        self.code = code
        self.message = message
        self.violations = violations
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "violations": self.violations,
        }


_REQUIRED_META_FIELDS = {
    "version": 1,
    "author": "ai-system",
    "generated_by": "ai",
    "model": "demo-llm",
    "timestamp": "2025-01-01T00:00:00Z",
    "prompt_hash": "demo_prompt_hash_v1",
    "deterministic": True,
}

_FORBIDDEN_MODULES = frozenset({
    "random", "datetime", "time", "requests", "urllib",
    "http", "socket", "subprocess", "os.system",
})

_FORBIDDEN_PATTERN = re.compile(
    r"\b(random|datetime|time|requests|urllib|http|socket|subprocess|os\.system)\b"
)

_NETWORK_PATTERNS = re.compile(
    r"\b(urlopen|urlretrieve|HTTPConnection|HTTPSConnection|"
    r"create_connection|getaddrinfo|connect)\b"
)


def validate_plugin_source(source: str) -> list[dict[str, str]]:
    """Validate plugin source code against governance policy.

    Returns list of violations (empty means PASS).
    """
    violations: list[dict[str, str]] = []

    # Parse AST
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        violations.append({"field": "syntax", "reason": f"SyntaxError: {e}"})
        return violations

    # Check PLUGIN_META exists
    meta_found = False
    meta_value: dict[str, Any] | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "PLUGIN_META":
                    meta_found = True
                    if isinstance(node.value, ast.Dict):
                        meta_value = _extract_dict_literal(node.value)

    if not meta_found:
        violations.append({"field": "PLUGIN_META", "reason": "PLUGIN_META not found"})
    elif meta_value is not None:
        for field, expected in _REQUIRED_META_FIELDS.items():
            actual = meta_value.get(field)
            if actual is None:
                violations.append({
                    "field": f"PLUGIN_META.{field}",
                    "reason": f"required field '{field}' missing",
                })
            elif actual != expected:
                violations.append({
                    "field": f"PLUGIN_META.{field}",
                    "reason": f"expected {expected!r}, got {actual!r}",
                })

    # Check forbidden imports
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in _FORBIDDEN_MODULES or alias.name.split(".")[0] in _FORBIDDEN_MODULES:
                    violations.append({
                        "field": "import",
                        "reason": f"forbidden import: {alias.name}",
                    })
        elif isinstance(node, ast.ImportFrom):
            if node.module and (node.module in _FORBIDDEN_MODULES or node.module.split(".")[0] in _FORBIDDEN_MODULES):
                violations.append({
                    "field": "import",
                    "reason": f"forbidden import: from {node.module}",
                })

    # Check forbidden patterns in source text
    for match in _NETWORK_PATTERNS.finditer(source):
        violations.append({
            "field": "network",
            "reason": f"forbidden network pattern: {match.group()}",
        })

    # Check register() entrypoint exists
    register_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "register":
            register_found = True

    if not register_found:
        violations.append({
            "field": "register",
            "reason": "register() entrypoint not found",
        })

    # Check at least one deterministic transform callable
    transform_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name not in ("register", "__init__"):
            transform_found = True

    if not transform_found:
        violations.append({
            "field": "transform",
            "reason": "no deterministic transform callable found",
        })

    return violations


def validate_plugin_or_raise(source: str) -> None:
    """Validate plugin source and raise ValidationError on failure."""
    violations = validate_plugin_source(source)
    if violations:
        raise ValidationError(
            code="PLUGIN_VALIDATION_FAILED",
            message=f"{len(violations)} policy violation(s) found",
            violations=violations,
        )


def _extract_dict_literal(node: ast.Dict) -> dict[str, Any]:
    """Extract a simple dict literal from an AST Dict node."""
    result: dict[str, Any] = {}
    for key, value in zip(node.keys, node.values):
        if key is None:
            continue
        k = _extract_const(key)
        v = _extract_const(value)
        if k is not None:
            result[k] = v
    return result


def _extract_const(node: ast.expr) -> Any:
    """Extract a constant value from an AST node."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.NameConstant):  # Python 3.7 compat
        return node.value
    return None
