"""Production-grade plugin source validator.

Validates that plugin Python source conforms to the fin123 safety and
compatibility policy.  Unlike the demo validator in
``demos/ai_governance_demo/plugin_validator.py``, this validator:

- Checks PLUGIN_META field **presence and types**, not exact values
- Detects ``eval`` / ``exec`` / ``__import__`` usage
- Detects filesystem write patterns
- Produces structured results with both errors and warnings
- Is reusable across draft validation, apply, and load paths
"""

from __future__ import annotations

import ast
import re
from typing import Any


# ── Forbidden imports ──

_FORBIDDEN_MODULES = frozenset({
    "random",
    "requests",
    "urllib",
    "http",
    "socket",
    "subprocess",
    "shutil",
    "os",
    "sys",
    "ctypes",
    "multiprocessing",
    "threading",
    "signal",
    "tempfile",
    "webbrowser",
    "smtplib",
    "ftplib",
    "telnetlib",
    "xmlrpc",
})

# Specific os/sys submodules that are unambiguously unsafe
_FORBIDDEN_FROM_PATTERNS = frozenset({
    "os.system",
    "os.popen",
    "os.exec",
    "os.spawn",
    "subprocess",
})

# ── Forbidden call patterns (regex on source) ──

_NETWORK_PATTERNS = re.compile(
    r"\b(urlopen|urlretrieve|HTTPConnection|HTTPSConnection|"
    r"create_connection|getaddrinfo|connect)\b"
)

_FILESYSTEM_WRITE_PATTERNS = re.compile(
    r"\b(open\s*\([^)]*['\"][wa]['\"]|write_text|write_bytes|"
    r"os\.remove|os\.unlink|os\.rmdir|shutil\.rmtree|shutil\.copy|"
    r"shutil\.move|Path\s*\([^)]*\)\s*\.write_)\b"
)

# ── Forbidden AST patterns ──

_FORBIDDEN_BUILTINS = frozenset({"eval", "exec", "compile", "__import__", "breakpoint"})


# ── Required PLUGIN_META fields ──

_REQUIRED_META_FIELDS = {
    "version": (int, str, float),
    "deterministic": (bool,),
}

_OPTIONAL_META_FIELDS = {
    "author": (str,),
    "generated_by": (str,),
    "model": (str,),
    "timestamp": (str,),
    "prompt_hash": (str,),
}


def validate_plugin_source(source: str) -> dict[str, Any]:
    """Validate plugin source code against the production policy.

    Returns:
        Dict with keys:
        - ``valid``: bool — True if no errors
        - ``errors``: list of ``{"field": ..., "reason": ...}``
        - ``warnings``: list of ``{"field": ..., "reason": ...}``
        - ``detected_type``: ``"scalar_plugin"`` | ``"table_plugin"`` | ``"mixed"`` | ``"unknown"``
        - ``metadata``: extracted PLUGIN_META dict or None
        - ``registered_names``: list of function names being registered (best-effort)
    """
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    metadata: dict[str, Any] | None = None
    registered_names: list[str] = []
    detected_type = "unknown"

    # ── 1. Syntax validation ──
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        errors.append({
            "field": "syntax",
            "reason": f"SyntaxError at line {e.lineno}: {e.msg}",
        })
        return _result(errors, warnings, metadata, detected_type, registered_names)

    # ── 2. PLUGIN_META validation ──
    metadata = _extract_plugin_meta(tree)
    if metadata is None:
        warnings.append({
            "field": "PLUGIN_META",
            "reason": "PLUGIN_META dict not found (recommended for provenance tracking)",
        })
    else:
        for field, allowed_types in _REQUIRED_META_FIELDS.items():
            val = metadata.get(field)
            if val is None:
                errors.append({
                    "field": f"PLUGIN_META.{field}",
                    "reason": f"required field '{field}' missing",
                })
            elif not isinstance(val, allowed_types):
                errors.append({
                    "field": f"PLUGIN_META.{field}",
                    "reason": f"expected type {allowed_types}, got {type(val).__name__}",
                })

        for field, allowed_types in _OPTIONAL_META_FIELDS.items():
            val = metadata.get(field)
            if val is not None and not isinstance(val, allowed_types):
                warnings.append({
                    "field": f"PLUGIN_META.{field}",
                    "reason": f"expected type {allowed_types}, got {type(val).__name__}",
                })

    # ── 3. Forbidden imports ──
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in _FORBIDDEN_MODULES:
                    errors.append({
                        "field": "import",
                        "reason": f"forbidden import: {alias.name}",
                    })
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                root = node.module.split(".")[0]
                if root in _FORBIDDEN_MODULES:
                    errors.append({
                        "field": "import",
                        "reason": f"forbidden import: from {node.module}",
                    })

    # ── 4. Forbidden builtin calls ──
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _call_name(node)
            if name in _FORBIDDEN_BUILTINS:
                errors.append({
                    "field": "unsafe_call",
                    "reason": f"forbidden call: {name}()",
                })

    # ── 5. Network patterns (regex) ──
    for match in _NETWORK_PATTERNS.finditer(source):
        errors.append({
            "field": "network",
            "reason": f"forbidden network pattern: {match.group()}",
        })

    # ── 6. Filesystem write patterns (regex) ──
    for match in _FILESYSTEM_WRITE_PATTERNS.finditer(source):
        warnings.append({
            "field": "filesystem",
            "reason": f"potential filesystem write: {match.group()}",
        })

    # ── 7. register() entrypoint ──
    register_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "register":
            register_found = True

    if not register_found:
        errors.append({
            "field": "register",
            "reason": "register() entrypoint not found",
        })

    # ── 8. At least one transform callable ──
    transform_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name not in (
            "register", "__init__",
        ):
            transform_found = True

    if not transform_found:
        errors.append({
            "field": "transform",
            "reason": "no transform callable found (need at least one function besides register)",
        })

    # ── 9. Detect artifact type and registered names ──
    has_scalar = "register_scalar" in source
    has_table = "register_table" in source
    if has_scalar and has_table:
        detected_type = "mixed"
    elif has_scalar:
        detected_type = "scalar_plugin"
    elif has_table:
        detected_type = "table_plugin"

    # Best-effort extraction of registered function names from string args
    for match in re.finditer(r'register_scalar\(["\']([^"\']+)["\']\)', source):
        registered_names.append(match.group(1))
    for match in re.finditer(r'register_table\(["\']([^"\']+)["\']\)', source):
        registered_names.append(match.group(1))

    return _result(errors, warnings, metadata, detected_type, registered_names)


def _result(
    errors: list,
    warnings: list,
    metadata: dict | None,
    detected_type: str,
    registered_names: list,
) -> dict[str, Any]:
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "detected_type": detected_type,
        "metadata": metadata,
        "registered_names": registered_names,
    }


def _extract_plugin_meta(tree: ast.Module) -> dict[str, Any] | None:
    """Extract PLUGIN_META dict literal from an AST."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "PLUGIN_META":
                    if isinstance(node.value, ast.Dict):
                        return _extract_dict_literal(node.value)
    return None


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
    return None


def _call_name(node: ast.Call) -> str:
    """Extract the function name from a Call node (best-effort)."""
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return ""
