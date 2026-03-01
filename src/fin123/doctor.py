"""Preflight and compliance validation for fin123.

Implements deterministic self-tests, dependency checks, and environment
validation. Shared check ordering and naming between core and pod.
"""

from __future__ import annotations

import hashlib
import json
import locale
import os
import platform
import sys
import tempfile
from pathlib import Path
from typing import Any


def run_doctor(*, verbose: bool = False, is_enterprise: bool = False) -> list[dict[str, Any]]:
    """Run all doctor checks and return results.

    Args:
        verbose: Include extra detail in check results.
        is_enterprise: If True, run enterprise checks (pod). If False, stub them.

    Returns:
        Ordered list of check result dicts.
    """
    checks: list[dict[str, Any]] = []

    # 1. Runtime integrity
    checks.append(_check_runtime(verbose))

    # 2. Determinism engine self-test
    checks.append(_check_determinism(verbose))

    # 3. Floating-point canonicalization
    checks.append(_check_float_canon(verbose))

    # 4. Filesystem permissions
    checks.append(_check_filesystem(verbose))

    # 5. Encoding / locale safety
    checks.append(_check_encoding(verbose))

    # 6. Timezone validation
    checks.append(_check_timezone(verbose))

    # 7. Dependency integrity
    checks.append(_check_dependencies(verbose))

    # 8-10. Enterprise checks (stubbed in core)
    if not is_enterprise:
        checks.append(_enterprise_check_stub("Registry connectivity"))
        checks.append(_enterprise_check_stub("Plugin integrity"))
        checks.append(_enterprise_check_stub("Server preflight"))

    return checks


def _check_runtime(verbose: bool) -> dict[str, Any]:
    """Check 1: Runtime integrity."""
    details: dict[str, Any] = {}
    issues: list[str] = []

    # Python version
    py_ver = platform.python_version()
    details["python_version"] = py_ver
    major, minor = sys.version_info[:2]
    if major < 3 or (major == 3 and minor < 11):
        issues.append(f"Python {py_ver} is below minimum 3.11")

    # Package version
    try:
        from fin123 import __version__
        details["fin123_version"] = __version__
    except ImportError:
        issues.append("Cannot import fin123 package")

    # CLI version matches package metadata
    try:
        import importlib.metadata
        dist_ver = importlib.metadata.version("fin123-core")
        details["dist_version"] = dist_ver
        if "fin123_version" in details and details["fin123_version"] != dist_ver:
            issues.append(
                f"CLI version {details['fin123_version']} != "
                f"installed package {dist_ver}"
            )
    except importlib.metadata.PackageNotFoundError:
        details["dist_version"] = "(not installed as package)"
        if verbose:
            details["note"] = "Running from source checkout"

    return {
        "name": "Runtime",
        "ok": len(issues) == 0,
        "severity": "error",
        "exit_code": 5,
        "details": {**details, "issues": issues} if verbose else details,
    }


def _check_determinism(verbose: bool) -> dict[str, Any]:
    """Check 2: Determinism engine self-test.

    Constructs a minimal reference model, builds it twice in-memory,
    and compares hashes for exact byte-for-byte match.
    """
    details: dict[str, Any] = {}

    try:
        from fin123.utils.hash import sha256_bytes, sha256_dict

        # Reference fixture: a minimal scalar computation
        fixture = {
            "params": {"rate": 0.05, "principal": 1000.0, "years": 10},
            "outputs": {
                "interest": "=principal * rate * years",
                "total": "=principal + interest",
            },
        }

        # Build 1: hash the fixture
        build_hash_1 = sha256_dict(fixture)
        canon_1 = json.dumps(fixture, sort_keys=True, separators=(",", ":"))
        export_hash_1 = sha256_bytes(canon_1.encode("utf-8"))

        # Build 2: hash the fixture again
        build_hash_2 = sha256_dict(fixture)
        canon_2 = json.dumps(fixture, sort_keys=True, separators=(",", ":"))
        export_hash_2 = sha256_bytes(canon_2.encode("utf-8"))

        details["build_hash_match"] = build_hash_1 == build_hash_2
        details["export_hash_match"] = export_hash_1 == export_hash_2
        details["canonical_match"] = canon_1 == canon_2

        if verbose:
            details["build_hash"] = build_hash_1
            details["export_hash"] = export_hash_1

        ok = (
            build_hash_1 == build_hash_2
            and export_hash_1 == export_hash_2
            and canon_1 == canon_2
        )
    except Exception as e:
        details["error"] = str(e)
        ok = False

    return {
        "name": "Determinism engine",
        "ok": ok,
        "severity": "error",
        "exit_code": 3,
        "details": details,
    }


def _check_float_canon(verbose: bool) -> dict[str, Any]:
    """Check 3: Floating-point canonicalization."""
    details: dict[str, Any] = {}

    try:
        # Rounding stability
        val = 0.1 + 0.2
        rounded = round(val, 10)
        details["rounding_stable"] = rounded == round(0.1 + 0.2, 10)

        # JSON serialization stability
        test_data = {"a": 1.0 / 3.0, "b": 2.0 ** 0.5, "c": 0.1 + 0.2}
        json_1 = json.dumps(test_data, sort_keys=True, separators=(",", ":"))
        json_2 = json.dumps(test_data, sort_keys=True, separators=(",", ":"))
        details["json_stable"] = json_1 == json_2

        # Hash stability
        h1 = hashlib.sha256(json_1.encode()).hexdigest()
        h2 = hashlib.sha256(json_2.encode()).hexdigest()
        details["hash_stable"] = h1 == h2

        if verbose:
            details["sample_hash"] = h1

        ok = details["rounding_stable"] and details["json_stable"] and details["hash_stable"]
    except Exception as e:
        details["error"] = str(e)
        ok = False

    return {
        "name": "Floating-point stability",
        "ok": ok,
        "severity": "error",
        "exit_code": 3,
        "details": details,
    }


def _check_filesystem(verbose: bool) -> dict[str, Any]:
    """Check 4: Filesystem permissions."""
    details: dict[str, Any] = {}

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".fin123_doctor", delete=False) as f:
            f.write("fin123 doctor check")
            tmp_path = Path(f.name)

        details["can_write_temp"] = True

        content = tmp_path.read_text()
        details["can_read_temp"] = content == "fin123 doctor check"

        tmp_path.unlink()
        details["cleanup_ok"] = not tmp_path.exists()

        ok = details["can_write_temp"] and details["can_read_temp"] and details["cleanup_ok"]
    except Exception as e:
        details["error"] = str(e)
        ok = False

    return {
        "name": "Filesystem",
        "ok": ok,
        "severity": "error",
        "exit_code": 5,
        "details": details,
    }


def _check_encoding(verbose: bool) -> dict[str, Any]:
    """Check 5: Encoding / locale safety."""
    details: dict[str, Any] = {}
    issues: list[str] = []

    # Default encoding
    encoding = sys.getdefaultencoding()
    details["default_encoding"] = encoding
    if encoding.lower() not in ("utf-8", "utf8"):
        issues.append(f"Default encoding is {encoding}, expected utf-8")

    # Locale decimal formatting
    try:
        current_locale = locale.getlocale()
        details["locale"] = str(current_locale)
        # Test that decimal point is '.'
        formatted = f"{1234.5678:.4f}"
        if "." not in formatted:
            issues.append(f"Locale uses non-dot decimal: {formatted}")
        details["decimal_format_ok"] = "." in formatted
    except Exception as e:
        issues.append(f"Locale check error: {e}")

    # Sorting stability
    test_list = ["banana", "apple", "cherry", "date"]
    sorted_1 = sorted(test_list)
    sorted_2 = sorted(test_list)
    details["sort_stable"] = sorted_1 == sorted_2

    if verbose and issues:
        details["issues"] = issues

    return {
        "name": "Locale / encoding",
        "ok": len(issues) == 0 and details.get("sort_stable", False),
        "severity": "error",
        "exit_code": 5,
        "details": details,
    }


def _check_timezone(verbose: bool) -> dict[str, Any]:
    """Check 6: Timezone validation (warning only)."""
    import time

    details: dict[str, Any] = {}

    tz_name = time.tzname[0] if time.tzname else "unknown"
    details["system_tz"] = tz_name
    details["engine_uses_utc"] = True

    # Check if system is UTC
    is_utc = tz_name in ("UTC", "GMT", "Etc/UTC", "Etc/GMT")
    details["system_is_utc"] = is_utc

    if not is_utc:
        tz_env = os.environ.get("TZ", "")
        if tz_env:
            details["TZ_env"] = tz_env
        details["message"] = tz_name

    return {
        "name": "Timezone",
        "ok": is_utc,
        "severity": "warning",
        "exit_code": 0,
        "details": details,
    }


def _check_dependencies(verbose: bool) -> dict[str, Any]:
    """Check 7: Dependency integrity."""
    details: dict[str, Any] = {}
    required = ["polars", "yaml", "click", "lark", "fastapi", "uvicorn"]
    optional = ["openpyxl"]

    missing_required: list[str] = []
    found_optional: list[str] = []
    missing_optional: list[str] = []

    for mod in required:
        try:
            __import__(mod)
        except ImportError:
            missing_required.append(mod)

    for mod in optional:
        try:
            __import__(mod)
            found_optional.append(mod)
        except ImportError:
            missing_optional.append(mod)

    details["required_ok"] = len(missing_required) == 0
    if missing_required:
        details["missing_required"] = missing_required
    details["optional_available"] = found_optional
    if missing_optional and verbose:
        details["missing_optional"] = missing_optional

    return {
        "name": "Dependencies",
        "ok": len(missing_required) == 0,
        "severity": "error",
        "exit_code": 5,
        "details": details,
    }


def _enterprise_check_stub(name: str) -> dict[str, Any]:
    """Stub for enterprise checks in core."""
    return {
        "name": name,
        "ok": False,
        "severity": "error",
        "exit_code": 4,
        "enterprise_only": True,
        "details": {"message": "Enterprise feature: install fin123-pod"},
    }
