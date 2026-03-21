"""Plugin manager: discover, validate, import, and register project plugins.

Plugins live in ``{project_dir}/plugins/*.py``.  Each plugin file must:

1. Define a ``PLUGIN_META`` dict with at least ``version`` and ``deterministic``.
2. Define a ``register()`` callable that registers functions via
   ``@register_scalar`` / ``@register_table`` and returns
   ``{"name": <str>, "version": <int|str>}``.
3. Pass the safety policy scan (no forbidden imports, no eval/exec, etc.).

This module is imported by :meth:`Workbook._load_plugins` during build.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def load_active_plugins(
    project_dir: Path,
) -> dict[str, dict[str, str]]:
    """Discover and load plugins from a project's ``plugins/`` directory.

    For each ``.py`` file found:

    1. Read source and run a lightweight safety scan.
    2. Import the module (triggers ``@register_scalar`` / ``@register_table``).
    3. Call the module's ``register()`` if present.
    4. Collect version and SHA-256 hash for run metadata.

    Args:
        project_dir: Root of the fin123 project.

    Returns:
        Dict mapping plugin names to ``{"version": ..., "sha256": ...}``.
        Empty dict if the ``plugins/`` directory does not exist.
    """
    plugins_dir = project_dir / "plugins"
    if not plugins_dir.is_dir():
        return {}

    plugin_files = sorted(plugins_dir.glob("*.py"))
    if not plugin_files:
        return {}

    from fin123.plugins.validator import validate_plugin_source
    from fin123.utils.hash import sha256_file

    results: dict[str, dict[str, str]] = {}

    for plugin_path in plugin_files:
        stem = plugin_path.stem
        if stem.startswith("_"):
            continue  # skip __init__.py, __pycache__ helpers, etc.

        log.info("Plugin discovered: %s", plugin_path.name)

        # ── 1. Read and validate source ──
        try:
            source = plugin_path.read_text(encoding="utf-8")
        except Exception as exc:
            log.warning("Plugin %s: cannot read file: %s", stem, exc)
            _emit_plugin_error(stem, "read_error", str(exc))
            continue

        validation = validate_plugin_source(source)
        if validation["errors"]:
            log.warning(
                "Plugin %s: validation failed: %s",
                stem,
                "; ".join(e["reason"] for e in validation["errors"]),
            )
            _emit_plugin_error(stem, "validation_failed", str(validation["errors"]))
            continue

        if validation["warnings"]:
            for w in validation["warnings"]:
                log.info("Plugin %s warning: %s", stem, w.get("reason", w))

        # ── 2. Import the module ──
        module_name = f"fin123._loaded_plugins.{stem}"
        try:
            spec = importlib.util.spec_from_file_location(module_name, plugin_path)
            if spec is None or spec.loader is None:
                log.warning("Plugin %s: cannot create module spec", stem)
                _emit_plugin_error(stem, "import_error", "Cannot create module spec")
                continue
            mod = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = mod
            spec.loader.exec_module(mod)
        except Exception as exc:
            log.warning("Plugin %s: import failed: %s", stem, exc)
            _emit_plugin_error(stem, "import_error", str(exc))
            # Clean up partial module registration
            sys.modules.pop(module_name, None)
            continue

        # ── 3. Call register() if present ──
        plugin_name = stem
        plugin_version = "unknown"

        register_fn = getattr(mod, "register", None)
        if callable(register_fn):
            try:
                reg_result = register_fn()
                if isinstance(reg_result, dict):
                    plugin_name = reg_result.get("name", stem)
                    plugin_version = str(reg_result.get("version", "unknown"))
            except Exception as exc:
                log.warning("Plugin %s: register() failed: %s", stem, exc)
                _emit_plugin_error(stem, "register_error", str(exc))
                sys.modules.pop(module_name, None)
                continue
        else:
            # No register() — functions may have been registered via decorators
            # at import time.  Extract version from PLUGIN_META if available.
            meta = getattr(mod, "PLUGIN_META", None)
            if isinstance(meta, dict):
                plugin_version = str(meta.get("version", "unknown"))

        # ── 4. Record result ──
        file_hash = sha256_file(plugin_path)
        results[plugin_name] = {
            "version": plugin_version,
            "sha256": file_hash,
        }

        log.info(
            "Plugin loaded: %s (version=%s, hash=%s…)",
            plugin_name,
            plugin_version,
            file_hash[:12],
        )
        _emit_plugin_activate(plugin_name, plugin_version, file_hash)

    return results


# ── Event helpers (best-effort, non-fatal) ──


def _emit_plugin_activate(
    name: str, version: str, sha256: str
) -> None:
    try:
        from fin123.logging.events import (
            EventLevel,
            EventType,
            emit,
            make_plugin_event,
        )

        emit(
            make_plugin_event(
                EventType.plugin_activate,
                EventLevel.info,
                f"Plugin activated: {name} v{version}",
                plugin_name=name,
                plugin_version=version,
                plugin_sha256=sha256,
            )
        )
    except Exception:
        pass


def _emit_plugin_error(name: str, error_code: str, detail: str) -> None:
    try:
        from fin123.logging.events import (
            EventLevel,
            EventType,
            emit,
            make_plugin_event,
        )

        emit(
            make_plugin_event(
                EventType.plugin_activate,
                EventLevel.warning,
                f"Plugin load failed: {name} — {detail}",
                plugin_name=name,
                error_code=error_code,
            )
        )
    except Exception:
        pass
