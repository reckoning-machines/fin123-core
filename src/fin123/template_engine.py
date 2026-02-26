"""Template loading, validation, substitution, and scaffolding.

Templates are bundled project directories that can be scaffolded into
new fin123 projects.  Each template contains a ``template.yaml`` file
describing the template metadata and optional placeholder parameters.
"""

from __future__ import annotations

import importlib.resources
import json
import re
import shutil
import uuid
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TEMPLATE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
_PLACEHOLDER_RE = re.compile(r"\{\{([a-z][a-z0-9_]*)\}\}")
_YAML_EXTENSIONS = {".yaml", ".yml"}
_TEXT_EXTENSIONS = {".yaml", ".yml", ".sql", ".csv", ".md", ".txt"}
_BINARY_EXTENSIONS = {".parquet", ".png", ".jpg", ".jpeg", ".gif", ".pdf"}


# ---------------------------------------------------------------------------
# Template discovery
# ---------------------------------------------------------------------------


def _bundled_templates_root() -> Path:
    """Return the filesystem path to the bundled templates package directory."""
    ref = importlib.resources.files("fin123.templates")
    # importlib.resources.files returns a Traversable; for installed packages
    # with real filesystem paths this resolves to a Path.
    root = Path(str(ref))
    if not root.is_dir():
        raise RuntimeError(f"Bundled templates directory not found: {root}")
    return root


def list_templates(template_dir: Path | None = None) -> list[dict[str, Any]]:
    """List available templates.

    Args:
        template_dir: If provided, list templates from this directory instead
            of the bundled package templates.  Each subdirectory containing
            a ``template.yaml`` is treated as a template.

    Returns:
        Sorted list of template metadata dicts (name, description,
        invariants, params).
    """
    root = template_dir or _bundled_templates_root()
    templates: list[dict[str, Any]] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        meta_path = child / "template.yaml"
        if not meta_path.exists():
            continue
        try:
            meta = _load_template_meta(meta_path)
            templates.append(meta)
        except (ValueError, yaml.YAMLError):
            continue
    return templates


def show_template(name: str, template_dir: Path | None = None) -> dict[str, Any]:
    """Load a template's metadata and file tree.

    Args:
        name: Template name.
        template_dir: Optional custom templates root.

    Returns:
        Dict with ``meta`` (template.yaml contents) and ``files`` (list of
        relative file paths in the template).

    Raises:
        FileNotFoundError: If the template does not exist.
    """
    tpl_dir = _resolve_template_dir(name, template_dir)
    meta = _load_template_meta(tpl_dir / "template.yaml")
    files = _collect_file_tree(tpl_dir)
    return {"meta": meta, "files": files}


# ---------------------------------------------------------------------------
# Scaffold
# ---------------------------------------------------------------------------


def scaffold_from_template(
    target_dir: Path,
    name: str | None = None,
    template_dir: Path | None = None,
    overrides: dict[str, str] | None = None,
) -> Path:
    """Scaffold a new project from a template.

    Args:
        target_dir: Directory to create the project in.
        name: Bundled template name (required if *template_dir* is None).
        template_dir: Path to a specific template directory.  If provided,
            *name* is ignored and the directory is used directly.
        overrides: ``--set key=value`` overrides for template params.

    Returns:
        Path to the created project directory.

    Raises:
        FileExistsError: If *target_dir* already contains ``workbook.yaml``.
        ValueError: On validation failures (unknown keys, type mismatches,
            placeholder safety violations).
    """
    overrides = overrides or {}

    # 1. Resolve template directory
    if template_dir is not None:
        tpl_dir = template_dir.resolve()
        if not (tpl_dir / "template.yaml").exists():
            raise FileNotFoundError(
                f"No template.yaml in {tpl_dir}"
            )
    elif name is not None:
        tpl_dir = _resolve_template_dir(name, None)
    else:
        raise ValueError("Either name or template_dir must be provided")

    meta = _load_template_meta(tpl_dir / "template.yaml")

    # 2. Validate overrides against declared params
    params = _resolve_params(meta, overrides)

    # 3. Copy tree to target
    target_dir = target_dir.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    if (target_dir / "workbook.yaml").exists():
        raise FileExistsError(f"workbook.yaml already exists in {target_dir}")

    shutil.copytree(tpl_dir, target_dir, dirs_exist_ok=True)

    # 4. Remove template.yaml from output
    tpl_yaml = target_dir / "template.yaml"
    if tpl_yaml.exists():
        tpl_yaml.unlink()

    # 5. Apply placeholder substitution to eligible files
    if params:
        _substitute_tree(target_dir, params)

    # 6. Inject fresh model_id into workbook.yaml
    wb_path = target_dir / "workbook.yaml"
    if not wb_path.exists():
        _abort_scaffold(target_dir, "Template is missing workbook.yaml")

    wb_text = wb_path.read_text()
    spec = yaml.safe_load(wb_text)
    if spec is None:
        _abort_scaffold(target_dir, "workbook.yaml is empty or invalid YAML")

    if not spec.get("model_id"):
        spec["model_id"] = str(uuid.uuid4())
        wb_path.write_text(yaml.dump(spec, default_flow_style=False, sort_keys=False))

    # 7. Validate YAML parse
    try:
        yaml.safe_load(wb_path.read_text())
    except yaml.YAMLError as exc:
        _abort_scaffold(
            target_dir,
            f"workbook.yaml failed YAML validation after substitution: {exc}",
        )

    # 8. Create required directories and initial snapshot
    for subdir in ("runs", "artifacts", "snapshots", "sync_runs", "cache"):
        (target_dir / subdir).mkdir(exist_ok=True)

    from fin123.versioning import SnapshotStore

    store = SnapshotStore(target_dir)
    store.save_snapshot(wb_path.read_text())

    return target_dir


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _abort_scaffold(target_dir: Path, message: str) -> None:
    """Remove a partially-scaffolded directory and raise ValueError."""
    shutil.rmtree(target_dir, ignore_errors=True)
    raise ValueError(message)


def _resolve_template_dir(name: str, custom_root: Path | None) -> Path:
    """Find the directory for a named template."""
    root = custom_root or _bundled_templates_root()
    tpl_dir = root / name
    if not tpl_dir.is_dir() or not (tpl_dir / "template.yaml").exists():
        available = [
            d.name for d in sorted(root.iterdir())
            if d.is_dir() and (d / "template.yaml").exists()
        ]
        raise FileNotFoundError(
            f"Template {name!r} not found. Available: {', '.join(available) or '(none)'}"
        )
    return tpl_dir


def _load_template_meta(meta_path: Path) -> dict[str, Any]:
    """Load and validate a template.yaml file.

    Returns:
        Validated metadata dict.

    Raises:
        ValueError: On schema violations.
    """
    raw = yaml.safe_load(meta_path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{meta_path}: template.yaml must be a YAML mapping")

    # name
    tpl_name = raw.get("name")
    if not tpl_name or not _TEMPLATE_NAME_RE.match(str(tpl_name)):
        raise ValueError(
            f"{meta_path}: 'name' is required and must match ^[a-z][a-z0-9_]{{0,39}}$"
        )
    # Check directory name matches
    if meta_path.parent.name != tpl_name:
        raise ValueError(
            f"{meta_path}: template name {tpl_name!r} does not match "
            f"directory name {meta_path.parent.name!r}"
        )

    # description
    desc = raw.get("description", "")
    if not desc or len(str(desc)) > 200:
        raise ValueError(f"{meta_path}: 'description' required, max 200 chars")

    # engine_compat
    if not raw.get("engine_compat"):
        raise ValueError(f"{meta_path}: 'engine_compat' is required")

    # invariants
    invariants = raw.get("invariants")
    if not isinstance(invariants, list) or len(invariants) == 0:
        raise ValueError(f"{meta_path}: 'invariants' must be a non-empty list")

    # params (optional)
    params = raw.get("params")
    if params is not None:
        if not isinstance(params, dict):
            raise ValueError(f"{meta_path}: 'params' must be a mapping")
        for pname, pdef in params.items():
            if not isinstance(pdef, dict):
                raise ValueError(f"{meta_path}: param {pname!r} must be a mapping")
            ptype = pdef.get("type")
            if ptype not in ("string", "number"):
                raise ValueError(
                    f"{meta_path}: param {pname!r} type must be 'string' or 'number'"
                )
            if "default" not in pdef:
                raise ValueError(f"{meta_path}: param {pname!r} must have a 'default'")

    return raw


def _resolve_params(
    meta: dict[str, Any],
    overrides: dict[str, str],
) -> dict[str, Any]:
    """Merge template defaults with user overrides, with validation.

    Returns:
        Dict mapping param names to resolved values.

    Raises:
        ValueError: On unknown keys or type mismatches.
    """
    declared = meta.get("params") or {}

    # Check for unknown override keys
    unknown = set(overrides.keys()) - set(declared.keys())
    if unknown:
        raise ValueError(
            f"Unknown template parameter(s): {', '.join(sorted(unknown))}. "
            f"Declared: {', '.join(sorted(declared.keys())) or '(none)'}"
        )

    result: dict[str, Any] = {}
    for pname, pdef in declared.items():
        ptype = pdef["type"]
        if pname in overrides:
            raw = overrides[pname]
            if ptype == "number":
                try:
                    result[pname] = _parse_number(raw)
                except ValueError:
                    raise ValueError(
                        f"Parameter {pname!r} expects a number, got {raw!r}"
                    )
            else:
                result[pname] = raw
        else:
            result[pname] = pdef["default"]
    return result


def _parse_number(s: str) -> int | float:
    """Parse a string as int or float."""
    try:
        v = int(s)
        return v
    except ValueError:
        return float(s)


def _collect_file_tree(tpl_dir: Path) -> list[str]:
    """Return sorted list of relative file paths in a template directory."""
    files: list[str] = []
    for p in sorted(tpl_dir.rglob("*")):
        if p.is_file() and p.name != "template.yaml":
            files.append(str(p.relative_to(tpl_dir)))
    return files


def _substitute_tree(target_dir: Path, params: dict[str, Any]) -> None:
    """Apply placeholder substitution to all eligible files in a directory."""
    for path in sorted(target_dir.rglob("*")):
        if not path.is_file():
            continue
        ext = path.suffix.lower()
        if ext in _BINARY_EXTENSIONS:
            continue
        if ext not in _TEXT_EXTENSIONS:
            continue

        content = path.read_text(encoding="utf-8")
        # Skip files with no placeholders
        if "{{" not in content:
            continue

        is_yaml = ext in _YAML_EXTENSIONS
        if is_yaml:
            _validate_yaml_placeholders(path, content, params)

        new_content = _substitute_content(content, params, is_yaml)
        path.write_text(new_content, encoding="utf-8")


def _validate_yaml_placeholders(
    path: Path,
    content: str,
    params: dict[str, Any],
) -> None:
    """Verify all placeholders in a YAML file are inside double-quoted scalars.

    .. note::
        v1 placeholder validation assumes single-line double-quoted YAML
        scalars.  Multi-line quoted scalars (folded ``>``, literal ``|``,
        or line-continued double-quoted strings) are **not** supported.

    Raises:
        ValueError: With file path and line number if a placeholder is
            found outside a double-quoted region.
    """
    for lineno, line in enumerate(content.splitlines(), start=1):
        for match in _PLACEHOLDER_RE.finditer(line):
            key = match.group(1)
            if key not in params:
                raise ValueError(
                    f"{path}:{lineno}: unknown placeholder '{{{{{{key}}}}}}'  "
                    f"(not in template params)"
                )
            start = match.start()
            end = match.end()
            if not _inside_double_quotes(line, start, end):
                raise ValueError(
                    f"{path}:{lineno}: placeholder '{{{{{key}}}}}' is not "
                    f"inside a double-quoted YAML scalar"
                )


def _inside_double_quotes(line: str, start: int, end: int) -> bool:
    """Check whether positions [start, end) in *line* fall inside double quotes.

    Walks the line tracking quote state, handling escaped quotes.
    """
    in_dq = False
    i = 0
    dq_start = -1
    while i < len(line):
        ch = line[i]
        if ch == "\\" and in_dq and i + 1 < len(line):
            # Skip escaped character inside quotes
            i += 2
            continue
        if ch == '"':
            if not in_dq:
                in_dq = True
                dq_start = i
            else:
                # Closing quote — check if our placeholder was inside
                if dq_start < start and i >= end:
                    return True
                in_dq = False
                dq_start = -1
        i += 1
    return False


def _substitute_content(
    content: str,
    params: dict[str, Any],
    is_yaml: bool,
) -> str:
    """Replace ``{{key}}`` placeholders with param values."""

    def _replacer(match: re.Match) -> str:
        key = match.group(1)
        if key not in params:
            raise ValueError(f"Unknown placeholder: {{{{{key}}}}}")
        value = params[key]
        text = _format_value(value)
        if is_yaml:
            text = _escape_yaml_dq(text)
        return text

    return _PLACEHOLDER_RE.sub(_replacer, content)


def _format_value(value: Any) -> str:
    """Format a parameter value as a string for substitution."""
    if isinstance(value, float):
        # Avoid scientific notation
        if value == int(value):
            return str(int(value))
        return repr(value)
    return str(value)


def _escape_yaml_dq(text: str) -> str:
    """Escape a string for inclusion inside a YAML double-quoted scalar."""
    text = text.replace("\\", "\\\\")
    text = text.replace('"', '\\"')
    text = text.replace("\n", "\\n")
    return text
