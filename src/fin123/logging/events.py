"""Unified event schema and module-level emit helpers.

All timestamps use UTC ISO-8601 with ``Z`` suffix.  The ``emit()``
family of functions is safe to call from any context -- failures are
swallowed and printed to stderr.
"""

from __future__ import annotations

import re
import sys
import time
import traceback
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import urlparse, urlunparse

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class EventLevel(str, Enum):
    info = "info"
    warning = "warning"
    error = "error"


class EventType(str, Enum):
    # Plugin lifecycle
    plugin_install = "plugin_install"
    plugin_activate = "plugin_activate"
    plugin_rollback = "plugin_rollback"
    plugin_uninstall = "plugin_uninstall"
    plugin_doctor = "plugin_doctor"

    # Run lifecycle
    run_started = "run_started"
    run_completed = "run_completed"
    run_failed = "run_failed"
    run_scalar_error = "run_scalar_error"

    # Sync lifecycle
    sync_started = "sync_started"
    sync_completed = "sync_completed"
    sync_failed = "sync_failed"
    sync_sql_error = "sync_sql_error"
    sync_sql_warning = "sync_sql_warning"
    sync_connector_error = "sync_connector_error"
    sync_connector_warning = "sync_connector_warning"

    # Assertions
    assertion_pass = "assertion_pass"
    assertion_warn = "assertion_warn"
    assertion_fail = "assertion_fail"

    # Verification
    run_verify_pass = "run_verify_pass"
    run_verify_fail = "run_verify_fail"

    # Timings
    run_timing = "run_timing"

    # Lookup diagnostics
    lookup_violation = "lookup_violation"

    # Mode enforcement
    mode_block = "mode_block"

    # Batch lifecycle
    batch_started = "batch_started"
    batch_completed = "batch_completed"

    # Release lifecycle
    release_created = "release_created"
    release_set_created = "release_set_created"


# ---------------------------------------------------------------------------
# Display-level event type mapping (run_* → build_*)
# ---------------------------------------------------------------------------

_DISPLAY_EVENT_MAP: dict[str, str] = {
    "run_started": "build_started",
    "run_completed": "build_completed",
    "run_failed": "build_failed",
    "run_scalar_error": "build_scalar_error",
    "run_verify_pass": "build_verify_pass",
    "run_verify_fail": "build_verify_fail",
    "run_timing": "build_timing",
}


def display_event_type(event_type: str | EventType) -> str:
    """Map raw event_type to display-friendly name.

    Translates ``run_*`` prefixed events to ``build_*`` for UI display.
    Stored NDJSON is never mutated — this is display-level only.
    """
    raw = event_type.value if isinstance(event_type, EventType) else event_type
    return _DISPLAY_EVENT_MAP.get(raw, raw)


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------

# Plugin
PLUGIN_HASH_MISMATCH = "plugin_hash_mismatch"
PLUGIN_ENGINE_INCOMPATIBLE = "plugin_engine_incompatible"
PLUGIN_SAFETY_SCAN_FAILED = "plugin_safety_scan_failed"
PLUGIN_IMPORT_ERROR = "plugin_import_error"
PLUGIN_INSTALL_FAILED = "plugin_install_failed"

# Run
SCALAR_EVAL_ERROR = "scalar_eval_error"
TABLE_PLAN_ERROR = "table_plan_error"
LOOKUP_MISSING_KEY = "lookup_missing_key"
LOOKUP_DUPLICATE_KEY = "lookup_duplicate_key"

# Sync
CONNECTOR_FETCH_FAILED = "connector_fetch_failed"
CONNECTOR_PARSE_FAILED = "connector_parse_failed"
CONNECTOR_SCHEMA_MISSING_COLUMNS = "connector_schema_missing_columns"
CONNECTOR_SCHEMA_EXTRA_COLUMNS = "connector_schema_extra_columns"
CONNECTOR_OUTPUT_WRITE_FAILED = "connector_output_write_failed"


# ---------------------------------------------------------------------------
# Secret redaction
# ---------------------------------------------------------------------------

_SENSITIVE_KEY_RE = re.compile(
    r"(password|passwd|secret|token|api_key|apikey|authorization|cookie"
    r"|set.cookie|session|bearer|dsn|connection_string)",
    re.IGNORECASE,
)

_SAFE_HEADER_KEYS = frozenset({"user-agent", "accept", "content-type"})

_MAX_VALUE_LEN = 256


def redact_context(context: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of *context* with sensitive values redacted.

    Rules:
    - Keys matching sensitive patterns have their values replaced with
      ``"[REDACTED]"``.
    - String values that look like URLs have query params stripped.
    - String values longer than 256 chars are truncated.
    - A ``headers`` sub-dict keeps only safe header keys.
    """
    return _redact_dict(context)


def _redact_dict(d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        if _SENSITIVE_KEY_RE.search(k):
            out[k] = "[REDACTED]"
        elif k.lower() == "headers" and isinstance(v, dict):
            out[k] = {
                hk: hv for hk, hv in v.items()
                if hk.lower() in _SAFE_HEADER_KEYS
            }
        elif isinstance(v, dict):
            out[k] = _redact_dict(v)
        elif isinstance(v, list):
            out[k] = [_redact_value(item) for item in v]
        else:
            out[k] = _redact_value(v)
    return out


def _redact_value(v: Any) -> Any:
    if isinstance(v, dict):
        return _redact_dict(v)
    if isinstance(v, str):
        # URL redaction: strip query params from anything with a scheme
        if "://" in v:
            try:
                parsed = urlparse(v)
                if parsed.scheme in ("http", "https", "postgresql", "postgres",
                                     "mysql", "sqlite", "file"):
                    # Strip query, fragment, and userinfo
                    clean = urlunparse((
                        parsed.scheme,
                        parsed.hostname or "",
                        parsed.path,
                        "",  # params
                        "",  # query
                        "",  # fragment
                    ))
                    return clean + "?[REDACTED]" if parsed.query else clean
            except Exception:
                pass
        # Truncation
        if len(v) > _MAX_VALUE_LEN:
            return v[:_MAX_VALUE_LEN] + "...[truncated]"
    return v


# ---------------------------------------------------------------------------
# Attribution invariants
# ---------------------------------------------------------------------------

# Required context keys per event type category.
_PLUGIN_EVENT_REQUIRED = {"plugin_name"}
_RUN_EVENT_REQUIRED = {"run_id", "model_id"}
_SYNC_EVENT_REQUIRED = {"sync_id"}

_EVENT_REQUIRED_KEYS: dict[str, set[str]] = {
    EventType.plugin_install.value: _PLUGIN_EVENT_REQUIRED,
    EventType.plugin_activate.value: _PLUGIN_EVENT_REQUIRED,
    EventType.plugin_rollback.value: _PLUGIN_EVENT_REQUIRED,
    EventType.plugin_uninstall.value: _PLUGIN_EVENT_REQUIRED,
    EventType.plugin_doctor.value: set(),  # summary event, no single plugin
    EventType.run_started.value: {"model_id"},
    EventType.run_completed.value: _RUN_EVENT_REQUIRED,
    EventType.run_failed.value: set(),  # run_id may not be known yet
    EventType.run_scalar_error.value: set(),
    EventType.sync_started.value: set(),  # sync_id not yet assigned
    EventType.sync_completed.value: _SYNC_EVENT_REQUIRED,
    EventType.sync_failed.value: set(),
    EventType.sync_sql_error.value: {"table_name"},
    EventType.sync_sql_warning.value: {"table_name"},
    EventType.sync_connector_error.value: set(),
    EventType.sync_connector_warning.value: set(),
    EventType.assertion_pass.value: {"run_id"},
    EventType.assertion_warn.value: {"run_id"},
    EventType.assertion_fail.value: {"run_id"},
    EventType.run_verify_pass.value: {"run_id"},
    EventType.run_verify_fail.value: {"run_id"},
    EventType.run_timing.value: set(),
    EventType.lookup_violation.value: set(),
    EventType.mode_block.value: set(),
}


def _validate_attribution(event: Fin123Event) -> Fin123Event:
    """Check required context keys; downgrade to warning if missing."""
    required = _EVENT_REQUIRED_KEYS.get(event.event_type.value if isinstance(event.event_type, EventType) else event.event_type, set())
    if not required:
        return event
    missing = required - set(event.context.keys())
    if missing:
        # Mutate a copy: downgrade to warning and annotate
        ctx = dict(event.context)
        ctx["_missing_attribution"] = sorted(missing)
        return Fin123Event(
            schema_version=event.schema_version,
            ts=event.ts,
            level=EventLevel.warning,
            event_type=event.event_type,
            context=ctx,
            message=event.message,
            error_code=event.error_code,
        )
    return event


# ---------------------------------------------------------------------------
# Helper constructors for consistent attribution
# ---------------------------------------------------------------------------


def make_plugin_event(
    event_type: EventType,
    level: EventLevel,
    message: str,
    *,
    plugin_name: str,
    plugin_version: str | None = None,
    plugin_sha256: str | None = None,
    engine_version: str | None = None,
    error_code: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Fin123Event:
    """Build an event with guaranteed plugin attribution context."""
    ctx: dict[str, Any] = {"plugin_name": plugin_name}
    if plugin_version is not None:
        ctx["plugin_version"] = plugin_version
    if plugin_sha256 is not None:
        ctx["plugin_sha256"] = plugin_sha256
    if engine_version is not None:
        ctx["engine_version"] = engine_version
    if extra:
        ctx.update(extra)
    return Fin123Event(
        level=level,
        event_type=event_type,
        message=message,
        context=ctx,
        error_code=error_code,
    )


def make_run_event(
    event_type: EventType,
    level: EventLevel,
    message: str,
    *,
    run_id: str | None = None,
    model_id: str | None = None,
    model_version_id: str | None = None,
    error_code: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Fin123Event:
    """Build an event with guaranteed run attribution context."""
    ctx: dict[str, Any] = {}
    if run_id is not None:
        ctx["run_id"] = run_id
    if model_id is not None:
        ctx["model_id"] = model_id
    if model_version_id is not None:
        ctx["model_version_id"] = model_version_id
    if extra:
        ctx.update(extra)
    return Fin123Event(
        level=level,
        event_type=event_type,
        message=message,
        context=ctx,
        error_code=error_code,
    )


def make_sync_event(
    event_type: EventType,
    level: EventLevel,
    message: str,
    *,
    sync_id: str | None = None,
    error_code: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Fin123Event:
    """Build an event with guaranteed sync attribution context."""
    ctx: dict[str, Any] = {}
    if sync_id is not None:
        ctx["sync_id"] = sync_id
    if extra:
        ctx.update(extra)
    return Fin123Event(
        level=level,
        event_type=event_type,
        message=message,
        context=ctx,
        error_code=error_code,
    )


# ---------------------------------------------------------------------------
# Event model
# ---------------------------------------------------------------------------


def _utc_now() -> str:
    """Return current UTC timestamp in ISO-8601 with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Fin123Event(BaseModel):
    """A single structured log event."""

    schema_version: int = 1
    ts: str = Field(default_factory=_utc_now)
    level: EventLevel
    event_type: EventType
    context: dict[str, Any] = Field(default_factory=dict)
    message: str = ""
    error_code: str | None = None


# ---------------------------------------------------------------------------
# Module-level sink reference
# ---------------------------------------------------------------------------

# Lazily initialised when ``set_project_dir`` is called.
_sink: Any = None  # EventSink | None
_project_dir: Any = None


def set_project_dir(project_dir: Any) -> None:
    """Configure the module-level event sink for a project directory.

    This should be called early in a CLI command or server startup.  If
    it is never called, ``emit()`` silently discards events.

    Reads ``logging_fsync`` and ``logging_tail_bytes`` from the project
    config (``fin123.yaml``) to configure the sink.
    """
    global _sink, _project_dir
    from pathlib import Path

    from fin123.logging.sink import EventSink

    _project_dir = project_dir

    # Load config for logging options
    fsync = False
    tail_bytes = None
    try:
        from fin123.project import load_project_config

        cfg = load_project_config(Path(project_dir))
        fsync = bool(cfg.get("logging_fsync", False))
        tb = cfg.get("logging_tail_bytes")
        if tb is not None:
            tail_bytes = int(tb)
    except Exception:
        pass

    _sink = EventSink(project_dir, fsync=fsync, tail_bytes=tail_bytes)


def _get_sink() -> Any:
    """Return the module-level sink, or None."""
    return _sink


# ---------------------------------------------------------------------------
# Rate-limited stderr warnings
# ---------------------------------------------------------------------------

_last_stderr_ts: float = 0.0
_STDERR_INTERVAL_SECS = 60.0


def _stderr_warning(msg: str) -> None:
    """Print a warning to stderr, rate-limited to one per 60 seconds."""
    global _last_stderr_ts
    now = time.monotonic()
    if now - _last_stderr_ts < _STDERR_INTERVAL_SECS:
        return
    _last_stderr_ts = now
    try:
        print(f"[fin123] {msg}", file=sys.stderr)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Safe emit helpers
# ---------------------------------------------------------------------------


def emit(event: Fin123Event, *, run_id: str | None = None, sync_id: str | None = None) -> None:
    """Write an event to the global log and optionally to a per-run/sync log.

    **Never raises.**  On failure, prints a rate-limited warning to stderr.

    Applies secret redaction and attribution validation before writing.
    """
    try:
        sink = _get_sink()
        if sink is None:
            return
        # Redact secrets from context
        event = Fin123Event(
            schema_version=event.schema_version,
            ts=event.ts,
            level=event.level,
            event_type=event.event_type,
            context=redact_context(event.context),
            message=event.message,
            error_code=event.error_code,
        )
        # Validate attribution
        event = _validate_attribution(event)
        sink.write(event, run_id=run_id, sync_id=sync_id)
    except Exception:
        _stderr_warning(f"logging failed: {traceback.format_exc()}")


def emit_info(
    event_type: EventType,
    message: str,
    context: dict[str, Any] | None = None,
    *,
    run_id: str | None = None,
    sync_id: str | None = None,
) -> None:
    """Convenience: emit an info-level event."""
    emit(
        Fin123Event(
            level=EventLevel.info,
            event_type=event_type,
            message=message,
            context=context or {},
        ),
        run_id=run_id,
        sync_id=sync_id,
    )


def emit_warning(
    event_type: EventType,
    message: str,
    context: dict[str, Any] | None = None,
    *,
    error_code: str | None = None,
    run_id: str | None = None,
    sync_id: str | None = None,
) -> None:
    """Convenience: emit a warning-level event."""
    emit(
        Fin123Event(
            level=EventLevel.warning,
            event_type=event_type,
            message=message,
            context=context or {},
            error_code=error_code,
        ),
        run_id=run_id,
        sync_id=sync_id,
    )


def emit_error(
    event_type: EventType,
    message: str,
    context: dict[str, Any] | None = None,
    *,
    error_code: str | None = None,
    run_id: str | None = None,
    sync_id: str | None = None,
) -> None:
    """Convenience: emit an error-level event."""
    emit(
        Fin123Event(
            level=EventLevel.error,
            event_type=event_type,
            message=message,
            context=context or {},
            error_code=error_code,
        ),
        run_id=run_id,
        sync_id=sync_id,
    )
