"""Structured event logging for fin123.

Provides a unified event schema, filesystem NDJSON sink, and safe
emit helpers that never raise uncaught exceptions.
"""

from fin123.logging.events import (
    EventLevel,
    EventType,
    Fin123Event,
    emit,
    emit_error,
    emit_info,
    emit_warning,
    make_plugin_event,
    make_run_event,
    make_sync_event,
    redact_context,
    set_project_dir,
)
from fin123.logging.sink import EventSink

__all__ = [
    "EventLevel",
    "EventSink",
    "EventType",
    "Fin123Event",
    "emit",
    "emit_error",
    "emit_info",
    "emit_warning",
    "make_plugin_event",
    "make_run_event",
    "make_sync_event",
    "redact_context",
    "set_project_dir",
]
