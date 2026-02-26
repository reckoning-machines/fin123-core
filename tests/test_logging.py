"""Tests for the fin123 structured event logging system."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Create a minimal project directory."""
    (tmp_path / "logs").mkdir()
    return tmp_path


@pytest.fixture
def sink(project_dir: Path):
    from fin123.logging.sink import EventSink

    return EventSink(project_dir)


# ---------------------------------------------------------------------------
# A) Event schema
# ---------------------------------------------------------------------------


class TestFin123Event:
    def test_event_defaults(self):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.info,
            event_type=EventType.run_started,
            message="hello",
        )
        assert evt.schema_version == 1
        assert evt.ts.endswith("Z")
        assert evt.level == "info"
        assert evt.event_type == "run_started"
        assert evt.context == {}
        assert evt.error_code is None

    def test_event_with_error_code(self):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.error,
            event_type=EventType.run_failed,
            message="failed",
            error_code="scalar_eval_error",
            context={"run_id": "abc"},
        )
        assert evt.error_code == "scalar_eval_error"
        assert evt.context["run_id"] == "abc"

    def test_event_serialization(self):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.warning,
            event_type=EventType.sync_connector_warning,
            message="extra cols",
        )
        d = evt.model_dump()
        assert isinstance(d, dict)
        assert d["level"] == "warning"
        assert d["event_type"] == "sync_connector_warning"

    def test_all_event_types_exist(self):
        from fin123.logging.events import EventType

        expected = {
            "plugin_install", "plugin_activate", "plugin_rollback",
            "plugin_uninstall", "plugin_doctor",
            "run_started", "run_completed", "run_failed", "run_scalar_error",
            "sync_started", "sync_completed", "sync_failed",
            "sync_sql_error", "sync_sql_warning",
            "sync_connector_error", "sync_connector_warning",
            "assertion_pass", "assertion_warn", "assertion_fail",
            "run_verify_pass", "run_verify_fail",
            "run_timing", "lookup_violation", "mode_block",
            "batch_started", "batch_completed",
            "release_created", "release_set_created",
        }
        actual = {e.value for e in EventType}
        assert expected == actual

    def test_error_codes_are_strings(self):
        from fin123.logging import events

        codes = [
            events.PLUGIN_HASH_MISMATCH,
            events.PLUGIN_ENGINE_INCOMPATIBLE,
            events.PLUGIN_SAFETY_SCAN_FAILED,
            events.PLUGIN_IMPORT_ERROR,
            events.PLUGIN_INSTALL_FAILED,
            events.SCALAR_EVAL_ERROR,
            events.TABLE_PLAN_ERROR,
            events.LOOKUP_MISSING_KEY,
            events.LOOKUP_DUPLICATE_KEY,
            events.CONNECTOR_FETCH_FAILED,
            events.CONNECTOR_PARSE_FAILED,
            events.CONNECTOR_SCHEMA_MISSING_COLUMNS,
            events.CONNECTOR_SCHEMA_EXTRA_COLUMNS,
            events.CONNECTOR_OUTPUT_WRITE_FAILED,
        ]
        for code in codes:
            assert isinstance(code, str)
            assert len(code) > 0


# ---------------------------------------------------------------------------
# B) Filesystem NDJSON sink
# ---------------------------------------------------------------------------


class TestEventSink:
    def test_write_creates_global_log(self, sink, project_dir):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.info,
            event_type=EventType.run_started,
            message="test run",
        )
        sink.write(evt)

        log_path = project_dir / "logs" / "events.ndjson"
        assert log_path.exists()
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["message"] == "test run"
        assert parsed["level"] == "info"

    def test_write_creates_per_run_log(self, sink, project_dir):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.info,
            event_type=EventType.run_completed,
            message="done",
        )
        sink.write(evt, run_id="run_001")

        run_log = project_dir / "logs" / "runs" / "run_001.ndjson"
        assert run_log.exists()
        lines = run_log.read_text().strip().splitlines()
        assert len(lines) == 1

    def test_write_creates_per_sync_log(self, sink, project_dir):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.info,
            event_type=EventType.sync_completed,
            message="synced",
        )
        sink.write(evt, sync_id="sync_001")

        sync_log = project_dir / "logs" / "sync" / "sync_001.ndjson"
        assert sync_log.exists()

    def test_json_sort_keys(self, sink, project_dir):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.info,
            event_type=EventType.run_started,
            message="m",
        )
        sink.write(evt)

        log_path = project_dir / "logs" / "events.ndjson"
        line = log_path.read_text().strip()
        parsed = json.loads(line)
        # Verify keys are sorted
        keys = list(parsed.keys())
        assert keys == sorted(keys)

    def test_append_mode(self, sink, project_dir):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        for i in range(3):
            evt = Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_started,
                message=f"run {i}",
            )
            sink.write(evt)

        log_path = project_dir / "logs" / "events.ndjson"
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 3

    def test_read_global_returns_most_recent_first(self, sink, project_dir):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        for i in range(5):
            evt = Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_started,
                message=f"run {i}",
            )
            sink.write(evt)

        events = sink.read_global()
        assert len(events) == 5
        # Most recent first
        assert events[0]["message"] == "run 4"
        assert events[4]["message"] == "run 0"

    def test_read_global_filter_by_level(self, sink):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        sink.write(Fin123Event(
            level=EventLevel.info,
            event_type=EventType.run_started,
            message="info msg",
        ))
        sink.write(Fin123Event(
            level=EventLevel.error,
            event_type=EventType.run_failed,
            message="error msg",
        ))

        errors = sink.read_global(level="error")
        assert len(errors) == 1
        assert errors[0]["message"] == "error msg"

    def test_read_global_filter_by_plugin(self, sink):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        sink.write(Fin123Event(
            level=EventLevel.info,
            event_type=EventType.plugin_install,
            message="installed yahoo",
            context={"plugin_name": "yahoo_prices"},
        ))
        sink.write(Fin123Event(
            level=EventLevel.info,
            event_type=EventType.plugin_install,
            message="installed other",
            context={"plugin_name": "other_plugin"},
        ))

        results = sink.read_global(plugin="yahoo_prices")
        assert len(results) == 1
        assert results[0]["context"]["plugin_name"] == "yahoo_prices"

    def test_read_global_limit(self, sink):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        for i in range(10):
            sink.write(Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_started,
                message=f"run {i}",
            ))

        events = sink.read_global(limit=3)
        assert len(events) == 3

    def test_read_run_log(self, sink):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        sink.write(
            Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_completed,
                message="done",
            ),
            run_id="r1",
        )
        sink.write(
            Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_completed,
                message="other",
            ),
            run_id="r2",
        )

        events = sink.read_run_log("r1")
        assert len(events) == 1
        assert events[0]["message"] == "done"

    def test_read_sync_log(self, sink):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        sink.write(
            Fin123Event(
                level=EventLevel.info,
                event_type=EventType.sync_completed,
                message="synced",
            ),
            sync_id="s1",
        )

        events = sink.read_sync_log("s1")
        assert len(events) == 1

    def test_read_missing_log_returns_empty(self, sink):
        assert sink.read_run_log("nonexistent") == []
        assert sink.read_sync_log("nonexistent") == []
        assert sink.read_global() == []


# ---------------------------------------------------------------------------
# C) Module-level emit helpers (safety)
# ---------------------------------------------------------------------------


class TestEmitHelpers:
    def test_emit_without_project_dir_is_noop(self):
        """emit() should silently discard events when no project dir is set."""
        import fin123.logging.events as mod

        # Reset module state
        old_sink = mod._sink
        mod._sink = None
        try:
            from fin123.logging.events import EventType, emit_info

            # Should not raise
            emit_info(EventType.run_started, "test")
        finally:
            mod._sink = old_sink

    def test_set_project_dir_enables_logging(self, project_dir):
        import fin123.logging.events as mod

        old_sink = mod._sink
        try:
            from fin123.logging.events import EventType, emit_info, set_project_dir

            set_project_dir(project_dir)
            emit_info(EventType.run_started, "hello from test")

            log_path = project_dir / "logs" / "events.ndjson"
            assert log_path.exists()
            lines = log_path.read_text().strip().splitlines()
            assert len(lines) == 1
            assert "hello from test" in lines[0]
        finally:
            mod._sink = old_sink

    def test_emit_error_sets_error_code(self, project_dir):
        import fin123.logging.events as mod

        old_sink = mod._sink
        try:
            from fin123.logging.events import EventType, emit_error, set_project_dir

            set_project_dir(project_dir)
            emit_error(
                EventType.run_failed,
                "boom",
                error_code="scalar_eval_error",
            )

            log_path = project_dir / "logs" / "events.ndjson"
            line = log_path.read_text().strip()
            parsed = json.loads(line)
            assert parsed["error_code"] == "scalar_eval_error"
            assert parsed["level"] == "error"
        finally:
            mod._sink = old_sink

    def test_emit_warning(self, project_dir):
        import fin123.logging.events as mod

        old_sink = mod._sink
        try:
            from fin123.logging.events import EventType, emit_warning, set_project_dir

            set_project_dir(project_dir)
            emit_warning(
                EventType.sync_connector_warning,
                "extra columns",
                error_code="connector_schema_extra_columns",
            )

            log_path = project_dir / "logs" / "events.ndjson"
            line = log_path.read_text().strip()
            parsed = json.loads(line)
            assert parsed["level"] == "warning"
        finally:
            mod._sink = old_sink


# ---------------------------------------------------------------------------
# D) Log purge
# ---------------------------------------------------------------------------


class TestLogPurge:
    def test_purge_old_logs(self, sink, project_dir):
        import os
        import time

        from fin123.logging.events import EventLevel, EventType, Fin123Event

        # Write some logs
        sink.write(
            Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_completed,
                message="old run",
            ),
            run_id="old_run",
        )
        sink.write(
            Fin123Event(
                level=EventLevel.info,
                event_type=EventType.sync_completed,
                message="old sync",
            ),
            sync_id="old_sync",
        )

        # Backdate the files
        old_time = time.time() - (60 * 86400)  # 60 days ago
        for log_file in (project_dir / "logs").rglob("*.ndjson"):
            os.utime(log_file, (old_time, old_time))

        deleted = sink.purge_old_logs(
            max_days=30,
            retained_run_ids=set(),
            retained_sync_ids=set(),
        )
        assert deleted == 2  # old_run.ndjson and old_sync.ndjson

    def test_purge_preserves_retained_runs(self, sink, project_dir):
        import os
        import time

        from fin123.logging.events import EventLevel, EventType, Fin123Event

        sink.write(
            Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_completed,
                message="retained",
            ),
            run_id="keep_me",
        )

        # Backdate
        old_time = time.time() - (60 * 86400)
        for log_file in (project_dir / "logs").rglob("*.ndjson"):
            os.utime(log_file, (old_time, old_time))

        deleted = sink.purge_old_logs(
            max_days=30,
            retained_run_ids={"keep_me"},
            retained_sync_ids=set(),
        )
        assert deleted == 0
        assert (project_dir / "logs" / "runs" / "keep_me.ndjson").exists()


# ---------------------------------------------------------------------------
# E) GC integration
# ---------------------------------------------------------------------------


class TestGCLogIntegration:
    def test_gc_summary_includes_log_files_deleted(self, tmp_path):
        """run_gc summary dict includes log_files_deleted key."""
        from fin123.gc import run_gc
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        summary = run_gc(project_dir, dry_run=True)
        assert "log_files_deleted" in summary


# ---------------------------------------------------------------------------
# F) CLI commands
# ---------------------------------------------------------------------------


@pytest.mark.pod
class TestCLI:
    def test_events_command_no_events(self, tmp_path):
        from click.testing import CliRunner

        from fin123.cli import main
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        runner = CliRunner()
        result = runner.invoke(main, ["events", str(project_dir)])
        assert result.exit_code == 0
        assert "No events found" in result.output

    def test_run_log_command_no_events(self, tmp_path):
        from click.testing import CliRunner

        from fin123.cli import main
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        runner = CliRunner()
        result = runner.invoke(main, ["run-log", str(project_dir), "nonexistent"])
        assert result.exit_code == 0
        assert "No events found" in result.output

    def test_sync_log_command_no_events(self, tmp_path):
        from click.testing import CliRunner

        from fin123.cli import main
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        runner = CliRunner()
        result = runner.invoke(main, ["sync-log", str(project_dir), "nonexistent"])
        assert result.exit_code == 0
        assert "No events found" in result.output

    def test_events_command_with_events(self, tmp_path):
        from click.testing import CliRunner

        from fin123.cli import main
        from fin123.logging.events import EventType, emit_info, set_project_dir
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        set_project_dir(project_dir)
        emit_info(EventType.run_started, "test run started")

        runner = CliRunner()
        result = runner.invoke(main, ["events", str(project_dir)])
        assert result.exit_code == 0
        assert "test run started" in result.output
        assert "run_started" in result.output

    def test_events_command_level_filter(self, tmp_path):
        from click.testing import CliRunner

        from fin123.cli import main
        from fin123.logging.events import EventType, emit_error, emit_info, set_project_dir
        from fin123.project import scaffold_project

        project_dir = scaffold_project(tmp_path / "proj")
        set_project_dir(project_dir)
        emit_info(EventType.run_started, "info msg")
        emit_error(EventType.run_failed, "error msg")

        runner = CliRunner()
        result = runner.invoke(main, ["events", str(project_dir), "--level", "error"])
        assert result.exit_code == 0
        assert "error msg" in result.output
        assert "info msg" not in result.output


# ---------------------------------------------------------------------------
# G) Config
# ---------------------------------------------------------------------------


class TestConfig:
    def test_default_config_has_logging_max_days(self):
        from fin123.project import DEFAULT_CONFIG

        assert "logging_max_days" in DEFAULT_CONFIG
        assert DEFAULT_CONFIG["logging_max_days"] is None

    def test_default_config_has_logging_fsync(self):
        from fin123.project import DEFAULT_CONFIG

        assert "logging_fsync" in DEFAULT_CONFIG
        assert DEFAULT_CONFIG["logging_fsync"] is False

    def test_default_config_has_logging_max_bytes(self):
        from fin123.project import DEFAULT_CONFIG

        assert "logging_max_bytes" in DEFAULT_CONFIG
        assert DEFAULT_CONFIG["logging_max_bytes"] is None

    def test_default_config_has_logging_tail_bytes(self):
        from fin123.project import DEFAULT_CONFIG

        assert "logging_tail_bytes" in DEFAULT_CONFIG
        assert DEFAULT_CONFIG["logging_tail_bytes"] == 2_097_152


# ---------------------------------------------------------------------------
# H) Hardening: Secret redaction
# ---------------------------------------------------------------------------


class TestRedaction:
    def test_redact_sensitive_keys(self):
        from fin123.logging.events import redact_context

        ctx = {
            "plugin_name": "foo",
            "password": "hunter2",
            "api_key": "sk-12345",
            "token": "abc",
            "authorization": "Bearer xyz",
        }
        redacted = redact_context(ctx)
        assert redacted["plugin_name"] == "foo"
        assert redacted["password"] == "[REDACTED]"
        assert redacted["api_key"] == "[REDACTED]"
        assert redacted["token"] == "[REDACTED]"
        assert redacted["authorization"] == "[REDACTED]"

    def test_redact_url_query_params(self):
        from fin123.logging.events import redact_context

        ctx = {
            "url": "https://api.example.com/data?key=secret&token=abc",
        }
        redacted = redact_context(ctx)
        assert "secret" not in redacted["url"]
        assert "abc" not in redacted["url"]
        assert "?[REDACTED]" in redacted["url"]

    def test_redact_url_without_query_unchanged(self):
        from fin123.logging.events import redact_context

        ctx = {"url": "https://api.example.com/data"}
        redacted = redact_context(ctx)
        assert redacted["url"] == "https://api.example.com/data"

    def test_redact_long_strings_truncated(self):
        from fin123.logging.events import redact_context

        ctx = {"big_value": "x" * 300}
        redacted = redact_context(ctx)
        assert len(redacted["big_value"]) < 300
        assert redacted["big_value"].endswith("...[truncated]")

    def test_redact_headers_whitelist(self):
        from fin123.logging.events import redact_context

        ctx = {
            "headers": {
                "user-agent": "fin123/1.0",
                "content-type": "application/json",
                "Authorization": "Bearer secret",
                "X-Custom": "value",
            }
        }
        redacted = redact_context(ctx)
        assert "user-agent" in redacted["headers"]
        assert "content-type" in redacted["headers"]
        assert "Authorization" not in redacted["headers"]
        assert "X-Custom" not in redacted["headers"]

    def test_redact_nested_dict(self):
        from fin123.logging.events import redact_context

        ctx = {
            "config": {
                "dsn": "postgres://user:pass@host/db",
                "name": "mydb",
            }
        }
        redacted = redact_context(ctx)
        assert redacted["config"]["dsn"] == "[REDACTED]"
        assert redacted["config"]["name"] == "mydb"

    def test_redact_list_values(self):
        from fin123.logging.events import redact_context

        ctx = {
            "urls": [
                "https://example.com/a?key=secret",
                "plain text",
            ]
        }
        redacted = redact_context(ctx)
        assert "secret" not in redacted["urls"][0]
        assert redacted["urls"][1] == "plain text"

    def test_emit_applies_redaction(self, project_dir):
        """Verify that emit() redacts secrets from context."""
        import fin123.logging.events as mod

        old_sink = mod._sink
        try:
            from fin123.logging.events import EventType, emit_info, set_project_dir

            set_project_dir(project_dir)
            emit_info(
                EventType.run_started,
                "test",
                context={"password": "hunter2", "model_id": "m1"},
            )

            log_path = project_dir / "logs" / "events.ndjson"
            parsed = json.loads(log_path.read_text().strip())
            assert parsed["context"]["password"] == "[REDACTED]"
            assert parsed["context"]["model_id"] == "m1"
        finally:
            mod._sink = old_sink


# ---------------------------------------------------------------------------
# I) Hardening: Attribution invariants
# ---------------------------------------------------------------------------


class TestAttributionInvariants:
    def test_missing_attribution_downgrades_to_warning(self, project_dir):
        """Event with missing required keys should be downgraded to warning."""
        import fin123.logging.events as mod

        old_sink = mod._sink
        try:
            from fin123.logging.events import EventType, Fin123Event, emit, set_project_dir

            set_project_dir(project_dir)

            # run_completed requires run_id and model_id -- emit without them
            evt = Fin123Event(
                level=mod.EventLevel.info,
                event_type=EventType.run_completed,
                message="completed without attribution",
                context={},
            )
            emit(evt)

            log_path = project_dir / "logs" / "events.ndjson"
            parsed = json.loads(log_path.read_text().strip())
            assert parsed["level"] == "warning"
            assert "_missing_attribution" in parsed["context"]
        finally:
            mod._sink = old_sink

    def test_valid_attribution_stays_info(self, project_dir):
        """Event with all required keys should keep its original level."""
        import fin123.logging.events as mod

        old_sink = mod._sink
        try:
            from fin123.logging.events import EventType, Fin123Event, emit, set_project_dir

            set_project_dir(project_dir)

            evt = Fin123Event(
                level=mod.EventLevel.info,
                event_type=EventType.run_completed,
                message="completed with attribution",
                context={"run_id": "r1", "model_id": "m1"},
            )
            emit(evt)

            log_path = project_dir / "logs" / "events.ndjson"
            parsed = json.loads(log_path.read_text().strip())
            assert parsed["level"] == "info"
            assert "_missing_attribution" not in parsed["context"]
        finally:
            mod._sink = old_sink


# ---------------------------------------------------------------------------
# J) Hardening: Helper constructors
# ---------------------------------------------------------------------------


class TestHelperConstructors:
    def test_make_plugin_event(self):
        from fin123.logging.events import EventLevel, EventType, make_plugin_event

        evt = make_plugin_event(
            EventType.plugin_install,
            EventLevel.info,
            "Installed foo",
            plugin_name="foo",
            plugin_version="v1",
            plugin_sha256="abc123",
            engine_version="1.0.0",
            extra={"activated": True},
        )
        assert evt.context["plugin_name"] == "foo"
        assert evt.context["plugin_version"] == "v1"
        assert evt.context["plugin_sha256"] == "abc123"
        assert evt.context["engine_version"] == "1.0.0"
        assert evt.context["activated"] is True
        assert evt.message == "Installed foo"

    def test_make_run_event(self):
        from fin123.logging.events import EventLevel, EventType, make_run_event

        evt = make_run_event(
            EventType.run_completed,
            EventLevel.info,
            "Run done",
            run_id="r1",
            model_id="m1",
            model_version_id="v001",
            extra={"scalar_count": 5},
        )
        assert evt.context["run_id"] == "r1"
        assert evt.context["model_id"] == "m1"
        assert evt.context["model_version_id"] == "v001"
        assert evt.context["scalar_count"] == 5

    def test_make_sync_event(self):
        from fin123.logging.events import EventLevel, EventType, make_sync_event

        evt = make_sync_event(
            EventType.sync_completed,
            EventLevel.info,
            "Sync done",
            sync_id="s1",
            extra={"synced": ["prices"]},
        )
        assert evt.context["sync_id"] == "s1"
        assert evt.context["synced"] == ["prices"]

    def test_make_plugin_event_optional_fields(self):
        from fin123.logging.events import EventLevel, EventType, make_plugin_event

        evt = make_plugin_event(
            EventType.plugin_uninstall,
            EventLevel.info,
            "Uninstalled bar",
            plugin_name="bar",
        )
        assert evt.context == {"plugin_name": "bar"}

    def test_make_run_event_with_error_code(self):
        from fin123.logging.events import EventLevel, EventType, make_run_event

        evt = make_run_event(
            EventType.run_failed,
            EventLevel.error,
            "Failed",
            error_code="scalar_eval_error",
        )
        assert evt.error_code == "scalar_eval_error"
        assert evt.level == EventLevel.error


# ---------------------------------------------------------------------------
# K) Hardening: Path traversal rejection
# ---------------------------------------------------------------------------


class TestPathTraversal:
    def test_sink_rejects_traversal_in_run_id(self, sink, project_dir):
        """write() should ignore run_id with path traversal."""
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.info,
            event_type=EventType.run_completed,
            message="traversal",
        )
        sink.write(evt, run_id="../../../etc/passwd")

        # Only global log should exist, no per-run log created
        run_files = list((project_dir / "logs" / "runs").glob("*.ndjson"))
        assert len(run_files) == 0
        # Global log should still have the event
        assert len(sink.read_global()) == 1

    def test_sink_rejects_traversal_in_sync_id(self, sink, project_dir):
        from fin123.logging.events import EventLevel, EventType, Fin123Event

        evt = Fin123Event(
            level=EventLevel.info,
            event_type=EventType.sync_completed,
            message="traversal",
        )
        sink.write(evt, sync_id="../../bad")

        # No per-sync log created
        sync_files = list((project_dir / "logs" / "sync").glob("*.ndjson"))
        assert len(sync_files) == 0

    def test_read_run_log_rejects_traversal(self, sink):
        result = sink.read_run_log("../../../etc/passwd")
        assert result == []

    def test_read_sync_log_rejects_traversal(self, sink):
        result = sink.read_sync_log("../../bad")
        assert result == []


# ---------------------------------------------------------------------------
# L) Hardening: Tail-read bounded reading
# ---------------------------------------------------------------------------


class TestTailRead:
    def test_tail_read_small_file(self, project_dir):
        """Small files should be read entirely."""
        from fin123.logging.events import EventLevel, EventType, Fin123Event
        from fin123.logging.sink import EventSink

        sink = EventSink(project_dir, tail_bytes=1024 * 1024)

        for i in range(5):
            sink.write(Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_started,
                message=f"event {i}",
            ))

        events = sink.read_global()
        assert len(events) == 5

    def test_tail_read_large_file_bounded(self, project_dir):
        """Large files should only return events from the tail."""
        from fin123.logging.events import EventLevel, EventType, Fin123Event
        from fin123.logging.sink import EventSink

        # Use a very small tail_bytes to test bounding
        sink = EventSink(project_dir, tail_bytes=200)

        # Write enough events that they exceed 200 bytes total
        for i in range(20):
            sink.write(Fin123Event(
                level=EventLevel.info,
                event_type=EventType.run_started,
                message=f"event {i:04d}",
            ))

        events = sink.read_global()
        # Should get fewer than 20 events since we bounded the read
        assert len(events) < 20
        assert len(events) > 0

    def test_limit_capped_at_2000(self, project_dir):
        """read_global should cap limit at 2000."""
        from fin123.logging.sink import EventSink

        sink = EventSink(project_dir)
        # Requesting limit > 2000 should be capped
        events = sink.read_global(limit=5000)
        assert events == []  # No events, but the cap is applied internally


# ---------------------------------------------------------------------------
# M) Hardening: Concurrency safety (threading)
# ---------------------------------------------------------------------------


class TestConcurrencySafety:
    def test_multi_threaded_appends(self, project_dir):
        """Multiple threads appending simultaneously should not lose events."""
        import threading

        from fin123.logging.events import EventLevel, EventType, Fin123Event
        from fin123.logging.sink import EventSink

        sink = EventSink(project_dir)
        num_threads = 4
        events_per_thread = 25
        barrier = threading.Barrier(num_threads)

        def writer(thread_id: int) -> None:
            barrier.wait()
            for i in range(events_per_thread):
                sink.write(Fin123Event(
                    level=EventLevel.info,
                    event_type=EventType.run_started,
                    message=f"t{thread_id}-e{i}",
                ))

        threads = [
            threading.Thread(target=writer, args=(tid,))
            for tid in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        log_path = project_dir / "logs" / "events.ndjson"
        lines = [l for l in log_path.read_text().splitlines() if l.strip()]
        assert len(lines) == num_threads * events_per_thread

        # Each line should be valid JSON
        for line in lines:
            parsed = json.loads(line)
            assert "message" in parsed

    def test_eager_directory_creation(self, tmp_path):
        """EventSink should create log directories on init."""
        from fin123.logging.sink import EventSink

        proj = tmp_path / "newproj"
        proj.mkdir()
        EventSink(proj)

        assert (proj / "logs").is_dir()
        assert (proj / "logs" / "runs").is_dir()
        assert (proj / "logs" / "sync").is_dir()


# ---------------------------------------------------------------------------
# N) Hardening: Fsync config
# ---------------------------------------------------------------------------


class TestFsyncConfig:
    def test_sink_accepts_fsync_flag(self, project_dir):
        from fin123.logging.events import EventLevel, EventType, Fin123Event
        from fin123.logging.sink import EventSink

        sink = EventSink(project_dir, fsync=True)
        evt = Fin123Event(
            level=EventLevel.info,
            event_type=EventType.run_started,
            message="fsync test",
        )
        sink.write(evt)

        log_path = project_dir / "logs" / "events.ndjson"
        assert log_path.exists()
        parsed = json.loads(log_path.read_text().strip())
        assert parsed["message"] == "fsync test"


# ---------------------------------------------------------------------------
# O) Hardening: Exports
# ---------------------------------------------------------------------------


class TestExports:
    def test_init_exports_new_helpers(self):
        from fin123.logging import (
            make_plugin_event,
            make_run_event,
            make_sync_event,
            redact_context,
        )

        assert callable(make_plugin_event)
        assert callable(make_run_event)
        assert callable(make_sync_event)
        assert callable(redact_context)
