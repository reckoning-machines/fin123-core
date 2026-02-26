"""Filesystem NDJSON event sink with concurrency-safe appends.

Events are appended as one JSON line per event.  Three log destinations:

- ``logs/events.ndjson``  -- global event log
- ``logs/runs/<run_id>.ndjson``  -- per-run log
- ``logs/sync/<sync_id>.ndjson`` -- per-sync log

Writes use ``json.dumps(sort_keys=True)`` for deterministic output.

Concurrency safety:

- Each append acquires an exclusive ``fcntl.flock`` on the target file.
- Reads acquire a shared lock.
- Lock duration is kept minimal (single write/read per lock).
- On platforms without ``fcntl`` (Windows), locking is skipped with a
  stderr warning.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from fin123.logging.events import EventLevel, Fin123Event

# Try to import fcntl for file locking (Unix only)
try:
    import fcntl

    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False
    print(
        "[fin123] fcntl not available; log file locking disabled",
        file=sys.stderr,
    )

# Path-component validation: reject anything that could escape the logs dir
import re

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")

# Default tail-read size (2 MB)
_DEFAULT_TAIL_BYTES = 2 * 1024 * 1024

# Default number of newest lines to preserve during global log purge
_DEFAULT_PRESERVE_LINES = 500


class EventSink:
    """Append-only NDJSON log writer with file locking."""

    def __init__(self, project_dir: Path, *, fsync: bool = False, tail_bytes: int | None = None) -> None:
        self.logs_dir = project_dir / "logs"
        self._fsync = fsync
        self._tail_bytes = tail_bytes if tail_bytes is not None else _DEFAULT_TAIL_BYTES

        # Eager directory creation (Deliverable F)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        (self.logs_dir / "runs").mkdir(exist_ok=True)
        (self.logs_dir / "sync").mkdir(exist_ok=True)

    def write(
        self,
        event: Fin123Event,
        *,
        run_id: str | None = None,
        sync_id: str | None = None,
    ) -> None:
        """Append *event* to the global log and optionally a scoped log."""
        line = json.dumps(event.model_dump(), sort_keys=True, default=str) + "\n"

        # Global log
        self._append(self.logs_dir / "events.ndjson", line)

        # Per-run log
        if run_id and _SAFE_ID_RE.match(run_id):
            self._append(self.logs_dir / "runs" / f"{run_id}.ndjson", line)

        # Per-sync log
        if sync_id and _SAFE_ID_RE.match(sync_id):
            self._append(self.logs_dir / "sync" / f"{sync_id}.ndjson", line)

    # ------------------------------------------------------------------
    # Query helpers (used by API / CLI)
    # ------------------------------------------------------------------

    def read_global(
        self,
        *,
        level: str | None = None,
        event_type: str | None = None,
        plugin: str | None = None,
        run_id: str | None = None,
        sync_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Read events from the global log, most-recent-first, with filters.

        Uses tail-style reading to bound memory usage on large log files.
        """
        limit = min(limit, 2000)

        path = self.logs_dir / "events.ndjson"
        events = self._read_ndjson(path)

        if level:
            events = [e for e in events if e.get("level") == level]
        if event_type:
            events = [e for e in events if e.get("event_type") == event_type]
        if plugin:
            events = [
                e for e in events
                if e.get("context", {}).get("plugin_name") == plugin
            ]
        if run_id:
            events = [
                e for e in events
                if e.get("context", {}).get("run_id") == run_id
            ]
        if sync_id:
            events = [
                e for e in events
                if e.get("context", {}).get("sync_id") == sync_id
            ]

        # Most recent first
        events.reverse()
        return events[:limit]

    def read_run_log(self, run_id: str) -> list[dict[str, Any]]:
        """Read all events for a specific run."""
        if not _SAFE_ID_RE.match(run_id):
            return []
        path = self.logs_dir / "runs" / f"{run_id}.ndjson"
        return self._read_ndjson(path)

    def read_sync_log(self, sync_id: str) -> list[dict[str, Any]]:
        """Read all events for a specific sync."""
        if not _SAFE_ID_RE.match(sync_id):
            return []
        path = self.logs_dir / "sync" / f"{sync_id}.ndjson"
        return self._read_ndjson(path)

    def purge_old_logs(
        self,
        max_days: int,
        retained_run_ids: set[str],
        retained_sync_ids: set[str],
        *,
        max_bytes: int | None = None,
        preserve_lines: int = _DEFAULT_PRESERVE_LINES,
    ) -> int:
        """Delete log files older than *max_days*, preserving retained runs/syncs.

        Global log purge uses an atomic rewrite under exclusive lock,
        preserving the newest *preserve_lines* lines (default 500).

        Returns the number of files deleted.
        """
        import time

        deleted = 0
        cutoff = time.time() - (max_days * 86400)

        # Purge per-run logs
        runs_log_dir = self.logs_dir / "runs"
        if runs_log_dir.exists():
            for f in runs_log_dir.iterdir():
                if not f.is_file() or f.suffix != ".ndjson":
                    continue
                rid = f.stem
                if rid in retained_run_ids:
                    continue
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    deleted += 1

        # Purge per-sync logs
        sync_log_dir = self.logs_dir / "sync"
        if sync_log_dir.exists():
            for f in sync_log_dir.iterdir():
                if not f.is_file() or f.suffix != ".ndjson":
                    continue
                sid = f.stem
                if sid in retained_sync_ids:
                    continue
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    deleted += 1

        # Trim global log: atomic rewrite under exclusive lock
        self._purge_global_log(cutoff, max_bytes=max_bytes, preserve_lines=preserve_lines)

        return deleted

    def _purge_global_log(
        self,
        cutoff: float,
        *,
        max_bytes: int | None = None,
        preserve_lines: int = _DEFAULT_PRESERVE_LINES,
    ) -> None:
        """Rewrite events.ndjson keeping only recent lines.

        Uses atomic write-to-tmp + os.replace under exclusive lock.
        """
        from datetime import datetime, timezone

        global_path = self.logs_dir / "events.ndjson"
        if not global_path.exists():
            return

        # Check if we need to purge at all
        try:
            file_size = global_path.stat().st_size
        except OSError:
            return

        needs_purge = False
        if max_bytes and file_size > max_bytes:
            needs_purge = True

        # Read all lines (under shared lock)
        all_lines = self._read_raw_lines(global_path)
        if not all_lines:
            return

        # Filter by timestamp
        kept_lines: list[str] = []
        for line in all_lines:
            if not line.strip():
                continue
            try:
                evt = json.loads(line)
                ts_str = evt.get("ts", "")
                ts = datetime.fromisoformat(
                    ts_str.replace("Z", "+00:00")
                ).timestamp()
                if ts >= cutoff:
                    kept_lines.append(line)
            except (json.JSONDecodeError, ValueError, OSError):
                # Unparseable lines are kept
                kept_lines.append(line)

        if not needs_purge and len(kept_lines) == len(all_lines):
            return  # Nothing to trim

        # Also enforce max_bytes by keeping only newest lines
        if max_bytes:
            total = 0
            trimmed: list[str] = []
            for line in reversed(kept_lines):
                total += len(line.encode("utf-8")) + 1  # +1 for newline
                if total > max_bytes:
                    break
                trimmed.append(line)
            trimmed.reverse()
            kept_lines = trimmed

        # Always preserve at least the newest N lines
        if len(kept_lines) < preserve_lines and len(all_lines) >= preserve_lines:
            # Take the last preserve_lines from the original set
            kept_lines = [l for l in all_lines[-preserve_lines:] if l.strip()]
        elif len(kept_lines) > preserve_lines:
            pass  # keep all that passed the timestamp filter

        # Atomic rewrite under exclusive lock
        tmp = global_path.with_suffix(".ndjson.tmp")
        content = "\n".join(kept_lines) + ("\n" if kept_lines else "")
        tmp.write_text(content, encoding="utf-8")

        if _HAS_FCNTL:
            fd = os.open(str(global_path), os.O_RDWR | os.O_CREAT)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                os.replace(str(tmp), str(global_path))
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
        else:
            os.replace(str(tmp), str(global_path))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _append(self, path: Path, line: str) -> None:
        """Append a single line to *path* under exclusive file lock."""
        path.parent.mkdir(parents=True, exist_ok=True)

        if _HAS_FCNTL:
            fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                os.write(fd, line.encode("utf-8"))
                if self._fsync:
                    os.fsync(fd)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
        else:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
                if self._fsync:
                    f.flush()
                    os.fsync(f.fileno())

    def _read_ndjson(self, path: Path) -> list[dict[str, Any]]:
        """Read an NDJSON file with tail-bounded reading and shared lock.

        Only reads the last ``self._tail_bytes`` of the file to bound
        memory usage on large log files.
        """
        if not path.exists():
            return []

        raw = self._read_tail(path)
        events: list[dict[str, Any]] = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return events

    def _read_tail(self, path: Path) -> str:
        """Read up to the last ``self._tail_bytes`` of a file under shared lock."""
        if _HAS_FCNTL:
            fd = os.open(str(path), os.O_RDONLY)
            try:
                fcntl.flock(fd, fcntl.LOCK_SH)
                file_size = os.fstat(fd).st_size
                if file_size <= self._tail_bytes:
                    data = os.read(fd, file_size)
                else:
                    os.lseek(fd, file_size - self._tail_bytes, os.SEEK_SET)
                    data = os.read(fd, self._tail_bytes)
                    # Drop the first (likely partial) line
                    idx = data.find(b"\n")
                    if idx >= 0:
                        data = data[idx + 1:]
                return data.decode("utf-8", errors="replace")
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
        else:
            # Fallback: read entire file (no locking)
            try:
                file_size = path.stat().st_size
            except OSError:
                return ""
            if file_size <= self._tail_bytes:
                return path.read_text(encoding="utf-8")
            else:
                with open(path, "rb") as f:
                    f.seek(file_size - self._tail_bytes)
                    data = f.read()
                    idx = data.find(b"\n")
                    if idx >= 0:
                        data = data[idx + 1:]
                    return data.decode("utf-8", errors="replace")

    def _read_raw_lines(self, path: Path) -> list[str]:
        """Read all non-empty lines from a file (for purge operations)."""
        if not path.exists():
            return []
        try:
            text = path.read_text(encoding="utf-8")
            return [l for l in text.splitlines() if l.strip()]
        except OSError:
            return []
