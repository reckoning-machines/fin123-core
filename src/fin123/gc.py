"""Garbage collection for bounded storage management."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fin123.project import load_project_config
from fin123.versioning import SnapshotStore


def run_gc(project_dir: Path, dry_run: bool = False) -> dict[str, Any]:
    """Run garbage collection on a fin123 project.

    Enforces configured limits on runs, artifacts, and sync_runs by deleting
    the oldest unpinned items when limits are exceeded.  The most recent run,
    the most recent version of each artifact, and the most recent sync run are
    never deleted even when unpinned.

    Args:
        project_dir: Root of the fin123 project.
        dry_run: When True, compute what would be deleted without actually
            removing anything.

    Returns:
        Summary dict with counts and bytes freed (or would-be freed).
    """
    project_dir = project_dir.resolve()
    config = load_project_config(project_dir)
    pins = _load_pins(project_dir)

    summary: dict[str, Any] = {
        "runs_deleted": 0,
        "artifact_versions_deleted": 0,
        "sync_runs_deleted": 0,
        "model_versions_deleted": 0,
        "log_files_deleted": 0,
        "bytes_freed": 0,
        "orphaned_cleaned": 0,
        "dry_run": dry_run,
    }

    # GC runs
    runs_dir = project_dir / "runs"
    if runs_dir.exists():
        _gc_runs(runs_dir, config, pins, summary, dry_run)

    # GC artifacts
    artifacts_dir = project_dir / "artifacts"
    if artifacts_dir.exists():
        _gc_artifacts(artifacts_dir, config, pins, summary, dry_run)

    # GC sync_runs
    sync_runs_dir = project_dir / "sync_runs"
    if sync_runs_dir.exists():
        _gc_sync_runs(sync_runs_dir, config, pins, summary, dry_run)

    # GC model versions (snapshots)
    snap_dir = project_dir / "snapshots" / "workbook"
    if snap_dir.exists():
        _gc_model_versions(project_dir, config, summary, dry_run)

    # GC log files
    logging_max_days = config.get("logging_max_days")
    if logging_max_days is not None:
        _gc_logs(project_dir, logging_max_days, summary, dry_run)

    # Clean orphaned cache files
    if not dry_run:
        _clean_orphans(project_dir, summary)

    return summary


def _load_pins(project_dir: Path) -> set[str]:
    """Load pinned IDs from ``pins.yaml`` if it exists.

    Args:
        project_dir: Root of the fin123 project.

    Returns:
        Set of pinned identifiers.
    """
    pins_path = project_dir / "pins.yaml"
    if not pins_path.exists():
        return set()

    import yaml

    data = yaml.safe_load(pins_path.read_text()) or {}
    pinned = set()
    for item in data.get("pinned", []):
        pinned.add(str(item))
    return pinned


def _is_pinned(meta_path: Path, pins: set[str]) -> bool:
    """Check whether an item is pinned via its metadata or the pins set.

    Args:
        meta_path: Path to the ``run_meta.json``, ``meta.json``, or
            ``sync_meta.json``.
        pins: Set of pinned identifiers from ``pins.yaml``.

    Returns:
        True if the item is pinned.
    """
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        if meta.get("pinned", False):
            return True
        for id_key in ("run_id", "sync_id"):
            if meta.get(id_key) in pins:
                return True
        art_id = f"{meta.get('artifact_name', '')}/{meta.get('version', '')}"
        if art_id in pins:
            return True
    return False


def _dir_size(path: Path) -> int:
    """Compute total size of all files under a directory.

    Args:
        path: Directory path.

    Returns:
        Total size in bytes.
    """
    total = 0
    for f in path.rglob("*"):
        if f.is_file():
            total += f.stat().st_size
    return total


def _is_in_progress(d: Path) -> bool:
    """Check whether a directory has an in-progress marker.

    Directories with a ``.in_progress`` marker are mid-write and must
    not be deleted by GC.

    Args:
        d: Directory path.

    Returns:
        True if the directory contains an in-progress marker.
    """
    return (d / ".in_progress").exists()


def _deletable_dirs(
    dirs: list[Path], meta_filename: str, pins: set[str]
) -> list[Path]:
    """Return directories eligible for deletion.

    Excludes the most recent directory (last when sorted by name), any
    pinned directories, and any directories with an in-progress marker.

    Args:
        dirs: Sorted list of directories (oldest first).
        meta_filename: Name of the metadata file inside each dir.
        pins: Pinned identifiers.

    Returns:
        List of directories that may be deleted.
    """
    if not dirs:
        return []
    # The most recent (last) is always protected
    protected = dirs[-1]
    return [
        d for d in dirs
        if d != protected
        and not _is_pinned(d / meta_filename, pins)
        and not _is_in_progress(d)
    ]


def _gc_runs(
    runs_dir: Path,
    config: dict[str, Any],
    pins: set[str],
    summary: dict[str, Any],
    dry_run: bool,
) -> None:
    """Enforce run limits by deleting oldest unpinned runs.

    The most recent run is never deleted.

    Args:
        runs_dir: Path to the ``runs/`` directory.
        config: Project configuration.
        pins: Pinned identifiers.
        summary: Mutable summary dict to update.
        dry_run: If True, do not actually delete.
    """
    max_runs = config.get("max_runs", 50)
    max_bytes = config.get("max_total_run_bytes", 2_000_000_000)
    ttl_days = config.get("ttl_days")

    run_dirs = sorted(
        [d for d in runs_dir.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )

    # Delete by TTL first
    if ttl_days is not None:
        cutoff = datetime.now(timezone.utc).timestamp() - (ttl_days * 86400)
        deletable = _deletable_dirs(run_dirs, "run_meta.json", pins)
        for rd in list(deletable):
            meta_path = rd / "run_meta.json"
            if meta_path.exists():
                meta = json.loads(meta_path.read_text())
                ts = datetime.fromisoformat(meta["timestamp"]).timestamp()
                if ts < cutoff:
                    freed = _dir_size(rd)
                    if not dry_run:
                        shutil.rmtree(rd)
                    run_dirs.remove(rd)
                    summary["runs_deleted"] += 1
                    summary["bytes_freed"] += freed

    # Delete by count
    deletable = _deletable_dirs(run_dirs, "run_meta.json", pins)
    while len(run_dirs) > max_runs and deletable:
        oldest = deletable.pop(0)
        freed = _dir_size(oldest)
        if not dry_run:
            shutil.rmtree(oldest)
        run_dirs.remove(oldest)
        summary["runs_deleted"] += 1
        summary["bytes_freed"] += freed

    # Delete by total size
    total_size = sum(_dir_size(d) for d in run_dirs)
    deletable = _deletable_dirs(run_dirs, "run_meta.json", pins)
    while total_size > max_bytes and deletable:
        oldest = deletable.pop(0)
        freed = _dir_size(oldest)
        if not dry_run:
            shutil.rmtree(oldest)
        run_dirs.remove(oldest)
        total_size -= freed
        summary["runs_deleted"] += 1
        summary["bytes_freed"] += freed


def _gc_artifacts(
    artifacts_dir: Path,
    config: dict[str, Any],
    pins: set[str],
    summary: dict[str, Any],
    dry_run: bool,
) -> None:
    """Enforce artifact version limits per artifact name.

    The most recent version of each artifact is never deleted.

    Args:
        artifacts_dir: Path to the ``artifacts/`` directory.
        config: Project configuration.
        pins: Pinned identifiers.
        summary: Mutable summary dict to update.
        dry_run: If True, do not actually delete.
    """
    max_versions = config.get("max_artifact_versions", 20)
    max_bytes = config.get("max_total_artifact_bytes", 5_000_000_000)

    for artifact_dir in sorted(artifacts_dir.iterdir()):
        if not artifact_dir.is_dir():
            continue

        version_dirs = sorted(
            [d for d in artifact_dir.iterdir() if d.is_dir()],
            key=lambda d: d.name,
        )

        deletable = _deletable_dirs(version_dirs, "meta.json", pins)

        # Delete by version count
        while len(version_dirs) > max_versions and deletable:
            oldest = deletable.pop(0)
            freed = _dir_size(oldest)
            if not dry_run:
                shutil.rmtree(oldest)
            version_dirs.remove(oldest)
            summary["artifact_versions_deleted"] += 1
            summary["bytes_freed"] += freed

    # Delete by total artifact size
    all_versions = []
    for artifact_dir in sorted(artifacts_dir.iterdir()):
        if not artifact_dir.is_dir():
            continue
        for vd in sorted(artifact_dir.iterdir()):
            if vd.is_dir():
                all_versions.append(vd)

    # Collect the latest version per artifact to protect them
    latest_per_artifact: set[Path] = set()
    for artifact_dir in sorted(artifacts_dir.iterdir()):
        if not artifact_dir.is_dir():
            continue
        vdirs = sorted(d for d in artifact_dir.iterdir() if d.is_dir())
        if vdirs:
            latest_per_artifact.add(vdirs[-1])

    deletable_versions = [
        v for v in all_versions
        if v not in latest_per_artifact and not _is_pinned(v / "meta.json", pins)
    ]
    total_size = sum(_dir_size(v) for v in all_versions)
    while total_size > max_bytes and deletable_versions:
        oldest = deletable_versions.pop(0)
        freed = _dir_size(oldest)
        if not dry_run:
            shutil.rmtree(oldest)
        total_size -= freed
        summary["artifact_versions_deleted"] += 1
        summary["bytes_freed"] += freed


def _gc_sync_runs(
    sync_runs_dir: Path,
    config: dict[str, Any],
    pins: set[str],
    summary: dict[str, Any],
    dry_run: bool,
) -> None:
    """Enforce sync run limits by deleting oldest unpinned sync runs.

    The most recent sync run is never deleted.

    Args:
        sync_runs_dir: Path to the ``sync_runs/`` directory.
        config: Project configuration.
        pins: Pinned identifiers.
        summary: Mutable summary dict to update.
        dry_run: If True, do not actually delete.
    """
    max_sync_runs = config.get("max_sync_runs", 50)
    max_bytes = config.get("max_total_sync_bytes", 1_000_000_000)
    ttl_days = config.get("ttl_sync_days")

    sync_dirs = sorted(
        [d for d in sync_runs_dir.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )

    # Delete by TTL
    if ttl_days is not None:
        cutoff = datetime.now(timezone.utc).timestamp() - (ttl_days * 86400)
        deletable = _deletable_dirs(sync_dirs, "sync_meta.json", pins)
        for sd in list(deletable):
            meta_path = sd / "sync_meta.json"
            if meta_path.exists():
                meta = json.loads(meta_path.read_text())
                ts = datetime.fromisoformat(meta["timestamp"]).timestamp()
                if ts < cutoff:
                    freed = _dir_size(sd)
                    if not dry_run:
                        shutil.rmtree(sd)
                    sync_dirs.remove(sd)
                    summary["sync_runs_deleted"] += 1
                    summary["bytes_freed"] += freed

    # Delete by count
    deletable = _deletable_dirs(sync_dirs, "sync_meta.json", pins)
    while len(sync_dirs) > max_sync_runs and deletable:
        oldest = deletable.pop(0)
        freed = _dir_size(oldest)
        if not dry_run:
            shutil.rmtree(oldest)
        sync_dirs.remove(oldest)
        summary["sync_runs_deleted"] += 1
        summary["bytes_freed"] += freed

    # Delete by total size
    total_size = sum(_dir_size(d) for d in sync_dirs)
    deletable = _deletable_dirs(sync_dirs, "sync_meta.json", pins)
    while total_size > max_bytes and deletable:
        oldest = deletable.pop(0)
        freed = _dir_size(oldest)
        if not dry_run:
            shutil.rmtree(oldest)
        sync_dirs.remove(oldest)
        total_size -= freed
        summary["sync_runs_deleted"] += 1
        summary["bytes_freed"] += freed


def _gc_model_versions(
    project_dir: Path,
    config: dict[str, Any],
    summary: dict[str, Any],
    dry_run: bool,
) -> None:
    """Enforce model version retention by deleting old unpinned snapshots.

    Protected versions: latest, pinned, and those referenced by retained runs.

    Args:
        project_dir: Root of the fin123 project.
        config: Project configuration.
        summary: Mutable summary dict to update.
        dry_run: If True, do not actually delete.
    """
    max_versions = config.get("max_model_versions", 200)
    max_bytes = config.get("max_total_model_version_bytes")
    ttl_days = config.get("ttl_model_versions_days")

    store = SnapshotStore(project_dir)
    index = store.load_index()
    versions = index.get("versions", [])
    if not versions:
        return

    # Build protected set: latest + pinned + referenced by retained runs
    protected: set[str] = set()
    # Latest is always protected
    protected.add(versions[-1]["model_version_id"])
    # Pinned versions
    for v in versions:
        if v.get("pinned"):
            protected.add(v["model_version_id"])

    # Versions referenced by retained runs
    runs_dir = project_dir / "runs"
    if runs_dir.exists():
        for run_dir in runs_dir.iterdir():
            meta_path = run_dir / "run_meta.json"
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text())
                    mvid = meta.get("model_version_id")
                    if mvid:
                        protected.add(mvid)
                except (json.JSONDecodeError, OSError):
                    pass

    # Build list of deletable versions (in order, oldest first)
    deletable_versions = [
        v for v in versions
        if v["model_version_id"] not in protected
    ]

    deleted_ids: set[str] = set()

    # Delete by TTL
    if ttl_days is not None:
        cutoff = datetime.now(timezone.utc).timestamp() - (ttl_days * 86400)
        for v in list(deletable_versions):
            if v.get("created_at"):
                try:
                    ts = datetime.fromisoformat(v["created_at"]).timestamp()
                    if ts < cutoff:
                        vid = v["model_version_id"]
                        vdir = store.snapshot_dir / vid
                        freed = _dir_size(vdir) if vdir.exists() else 0
                        if not dry_run and vdir.exists():
                            shutil.rmtree(vdir)
                        deleted_ids.add(vid)
                        deletable_versions.remove(v)
                        summary["model_versions_deleted"] += 1
                        summary["bytes_freed"] += freed
                except (ValueError, OSError):
                    pass

    # Delete by count
    remaining_count = len(versions) - len(deleted_ids)
    while remaining_count > max_versions and deletable_versions:
        v = deletable_versions.pop(0)
        vid = v["model_version_id"]
        vdir = store.snapshot_dir / vid
        freed = _dir_size(vdir) if vdir.exists() else 0
        if not dry_run and vdir.exists():
            shutil.rmtree(vdir)
        deleted_ids.add(vid)
        remaining_count -= 1
        summary["model_versions_deleted"] += 1
        summary["bytes_freed"] += freed

    # Delete by total size
    if max_bytes is not None:
        total_size = sum(
            _dir_size(store.snapshot_dir / v["model_version_id"])
            for v in versions
            if v["model_version_id"] not in deleted_ids
            and (store.snapshot_dir / v["model_version_id"]).exists()
        )
        while total_size > max_bytes and deletable_versions:
            v = deletable_versions.pop(0)
            vid = v["model_version_id"]
            vdir = store.snapshot_dir / vid
            freed = _dir_size(vdir) if vdir.exists() else 0
            if not dry_run and vdir.exists():
                shutil.rmtree(vdir)
            deleted_ids.add(vid)
            total_size -= freed
            summary["model_versions_deleted"] += 1
            summary["bytes_freed"] += freed

    # Update index.json to remove deleted versions
    if deleted_ids and not dry_run:
        index["versions"] = [
            v for v in index["versions"]
            if v["model_version_id"] not in deleted_ids
        ]
        store._write_index(index)


def _gc_logs(
    project_dir: Path,
    max_days: int,
    summary: dict[str, Any],
    dry_run: bool,
) -> None:
    """Purge old event log files, preserving logs for retained runs/syncs.

    Args:
        project_dir: Root of the fin123 project.
        max_days: Maximum age in days for log files.
        summary: Mutable summary dict to update.
        dry_run: If True, do not actually delete.
    """
    logs_dir = project_dir / "logs"
    if not logs_dir.exists():
        return

    # Build set of retained run/sync IDs (those still on disk)
    retained_run_ids: set[str] = set()
    runs_dir = project_dir / "runs"
    if runs_dir.exists():
        for d in runs_dir.iterdir():
            if d.is_dir():
                retained_run_ids.add(d.name)

    retained_sync_ids: set[str] = set()
    sync_runs_dir = project_dir / "sync_runs"
    if sync_runs_dir.exists():
        for d in sync_runs_dir.iterdir():
            if d.is_dir():
                retained_sync_ids.add(d.name)

    if dry_run:
        # Count what would be deleted
        import time

        cutoff = time.time() - (max_days * 86400)
        count = 0
        for subdir_name in ("runs", "sync"):
            subdir = logs_dir / subdir_name
            if not subdir.exists():
                continue
            retained = retained_run_ids if subdir_name == "runs" else retained_sync_ids
            for f in subdir.iterdir():
                if f.is_file() and f.suffix == ".ndjson":
                    if f.stem not in retained and f.stat().st_mtime < cutoff:
                        count += 1
        summary["log_files_deleted"] = count
    else:
        from fin123.logging.sink import EventSink

        sink = EventSink(project_dir)
        deleted = sink.purge_old_logs(max_days, retained_run_ids, retained_sync_ids)
        summary["log_files_deleted"] = deleted


def _clean_orphans(project_dir: Path, summary: dict[str, Any]) -> None:
    """Remove empty directories in runs/, artifacts/, sync_runs/.

    Args:
        project_dir: Root of the fin123 project.
        summary: Mutable summary dict to update.
    """
    for subdir_name in ("runs", "artifacts", "snapshots", "sync_runs"):
        subdir = project_dir / subdir_name
        if not subdir.exists():
            continue
        for d in sorted(subdir.rglob("*"), reverse=True):
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
                summary["orphaned_cleaned"] += 1
