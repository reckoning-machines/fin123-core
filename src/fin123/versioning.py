"""Native immutable versioning for runs, artifacts, and workbook snapshots."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from fin123 import __version__
from fin123.utils.hash import sha256_dict


def _atomic_json_write(path: Path, data: Any) -> None:
    """Write JSON to a file atomically via write-to-tmp then os.replace.

    Ensures that readers never observe a partially written file.

    Args:
        path: Destination file path.
        data: JSON-serializable data.
    """
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True))
    os.replace(str(tmp_path), str(path))


def _utc_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


def _next_version(directory: Path) -> str:
    """Determine the next monotonic version string (v0001, v0002, ...).

    Args:
        directory: Parent directory containing version subdirectories.

    Returns:
        Next version string.
    """
    if not directory.exists():
        return "v0001"
    existing = sorted(
        d.name for d in directory.iterdir() if d.is_dir() and d.name.startswith("v")
    )
    if not existing:
        return "v0001"
    last_num = int(existing[-1][1:])
    return f"v{last_num + 1:04d}"


def _deterministic_sort(df: pl.DataFrame) -> pl.DataFrame:
    """Sort a DataFrame by all columns in alphabetical order.

    Applied at export time to tables that lack an explicit sort step, ensuring
    deterministic parquet output regardless of upstream evaluation order.

    Args:
        df: The DataFrame to sort.

    Returns:
        Sorted DataFrame.
    """
    if df.is_empty():
        return df
    sort_cols = sorted(df.columns)
    return df.sort(sort_cols, nulls_last=True)


class RunStore:
    """Manages the ``runs/`` directory inside a project."""

    def __init__(self, project_dir: Path) -> None:
        """Initialize the run store.

        Args:
            project_dir: Root of the fin123 project.
        """
        self.runs_dir = project_dir / "runs"
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    def create_run(
        self,
        workbook_spec: dict[str, Any],
        input_hashes: dict[str, str],
        scalar_outputs: dict[str, Any],
        table_outputs: dict[str, pl.DataFrame],
        artifact_versions: dict[str, str] | None = None,
        sorted_tables: set[str] | None = None,
        model_id: str | None = None,
        model_version_id: str | None = None,
        plugins: dict[str, dict[str, str]] | None = None,
    ) -> Path:
        """Create a new run directory with full metadata and outputs.

        Tables without an explicit sort step are sorted by all columns in
        alphabetical order at export time to guarantee deterministic parquet
        output.  The plan itself is not mutated.

        Args:
            workbook_spec: The parsed workbook YAML as a dict.
            input_hashes: Mapping of input file paths to their SHA-256 hashes.
            scalar_outputs: Computed scalar values.
            table_outputs: Computed table DataFrames.
            artifact_versions: Optional mapping of artifact names to versions used.
            sorted_tables: Set of table names that already have an explicit
                sort step.  Other tables receive a deterministic secondary sort.
            plugins: Mapping of plugin names to ``{"version": ..., "sha256": ...}``.

        Returns:
            Path to the created run directory.
        """
        sorted_tables = sorted_tables or set()
        now = _utc_now()
        run_count = len(list(self.runs_dir.iterdir())) + 1
        ts = now.strftime("%Y%m%d_%H%M%S")
        run_dir_name = f"{ts}_run_{run_count}"
        run_dir = self.runs_dir / run_dir_name
        run_dir.mkdir(parents=True)

        # Write in-progress marker so GC skips this directory
        in_progress_marker = run_dir / ".in_progress"
        in_progress_marker.write_text("")

        export_row_counts: dict[str, int] = {}
        outputs_dir = run_dir / "outputs"
        outputs_dir.mkdir()
        (outputs_dir / "scalars.json").write_text(
            json.dumps(scalar_outputs, indent=2, default=str)
        )
        auto_sorted: list[str] = []
        for table_name, df in table_outputs.items():
            if table_name not in sorted_tables:
                df = _deterministic_sort(df)
                auto_sorted.append(table_name)
            df.write_parquet(outputs_dir / f"{table_name}.parquet")
            export_row_counts[table_name] = len(df)

        run_meta = {
            "run_id": run_dir_name,
            "timestamp": now.isoformat(),
            "workbook_spec_hash": sha256_dict(workbook_spec),
            "input_hashes": input_hashes,
            "artifact_versions_used": artifact_versions or {},
            "engine_version": __version__,
            "pinned": False,
            "sorted_exports": sorted(auto_sorted),
            "export_row_counts": export_row_counts,
            "model_id": model_id,
            "model_version_id": model_version_id,
            "plugins": plugins or {},
        }
        _atomic_json_write(run_dir / "run_meta.json", run_meta)

        # Remove in-progress marker — run is now complete
        if in_progress_marker.exists():
            in_progress_marker.unlink()

        return run_dir

    def list_runs(self) -> list[dict[str, Any]]:
        """List all runs with their metadata, sorted oldest first.

        Returns:
            List of run metadata dicts.
        """
        runs = []
        for d in sorted(self.runs_dir.iterdir()):
            meta_path = d / "run_meta.json"
            if meta_path.exists():
                runs.append(json.loads(meta_path.read_text()))
        return runs

    def dir_size(self, run_dir: Path) -> int:
        """Compute the total size in bytes of a run directory.

        Args:
            run_dir: Path to the run directory.

        Returns:
            Total size in bytes.
        """
        total = 0
        for f in run_dir.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
        return total


class ArtifactStore:
    """Manages the ``artifacts/`` directory inside a project."""

    def __init__(self, project_dir: Path) -> None:
        """Initialize the artifact store.

        Args:
            project_dir: Root of the fin123 project.
        """
        self.artifacts_dir = project_dir / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def create_artifact(
        self,
        name: str,
        workflow_name: str,
        input_hash: str,
        data: dict[str, Any] | None = None,
        table: pl.DataFrame | None = None,
        model: str | None = None,
        status: str = "completed",
    ) -> tuple[str, Path]:
        """Create a new versioned artifact.

        Args:
            name: Artifact name.
            workflow_name: Name of the producing workflow.
            input_hash: Hash of the inputs that produced this artifact.
            data: Optional JSON-serializable data.
            table: Optional Polars DataFrame to persist.
            model: Optional model identifier (for AI-produced artifacts).
            status: Status string.

        Returns:
            Tuple of (version_string, artifact_version_dir).
        """
        artifact_dir = self.artifacts_dir / name
        artifact_dir.mkdir(parents=True, exist_ok=True)

        version = _next_version(artifact_dir)
        version_dir = artifact_dir / version
        version_dir.mkdir()

        now = _utc_now()
        meta = {
            "artifact_name": name,
            "version": version,
            "created_at": now.isoformat(),
            "input_hash": input_hash,
            "workflow_name": workflow_name,
            "status": status,
            "model": model,
            "pinned": False,
        }
        (version_dir / "meta.json").write_text(json.dumps(meta, indent=2))

        if data is not None:
            (version_dir / "artifact.json").write_text(
                json.dumps(data, indent=2, default=str)
            )

        if table is not None:
            table.write_parquet(version_dir / "table.parquet")

        return version, version_dir

    def list_artifacts(self) -> dict[str, list[dict[str, Any]]]:
        """List all artifacts grouped by name.

        Returns:
            Dict mapping artifact names to lists of version metadata.
        """
        result: dict[str, list[dict[str, Any]]] = {}
        if not self.artifacts_dir.exists():
            return result
        for artifact_dir in sorted(self.artifacts_dir.iterdir()):
            if not artifact_dir.is_dir():
                continue
            versions = []
            for version_dir in sorted(artifact_dir.iterdir()):
                meta_path = version_dir / "meta.json"
                if meta_path.exists():
                    versions.append(json.loads(meta_path.read_text()))
            if versions:
                result[artifact_dir.name] = versions
        return result

    def latest_version(self, name: str) -> dict[str, Any] | None:
        """Get metadata for the latest version of an artifact.

        Args:
            name: Artifact name.

        Returns:
            Metadata dict, or None if no versions exist.
        """
        artifact_dir = self.artifacts_dir / name
        if not artifact_dir.exists():
            return None
        versions = sorted(d for d in artifact_dir.iterdir() if d.is_dir())
        if not versions:
            return None
        meta_path = versions[-1] / "meta.json"
        if meta_path.exists():
            return json.loads(meta_path.read_text())
        return None

    def _load_meta(self, name: str, version: str) -> tuple[dict[str, Any], Path]:
        """Load artifact meta.json for a specific version.

        Returns:
            Tuple of (meta_dict, meta_path).

        Raises:
            FileNotFoundError: If the artifact version does not exist.
        """
        meta_path = self.artifacts_dir / name / version / "meta.json"
        if not meta_path.exists():
            raise FileNotFoundError(
                f"Artifact {name!r} version {version!r} not found"
            )
        return json.loads(meta_path.read_text()), meta_path

    def approve_artifact(
        self,
        name: str,
        version: str,
        approved_by: str = "",
        note: str = "",
    ) -> dict[str, Any]:
        """Approve an artifact version.

        Idempotent: approving an already-approved artifact preserves the
        original ``approved_at`` timestamp. Approving a rejected artifact
        updates status to approved with a new timestamp.

        Args:
            name: Artifact name.
            version: Version string (e.g. 'v0001').
            approved_by: Identifier of the approver.
            note: Optional free-text note.

        Returns:
            The updated approval object.
        """
        meta, meta_path = self._load_meta(name, version)
        existing = meta.get("approval", {})

        if existing.get("status") == "approved":
            # Idempotent: no-op, preserve existing timestamp
            return existing

        now = _utc_now()
        approval = {
            "status": "approved",
            "approved_by": approved_by or None,
            "approved_at": now.isoformat(),
            "note": note,
            "reason_code": None,
        }
        meta["approval"] = approval
        _atomic_json_write(meta_path, meta)
        return approval

    def reject_artifact(
        self,
        name: str,
        version: str,
        approved_by: str = "",
        note: str = "",
        reason_code: str = "",
    ) -> dict[str, Any]:
        """Reject an artifact version.

        Idempotent: rejecting an already-rejected artifact preserves the
        original ``approved_at`` timestamp.

        Args:
            name: Artifact name.
            version: Version string (e.g. 'v0001').
            approved_by: Identifier of the rejector.
            note: Optional free-text note.
            reason_code: Optional machine-readable rejection reason.

        Returns:
            The updated approval object.
        """
        meta, meta_path = self._load_meta(name, version)
        existing = meta.get("approval", {})

        if existing.get("status") == "rejected":
            # Idempotent: no-op, preserve existing timestamp
            return existing

        now = _utc_now()
        approval = {
            "status": "rejected",
            "approved_by": approved_by or None,
            "approved_at": now.isoformat(),
            "note": note,
            "reason_code": reason_code or None,
        }
        meta["approval"] = approval
        _atomic_json_write(meta_path, meta)
        return approval

    def get_artifact_approval(
        self,
        name: str,
        version: str,
    ) -> dict[str, Any]:
        """Get the approval status of an artifact version.

        Returns a synthetic ``approved`` approval if no approval key exists
        (backward compatibility).

        Args:
            name: Artifact name.
            version: Version string.

        Returns:
            Approval object dict.
        """
        meta, _ = self._load_meta(name, version)
        approval = meta.get("approval")
        if approval is None:
            # Backward compat: no approval key means approved
            return {
                "status": "approved",
                "approved_by": None,
                "approved_at": None,
                "note": "",
                "reason_code": None,
            }
        return approval


class SnapshotStore:
    """Manages workbook spec snapshots in ``snapshots/workbook/``."""

    def __init__(self, project_dir: Path) -> None:
        """Initialize the snapshot store.

        Args:
            project_dir: Root of the fin123 project.
        """
        self.project_dir = project_dir
        self.snapshot_dir = project_dir / "snapshots" / "workbook"
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def save_snapshot(self, workbook_yaml: str) -> str:
        """Save a workbook YAML snapshot and update the index.

        Args:
            workbook_yaml: Raw YAML content of the workbook spec.

        Returns:
            The version string assigned to this snapshot.
        """
        version = _next_version(self.snapshot_dir)
        version_dir = self.snapshot_dir / version
        version_dir.mkdir()
        (version_dir / "workbook.yaml").write_text(workbook_yaml)

        # Update index.json
        spec_dict = yaml.safe_load(workbook_yaml) or {}
        content_hash = sha256_dict(spec_dict)
        now = _utc_now()

        index = self.load_index()
        # Guard against duplicate entries (rebuild_index may have found
        # the version dir we just created above)
        existing_ids = {v["model_version_id"] for v in index["versions"]}
        if version not in existing_ids:
            index["versions"].append({
                "model_version_id": version,
                "created_at": now.isoformat(),
                "hash": content_hash,
                "pinned": False,
            })
        self._write_index(index)

        return version

    def load_index(self) -> dict:
        """Read index.json or rebuild from disk if missing.

        Returns:
            Index dict with model_id and versions list.
        """
        index_path = self.snapshot_dir / "index.json"
        if index_path.exists():
            try:
                return json.loads(index_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass

        # Rebuild from disk
        spec_path = self.project_dir / "workbook.yaml"
        model_id = None
        if spec_path.exists():
            spec = yaml.safe_load(spec_path.read_text()) or {}
            model_id = spec.get("model_id")
        return self.rebuild_index(model_id or "unknown")

    def rebuild_index(self, model_id: str) -> dict:
        """Scan disk and rebuild index.json (migration path).

        Args:
            model_id: The model_id to use in the index.

        Returns:
            Rebuilt index dict.
        """
        versions = []
        if self.snapshot_dir.exists():
            for d in sorted(self.snapshot_dir.iterdir()):
                if not d.is_dir() or not d.name.startswith("v"):
                    continue
                wb_path = d / "workbook.yaml"
                content_hash = ""
                created_at = ""
                if wb_path.exists():
                    spec = yaml.safe_load(wb_path.read_text()) or {}
                    content_hash = sha256_dict(spec)
                    stat = wb_path.stat()
                    created_at = datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).isoformat()
                versions.append({
                    "model_version_id": d.name,
                    "created_at": created_at,
                    "hash": content_hash,
                    "pinned": False,
                })

        index = {"model_id": model_id, "versions": versions}
        self._write_index(index)
        return index

    def load_version(self, version: str) -> dict:
        """Read and parse a specific snapshot's workbook.yaml.

        Args:
            version: Version string (e.g. 'v0001').

        Returns:
            Parsed workbook spec dict.

        Raises:
            FileNotFoundError: If the version does not exist.
        """
        wb_path = self.snapshot_dir / version / "workbook.yaml"
        if not wb_path.exists():
            raise FileNotFoundError(f"Snapshot version {version!r} not found")
        return yaml.safe_load(wb_path.read_text()) or {}

    def list_versions(self) -> list[str]:
        """Return sorted version strings from the index.

        Returns:
            List of version strings (e.g. ['v0001', 'v0002']).
        """
        index = self.load_index()
        return [v["model_version_id"] for v in index["versions"]]

    def pin_version(self, version: str) -> None:
        """Mark a version as pinned in the index.

        Args:
            version: Version string to pin.
        """
        index = self.load_index()
        for v in index["versions"]:
            if v["model_version_id"] == version:
                v["pinned"] = True
                break
        self._write_index(index)

    def unpin_version(self, version: str) -> None:
        """Unpin a version in the index.

        Args:
            version: Version string to unpin.
        """
        index = self.load_index()
        for v in index["versions"]:
            if v["model_version_id"] == version:
                v["pinned"] = False
                break
        self._write_index(index)

    def _write_index(self, index: dict) -> None:
        """Atomic write of index.json (write to .tmp then rename).

        Args:
            index: Index dict to write.
        """
        index_path = self.snapshot_dir / "index.json"
        tmp_path = self.snapshot_dir / "index.json.tmp"
        tmp_path.write_text(json.dumps(index, indent=2))
        os.replace(str(tmp_path), str(index_path))
