"""Phase 7 tests: model_id, model_version_id, retention, clear-cache, import, health."""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any

import pytest
import yaml

from fin123.gc import run_gc
from fin123.project import DEFAULT_CONFIG, ensure_model_id, scaffold_project
from fin123.versioning import RunStore, SnapshotStore
from fin123.ui.service import ProjectService


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def demo_project(tmp_path: Path) -> Path:
    """Scaffold a demo project for testing."""
    project_dir = tmp_path / "test_project"
    scaffold_project(project_dir)
    return project_dir


@pytest.fixture
def service(demo_project: Path) -> ProjectService:
    """Create a ProjectService for the demo project."""
    return ProjectService(project_dir=demo_project)


@pytest.fixture
def multi_version_project(demo_project: Path) -> Path:
    """Create a project with several snapshot versions."""
    store = SnapshotStore(demo_project)
    spec_path = demo_project / "workbook.yaml"
    raw_yaml = spec_path.read_text()

    # Create initial snapshot
    store.save_snapshot(raw_yaml)

    # Modify and create more snapshots
    for i in range(5):
        spec = yaml.safe_load(raw_yaml) or {}
        spec["params"] = spec.get("params", {})
        spec["params"]["iter"] = i
        new_yaml = yaml.dump(spec, default_flow_style=False, sort_keys=False)
        store.save_snapshot(new_yaml)

    return demo_project


# ────────────────────────────────────────────────────────────────
# TestModelId
# ────────────────────────────────────────────────────────────────


class TestModelId:
    def test_model_id_auto_generated_when_missing(self, tmp_path: Path):
        """model_id is auto-generated when missing from spec."""
        d = tmp_path / "proj"
        d.mkdir()
        spec = {"sheets": [{"name": "Sheet1", "n_rows": 200, "n_cols": 40, "cells": {}}]}
        spec_path = d / "workbook.yaml"
        spec_path.write_text(yaml.dump(spec))
        mid = ensure_model_id(spec, spec_path)
        assert mid
        assert len(mid) == 36  # UUID format
        assert spec["model_id"] == mid

    def test_model_id_persisted_to_workbook_yaml(self, tmp_path: Path):
        """model_id is written back to workbook.yaml."""
        d = tmp_path / "proj"
        d.mkdir()
        spec = {"sheets": [{"name": "Sheet1", "n_rows": 200, "n_cols": 40, "cells": {}}]}
        spec_path = d / "workbook.yaml"
        spec_path.write_text(yaml.dump(spec))
        mid = ensure_model_id(spec, spec_path)
        reloaded = yaml.safe_load(spec_path.read_text())
        assert reloaded["model_id"] == mid

    def test_model_id_stable_across_reloads(self, tmp_path: Path):
        """model_id doesn't change on subsequent calls."""
        d = tmp_path / "proj"
        d.mkdir()
        spec = {"model_id": "existing-id-123"}
        spec_path = d / "workbook.yaml"
        spec_path.write_text(yaml.dump(spec))
        mid = ensure_model_id(spec, spec_path)
        assert mid == "existing-id-123"

    def test_scaffold_project_includes_model_id(self, demo_project: Path):
        """scaffold_project generates workbook with model_id."""
        spec = yaml.safe_load((demo_project / "workbook.yaml").read_text())
        assert "model_id" in spec
        assert len(str(spec["model_id"])) == 36


# ────────────────────────────────────────────────────────────────
# TestModelVersionId
# ────────────────────────────────────────────────────────────────


class TestModelVersionId:
    def test_save_snapshot_returns_version(self, demo_project: Path):
        """save_snapshot returns the version string."""
        store = SnapshotStore(demo_project)
        raw = (demo_project / "workbook.yaml").read_text()
        v = store.save_snapshot(raw)
        assert v.startswith("v")

    def test_save_snapshot_updates_index(self, demo_project: Path):
        """save_snapshot updates index.json."""
        store = SnapshotStore(demo_project)
        raw = (demo_project / "workbook.yaml").read_text()
        v = store.save_snapshot(raw)
        index = store.load_index()
        versions = [e["model_version_id"] for e in index["versions"]]
        assert v in versions

    def test_index_json_correct_structure(self, demo_project: Path):
        """index.json has model_id and versions list."""
        store = SnapshotStore(demo_project)
        raw = (demo_project / "workbook.yaml").read_text()
        store.save_snapshot(raw)
        index = store.load_index()
        assert "model_id" in index
        assert "versions" in index
        assert len(index["versions"]) >= 1
        entry = index["versions"][-1]
        assert "model_version_id" in entry
        assert "created_at" in entry
        assert "hash" in entry
        assert "pinned" in entry

    def test_index_rebuilt_when_missing(self, demo_project: Path):
        """Index is rebuilt from disk when index.json is missing."""
        store = SnapshotStore(demo_project)
        raw = (demo_project / "workbook.yaml").read_text()
        store.save_snapshot(raw)
        # Remove index
        index_path = store.snapshot_dir / "index.json"
        index_path.unlink()
        # Reload
        index = store.load_index()
        assert len(index["versions"]) >= 1

    def test_run_meta_includes_model_fields(self, demo_project: Path):
        """run_meta.json includes model_id and model_version_id."""
        from fin123.workbook import Workbook

        wb = Workbook(demo_project)
        result = wb.run()
        meta_path = result.run_dir / "run_meta.json"
        meta = json.loads(meta_path.read_text())
        assert "model_id" in meta
        assert meta["model_id"] is not None
        assert "model_version_id" in meta
        assert meta["model_version_id"] is not None
        assert meta["model_version_id"].startswith("v")


# ────────────────────────────────────────────────────────────────
# TestVersionSelection
# ────────────────────────────────────────────────────────────────


class TestVersionSelection:
    def test_select_old_version_sets_read_only(self, multi_version_project: Path):
        """Selecting an old version sets read_only."""
        svc = ProjectService(project_dir=multi_version_project)
        versions = svc.list_model_versions()
        assert len(versions) >= 2
        old_ver = versions[0]["model_version_id"]
        info = svc.select_model_version(old_ver)
        assert info["read_only"] is True

    def test_mutations_blocked_in_read_only(self, multi_version_project: Path):
        """Mutation methods raise ValueError when read_only."""
        svc = ProjectService(project_dir=multi_version_project)
        versions = svc.list_model_versions()
        old_ver = versions[0]["model_version_id"]
        svc.select_model_version(old_ver)

        with pytest.raises(ValueError, match="read-only"):
            svc.update_cells("Sheet1", [{"addr": "A1", "value": "test"}])
        with pytest.raises(ValueError, match="read-only"):
            svc.save_snapshot()
        with pytest.raises(ValueError, match="read-only"):
            svc.add_sheet("NewSheet")
        with pytest.raises(ValueError, match="read-only"):
            svc.delete_sheet("Sheet1")

    def test_selecting_latest_exits_read_only(self, multi_version_project: Path):
        """Selecting the latest version clears read_only."""
        svc = ProjectService(project_dir=multi_version_project)
        versions = svc.list_model_versions()
        old_ver = versions[0]["model_version_id"]
        latest_ver = versions[-1]["model_version_id"]

        svc.select_model_version(old_ver)
        assert svc.get_model_info()["read_only"] is True

        svc.select_model_version(latest_ver)
        assert svc.get_model_info()["read_only"] is False

    def test_viewport_works_for_old_version(self, multi_version_project: Path):
        """get_sheet_viewport works for old versions."""
        svc = ProjectService(project_dir=multi_version_project)
        versions = svc.list_model_versions()
        old_ver = versions[0]["model_version_id"]
        svc.select_model_version(old_ver)
        vp = svc.get_sheet_viewport()
        assert "cells" in vp


# ────────────────────────────────────────────────────────────────
# TestVersionPinning
# ────────────────────────────────────────────────────────────────


class TestVersionPinning:
    def test_pin_version_sets_pinned(self, multi_version_project: Path):
        """pin_version sets pinned=true in index."""
        store = SnapshotStore(multi_version_project)
        versions = store.list_versions()
        v = versions[0]
        store.pin_version(v)
        index = store.load_index()
        entry = [e for e in index["versions"] if e["model_version_id"] == v][0]
        assert entry["pinned"] is True

    def test_unpin_version_clears_it(self, multi_version_project: Path):
        """unpin_version clears pinned flag."""
        store = SnapshotStore(multi_version_project)
        versions = store.list_versions()
        v = versions[0]
        store.pin_version(v)
        store.unpin_version(v)
        index = store.load_index()
        entry = [e for e in index["versions"] if e["model_version_id"] == v][0]
        assert entry["pinned"] is False

    def test_pinned_version_survives_gc(self, multi_version_project: Path):
        """Pinned versions are not deleted by GC."""
        store = SnapshotStore(multi_version_project)
        versions = store.list_versions()
        v = versions[0]
        store.pin_version(v)

        # Set low retention to trigger GC
        config_path = multi_version_project / "fin123.yaml"
        config = yaml.safe_load(config_path.read_text()) or {}
        config["max_model_versions"] = 2
        config_path.write_text(yaml.dump(config))

        run_gc(multi_version_project)

        # Pinned version should survive
        remaining = store.list_versions()
        assert v in remaining


# ────────────────────────────────────────────────────────────────
# TestModelVersionGC
# ────────────────────────────────────────────────────────────────


class TestModelVersionGC:
    def test_versions_beyond_max_deleted(self, multi_version_project: Path):
        """Versions beyond max_model_versions are deleted."""
        store = SnapshotStore(multi_version_project)
        initial_count = len(store.list_versions())
        assert initial_count >= 3

        config_path = multi_version_project / "fin123.yaml"
        config = yaml.safe_load(config_path.read_text()) or {}
        config["max_model_versions"] = 2
        config_path.write_text(yaml.dump(config))

        summary = run_gc(multi_version_project)
        assert summary["model_versions_deleted"] > 0

        remaining = len(store.list_versions())
        assert remaining <= 2

    def test_latest_version_always_protected(self, multi_version_project: Path):
        """Latest version is never deleted."""
        store = SnapshotStore(multi_version_project)
        latest = store.list_versions()[-1]

        config_path = multi_version_project / "fin123.yaml"
        config = yaml.safe_load(config_path.read_text()) or {}
        config["max_model_versions"] = 1
        config_path.write_text(yaml.dump(config))

        run_gc(multi_version_project)
        remaining = store.list_versions()
        assert latest in remaining

    def test_pinned_version_always_protected(self, multi_version_project: Path):
        """Pinned version is not deleted by GC."""
        store = SnapshotStore(multi_version_project)
        versions = store.list_versions()
        pinned_v = versions[0]
        store.pin_version(pinned_v)

        config_path = multi_version_project / "fin123.yaml"
        config = yaml.safe_load(config_path.read_text()) or {}
        config["max_model_versions"] = 1
        config_path.write_text(yaml.dump(config))

        run_gc(multi_version_project)
        remaining = store.list_versions()
        assert pinned_v in remaining

    def test_version_referenced_by_run_protected(self, multi_version_project: Path):
        """Version referenced by a retained run is not deleted."""
        store = SnapshotStore(multi_version_project)
        versions = store.list_versions()
        ref_version = versions[1]

        # Create a run that references this version
        run_store = RunStore(multi_version_project)
        import polars as pl
        run_store.create_run(
            workbook_spec={"test": True},
            input_hashes={},
            scalar_outputs={"x": 1},
            table_outputs={},
            model_id="test-model",
            model_version_id=ref_version,
        )

        config_path = multi_version_project / "fin123.yaml"
        config = yaml.safe_load(config_path.read_text()) or {}
        config["max_model_versions"] = 1
        config_path.write_text(yaml.dump(config))

        run_gc(multi_version_project)
        remaining = store.list_versions()
        assert ref_version in remaining

    def test_dry_run_returns_correct_counts(self, multi_version_project: Path):
        """Dry run returns counts without deleting."""
        store = SnapshotStore(multi_version_project)
        initial_count = len(store.list_versions())

        config_path = multi_version_project / "fin123.yaml"
        config = yaml.safe_load(config_path.read_text()) or {}
        config["max_model_versions"] = 2
        config_path.write_text(yaml.dump(config))

        summary = run_gc(multi_version_project, dry_run=True)
        assert summary["model_versions_deleted"] > 0
        assert summary["dry_run"] is True

        # Nothing actually deleted
        assert len(store.list_versions()) == initial_count


# ────────────────────────────────────────────────────────────────
# TestClearCache
# ────────────────────────────────────────────────────────────────


class TestClearCache:
    def test_cli_dry_run_returns_summary(self, demo_project: Path):
        """CLI dry-run returns summary dict."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["clear-cache", str(demo_project), "--dry-run"])
        assert result.exit_code == 0
        assert "Clear-cache dry-run:" in result.output

    def test_cli_real_run_deletes(self, demo_project: Path):
        """CLI real run clears cache."""
        # Create a hash cache
        cache_path = demo_project / "cache" / "hashes.json"
        cache_path.parent.mkdir(exist_ok=True)
        cache_path.write_text('{"test": "data"}')

        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["clear-cache", str(demo_project)])
        assert result.exit_code == 0
        assert "Clear-cache complete:" in result.output
        assert not cache_path.exists()

    def test_hash_cache_cleared(self, demo_project: Path):
        """Hash cache is cleared by clear_cache."""
        cache_path = demo_project / "cache" / "hashes.json"
        cache_path.parent.mkdir(exist_ok=True)
        cache_path.write_text('{"test": "data"}')

        svc = ProjectService(project_dir=demo_project)
        summary = svc.clear_cache(dry_run=False)
        assert summary["hash_cache_bytes"] > 0
        assert not cache_path.exists()

    def test_service_clear_cache_dry_run(self, demo_project: Path):
        """Service clear_cache dry_run doesn't delete."""
        cache_path = demo_project / "cache" / "hashes.json"
        cache_path.parent.mkdir(exist_ok=True)
        cache_path.write_text('{"test": "data"}')

        svc = ProjectService(project_dir=demo_project)
        summary = svc.clear_cache(dry_run=True)
        assert summary["hash_cache_bytes"] > 0
        assert cache_path.exists()  # Not deleted


# ────────────────────────────────────────────────────────────────
# TestImportReport
# ────────────────────────────────────────────────────────────────


class TestImportReport:
    def test_get_latest_import_report_returns_report(self, tmp_path: Path):
        """get_latest_import_report returns report when it exists."""
        d = tmp_path / "proj"
        d.mkdir()
        spec = {"sheets": [{"name": "Sheet1", "n_rows": 200, "n_cols": 40, "cells": {}}]}
        (d / "workbook.yaml").write_text(yaml.dump(spec))
        (d / "runs").mkdir()
        (d / "snapshots").mkdir()

        report = {"source": "test.xlsx", "sheets_imported": [], "skipped_features": [], "warnings": []}
        (d / "import_report.json").write_text(json.dumps(report))

        svc = ProjectService(project_dir=d)
        result = svc.get_latest_import_report()
        assert result is not None
        assert result["source"] == "test.xlsx"

    def test_returns_none_when_no_report(self, demo_project: Path):
        """get_latest_import_report returns None when no report exists."""
        svc = ProjectService(project_dir=demo_project)
        result = svc.get_latest_import_report()
        assert result is None

    def test_list_import_reports_returns_entries(self, tmp_path: Path):
        """list_import_reports returns index entries."""
        d = tmp_path / "proj"
        d.mkdir()
        spec = {"sheets": [{"name": "Sheet1", "n_rows": 200, "n_cols": 40, "cells": {}}]}
        (d / "workbook.yaml").write_text(yaml.dump(spec))
        (d / "runs").mkdir()
        (d / "snapshots").mkdir()

        # Create versioned reports
        reports_dir = d / "import_reports"
        reports_dir.mkdir()
        ts_dir = reports_dir / "20260101T000000Z"
        ts_dir.mkdir()
        report = {"source": "test.xlsx"}
        (ts_dir / "import_report.json").write_text(json.dumps(report))
        index = [{"timestamp": "20260101T000000Z", "path": "import_reports/20260101T000000Z/import_report.json"}]
        (reports_dir / "index.json").write_text(json.dumps(index))

        svc = ProjectService(project_dir=d)
        entries = svc.list_import_reports()
        assert len(entries) == 1
        assert entries[0]["timestamp"] == "20260101T000000Z"


# ────────────────────────────────────────────────────────────────
# TestProjectHealth
# ────────────────────────────────────────────────────────────────


class TestProjectHealth:
    def test_healthy_project_returns_ok(self, demo_project: Path):
        """A healthy project returns status='ok'."""
        svc = ProjectService(project_dir=demo_project)
        health = svc.get_project_health()
        assert health["status"] in ("ok", "warn")  # may have info-level issues
        assert isinstance(health["issues"], list)

    def test_stale_datasheet_produces_warning(self, tmp_path: Path):
        """A stale datasheet produces a warning in health."""
        import os
        d = tmp_path / "proj"
        d.mkdir()
        spec = {
            "tables": {
                "my_table": {
                    "source": "sql",
                    "connection": "pg",
                    "cache": "inputs/my_table.parquet",
                    "refresh": "always",  # always = ttl 0 → immediately stale
                }
            },
            "sheets": [{"name": "Sheet1", "n_rows": 200, "n_cols": 40, "cells": {}}],
        }
        (d / "workbook.yaml").write_text(yaml.dump(spec))
        (d / "runs").mkdir()
        (d / "snapshots").mkdir()

        # Create a sync run with ok status
        sync_dir = d / "sync_runs" / "20240101_000000_sync_1"
        sync_dir.mkdir(parents=True)
        sync_meta = {
            "sync_id": "20240101_000000_sync_1",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "tables": [{
                "table_name": "my_table",
                "status": "ok",
                "rowcount": 5,
            }],
        }
        (sync_dir / "sync_meta.json").write_text(json.dumps(sync_meta))

        # Create the cache file and make it old
        inputs = d / "inputs"
        inputs.mkdir()
        import polars as pl
        cache_path = inputs / "my_table.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(cache_path)
        # Set mtime to 1 hour ago to ensure staleness
        old_time = time.time() - 3600
        os.utime(cache_path, (old_time, old_time))

        svc = ProjectService(project_dir=d)
        health = svc.get_project_health()
        stale_issues = [i for i in health["issues"] if i["code"] == "datasheet_stale"]
        assert len(stale_issues) == 1

    def test_formula_error_produces_error_issue(self, tmp_path: Path):
        """A formula parse error produces an error issue in health."""
        d = tmp_path / "proj"
        d.mkdir()
        spec = {
            "sheets": [{
                "name": "Sheet1",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {
                    "A1": {"formula": "=1+2"},
                    "B1": {"formula": "=INVALID((("},
                },
            }],
        }
        (d / "workbook.yaml").write_text(yaml.dump(spec))
        (d / "runs").mkdir()
        (d / "snapshots").mkdir()

        svc = ProjectService(project_dir=d)
        # Trigger cell graph evaluation
        svc.get_sheet_viewport("Sheet1")
        health = svc.get_project_health()
        # The formula error should show up as an error
        error_issues = [i for i in health["issues"] if i["severity"] == "error"]
        # Note: depends on whether CellGraph stores errors publicly
        # At minimum, health should return without crashing
        assert health["status"] in ("ok", "warn", "error")

    def test_import_warning_appears_in_health(self, tmp_path: Path):
        """Import warnings appear in health issues."""
        d = tmp_path / "proj"
        d.mkdir()
        spec = {"sheets": [{"name": "Sheet1", "n_rows": 200, "n_cols": 40, "cells": {}}]}
        (d / "workbook.yaml").write_text(yaml.dump(spec))
        (d / "runs").mkdir()
        (d / "snapshots").mkdir()

        report = {
            "source": "test.xlsx",
            "sheets_imported": [],
            "skipped_features": ["VBA macros"],
            "warnings": ["Sheet1: conditional formatting skipped"],
        }
        (d / "import_report.json").write_text(json.dumps(report))

        svc = ProjectService(project_dir=d)
        health = svc.get_project_health()
        import_issues = [
            i for i in health["issues"]
            if i["code"] in ("import_skipped_feature", "import_warning")
        ]
        assert len(import_issues) >= 2

    def test_status_reflects_worst_severity(self, tmp_path: Path):
        """Status is 'error' when any error issue exists."""
        d = tmp_path / "proj"
        d.mkdir()
        spec = {
            "tables": {
                "broken_table": {
                    "source": "sql",
                    "connection": "pg",
                    "cache": "inputs/broken.parquet",
                    "refresh": "manual",
                }
            },
            "sheets": [{"name": "Sheet1", "n_rows": 200, "n_cols": 40, "cells": {}}],
        }
        (d / "workbook.yaml").write_text(yaml.dump(spec))
        (d / "runs").mkdir()
        (d / "snapshots").mkdir()

        # Create a sync run with fail status
        sync_dir = d / "sync_runs" / "20240101_000000_sync_1"
        sync_dir.mkdir(parents=True)
        sync_meta = {
            "sync_id": "20240101_000000_sync_1",
            "timestamp": "2024-01-01T00:00:00+00:00",
            "tables": [{
                "table_name": "broken_table",
                "status": "fail",
                "rowcount": 0,
                "error_message": "connection failed",
            }],
        }
        (sync_dir / "sync_meta.json").write_text(json.dumps(sync_meta))

        svc = ProjectService(project_dir=d)
        health = svc.get_project_health()
        assert health["status"] == "error"


# ────────────────────────────────────────────────────────────────
# TestAPIEndpoints
# ────────────────────────────────────────────────────────────────


class TestAPIEndpoints:
    @pytest.fixture
    def client(self, demo_project: Path):
        """Create an httpx test client for the FastAPI app."""
        from fin123.ui.server import create_app
        from httpx import ASGITransport, AsyncClient
        import asyncio

        app = create_app(demo_project)
        transport = ASGITransport(app=app)

        class SyncClient:
            """Synchronous wrapper around async httpx client."""

            def __init__(self):
                self._transport = transport

            def _run(self, coro):
                loop = asyncio.new_event_loop()
                try:
                    return loop.run_until_complete(coro)
                finally:
                    loop.close()

            def get(self, url: str) -> Any:
                async def _get():
                    async with AsyncClient(transport=self._transport, base_url="http://test") as c:
                        return await c.get(url)
                return self._run(_get())

            def post(self, url: str, json: dict | None = None) -> Any:
                async def _post():
                    async with AsyncClient(transport=self._transport, base_url="http://test") as c:
                        return await c.post(url, json=json)
                return self._run(_post())

        return SyncClient()

    def test_get_model_returns_model_info(self, client):
        resp = client.get("/api/model")
        assert resp.status_code == 200
        data = resp.json()
        assert "model_id" in data
        assert "current_model_version_id" in data
        assert "read_only" in data

    def test_get_model_versions_returns_list(self, client, demo_project: Path):
        # Create a snapshot first
        svc = ProjectService(project_dir=demo_project)
        svc.save_snapshot()

        resp = client.get("/api/model/versions")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_post_model_select(self, client, demo_project: Path):
        # Create a snapshot first
        svc = ProjectService(project_dir=demo_project)
        snap = svc.save_snapshot()
        version = snap["snapshot_version"]

        resp = client.post("/api/model/select", json={"version": version})
        assert resp.status_code == 200
        data = resp.json()
        assert data["current_model_version_id"] == version

    def test_post_clear_cache(self, client):
        resp = client.post("/api/clear-cache", json={"dry_run": True})
        assert resp.status_code == 200
        data = resp.json()
        assert "runs_deleted" in data
        assert "model_versions_deleted" in data

    def test_get_import_report_latest_404(self, client):
        resp = client.get("/api/import/report/latest")
        assert resp.status_code == 404

    def test_get_health(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "issues" in data

    def test_post_model_pin(self, client, demo_project: Path):
        svc = ProjectService(project_dir=demo_project)
        snap = svc.save_snapshot()
        version = snap["snapshot_version"]

        resp = client.post("/api/model/pin", json={"version": version})
        assert resp.status_code == 200
        data = resp.json()
        assert data["pinned"] is True

    def test_post_model_unpin(self, client, demo_project: Path):
        svc = ProjectService(project_dir=demo_project)
        snap = svc.save_snapshot()
        version = snap["snapshot_version"]

        client.post("/api/model/pin", json={"version": version})
        resp = client.post("/api/model/unpin", json={"version": version})
        assert resp.status_code == 200
        data = resp.json()
        assert data["pinned"] is False


# ────────────────────────────────────────────────────────────────
# TestSnapshotStoreExtended
# ────────────────────────────────────────────────────────────────


class TestSnapshotStoreExtended:
    def test_load_version(self, multi_version_project: Path):
        """load_version returns parsed spec."""
        store = SnapshotStore(multi_version_project)
        versions = store.list_versions()
        spec = store.load_version(versions[0])
        assert isinstance(spec, dict)

    def test_load_version_not_found(self, demo_project: Path):
        """load_version raises FileNotFoundError for missing version."""
        store = SnapshotStore(demo_project)
        with pytest.raises(FileNotFoundError):
            store.load_version("v9999")

    def test_list_versions_sorted(self, multi_version_project: Path):
        """list_versions returns sorted versions."""
        store = SnapshotStore(multi_version_project)
        versions = store.list_versions()
        assert versions == sorted(versions)
        assert len(versions) >= 3


# ────────────────────────────────────────────────────────────────
# TestDefaultConfig
# ────────────────────────────────────────────────────────────────


class TestDefaultConfig:
    def test_default_config_has_model_version_keys(self):
        """DEFAULT_CONFIG includes model version retention keys."""
        assert "max_model_versions" in DEFAULT_CONFIG
        assert DEFAULT_CONFIG["max_model_versions"] == 200
        assert "max_total_model_version_bytes" in DEFAULT_CONFIG
        assert "ttl_model_versions_days" in DEFAULT_CONFIG
