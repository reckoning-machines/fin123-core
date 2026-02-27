"""FastAPI server for the fin123 local browser UI.

Routes are thin wrappers over the shared :class:`ProjectService`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from fin123.ui.service import ProjectService, import_xlsx_upload
from fin123.ui.view_transforms import TableViewRequest

# The singleton service is set at startup by ``create_app()``.
_service: ProjectService | None = None


def create_app(project_dir: Path) -> FastAPI:
    """Create the FastAPI application for a given project.

    Args:
        project_dir: Root of the fin123 project.

    Returns:
        Configured FastAPI instance.
    """
    global _service
    _service = ProjectService(project_dir=project_dir)

    from fin123 import __version__

    app = FastAPI(title="fin123 UI", version=__version__)

    # Mount static files
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # Register routes
    app.include_router(_api_router())

    # Serve index.html at root
    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(str(static_dir / "index.html"))

    return app


def _svc() -> ProjectService:
    """Get the singleton service, raising if not initialised."""
    if _service is None:
        raise HTTPException(500, "Service not initialised")
    return _service


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------


class CellEdit(BaseModel):
    addr: str
    value: str | None = None
    formula: str | None = None


class CellUpdateRequest(BaseModel):
    sheet: str = "Sheet1"
    edits: list[CellEdit]


class SyncRequest(BaseModel):
    table_name: str | None = None


class ValidateFormulaRequest(BaseModel):
    text: str


class WorkflowRunRequest(BaseModel):
    workflow_name: str


class AddSheetRequest(BaseModel):
    name: str


class RenameSheetRequest(BaseModel):
    old_name: str
    new_name: str


class DeleteSheetRequest(BaseModel):
    name: str


class FormatUpdate(BaseModel):
    addr: str
    color: str | None = None


class CellFormatRequest(BaseModel):
    sheet: str = "Sheet1"
    updates: list[FormatUpdate]


class NameRequest(BaseModel):
    name: str
    sheet: str
    start: str
    end: str


class NameUpdateRequest(BaseModel):
    sheet: str | None = None
    start: str | None = None
    end: str | None = None


class SelectVersionRequest(BaseModel):
    version: str


class PinVersionRequest(BaseModel):
    version: str


class ClearCacheRequest(BaseModel):
    dry_run: bool = True


class ImportTodoRequest(BaseModel):
    sheet: str
    addr: str


class ImportConvertRequest(BaseModel):
    sheet: str
    addr: str


class RegistryPushRequest(BaseModel):
    versions: list[str] | None = None


class RegistryPullRequest(BaseModel):
    model_id: str
    version: str


class UnbindParamRequest(BaseModel):
    sheet: str
    addr: str


class VerifyRunRequest(BaseModel):
    run_id: str


class RowInsertRequest(BaseModel):
    sheet: str = "Sheet1"
    row_idx: int
    count: int = 1


class RowDeleteRequest(BaseModel):
    sheet: str = "Sheet1"
    row_idx: int
    count: int = 1


class ColInsertRequest(BaseModel):
    sheet: str = "Sheet1"
    col_idx: int
    count: int = 1


class ColDeleteRequest(BaseModel):
    sheet: str = "Sheet1"
    col_idx: int
    count: int = 1


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------


def _api_router():
    from fastapi import APIRouter

    router = APIRouter(prefix="/api")

    # -- Project info --

    @router.get("/project")
    async def get_project() -> dict[str, Any]:
        return _svc().get_project_info()

    # -- Sheet --

    @router.get("/sheet")
    async def get_sheet(
        sheet: str = Query("Sheet1"),
        r0: int = Query(0, ge=0),
        c0: int = Query(0, ge=0),
        rows: int = Query(30, ge=1, le=500),
        cols: int = Query(15, ge=1, le=200),
    ) -> dict[str, Any]:
        try:
            return _svc().get_sheet_viewport(sheet, r0, c0, rows, cols)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @router.post("/sheet/cells")
    async def update_cells(req: CellUpdateRequest) -> dict[str, Any]:
        edits = [e.model_dump() for e in req.edits]
        try:
            return _svc().update_cells(req.sheet, edits)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    # -- Sheet CRUD --

    @router.get("/sheets")
    async def list_sheets() -> list[dict[str, Any]]:
        return _svc().list_sheets()

    @router.post("/sheets")
    async def add_sheet(req: AddSheetRequest) -> dict[str, Any]:
        try:
            return _svc().add_sheet(req.name)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @router.delete("/sheets")
    async def delete_sheet(req: DeleteSheetRequest) -> dict[str, Any]:
        try:
            return _svc().delete_sheet(req.name)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @router.patch("/sheets")
    async def rename_sheet(req: RenameSheetRequest) -> dict[str, Any]:
        try:
            return _svc().rename_sheet(req.old_name, req.new_name)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    # -- Cell format --

    @router.post("/sheet/format")
    async def update_format(req: CellFormatRequest) -> dict[str, Any]:
        try:
            return _svc().update_cell_format(req.sheet, [u.model_dump() for u in req.updates])
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    # -- Row/column insert & delete --

    @router.post("/sheet/rows/insert")
    async def insert_rows(req: RowInsertRequest) -> dict[str, Any]:
        try:
            return _svc().insert_rows(req.sheet, req.row_idx, req.count)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @router.post("/sheet/rows/delete")
    async def delete_rows(req: RowDeleteRequest) -> dict[str, Any]:
        try:
            return _svc().delete_rows(req.sheet, req.row_idx, req.count)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @router.post("/sheet/cols/insert")
    async def insert_cols(req: ColInsertRequest) -> dict[str, Any]:
        try:
            return _svc().insert_cols(req.sheet, req.col_idx, req.count)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @router.post("/sheet/cols/delete")
    async def delete_cols(req: ColDeleteRequest) -> dict[str, Any]:
        try:
            return _svc().delete_cols(req.sheet, req.col_idx, req.count)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    # -- Incidents --

    @router.get("/incidents")
    async def get_incidents(run_id: str | None = Query(None)) -> dict[str, Any]:
        return _svc().get_incidents(run_id)

    # -- Pipeline --

    @router.post("/pipeline/run")
    async def run_pipeline() -> dict[str, Any]:
        result = _svc().run_pipeline()
        if result.get("status") == "error" and result.get("error", "").startswith("Working copy"):
            raise HTTPException(409, result["error"])
        return result

    # -- Commit (canonical) / Save (legacy) --

    @router.post("/commit")
    async def commit() -> dict[str, Any]:
        return _svc().save_snapshot()

    @router.post("/save")
    async def save() -> dict[str, Any]:
        return _svc().save_snapshot()

    # -- Build (canonical) / Run (legacy) --

    @router.post("/build")
    async def build_workbook() -> dict[str, Any]:
        result = _svc().build_workbook()
        if "error" in result:
            raise HTTPException(409, result["error"])
        return result

    @router.post("/run")
    async def run_workbook() -> dict[str, Any]:
        result = _svc().build_workbook()
        if "error" in result:
            raise HTTPException(409, result["error"])
        return result

    # -- Sync --

    @router.post("/sync")
    async def sync(req: SyncRequest | None = None) -> dict[str, Any]:
        table_name = req.table_name if req else None
        return _svc().run_sync(table_name)

    # -- Workflow --

    @router.post("/workflow/run")
    async def workflow_run(req: WorkflowRunRequest) -> dict[str, Any]:
        try:
            return _svc().run_workflow(req.workflow_name)
        except Exception as exc:
            raise HTTPException(400, str(exc))

    # -- Runs --

    @router.get("/runs")
    async def list_runs(limit: int = Query(50, ge=1, le=500)) -> list[dict[str, Any]]:
        return _svc().list_runs(limit)

    @router.get("/run/latest")
    async def latest_run() -> dict[str, Any]:
        result = _svc().get_latest_run()
        if result is None:
            raise HTTPException(404, "No runs found")
        return result

    # -- Outputs --

    @router.get("/outputs/scalars")
    async def get_scalars(run_id: str | None = Query(None)) -> dict[str, Any]:
        return _svc().get_scalar_outputs(run_id)

    @router.get("/outputs/table")
    async def get_table(
        name: str = Query(...),
        run_id: str | None = Query(None),
        limit: int = Query(5000, ge=1, le=50000),
    ) -> dict[str, Any]:
        result = _svc().get_table_output(name, run_id, limit)
        if "error" in result:
            raise HTTPException(404, result["error"])
        return result

    @router.post("/outputs/table/view")
    async def view_table(req: TableViewRequest) -> dict[str, Any]:
        """Apply view-only sort/filter transforms to a table output."""
        from fin123.ui.view_transforms import apply_view_transforms

        result = _svc().get_table_output(req.name, req.run_id, limit=50000)
        if "error" in result:
            raise HTTPException(404, result["error"])

        import polars as _pl

        df = _pl.DataFrame(result["rows"])
        df = apply_view_transforms(df, sorts=req.sorts, filters=req.filters)
        total_rows = len(df)
        df = df.head(req.limit)

        return {
            "table": req.name,
            "columns": df.columns,
            "rows": df.to_dicts(),
            "total_rows": total_rows,
            "limited": total_rows > req.limit,
        }

    @router.get("/outputs/table/download")
    async def download_table(
        name: str = Query(...),
        run_id: str | None = Query(None),
    ) -> FileResponse:
        path = _svc().get_table_download_path(name, run_id)
        if path is None:
            raise HTTPException(404, "Table not found")
        return FileResponse(
            str(path),
            media_type="application/octet-stream",
            filename=f"{name}.parquet",
        )

    # -- Snapshots --

    @router.get("/snapshots")
    async def list_snapshots(limit: int = Query(50, ge=1, le=500)) -> list[dict[str, str]]:
        return _svc().list_snapshots(limit)

    # -- Artifacts --

    @router.get("/artifacts")
    async def list_artifacts() -> dict[str, list[dict[str, Any]]]:
        return _svc().list_artifacts()

    # -- Datasheets --

    @router.get("/datasheets")
    async def get_datasheets() -> list[dict[str, Any]]:
        return _svc().get_datasheets()

    # -- Named ranges --

    @router.get("/names")
    async def list_names() -> dict[str, dict[str, str]]:
        return _svc().list_names()

    @router.post("/names")
    async def create_name(req: NameRequest) -> dict[str, Any]:
        try:
            return _svc().set_name(req.name, req.sheet, req.start, req.end)
        except (ValueError, KeyError) as exc:
            raise HTTPException(400, str(exc))

    @router.patch("/names/{name}")
    async def update_name(name: str, req: NameUpdateRequest) -> dict[str, Any]:
        updates = {k: v for k, v in req.model_dump().items() if v is not None}
        try:
            return _svc().update_name(name, **updates)
        except KeyError as exc:
            raise HTTPException(404, str(exc))
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @router.delete("/names/{name}")
    async def delete_name(name: str) -> dict[str, Any]:
        try:
            return _svc().delete_name(name)
        except KeyError as exc:
            raise HTTPException(404, str(exc))

    # -- Formula validation --

    @router.post("/validate-formula")
    async def validate_formula(req: ValidateFormulaRequest) -> dict[str, Any]:
        return _svc().validate_formula(req.text)

    # -- PARAM unbinding --

    @router.post("/unbind-param")
    async def unbind_param(req: UnbindParamRequest) -> dict[str, Any]:
        try:
            return _svc().unbind_param(req.sheet, req.addr)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    # -- Model identity & versioning --

    @router.get("/model")
    async def get_model_info() -> dict[str, Any]:
        return _svc().get_model_info()

    @router.get("/model/versions")
    async def list_model_versions() -> list[dict]:
        return _svc().list_model_versions()

    @router.post("/model/select")
    async def select_model_version(req: SelectVersionRequest) -> dict[str, Any]:
        try:
            return _svc().select_model_version(req.version)
        except FileNotFoundError as exc:
            raise HTTPException(404, str(exc))

    @router.post("/model/pin")
    async def pin_model_version(req: PinVersionRequest) -> dict[str, Any]:
        _svc().pin_model_version(req.version)
        return {"ok": True, "version": req.version, "pinned": True}

    @router.post("/model/unpin")
    async def unpin_model_version(req: PinVersionRequest) -> dict[str, Any]:
        _svc().unpin_model_version(req.version)
        return {"ok": True, "version": req.version, "pinned": False}

    # -- Clear cache --

    @router.post("/clear-cache")
    async def clear_cache(req: ClearCacheRequest) -> dict[str, Any]:
        return _svc().clear_cache(dry_run=req.dry_run)

    # -- Import reports --

    @router.get("/import/report/latest")
    async def get_latest_import_report() -> dict[str, Any]:
        result = _svc().get_latest_import_report()
        if result is None:
            raise HTTPException(404, "No import reports found")
        return result

    @router.get("/import/report/list")
    async def list_import_reports() -> list[dict[str, str]]:
        return _svc().list_import_reports()

    # -- Import trace log --

    @router.get("/import/trace/latest")
    async def get_latest_import_trace():
        content = _svc().get_latest_import_trace()
        if content is None:
            raise HTTPException(404, "No import trace log found")
        return JSONResponse(content={"trace": content})

    @router.get("/import/trace/download/latest")
    async def download_latest_import_trace():
        path = _svc().get_latest_import_trace_path()
        if path is None:
            raise HTTPException(404, "No import trace log found")
        return FileResponse(
            str(path),
            media_type="text/plain",
            filename="import_trace.log",
            headers={"Content-Disposition": "attachment; filename=import_trace.log"},
        )

    # -- Import review --

    @router.post("/import/review/todo")
    async def mark_import_todo(req: ImportTodoRequest) -> dict[str, Any]:
        try:
            return _svc().mark_import_todo(req.sheet, req.addr)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @router.post("/import/review/convert-value")
    async def convert_import_to_value(req: ImportConvertRequest) -> dict[str, Any]:
        try:
            return _svc().convert_to_value(req.sheet, req.addr)
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    # -- Import upload (new project) --

    _MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB

    @router.post("/import/xlsx")
    async def upload_xlsx(
        file: UploadFile = File(...),
        project_name: str | None = Form(None),
    ) -> dict[str, Any]:
        fname = file.filename or ""
        if not fname.lower().endswith(".xlsx"):
            raise HTTPException(400, "Only .xlsx files are accepted")

        data = await file.read()
        if len(data) == 0:
            raise HTTPException(400, "Uploaded file is empty")
        if len(data) > _MAX_UPLOAD_BYTES:
            raise HTTPException(400, f"File too large (max {_MAX_UPLOAD_BYTES // (1024*1024)} MB)")

        try:
            result = import_xlsx_upload(
                file_bytes=data,
                filename=fname,
                project_name=project_name or None,
            )
        except ValueError as exc:
            raise HTTPException(400, str(exc))

        return result

    # -- Event logs --

    import re as _re

    _SAFE_LOG_ID = _re.compile(r"^[A-Za-z0-9_\-]+$")

    def _inject_display_type(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add display_type field to each event dict."""
        from fin123.logging.events import display_event_type

        for evt in events:
            raw = evt.get("event_type", "")
            evt["display_type"] = display_event_type(raw)
        return events

    @router.get("/events/tail")
    async def tail_events(
        scope: str = Query("global"),
        id: str | None = Query(None),
        n: int = Query(500, ge=1, le=2000),
    ) -> list[dict[str, Any]]:
        """Canonical event tail endpoint.

        Scopes: global, build, run, sync, import.
        ``build`` and ``run`` are synonyms.
        For build/run/sync scopes, ``id`` is required.
        """
        # Normalize "build" scope to "run" (internal)
        effective_scope = "run" if scope == "build" else scope
        if effective_scope in ("run", "sync") and not id:
            raise HTTPException(400, f"scope={scope!r} requires id parameter")
        if id and not _SAFE_LOG_ID.match(id):
            raise HTTPException(400, "Invalid id")
        events = _svc().tail_events(scope=effective_scope, scope_id=id, n=n)
        return _inject_display_type(events)

    @router.get("/events")
    async def get_events(
        level: str | None = Query(None),
        event_type: str | None = Query(None),
        plugin: str | None = Query(None),
        run_id: str | None = Query(None),
        sync_id: str | None = Query(None),
        limit: int = Query(200, ge=1, le=2000),
    ) -> list[dict[str, Any]]:
        from fin123.logging.sink import EventSink

        sink = EventSink(_svc().project_dir)
        return sink.read_global(
            level=level,
            event_type=event_type,
            plugin=plugin,
            run_id=run_id,
            sync_id=sync_id,
            limit=limit,
        )

    @router.get("/run/log")
    async def get_run_log(run_id: str = Query(...)) -> list[dict[str, Any]]:
        if not _SAFE_LOG_ID.match(run_id):
            raise HTTPException(400, "Invalid run_id")
        from fin123.logging.sink import EventSink

        sink = EventSink(_svc().project_dir)
        return sink.read_run_log(run_id)

    @router.get("/sync/log")
    async def get_sync_log(sync_id: str = Query(...)) -> list[dict[str, Any]]:
        if not _SAFE_LOG_ID.match(sync_id):
            raise HTTPException(400, "Invalid sync_id")
        from fin123.logging.sink import EventSink

        sink = EventSink(_svc().project_dir)
        return sink.read_sync_log(sync_id)

    # -- Build checks & verify (canonical) / Run checks & verify (legacy) --

    @router.get("/build/checks")
    async def get_build_checks(run_id: str = Query(...)) -> dict[str, Any]:
        """Return check results for a build: assertions, verify, timings."""
        if not _SAFE_LOG_ID.match(run_id):
            raise HTTPException(400, "Invalid run_id")
        result = _svc().get_build_checks(run_id)
        if not result["exists"]:
            raise HTTPException(404, f"Build {run_id!r} not found")
        return result

    @router.get("/run/checks")
    async def get_run_checks(run_id: str = Query(...)) -> dict[str, Any]:
        """Legacy endpoint — use /api/build/checks instead."""
        if not _SAFE_LOG_ID.match(run_id):
            raise HTTPException(400, "Invalid run_id")
        result = _svc().get_build_checks(run_id)
        if not result["exists"]:
            raise HTTPException(404, f"Run {run_id!r} not found")
        return result

    @router.post("/build/verify")
    async def build_verify(req: VerifyRunRequest) -> dict[str, Any]:
        """Run verification on a completed build."""
        if not _SAFE_LOG_ID.match(req.run_id):
            raise HTTPException(400, "Invalid run_id")
        return _svc().build_verify(req.run_id)

    @router.post("/run/verify")
    async def verify_run(req: VerifyRunRequest) -> dict[str, Any]:
        """Legacy endpoint — use /api/build/verify instead."""
        if not _SAFE_LOG_ID.match(req.run_id):
            raise HTTPException(400, "Invalid run_id")
        return _svc().build_verify(req.run_id)

    # -- Health --

    @router.get("/health")
    async def get_project_health() -> dict[str, Any]:
        return _svc().get_project_health()

    # -- Model status ribbon --

    @router.get("/status")
    async def get_model_status() -> dict[str, Any]:
        """Compact status for the UI ribbon: dirty, datasheets, build, verify."""
        return _svc().get_model_status()

    # -- Latest table output --

    @router.get("/run/latest/table")
    async def get_latest_table(run_id: str | None = Query(None)) -> dict[str, Any]:
        """Return the primary output table name for a run."""
        result = _svc().get_latest_table_output_name(run_id)
        if "error" in result:
            raise HTTPException(404, result["error"])
        return result

    # -- Registry --

    @router.get("/registry/status")
    async def get_registry_status() -> dict[str, Any]:
        return _svc().get_registry_status()

    @router.post("/registry/push")
    async def registry_push(req: RegistryPushRequest | None = None) -> dict[str, Any]:
        versions = req.versions if req else None
        result = _svc().registry_push_versions(versions)
        if "error" in result:
            raise HTTPException(400, result["error"])
        return result

    @router.post("/registry/pull")
    async def registry_pull(req: RegistryPullRequest) -> dict[str, Any]:
        result = _svc().registry_pull_version(req.model_id, req.version)
        if "error" in result:
            raise HTTPException(400, result["error"])
        return result

    return router
