"""Command-line interface for fin123 (standalone local engine + UI).

Unified CLI surface shared with fin123-pod. Enterprise commands that
require fin123-pod are stubbed here and return exit code 4.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from fin123 import __core_api_version__, __version__

# ---------------------------------------------------------------------------
# Exit codes (shared contract with fin123-pod)
# ---------------------------------------------------------------------------
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_VERIFY_FAIL = 3
EXIT_ENTERPRISE = 4
EXIT_DEPENDENCY = 5

# ---------------------------------------------------------------------------
# JSON output helper
# ---------------------------------------------------------------------------


def _json_out(ok: bool, cmd: str, data: dict | None = None, error: dict | None = None) -> str:
    """Build the standard JSON response envelope."""
    return json.dumps(
        {
            "ok": ok,
            "cmd": cmd,
            "version": __version__,
            "data": data or {},
            "error": error,
        },
        indent=2,
        sort_keys=True,
    )


def _emit(ctx: click.Context, text: str) -> None:
    """Emit text unless --quiet is active."""
    if not ctx.obj.get("quiet"):
        click.echo(text)


def _emit_err(ctx: click.Context, text: str) -> None:
    """Emit to stderr unless --quiet is active."""
    if not ctx.obj.get("quiet"):
        click.echo(text, err=True)


def _enterprise_stub(ctx: click.Context, cmd_name: str) -> None:
    """Emit enterprise-only message and exit 4."""
    if ctx.obj.get("json"):
        click.echo(_json_out(
            False, cmd_name,
            error={"code": EXIT_ENTERPRISE, "message": "Enterprise feature: install fin123-pod"},
        ))
    else:
        _emit_err(ctx, f"Enterprise feature: {cmd_name} requires fin123-pod.")
    sys.exit(EXIT_ENTERPRISE)


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(
    version=f"{__version__} (core_api={__core_api_version__})",
    prog_name="fin123",
)
@click.option("--json", "use_json", is_flag=True, help="Machine-readable JSON output.")
@click.option("--quiet", is_flag=True, help="Suppress non-essential output.")
@click.option("--verbose", is_flag=True, help="Verbose diagnostic output.")
@click.pass_context
def main(ctx: click.Context, use_json: bool, quiet: bool, verbose: bool) -> None:
    """fin123 -- deterministic financial model engine.

    Lifecycle: Edit -> Commit -> Build -> Verify

    Core commands:

      init           Scaffold a new project
      build          Execute workbook
      verify         Verify build integrity
      diff           Compare runs or versions
      export         Export run outputs
      doctor         Preflight and compliance checks

    Enterprise commands (require fin123-pod):

      registry       Registry operations
      plugins        Plugin management
      server         Runner service
    """
    ctx.ensure_object(dict)
    ctx.obj["json"] = use_json
    ctx.obj["quiet"] = quiet
    ctx.obj["verbose"] = verbose


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_overrides(overrides: tuple[str, ...]) -> dict[str, str]:
    params: dict[str, str] = {}
    for item in overrides:
        if "=" not in item:
            raise click.ClickException(f"Invalid --set format: {item!r}. Use key=value.")
        k, v = item.split("=", 1)
        params[k] = v
    return params


# ---------------------------------------------------------------------------
# Init (unified name for 'new')
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path())
@click.option("--template", "template_name", default=None, help="Scaffold from a bundled template.")
@click.option("--template-dir", "template_dir", default=None, type=click.Path(exists=True), help="Scaffold from a local template directory.")
@click.option("--set", "overrides", multiple=True, help="Override template params as key=value.")
@click.pass_context
def init(ctx: click.Context, directory: str, template_name: str | None, template_dir: str | None, overrides: tuple[str, ...]) -> None:
    """Scaffold a new project at DIRECTORY.

    Examples:

      fin123 init my_model
      fin123 init my_model --template single_company --set ticker=AAPL
    """
    target = Path(directory)

    if template_name or template_dir:
        from fin123.template_engine import scaffold_from_template

        parsed_overrides: dict[str, str] = {}
        for item in overrides:
            if "=" not in item:
                raise click.ClickException(f"Invalid --set format: {item!r}. Use key=value.")
            k, v = item.split("=", 1)
            parsed_overrides[k] = v

        try:
            result = scaffold_from_template(
                target_dir=target,
                name=template_name,
                template_dir=Path(template_dir) if template_dir else None,
                overrides=parsed_overrides,
            )
            if ctx.obj.get("json"):
                click.echo(_json_out(True, "init", {"path": str(result)}))
            else:
                _emit(ctx, f"Created project from template at {result}")
        except (FileExistsError, FileNotFoundError, ValueError) as e:
            if ctx.obj.get("json"):
                click.echo(_json_out(False, "init", error={"code": EXIT_ERROR, "message": str(e)}))
                sys.exit(EXIT_ERROR)
            raise click.ClickException(str(e))
    else:
        if overrides:
            raise click.ClickException("--set requires --template or --template-dir")
        from fin123.project import scaffold_project

        try:
            result = scaffold_project(target)
            if ctx.obj.get("json"):
                click.echo(_json_out(True, "init", {"path": str(result)}))
            else:
                _emit(ctx, f"Created project at {result}")
        except FileExistsError as e:
            if ctx.obj.get("json"):
                click.echo(_json_out(False, "init", error={"code": EXIT_ERROR, "message": str(e)}))
                sys.exit(EXIT_ERROR)
            raise click.ClickException(str(e))


# Keep 'new' as an alias for backward compatibility
@main.command("new", hidden=True)
@click.argument("directory", type=click.Path())
@click.option("--template", "template_name", default=None, help="Scaffold from a bundled template.")
@click.option("--template-dir", "template_dir", default=None, type=click.Path(exists=True), help="Scaffold from a local template directory.")
@click.option("--set", "overrides", multiple=True, help="Override template params as key=value.")
@click.pass_context
def new(ctx: click.Context, directory: str, template_name: str | None, template_dir: str | None, overrides: tuple[str, ...]) -> None:
    """Scaffold a new project at DIRECTORY (alias for init)."""
    ctx.invoke(init, directory=directory, template_name=template_name, template_dir=template_dir, overrides=overrides)


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------


@main.group()
def template() -> None:
    """Template management commands."""


@template.command("list")
@click.pass_context
def template_list(ctx: click.Context) -> None:
    """List available project templates."""
    from fin123.template_engine import list_templates

    templates = list_templates()
    if ctx.obj.get("json"):
        out = [
            {
                "name": t["name"],
                "description": t["description"],
                "invariants": t.get("invariants", []),
                "params": list((t.get("params") or {}).keys()),
            }
            for t in templates
        ]
        click.echo(_json_out(True, "template list", {"templates": out}))
    else:
        if not templates:
            _emit(ctx, "No templates found.")
            return
        for t in templates:
            params = list((t.get("params") or {}).keys())
            params_str = f"  params: {', '.join(params)}" if params else ""
            _emit(ctx, f"  {t['name']:20s} {t['description']}{params_str}")


@template.command("show")
@click.argument("name")
@click.pass_context
def template_show(ctx: click.Context, name: str) -> None:
    """Show template details and file tree."""
    from fin123.template_engine import show_template

    try:
        info = show_template(name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "template show", info))
        return

    meta = info["meta"]
    _emit(ctx, f"Template: {meta['name']}")
    _emit(ctx, f"Description: {meta['description']}")
    _emit(ctx, f"Engine compat: {meta.get('engine_compat', 'n/a')}")
    _emit(ctx, f"Invariants: {', '.join(meta.get('invariants', []))}")
    params = meta.get("params") or {}
    if params:
        _emit(ctx, "Parameters:")
        for pname, pdef in params.items():
            _emit(ctx, f"  {pname}: {pdef['type']} (default: {pdef['default']})")
    _emit(ctx, "Files:")
    for f in info["files"]:
        _emit(ctx, f"  {f}")


# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.pass_context
def commit(ctx: click.Context, directory: str) -> None:
    """Commit the current workbook state as a new snapshot version.

    Lifecycle: Edit -> *Commit* -> Build -> Verify
    """
    from fin123.versioning import SnapshotStore

    project_dir = Path(directory)
    wb_path = project_dir / "workbook.yaml"
    if not wb_path.exists():
        raise click.ClickException(f"No workbook.yaml in {project_dir}")
    wb_yaml = wb_path.read_text()
    store = SnapshotStore(project_dir)
    version = store.save_snapshot(wb_yaml)
    if ctx.obj.get("json"):
        click.echo(_json_out(True, "commit", {"version": version}))
    else:
        _emit(ctx, f"Committed snapshot: {version}")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _do_build(
    ctx: click.Context,
    project_dir: Path,
    overrides: tuple[str, ...],
    scenario_name: str | None,
    all_scenarios: bool,
    out_path: str | None = None,
) -> None:
    """Core build logic."""
    from fin123.workbook import Workbook

    params = _parse_overrides(overrides)

    if all_scenarios:
        wb_probe = Workbook(project_dir)
        scenario_names = wb_probe.get_scenario_names()
        if not scenario_names:
            raise click.ClickException("No scenarios defined in workbook.yaml")
        _emit(ctx, f"Building {len(scenario_names)} scenario(s): {', '.join(scenario_names)}")
        results = []
        for sname in scenario_names:
            wb = Workbook(project_dir, overrides=params, scenario_name=sname)
            result = wb.run()
            _emit(ctx, f"  [{sname}] Build saved to: {result.run_dir.name}")
            results.append({"scenario": sname, "run_dir": result.run_dir.name})
        if ctx.obj.get("json"):
            click.echo(_json_out(True, "build", {"builds": results}))
        return

    wb = Workbook(project_dir, overrides=params, scenario_name=scenario_name)
    result = wb.run()

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "build", {
            "run_dir": result.run_dir.name,
            "scalars_count": len(result.scalars),
            "tables": list(result.tables.keys()),
        }))
    else:
        _emit(ctx, f"Build saved to: {result.run_dir.name}")
        _emit(ctx, f"Scalars: {len(result.scalars)}")
        _emit(ctx, f"Tables: {', '.join(result.tables.keys())}")


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--set", "overrides", multiple=True, help="Override params as key=value.")
@click.option("--scenario", "scenario_name", default=None, help="Build a named scenario.")
@click.option("--all-scenarios", is_flag=True, help="Build all scenarios.")
@click.option("--out", "out_path", default=None, type=click.Path(), help="Output directory override.")
@click.pass_context
def build(ctx: click.Context, directory: str, overrides: tuple[str, ...], scenario_name: str | None, all_scenarios: bool, out_path: str | None) -> None:
    """Build (execute) the workbook in DIRECTORY.

    Lifecycle: Edit -> Commit -> *Build* -> Verify

    Examples:

      fin123 build my_model
      fin123 build my_model --set tax_rate=0.25
      fin123 build my_model --scenario bear_case
      fin123 build my_model --json
    """
    _do_build(ctx, Path(directory), overrides, scenario_name, all_scenarios, out_path)


# ---------------------------------------------------------------------------
# Artifact
# ---------------------------------------------------------------------------


@main.group()
def artifact() -> None:
    """Artifact commands."""


@artifact.command("list")
@click.argument("directory", type=click.Path(exists=True))
@click.pass_context
def artifact_list(ctx: click.Context, directory: str) -> None:
    """List all artifacts in DIRECTORY."""
    from fin123.versioning import ArtifactStore

    project_dir = Path(directory)
    store = ArtifactStore(project_dir)
    artifacts = store.list_artifacts()

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "artifact list", {"artifacts": artifacts}))
        return

    if not artifacts:
        _emit(ctx, "No artifacts found.")
        return

    for name, versions in artifacts.items():
        _emit(ctx, f"{name}:")
        for v in versions:
            _emit(ctx, (
                f"  {v['version']}  {v['created_at']}  "
                f"status={v['status']}  workflow={v['workflow_name']}"
            ))


@artifact.command("approve")
@click.argument("artifact_name")
@click.argument("version")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.option("--by", "approved_by", default="", help="Approver identifier.")
@click.option("--note", default="", help="Free-text note.")
@click.pass_context
def artifact_approve(ctx: click.Context, artifact_name: str, version: str, directory: str, approved_by: str, note: str) -> None:
    """Approve an artifact version."""
    from fin123.versioning import ArtifactStore

    store = ArtifactStore(Path(directory))
    try:
        approval = store.approve_artifact(artifact_name, version, approved_by=approved_by, note=note)
        if ctx.obj.get("json"):
            click.echo(_json_out(True, "artifact approve", approval))
        else:
            _emit(ctx, f"Artifact {artifact_name} {version}: {approval['status']}")
    except FileNotFoundError as e:
        if ctx.obj.get("json"):
            click.echo(_json_out(False, "artifact approve", error={"code": EXIT_ERROR, "message": str(e)}))
            sys.exit(EXIT_ERROR)
        raise click.ClickException(str(e))


@artifact.command("reject")
@click.argument("artifact_name")
@click.argument("version")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.option("--by", "approved_by", default="", help="Rejector identifier.")
@click.option("--note", default="", help="Free-text note.")
@click.option("--reason-code", default="", help="Machine-readable rejection reason.")
@click.pass_context
def artifact_reject(ctx: click.Context, artifact_name: str, version: str, directory: str, approved_by: str, note: str, reason_code: str) -> None:
    """Reject an artifact version."""
    from fin123.versioning import ArtifactStore

    store = ArtifactStore(Path(directory))
    try:
        approval = store.reject_artifact(artifact_name, version, approved_by=approved_by, note=note, reason_code=reason_code)
        if ctx.obj.get("json"):
            click.echo(_json_out(True, "artifact reject", approval))
        else:
            _emit(ctx, f"Artifact {artifact_name} {version}: {approval['status']}")
    except FileNotFoundError as e:
        if ctx.obj.get("json"):
            click.echo(_json_out(False, "artifact reject", error={"code": EXIT_ERROR, "message": str(e)}))
            sys.exit(EXIT_ERROR)
        raise click.ClickException(str(e))


@artifact.command("status")
@click.argument("artifact_name")
@click.argument("version")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.pass_context
def artifact_status(ctx: click.Context, artifact_name: str, version: str, directory: str) -> None:
    """Show the approval status of an artifact version."""
    from fin123.versioning import ArtifactStore

    store = ArtifactStore(Path(directory))
    try:
        approval = store.get_artifact_approval(artifact_name, version)
        if ctx.obj.get("json"):
            click.echo(_json_out(True, "artifact status", approval))
        else:
            _emit(ctx, f"Artifact: {artifact_name} {version}")
            _emit(ctx, f"Status: {approval['status']}")
            if approval.get("approved_by"):
                _emit(ctx, f"By: {approval['approved_by']}")
            if approval.get("approved_at"):
                _emit(ctx, f"At: {approval['approved_at']}")
            if approval.get("note"):
                _emit(ctx, f"Note: {approval['note']}")
            if approval.get("reason_code"):
                _emit(ctx, f"Reason: {approval['reason_code']}")
    except FileNotFoundError as e:
        if ctx.obj.get("json"):
            click.echo(_json_out(False, "artifact status", error={"code": EXIT_ERROR, "message": str(e)}))
            sys.exit(EXIT_ERROR)
        raise click.ClickException(str(e))


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


@main.group()
def diff() -> None:
    """Diff commands for comparing runs and versions.

    Examples:

      fin123 diff run <run_a> <run_b> --project my_model
      fin123 diff version v0001 v0002 --project my_model
    """


@diff.command("run")
@click.argument("run_a")
@click.argument("run_b")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.pass_context
def diff_run(ctx: click.Context, run_a: str, run_b: str, directory: str) -> None:
    """Compare two runs."""
    from fin123.diff import diff_runs, format_run_diff

    try:
        result = diff_runs(Path(directory), run_a, run_b)
    except FileNotFoundError as e:
        if ctx.obj.get("json"):
            click.echo(_json_out(False, "diff run", error={"code": EXIT_USAGE, "message": str(e)}))
        else:
            _emit_err(ctx, str(e))
        sys.exit(EXIT_USAGE)

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "diff run", result))
    else:
        _emit(ctx, format_run_diff(result))


@diff.command("version")
@click.argument("version_a")
@click.argument("version_b")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.pass_context
def diff_version(ctx: click.Context, version_a: str, version_b: str, directory: str) -> None:
    """Compare two workbook snapshot versions."""
    from fin123.diff import diff_versions, format_version_diff

    try:
        result = diff_versions(Path(directory), version_a, version_b)
    except FileNotFoundError as e:
        if ctx.obj.get("json"):
            click.echo(_json_out(False, "diff version", error={"code": EXIT_USAGE, "message": str(e)}))
        else:
            _emit_err(ctx, str(e))
        sys.exit(EXIT_USAGE)

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "diff version", result))
    else:
        _emit(ctx, format_version_diff(result))


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------


def _do_verify_build(ctx: click.Context, run_id: str, directory: str) -> None:
    from fin123.logging.events import set_project_dir
    from fin123.verify import verify_run

    project_dir = Path(directory)
    set_project_dir(project_dir)
    report = verify_run(project_dir, run_id)

    if report.get("no_run"):
        if ctx.obj.get("json"):
            click.echo(_json_out(False, "verify", data=report, error={"code": EXIT_USAGE, "message": "No completed build found"}))
        else:
            _emit(ctx, f"Verify build: {run_id}")
            _emit(ctx, "Status: FAIL")
            _emit(ctx, f"  no completed build run found for project '{run_id}'")
            _emit(ctx, "")
            _emit(ctx, "Next steps:")
            _emit(ctx, "  Run:  fin123 build <project_dir>")
            _emit(ctx, "  Or:   open the UI and click Build")
        sys.exit(EXIT_USAGE)

    if ctx.obj.get("json"):
        ok = report["status"] == "pass"
        click.echo(_json_out(ok, "verify", data=report, error=None if ok else {"code": EXIT_VERIFY_FAIL, "message": "Verification failed"}))
    else:
        _emit(ctx, f"Verify build: {run_id}")
        _emit(ctx, f"Status: {report['status'].upper()}")
        if report["failures"]:
            for f in report["failures"]:
                _emit(ctx, f"  FAIL: {f}")
        else:
            _emit(ctx, "  All checks passed.")

    if report["status"] != "pass":
        sys.exit(EXIT_VERIFY_FAIL)


@main.command()
@click.argument("path")
@click.option("--project", "directory", type=click.Path(exists=True), default=".", help="Project directory.")
@click.pass_context
def verify(ctx: click.Context, path: str, directory: str) -> None:
    """Verify the integrity of a completed build or artifact.

    PATH is a run ID (e.g. 20260227T120000_run_1).

    Lifecycle: Edit -> Commit -> Build -> *Verify*

    Examples:

      fin123 verify 20260227T120000_run_1 --project my_model
      fin123 verify 20260227T120000_run_1 --project my_model --json
    """
    _do_verify_build(ctx, path, directory)


# Keep verify-build as hidden alias for backward compatibility
@main.command("verify-build", hidden=True)
@click.argument("run_id")
@click.option("--project", "directory", type=click.Path(exists=True), default=".", help="Project directory.")
@click.option("--json", "as_json", is_flag=True, help="Output report as JSON.")
@click.pass_context
def verify_build_cmd(ctx: click.Context, run_id: str, directory: str, as_json: bool) -> None:
    """Verify the integrity of a completed build (alias for verify)."""
    if as_json:
        ctx.obj["json"] = True
    _do_verify_build(ctx, run_id, directory)


# ---------------------------------------------------------------------------
# GC
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without deleting.")
@click.pass_context
def gc(ctx: click.Context, directory: str, dry_run: bool) -> None:
    """Run garbage collection on DIRECTORY."""
    from fin123.gc import run_gc

    project_dir = Path(directory)
    summary = run_gc(project_dir, dry_run=dry_run)

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "gc", summary))
        return

    label = "GC dry-run:" if dry_run else "GC complete:"
    _emit(ctx, label)
    _emit(ctx, f"  Runs deleted: {summary['runs_deleted']}")
    _emit(ctx, f"  Artifact versions deleted: {summary['artifact_versions_deleted']}")
    _emit(ctx, f"  Sync runs deleted: {summary['sync_runs_deleted']}")
    _emit(ctx, f"  Model versions deleted: {summary['model_versions_deleted']}")
    _emit(ctx, f"  Bytes freed: {summary['bytes_freed']:,}")
    _emit(ctx, f"  Orphaned dirs cleaned: {summary['orphaned_cleaned']}")


@main.command("clear-cache")
@click.argument("directory", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without deleting.")
@click.option("--aggressive", is_flag=True, help="Apply lower retention thresholds.")
@click.pass_context
def clear_cache(ctx: click.Context, directory: str, dry_run: bool, aggressive: bool) -> None:
    """Clear caches and run GC on DIRECTORY."""
    from fin123.gc import run_gc

    project_dir = Path(directory)

    if aggressive and not dry_run:
        run_gc(project_dir, dry_run=False)

    summary = run_gc(project_dir, dry_run=dry_run)

    hash_cache_path = project_dir / "cache" / "hashes.json"
    hash_cache_size = 0
    if hash_cache_path.exists():
        hash_cache_size = hash_cache_path.stat().st_size
        if not dry_run:
            hash_cache_path.unlink()

    if ctx.obj.get("json"):
        summary["hash_cache_cleared_bytes"] = hash_cache_size
        click.echo(_json_out(True, "clear-cache", summary))
        return

    label = "Clear-cache dry-run:" if dry_run else "Clear-cache complete:"
    _emit(ctx, label)
    _emit(ctx, f"  Runs deleted: {summary['runs_deleted']}")
    _emit(ctx, f"  Artifact versions deleted: {summary['artifact_versions_deleted']}")
    _emit(ctx, f"  Sync runs deleted: {summary['sync_runs_deleted']}")
    _emit(ctx, f"  Model versions deleted: {summary['model_versions_deleted']}")
    _emit(ctx, f"  Hash cache cleared: {hash_cache_size:,} bytes")
    _emit(ctx, f"  Bytes freed: {summary['bytes_freed'] + hash_cache_size:,}")


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--format", "fmt", type=click.Choice(["json", "csv", "xlsx"]), default="json", help="Output format.")
@click.option("--out", "out_path", default=None, type=click.Path(), help="Output file path.")
@click.pass_context
def export(ctx: click.Context, directory: str, fmt: str, out_path: str | None) -> None:
    """Export run outputs from DIRECTORY.

    Examples:

      fin123 export my_model
      fin123 export my_model --format csv --out results.csv
      fin123 export my_model --json
    """
    from fin123.versioning import RunStore

    project_dir = Path(directory)
    run_store = RunStore(project_dir)
    runs = run_store.list_runs()

    if not runs:
        if ctx.obj.get("json"):
            click.echo(_json_out(False, "export", error={"code": EXIT_ERROR, "message": "No runs found"}))
            sys.exit(EXIT_ERROR)
        raise click.ClickException("No runs found.")

    run_meta = runs[-1]
    run_dir = project_dir / "runs" / run_meta["run_id"]
    outputs_dir = run_dir / "outputs"

    scalars: dict[str, Any] = {}
    scalars_path = outputs_dir / "scalars.json"
    if scalars_path.exists():
        scalars = json.loads(scalars_path.read_text())

    tables_data: dict[str, list[dict]] = {}
    for f in sorted(outputs_dir.iterdir()):
        if f.suffix == ".parquet":
            import polars as pl
            df = pl.read_parquet(f)
            tables_data[f.stem] = df.to_dicts()

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "export", {
            "run_id": run_meta["run_id"],
            "timestamp": run_meta.get("timestamp", ""),
            "format": fmt,
            "scalars": scalars,
            "tables": {k: {"rows": len(v)} for k, v in tables_data.items()},
        }))
        return

    _emit(ctx, f"Exporting run: {run_meta['run_id']}")
    _emit(ctx, f"Timestamp: {run_meta.get('timestamp', '')}")

    if scalars:
        _emit(ctx, "Scalars:")
        _emit(ctx, json.dumps(scalars, indent=2))

    for tname, rows in tables_data.items():
        import polars as pl
        df = pl.DataFrame(rows)
        _emit(ctx, f"\nTable: {tname}")
        _emit(ctx, str(df))


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------


@main.group()
def batch() -> None:
    """Batch build commands."""


@batch.command("build")
@click.argument("directory", type=click.Path(exists=True))
@click.option("--params-file", required=True, type=click.Path(exists=True), help="CSV file with parameter sets.")
@click.option("--scenario", "scenario_name", default=None, help="Apply a scenario to each build.")
@click.option("--max-workers", type=int, default=1, help="Number of parallel workers (1=sequential).")
@click.pass_context
def batch_build(ctx: click.Context, directory: str, params_file: str, scenario_name: str | None, max_workers: int) -> None:
    """Build the workbook once per row in a params CSV file."""
    from fin123.batch import load_params_csv, run_batch

    project_dir = Path(directory)
    params_path = Path(params_file)

    rows = load_params_csv(params_path)
    if not rows:
        _emit(ctx, "No parameter rows found in CSV.")
        return

    _emit(ctx, f"Batch build: {len(rows)} parameter set(s) from {params_path.name}")
    if scenario_name:
        _emit(ctx, f"Scenario: {scenario_name}")
    if max_workers > 1:
        _emit(ctx, f"Parallel workers: {max_workers}")

    summary = run_batch(project_dir, rows, scenario_name=scenario_name, max_workers=max_workers)

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "batch build", summary))
        return

    _emit(ctx, f"\nBatch ID: {summary['build_batch_id']}")
    _emit(ctx, f"Total: {summary['total']}  OK: {summary['ok']}  Failed: {summary['failed']}")
    for r in summary["results"]:
        if r["status"] == "ok":
            _emit(ctx, f"  [{r['index']}] OK  run_id={r['run_id']}")
        else:
            _emit(ctx, f"  [{r['index']}] FAIL  {r['error']}")


# ---------------------------------------------------------------------------
# Import XLSX
# ---------------------------------------------------------------------------


@main.command("import-xlsx")
@click.argument("xlsx_file", type=click.Path(exists=True))
@click.argument("directory", type=click.Path())
@click.option("--max-rows", type=int, default=500, help="Max rows per sheet to import.")
@click.option("--max-cols", type=int, default=100, help="Max columns per sheet to import.")
@click.pass_context
def import_xlsx(ctx: click.Context, xlsx_file: str, directory: str, max_rows: int, max_cols: int) -> None:
    """Import an XLSX file into a new fin123 project at DIRECTORY."""
    from fin123.xlsx_import import import_xlsx as do_import

    xlsx_path = Path(xlsx_file)
    target = Path(directory)

    try:
        report = do_import(xlsx_path, target, max_rows=max_rows, max_cols=max_cols)
    except ImportError as e:
        raise click.ClickException(str(e))

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "import-xlsx", report))
        return

    _emit(ctx, f"Imported {xlsx_path.name} -> {target}")
    for s in report["sheets_imported"]:
        _emit(ctx, (
            f"  {s['name']}: {s['cells']} cells, "
            f"{s['formulas']} formulas, {s['colors']} colors"
        ))
    if report["skipped_features"]:
        _emit(ctx, f"  Skipped: {', '.join(report['skipped_features'])}")
    if report["warnings"]:
        for w in report["warnings"]:
            _emit_err(ctx, f"  Warning: {w}")
    _emit(ctx, f"Report: {target / 'import_report.json'}")


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--port", type=int, default=None, help="Port (auto-select if omitted).")
@click.option("--no-open", is_flag=True, help="Don't auto-open browser.")
@click.pass_context
def ui(ctx: click.Context, directory: str, host: str, port: int | None, no_open: bool) -> None:
    """Launch the local browser UI for DIRECTORY."""
    import socket
    import webbrowser

    import uvicorn

    from fin123.ui.server import create_app

    project_dir = Path(directory)
    app = create_app(project_dir)

    if port is None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, 0))
            port = s.getsockname()[1]

    url = f"http://{host}:{port}"
    _emit(ctx, f"Serving UI at {url}")
    _emit(ctx, "Press Ctrl+C to stop")

    if not no_open:
        import threading
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        uvicorn.run(app, host=host, port=port, log_level="warning")
    except KeyboardInterrupt:
        _emit(ctx, "\nStopped.")


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


@main.command("events")
@click.argument("directory", type=click.Path(exists=True))
@click.option("--level", default=None, type=click.Choice(["info", "warning", "error"]), help="Filter by level.")
@click.option("--type", "event_type", default=None, help="Filter by event type.")
@click.option("--plugin", default=None, help="Filter by plugin name.")
@click.option("--run-id", default=None, help="Filter by run ID.")
@click.option("--sync-id", default=None, help="Filter by sync ID.")
@click.option("--limit", default=100, type=int, help="Maximum events to show.")
@click.pass_context
def events_cmd(
    ctx: click.Context,
    directory: str,
    level: str | None,
    event_type: str | None,
    plugin: str | None,
    run_id: str | None,
    sync_id: str | None,
    limit: int,
) -> None:
    """Show structured event log for DIRECTORY."""
    from fin123.logging.sink import EventSink

    project_dir = Path(directory)
    sink = EventSink(project_dir)
    events = sink.read_global(
        level=level,
        event_type=event_type,
        plugin=plugin,
        run_id=run_id,
        sync_id=sync_id,
        limit=limit,
    )

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "events", {"events": events}))
        return

    if not events:
        _emit(ctx, "No events found.")
        return

    for evt in events:
        ts = evt.get("ts", "")
        lvl = evt.get("level", "").upper()
        etype = evt.get("event_type", "")
        msg = evt.get("message", "")
        err = evt.get("error_code")
        line = f"[{ts}] {lvl:7s} {etype}: {msg}"
        if err:
            line += f"  ({err})"
        _emit(ctx, line)


@main.command("run-log")
@click.argument("directory", type=click.Path(exists=True))
@click.argument("run_id")
@click.pass_context
def run_log_cmd(ctx: click.Context, directory: str, run_id: str) -> None:
    """Show event log for a specific run."""
    from fin123.logging.sink import EventSink

    project_dir = Path(directory)
    sink = EventSink(project_dir)
    events = sink.read_run_log(run_id)

    if ctx.obj.get("json"):
        click.echo(_json_out(True, "run-log", {"events": events, "run_id": run_id}))
        return

    if not events:
        _emit(ctx, f"No events found for run {run_id}.")
        return

    for evt in events:
        ts = evt.get("ts", "")
        lvl = evt.get("level", "").upper()
        etype = evt.get("event_type", "")
        msg = evt.get("message", "")
        err = evt.get("error_code")
        line = f"[{ts}] {lvl:7s} {etype}: {msg}"
        if err:
            line += f"  ({err})"
        _emit(ctx, line)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------


@main.group()
def demo() -> None:
    """Run built-in demos."""


@demo.command("ai-governance")
def demo_ai_governance() -> None:
    """Demo 1: AI governance -- plugin validation + compliance report."""
    from demos.ai_governance_demo.run import run_demo

    run_demo()


@demo.command("deterministic-build")
def demo_deterministic_build() -> None:
    """Demo 2: Deterministic build -- scaffold, build, verify with stable hashes."""
    from demos.deterministic_build_demo.run import run_demo

    run_demo()


@demo.command("batch-sweep")
def demo_batch_sweep() -> None:
    """Demo 3: Batch sweep -- 3-scenario parameter grid with stable manifest."""
    from demos.batch_sweep_demo.run import run_demo

    run_demo()


@demo.command("data-guardrails")
def demo_data_guardrails() -> None:
    """Demo 4: Data guardrails -- join validation failures + success."""
    from demos.data_guardrails_demo.run import run_demo

    run_demo()


# ---------------------------------------------------------------------------
# Doctor
# ---------------------------------------------------------------------------


@main.command()
@click.pass_context
def doctor(ctx: click.Context) -> None:
    """Preflight and compliance validation.

    Runs deterministic self-tests, dependency checks, and environment
    validation. Returns exit 0 if all required checks pass.

    Exit codes:
      0  All required checks passed
      3  Determinism or integrity failure
      5  Environment or dependency failure

    Examples:

      fin123 doctor
      fin123 doctor --json
      fin123 doctor --verbose
    """
    from fin123.doctor import run_doctor
    checks = run_doctor(verbose=ctx.obj.get("verbose", False), is_enterprise=False)
    _doctor_output(ctx, checks)


def _doctor_output(ctx: click.Context, checks: list[dict[str, Any]]) -> None:
    """Shared output formatter for doctor results."""
    # Enterprise stubs (exit_code=4) are informational in core, not real errors
    real_errors = sum(
        1 for c in checks
        if not c["ok"] and c["severity"] == "error" and not c.get("enterprise_only")
    )
    enterprise_stubs = sum(1 for c in checks if c.get("enterprise_only"))
    warnings = sum(1 for c in checks if not c["ok"] and c["severity"] == "warning")

    if ctx.obj.get("json"):
        exit_code = EXIT_OK
        error_obj = None
        if real_errors > 0:
            for c in checks:
                if not c["ok"] and c["severity"] == "error" and not c.get("enterprise_only"):
                    exit_code = c.get("exit_code", EXIT_DEPENDENCY)
                    break
            error_obj = {"code": exit_code, "message": f"{real_errors} check(s) failed"}

        click.echo(_json_out(
            real_errors == 0,
            "doctor",
            data={
                "summary": {
                    "errors": real_errors,
                    "warnings": warnings,
                    "enterprise_stubs": enterprise_stubs,
                },
                "checks": checks,
            },
            error=error_obj,
        ))
        if real_errors > 0:
            sys.exit(exit_code)
        return

    # Human output
    label_width = 30
    for c in checks:
        name = c["name"]
        if c["ok"]:
            status = "OK"
        elif c["severity"] == "warning":
            detail = c.get("details", {}).get("message", "")
            status = f"WARNING ({detail})" if detail else "WARNING"
        elif c.get("enterprise_only"):
            status = "ENTERPRISE (core)"
        else:
            status = "FAIL"
        dots = "." * (label_width - len(name) - 1)
        _emit(ctx, f"{name} {dots} {status}")

    _emit(ctx, "")
    if real_errors > 0:
        _emit(ctx, f"Overall: FAIL ({real_errors} error(s), {warnings} warning(s))")
        exit_code = EXIT_DEPENDENCY
        for c in checks:
            if not c["ok"] and c["severity"] == "error" and not c.get("enterprise_only"):
                exit_code = c.get("exit_code", EXIT_DEPENDENCY)
                break
        sys.exit(exit_code)
    elif warnings > 0:
        _emit(ctx, f"Overall: PASS ({warnings} warning(s))")
    else:
        _emit(ctx, "Overall: PASS")


# ---------------------------------------------------------------------------
# Enterprise stubs -- registry, plugins, server
# ---------------------------------------------------------------------------


@main.group()
def registry() -> None:
    """Registry operations (enterprise).

    Requires fin123-pod for full functionality.
    """


@registry.command("status")
@click.pass_context
def registry_status(ctx: click.Context) -> None:
    """Show registry status.

    Examples:

      fin123 registry status
      fin123 registry status --json
    """
    _enterprise_stub(ctx, "registry status")


@registry.command("sync")
@click.pass_context
def registry_sync(ctx: click.Context) -> None:
    """Sync with registry.

    Examples:

      fin123 registry sync
      fin123 registry sync --json
    """
    _enterprise_stub(ctx, "registry sync")


@main.group()
def plugins() -> None:
    """Plugin management (enterprise).

    Requires fin123-pod for full functionality.
    """


@plugins.command("list")
@click.pass_context
def plugins_list(ctx: click.Context) -> None:
    """List installed plugins.

    Examples:

      fin123 plugins list
      fin123 plugins list --json
    """
    _enterprise_stub(ctx, "plugins list")


@plugins.command("run")
@click.argument("plugin_name")
@click.option("--input", "input_path", default=None, type=click.Path(), help="Input file or reference.")
@click.pass_context
def plugins_run(ctx: click.Context, plugin_name: str, input_path: str | None) -> None:
    """Run a plugin.

    Examples:

      fin123 plugins run my_plugin --input data.csv
      fin123 plugins run my_plugin --json
    """
    _enterprise_stub(ctx, "plugins run")


@main.group()
def server() -> None:
    """Runner service (enterprise).

    Requires fin123-pod for full functionality.
    """


@server.command("start")
@click.option("--port", type=int, default=9188, help="Port to bind to.")
@click.pass_context
def server_start(ctx: click.Context, port: int) -> None:
    """Start the runner service.

    Examples:

      fin123 server start
      fin123 server start --port 8080
      fin123 server start --json
    """
    _enterprise_stub(ctx, "server start")


@server.command("status")
@click.pass_context
def server_status(ctx: click.Context) -> None:
    """Show runner service status.

    Examples:

      fin123 server status
      fin123 server status --json
    """
    _enterprise_stub(ctx, "server status")


if __name__ == "__main__":
    main()
