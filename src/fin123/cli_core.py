"""Command-line interface for fin123-core (standalone local engine + UI)."""

from __future__ import annotations

import json
from pathlib import Path

import click

from fin123 import __core_api_version__, __version__


def _warn_namespace_collision() -> None:
    """Warn if fin123-pod is installed in the same environment."""
    import importlib.metadata
    import sys

    for dist_name in ("fin123-pod", "fin123_pod"):
        try:
            importlib.metadata.version(dist_name)
        except importlib.metadata.PackageNotFoundError:
            continue
        print(
            "WARNING: fin123-pod is installed in this environment; "
            "fin123-core CLI may be shadowed. "
            "Prefer using a clean venv or install only fin123 (Pod) for enterprise.",
            file=sys.stderr,
        )
        break


@click.group()
@click.version_option(
    version=f"{__version__} (core_api={__core_api_version__})",
    prog_name="fin123-core",
)
def main() -> None:
    """fin123-core -- deterministic financial model engine with local UI.

    Lifecycle: Edit -> Commit -> Build -> Verify
    """
    _warn_namespace_collision()


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
# New
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path())
@click.option("--template", "template_name", default=None, help="Scaffold from a bundled template.")
@click.option("--template-dir", "template_dir", default=None, type=click.Path(exists=True), help="Scaffold from a local template directory.")
@click.option("--set", "overrides", multiple=True, help="Override template params as key=value.")
def new(directory: str, template_name: str | None, template_dir: str | None, overrides: tuple[str, ...]) -> None:
    """Scaffold a new project at DIRECTORY."""
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
            click.echo(f"Created project from template at {result}")
        except (FileExistsError, FileNotFoundError, ValueError) as e:
            raise click.ClickException(str(e))
    else:
        if overrides:
            raise click.ClickException("--set requires --template or --template-dir")
        from fin123.project import scaffold_project

        try:
            result = scaffold_project(target)
            click.echo(f"Created project at {result}")
        except FileExistsError as e:
            raise click.ClickException(str(e))


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------


@main.group()
def template() -> None:
    """Template management commands."""


@template.command("list")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
def template_list(as_json: bool) -> None:
    """List available project templates."""
    import json as _json

    from fin123.template_engine import list_templates

    templates = list_templates()
    if as_json:
        out = [
            {
                "name": t["name"],
                "description": t["description"],
                "invariants": t.get("invariants", []),
                "params": list((t.get("params") or {}).keys()),
            }
            for t in templates
        ]
        click.echo(_json.dumps(out, indent=2))
    else:
        if not templates:
            click.echo("No templates found.")
            return
        for t in templates:
            params = list((t.get("params") or {}).keys())
            params_str = f"  params: {', '.join(params)}" if params else ""
            click.echo(f"  {t['name']:20s} {t['description']}{params_str}")


@template.command("show")
@click.argument("name")
def template_show(name: str) -> None:
    """Show template details and file tree."""
    from fin123.template_engine import show_template

    try:
        info = show_template(name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    meta = info["meta"]
    click.echo(f"Template: {meta['name']}")
    click.echo(f"Description: {meta['description']}")
    click.echo(f"Engine compat: {meta.get('engine_compat', 'n/a')}")
    click.echo(f"Invariants: {', '.join(meta.get('invariants', []))}")
    params = meta.get("params") or {}
    if params:
        click.echo("Parameters:")
        for pname, pdef in params.items():
            click.echo(f"  {pname}: {pdef['type']} (default: {pdef['default']})")
    click.echo("Files:")
    for f in info["files"]:
        click.echo(f"  {f}")


# ---------------------------------------------------------------------------
# Commit
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path(exists=True))
def commit(directory: str) -> None:
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
    click.echo(f"Committed snapshot: {version}")


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _do_build(
    project_dir: Path,
    overrides: tuple[str, ...],
    scenario_name: str | None,
    all_scenarios: bool,
) -> None:
    """Core build logic."""
    from fin123.workbook import Workbook

    params = _parse_overrides(overrides)

    if all_scenarios:
        wb_probe = Workbook(project_dir)
        scenario_names = wb_probe.get_scenario_names()
        if not scenario_names:
            raise click.ClickException("No scenarios defined in workbook.yaml")
        click.echo(f"Building {len(scenario_names)} scenario(s): {', '.join(scenario_names)}")
        for sname in scenario_names:
            wb = Workbook(project_dir, overrides=params, scenario_name=sname)
            result = wb.run()
            click.echo(f"  [{sname}] Build saved to: {result.run_dir.name}")
        return

    wb = Workbook(project_dir, overrides=params, scenario_name=scenario_name)
    result = wb.run()

    click.echo(f"Build saved to: {result.run_dir.name}")
    click.echo(f"Scalars: {len(result.scalars)}")
    click.echo(f"Tables: {', '.join(result.tables.keys())}")


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--set", "overrides", multiple=True, help="Override params as key=value.")
@click.option("--scenario", "scenario_name", default=None, help="Build a named scenario.")
@click.option("--all-scenarios", is_flag=True, help="Build all scenarios.")
def build(directory: str, overrides: tuple[str, ...], scenario_name: str | None, all_scenarios: bool) -> None:
    """Build (execute) the workbook in DIRECTORY.

    Lifecycle: Edit -> Commit -> *Build* -> Verify
    """
    _do_build(Path(directory), overrides, scenario_name, all_scenarios)


# ---------------------------------------------------------------------------
# Artifact
# ---------------------------------------------------------------------------


@main.group()
def artifact() -> None:
    """Artifact commands."""


@artifact.command("list")
@click.argument("directory", type=click.Path(exists=True))
def artifact_list(directory: str) -> None:
    """List all artifacts in DIRECTORY."""
    from fin123.versioning import ArtifactStore

    project_dir = Path(directory)
    store = ArtifactStore(project_dir)
    artifacts = store.list_artifacts()

    if not artifacts:
        click.echo("No artifacts found.")
        return

    for name, versions in artifacts.items():
        click.echo(f"{name}:")
        for v in versions:
            click.echo(
                f"  {v['version']}  {v['created_at']}  "
                f"status={v['status']}  workflow={v['workflow_name']}"
            )


@artifact.command("approve")
@click.argument("artifact_name")
@click.argument("version")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.option("--by", "approved_by", default="", help="Approver identifier.")
@click.option("--note", default="", help="Free-text note.")
def artifact_approve(artifact_name: str, version: str, directory: str, approved_by: str, note: str) -> None:
    """Approve an artifact version."""
    from fin123.versioning import ArtifactStore

    store = ArtifactStore(Path(directory))
    try:
        approval = store.approve_artifact(artifact_name, version, approved_by=approved_by, note=note)
        click.echo(f"Artifact {artifact_name} {version}: {approval['status']}")
    except FileNotFoundError as e:
        raise click.ClickException(str(e))


@artifact.command("reject")
@click.argument("artifact_name")
@click.argument("version")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.option("--by", "approved_by", default="", help="Rejector identifier.")
@click.option("--note", default="", help="Free-text note.")
@click.option("--reason-code", default="", help="Machine-readable rejection reason.")
def artifact_reject(artifact_name: str, version: str, directory: str, approved_by: str, note: str, reason_code: str) -> None:
    """Reject an artifact version."""
    from fin123.versioning import ArtifactStore

    store = ArtifactStore(Path(directory))
    try:
        approval = store.reject_artifact(artifact_name, version, approved_by=approved_by, note=note, reason_code=reason_code)
        click.echo(f"Artifact {artifact_name} {version}: {approval['status']}")
    except FileNotFoundError as e:
        raise click.ClickException(str(e))


@artifact.command("status")
@click.argument("artifact_name")
@click.argument("version")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
def artifact_status(artifact_name: str, version: str, directory: str) -> None:
    """Show the approval status of an artifact version."""
    from fin123.versioning import ArtifactStore

    store = ArtifactStore(Path(directory))
    try:
        approval = store.get_artifact_approval(artifact_name, version)
        click.echo(f"Artifact: {artifact_name} {version}")
        click.echo(f"Status: {approval['status']}")
        if approval.get("approved_by"):
            click.echo(f"By: {approval['approved_by']}")
        if approval.get("approved_at"):
            click.echo(f"At: {approval['approved_at']}")
        if approval.get("note"):
            click.echo(f"Note: {approval['note']}")
        if approval.get("reason_code"):
            click.echo(f"Reason: {approval['reason_code']}")
    except FileNotFoundError as e:
        raise click.ClickException(str(e))


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


@main.group()
def diff() -> None:
    """Diff commands for comparing runs and versions."""


@diff.command("run")
@click.argument("run_a")
@click.argument("run_b")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
def diff_run(run_a: str, run_b: str, directory: str, as_json: bool) -> None:
    """Compare two runs."""
    import sys

    from fin123.diff import diff_runs, format_run_diff

    try:
        result = diff_runs(Path(directory), run_a, run_b)
    except FileNotFoundError as e:
        click.echo(str(e), err=True)
        sys.exit(2)

    if as_json:
        click.echo(json.dumps(result, indent=2, default=str))
    else:
        click.echo(format_run_diff(result))


@diff.command("version")
@click.argument("version_a")
@click.argument("version_b")
@click.option("--project", "directory", default=".", type=click.Path(exists=True), help="Project directory.")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
def diff_version(version_a: str, version_b: str, directory: str, as_json: bool) -> None:
    """Compare two workbook snapshot versions."""
    import sys

    from fin123.diff import diff_versions, format_version_diff

    try:
        result = diff_versions(Path(directory), version_a, version_b)
    except FileNotFoundError as e:
        click.echo(str(e), err=True)
        sys.exit(2)

    if as_json:
        click.echo(json.dumps(result, indent=2, default=str))
    else:
        click.echo(format_version_diff(result))


# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------


def _do_verify_build(run_id: str, directory: str, as_json: bool) -> None:
    from fin123.logging.events import set_project_dir
    from fin123.verify import verify_run

    project_dir = Path(directory)
    set_project_dir(project_dir)
    report = verify_run(project_dir, run_id)

    if report.get("no_run"):
        if as_json:
            click.echo(json.dumps(report, indent=2))
        else:
            click.echo(f"Verify build: {run_id}")
            click.echo("Status: FAIL")
            click.echo(f"  no completed build run found for project '{run_id}'")
            click.echo("")
            click.echo("Next steps:")
            click.echo(f"  Run:  fin123-core build <project_dir>")
            click.echo(f"  Or:   open the UI and click Build")
        raise SystemExit(2)

    if as_json:
        click.echo(json.dumps(report, indent=2))
    else:
        click.echo(f"Verify build: {run_id}")
        click.echo(f"Status: {report['status'].upper()}")
        if report["failures"]:
            for f in report["failures"]:
                click.echo(f"  FAIL: {f}")
        else:
            click.echo("  All checks passed.")

    if report["status"] != "pass":
        raise SystemExit(2)


@main.command("verify-build")
@click.argument("run_id")
@click.option("--project", "directory", type=click.Path(exists=True), default=".", help="Project directory.")
@click.option("--json", "as_json", is_flag=True, help="Output report as JSON.")
def verify_build_cmd(run_id: str, directory: str, as_json: bool) -> None:
    """Verify the integrity of a completed build.

    Lifecycle: Edit -> Commit -> Build -> *Verify*
    """
    _do_verify_build(run_id, directory, as_json)


# ---------------------------------------------------------------------------
# GC
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without deleting.")
def gc(directory: str, dry_run: bool) -> None:
    """Run garbage collection on DIRECTORY."""
    from fin123.gc import run_gc

    project_dir = Path(directory)
    summary = run_gc(project_dir, dry_run=dry_run)

    label = "GC dry-run:" if dry_run else "GC complete:"
    click.echo(label)
    click.echo(f"  Runs deleted: {summary['runs_deleted']}")
    click.echo(f"  Artifact versions deleted: {summary['artifact_versions_deleted']}")
    click.echo(f"  Sync runs deleted: {summary['sync_runs_deleted']}")
    click.echo(f"  Model versions deleted: {summary['model_versions_deleted']}")
    click.echo(f"  Bytes freed: {summary['bytes_freed']:,}")
    click.echo(f"  Orphaned dirs cleaned: {summary['orphaned_cleaned']}")


@main.command("clear-cache")
@click.argument("directory", type=click.Path(exists=True))
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without deleting.")
@click.option("--aggressive", is_flag=True, help="Apply lower retention thresholds.")
def clear_cache(directory: str, dry_run: bool, aggressive: bool) -> None:
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

    label = "Clear-cache dry-run:" if dry_run else "Clear-cache complete:"
    click.echo(label)
    click.echo(f"  Runs deleted: {summary['runs_deleted']}")
    click.echo(f"  Artifact versions deleted: {summary['artifact_versions_deleted']}")
    click.echo(f"  Sync runs deleted: {summary['sync_runs_deleted']}")
    click.echo(f"  Model versions deleted: {summary['model_versions_deleted']}")
    click.echo(f"  Hash cache cleared: {hash_cache_size:,} bytes")
    click.echo(f"  Bytes freed: {summary['bytes_freed'] + hash_cache_size:,}")


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--latest", is_flag=True, default=True, help="Export the latest run.")
def export(directory: str, latest: bool) -> None:
    """Export run outputs from DIRECTORY."""
    from fin123.versioning import RunStore

    project_dir = Path(directory)
    run_store = RunStore(project_dir)
    runs = run_store.list_runs()

    if not runs:
        raise click.ClickException("No runs found.")

    run_meta = runs[-1]
    run_dir = project_dir / "runs" / run_meta["run_id"]
    outputs_dir = run_dir / "outputs"

    click.echo(f"Exporting run: {run_meta['run_id']}")
    click.echo(f"Timestamp: {run_meta['timestamp']}")

    scalars_path = outputs_dir / "scalars.json"
    if scalars_path.exists():
        scalars = json.loads(scalars_path.read_text())
        click.echo("Scalars:")
        click.echo(json.dumps(scalars, indent=2))

    for f in sorted(outputs_dir.iterdir()):
        if f.suffix == ".parquet":
            import polars as pl

            df = pl.read_parquet(f)
            click.echo(f"\nTable: {f.stem}")
            click.echo(str(df))


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
def batch_build(directory: str, params_file: str, scenario_name: str | None, max_workers: int) -> None:
    """Build the workbook once per row in a params CSV file."""
    from fin123.batch import load_params_csv, run_batch

    project_dir = Path(directory)
    params_path = Path(params_file)

    rows = load_params_csv(params_path)
    if not rows:
        click.echo("No parameter rows found in CSV.")
        return

    click.echo(f"Batch build: {len(rows)} parameter set(s) from {params_path.name}")
    if scenario_name:
        click.echo(f"Scenario: {scenario_name}")
    if max_workers > 1:
        click.echo(f"Parallel workers: {max_workers}")

    summary = run_batch(project_dir, rows, scenario_name=scenario_name, max_workers=max_workers)

    click.echo(f"\nBatch ID: {summary['build_batch_id']}")
    click.echo(f"Total: {summary['total']}  OK: {summary['ok']}  Failed: {summary['failed']}")
    for r in summary["results"]:
        if r["status"] == "ok":
            click.echo(f"  [{r['index']}] OK  run_id={r['run_id']}")
        else:
            click.echo(f"  [{r['index']}] FAIL  {r['error']}")


# ---------------------------------------------------------------------------
# Import XLSX
# ---------------------------------------------------------------------------


@main.command("import-xlsx")
@click.argument("xlsx_file", type=click.Path(exists=True))
@click.argument("directory", type=click.Path())
@click.option("--max-rows", type=int, default=500, help="Max rows per sheet to import.")
@click.option("--max-cols", type=int, default=100, help="Max columns per sheet to import.")
def import_xlsx(xlsx_file: str, directory: str, max_rows: int, max_cols: int) -> None:
    """Import an XLSX file into a new fin123 project at DIRECTORY."""
    from fin123.xlsx_import import import_xlsx as do_import

    xlsx_path = Path(xlsx_file)
    target = Path(directory)

    try:
        report = do_import(xlsx_path, target, max_rows=max_rows, max_cols=max_cols)
    except ImportError as e:
        raise click.ClickException(str(e))

    click.echo(f"Imported {xlsx_path.name} -> {target}")
    for s in report["sheets_imported"]:
        click.echo(
            f"  {s['name']}: {s['cells']} cells, "
            f"{s['formulas']} formulas, {s['colors']} colors"
        )
    if report["skipped_features"]:
        click.echo(f"  Skipped: {', '.join(report['skipped_features'])}")
    if report["warnings"]:
        for w in report["warnings"]:
            click.echo(f"  Warning: {w}", err=True)
    click.echo(f"Report: {target / 'import_report.json'}")


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


@main.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--port", type=int, default=None, help="Port (auto-select if omitted).")
@click.option("--no-open", is_flag=True, help="Don't auto-open browser.")
def ui(directory: str, host: str, port: int | None, no_open: bool) -> None:
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
    click.echo(f"Serving UI at {url}")
    click.echo("Press Ctrl+C to stop")

    if not no_open:
        import threading
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        uvicorn.run(app, host=host, port=port, log_level="warning")
    except KeyboardInterrupt:
        click.echo("\nStopped.")


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
def events_cmd(
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

    if not events:
        click.echo("No events found.")
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
        click.echo(line)


@main.command("run-log")
@click.argument("directory", type=click.Path(exists=True))
@click.argument("run_id")
def run_log_cmd(directory: str, run_id: str) -> None:
    """Show event log for a specific run."""
    from fin123.logging.sink import EventSink

    project_dir = Path(directory)
    sink = EventSink(project_dir)
    events = sink.read_run_log(run_id)

    if not events:
        click.echo(f"No events found for run {run_id}.")
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
        click.echo(line)


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


if __name__ == "__main__":
    main()
