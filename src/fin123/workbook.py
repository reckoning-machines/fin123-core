"""Workbook: the central unit of computation in fin123.

A workbook is defined by a YAML spec and orchestrates evaluation of both the
scalar graph and the table graph in a deterministic order.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import polars as pl
import yaml

# Ensure built-in functions are registered on import
import fin123.functions.scalar  # noqa: F401
import fin123.functions.table  # noqa: F401
from fin123.formulas import parse_formula, extract_refs
from fin123.project import ensure_model_id
from fin123.scalars import ScalarGraph
from fin123.tables import TableGraph
from fin123.utils.hash import InputHashCache
from fin123.versioning import RunStore, SnapshotStore


def _resolve_cache_path(project_dir: Path, cache_rel_path: str) -> Path:
    """Resolve the actual file path for a SQL cache entry.

    Checks the ``.current.json`` pointer first.  If the pointer's
    ``active_path`` refers to an existing file, that path is returned.
    Otherwise falls back to the direct cache path.
    """
    cache_path = project_dir / cache_rel_path
    pointer_path = cache_path.with_suffix(".current.json")

    if pointer_path.exists():
        try:
            pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
            active_path = pointer.get("active_path")
            if active_path:
                resolved = project_dir / active_path
                if resolved.exists():
                    return resolved
        except (json.JSONDecodeError, OSError):
            pass

    return cache_path


class WorkbookResult:
    """Container for the outputs of a workbook run.

    Attributes:
        scalars: Computed scalar values.
        tables: Computed table DataFrames.
        run_dir: Path to the persisted run directory.
    """

    def __init__(
        self,
        scalars: dict[str, Any],
        tables: dict[str, pl.DataFrame],
        run_dir: Path,
    ) -> None:
        """Initialize a WorkbookResult.

        Args:
            scalars: Computed scalar values.
            tables: Computed table DataFrames.
            run_dir: Path to the persisted run directory.
        """
        self.scalars = scalars
        self.tables = tables
        self.run_dir = run_dir


class Workbook:
    """Loads a workbook YAML spec and orchestrates evaluation.

    Usage::

        wb = Workbook(Path("my_project"))
        result = wb.run()
        print(result.scalars)
    """

    def __init__(
        self,
        project_dir: Path,
        overrides: dict[str, Any] | None = None,
        scenario_name: str | None = None,
    ) -> None:
        """Initialize a Workbook from a project directory.

        Args:
            project_dir: Path to the project root containing ``workbook.yaml``.
            overrides: Optional parameter overrides (e.g. from CLI ``--set``).
            scenario_name: Optional scenario name from workbook.yaml scenarios.
        """
        self.project_dir = project_dir.resolve()
        self.spec_path = self.project_dir / "workbook.yaml"
        if not self.spec_path.exists():
            raise FileNotFoundError(f"No workbook.yaml found in {self.project_dir}")

        self.raw_yaml = self.spec_path.read_text()
        self.spec: dict[str, Any] = yaml.safe_load(self.raw_yaml)
        self.overrides = overrides or {}
        self.scenario_name = scenario_name or ""

        # Resolve and store scenario-only overrides (for overlay_hash)
        self._scenario_overrides: dict[str, Any] = {}
        if self.scenario_name:
            self._scenario_overrides = self._resolve_scenario(self.scenario_name)
            # committed defaults -> scenario overrides -> per-build overrides
            merged = dict(self._scenario_overrides)
            merged.update(self.overrides)
            self.overrides = merged

        # Ensure model_id exists
        ensure_model_id(self.spec, self.spec_path)

    def run(self) -> WorkbookResult:
        """Execute the workbook: evaluate scalars and tables, persist results.

        Tables are evaluated first so that their materialized DataFrames can
        serve as a lookup cache for ``lookup_scalar`` calls in the scalar graph.

        Active plugins are loaded before evaluation so that plugin-registered
        scalar functions are available in the formula engine.

        Returns:
            A WorkbookResult with all computed outputs.
        """
        from fin123.logging.events import EventLevel, EventType, emit, make_run_event, set_project_dir
        from fin123.utils.hash import compute_export_hash, compute_params_hash, compute_plugin_hash_combined, overlay_hash

        # Initialise event logging for this project
        set_project_dir(self.project_dir)

        run_id: str | None = None
        timings_ms: dict[str, float] = {}

        # Compute overlay hash early for the build-start event
        scenario_overlay_hash_early = overlay_hash(self.scenario_name, self._scenario_overrides)

        # Emit run_started
        _start_extra: dict[str, Any] = {}
        if self.scenario_name:
            _start_extra["scenario_name"] = self.scenario_name
            _start_extra["overlay_hash"] = scenario_overlay_hash_early[:12]
        emit(make_run_event(
            EventType.run_started,
            EventLevel.info,
            "Workbook run started",
            model_id=str(self.spec.get("model_id", "")),
            extra=_start_extra or None,
        ))

        try:
            # Load active plugins (registers scalar functions, records versions)
            plugins_info = self._load_plugins()

            # Snapshot the workbook spec
            snapshot_store = SnapshotStore(self.project_dir)
            snapshot_version = snapshot_store.save_snapshot(self.raw_yaml)

            # Resolve parameters
            t0 = time.monotonic()
            params = dict(self.spec.get("params", {}))
            params.update(self.overrides)
            effective_params = dict(params)
            effective_params_hash = compute_params_hash(effective_params)
            timings_ms["resolve_params"] = round((time.monotonic() - t0) * 1000, 2)

            # Hash inputs
            t0 = time.monotonic()
            hash_cache = InputHashCache(self.project_dir / "cache" / "hashes.json")
            input_paths = self._collect_input_paths()
            input_hashes = hash_cache.hashes_for(input_paths)
            timings_ms["hash_inputs"] = round((time.monotonic() - t0) * 1000, 2)

            # Build and evaluate table graph first (needed for lookup_scalar cache)
            t0 = time.monotonic()
            table_graph = self._build_table_graph(params)
            table_frames = table_graph.evaluate()
            timings_ms["eval_tables"] = round((time.monotonic() - t0) * 1000, 2)

            # Enforce primary_key uniqueness on tables that declare one
            self._enforce_primary_keys(table_frames)

            # Build and evaluate scalar graph with table cache for lookups
            t0 = time.monotonic()
            scalar_graph = self._build_scalar_graph(params, table_cache=table_frames)
            scalar_values = scalar_graph.evaluate()
            timings_ms["eval_scalars"] = round((time.monotonic() - t0) * 1000, 2)

            # Determine which outputs to export
            output_scalars = self._select_scalar_outputs(scalar_values)
            output_tables = self._select_table_outputs(table_frames)

            # Track which plans have explicit sort steps
            sorted_tables = self._sorted_plan_names()

            # Compute overlay hash (scenario-only overrides, not CLI overrides)
            scenario_overlay_hash = overlay_hash(self.scenario_name, self._scenario_overrides)

            # Compute plugin hash
            from fin123 import __version__
            plugin_hash = compute_plugin_hash_combined(__version__, plugins_info)

            # Compute plugin lock hash if plugins.lock exists
            plugin_lock_hash = ""
            plugin_lock_hash_mode = ""
            plugins_lock_path = self.project_dir / "plugins.lock"
            if plugins_lock_path.exists():
                from fin123.utils.hash import sha256_canonical_json_file
                plugin_lock_hash, plugin_lock_hash_mode = sha256_canonical_json_file(
                    plugins_lock_path
                )

            # Evaluate assertions
            assertion_report = self._evaluate_assertions(scalar_values, run_id)

            # Persist run
            t0 = time.monotonic()
            run_store = RunStore(self.project_dir)
            run_dir = run_store.create_run(
                workbook_spec=self.spec,
                input_hashes=input_hashes,
                scalar_outputs=output_scalars,
                table_outputs=output_tables,
                sorted_tables=sorted_tables,
                model_id=self.spec.get("model_id"),
                model_version_id=snapshot_version,
                plugins=plugins_info,
            )
            run_id = run_dir.name

            # Compute export hash and amend run_meta.json with new fields
            export_hash = compute_export_hash(run_dir / "outputs")
            timings_ms["export_outputs"] = round((time.monotonic() - t0) * 1000, 2)

            self._amend_run_meta(
                run_dir,
                scenario_name=self.scenario_name,
                overlay_hash=scenario_overlay_hash,
                plugin_hash=plugin_hash,
                export_hash=export_hash,
                timings_ms=timings_ms,
                assertion_report=assertion_report,
                params_hash=effective_params_hash,
                effective_params=effective_params,
                plugin_lock_hash=plugin_lock_hash,
                plugin_lock_hash_mode=plugin_lock_hash_mode,
            )

            # Emit timing events
            emit(
                make_run_event(
                    EventType.run_timing,
                    EventLevel.info,
                    f"Run timings: {timings_ms}",
                    run_id=run_id,
                    model_id=str(self.spec.get("model_id", "")),
                    extra={"timings_ms": timings_ms},
                ),
                run_id=run_id,
            )

            # Push to registry if enabled (never fatal)
            run_meta_for_registry = {
                "run_id": run_dir.name,
                "scalars": {k: str(v) for k, v in output_scalars.items()},
            }
            self._registry_push(
                snapshot_version=snapshot_version,
                run_dir_name=run_dir.name,
                workbook_hash=hash_cache.hashes_for([self.spec_path]).get(
                    str(self.spec_path), ""
                ),
                run_meta=run_meta_for_registry,
                scenario_name=self.scenario_name,
                overlay_hash=scenario_overlay_hash,
                params_hash=effective_params_hash,
                plugin_hash=plugin_hash,
                export_hash=export_hash,
            )

            # Emit run_completed
            emit(
                make_run_event(
                    EventType.run_completed,
                    EventLevel.info,
                    f"Workbook run completed: {run_dir.name}",
                    run_id=run_dir.name,
                    model_id=str(self.spec.get("model_id", "")),
                    model_version_id=snapshot_version,
                    extra={
                        "scalar_count": len(output_scalars),
                        "table_count": len(output_tables),
                        "scenario_name": self.scenario_name,
                        "overlay_hash": scenario_overlay_hash[:12],
                        "assertions_status": assertion_report.get("status", "pass"),
                    },
                ),
                run_id=run_dir.name,
            )

            return WorkbookResult(
                scalars=output_scalars,
                tables=output_tables,
                run_dir=run_dir,
            )

        except Exception as exc:
            # Emit run_failed (stacktrace only in per-run log, not global)
            emit(
                make_run_event(
                    EventType.run_failed,
                    EventLevel.error,
                    f"Workbook run failed: {exc}",
                    run_id=run_id,
                    model_id=str(self.spec.get("model_id", "")),
                    extra={"error": str(exc)},
                ),
                run_id=run_id,
            )
            raise

    def _registry_push(
        self,
        snapshot_version: str,
        run_dir_name: str,
        workbook_hash: str,
        run_meta: dict[str, Any],
        *,
        scenario_name: str = "",
        overlay_hash: str = "",
        params_hash: str = "",
        plugin_hash: str = "",
        export_hash: str = "",
        build_batch_id: str | None = None,
    ) -> None:
        """Push version and build to registry if enabled. Never raises."""
        try:
            from uuid import UUID

            from fin123.project import load_project_config
            from fin123.registry.backend import get_registry
            from fin123.utils.hash import sha256_dict

            config = load_project_config(self.project_dir)
            registry = get_registry(self.project_dir, config)
            if registry is None:
                return

            model_id = UUID(str(self.spec.get("model_id", "")))
            wb_hash = sha256_dict(self.spec)

            # Upsert model + version
            registry.upsert_model(model_id)
            from fin123.registry.backend import parse_version_ordinal

            ordinal = parse_version_ordinal(snapshot_version)
            registry.put_model_version(
                model_id=model_id,
                model_version_id=snapshot_version,
                version_ordinal=ordinal,
                workbook_yaml=self.raw_yaml,
                workbook_hash=wb_hash,
            )

            # Push build if configured
            if config.get("registry_store_builds", False):
                registry.put_build(
                    run_id=run_dir_name,
                    model_id=model_id,
                    model_version_id=snapshot_version,
                    workbook_hash=wb_hash,
                    run_meta=run_meta,
                    scenario_name=scenario_name,
                    overlay_hash=overlay_hash,
                    params_hash=params_hash,
                    plugin_hash=plugin_hash,
                    export_hash=export_hash,
                    build_batch_id=build_batch_id,
                )

            # Backward compat: push to fin123_runs if store_runs enabled
            if config.get("registry_store_runs", False):
                registry.put_run(
                    run_id=run_dir_name,
                    model_id=model_id,
                    model_version_id=snapshot_version,
                    workbook_hash=wb_hash,
                    run_meta=run_meta,
                )
        except Exception:
            import logging

            logging.getLogger(__name__).debug(
                "Registry push failed (non-fatal)", exc_info=True
            )

    def _load_plugins(self) -> dict[str, dict[str, str]]:
        """Load active plugins from the project's plugins directory.

        Scalar plugin modules are imported, which triggers their
        ``@register_scalar`` decorators.  Plugin version and hash info
        is collected for recording in ``run_meta.json``.

        Returns:
            Dict mapping plugin names to ``{"version": ..., "sha256": ...}``.
            Empty dict if no plugins directory exists or no plugins are active.
        """
        try:
            from fin123.plugins.manager import load_active_plugins

            return load_active_plugins(self.project_dir)
        except Exception:
            import logging

            logging.getLogger(__name__).debug(
                "Plugin loading failed (non-fatal)", exc_info=True
            )
            return {}

    def _collect_input_paths(self) -> list[Path]:
        """Gather all input file paths referenced by the workbook spec.

        For SQL-sourced tables, the cache path is used as the input file.

        Returns:
            List of resolved file paths.
        """
        paths = []
        for table_spec in self.spec.get("tables", {}).values():
            source = table_spec.get("source")
            if source == "sql":
                # SQL tables use their cache file as the local input
                cache = table_spec.get("cache")
                if cache:
                    resolved = _resolve_cache_path(self.project_dir, cache)
                    if resolved.exists():
                        paths.append(resolved)
            elif source:
                paths.append(self.project_dir / source)
        return paths

    def _build_scalar_graph(
        self,
        params: dict[str, Any],
        table_cache: dict[str, pl.DataFrame] | None = None,
    ) -> ScalarGraph:
        """Construct the scalar graph from the workbook spec.

        Args:
            params: Resolved parameters (spec defaults + overrides).
            table_cache: Materialized table DataFrames for lookup_scalar.

        Returns:
            A populated ScalarGraph ready for evaluation.
        """
        sg = ScalarGraph()

        # Provide table cache for formula evaluation (VLOOKUP, SUMIFS, etc.)
        if table_cache:
            sg.set_table_cache(table_cache)

        # Load params as scalar values
        for name, value in params.items():
            sg.set_value(name, value)

        # Load scalar definitions from outputs
        for output_spec in self.spec.get("outputs", []):
            if output_spec.get("type") == "scalar":
                name = output_spec["name"]
                # Check in priority order:
                # 1. formula: "=..." — explicit formula key
                # 2. value: "=..." — string starting with = is a formula
                # 3. value: (non-formula) — literal value
                # 4. func: + args: — structured formula
                formula_text = output_spec.get("formula")
                value = output_spec.get("value")

                if formula_text and isinstance(formula_text, str) and formula_text.startswith("="):
                    tree = parse_formula(formula_text)
                    deps = extract_refs(tree)
                    sg.set_parsed_formula(name, tree, deps)
                elif value is not None and isinstance(value, str) and value.startswith("="):
                    tree = parse_formula(value)
                    deps = extract_refs(tree)
                    sg.set_parsed_formula(name, tree, deps)
                elif "value" in output_spec:
                    sg.set_value(name, output_spec["value"])
                elif "func" in output_spec:
                    args = dict(output_spec.get("args", {}))
                    # Inject table cache and project dir for lookup_scalar
                    if output_spec["func"] == "lookup_scalar":
                        args["_table_cache"] = table_cache or {}
                        args["_project_dir"] = str(self.project_dir)
                    sg.set_formula(name, output_spec["func"], args)

        return sg

    def _build_table_graph(self, params: dict[str, Any]) -> TableGraph:
        """Construct the table graph from the workbook spec.

        SQL-sourced tables are loaded from their local cache files (parquet).
        The workbook never executes SQL directly.

        Args:
            params: Resolved parameters (unused currently but available for
                    future parameterized table logic).

        Returns:
            A populated TableGraph ready for evaluation.
        """
        tg = TableGraph(self.project_dir)

        # Register source tables
        for name, table_spec in self.spec.get("tables", {}).items():
            source = table_spec.get("source")
            if source == "sql":
                # SQL tables read from their local cache file
                cache = table_spec.get("cache")
                if not cache:
                    raise ValueError(
                        f"SQL table {name!r} must specify a 'cache' path"
                    )
                resolved = _resolve_cache_path(self.project_dir, cache)
                if not resolved.exists():
                    raise FileNotFoundError(
                        f"Cache file for SQL table {name!r} not found at "
                        f"{resolved}. Run 'fin123 sync' first."
                    )
                # Determine format from file extension
                fmt = "parquet" if cache.endswith(".parquet") else "csv"
                # Use the resolved path relative to project_dir
                resolved_rel = str(resolved.relative_to(self.project_dir))
                tg.add_source(name, resolved_rel, format=fmt)
            elif source:
                fmt = table_spec.get("format", "csv")
                tg.add_source(name, source, format=fmt)

        # Register plans
        for plan_spec in self.spec.get("plans", []):
            tg.add_plan(
                name=plan_spec["name"],
                source=plan_spec["source"],
                steps=plan_spec.get("steps", []),
            )

        return tg

    def _enforce_primary_keys(self, table_frames: dict[str, pl.DataFrame]) -> None:
        """Validate primary key uniqueness on tables that declare one.

        Args:
            table_frames: Materialized table DataFrames.

        Raises:
            ValueError: If any declared primary key has duplicate values.
        """
        for name, table_spec in self.spec.get("tables", {}).items():
            pk = table_spec.get("primary_key")
            if not pk or name not in table_frames:
                continue
            df = table_frames[name]
            pk_cols = [pk] if isinstance(pk, str) else pk
            for col in pk_cols:
                if col not in df.columns:
                    continue
            dup_count = len(df) - df.select(pk_cols).unique().height
            if dup_count > 0:
                sample = df.group_by(pk_cols).len().filter(pl.col("len") > 1)
                sample_keys = sample.select(pk_cols).head(5).to_dicts()
                raise ValueError(
                    f"Table {name!r}: primary_key {pk_cols} has {dup_count} "
                    f"duplicate(s). Samples: {sample_keys}"
                )

    def _sorted_plan_names(self) -> set[str]:
        """Return the set of plan names that have an explicit sort step.

        Returns:
            Set of table names with explicit sort ordering.
        """
        names: set[str] = set()
        for plan_spec in self.spec.get("plans", []):
            for step in plan_spec.get("steps", []):
                if step.get("func") == "sort":
                    names.add(plan_spec["name"])
                    break
        return names

    def _select_scalar_outputs(self, all_scalars: dict[str, Any]) -> dict[str, Any]:
        """Filter scalars to only those declared as outputs.

        Args:
            all_scalars: All evaluated scalar values.

        Returns:
            Dict of output scalar values.
        """
        output_names = set()
        for output_spec in self.spec.get("outputs", []):
            if output_spec.get("type") == "scalar":
                output_names.add(output_spec["name"])
        if not output_names:
            return all_scalars
        return {k: v for k, v in all_scalars.items() if k in output_names}

    def _select_table_outputs(self, all_tables: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
        """Filter tables to only those declared as outputs.

        Args:
            all_tables: All evaluated table DataFrames.

        Returns:
            Dict of output table DataFrames.
        """
        output_names = set()
        for output_spec in self.spec.get("outputs", []):
            if output_spec.get("type") == "table":
                output_names.add(output_spec["name"])
        if not output_names:
            # If no table outputs specified, export all plan results
            return all_tables
        return {k: v for k, v in all_tables.items() if k in output_names}

    def _resolve_scenario(self, scenario_name: str) -> dict[str, Any]:
        """Resolve scenario overrides from workbook.yaml scenarios block.

        Args:
            scenario_name: Name of the scenario.

        Returns:
            Dict of parameter overrides for this scenario.

        Raises:
            ValueError: If the scenario is not found.
        """
        scenarios = self.spec.get("scenarios", {})
        if scenario_name not in scenarios:
            available = sorted(scenarios.keys()) if scenarios else []
            raise ValueError(
                f"Scenario {scenario_name!r} not found in workbook.yaml. "
                f"Available: {available}"
            )
        scenario_spec = scenarios[scenario_name]
        return dict(scenario_spec.get("overrides", {}))

    def get_scenario_names(self) -> list[str]:
        """Return sorted list of scenario names from workbook.yaml.

        Returns:
            List of scenario name strings.
        """
        return sorted(self.spec.get("scenarios", {}).keys())

    def _evaluate_assertions(
        self,
        scalar_values: dict[str, Any],
        run_id: str | None,
    ) -> dict[str, Any]:
        """Evaluate assertions defined in workbook.yaml.

        Args:
            scalar_values: Computed scalar values.
            run_id: Current run ID for event logging.

        Returns:
            Assertion report dict.
        """
        assertion_specs = self.spec.get("assertions", [])
        if not assertion_specs:
            return {"status": "pass", "results": [], "failed_count": 0, "warn_count": 0}

        from fin123.assertions import evaluate_assertions

        report = evaluate_assertions(assertion_specs, scalar_values)

        # Emit assertion events
        try:
            from fin123.logging.events import (
                EventLevel,
                EventType,
                emit,
                make_run_event,
            )

            for result in report["results"]:
                if result["ok"]:
                    etype = EventType.assertion_pass
                    level = EventLevel.info
                elif result["severity"] == "error":
                    etype = EventType.assertion_fail
                    level = EventLevel.error
                else:
                    etype = EventType.assertion_warn
                    level = EventLevel.warning

                emit(
                    make_run_event(
                        etype,
                        level,
                        f"Assertion '{result['name']}': {'ok' if result['ok'] else result['message']}",
                        run_id=run_id,
                        model_id=str(self.spec.get("model_id", "")),
                        extra={"assertion_name": result["name"], "ok": result["ok"]},
                    ),
                    run_id=run_id,
                )
        except Exception:
            pass

        return report

    @staticmethod
    def _amend_run_meta(
        run_dir: Path,
        scenario_name: str,
        overlay_hash: str,
        plugin_hash: str,
        export_hash: str,
        timings_ms: dict[str, float],
        assertion_report: dict[str, Any],
        params_hash: str = "",
        effective_params: dict[str, Any] | None = None,
        plugin_lock_hash: str = "",
        plugin_lock_hash_mode: str = "",
    ) -> None:
        """Amend run_meta.json with scenario, hash, timing, and assertion data.

        Args:
            run_dir: Path to the run directory.
            scenario_name: Scenario name (empty for default).
            overlay_hash: Computed overlay hash.
            plugin_hash: Combined plugin hash.
            export_hash: Hash of exported artifacts.
            timings_ms: Phase timing dict.
            assertion_report: Assertion evaluation report.
            params_hash: SHA-256 of effective parameters.
            effective_params: The resolved parameter dict.
            plugin_lock_hash: SHA-256 of plugins.lock (empty if none).
            plugin_lock_hash_mode: Hashing mode ("canonical_json" or "raw_bytes").
        """
        import os

        meta_path = run_dir / "run_meta.json"
        meta = json.loads(meta_path.read_text())
        meta["scenario_name"] = scenario_name
        meta["overlay_hash"] = overlay_hash
        meta["plugin_hash"] = plugin_hash
        meta["export_hash"] = export_hash
        meta["params_hash"] = params_hash
        meta["effective_params"] = effective_params or {}
        meta["timings_ms"] = timings_ms
        meta["assertions_status"] = assertion_report.get("status", "pass")
        meta["assertions_failed_count"] = assertion_report.get("failed_count", 0)
        meta["assertions_warn_count"] = assertion_report.get("warn_count", 0)
        if plugin_lock_hash:
            meta["plugin_lock_hash"] = plugin_lock_hash
            meta["plugin_lock_hash_mode"] = plugin_lock_hash_mode
        # Atomic write: tmp file then os.replace
        tmp_path = meta_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(meta, indent=2, sort_keys=True))
        os.replace(str(tmp_path), str(meta_path))
