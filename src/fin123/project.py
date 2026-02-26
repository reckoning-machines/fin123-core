"""Project-level configuration and scaffolding."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG = {
    "max_runs": 50,
    "max_artifact_versions": 20,
    "max_total_run_bytes": 2_000_000_000,  # 2 GB
    "max_total_artifact_bytes": 5_000_000_000,  # 5 GB
    "ttl_days": None,
    "max_sync_runs": 50,
    "max_total_sync_bytes": 1_000_000_000,  # 1 GB
    "ttl_sync_days": None,
    "max_model_versions": 200,
    "max_total_model_version_bytes": None,
    "ttl_model_versions_days": None,
    "max_import_rows_per_sheet": 500,
    "max_import_cols_per_sheet": 100,
    "max_import_total_cells": 500_000,
    "registry_backend": "file",
    "registry_db_env": "FIN123_REGISTRY_URL",
    "registry_store_runs": False,
    "registry_store_builds": False,
    "registry_store_releases": False,
    "plugin_registry_url": None,
    "logging_max_days": None,
    "logging_max_bytes": None,
    "logging_fsync": False,
    "logging_tail_bytes": 2_097_152,  # 2 MB
    "mode": "dev",
    "import_projects_base": None,  # default: ~/Documents/fin123_projects
    "connectors_enabled": None,  # prod mode: list of allowed built-in connectors
}


def _flatten_registry_block(user_config: dict[str, Any]) -> dict[str, Any]:
    """Flatten a nested ``registry:`` block into flat config keys.

    Supports::

        registry:
          backend: postgres
          db_env: FIN123_REGISTRY_URL
          store_builds: true
          store_releases: true

    Maps to ``registry_backend``, ``registry_db_env``, ``registry_store_builds``,
    ``registry_store_releases``.  Also maps ``store_runs`` as alias for
    ``store_builds`` (backward compat).
    """
    reg = user_config.pop("registry", None)
    if not isinstance(reg, dict):
        return user_config

    mapping = {
        "backend": "registry_backend",
        "db_env": "registry_db_env",
        "store_builds": "registry_store_builds",
        "store_releases": "registry_store_releases",
        # Backward compat: nested store_runs maps to both flat keys
        "store_runs": "registry_store_runs",
    }
    for short_key, flat_key in mapping.items():
        if short_key in reg:
            user_config[flat_key] = reg[short_key]

    # If store_builds not explicitly set but store_runs was, mirror it
    if "store_builds" not in reg and "store_runs" in reg:
        user_config.setdefault("registry_store_builds", reg["store_runs"])

    return user_config

DEMO_WORKBOOK = """\
# fin123 workbook spec v1
version: 1
model_id: {model_id}

connections:
  pg_main:
    driver: postgres
    env: PG_MAIN_URL
    notes: Main analytics database (Visible Alpha feeds)

params:
  tax_rate: 0.15
  discount_rate: 0.10
  ticker: AAPL

tables:
  prices:
    source: inputs/prices.csv
    format: csv

  va_estimates:
    source: sql
    connection: pg_main
    query_file: queries/va_estimates.sql
    cache: inputs/va_estimates.parquet
    refresh: manual
    primary_key: ticker
    expected_columns: [ticker, eps, revenue_estimate, pe_ratio]

plans:
  - name: filtered_prices
    source: prices
    steps:
      - func: filter
        column: price
        op: ">"
        value: 50

  - name: summary_by_category
    source: prices
    steps:
      - func: group_agg
        group_by: [category]
        aggs:
          total_revenue: "sum(revenue)"
          avg_price: "mean(price)"
          item_count: "count(price)"
      - func: sort
        by: [total_revenue]
        descending: true

  - name: prices_with_estimates
    source: prices
    steps:
      - func: with_column
        name: ticker
        expression: "lit('AAPL')"
      - func: join_left
        right: va_estimates
        on: ticker
        validate: many_to_one

outputs:
  - name: total_revenue
    type: scalar
    func: expr
    args:
      expression: "a * (1 - b)"
      variables:
        a: "$gross_revenue"
        b: "$tax_rate"

  - name: gross_revenue
    type: scalar
    value: 125000.0

  - name: ticker_eps
    type: scalar
    func: lookup_scalar
    args:
      table_name: va_estimates
      key_col: ticker
      value_col: eps
      key_value: "$ticker"

  - name: filtered_prices
    type: table

  - name: summary_by_category
    type: table

  - name: prices_with_estimates
    type: table

workflows:
  - name: scenario_sweep
    file: workflows/scenario_sweep.yaml
"""

DEMO_PRICES_CSV = """\
product,category,price,quantity,revenue
Widget A,electronics,120.00,100,12000.00
Widget B,electronics,45.00,200,9000.00
Gadget C,home,75.50,150,11325.00
Gadget D,home,30.00,300,9000.00
Tool E,industrial,200.00,50,10000.00
Tool F,industrial,55.00,180,9900.00
Part G,electronics,90.00,120,10800.00
Part H,home,25.00,400,10000.00
Device I,industrial,150.00,80,12000.00
Device J,electronics,60.00,250,15000.00
"""

DEMO_SCENARIO_SWEEP = """\
# Scenario sweep workflow
name: scenario_sweep
description: Run workbook across multiple parameter sets and collect results.

type: scenario_sweep

scenarios:
  - name: low_tax
    params:
      tax_rate: 0.10
  - name: medium_tax
    params:
      tax_rate: 0.20
  - name: high_tax
    params:
      tax_rate: 0.30

collect:
  scalars:
    - total_revenue
"""

DEMO_VA_QUERY = """\
-- Visible Alpha analyst estimates
-- This query would run against your analytics database.
-- For demo purposes, a sample parquet is pre-populated at inputs/va_estimates.parquet
SELECT
    ticker,
    eps,
    revenue_estimate,
    pe_ratio
FROM analyst_estimates
WHERE period = 'FY2026'
ORDER BY ticker;
"""

DEMO_FIN123_CONFIG = """\
# fin123 project configuration
max_runs: 50
max_artifact_versions: 20
max_total_run_bytes: 2000000000
max_total_artifact_bytes: 5000000000
max_sync_runs: 50
max_total_sync_bytes: 1000000000
# ttl_days: 30
# ttl_sync_days: 14
"""


def load_project_config(project_dir: Path) -> dict[str, Any]:
    """Load project configuration from ``fin123.yaml``, with defaults.

    Supports both flat registry keys (``registry_backend``) and a nested
    ``registry:`` block.  The nested block is flattened before merging.

    Args:
        project_dir: Root of the fin123 project.

    Returns:
        Merged configuration dict.
    """
    config = dict(DEFAULT_CONFIG)
    config_path = project_dir / "fin123.yaml"
    if config_path.exists():
        user_config = yaml.safe_load(config_path.read_text()) or {}
        user_config = _flatten_registry_block(user_config)
        config.update(user_config)

    # Backward compat: registry_store_runs → registry_store_builds alias
    if config.get("registry_store_runs") and not config.get("registry_store_builds"):
        config["registry_store_builds"] = config["registry_store_runs"]

    return config


def get_project_mode(project_dir: Path) -> str:
    """Return the project mode: 'dev' or 'prod'.

    Args:
        project_dir: Root of the fin123 project.

    Returns:
        Mode string, defaulting to 'dev'.
    """
    config = load_project_config(project_dir)
    mode = str(config.get("mode", "dev")).lower()
    if mode not in ("dev", "prod"):
        mode = "dev"
    return mode


def enforce_prod_mode(
    project_dir: Path,
    workbook_spec: dict[str, Any],
    model_version_id: str | None,
    plugins_info: dict[str, dict[str, str]],
    assertion_report: dict[str, Any] | None = None,
) -> list[str]:
    """Check production mode constraints and return blocking errors.

    In prod mode the following must hold:
    - Runs must reference a model_version_id.
    - If latest import report has parse_error classifications, block run.
    - If a table declares expected_columns and columns are missing, block run.
    - All active plugins must have pin entries in pins.yaml.
    - Assertions with severity=error must all pass.

    Args:
        project_dir: Root of the fin123 project.
        workbook_spec: Parsed workbook spec dict.
        model_version_id: The snapshot version for this run.
        plugins_info: Active plugin version/hash info.
        assertion_report: Assertion evaluation report (if available).

    Returns:
        List of blocking error messages. Empty list means run is allowed.
    """
    errors: list[str] = []

    # Must have model_version_id
    if not model_version_id:
        errors.append(
            "prod mode requires a saved snapshot (model_version_id). "
            "Run 'fin123 save' first."
        )

    # Check import parse errors
    import_report_path = project_dir / "import_report.json"
    if import_report_path.exists():
        try:
            report = json.loads(import_report_path.read_text())
            classifications = report.get("formula_classifications", [])
            parse_errors = [c for c in classifications if c.get("classification") == "parse_error"]
            if parse_errors:
                errors.append(
                    f"prod mode blocked: {len(parse_errors)} import parse error(s) remain. "
                    f"Review and resolve in the Import tab."
                )
        except Exception:
            pass

    # Check expected_columns on SQL tables
    for table_name, tspec in workbook_spec.get("tables", {}).items():
        expected = tspec.get("expected_columns")
        cache = tspec.get("cache")
        if expected and cache:
            cache_path = project_dir / cache
            if cache_path.exists():
                try:
                    import polars as pl
                    df = pl.read_parquet(cache_path)
                    missing = set(expected) - set(df.columns)
                    if missing:
                        errors.append(
                            f"prod mode blocked: table {table_name!r} missing columns: "
                            f"{sorted(missing)}"
                        )
                except Exception:
                    pass

    # Check plugins.lock exists when plugins are active
    if plugins_info:
        plugins_lock_path = project_dir / "plugins.lock"
        if not plugins_lock_path.exists():
            errors.append(
                "prod mode blocked: active plugins detected but plugins.lock "
                "not found. Run 'fin123 plugin lock' to generate it."
            )

    # Check plugin pins
    pins_path = project_dir / "pins.yaml"
    if plugins_info:
        if not pins_path.exists():
            errors.append(
                "prod mode requires pins.yaml with plugin entries. "
                "No pins.yaml found."
            )
        else:
            try:
                pins = yaml.safe_load(pins_path.read_text()) or {}
                plugin_pins = {p["name"]: p for p in pins.get("plugins", [])}
                for pname, pinfo in plugins_info.items():
                    if pname not in plugin_pins:
                        errors.append(
                            f"prod mode blocked: plugin {pname!r} is active but not pinned. "
                            f"Add it to pins.yaml."
                        )
                    else:
                        pinned_hash = plugin_pins[pname].get("hash")
                        if pinned_hash and pinned_hash != pinfo.get("sha256"):
                            errors.append(
                                f"prod mode blocked: plugin {pname!r} hash mismatch. "
                                f"Pinned={pinned_hash[:16]}... actual={pinfo.get('sha256', '')[:16]}..."
                            )
            except Exception:
                pass

    # Check assertions
    if assertion_report and assertion_report.get("failed_count", 0) > 0:
        errors.append(
            f"prod mode blocked: {assertion_report['failed_count']} assertion(s) "
            f"with severity=error failed."
        )

    # Check registry availability when store_builds or store_releases enabled
    config = load_project_config(project_dir)
    if config.get("registry_backend") == "postgres":
        needs_registry = (
            config.get("registry_store_builds", False)
            or config.get("registry_store_releases", False)
        )
        if needs_registry:
            import os

            dsn_env = config.get("registry_db_env", "FIN123_REGISTRY_URL")
            if not os.environ.get(dsn_env):
                errors.append(
                    f"prod mode blocked: registry.backend=postgres with "
                    f"store_builds or store_releases enabled, but "
                    f"environment variable {dsn_env!r} is not set. "
                    f"Set {dsn_env} or disable store_builds/store_releases."
                )
            else:
                from fin123.registry.backend import get_registry

                reg = get_registry(project_dir, config)
                if reg is None or not reg.ping():
                    errors.append(
                        "prod mode blocked: registry.backend=postgres is "
                        "configured but the database is not reachable. "
                        "Check your connection or disable store_builds/store_releases."
                    )

    return errors


def scaffold_project(target_dir: Path) -> Path:
    """Create a new fin123 demo project at the target directory.

    Args:
        target_dir: Directory to create (must not already contain workbook.yaml).

    Returns:
        Path to the created project directory.
    """
    target_dir = target_dir.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    if (target_dir / "workbook.yaml").exists():
        raise FileExistsError(f"workbook.yaml already exists in {target_dir}")

    # Write workbook
    workbook_content = DEMO_WORKBOOK.format(model_id=str(uuid.uuid4()))
    (target_dir / "workbook.yaml").write_text(workbook_content)

    # Write config
    (target_dir / "fin123.yaml").write_text(DEMO_FIN123_CONFIG)

    # Write inputs
    inputs_dir = target_dir / "inputs"
    inputs_dir.mkdir(exist_ok=True)
    (inputs_dir / "prices.csv").write_text(DEMO_PRICES_CSV)

    # Write sample VA estimates parquet (so demo works without a DB)
    _write_demo_va_estimates(inputs_dir / "va_estimates.parquet")

    # Write queries
    queries_dir = target_dir / "queries"
    queries_dir.mkdir(exist_ok=True)
    (queries_dir / "va_estimates.sql").write_text(DEMO_VA_QUERY)

    # Write workflow
    wf_dir = target_dir / "workflows"
    wf_dir.mkdir(exist_ok=True)
    (wf_dir / "scenario_sweep.yaml").write_text(DEMO_SCENARIO_SWEEP)

    # Create empty directories
    (target_dir / "runs").mkdir(exist_ok=True)
    (target_dir / "artifacts").mkdir(exist_ok=True)
    (target_dir / "snapshots").mkdir(exist_ok=True)
    (target_dir / "sync_runs").mkdir(exist_ok=True)
    (target_dir / "cache").mkdir(exist_ok=True)

    # Create initial snapshot (v0001)
    from fin123.versioning import SnapshotStore

    store = SnapshotStore(target_dir)
    store.save_snapshot(workbook_content)

    return target_dir


def ensure_model_id(spec: dict[str, Any], spec_path: Path) -> str:
    """Ensure the workbook spec has a model_id; generate and persist one if missing.

    Args:
        spec: Parsed workbook spec dict (mutated in place if model_id is added).
        spec_path: Path to the workbook.yaml file for writing back.

    Returns:
        The model_id string.
    """
    if spec.get("model_id"):
        return str(spec["model_id"])

    model_id = str(uuid.uuid4())
    spec["model_id"] = model_id

    # Write back to workbook.yaml
    new_yaml = yaml.dump(spec, default_flow_style=False, sort_keys=False)
    spec_path.write_text(new_yaml)
    return model_id


def _write_demo_va_estimates(path: Path) -> None:
    """Write a sample VA estimates parquet file for the demo project.

    Args:
        path: Output parquet file path.
    """
    import polars as pl

    df = pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOGL", "AMZN", "META"],
        "eps": [6.75, 12.10, 7.50, 4.80, 18.20],
        "revenue_estimate": [420_000.0, 265_000.0, 380_000.0, 650_000.0, 170_000.0],
        "pe_ratio": [28.5, 34.2, 22.8, 62.5, 23.1],
    })
    df.write_parquet(path)
