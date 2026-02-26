"""Table graph for Polars LazyFrame plan composition."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from fin123.functions.registry import get_table_fn


class TableGraph:
    """A graph of named Polars LazyFrame plans.

    Nodes are either source tables (loaded from CSV/Parquet) or derived plans
    that apply registered table functions to upstream tables.  Evaluation
    materializes all plans into DataFrames in dependency order.
    """

    def __init__(self, base_dir: Path) -> None:
        """Initialize a table graph.

        Args:
            base_dir: Project directory, used to resolve relative file paths.
        """
        self.base_dir = base_dir
        self._sources: dict[str, pl.LazyFrame] = {}
        self._plans: list[dict[str, Any]] = []

    def add_source(self, name: str, path: str, format: str = "csv") -> None:
        """Register a source table from a file.

        Args:
            name: Logical name for this table.
            path: File path relative to base_dir.
            format: File format (``csv`` or ``parquet``).
        """
        full_path = self.base_dir / path
        if format == "csv":
            lf = pl.scan_csv(full_path)
        elif format == "parquet":
            lf = pl.scan_parquet(full_path)
        else:
            raise ValueError(f"Unsupported table format: {format!r}")
        self._sources[name] = lf

    def add_plan(self, name: str, source: str, steps: list[dict[str, Any]]) -> None:
        """Register a derived table plan.

        Args:
            name: Logical name for the output table.
            source: Name of the upstream source or plan.
            steps: List of step dicts, each with a ``func`` key and arguments.
        """
        self._plans.append({"name": name, "source": source, "steps": steps})

    def evaluate(self) -> dict[str, pl.DataFrame]:
        """Evaluate all plans and return materialized DataFrames.

        Join operations receive a ``_tables`` dict so they can resolve
        references to other named tables.

        Returns:
            Dict mapping table names to Polars DataFrames.
        """
        frames: dict[str, pl.LazyFrame] = dict(self._sources)

        for plan in self._plans:
            source_name = plan["source"]
            if source_name not in frames:
                raise ValueError(
                    f"Plan {plan['name']!r} references unknown source {source_name!r}"
                )
            lf = frames[source_name]
            for step in plan["steps"]:
                func_name = step["func"]
                fn = get_table_fn(func_name)
                kwargs = {}
                for k, v in step.items():
                    # YAML 1.1 parses bare 'on'/'off'/'yes'/'no' as booleans.
                    # Normalize to the expected string key name.
                    key = self._yaml_key_fixup(k)
                    if key != "func":
                        kwargs[key] = v
                # Inject available tables for join operations
                if func_name == "join_left":
                    kwargs["_tables"] = frames
                lf = fn(lf, **kwargs)
            frames[plan["name"]] = lf

        return {name: lf.collect() for name, lf in frames.items()}

    @staticmethod
    def _yaml_key_fixup(k: object) -> str:
        """Normalize a YAML-parsed key to a string.

        YAML 1.1 (PyYAML default) treats bare ``on``/``off``/``yes``/``no`` as
        booleans.  This maps ``True`` back to ``"on"`` so that step specs like
        ``on: ticker`` work correctly.
        """
        if k is True:
            return "on"
        if k is False:
            return "off"
        return str(k)

    def get_source_paths(self) -> list[Path]:
        """Return resolved paths for all source files.

        Returns:
            List of Path objects for source data files.
        """
        # We need to track paths separately since LazyFrame doesn't expose them
        return list(self._source_paths.values()) if hasattr(self, "_source_paths") else []
