"""Scalar dependency graph for lightweight named computations."""

from __future__ import annotations

from typing import Any

import polars as pl

from fin123.functions.registry import get_scalar_fn


class ScalarGraph:
    """A directed acyclic graph of named scalar values.

    Each node is either a literal value or a function call that depends on
    other named scalars.  Evaluation proceeds in topological order so that
    every dependency is resolved before it is needed.
    """

    def __init__(self) -> None:
        """Initialize an empty scalar graph."""
        self._values: dict[str, Any] = {}
        self._formulas: dict[str, dict[str, Any]] = {}
        self._parsed_formulas: dict[str, dict[str, Any]] = {}
        self._table_cache: dict[str, pl.DataFrame] = {}

    def set_value(self, name: str, value: Any) -> None:
        """Set a scalar to a literal value.

        Args:
            name: Scalar name.
            value: The literal value.
        """
        self._values[name] = value

    def set_formula(self, name: str, func: str, args: dict[str, Any]) -> None:
        """Define a scalar as a function of other scalars.

        Args:
            name: Scalar name.
            func: Registered scalar function name.
            args: Keyword arguments for the function. Values that are strings
                  starting with ``$`` are treated as references to other scalars.
        """
        self._formulas[name] = {"func": func, "args": args}

    def set_parsed_formula(
        self, name: str, tree: Any, deps: set[str]
    ) -> None:
        """Register a parsed formula expression.

        Args:
            name: Scalar name.
            tree: Lark parse tree from ``parse_formula()``.
            deps: Set of scalar names this formula depends on.
        """
        self._parsed_formulas[name] = {"tree": tree, "deps": deps}

    def set_table_cache(self, cache: dict[str, pl.DataFrame]) -> None:
        """Provide materialized table DataFrames for formula evaluation.

        Args:
            cache: Mapping of table names to DataFrames.
        """
        self._table_cache = cache

    def evaluate(self) -> dict[str, Any]:
        """Evaluate all scalars in dependency order.

        Merges structured formulas and parsed formulas into a single
        iterative pass so they can depend on each other.

        Returns:
            Dict mapping scalar names to their computed values.
        """
        from fin123.formulas.evaluator import evaluate_formula

        resolved: dict[str, Any] = dict(self._values)

        # Combine both formula types into a single remaining set
        remaining_structured = dict(self._formulas)
        remaining_parsed = dict(self._parsed_formulas)

        total_remaining = len(remaining_structured) + len(remaining_parsed)
        max_iterations = total_remaining + 1

        for _ in range(max_iterations):
            if not remaining_structured and not remaining_parsed:
                break

            progress = False

            # Try structured formulas
            still_structured = {}
            for name, spec in remaining_structured.items():
                resolved_args = self._resolve_args(spec["args"], resolved)
                if resolved_args is None:
                    still_structured[name] = spec
                else:
                    fn = get_scalar_fn(spec["func"])
                    resolved[name] = fn(**resolved_args)
                    progress = True
            remaining_structured = still_structured

            # Try parsed formulas
            still_parsed = {}
            for name, spec in remaining_parsed.items():
                deps = spec["deps"]
                if all(dep in resolved for dep in deps):
                    resolved[name] = evaluate_formula(
                        spec["tree"], resolved, self._table_cache
                    )
                    progress = True
                else:
                    still_parsed[name] = spec
            remaining_parsed = still_parsed

            if not progress and (remaining_structured or remaining_parsed):
                unresolved = list(remaining_structured.keys()) + list(
                    remaining_parsed.keys()
                )
                raise ValueError(
                    f"Circular or unresolvable dependencies: {unresolved}"
                )

        return resolved

    def _resolve_args(
        self, args: dict[str, Any], resolved: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Attempt to resolve references in argument values.

        Args:
            args: Raw arguments, possibly containing ``$ref`` references.
            resolved: Currently resolved scalar values.

        Returns:
            Resolved arguments dict, or ``None`` if any dependency is missing.
        """
        out: dict[str, Any] = {}
        for key, val in args.items():
            resolved_val = self._resolve_value(val, resolved)
            if resolved_val is _UNRESOLVED:
                return None
            out[key] = resolved_val
        return out

    def _resolve_value(self, val: Any, resolved: dict[str, Any]) -> Any:
        """Recursively resolve a single value, which may be a scalar ref, list, or dict.

        Args:
            val: The value to resolve.
            resolved: Currently resolved scalar values.

        Returns:
            The resolved value, or the sentinel ``_UNRESOLVED`` if a dependency
            is missing.
        """
        if isinstance(val, str) and val.startswith("$"):
            ref = val[1:]
            if ref not in resolved:
                return _UNRESOLVED
            return resolved[ref]
        elif isinstance(val, list):
            resolved_list = []
            for item in val:
                r = self._resolve_value(item, resolved)
                if r is _UNRESOLVED:
                    return _UNRESOLVED
                resolved_list.append(r)
            return resolved_list
        elif isinstance(val, dict):
            resolved_dict = {}
            for k, v in val.items():
                r = self._resolve_value(v, resolved)
                if r is _UNRESOLVED:
                    return _UNRESOLVED
                resolved_dict[k] = r
            return resolved_dict
        return val


_UNRESOLVED = object()
