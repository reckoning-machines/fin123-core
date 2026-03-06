"""Worksheet compilation: ViewTable + WorksheetView → CompiledWorksheet.

Pipeline:
  1. Validate spec against ViewTable
  2. Parse derived expressions, extract references, build dependency graph
  3. Topologically order derived column evaluation (cycles → hard error)
  4. For each row: project source columns, evaluate derived columns in
     dependency order, evaluate flags
  5. Apply sorts
  6. Build provenance and error summary
  7. Assemble CompiledWorksheet

Display order in the output is always the spec's column order.
Evaluation order is determined by the dependency graph.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Any

from lark import Tree

import polars as pl

from fin123 import __version__
from fin123.formulas.errors import ENGINE_ERRORS
from fin123.formulas.parser import extract_refs
from fin123.worksheet.compiled import (
    ColumnProvenance,
    CompiledColumn,
    CompiledFlag,
    CompiledHeaderGroup,
    CompiledWorksheet,
    ErrorSummary,
    Provenance,
    SortEntry,
    ViewTableProvenance,
)
from fin123.worksheet.eval_row import (
    _error_code,
    evaluate_row_tree,
    parse_row_expression,
)
from fin123.worksheet.spec import (
    DerivedColumnSpec,
    FlagSpec,
    SourceColumnSpec,
    WorksheetView,
    validate_worksheet_view,
)
from fin123.worksheet.types import ColumnType, DisplayFormat
from fin123.worksheet.view_table import ViewTable, _polars_dtype_to_column_type


# ────────────────────────────────────────────────────────────────
# Dependency graph / topological sort
# ────────────────────────────────────────────────────────────────


def _extract_derived_refs(expression: str) -> set[str]:
    """Parse an expression and extract all scalar/column references."""
    tree = parse_row_expression(expression)
    return extract_refs(tree)


def _topological_order(
    derived_columns: list[DerivedColumnSpec],
    view_table_columns: set[str],
) -> list[str]:
    """Topologically sort derived columns by their dependencies.

    Args:
        derived_columns: The derived column specs.
        view_table_columns: Column names available from the ViewTable.

    Returns:
        List of derived column names in evaluation order.

    Raises:
        ValueError: If there is a cycle in the dependency graph.
    """
    derived_names = {dc.name for dc in derived_columns}

    # Build adjacency: name → set of derived columns it depends on
    deps: dict[str, set[str]] = {}
    for dc in derived_columns:
        refs = _extract_derived_refs(dc.expression)
        # Only keep refs that are derived columns (including self = cycle)
        deps[dc.name] = refs & derived_names

    # Kahn's algorithm
    in_degree = {name: len(d) for name, d in deps.items()}
    # Use sorted() for deterministic output when multiple nodes have in_degree 0
    queue = deque(sorted(name for name, deg in in_degree.items() if deg == 0))
    order: list[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for name in sorted(deps.keys()):
            if node in deps[name]:
                in_degree[name] -= 1
                if in_degree[name] == 0:
                    queue.append(name)

    if len(order) != len(derived_columns):
        remaining = sorted(set(derived_names) - set(order))
        raise ValueError(
            f"Cycle in derived column dependencies: {remaining}"
        )

    return order


# ────────────────────────────────────────────────────────────────
# Compiler
# ────────────────────────────────────────────────────────────────


def compile_worksheet(
    view_table: ViewTable,
    spec: WorksheetView,
    compiled_at: str | None = None,
) -> CompiledWorksheet:
    """Compile a worksheet from a ViewTable and a WorksheetView spec.

    Args:
        view_table: The source data.
        spec: The worksheet specification.
        compiled_at: ISO 8601 timestamp. If None, uses current UTC time.
            Pass a fixed value for deterministic test output.

    Returns:
        CompiledWorksheet artifact.

    Raises:
        ValueError: On structural validation failures or dependency cycles.
    """
    # 1. Validate spec against ViewTable
    errors = validate_worksheet_view(spec, view_table.columns)
    if errors:
        raise ValueError(
            "Worksheet spec validation failed:\n  - " + "\n  - ".join(errors)
        )

    if compiled_at is None:
        compiled_at = datetime.now(timezone.utc).isoformat()

    # 2. Gather column specs by type
    source_specs: dict[str, SourceColumnSpec] = {}
    derived_specs: dict[str, DerivedColumnSpec] = {}
    for col in spec.columns:
        if isinstance(col, SourceColumnSpec):
            source_specs[col.canonical_name] = col
        elif isinstance(col, DerivedColumnSpec):
            derived_specs[col.canonical_name] = col

    # 3. Topological order for derived columns
    derived_list = list(derived_specs.values())
    if derived_list:
        eval_order = _topological_order(derived_list, set(view_table.columns))
    else:
        eval_order = []

    # 4. Parse derived expressions and flag expressions (once)
    derived_trees: dict[str, Tree] = {}
    for name in eval_order:
        dc = derived_specs[name]
        derived_trees[name] = parse_row_expression(dc.expression)

    flag_trees: list[tuple[FlagSpec, Tree]] = []
    for flag in spec.flags:
        flag_trees.append((flag, parse_row_expression(flag.expression)))

    # 5. Determine output column names (spec order) and the full
    #    set of ViewTable columns needed (may include non-projected ones)
    output_names = spec.canonical_names()
    vt_columns = view_table.columns

    # 6. Build compiled column metadata
    compiled_columns = _build_compiled_columns(spec, view_table)

    # 7. Evaluate rows
    df = view_table.df
    rows: list[dict[str, Any]] = []
    all_flags: list[list[CompiledFlag]] = []
    error_counts: dict[str, int] = {}

    for row_idx in range(len(df)):
        # Start with all ViewTable columns as the row context
        row_context: dict[str, Any] = {
            col: _native_value(df[col][row_idx]) for col in vt_columns
        }

        # Evaluate derived columns in dependency order
        for name in eval_order:
            tree = derived_trees[name]
            try:
                value = evaluate_row_tree(tree, row_context)
            except ENGINE_ERRORS as exc:
                value = {"error": _error_code(exc)}
                error_counts[name] = error_counts.get(name, 0) + 1
            row_context[name] = value

        # Build output row in spec column order
        out_row: dict[str, Any] = {}
        for col_name in output_names:
            out_row[col_name] = row_context.get(col_name)

        rows.append(out_row)

        # Evaluate flags
        row_flags: list[CompiledFlag] = []
        for flag_spec, flag_tree in flag_trees:
            try:
                result = evaluate_row_tree(flag_tree, row_context)
                if result:
                    row_flags.append(CompiledFlag(
                        name=flag_spec.name,
                        severity=flag_spec.severity,
                        message=flag_spec.message,
                    ))
            except ENGINE_ERRORS:
                pass  # Flag eval errors are silent — flag just doesn't trigger

        all_flags.append(row_flags)

    # 8. Apply sorts
    sorts_applied = [
        SortEntry(column=s.column, descending=s.descending)
        for s in spec.sorts
    ]
    if spec.sorts:
        rows, all_flags = _apply_sorts(rows, all_flags, spec.sorts)

    # 9. Build provenance
    column_provenance: dict[str, ColumnProvenance] = {}
    for col in spec.columns:
        if isinstance(col, SourceColumnSpec):
            column_provenance[col.canonical_name] = ColumnProvenance(
                type="source", source_column=col.source
            )
        elif isinstance(col, DerivedColumnSpec):
            column_provenance[col.canonical_name] = ColumnProvenance(
                type="derived", expression=col.expression
            )

    provenance = Provenance(
        view_table=ViewTableProvenance(
            source_label=view_table.source_label,
            row_key=view_table.row_key,
            input_row_count=view_table.row_count,
            input_columns=view_table.columns,
        ),
        compiled_at=compiled_at,
        fin123_version=__version__,
        spec_name=spec.name,
        row_count=len(rows),
        column_count=len(compiled_columns),
        columns=column_provenance,
    )

    # 10. Build error summary
    total_errors = sum(error_counts.values())
    error_summary = (
        ErrorSummary(total_errors=total_errors, by_column=error_counts)
        if total_errors > 0
        else None
    )

    # 11. Build header groups
    header_groups = [
        CompiledHeaderGroup(label=g.label, columns=g.columns)
        for g in spec.header_groups
    ]

    return CompiledWorksheet(
        name=spec.name,
        title=spec.title,
        columns=compiled_columns,
        sorts=sorts_applied,
        header_groups=header_groups,
        rows=rows,
        flags=all_flags,
        provenance=provenance,
        error_summary=error_summary,
    )


# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────


def _native_value(val: Any) -> Any:
    """Convert Polars scalar to native Python type."""
    if val is None:
        return None
    # Polars returns numpy-like types; convert to native Python
    if hasattr(val, "item"):
        return val.item()
    return val


def _build_compiled_columns(
    spec: WorksheetView,
    view_table: ViewTable,
) -> list[CompiledColumn]:
    """Build CompiledColumn metadata from spec + ViewTable schema."""
    result: list[CompiledColumn] = []
    for col in spec.columns:
        if isinstance(col, SourceColumnSpec):
            # Determine type from column_type override or ViewTable schema
            if col.column_type is not None:
                ct = col.column_type
            else:
                ct = _polars_dtype_to_column_type(
                    view_table.df[col.source].dtype
                )
            result.append(CompiledColumn(
                name=col.canonical_name,
                label=col.label or col.source,
                column_type=ct,
                display_format=col.display_format,
                source=col.source,
            ))
        elif isinstance(col, DerivedColumnSpec):
            ct = col.column_type or ColumnType.FLOAT64
            result.append(CompiledColumn(
                name=col.canonical_name,
                label=col.label or col.name,
                column_type=ct,
                display_format=col.display_format,
                expression=col.expression,
            ))
    return result


def _apply_sorts(
    rows: list[dict[str, Any]],
    flags: list[list[CompiledFlag]],
    sorts: list,
) -> tuple[list[dict[str, Any]], list[list[CompiledFlag]]]:
    """Sort rows and their corresponding flags by the sort spec.

    Uses Python's stable sort. Multiple sort keys are applied in reverse
    order (last sort is primary, matching Polars multi-sort semantics
    applied iteratively).
    """
    # Pair rows with flags and original index for stable tie-breaking
    paired = list(enumerate(zip(rows, flags)))

    # Apply sorts in reverse order for correct multi-key priority
    for sort in reversed(sorts):
        col = sort.column
        desc = sort.descending

        def sort_key(item, _col=col, _desc=desc):
            idx, (row, _) = item
            val = row.get(_col)
            # Handle error dicts and Nones — push to end
            if isinstance(val, dict) or val is None:
                return (1, 0, idx)
            return (0, val if not _desc else val, idx)

        # Custom comparison: nulls/errors last, then value, then stable index
        paired.sort(
            key=lambda item, _col=col, _desc=desc: _sort_key(item, _col, _desc),
            reverse=desc,
        )

    sorted_rows = [row for _, (row, _) in paired]
    sorted_flags = [fl for _, (_, fl) in paired]
    return sorted_rows, sorted_flags


def _sort_key(
    item: tuple[int, tuple[dict[str, Any], list]],
    col: str,
    desc: bool,
) -> tuple:
    """Generate a sort key for a paired (index, (row, flags)) item.

    Nulls and error dicts sort last. Original index provides stable
    tie-breaking.
    """
    idx, (row, _) = item
    val = row.get(col)
    if isinstance(val, dict) or val is None:
        # Nulls/errors: always last regardless of direction.
        # When desc=True the list is reversed, so "last" means
        # the smallest key value in a descending sort.
        if desc:
            return (0,)  # Will end up last after reverse
        return (1, 0, idx)
    if desc:
        return (1, val, idx)
    return (0, val, idx)
