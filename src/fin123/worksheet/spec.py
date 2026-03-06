"""WorksheetView: declarative worksheet specification.

A WorksheetView defines how a ViewTable is projected, derived, sorted,
flagged, and grouped into a compiled worksheet. It is compile-time only
— no filtering, no scalar injection, no app-layer concepts.

Specs are typically authored as YAML and loaded via load_worksheet_view().
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from fin123.worksheet.eval_row import validate_row_local
from fin123.worksheet.types import ColumnType, DisplayFormat

# ────────────────────────────────────────────────────────────────
# Column specs
# ────────────────────────────────────────────────────────────────


class SourceColumnSpec(BaseModel):
    """A column pulled directly from the ViewTable."""

    kind: Literal["source"] = "source"
    source: str
    label: str | None = None
    display_format: DisplayFormat | None = None
    column_type: ColumnType | None = None

    @property
    def canonical_name(self) -> str:
        """The canonical output column name (always the source name)."""
        return self.source


class DerivedColumnSpec(BaseModel):
    """A column computed via a row-local formula."""

    kind: Literal["derived"] = "derived"
    name: str
    expression: str
    label: str | None = None
    display_format: DisplayFormat | None = None
    column_type: ColumnType | None = None

    @property
    def canonical_name(self) -> str:
        """The canonical output column name."""
        return self.name


# ────────────────────────────────────────────────────────────────
# Sort, flag, header group
# ────────────────────────────────────────────────────────────────


class SortSpec(BaseModel):
    """Compile-time sort specification."""

    column: str
    descending: bool = False


class FlagSpec(BaseModel):
    """Row-local boolean condition with severity."""

    name: str
    expression: str
    severity: Literal["info", "warning", "error"] = "warning"
    message: str = ""


class HeaderGroup(BaseModel):
    """Single-level grouped header. References canonical column names."""

    label: str
    columns: list[str]


# ────────────────────────────────────────────────────────────────
# WorksheetView
# ────────────────────────────────────────────────────────────────


class WorksheetView(BaseModel):
    """Declarative worksheet specification.

    Applied to a ViewTable to produce a CompiledWorksheet.
    No filtering. No scalar injection. No row_key override.
    """

    name: str
    title: str | None = None
    columns: list[SourceColumnSpec | DerivedColumnSpec]
    sorts: list[SortSpec] = Field(default_factory=list)
    flags: list[FlagSpec] = Field(default_factory=list)
    header_groups: list[HeaderGroup] = Field(default_factory=list)

    @field_validator("columns")
    @classmethod
    def columns_must_be_nonempty(
        cls, v: list[SourceColumnSpec | DerivedColumnSpec],
    ) -> list[SourceColumnSpec | DerivedColumnSpec]:
        if not v:
            raise ValueError("columns must not be empty")
        return v

    def canonical_names(self) -> list[str]:
        """Return the list of canonical output column names in order."""
        return [c.canonical_name for c in self.columns]


# ────────────────────────────────────────────────────────────────
# YAML loading
# ────────────────────────────────────────────────────────────────


def _parse_column(raw: dict[str, Any]) -> SourceColumnSpec | DerivedColumnSpec:
    """Parse a single column dict from YAML into the right spec type."""
    if "source" in raw:
        return SourceColumnSpec(**raw)
    if "name" in raw and "expression" in raw:
        return DerivedColumnSpec(**raw)
    raise ValueError(
        f"Column spec must have 'source' (source column) or "
        f"'name'+'expression' (derived column), got keys: {sorted(raw.keys())}"
    )


def load_worksheet_view(path: str | Path) -> WorksheetView:
    """Load a WorksheetView from a YAML file.

    Args:
        path: Path to the YAML spec file.

    Returns:
        Parsed WorksheetView.

    Raises:
        ValueError: On invalid spec structure.
        FileNotFoundError: If the file does not exist.
    """
    path = Path(path)
    raw = yaml.safe_load(path.read_text())
    return parse_worksheet_view(raw)


def parse_worksheet_view(raw: dict[str, Any]) -> WorksheetView:
    """Parse a WorksheetView from a raw dict (e.g. loaded from YAML).

    Args:
        raw: Dict with spec fields.

    Returns:
        Parsed WorksheetView.

    Raises:
        ValueError: On invalid spec structure.
    """
    if not isinstance(raw, dict):
        raise ValueError(f"Spec must be a dict, got {type(raw).__name__}")

    # Parse columns from raw dicts
    raw_columns = raw.get("columns", [])
    if not isinstance(raw_columns, list):
        raise ValueError("'columns' must be a list")

    columns = [_parse_column(c) for c in raw_columns]

    return WorksheetView(
        name=raw.get("name", ""),
        title=raw.get("title"),
        columns=columns,
        sorts=[SortSpec(**s) for s in raw.get("sorts", [])],
        flags=[FlagSpec(**f) for f in raw.get("flags", [])],
        header_groups=[HeaderGroup(**g) for g in raw.get("header_groups", [])],
    )


# ────────────────────────────────────────────────────────────────
# Validation against a ViewTable's column set
# ────────────────────────────────────────────────────────────────


def validate_worksheet_view(
    spec: WorksheetView,
    available_columns: list[str],
) -> list[str]:
    """Validate a WorksheetView against the columns available from a ViewTable.

    Checks:
    - Source columns exist in available_columns
    - No duplicate canonical output names
    - Sort columns reference valid output columns
    - Flag expressions are valid row-local formulas
    - Derived expressions are valid row-local formulas
    - Header groups reference valid canonical column names
    - Derived column references exist (source columns or earlier derived)

    Args:
        spec: The WorksheetView to validate.
        available_columns: Column names from the ViewTable.

    Returns:
        List of error messages. Empty means valid.
    """
    errors: list[str] = []
    available_set = set(available_columns)

    # Collect canonical output names and check duplicates
    canonical_names: list[str] = []
    seen_names: set[str] = set()
    for col in spec.columns:
        cn = col.canonical_name
        if cn in seen_names:
            errors.append(f"Duplicate output column name: '{cn}'")
        seen_names.add(cn)
        canonical_names.append(cn)

    # Build the full set of names referenceable by derived expressions:
    # all ViewTable columns + all derived column names.
    # Dependency ordering (cycle detection) is the compiler's job.
    all_derived_names = {
        col.canonical_name
        for col in spec.columns
        if isinstance(col, DerivedColumnSpec)
    }
    referenceable: set[str] = set(available_columns) | all_derived_names

    for col in spec.columns:
        if isinstance(col, SourceColumnSpec):
            if col.source not in available_set:
                errors.append(
                    f"Source column '{col.source}' not found in ViewTable. "
                    f"Available: {sorted(available_columns)}"
                )
        elif isinstance(col, DerivedColumnSpec):
            # Validate expression syntax and function allowlist
            expr_errors = validate_row_local(
                col.expression,
                available_columns=sorted(referenceable),
            )
            for e in expr_errors:
                errors.append(f"Derived column '{col.name}': {e}")

    # Validate sorts reference canonical output names
    output_name_set = set(canonical_names)
    for sort in spec.sorts:
        if sort.column not in output_name_set:
            errors.append(
                f"Sort column '{sort.column}' not found in output columns. "
                f"Available: {sorted(canonical_names)}"
            )

    # Validate flag expressions
    # Flags can reference any output column (source + derived)
    flag_context = sorted(referenceable)
    for flag in spec.flags:
        expr_errors = validate_row_local(flag.expression, available_columns=flag_context)
        for e in expr_errors:
            errors.append(f"Flag '{flag.name}': {e}")

    # Validate header groups reference canonical column names
    for group in spec.header_groups:
        for col_name in group.columns:
            if col_name not in output_name_set:
                errors.append(
                    f"Header group '{group.label}' references "
                    f"unknown column '{col_name}'. "
                    f"Use canonical column names: {sorted(canonical_names)}"
                )

    return errors
