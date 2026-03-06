"""CompiledWorksheet: the portable, immutable worksheet artifact.

Row-oriented JSON canonical form.
Inspectable, diffable, auditable, renderable.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from fin123.worksheet.types import ColumnType, DisplayFormat

# ────────────────────────────────────────────────────────────────
# Column metadata
# ────────────────────────────────────────────────────────────────


class CompiledColumn(BaseModel):
    """Column metadata in the compiled artifact."""

    name: str
    label: str
    column_type: ColumnType
    display_format: DisplayFormat | None = None
    source: str | None = None
    expression: str | None = None


# ────────────────────────────────────────────────────────────────
# Sort metadata
# ────────────────────────────────────────────────────────────────


class SortEntry(BaseModel):
    """Sort applied during compilation."""

    column: str
    descending: bool = False


# ────────────────────────────────────────────────────────────────
# Flags
# ────────────────────────────────────────────────────────────────


class CompiledFlag(BaseModel):
    """A triggered flag for a single row."""

    name: str
    severity: str
    message: str


# ────────────────────────────────────────────────────────────────
# Header groups
# ────────────────────────────────────────────────────────────────


class CompiledHeaderGroup(BaseModel):
    """Grouped header preserved in the artifact."""

    label: str
    columns: list[str]


# ────────────────────────────────────────────────────────────────
# Provenance
# ────────────────────────────────────────────────────────────────


class ViewTableProvenance(BaseModel):
    """Provenance block describing the source ViewTable."""

    source_label: str
    row_key: str | None = None
    input_row_count: int
    input_columns: list[str]


class ColumnProvenance(BaseModel):
    """Per-column provenance entry."""

    type: Literal["source", "derived"]
    source_column: str | None = None
    expression: str | None = None


class Provenance(BaseModel):
    """Structured compilation provenance — always emitted."""

    view_table: ViewTableProvenance
    compiled_at: str
    fin123_version: str
    spec_name: str
    row_count: int
    column_count: int
    columns: dict[str, ColumnProvenance]


# ────────────────────────────────────────────────────────────────
# Error summary
# ────────────────────────────────────────────────────────────────


class ErrorSummary(BaseModel):
    """Worksheet-level error summary."""

    total_errors: int
    by_column: dict[str, int]


# ────────────────────────────────────────────────────────────────
# CompiledWorksheet
# ────────────────────────────────────────────────────────────────


class CompiledWorksheet(BaseModel):
    """The compiled worksheet artifact.

    Row-oriented JSON canonical form.
    """

    name: str
    title: str | None = None
    columns: list[CompiledColumn]
    sorts: list[SortEntry] = Field(default_factory=list)
    header_groups: list[CompiledHeaderGroup] = Field(default_factory=list)
    rows: list[dict[str, Any]]
    flags: list[list[CompiledFlag]]
    provenance: Provenance
    error_summary: ErrorSummary | None = None

    def to_json(self, indent: int = 2) -> str:
        """Serialize to canonical deterministic JSON."""
        data = self.model_dump(mode="python")
        return json.dumps(data, indent=indent, sort_keys=True, default=str)

    @classmethod
    def from_json(cls, text: str) -> CompiledWorksheet:
        """Deserialize from JSON."""
        data = json.loads(text)
        return cls.model_validate(data)

    def to_file(self, path: str | Path) -> None:
        """Write canonical JSON to file."""
        Path(path).write_text(self.to_json())

    @classmethod
    def from_file(cls, path: str | Path) -> CompiledWorksheet:
        """Read from JSON file."""
        return cls.from_json(Path(path).read_text())

    def content_hash_data(self) -> str:
        """Serialize for content hashing, excluding compiled_at.

        Returns deterministic JSON with compiled_at zeroed out,
        suitable for equality comparison and hashing.
        """
        data = self.model_dump(mode="python")
        data["provenance"]["compiled_at"] = ""
        return json.dumps(data, indent=None, sort_keys=True, default=str)
