"""Shared service layer for the fin123 UI.

This module encapsulates all UI operations so that both the FastAPI server
and a future TUI can share the same logic.  It is the single place that
reads/writes project files, loads the workbook, applies sheet edits, saves
snapshots, triggers run/sync/workflow, and fetches outputs.
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from fin123 import __version__
from fin123.project import ensure_model_id, load_project_config
from fin123.versioning import ArtifactStore, RunStore, SnapshotStore


# ---------------------------------------------------------------------------
# Cell address helpers
# ---------------------------------------------------------------------------

_ADDR_RE = re.compile(r"^([A-Z]{1,3})(\d+)$")

# Regex for formula reference rewriting.
# Matches (in priority order):
#   1. Quoted-sheet refs:  'Sheet Name'!A1
#   2. Unquoted-sheet refs: Sheet1!A1
#   3. Bare A1 refs (with lookbehind/lookahead to avoid identifiers)
_QUOTED_SHEET_REF = r"'([^']+)'!([A-Z]{1,3})(\d+)"
_UNQUOTED_SHEET_REF = r"([A-Za-z_]\w*)!([A-Z]{1,3})(\d+)"
_BARE_REF = r"(?<![A-Za-z_])([A-Z]{1,3})(\d+)(?![A-Za-z0-9_])"
_FORMULA_REF_RE = re.compile(
    rf"({_QUOTED_SHEET_REF})|({_UNQUOTED_SHEET_REF})|({_BARE_REF})"
)
# String literal ranges to skip refs inside "..."
_STRING_LIT_RE = re.compile(r'"[^"]*"')


def _find_string_ranges(formula: str) -> list[tuple[int, int]]:
    """Return list of (start, end) index ranges for string literals in formula."""
    return [(m.start(), m.end()) for m in _STRING_LIT_RE.finditer(formula)]


def _in_string(pos: int, string_ranges: list[tuple[int, int]]) -> bool:
    """Check if position falls inside any string literal range."""
    for s, e in string_ranges:
        if s <= pos < e:
            return True
    return False


def rewrite_formula_refs(
    formula: str,
    affected_sheet: str,
    current_sheet: str,
    axis: str,
    index: int,
    count: int,
) -> str:
    """Rewrite cell references in a formula after row/col insertion or deletion.

    Args:
        formula: The formula string (with leading '=').
        affected_sheet: The sheet where rows/cols were inserted/deleted.
        current_sheet: The sheet this formula lives on (for bare ref resolution).
        axis: "row" or "col".
        index: 0-based index where insertion/deletion starts.
        count: Positive for insert, negative for delete.

    Returns:
        Rewritten formula string.
    """
    if not formula or not formula.startswith("="):
        return formula

    string_ranges = _find_string_ranges(formula)
    result_parts: list[str] = []
    last_end = 0

    for m in _FORMULA_REF_RE.finditer(formula):
        # Skip matches inside string literals
        if _in_string(m.start(), string_ranges):
            continue

        # Determine which alternative matched
        if m.group(1):
            # Quoted sheet ref: 'Sheet Name'!A1
            ref_sheet = m.group(2)
            col_str = m.group(3)
            row_str = m.group(4)
        elif m.group(5):
            # Unquoted sheet ref: Sheet1!A1
            ref_sheet = m.group(6)
            col_str = m.group(7)
            row_str = m.group(8)
        else:
            # Bare ref: A1
            ref_sheet = current_sheet
            col_str = m.group(10)
            row_str = m.group(11)

        # Only rewrite if ref points to the affected sheet
        if ref_sheet != affected_sheet:
            continue

        col_idx = col_letter_to_index(col_str)
        row_idx = int(row_str) - 1  # 0-based

        if axis == "row":
            ref_pos = row_idx
        else:
            ref_pos = col_idx

        new_ref = None
        if count > 0:
            # Insert: shift refs at or past index
            if ref_pos >= index:
                if axis == "row":
                    new_row = row_idx + count
                    new_ref = f"{col_str}{new_row + 1}"
                else:
                    new_col = col_idx + count
                    new_ref = f"{index_to_col_letter(new_col)}{row_str}"
        else:
            # Delete: refs in deleted range → #REF!, past range shift back
            abs_count = abs(count)
            if index <= ref_pos < index + abs_count:
                new_ref = "#REF!"
            elif ref_pos >= index + abs_count:
                if axis == "row":
                    new_row = row_idx + count
                    new_ref = f"{col_str}{new_row + 1}"
                else:
                    new_col = col_idx + count
                    new_ref = f"{index_to_col_letter(new_col)}{row_str}"

        if new_ref is not None:
            # Reconstruct with sheet prefix if original had one
            if m.group(1):
                replacement = f"'{ref_sheet}'!{new_ref}"
            elif m.group(5):
                replacement = f"{ref_sheet}!{new_ref}"
            else:
                replacement = new_ref

            result_parts.append(formula[last_end:m.start()])
            result_parts.append(replacement)
            last_end = m.end()

    result_parts.append(formula[last_end:])
    return "".join(result_parts)


def _remap_addresses(
    addr_dict: dict[str, Any],
    axis: str,
    index: int,
    count: int,
) -> dict[str, Any]:
    """Rebuild a cells/fmt dict with shifted address keys.

    Args:
        addr_dict: Dict keyed by cell addresses (e.g. "A1").
        axis: "row" or "col".
        index: 0-based index where insertion/deletion starts.
        count: Positive for insert, negative for delete.

    Returns:
        New dict with shifted keys. Entries in deleted range are dropped.
    """
    new_dict: dict[str, Any] = {}
    for addr_key, value in addr_dict.items():
        try:
            row, col = parse_addr(addr_key)
        except ValueError:
            new_dict[addr_key] = value
            continue

        ref_pos = row if axis == "row" else col

        if count > 0:
            # Insert: shift at or past index
            if ref_pos >= index:
                if axis == "row":
                    row += count
                else:
                    col += count
        else:
            # Delete: drop entries in deleted range, shift rest
            abs_count = abs(count)
            if index <= ref_pos < index + abs_count:
                continue  # drop
            elif ref_pos >= index + abs_count:
                if axis == "row":
                    row += count
                else:
                    col += count

        new_dict[make_addr(row, col)] = value
    return new_dict


def col_letter_to_index(letters: str) -> int:
    """Convert column letter(s) to 0-based index.  A=0, B=1, ..., Z=25, AA=26."""
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


def index_to_col_letter(idx: int) -> str:
    """Convert 0-based column index to letter(s).  0=A, 25=Z, 26=AA."""
    result = ""
    n = idx + 1
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def parse_addr(addr: str) -> tuple[int, int]:
    """Parse 'A1' -> (row_0based, col_0based).

    Raises ValueError on bad address.
    """
    m = _ADDR_RE.match(addr.upper())
    if not m:
        raise ValueError(f"Invalid cell address: {addr!r}")
    col = col_letter_to_index(m.group(1))
    row = int(m.group(2)) - 1
    return row, col


def make_addr(row: int, col: int) -> str:
    """Build cell address from 0-based row/col."""
    return f"{index_to_col_letter(col)}{row + 1}"


# ---------------------------------------------------------------------------
# ProjectService
# ---------------------------------------------------------------------------


class ProjectService:
    """In-memory service that wraps a single fin123 project.

    Parameters
    ----------
    project_dir : Path
        Root of the fin123 project.
    model_version_id : str | None
        Load a specific snapshot version. If provided, the service starts
        in read-only mode (unless it matches the latest version).
    """

    def __init__(
        self,
        project_dir: Path | None = None,
        model_version_id: str | None = None,
    ) -> None:
        if project_dir is None:
            raise ValueError("project_dir is required")

        self.project_dir = project_dir.resolve()
        spec_path = self.project_dir / "workbook.yaml"
        if not spec_path.exists():
            raise FileNotFoundError(f"No workbook.yaml in {self.project_dir}")

        self._raw_yaml = spec_path.read_text()
        self._spec: dict[str, Any] = yaml.safe_load(self._raw_yaml) or {}

        # Ensure model_id exists
        self._model_id = ensure_model_id(self._spec, spec_path)

        # Read-only flag — set when viewing old versions
        self._read_only = False

        # Working copy of sheets (in-memory, potentially dirty)
        self._sheets: list[dict[str, Any]] = self._load_sheets()
        self._names: dict[str, dict[str, str]] = dict(self._spec.get("names", {}))
        self._dirty = False

        # Track current snapshot version
        self._snapshot_version = self._latest_snapshot_version()

        # If a specific version was requested, load it
        if model_version_id is not None:
            self.select_model_version(model_version_id)

        # Lazy CellGraph — rebuilt when needed
        self._cell_graph = None

    # ------------------------------------------------------------------
    # Read-only guard
    # ------------------------------------------------------------------

    def _check_writable(self) -> None:
        """Raise ValueError if the service is in read-only mode."""
        if self._read_only:
            raise ValueError(
                "Service is in read-only mode (viewing an old version). "
                "Switch back to the latest version to make changes."
            )

    # ------------------------------------------------------------------
    # Sheet data helpers
    # ------------------------------------------------------------------

    def _load_sheets(self) -> list[dict[str, Any]]:
        """Load sheet definitions from the workbook spec.

        If the spec doesn't have a ``sheets`` key, create a default Sheet1.
        Each sheet dict has: name, n_rows, n_cols, cells, fmt (optional).
        """
        raw_sheets = self._spec.get("sheets")
        if raw_sheets:
            sheets = []
            for s in raw_sheets:
                d = dict(s)
                d.setdefault("cells", {})
                d.setdefault("fmt", {})
                d.setdefault("n_rows", 200)
                d.setdefault("n_cols", 40)
                sheets.append(d)
            return sheets
        return [
            {
                "name": "Sheet1",
                "n_rows": 200,
                "n_cols": 40,
                "cells": {},
                "fmt": {},
            }
        ]

    def _latest_snapshot_version(self) -> str | None:
        """Return the latest snapshot version string, or None."""
        snap_dir = self.project_dir / "snapshots" / "workbook"
        if not snap_dir.exists():
            return None
        versions = sorted(
            d.name for d in snap_dir.iterdir() if d.is_dir() and d.name.startswith("v")
        )
        return versions[-1] if versions else None

    def _latest_run_id(self) -> str | None:
        """Return the most recent run_id, or None."""
        store = RunStore(self.project_dir)
        runs = store.list_runs()
        return runs[-1]["run_id"] if runs else None

    def _get_sheet(self, sheet_name: str) -> dict[str, Any]:
        """Get sheet dict by name, raising ValueError if missing."""
        for s in self._sheets:
            if s["name"] == sheet_name:
                return s
        raise ValueError(f"Sheet {sheet_name!r} not found")

    # ------------------------------------------------------------------
    # Sheet management (CRUD)
    # ------------------------------------------------------------------

    def list_sheets(self) -> list[dict[str, Any]]:
        """Return list of sheet summaries (name, n_rows, n_cols)."""
        return [
            {"name": s["name"], "n_rows": s.get("n_rows", 200), "n_cols": s.get("n_cols", 40)}
            for s in self._sheets
        ]

    def add_sheet(self, name: str) -> dict[str, Any]:
        """Add a new empty sheet.  Raises ValueError on duplicate name."""
        self._check_writable()
        if any(s["name"] == name for s in self._sheets):
            raise ValueError(f"Sheet {name!r} already exists")
        sheet = {
            "name": name,
            "n_rows": 200,
            "n_cols": 40,
            "cells": {},
            "fmt": {},
        }
        self._sheets.append(sheet)
        self._dirty = True
        self._cell_graph = None
        return {"name": name, "n_rows": 200, "n_cols": 40}

    def delete_sheet(self, name: str) -> dict[str, Any]:
        """Delete a sheet by name.  Must keep at least one sheet."""
        self._check_writable()
        if len(self._sheets) <= 1:
            raise ValueError("Cannot delete the only sheet")
        idx = None
        for i, s in enumerate(self._sheets):
            if s["name"] == name:
                idx = i
                break
        if idx is None:
            raise ValueError(f"Sheet {name!r} not found")
        self._sheets.pop(idx)
        self._dirty = True
        self._cell_graph = None
        return {"deleted": name, "remaining": [s["name"] for s in self._sheets]}

    def rename_sheet(self, old_name: str, new_name: str) -> dict[str, Any]:
        """Rename a sheet.  Raises ValueError on duplicate or missing."""
        self._check_writable()
        if any(s["name"] == new_name for s in self._sheets):
            raise ValueError(f"Sheet {new_name!r} already exists")
        sheet = self._get_sheet(old_name)
        sheet["name"] = new_name
        self._dirty = True
        self._cell_graph = None
        return {"old_name": old_name, "new_name": new_name}

    # ------------------------------------------------------------------
    # Row/column insertion & deletion
    # ------------------------------------------------------------------

    def _shift_sheet(
        self,
        sheet_name: str,
        axis: str,
        index: int,
        count: int,
    ) -> None:
        """Shift rows or columns in a sheet and update all references.

        Args:
            sheet_name: The sheet to modify.
            axis: "row" or "col".
            index: 0-based index where insertion/deletion starts.
            count: Positive for insert, negative for delete.
        """
        sheet = self._get_sheet(sheet_name)

        # 1. Remap cells and fmt address keys in the affected sheet
        sheet["cells"] = _remap_addresses(sheet.get("cells", {}), axis, index, count)
        sheet["fmt"] = _remap_addresses(sheet.get("fmt", {}), axis, index, count)

        # 2. Rewrite formulas in ALL sheets (cross-sheet refs may point here)
        for s in self._sheets:
            cells_map = s.get("cells", {})
            for addr_key, cell in list(cells_map.items()):
                formula = cell.get("formula")
                if formula:
                    new_formula = rewrite_formula_refs(
                        formula, sheet_name, s["name"], axis, index, count
                    )
                    if new_formula != formula:
                        cells_map[addr_key] = {"formula": new_formula}

        # 3. Shift named range start/end addresses on affected sheet
        for _name, defn in self._names.items():
            if defn.get("sheet") != sheet_name:
                continue
            for field in ("start", "end"):
                addr_val = defn.get(field, "")
                if not addr_val:
                    continue
                try:
                    r, c = parse_addr(addr_val)
                except ValueError:
                    continue
                ref_pos = r if axis == "row" else c
                if count > 0:
                    if ref_pos >= index:
                        if axis == "row":
                            r += count
                        else:
                            c += count
                else:
                    abs_count = abs(count)
                    if index <= ref_pos < index + abs_count:
                        continue  # in deleted range, leave as-is
                    elif ref_pos >= index + abs_count:
                        if axis == "row":
                            r += count
                        else:
                            c += count
                defn[field] = make_addr(r, c)

        # 4. Adjust n_rows / n_cols (floor at 1)
        if axis == "row":
            sheet["n_rows"] = max(1, sheet.get("n_rows", 200) + count)
        else:
            sheet["n_cols"] = max(1, sheet.get("n_cols", 40) + count)

        # 5. Mark dirty, invalidate graph
        self._dirty = True
        self._cell_graph = None

    def insert_rows(
        self, sheet_name: str, row_idx: int, count: int = 1
    ) -> dict[str, Any]:
        """Insert rows before the given 0-based row index.

        Args:
            sheet_name: Target sheet.
            row_idx: 0-based row index to insert before.
            count: Number of rows to insert (must be >= 1).

        Returns:
            Dict with ok, n_rows, dirty.
        """
        self._check_writable()
        sheet = self._get_sheet(sheet_name)
        n_rows = sheet.get("n_rows", 200)
        if row_idx < 0 or row_idx > n_rows:
            raise ValueError(f"row_idx {row_idx} out of range [0, {n_rows}]")
        if count < 1:
            raise ValueError("count must be >= 1")
        self._shift_sheet(sheet_name, "row", row_idx, count)
        return {"ok": True, "n_rows": sheet["n_rows"], "n_cols": sheet["n_cols"], "dirty": self._dirty}

    def delete_rows(
        self, sheet_name: str, row_idx: int, count: int = 1
    ) -> dict[str, Any]:
        """Delete rows starting at the given 0-based row index.

        Args:
            sheet_name: Target sheet.
            row_idx: 0-based row index to start deleting.
            count: Number of rows to delete (must be >= 1).

        Returns:
            Dict with ok, n_rows, dirty.
        """
        self._check_writable()
        sheet = self._get_sheet(sheet_name)
        n_rows = sheet.get("n_rows", 200)
        if row_idx < 0 or row_idx >= n_rows:
            raise ValueError(f"row_idx {row_idx} out of range [0, {n_rows})")
        if count < 1:
            raise ValueError("count must be >= 1")
        count = min(count, n_rows - row_idx)  # clamp to available
        self._shift_sheet(sheet_name, "row", row_idx, -count)
        return {"ok": True, "n_rows": sheet["n_rows"], "n_cols": sheet["n_cols"], "dirty": self._dirty}

    def insert_cols(
        self, sheet_name: str, col_idx: int, count: int = 1
    ) -> dict[str, Any]:
        """Insert columns before the given 0-based column index.

        Args:
            sheet_name: Target sheet.
            col_idx: 0-based column index to insert before.
            count: Number of columns to insert (must be >= 1).

        Returns:
            Dict with ok, n_cols, dirty.
        """
        self._check_writable()
        sheet = self._get_sheet(sheet_name)
        n_cols = sheet.get("n_cols", 40)
        if col_idx < 0 or col_idx > n_cols:
            raise ValueError(f"col_idx {col_idx} out of range [0, {n_cols}]")
        if count < 1:
            raise ValueError("count must be >= 1")
        self._shift_sheet(sheet_name, "col", col_idx, count)
        return {"ok": True, "n_rows": sheet["n_rows"], "n_cols": sheet["n_cols"], "dirty": self._dirty}

    def delete_cols(
        self, sheet_name: str, col_idx: int, count: int = 1
    ) -> dict[str, Any]:
        """Delete columns starting at the given 0-based column index.

        Args:
            sheet_name: Target sheet.
            col_idx: 0-based column index to start deleting.
            count: Number of columns to delete (must be >= 1).

        Returns:
            Dict with ok, n_cols, dirty.
        """
        self._check_writable()
        sheet = self._get_sheet(sheet_name)
        n_cols = sheet.get("n_cols", 40)
        if col_idx < 0 or col_idx >= n_cols:
            raise ValueError(f"col_idx {col_idx} out of range [0, {n_cols})")
        if count < 1:
            raise ValueError("count must be >= 1")
        count = min(count, n_cols - col_idx)  # clamp to available
        self._shift_sheet(sheet_name, "col", col_idx, -count)
        return {"ok": True, "n_rows": sheet["n_rows"], "n_cols": sheet["n_cols"], "dirty": self._dirty}

    # ------------------------------------------------------------------
    # Named ranges (CRUD)
    # ------------------------------------------------------------------

    def list_names(self) -> dict[str, dict[str, str]]:
        """Return all named ranges."""
        return dict(self._names)

    def get_name(self, name: str) -> dict[str, str]:
        """Get a named range definition.  Raises KeyError if missing."""
        if name not in self._names:
            raise KeyError(f"Named range {name!r} not found")
        return dict(self._names[name])

    def set_name(self, name: str, sheet: str, start: str, end: str) -> dict[str, Any]:
        """Create or overwrite a named range.

        Args:
            name: Range identifier (must be a valid identifier).
            sheet: Sheet name the range belongs to.
            start: Top-left address (e.g. "A1").
            end: Bottom-right address (e.g. "C10").

        Returns:
            Dict with the created definition.
        """
        self._check_writable()
        # Validate addresses
        parse_addr(start)
        parse_addr(end)
        # Validate sheet exists
        self._get_sheet(sheet)

        self._names[name] = {"sheet": sheet, "start": start.upper(), "end": end.upper()}
        self._dirty = True
        self._cell_graph = None  # invalidate
        return {"name": name, **self._names[name]}

    def update_name(self, name: str, **updates: str) -> dict[str, Any]:
        """Update fields of an existing named range.

        Args:
            name: Range identifier.
            **updates: Fields to update (sheet, start, end).

        Returns:
            Updated definition.
        """
        self._check_writable()
        if name not in self._names:
            raise KeyError(f"Named range {name!r} not found")

        defn = self._names[name]
        if "sheet" in updates:
            self._get_sheet(updates["sheet"])
            defn["sheet"] = updates["sheet"]
        if "start" in updates:
            parse_addr(updates["start"])
            defn["start"] = updates["start"].upper()
        if "end" in updates:
            parse_addr(updates["end"])
            defn["end"] = updates["end"].upper()

        self._dirty = True
        self._cell_graph = None
        return {"name": name, **defn}

    def delete_name(self, name: str) -> dict[str, Any]:
        """Delete a named range.  Raises KeyError if missing."""
        self._check_writable()
        if name not in self._names:
            raise KeyError(f"Named range {name!r} not found")
        del self._names[name]
        self._dirty = True
        self._cell_graph = None
        return {"deleted": name}

    # ------------------------------------------------------------------
    # CellGraph integration
    # ------------------------------------------------------------------

    def _build_cell_graph(self):
        """Lazily build (or rebuild) the CellGraph from current sheet data."""
        from fin123.cell_graph import CellGraph

        sheets_data: dict[str, dict[str, Any]] = {}
        for s in self._sheets:
            sheets_data[s["name"]] = s.get("cells", {})
        params = dict(self._spec.get("params", {}))
        self._cell_graph = CellGraph(sheets_data, self._names, params=params)
        return self._cell_graph

    def _get_cell_graph(self):
        """Return the current CellGraph, building if needed."""
        if self._cell_graph is None:
            return self._build_cell_graph()
        return self._cell_graph

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_project_info(self) -> dict[str, Any]:
        """Return project metadata for the UI."""
        from fin123.project import get_project_mode

        workflows = []
        for wf in self._spec.get("workflows", []):
            workflows.append(wf.get("name", wf.get("file", "unknown")))

        output_tables = []
        for out in self._spec.get("outputs", []):
            if out.get("type") == "table":
                output_tables.append(out["name"])

        return {
            "project_dir": str(self.project_dir),
            "engine_version": __version__,
            "snapshot_version": self._snapshot_version,
            "last_run_id": self._latest_run_id(),
            "dirty": self._dirty,
            "sheets": [s["name"] for s in self._sheets],
            "names": dict(self._names),
            "params": self._spec.get("params", {}),
            "workflows": workflows,
            "output_tables": output_tables,
            "has_import_report": (self.project_dir / "import_reports" / "index.json").exists(),
            "mode": get_project_mode(self.project_dir),
        }

    def get_sheet_viewport(
        self,
        sheet_name: str = "Sheet1",
        r0: int = 0,
        c0: int = 0,
        rows: int = 30,
        cols: int = 15,
    ) -> dict[str, Any]:
        """Return a viewport of cells for rendering.

        Returns dict with:
          - cells: list of {addr, row, col, raw, display, fmt?} for non-empty cells in range
          - n_rows, n_cols: sheet dimensions
          - r0, c0, rows, cols: the requested viewport

        Formulas are evaluated via CellGraph to produce computed display values.
        """
        sheet = self._get_sheet(sheet_name)
        cells_map = sheet.get("cells", {})
        fmt_map = sheet.get("fmt", {})
        n_rows = sheet.get("n_rows", 200)
        n_cols = sheet.get("n_cols", 40)

        cg = self._get_cell_graph()

        cells = []
        for r in range(r0, min(r0 + rows, n_rows)):
            for c in range(c0, min(c0 + cols, n_cols)):
                addr = make_addr(r, c)
                has_cell = addr in cells_map
                has_fmt = addr in fmt_map
                if not has_cell and not has_fmt:
                    continue
                entry: dict[str, Any] = {
                    "addr": addr,
                    "row": r,
                    "col": c,
                }
                if has_cell:
                    cell = cells_map[addr]
                    raw = cell.get("formula") or cell.get("value", "")
                    entry["raw"] = str(raw)
                    # Use CellGraph for computed display values
                    entry["display"] = cg.get_display_value(sheet_name, addr)
                else:
                    entry["raw"] = ""
                    entry["display"] = ""
                if has_fmt:
                    entry["fmt"] = fmt_map[addr]
                cells.append(entry)

        return {
            "sheet": sheet_name,
            "n_rows": n_rows,
            "n_cols": n_cols,
            "r0": r0,
            "c0": c0,
            "rows": rows,
            "cols": cols,
            "cells": cells,
        }

    def update_cells(
        self,
        sheet_name: str,
        edits: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Apply batch cell edits to the working copy.

        Args:
            sheet_name: Target sheet name.
            edits: List of dicts with ``addr`` and either ``value`` or ``formula``.

        Returns:
            Dict with ``ok``, ``dirty``, ``errors`` (per-cell structured errors).
            Each error has: addr, code, message, and optionally position.
        """
        self._check_writable()
        sheet = self._get_sheet(sheet_name)
        cells_map = sheet.setdefault("cells", {})
        errors: list[dict[str, Any]] = []

        for edit in edits:
            addr = edit.get("addr", "").upper()
            try:
                parse_addr(addr)
            except ValueError:
                errors.append({
                    "addr": addr,
                    "code": "invalid_address",
                    "message": f"Invalid cell address: {addr!r}",
                })
                continue

            raw = edit.get("formula") or edit.get("value", "")
            raw_str = str(raw)

            if raw_str == "":
                # Clear cell
                cells_map.pop(addr, None)
            elif raw_str.startswith("="):
                # Validate formula parse
                try:
                    from fin123.formulas import parse_formula
                    parse_formula(raw_str)
                except Exception as exc:
                    err: dict[str, Any] = {
                        "addr": addr,
                        "code": "parse_error",
                        "message": str(exc),
                    }
                    # Extract position from FormulaParseError if available
                    if hasattr(exc, "pos"):
                        err["position"] = exc.pos
                    errors.append(err)
                    continue
                cells_map[addr] = {"formula": raw_str}
            else:
                # Try to parse as number
                try:
                    num = float(raw_str)
                    if num == int(num) and "." not in raw_str:
                        cells_map[addr] = {"value": int(num)}
                    else:
                        cells_map[addr] = {"value": num}
                except ValueError:
                    cells_map[addr] = {"value": raw_str}

        self._dirty = True
        self._cell_graph = None  # invalidate computed values

        return {
            "ok": len(errors) == 0,
            "dirty": self._dirty,
            "errors": errors,
        }

    def update_cell_format(
        self,
        sheet_name: str,
        updates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Apply format updates to cells.

        Args:
            sheet_name: Target sheet.
            updates: List of dicts with ``addr`` and ``color`` (hex string or None to clear).

        Returns:
            Dict with ``ok`` and ``dirty``.
        """
        self._check_writable()
        sheet = self._get_sheet(sheet_name)
        fmt_map = sheet.setdefault("fmt", {})

        for upd in updates:
            addr_str = upd.get("addr", "").upper()
            try:
                parse_addr(addr_str)
            except ValueError:
                continue
            color = upd.get("color")
            if color:
                fmt_map[addr_str] = {"color": color}
            else:
                fmt_map.pop(addr_str, None)

        self._dirty = True
        return {"ok": True, "dirty": self._dirty}

    def validate_formula(self, text: str) -> dict[str, Any]:
        """Lightweight formula parse validation (no evaluation).

        Args:
            text: Formula text (must start with '=').

        Returns:
            Dict with ``valid`` bool and optional ``message`` / ``position``.
        """
        if not text.startswith("="):
            return {"valid": False, "message": "Formula must start with '='"}
        try:
            from fin123.formulas import parse_formula
            parse_formula(text)
            return {"valid": True}
        except Exception as exc:
            result: dict[str, Any] = {"valid": False, "message": str(exc)}
            if hasattr(exc, "pos"):
                result["position"] = exc.pos
            return result

    @staticmethod
    def _parse_literal(val: str) -> Any:
        """Convert a display string to a typed value (float, bool, string)."""
        if val in ("TRUE", "FALSE"):
            return val == "TRUE"
        try:
            num = float(val)
            if num == int(num) and "." not in val:
                return int(num)
            return num
        except (ValueError, TypeError):
            return val

    def save_snapshot(self) -> dict[str, Any]:
        """Persist working copy into workbook.yaml and create a new snapshot.

        Returns dict with new snapshot_version and workbook_hash.
        """
        self._check_writable()

        # Scan PARAM() bindings — reject duplicates
        from fin123.cell_graph import scan_param_bindings

        bindings, binding_errors = scan_param_bindings(self._sheets)
        if binding_errors:
            raise ValueError(
                "Cannot commit: duplicate PARAM bindings — " + "; ".join(binding_errors)
            )

        # Store bindings in spec (or remove if empty)
        if bindings:
            self._spec["bindings"] = {
                name: {"sheet": loc[0], "addr": loc[1]}
                for name, loc in bindings.items()
            }
        else:
            self._spec.pop("bindings", None)

        # Auto-declare params: if a PARAM proxy name is not in spec.params, add it
        params = self._spec.setdefault("params", {})
        cg = self._get_cell_graph()
        for param_name, (sheet_name, addr) in bindings.items():
            if param_name not in params:
                display = cg.get_display_value(sheet_name, addr)
                params[param_name] = self._parse_literal(display)

        # Update sheets in spec — strip empty fmt maps for cleaner YAML
        sheets_to_save = []
        for s in self._sheets:
            sd = dict(s)
            if not sd.get("fmt"):
                sd.pop("fmt", None)
            sheets_to_save.append(sd)
        self._spec["sheets"] = sheets_to_save

        # Persist named ranges (strip if empty for cleaner YAML)
        if self._names:
            self._spec["names"] = dict(self._names)
        else:
            self._spec.pop("names", None)

        # Write back to workbook.yaml
        new_yaml = yaml.dump(self._spec, default_flow_style=False, sort_keys=False)
        (self.project_dir / "workbook.yaml").write_text(new_yaml)
        self._raw_yaml = new_yaml

        # Create snapshot
        store = SnapshotStore(self.project_dir)
        version = store.save_snapshot(new_yaml)
        self._snapshot_version = version
        self._dirty = False

        from fin123.utils.hash import sha256_dict

        workbook_hash = sha256_dict(self._spec)

        # Push to registry if enabled
        self._registry_push_version(version, new_yaml, workbook_hash)

        return {
            "snapshot_version": version,
            "workbook_hash": workbook_hash,
            "dirty": False,
        }

    def unbind_param(self, sheet_name: str, addr: str) -> dict[str, Any]:
        """Replace =PARAM("name") with its current literal value.

        Args:
            sheet_name: Sheet containing the PARAM cell.
            addr: Cell address.

        Returns:
            Dict with ok and the resolved value.
        """
        self._check_writable()
        addr = addr.upper()
        parse_addr(addr)
        sheet = self._get_sheet(sheet_name)
        cells_map = sheet.get("cells", {})
        cell = cells_map.get(addr)

        if not cell or "formula" not in cell:
            raise ValueError(f"Cell {sheet_name}!{addr} does not contain a formula")

        import re as _re
        m = _re.match(r'^=PARAM\(\s*"([^"]+)"\s*\)$', cell["formula"], _re.IGNORECASE)
        if not m:
            raise ValueError(f"Cell {sheet_name}!{addr} is not a PARAM formula")

        cg = self._get_cell_graph()
        display = cg.get_display_value(sheet_name, addr)
        value = self._parse_literal(display)

        cells_map[addr] = {"value": value}
        self._cell_graph = None
        self._dirty = True
        return {"ok": True, "value": value}

    def build_workbook(self) -> dict[str, Any]:
        """Build (execute) the latest saved workbook snapshot.

        Returns dict with run_id, snapshot_version_used, scalar/table summaries.
        """
        if self._dirty:
            return {
                "error": "Working copy has uncommitted edits. Commit before building.",
                "dirty": True,
            }

        from fin123.workbook import Workbook

        wb = Workbook(self.project_dir)
        result = wb.run()

        run_id = result.run_dir.name

        # Push run to registry if enabled
        from fin123.utils.hash import sha256_dict

        self._registry_push_run(
            run_id=run_id,
            model_version_id=self._snapshot_version or "",
            workbook_hash=sha256_dict(self._spec),
            run_meta={
                "run_id": run_id,
                "scalars": {k: str(v) for k, v in result.scalars.items()},
            },
        )

        return {
            "run_id": run_id,
            "snapshot_version": self._snapshot_version,
            "scalars": result.scalars,
            "tables": {
                name: {"rows": len(df), "cols": len(df.columns)}
                for name, df in result.tables.items()
            },
        }

    # Backward-compatible alias
    run_workbook = build_workbook

    def run_sync(self, table_name: str | None = None) -> dict[str, Any]:
        """Trigger SQL sync.

        Args:
            table_name: Optional specific table to sync.

        Returns:
            Sync result summary.
        """
        try:
            from fin123.sync import run_sync
        except ImportError:
            return {"synced": [], "skipped": [], "errors": ["Requires fin123-pod"], "warnings": []}

        result = run_sync(self.project_dir, table_name=table_name)
        return {
            "synced": result["synced"],
            "skipped": result["skipped"],
            "errors": result["errors"],
            "warnings": result.get("warnings", []),
        }

    def run_workflow(self, workflow_name: str) -> dict[str, Any]:
        """Run a named workflow.

        Returns artifact version and status.
        """
        try:
            from fin123.workflows.runner import run_workflow
        except ImportError:
            return {
                "workflow": workflow_name,
                "artifact_name": "",
                "artifact_version": "",
                "scenario_count": 0,
                "status": "error",
                "error": "Requires fin123-pod",
            }

        result = run_workflow(workflow_name, self.project_dir)
        return {
            "workflow": result["workflow"],
            "artifact_name": result.get("artifact_name", ""),
            "artifact_version": result.get("artifact_version", ""),
            "scenario_count": result.get("scenario_count", 0),
            "status": "completed",
        }

    def list_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent runs (newest first)."""
        store = RunStore(self.project_dir)
        runs = store.list_runs()
        # Newest first, limited
        return list(reversed(runs[-limit:]))

    def get_latest_run(self) -> dict[str, Any] | None:
        """Return metadata for the latest run, or None."""
        store = RunStore(self.project_dir)
        runs = store.list_runs()
        return runs[-1] if runs else None

    def get_scalar_outputs(self, run_id: str | None = None) -> dict[str, Any]:
        """Return scalar outputs for a run (default latest)."""
        run_dir = self._resolve_run_dir(run_id)
        if run_dir is None:
            return {"error": "No runs found", "scalars": {}}

        scalars_path = run_dir / "outputs" / "scalars.json"
        if scalars_path.exists():
            return {"scalars": json.loads(scalars_path.read_text())}
        return {"scalars": {}}

    def get_table_output(
        self,
        table_name: str,
        run_id: str | None = None,
        limit: int = 5000,
    ) -> dict[str, Any]:
        """Return table output as JSON-serializable rows (limited)."""
        run_dir = self._resolve_run_dir(run_id)
        if run_dir is None:
            return {"error": "No runs found"}

        parquet_path = run_dir / "outputs" / f"{table_name}.parquet"
        if not parquet_path.exists():
            return {"error": f"Table {table_name!r} not found in run outputs"}

        df = pl.read_parquet(parquet_path)
        total_rows = len(df)
        df = df.head(limit)

        return {
            "table": table_name,
            "columns": df.columns,
            "rows": df.to_dicts(),
            "total_rows": total_rows,
            "limited": total_rows > limit,
        }

    def get_table_download_path(
        self,
        table_name: str,
        run_id: str | None = None,
    ) -> Path | None:
        """Return the parquet file path for streaming download."""
        run_dir = self._resolve_run_dir(run_id)
        if run_dir is None:
            return None
        parquet_path = run_dir / "outputs" / f"{table_name}.parquet"
        return parquet_path if parquet_path.exists() else None

    def list_snapshots(self, limit: int = 50) -> list[dict[str, str]]:
        """Return snapshot versions (newest first)."""
        snap_dir = self.project_dir / "snapshots" / "workbook"
        if not snap_dir.exists():
            return []
        versions = sorted(
            (d.name for d in snap_dir.iterdir() if d.is_dir() and d.name.startswith("v")),
            reverse=True,
        )
        return [{"version": v} for v in versions[:limit]]

    def list_artifacts(self) -> dict[str, list[dict[str, Any]]]:
        """Return all artifacts grouped by name."""
        store = ArtifactStore(self.project_dir)
        return store.list_artifacts()

    def get_datasheets(self) -> list[dict[str, Any]]:
        """Return status information for each SQL-sourced table.

        For each SQL table in the workbook spec, returns:
        - table_name, cache_path, refresh_policy
        - last_sync_id, last_sync_time, last_status, last_rowcount
        - last_schema_warnings
        - cache_file_exists, cache_file_mtime
        - staleness: "fresh", "stale", "unknown", "fail"
        """
        tables = self._spec.get("tables", {})
        sync_history = self._load_sync_history()

        results = []
        for tname, tspec in tables.items():
            if tspec.get("source") != "sql":
                continue

            cache_rel = tspec.get("cache", "")
            cache_path = self.project_dir / cache_rel if cache_rel else None
            refresh = tspec.get("refresh", "manual")

            # Find latest sync for this table
            last_sync = sync_history.get(tname)

            cache_exists = cache_path is not None and cache_path.exists()
            cache_mtime = None
            if cache_exists:
                cache_mtime = cache_path.stat().st_mtime

            # Determine staleness
            staleness = self._classify_staleness(
                last_sync=last_sync,
                cache_exists=cache_exists,
                cache_mtime=cache_mtime,
                refresh=refresh,
                tspec=tspec,
            )

            entry: dict[str, Any] = {
                "table_name": tname,
                "cache_path": cache_rel,
                "refresh_policy": refresh,
                "last_sync_id": last_sync["sync_id"] if last_sync else None,
                "last_sync_time": last_sync["timestamp"] if last_sync else None,
                "last_status": last_sync["status"] if last_sync else None,
                "last_rowcount": last_sync["rowcount"] if last_sync else None,
                "last_schema_warnings": last_sync.get("warnings", []) if last_sync else [],
                "cache_file_exists": cache_exists,
                "cache_file_mtime": cache_mtime,
                "staleness": staleness,
            }
            results.append(entry)

        return results

    # ------------------------------------------------------------------
    # Model identity & versioning
    # ------------------------------------------------------------------

    def get_model_info(self) -> dict[str, Any]:
        """Return model identity and version info."""
        latest = self._latest_snapshot_version()
        return {
            "model_id": self._model_id,
            "current_model_version_id": self._snapshot_version,
            "latest_model_version_id": latest,
            "read_only": self._read_only,
        }

    def list_model_versions(self) -> list[dict]:
        """Return all model versions from the snapshot index."""
        store = SnapshotStore(self.project_dir)
        index = store.load_index()
        return index.get("versions", [])

    def select_model_version(self, version: str) -> dict[str, Any]:
        """Load a specific snapshot version into memory.

        If the version is the latest, read_only is cleared. Otherwise,
        read_only is set to True.

        Args:
            version: Version string (e.g. 'v0001').

        Returns:
            Model info dict.
        """
        store = SnapshotStore(self.project_dir)
        spec = store.load_version(version)
        latest = self._latest_snapshot_version()

        self._spec = spec
        self._sheets = self._load_sheets()
        self._names = dict(self._spec.get("names", {}))
        self._snapshot_version = version
        self._read_only = (version != latest)
        self._dirty = False
        self._cell_graph = None

        return self.get_model_info()

    def pin_model_version(self, version: str) -> None:
        """Pin a model version to protect it from GC."""
        store = SnapshotStore(self.project_dir)
        store.pin_version(version)

    def unpin_model_version(self, version: str) -> None:
        """Unpin a model version."""
        store = SnapshotStore(self.project_dir)
        store.unpin_version(version)

    # ------------------------------------------------------------------
    # Clear cache
    # ------------------------------------------------------------------

    def clear_cache(self, dry_run: bool = True) -> dict[str, Any]:
        """Run GC and clear hash cache.

        Args:
            dry_run: If True, report what would be done without doing it.

        Returns:
            Summary dict.
        """
        from fin123.gc import run_gc

        summary = run_gc(self.project_dir, dry_run=dry_run)

        # Tally hash cache size
        hash_path = self.project_dir / "cache" / "hashes.json"
        hash_cache_bytes = 0
        if hash_path.exists():
            hash_cache_bytes = hash_path.stat().st_size
            if not dry_run:
                hash_path.unlink()

        summary["hash_cache_bytes"] = hash_cache_bytes
        return summary

    # ------------------------------------------------------------------
    # Import reports
    # ------------------------------------------------------------------

    def get_latest_import_report(self) -> dict[str, Any] | None:
        """Return the latest import report, or None."""
        # Check versioned index first
        index_path = self.project_dir / "import_reports" / "index.json"
        if index_path.exists():
            try:
                index = json.loads(index_path.read_text())
                if index:
                    latest = index[-1]
                    report_path = self.project_dir / latest["path"]
                    if report_path.exists():
                        return json.loads(report_path.read_text())
            except (json.JSONDecodeError, OSError, KeyError):
                pass

        # Fall back to root import_report.json
        root_path = self.project_dir / "import_report.json"
        if root_path.exists():
            try:
                return json.loads(root_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        return None

    def list_import_reports(self) -> list[dict[str, str]]:
        """Return index entries of all import reports."""
        index_path = self.project_dir / "import_reports" / "index.json"
        if index_path.exists():
            try:
                return json.loads(index_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass
        return []

    def _get_latest_import_report_dir(self) -> Path | None:
        """Return the Path of the latest import report directory, or None."""
        index_path = self.project_dir / "import_reports" / "index.json"
        if index_path.exists():
            try:
                index = json.loads(index_path.read_text())
                if index:
                    latest = index[-1]
                    report_path = self.project_dir / latest["path"]
                    return report_path.parent
            except (json.JSONDecodeError, OSError, KeyError):
                pass
        return None

    def get_latest_import_trace(self) -> str | None:
        """Return the contents of the latest import trace log, or None."""
        path = self.get_latest_import_trace_path()
        if path and path.exists():
            return path.read_text()
        return None

    def get_latest_import_trace_path(self) -> Path | None:
        """Return the Path of the latest import_trace.log, or None."""
        report_dir = self._get_latest_import_report_dir()
        if report_dir:
            trace = report_dir / "import_trace.log"
            if trace.exists():
                return trace
        return None

    def mark_import_todo(self, sheet_name: str, addr: str) -> dict[str, Any]:
        """Mark a cell as TODO for import review.

        Sets amber color and a review comment.
        """
        self._check_writable()
        sheet = self._get_sheet(sheet_name)
        fmt_map = sheet.setdefault("fmt", {})
        cells_map = sheet.setdefault("cells", {})
        addr = addr.upper()
        parse_addr(addr)  # validate

        fmt_map[addr] = {"color": "#f59e0b"}
        if addr in cells_map:
            cells_map[addr]["comment"] = "TODO: review imported formula"
        self._dirty = True
        return {"ok": True}

    def convert_to_value(self, sheet_name: str, addr: str) -> dict[str, Any]:
        """Replace a formula cell with its cached computed value.

        Rejects if the value is an error or empty.
        """
        self._check_writable()
        sheet = self._get_sheet(sheet_name)
        cells_map = sheet.get("cells", {})
        addr = addr.upper()
        parse_addr(addr)

        cell = cells_map.get(addr)
        if not cell or "formula" not in cell:
            raise ValueError(f"Cell {addr} does not contain a formula")

        cg = self._get_cell_graph()
        display = cg.get_display_value(sheet_name, addr)

        if display in ("#ERR!", "#CIRC!", "") or display is None:
            raise ValueError(f"Cannot convert: cell {addr} has value {display!r}")

        # Parse numeric
        try:
            num = float(display)
            if num == int(num) and "." not in str(display):
                value: Any = int(num)
            else:
                value = num
        except (ValueError, TypeError):
            value = display

        cells_map[addr] = {"value": value}
        self._cell_graph = None
        self._dirty = True
        return {"ok": True, "value": value}

    # ------------------------------------------------------------------
    # Project health
    # ------------------------------------------------------------------

    def get_project_health(self) -> dict[str, Any]:
        """Aggregate project health from multiple sources.

        Returns:
            Dict with status ("ok"|"warn"|"error") and issues list.
        """
        issues: list[dict[str, Any]] = []

        # 1. Datasheets
        try:
            for ds in self.get_datasheets():
                staleness = ds.get("staleness", "unknown")
                tname = ds.get("table_name", "")
                if staleness == "stale":
                    issues.append({
                        "severity": "warning",
                        "code": "datasheet_stale",
                        "message": f"Datasheet '{tname}' cache is stale",
                        "target": tname,
                    })
                elif staleness == "fail":
                    issues.append({
                        "severity": "error",
                        "code": "datasheet_fail",
                        "message": f"Datasheet '{tname}' last sync failed",
                        "target": tname,
                    })
                elif staleness == "unknown":
                    issues.append({
                        "severity": "info",
                        "code": "datasheet_never_synced",
                        "message": f"Datasheet '{tname}' has never been synced",
                        "target": tname,
                    })
        except Exception:
            pass

        # 2. Import report
        try:
            report = self.get_latest_import_report()
            if report:
                for feat in report.get("skipped_features", []):
                    issues.append({
                        "severity": "info",
                        "code": "import_skipped_feature",
                        "message": f"Import skipped: {feat}",
                        "target": None,
                    })
                for warn in report.get("warnings", []):
                    issues.append({
                        "severity": "warning",
                        "code": "import_warning",
                        "message": warn,
                        "target": None,
                    })

                # Classification-based issues (Phase 8)
                cls_summary = report.get("classification_summary", {})
                if cls_summary.get("parse_errors", 0) > 0:
                    # Per-cell error entries (max 10)
                    count = 0
                    for cls_entry in report.get("formula_classifications", []):
                        if cls_entry.get("classification") == "parse_error" and count < 10:
                            issues.append({
                                "severity": "error",
                                "code": "import_formula_parse_error",
                                "message": f"Import parse error in {cls_entry['sheet']}!{cls_entry['addr']}: {cls_entry.get('error_message', '')}",
                                "target": {"sheet": cls_entry["sheet"], "addr": cls_entry["addr"]},
                            })
                            count += 1
                if cls_summary.get("unsupported_functions", 0) > 0:
                    issues.append({
                        "severity": "warning",
                        "code": "import_unsupported_functions",
                        "message": f"Import has {cls_summary['unsupported_functions']} formula(s) with unsupported functions",
                        "target": None,
                    })
                if cls_summary.get("external_links", 0) > 0:
                    issues.append({
                        "severity": "warning",
                        "code": "import_external_links",
                        "message": f"Import has {cls_summary['external_links']} formula(s) with external links",
                        "target": None,
                    })
                if cls_summary.get("plugin_formulas", 0) > 0:
                    issues.append({
                        "severity": "warning",
                        "code": "import_plugin_formulas",
                        "message": f"Import has {cls_summary['plugin_formulas']} plugin formula(s)",
                        "target": None,
                    })
        except Exception:
            pass

        # 3. Formula errors from CellGraph
        try:
            cg = self._get_cell_graph()
            if hasattr(cg, "_errors") and cg._errors:
                for key, err in cg._errors.items():
                    target = key if isinstance(key, str) else str(key)
                    code = "formula_parse_error"
                    msg = str(err)
                    if "cycle" in msg.lower() or "circ" in msg.lower():
                        code = "formula_cycle"
                    issues.append({
                        "severity": "error",
                        "code": code,
                        "message": msg,
                        "target": target,
                    })
        except Exception:
            pass

        # 4. Registry status
        try:
            config = load_project_config(self.project_dir)
            if config.get("registry_backend") == "postgres":
                reg = self._get_registry()
                if reg is None:
                    issues.append({
                        "severity": "warning",
                        "code": "registry_unavailable",
                        "message": "Postgres registry configured but not reachable",
                        "target": None,
                    })
                elif not reg.ping():
                    issues.append({
                        "severity": "warning",
                        "code": "registry_unreachable",
                        "message": "Postgres registry is not responding",
                        "target": None,
                    })
        except Exception:
            pass

        # 5. Model version status
        try:
            store = SnapshotStore(self.project_dir)
            index = store.load_index()
            versions = index.get("versions", [])
            config = load_project_config(self.project_dir)
            max_versions = config.get("max_model_versions", 200)
            if len(versions) > max_versions * 0.9:
                issues.append({
                    "severity": "warning",
                    "code": "versions_near_limit",
                    "message": f"Model versions ({len(versions)}) approaching limit ({max_versions})",
                    "target": None,
                })
        except Exception:
            pass

        # Derive status
        has_error = any(i["severity"] == "error" for i in issues)
        has_warning = any(i["severity"] == "warning" for i in issues)
        if has_error:
            status = "error"
        elif has_warning:
            status = "warn"
        else:
            status = "ok"

        return {"status": status, "issues": issues}

    # ------------------------------------------------------------------
    # Model status (compact ribbon data)
    # ------------------------------------------------------------------

    def get_model_status(self) -> dict[str, Any]:
        """Return a compact status snapshot for the UI status ribbon.

        Aggregates dirty state, datasheet staleness, latest build info,
        and verify status into a single cheap response.

        Returns:
            Dict with project, datasheets, build, and verify sections.
        """
        # -- Project section --
        project_section = {
            "dirty": self._dirty,
            "model_id": self._model_id,
            "model_version_id": self._snapshot_version,
            "read_only": self._read_only,
        }

        # -- Datasheets section --
        counts: dict[str, int] = {"fresh": 0, "stale": 0, "fail": 0, "unknown": 0}
        stale_tables: list[str] = []
        try:
            for ds in self.get_datasheets():
                s = ds.get("staleness", "unknown")
                counts[s] = counts.get(s, 0) + 1
                if s in ("stale", "fail"):
                    stale_tables.append(ds.get("table_name", ""))
        except Exception:
            pass

        total_ds = sum(counts.values())
        if total_ds == 0:
            summary_status = "none"
        elif counts["fail"] > 0:
            summary_status = "fail"
        elif counts["stale"] > 0:
            summary_status = "stale"
        elif counts["unknown"] > 0:
            summary_status = "unknown"
        else:
            summary_status = "fresh"

        datasheets_section = {
            "summary_status": summary_status,
            "counts": counts,
            "stale_tables": stale_tables,
        }

        # -- Build section --
        build_section: dict[str, Any] = {
            "has_build": False,
            "run_id": None,
            "built_at": None,
            "duration_ms": None,
            "status": None,
        }
        try:
            run_dir = self._resolve_run_dir(None)
            if run_dir is not None:
                meta_path = run_dir / "run_meta.json"
                if meta_path.exists():
                    meta = json.loads(meta_path.read_text())
                    build_section["has_build"] = True
                    build_section["run_id"] = meta.get("run_id", run_dir.name)
                    build_section["built_at"] = meta.get("timestamp")
                    timings = meta.get("timings_ms")
                    if isinstance(timings, dict):
                        build_section["duration_ms"] = sum(timings.values())
                    # Determine build status from assertions
                    a_status = meta.get("assertions_status", "")
                    a_failed = meta.get("assertions_failed_count", 0)
                    if a_failed > 0 or a_status == "fail":
                        build_section["status"] = "fail"
                    else:
                        build_section["status"] = "ok"
        except Exception:
            pass

        # -- Verify section --
        verify_section: dict[str, Any] = {
            "status": "unknown",
            "checked_at": None,
        }
        try:
            if run_dir is not None:
                verify_path = run_dir / "verify_report.json"
                if verify_path.exists():
                    report = json.loads(verify_path.read_text())
                    verify_section["status"] = report.get("status", "unknown")
                    verify_section["checked_at"] = report.get("checked_at")
        except Exception:
            pass

        return {
            "project": project_section,
            "datasheets": datasheets_section,
            "build": build_section,
            "verify": verify_section,
        }

    # ------------------------------------------------------------------
    # Latest table output helper
    # ------------------------------------------------------------------

    def get_latest_table_output_name(self, run_id: str | None = None) -> dict[str, Any]:
        """Return the primary output table name for a run.

        Checks workbook.yaml for ``ui.primary_table`` hint first,
        then falls back to the first table output alphabetically.

        Args:
            run_id: Specific run. Defaults to latest.

        Returns:
            Dict with run_id, table_name, and download_url, or error.
        """
        run_dir = self._resolve_run_dir(run_id)
        if run_dir is None:
            return {"error": "No runs found"}

        resolved_run_id = run_dir.name
        outputs_dir = run_dir / "outputs"
        if not outputs_dir.exists():
            return {"error": "No outputs directory"}

        # List parquet files excluding internal artifacts
        parquet_files = sorted(
            f.stem for f in outputs_dir.iterdir()
            if f.suffix == ".parquet" and not f.stem.startswith("_")
        )
        if not parquet_files:
            return {"error": "No table outputs found"}

        # Check for primary_table hint in workbook.yaml
        ui_config = self._spec.get("ui", {})
        primary = ui_config.get("primary_table")
        if primary and primary in parquet_files:
            table_name = primary
        else:
            table_name = parquet_files[0]

        return {
            "run_id": resolved_run_id,
            "table_name": table_name,
            "download_url": f"/api/outputs/table/download?name={table_name}&run_id={resolved_run_id}",
        }

    # ------------------------------------------------------------------
    # Registry integration
    # ------------------------------------------------------------------

    def _get_registry(self):
        """Return the registry backend, or None if disabled."""
        try:
            from fin123.registry.backend import get_registry

            return get_registry(self.project_dir)
        except Exception:
            return None

    def get_registry_status(self) -> dict[str, Any]:
        """Return registry backend status."""
        config = load_project_config(self.project_dir)
        backend = config.get("registry_backend", "file")
        reg = self._get_registry()
        reachable = reg.ping() if reg is not None else False
        return {
            "backend": backend,
            "enabled": reg is not None,
            "reachable": reachable,
            "store_runs": config.get("registry_store_runs", False),
        }

    def registry_push_versions(
        self, version_ids: list[str] | None = None, force: bool = False
    ) -> dict[str, Any]:
        """Push local versions to the registry.

        Args:
            version_ids: Specific versions to push, or None for latest.
            force: Overwrite on hash conflict.

        Returns:
            Summary with per-version actions.
        """
        reg = self._get_registry()
        if reg is None:
            return {"error": "Registry not configured or not reachable"}

        from uuid import UUID

        import yaml

        from fin123.registry.backend import parse_version_ordinal
        from fin123.utils.hash import sha256_dict

        store = SnapshotStore(self.project_dir)
        index = store.load_index()
        all_versions = index.get("versions", [])

        if version_ids is None:
            if all_versions:
                version_ids = [all_versions[-1]["model_version_id"]]
            else:
                return {"error": "No local versions found"}

        model_id = UUID(self._model_id)
        reg.upsert_model(model_id)

        pushed = []
        errors = []
        for vid in version_ids:
            try:
                wb_path = store.snapshot_dir / vid / "workbook.yaml"
                if not wb_path.exists():
                    errors.append(f"{vid}: not found locally")
                    continue
                wb_yaml = wb_path.read_text()
                wb_spec = yaml.safe_load(wb_yaml) or {}
                wb_hash = sha256_dict(wb_spec)
                ordinal = parse_version_ordinal(vid)
                reg.put_model_version(
                    model_id=model_id,
                    model_version_id=vid,
                    version_ordinal=ordinal,
                    workbook_yaml=wb_yaml,
                    workbook_hash=wb_hash,
                    force=force,
                )
                pushed.append(vid)
            except Exception as exc:
                errors.append(f"{vid}: {exc}")

        return {"pushed": pushed, "errors": errors}

    def registry_pull_version(
        self, model_id_str: str, version_id: str
    ) -> dict[str, Any]:
        """Pull a version from the registry into local snapshots.

        Args:
            model_id_str: Model UUID string.
            version_id: Version to pull.

        Returns:
            Summary dict.
        """
        from uuid import UUID

        import yaml

        from fin123.utils.hash import sha256_dict

        reg = self._get_registry()
        if reg is None:
            return {"error": "Registry not configured or not reachable"}

        try:
            mid = UUID(model_id_str)
        except ValueError:
            return {"error": f"Invalid model_id: {model_id_str!r}"}

        try:
            ver = reg.get_model_version(mid, version_id)
        except (KeyError, FileNotFoundError) as exc:
            return {"error": str(exc)}

        workbook_yaml = ver.get("workbook_yaml")
        if not workbook_yaml:
            return {"error": "Registry returned empty workbook_yaml"}

        store = SnapshotStore(self.project_dir)
        version_dir = store.snapshot_dir / version_id
        version_dir.mkdir(parents=True, exist_ok=True)
        (version_dir / "workbook.yaml").write_text(workbook_yaml)

        # Update index
        wb_spec = yaml.safe_load(workbook_yaml) or {}
        content_hash = sha256_dict(wb_spec)
        index = store.load_index()
        existing = [
            v for v in index["versions"] if v["model_version_id"] == version_id
        ]
        if not existing:
            from datetime import datetime, timezone

            index["versions"].append({
                "model_version_id": version_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "hash": content_hash,
                "pinned": False,
            })
            from fin123.registry.backend import parse_version_ordinal

            index["versions"].sort(
                key=lambda v: parse_version_ordinal(v["model_version_id"])
            )
            store._write_index(index)

        return {"ok": True, "version": version_id}

    def _registry_push_version(
        self, version: str, workbook_yaml: str, workbook_hash: str
    ) -> None:
        """Push a model version to the registry (if enabled). Never raises."""
        try:
            from uuid import UUID

            registry = self._get_registry()
            if registry is None:
                return

            model_id = UUID(self._model_id)
            registry.upsert_model(model_id)

            from fin123.registry.backend import parse_version_ordinal

            ordinal = parse_version_ordinal(version)

            registry.put_model_version(
                model_id=model_id,
                model_version_id=version,
                version_ordinal=ordinal,
                workbook_yaml=workbook_yaml,
                workbook_hash=workbook_hash,
            )
        except Exception:
            import logging

            logging.getLogger(__name__).debug(
                "Registry push failed (non-fatal)", exc_info=True
            )

    def _registry_push_run(
        self,
        run_id: str,
        model_version_id: str,
        workbook_hash: str,
        run_meta: dict[str, Any],
    ) -> None:
        """Push build/run metadata to the registry (if enabled). Never raises."""
        try:
            from uuid import UUID

            config = load_project_config(self.project_dir)
            registry = self._get_registry()
            if registry is None:
                return

            model_id = UUID(self._model_id)

            # Push to fin123_builds if store_builds enabled
            if config.get("registry_store_builds", False):
                registry.put_build(
                    run_id=run_id,
                    model_id=model_id,
                    model_version_id=model_version_id,
                    workbook_hash=workbook_hash,
                    run_meta=run_meta,
                )

            # Backward compat: push to fin123_runs if store_runs enabled
            if config.get("registry_store_runs", False):
                registry.put_run(
                    run_id=run_id,
                    model_id=model_id,
                    model_version_id=model_version_id,
                    workbook_hash=workbook_hash,
                    run_meta=run_meta,
                )
        except Exception:
            import logging

            logging.getLogger(__name__).debug(
                "Registry run push failed (non-fatal)", exc_info=True
            )

    # ------------------------------------------------------------------
    # Event tail (canonical)
    # ------------------------------------------------------------------

    def tail_events(
        self,
        scope: str = "global",
        scope_id: str | None = None,
        n: int = 500,
    ) -> list[dict[str, Any]]:
        """Canonical event tail: return the last *n* events for a given scope.

        Args:
            scope: One of "global", "run", "sync", "import".
            scope_id: Required for "run" and "sync" scopes.
            n: Maximum events to return (capped at 2000).

        Returns:
            List of event dicts, most-recent-first.
        """
        from fin123.logging.sink import EventSink

        sink = EventSink(self.project_dir)
        n = min(n, 2000)

        if scope == "run":
            if not scope_id:
                return []
            return list(reversed(sink.read_run_log(scope_id)))[:n]
        elif scope == "sync":
            if not scope_id:
                return []
            return list(reversed(sink.read_sync_log(scope_id)))[:n]
        elif scope == "import":
            return sink.read_global(
                event_type=None, limit=n,
            )  # filter to import-related types client-side for flexibility
        else:
            # global
            return sink.read_global(limit=n)

    # ------------------------------------------------------------------
    # Run checks & verify
    # ------------------------------------------------------------------

    def get_build_checks(self, run_id: str) -> dict[str, Any]:
        """Return check results for a build: assertions, verify, timings, lookup violations.

        Reads from run_meta.json, verify_report.json, and per-run event log.

        Args:
            run_id: The run directory name.

        Returns:
            Dict with assertions, verify, timings, lookup_violations, and mode info.
        """
        run_dir = self.project_dir / "runs" / run_id
        meta_path = run_dir / "run_meta.json"
        verify_path = run_dir / "verify_report.json"

        result: dict[str, Any] = {
            "run_id": run_id,
            "exists": run_dir.exists(),
            "assertions": None,
            "verify": None,
            "timings_ms": None,
            "scenario_name": None,
            "overlay_hash": None,
            "lookup_violations": [],
        }

        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                result["assertions"] = {
                    "status": meta.get("assertions_status"),
                    "failed_count": meta.get("assertions_failed_count", 0),
                    "warn_count": meta.get("assertions_warn_count", 0),
                }
                result["timings_ms"] = meta.get("timings_ms")
                result["scenario_name"] = meta.get("scenario_name")
                result["overlay_hash"] = meta.get("overlay_hash")
            except (json.JSONDecodeError, OSError):
                pass

        if verify_path.exists():
            try:
                result["verify"] = json.loads(verify_path.read_text())
            except (json.JSONDecodeError, OSError):
                pass

        # Collect lookup violations from the per-run event log
        try:
            from fin123.logging.sink import EventSink

            sink = EventSink(self.project_dir)
            run_events = sink.read_run_log(run_id)
            violations = [
                e for e in run_events
                if e.get("event_type") == "lookup_violation"
            ]
            result["lookup_violations"] = violations
        except Exception:
            pass

        return result

    # Backward-compatible alias
    get_run_checks = get_build_checks

    def build_verify(self, run_id: str) -> dict[str, Any]:
        """Run verification on a completed build.

        Recomputes all hashes and compares against run_meta.json.
        Writes verify_report.json into the run directory.

        Args:
            run_id: The run directory name.

        Returns:
            Verification report dict.
        """
        from fin123.verify import verify_run

        return verify_run(self.project_dir, run_id)

    # Backward-compatible alias
    run_verify = build_verify

    # ------------------------------------------------------------------
    # Incidents
    # ------------------------------------------------------------------

    def get_incidents(self, run_id: str | None = None) -> dict[str, Any]:
        """Collect structured incidents from multiple sources.

        Args:
            run_id: Specific run to check. Defaults to latest run.

        Returns:
            Dict with run_id, total, counts, and incidents list.
        """
        if run_id is None:
            run_id = self._latest_run_id()

        incidents: list[dict[str, Any]] = []
        _next_id = 0

        def _make_id() -> str:
            nonlocal _next_id
            _next_id += 1
            return f"inc-{_next_id}"

        # 1. Build errors from run event log
        if run_id:
            try:
                from fin123.logging.sink import EventSink

                sink = EventSink(self.project_dir)
                run_events = sink.read_run_log(run_id)
                for evt in run_events:
                    if evt.get("event_type") == "run_failed":
                        incidents.append({
                            "id": _make_id(),
                            "category": "build_error",
                            "severity": "error",
                            "code": "build_failed",
                            "title": "Build failed",
                            "detail": evt.get("message", "Unknown build error"),
                            "source_run_id": run_id,
                        })
            except Exception:
                pass

        # 2. Verify failures from verify_report.json
        if run_id:
            try:
                store = RunStore(self.project_dir)
                run_dir = store.runs_dir / run_id
                verify_path = run_dir / "verify_report.json"
                if verify_path.exists():
                    report = json.loads(verify_path.read_text())
                    for failure in report.get("failures", []):
                        msg = failure if isinstance(failure, str) else str(failure)
                        code = "verify_hash_mismatch"
                        if "missing" in msg.lower():
                            code = "verify_missing_file"
                        elif "schema" in msg.lower():
                            code = "verify_schema_mismatch"
                        incidents.append({
                            "id": _make_id(),
                            "category": "verify_fail",
                            "severity": "error",
                            "code": code,
                            "title": "Verification failure",
                            "detail": msg,
                            "source_run_id": run_id,
                        })
            except Exception:
                pass

        # 3. Assertion failures from run_meta.json
        if run_id:
            try:
                store = RunStore(self.project_dir)
                run_dir = store.runs_dir / run_id
                meta_path = run_dir / "run_meta.json"
                if meta_path.exists():
                    meta = json.loads(meta_path.read_text())
                    status = meta.get("assertions_status", "")
                    failed = meta.get("assertions_failed_count", 0)
                    warn = meta.get("assertions_warn_count", 0)
                    if failed > 0:
                        incidents.append({
                            "id": _make_id(),
                            "category": "assertion_fail",
                            "severity": "error",
                            "code": "assertion_failed",
                            "title": f"{failed} assertion(s) failed",
                            "detail": f"Build {run_id}: {failed} assertion(s) failed, {warn} warning(s)",
                            "source_run_id": run_id,
                        })
                    elif warn > 0:
                        incidents.append({
                            "id": _make_id(),
                            "category": "assertion_fail",
                            "severity": "warning",
                            "code": "assertion_warning",
                            "title": f"{warn} assertion warning(s)",
                            "detail": f"Build {run_id}: {warn} assertion warning(s)",
                            "source_run_id": run_id,
                        })
            except Exception:
                pass

        # 4. Sync errors from latest sync_meta.json
        try:
            sync_history = self._load_sync_history()
            for tname, info in sync_history.items():
                status = info.get("status", "unknown")
                cname = info.get("connector_name")
                if status == "fail":
                    if cname:
                        incidents.append({
                            "id": _make_id(),
                            "category": "connector_error",
                            "severity": "error",
                            "code": "connector_failed",
                            "title": f"Connector failed: {cname}",
                            "detail": info.get("error_message", f"Connector '{cname}' sync failed"),
                            "source_run_id": run_id,
                            "suggested_action": f"Check connector '{cname}' configuration and dependencies",
                        })
                    else:
                        incidents.append({
                            "id": _make_id(),
                            "category": "sync_error",
                            "severity": "error",
                            "code": "sync_failed",
                            "title": f"Sync failed: {tname}",
                            "detail": info.get("error_message", f"Table '{tname}' sync failed"),
                            "source_run_id": run_id,
                            "suggested_action": f"Re-run sync for table '{tname}'",
                        })
                elif status == "skipped" and cname:
                    incidents.append({
                        "id": _make_id(),
                        "category": "connector_warning",
                        "severity": "warning",
                        "code": "connector_skipped",
                        "title": f"Connector skipped: {cname}",
                        "detail": info.get("error_message", f"Connector '{cname}' was skipped"),
                        "source_run_id": run_id,
                        "suggested_action": f"Install required dependencies for '{cname}'",
                    })
        except Exception:
            pass

        # Sort: errors first, then warnings, then info
        severity_order = {"error": 0, "warning": 1, "info": 2}
        incidents.sort(key=lambda i: severity_order.get(i.get("severity", "info"), 3))

        counts = {
            "error": sum(1 for i in incidents if i["severity"] == "error"),
            "warning": sum(1 for i in incidents if i["severity"] == "warning"),
            "info": sum(1 for i in incidents if i["severity"] == "info"),
        }

        return {
            "run_id": run_id,
            "total": len(incidents),
            "counts": counts,
            "incidents": incidents,
        }

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    def run_pipeline(self) -> dict[str, Any]:
        """Run the Sync → Build → Verify pipeline sequentially.

        Returns:
            Dict with steps, status, run_id, incidents, and optional error.
        """
        steps: list[dict[str, Any]] = []

        # Pre-check: dirty working copy
        if self._dirty:
            return {
                "steps": [],
                "status": "error",
                "run_id": None,
                "incidents": None,
                "error": "Working copy has uncommitted edits. Commit before running pipeline.",
            }

        # Step 1: Sync
        sync_needed = False
        try:
            datasheets = self.get_datasheets()
            for ds in datasheets:
                staleness = ds.get("staleness", "unknown")
                if staleness in ("stale", "unknown"):
                    sync_needed = True
                    break
        except Exception:
            pass

        if sync_needed:
            try:
                sync_result = self.run_sync()
                has_errors = bool(sync_result.get("errors"))
                steps.append({
                    "step": "sync",
                    "status": "error" if has_errors else "ok",
                    "detail": sync_result,
                })
                if has_errors:
                    return {
                        "steps": steps,
                        "status": "error",
                        "run_id": None,
                        "incidents": None,
                        "error": "Sync failed: " + ", ".join(sync_result["errors"]),
                    }
            except Exception as exc:
                steps.append({"step": "sync", "status": "error", "detail": str(exc)})
                return {
                    "steps": steps,
                    "status": "error",
                    "run_id": None,
                    "incidents": None,
                    "error": f"Sync failed: {exc}",
                }
        else:
            steps.append({"step": "sync", "status": "skipped", "detail": "All datasheets fresh"})

        # Step 2: Build
        try:
            build_result = self.build_workbook()
            if "error" in build_result:
                steps.append({"step": "build", "status": "error", "detail": build_result["error"]})
                return {
                    "steps": steps,
                    "status": "error",
                    "run_id": None,
                    "incidents": None,
                    "error": build_result["error"],
                }
            run_id = build_result["run_id"]
            steps.append({"step": "build", "status": "ok", "detail": build_result})
        except Exception as exc:
            steps.append({"step": "build", "status": "error", "detail": str(exc)})
            return {
                "steps": steps,
                "status": "error",
                "run_id": None,
                "incidents": None,
                "error": f"Build failed: {exc}",
            }

        # Step 3: Verify
        try:
            verify_result = self.build_verify(run_id)
            v_status = verify_result.get("status", "unknown")
            steps.append({"step": "verify", "status": v_status, "detail": verify_result})
        except Exception as exc:
            steps.append({"step": "verify", "status": "error", "detail": str(exc)})

        # Collect incidents
        incidents_data = self.get_incidents(run_id)

        # Overall status
        has_error = any(s["status"] == "error" for s in steps)
        has_fail = any(s["status"] == "fail" for s in steps)
        overall = "error" if (has_error or has_fail) else "ok"

        return {
            "steps": steps,
            "status": overall,
            "run_id": run_id,
            "incidents": incidents_data,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_sync_history(self) -> dict[str, dict[str, Any]]:
        """Load the latest sync result per table from sync_runs/.

        Returns:
            Dict of table_name -> latest sync info for that table.
        """
        sync_dir = self.project_dir / "sync_runs"
        if not sync_dir.exists():
            return {}

        # Find all sync_meta.json files, sorted by directory name (timestamp-based)
        sync_dirs = sorted(
            (d for d in sync_dir.iterdir() if d.is_dir()),
            key=lambda d: d.name,
        )

        latest_per_table: dict[str, dict[str, Any]] = {}
        for sdir in sync_dirs:
            meta_path = sdir / "sync_meta.json"
            if not meta_path.exists():
                continue
            try:
                meta = json.loads(meta_path.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            sync_id = meta.get("sync_id", sdir.name)
            timestamp = meta.get("timestamp")

            for table_result in meta.get("tables", []):
                tname = (
                    table_result.get("table_name")
                    or table_result.get("connector_name")
                )
                if not tname:
                    continue
                latest_per_table[tname] = {
                    "sync_id": sync_id,
                    "timestamp": timestamp,
                    "status": table_result.get("status", "unknown"),
                    "rowcount": table_result.get("rowcount", 0),
                    "warnings": table_result.get("warnings", []),
                    "error_message": table_result.get("error_message", ""),
                    "connector_name": table_result.get("connector_name"),
                }

        return latest_per_table

    @staticmethod
    def _classify_staleness(
        *,
        last_sync: dict[str, Any] | None,
        cache_exists: bool,
        cache_mtime: float | None,
        refresh: str,
        tspec: dict[str, Any],
    ) -> str:
        """Classify datasheet staleness.

        Returns one of: "fresh", "stale", "unknown", "fail".
        """
        if last_sync is None:
            return "unknown"
        if last_sync["status"] == "fail":
            return "fail"
        if not cache_exists:
            return "unknown"

        # Check TTL-based staleness
        ttl_hours = tspec.get("ttl_hours")
        if refresh == "always":
            ttl_hours = 0.0
        elif isinstance(refresh, str) and refresh != "manual":
            # Try to parse as number (legacy format)
            try:
                ttl_hours = float(refresh)
            except ValueError:
                pass

        if ttl_hours is not None and cache_mtime is not None:
            age_hours = (time.time() - cache_mtime) / 3600.0
            if age_hours > ttl_hours:
                return "stale"

        return "fresh"

    def _resolve_run_dir(self, run_id: str | None) -> Path | None:
        """Resolve a run directory by id, defaulting to latest."""
        runs_dir = self.project_dir / "runs"
        if run_id:
            d = runs_dir / run_id
            return d if d.exists() else None

        # Latest
        store = RunStore(self.project_dir)
        runs = store.list_runs()
        if not runs:
            return None
        return runs_dir / runs[-1]["run_id"]


# ---------------------------------------------------------------------------
# Standalone helpers (not bound to the ProjectService singleton)
# ---------------------------------------------------------------------------

_SAFE_PROJECT_NAME = re.compile(r"^[a-z0-9_-]+$")


def import_xlsx_upload(
    file_bytes: bytes,
    filename: str,
    project_name: str | None = None,
    base_dir: Path | None = None,
) -> dict[str, Any]:
    """Import an uploaded XLSX file into a *new* fin123 project.

    This is a module-level function (not a ProjectService method) because it
    creates a brand-new project directory rather than mutating the one the
    current session is bound to.

    Args:
        file_bytes: Raw bytes of the uploaded .xlsx file.
        filename: Original filename (used to derive project_name if omitted).
        project_name: Explicit project slug.  Derived from *filename* if None.
        base_dir: Parent directory for new projects.  Defaults to
            ``~/Documents/fin123_projects`` (or the ``import_projects_base``
            config value).

    Returns:
        Dict with ``ok``, ``project_dir``, ``project_name``, ``report``,
        and ``snapshot_version``.
    """
    import tempfile

    from fin123.xlsx_import import import_xlsx

    # Derive project_name from filename stem if not provided
    if not project_name:
        stem = Path(filename).stem
        project_name = re.sub(r"[^a-z0-9_-]", "_", stem.lower().replace(" ", "_"))
        project_name = re.sub(r"_+", "_", project_name).strip("_")

    if not project_name:
        raise ValueError("Could not derive a valid project name from filename")

    if not _SAFE_PROJECT_NAME.match(project_name):
        raise ValueError(
            f"Invalid project name {project_name!r}: "
            "only lowercase letters, digits, hyphens, and underscores allowed"
        )

    # Resolve base directory
    if base_dir is None:
        try:
            cfg = load_project_config(Path.cwd())
            configured = cfg.get("import_projects_base")
            if configured:
                base_dir = Path(configured)
        except Exception:
            pass
    if base_dir is None:
        base_dir = Path.home() / "Documents" / "fin123_projects"

    base_dir.mkdir(parents=True, exist_ok=True)
    project_dir = base_dir / project_name

    if (project_dir / "workbook.yaml").exists():
        raise ValueError(
            f"Project {project_name!r} already exists at {project_dir}"
        )

    # Write to temp file and import
    tmp_fd = None
    tmp_path = None
    try:
        tmp_fd = tempfile.NamedTemporaryFile(
            suffix=".xlsx", delete=False
        )
        tmp_path = Path(tmp_fd.name)
        tmp_fd.write(file_bytes)
        tmp_fd.close()

        report = import_xlsx(tmp_path, project_dir)
    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()

    # Read snapshot version from created project
    snapshot_version = None
    try:
        store = SnapshotStore(project_dir)
        versions = store.list_versions()
        if versions:
            snapshot_version = versions[-1]
    except Exception:
        pass

    return {
        "ok": True,
        "project_dir": str(project_dir),
        "project_name": project_name,
        "report": report,
        "snapshot_version": snapshot_version,
    }
