"""On-demand memoized cell formula evaluator with cross-sheet support.

Evaluates sheet cell formulas lazily: a cell is computed only when
referenced, and the result is cached for the duration of one evaluation
pass.  Detects cycles across cells (including cross-sheet) and raises
a clear error showing the cycle path.

Also supports named ranges — rectangular regions that expand to flat
lists of values for use in aggregate functions (SUM, AVERAGE, etc.).
"""

from __future__ import annotations

import re
from typing import Any

from fin123.formulas.errors import FormulaError, FormulaRefError
from fin123.formulas.evaluator import CellResolver, evaluate_formula
from fin123.formulas.parser import extract_all_refs, parse_formula
from fin123.ui.service import col_letter_to_index, index_to_col_letter, parse_addr, make_addr


# ---------------------------------------------------------------------------
# Named-range helpers
# ---------------------------------------------------------------------------

_ADDR_RE = re.compile(r"^([A-Z]{1,3})(\d+)$")


def _expand_rect(start: str, end: str) -> list[str]:
    """Expand a rectangular range (e.g. A1:C3) into a flat list of addresses (row-major).

    Args:
        start: Top-left address, e.g. "A1".
        end: Bottom-right address, e.g. "C3".

    Returns:
        Flat list of cell addresses in row-major order.
    """
    r0, c0 = parse_addr(start)
    r1, c1 = parse_addr(end)
    # Normalise so r0 <= r1, c0 <= c1
    if r0 > r1:
        r0, r1 = r1, r0
    if c0 > c1:
        c0, c1 = c1, c0
    addrs: list[str] = []
    for r in range(r0, r1 + 1):
        for c in range(c0, c1 + 1):
            addrs.append(make_addr(r, c))
    return addrs


# ---------------------------------------------------------------------------
# CellGraph
# ---------------------------------------------------------------------------


class CellCycleError(FormulaError):
    """Raised when a cycle is detected during cell evaluation.

    Attributes:
        cycle_path: List of (sheet, addr) tuples showing the cycle.
    """

    def __init__(self, cycle_path: list[tuple[str, str]]) -> None:
        self.cycle_path = cycle_path
        parts = [f"{s}!{a}" for s, a in cycle_path]
        super().__init__(f"Circular cell reference: {' -> '.join(parts)}")


class CellGraph:
    """On-demand memoized evaluator for sheet cell formulas.

    Usage::

        cg = CellGraph(sheets_data, names)
        value = cg.evaluate_cell("Sheet1", "A1")

        # Or evaluate all formulas in all sheets:
        results = cg.evaluate_all()

    Parameters
    ----------
    sheets_data : dict[str, dict[str, Any]]
        Mapping of sheet_name -> cells dict.  Each cells dict maps
        cell addresses (e.g. "A1") to cell dicts with either
        ``{"formula": "=..."}`` or ``{"value": ...}``.
    names : dict[str, dict[str, str]] | None
        Named ranges.  Each entry maps a name to a dict with keys
        ``sheet``, ``start``, ``end`` (e.g. ``{"sheet": "Data", "start": "B2", "end": "B10"}``).
    """

    def __init__(
        self,
        sheets_data: dict[str, dict[str, Any]],
        names: dict[str, dict[str, str]] | None = None,
        params: dict[str, Any] | None = None,
    ) -> None:
        self._sheets = sheets_data  # sheet_name -> {addr: cell_dict}
        self._names = names or {}
        self._params = params or {}
        self._cache: dict[tuple[str, str], Any] = {}
        self._in_progress: set[tuple[str, str]] = set()
        self._eval_stack: list[tuple[str, str]] = []
        self._errors: dict[tuple[str, str], str] = {}

    # ------------------------------------------------------------------
    # CellResolver protocol implementation
    # ------------------------------------------------------------------

    def resolve_cell(self, sheet: str, addr: str) -> Any:
        """Resolve a cell value, triggering recursive evaluation if needed."""
        return self.evaluate_cell(sheet, addr.upper())

    def resolve_range(self, name: str) -> list[Any]:
        """Resolve a named range to a flat list of values (row-major)."""
        if name not in self._names:
            raise FormulaRefError(name, available=sorted(self._names.keys()))
        defn = self._names[name]
        sheet = defn["sheet"]
        addrs = _expand_rect(defn["start"], defn["end"])
        values: list[Any] = []
        for addr in addrs:
            val = self.evaluate_cell(sheet, addr)
            if val is not None and val != "":
                values.append(val)
        return values

    def has_named_range(self, name: str) -> bool:
        """Check if a name is a defined named range."""
        return name in self._names

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------

    def evaluate_cell(self, sheet: str, addr: str) -> Any:
        """Evaluate a single cell, with memoization and cycle detection.

        Args:
            sheet: Sheet name.
            addr: Cell address (e.g. "A1").

        Returns:
            The computed cell value.

        Raises:
            CellCycleError: If a circular reference is detected.
            ValueError: If the sheet doesn't exist.
        """
        addr = addr.upper()
        key = (sheet, addr)

        # Already computed?
        if key in self._cache:
            return self._cache[key]

        # Cycle detection
        if key in self._in_progress:
            # Build cycle path from the stack
            cycle_start = self._eval_stack.index(key)
            cycle_path = self._eval_stack[cycle_start:] + [key]
            raise CellCycleError(cycle_path)

        # Get the cell data
        if sheet not in self._sheets:
            raise ValueError(f"Sheet {sheet!r} not found")
        cells = self._sheets[sheet]
        cell = cells.get(addr)

        if cell is None:
            # Empty cell → None
            self._cache[key] = None
            return None

        raw_formula = cell.get("formula")
        if raw_formula and isinstance(raw_formula, str) and raw_formula.startswith("="):
            # It's a formula — parse and evaluate
            self._in_progress.add(key)
            self._eval_stack.append(key)
            try:
                tree = parse_formula(raw_formula)
                # Context: empty dict — cell formulas resolve everything
                # through the resolver (cross-sheet refs and named ranges).
                # Scalar context is empty because cells don't have scalar params.
                # current_sheet enables bare A1 cell refs (e.g. F2) to resolve
                # within the sheet that owns this formula.
                result = evaluate_formula(tree, self._params, resolver=self, current_sheet=sheet)
                self._cache[key] = result
                return result
            except CellCycleError:
                raise
            except Exception as exc:
                # Store error, cache the error message for display
                self._errors[key] = str(exc)
                self._cache[key] = None
                return None
            finally:
                self._in_progress.discard(key)
                if self._eval_stack and self._eval_stack[-1] == key:
                    self._eval_stack.pop()
        else:
            # Literal value
            value = cell.get("value", cell.get("formula", ""))
            self._cache[key] = value
            return value

    def evaluate_all(self) -> dict[str, dict[str, Any]]:
        """Evaluate all cells across all sheets.

        Returns:
            Dict of sheet_name -> {addr: computed_value} for non-empty cells.
        """
        results: dict[str, dict[str, Any]] = {}
        for sheet_name, cells in self._sheets.items():
            sheet_results: dict[str, Any] = {}
            for addr in cells:
                val = self.evaluate_cell(sheet_name, addr)
                if val is not None:
                    sheet_results[addr] = val
            results[sheet_name] = sheet_results
        return results

    def get_display_value(self, sheet: str, addr: str) -> str:
        """Get a display-friendly string for a cell value.

        Evaluates the cell if not yet cached, and formats the result.
        """
        try:
            val = self.evaluate_cell(sheet, addr)
        except CellCycleError:
            return "#CIRC!"
        except Exception:
            return "#ERR!"

        if val is None:
            return ""
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        if isinstance(val, float):
            # Format floats cleanly
            if val == int(val):
                return str(int(val))
            return f"{val:.10g}"
        return str(val)

    def get_errors(self) -> dict[tuple[str, str], str]:
        """Return all evaluation errors collected during this pass.

        Returns:
            Dict of (sheet, addr) -> error message.
        """
        return dict(self._errors)

    def invalidate(self) -> None:
        """Clear all cached values and errors.

        Call this when cells have been edited and need re-evaluation.
        """
        self._cache.clear()
        self._in_progress.clear()
        self._eval_stack.clear()
        self._errors.clear()


# ---------------------------------------------------------------------------
# PARAM() binding scanner
# ---------------------------------------------------------------------------

_PARAM_RE = re.compile(r'^=PARAM\(\s*"([^"]+)"\s*\)$', re.IGNORECASE)


def scan_param_bindings(
    sheets: list[dict[str, Any]],
) -> tuple[dict[str, tuple[str, str]], list[str]]:
    """Scan all sheet cells for =PARAM("name") formulas.

    Returns:
        A tuple of (bindings, errors).
        bindings: dict mapping param_name -> (sheet_name, addr).
        errors: list of error messages (e.g. duplicate bindings).
    """
    bindings: dict[str, tuple[str, str]] = {}
    errors: list[str] = []

    for sheet in sheets:
        sheet_name = sheet["name"]
        cells = sheet.get("cells", {})
        for addr, cell in cells.items():
            formula = cell.get("formula", "")
            if not isinstance(formula, str):
                continue
            m = _PARAM_RE.match(formula)
            if m:
                param_name = m.group(1)
                if param_name in bindings:
                    prev_sheet, prev_addr = bindings[param_name]
                    errors.append(
                        f"Duplicate PARAM binding for {param_name!r}: "
                        f"already bound at {prev_sheet}!{prev_addr}, "
                        f"also found at {sheet_name}!{addr}"
                    )
                else:
                    bindings[param_name] = (sheet_name, addr)

    return bindings, errors
