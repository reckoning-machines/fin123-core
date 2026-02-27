"""Tests for new formula functions: logical, error, date, lookup, finance."""

from __future__ import annotations

import datetime
from typing import Any

import polars as pl
import pytest

from fin123.formulas import (
    ENGINE_ERRORS,
    FormulaFunctionError,
    evaluate_formula,
    parse_formula,
)


# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────


def _eval(formula: str, ctx: dict | None = None, tc: dict | None = None) -> Any:
    """Parse and evaluate a formula string."""
    tree = parse_formula(formula)
    return evaluate_formula(tree, ctx or {}, tc)


# ────────────────────────────────────────────────────────────────
# Logical: AND, OR, NOT
# ────────────────────────────────────────────────────────────────


class TestLogical:
    def test_and_true(self) -> None:
        assert _eval("=AND(TRUE, TRUE)") is True

    def test_and_false(self) -> None:
        assert _eval("=AND(TRUE, FALSE)") is False

    def test_and_multiple(self) -> None:
        assert _eval("=AND(TRUE, TRUE, TRUE)") is True
        assert _eval("=AND(TRUE, TRUE, FALSE)") is False

    def test_or_true(self) -> None:
        assert _eval("=OR(FALSE, TRUE)") is True

    def test_or_false(self) -> None:
        assert _eval("=OR(FALSE, FALSE)") is False

    def test_or_multiple(self) -> None:
        assert _eval("=OR(FALSE, FALSE, TRUE)") is True

    def test_not_true(self) -> None:
        assert _eval("=NOT(TRUE)") is False

    def test_not_false(self) -> None:
        assert _eval("=NOT(FALSE)") is True

    def test_and_with_expression(self) -> None:
        assert _eval("=AND(1 > 0, 2 > 1)") is True
        assert _eval("=AND(1 > 0, 2 < 1)") is False

    def test_not_bad_arity(self) -> None:
        with pytest.raises(FormulaFunctionError, match="NOT"):
            _eval("=NOT(TRUE, FALSE)")


# ────────────────────────────────────────────────────────────────
# Error: ISERROR
# ────────────────────────────────────────────────────────────────


class TestIsError:
    def test_div_zero(self) -> None:
        assert _eval("=ISERROR(1/0)") is True

    def test_no_error(self) -> None:
        assert _eval("=ISERROR(42)") is False

    def test_missing_ref(self) -> None:
        assert _eval("=ISERROR(missing_name)") is True

    def test_valid_ref(self) -> None:
        assert _eval("=ISERROR(x)", {"x": 10}) is False

    def test_iserror_bad_arity(self) -> None:
        with pytest.raises(FormulaFunctionError, match="ISERROR"):
            _eval("=ISERROR(1, 2)")

    def test_nested_in_if(self) -> None:
        result = _eval('=IF(ISERROR(1/0), "err", "ok")')
        assert result == "err"


# ────────────────────────────────────────────────────────────────
# Date: DATE, YEAR, MONTH, DAY, EOMONTH
# ────────────────────────────────────────────────────────────────


class TestDate:
    def test_date_construct(self) -> None:
        result = _eval("=DATE(2024, 3, 15)")
        assert result == datetime.date(2024, 3, 15)

    def test_year(self) -> None:
        assert _eval("=YEAR(DATE(2024, 3, 15))") == 2024

    def test_month(self) -> None:
        assert _eval("=MONTH(DATE(2024, 3, 15))") == 3

    def test_day(self) -> None:
        assert _eval("=DAY(DATE(2024, 3, 15))") == 15

    def test_eomonth_same_month(self) -> None:
        result = _eval("=EOMONTH(DATE(2024, 1, 15), 0)")
        assert result == datetime.date(2024, 1, 31)

    def test_eomonth_next_month(self) -> None:
        result = _eval("=EOMONTH(DATE(2024, 1, 15), 1)")
        assert result == datetime.date(2024, 2, 29)  # 2024 is a leap year

    def test_eomonth_prev_month(self) -> None:
        result = _eval("=EOMONTH(DATE(2024, 3, 15), -1)")
        assert result == datetime.date(2024, 2, 29)

    def test_eomonth_year_boundary(self) -> None:
        result = _eval("=EOMONTH(DATE(2024, 11, 1), 2)")
        assert result == datetime.date(2025, 1, 31)

    def test_date_bad_arity(self) -> None:
        with pytest.raises(FormulaFunctionError, match="DATE"):
            _eval("=DATE(2024, 3)")

    def test_year_from_string(self) -> None:
        """YEAR can accept a date string via scalar context."""
        tree = parse_formula("=YEAR(d)")
        result = evaluate_formula(tree, {"d": "2024-06-15"})
        assert result == 2024

    def test_date_arithmetic_via_eomonth(self) -> None:
        """EOMONTH jumping 12 months forward."""
        result = _eval("=EOMONTH(DATE(2023, 2, 28), 12)")
        assert result == datetime.date(2024, 2, 29)


# ────────────────────────────────────────────────────────────────
# Lookup: MATCH, INDEX, XLOOKUP
# ────────────────────────────────────────────────────────────────


class TestLookup:
    @pytest.fixture
    def table_cache(self) -> dict[str, pl.DataFrame]:
        return {
            "prices": pl.DataFrame({
                "ticker": ["AAPL", "GOOG", "MSFT"],
                "price": [150.0, 2800.0, 300.0],
            })
        }

    def test_match_found(self, table_cache: dict) -> None:
        result = _eval('=MATCH("GOOG", "prices", "ticker")', tc=table_cache)
        assert result == 2

    def test_match_first(self, table_cache: dict) -> None:
        result = _eval('=MATCH("AAPL", "prices", "ticker")', tc=table_cache)
        assert result == 1

    def test_match_not_found(self, table_cache: dict) -> None:
        with pytest.raises(FormulaFunctionError, match="not found"):
            _eval('=MATCH("TSLA", "prices", "ticker")', tc=table_cache)

    def test_index_basic(self, table_cache: dict) -> None:
        result = _eval('=INDEX("prices", "price", 2)', tc=table_cache)
        assert result == 2800.0

    def test_index_out_of_range(self, table_cache: dict) -> None:
        with pytest.raises(FormulaFunctionError, match="out of range"):
            _eval('=INDEX("prices", "price", 5)', tc=table_cache)

    def test_xlookup_basic(self, table_cache: dict) -> None:
        result = _eval('=XLOOKUP("AAPL", "prices", "ticker", "price")', tc=table_cache)
        assert result == 150.0

    def test_xlookup_not_found_with_default(self, table_cache: dict) -> None:
        result = _eval('=XLOOKUP("TSLA", "prices", "ticker", "price", -1)', tc=table_cache)
        assert result == -1

    def test_xlookup_not_found_no_default(self, table_cache: dict) -> None:
        with pytest.raises(FormulaFunctionError, match="XLOOKUP"):
            _eval('=XLOOKUP("TSLA", "prices", "ticker", "price")', tc=table_cache)

    def test_match_index_combo(self, table_cache: dict) -> None:
        """Use MATCH result in INDEX to replicate VLOOKUP."""
        row = _eval('=MATCH("GOOG", "prices", "ticker")', tc=table_cache)
        assert row == 2
        result = _eval('=INDEX("prices", "price", 2)', tc=table_cache)
        assert result == 2800.0


# ────────────────────────────────────────────────────────────────
# Finance: NPV, IRR, XNPV, XIRR
# ────────────────────────────────────────────────────────────────


class TestFinance:
    def test_npv_basic(self) -> None:
        result = _eval("=NPV(0.1, 100, 100, 100)")
        assert result == pytest.approx(248.685, rel=1e-3)

    def test_npv_single_cf(self) -> None:
        result = _eval("=NPV(0.1, 110)")
        assert result == pytest.approx(100.0, rel=1e-3)

    def test_irr_basic(self) -> None:
        result = _eval("=IRR(-1000, 400, 400, 400)")
        assert result == pytest.approx(0.09701, rel=1e-3)

    def test_irr_exact(self) -> None:
        """IRR of [-100, 110] at 10% exactly."""
        result = _eval("=IRR(-100, 110)")
        assert result == pytest.approx(0.10, rel=1e-3)

    def test_npv_bad_arity(self) -> None:
        with pytest.raises(FormulaFunctionError, match="NPV"):
            _eval("=NPV(0.1)")

    def test_irr_bad_arity(self) -> None:
        with pytest.raises(FormulaFunctionError, match="IRR"):
            _eval("=IRR(-100)")

    def test_xnpv(self) -> None:
        tc = {
            "cf": pl.DataFrame({
                "date": ["2024-01-01", "2024-07-01", "2025-01-01"],
                "amount": [-1000.0, 500.0, 600.0],
            })
        }
        result = _eval('=XNPV(0.1, "cf", "date", "amount")', tc=tc)
        # First cashflow at t=0 (no discounting), subsequent discounted by Actual/365
        assert result == pytest.approx(22.11, rel=1e-2)

    def test_xirr(self) -> None:
        tc = {
            "cf": pl.DataFrame({
                "date": ["2024-01-01", "2025-01-01"],
                "amount": [-1000.0, 1100.0],
            })
        }
        result = _eval('=XIRR("cf", "date", "amount")', tc=tc)
        # ~10% annual return (Actual/365)
        assert result == pytest.approx(0.10, rel=1e-2)

    def test_xnpv_bad_arity(self) -> None:
        with pytest.raises(FormulaFunctionError, match="XNPV"):
            _eval('=XNPV(0.1, "cf", "date")')

    def test_xirr_bad_arity(self) -> None:
        with pytest.raises(FormulaFunctionError, match="XIRR"):
            _eval('=XIRR("cf", "date")')


# ────────────────────────────────────────────────────────────────
# CellGraph display value for dates
# ────────────────────────────────────────────────────────────────


class TestCellGraphDate:
    def test_date_display(self) -> None:
        from fin123.cell_graph import CellGraph

        sheets = {
            "Sheet1": {
                "A1": {"formula": "=DATE(2024, 3, 15)"},
            }
        }
        cg = CellGraph(sheets)
        assert cg.get_display_value("Sheet1", "A1") == "2024-03-15"


class TestCellGraphErrorDisplay:
    """Error rendering policy: each error type maps to a canonical display code."""

    def _display(self, formula: str) -> str:
        from fin123.cell_graph import CellGraph

        sheets = {"S": {"A1": {"formula": formula}}}
        cg = CellGraph(sheets)
        return cg.get_display_value("S", "A1")

    def test_circ(self) -> None:
        from fin123.cell_graph import CellGraph

        sheets = {"S": {"A1": {"formula": "=S!A1"}}}
        cg = CellGraph(sheets)
        assert cg.get_display_value("S", "A1") == "#CIRC!"

    def test_name_unknown_function(self) -> None:
        assert self._display("=BOGUS(1)") == "#NAME?"

    def test_div_zero(self) -> None:
        assert self._display("=1/0") == "#DIV/0!"

    def test_ref_missing(self) -> None:
        assert self._display("=no_such_ref") == "#REF!"

    def test_generic_err(self) -> None:
        # A formula that produces an error not matching any specific rule
        # triggers the #ERR! fallback.  Force a TypeError via None arithmetic.
        from fin123.cell_graph import CellGraph

        sheets = {
            "S": {
                "A1": {"value": None},
                "B1": {"formula": "=S!A1 + 1"},
            }
        }
        cg = CellGraph(sheets)
        # The stored error for None + 1 is a TypeError which doesn't match
        # any specific pattern → #ERR!
        assert cg.get_display_value("S", "B1") == "#ERR!"

    def test_unmapped_error_falls_back_to_err(self) -> None:
        """Guard: any error message not matching a known rule returns #ERR!.

        Injects a synthetic error directly into the CellGraph error store
        so this test does not depend on any specific formula error format.
        """
        from fin123.cell_graph import CellGraph

        sheets = {"S": {"A1": {"formula": "=1"}}}
        cg = CellGraph(sheets)
        # Evaluate first so the cell is cached.
        cg.evaluate_cell("S", "A1")
        # Overwrite: simulate an error with a message that matches NO rule.
        cg._cache[("S", "A1")] = None
        cg._errors[("S", "A1")] = "some totally unknown failure mode"
        assert cg.get_display_value("S", "A1") == "#ERR!"

    def test_classify_error_message_standalone(self) -> None:
        """The module-level classifier is importable and consistent."""
        from fin123.cell_graph import classify_error_message

        assert classify_error_message("Unknown function: 'BOGUS'") == "#NAME?"
        assert classify_error_message("Unknown reference: 'x'") == "#REF!"
        assert classify_error_message("Division by zero in formula") == "#DIV/0!"
        assert classify_error_message("IRR: did not converge") == "#NUM!"
        assert classify_error_message("completely novel error") == "#ERR!"


# ────────────────────────────────────────────────────────────────
# Phase 1A: Error semantics — ISERROR / IFERROR alignment
# ────────────────────────────────────────────────────────────────


class TestErrorSemanticsAlignment:
    """ISERROR and IFERROR must catch the same ENGINE_ERRORS tuple."""

    def test_engine_errors_contains_type_error(self) -> None:
        assert TypeError in ENGINE_ERRORS

    def test_iferror_catches_type_error(self) -> None:
        """IFERROR must catch TypeError (e.g. None + 1)."""
        result = _eval('=IFERROR(x + 1, "fallback")', {"x": None})
        assert result == "fallback"

    def test_iserror_catches_type_error(self) -> None:
        result = _eval("=ISERROR(x + 1)", {"x": None})
        assert result is True

    def test_iserror_div_zero(self) -> None:
        assert _eval("=ISERROR(1/0)") is True

    def test_iferror_div_zero(self) -> None:
        assert _eval("=IFERROR(1/0, -1)") == -1

    def test_iserror_missing_ref(self) -> None:
        assert _eval("=ISERROR(no_such_name)") is True

    def test_iferror_missing_ref(self) -> None:
        assert _eval('=IFERROR(no_such_name, "default")') == "default"

    def test_iserror_failed_irr(self) -> None:
        """IRR non-convergence raises FormulaFunctionError — caught by ISERROR."""
        # All-positive cashflows have no IRR
        assert _eval("=ISERROR(IRR(100, 100))") is True

    def test_iferror_failed_irr(self) -> None:
        assert _eval("=IFERROR(IRR(100, 100), -999)") == -999


# ────────────────────────────────────────────────────────────────
# Phase 1B: Lookup determinism — duplicates return first match
# ────────────────────────────────────────────────────────────────


class TestLookupDuplicates:
    """When a lookup column contains duplicates, first-row-wins."""

    @pytest.fixture
    def dup_cache(self) -> dict[str, pl.DataFrame]:
        return {
            "data": pl.DataFrame({
                "key": ["A", "B", "A", "C"],
                "val": [10, 20, 30, 40],
            })
        }

    def test_match_returns_first_dup(self, dup_cache: dict) -> None:
        result = _eval('=MATCH("A", "data", "key")', tc=dup_cache)
        assert result == 1  # first occurrence (1-based)

    def test_xlookup_returns_first_dup(self, dup_cache: dict) -> None:
        result = _eval('=XLOOKUP("A", "data", "key", "val")', tc=dup_cache)
        assert result == 10  # value from first match

    def test_index_zero_raises(self, dup_cache: dict) -> None:
        """INDEX with row_num=0 is out of range (1-based)."""
        with pytest.raises(FormulaFunctionError, match="out of range"):
            _eval('=INDEX("data", "val", 0)', tc=dup_cache)

    def test_index_negative_raises(self, dup_cache: dict) -> None:
        with pytest.raises(FormulaFunctionError, match="out of range"):
            _eval('=INDEX("data", "val", -1)', tc=dup_cache)


# ────────────────────────────────────────────────────────────────
# Phase 1C: Finance convergence edge cases
# ────────────────────────────────────────────────────────────────


class TestFinanceConvergence:
    def test_irr_no_sign_change(self) -> None:
        """All-positive cashflows — no IRR exists."""
        with pytest.raises(FormulaFunctionError, match="did not converge"):
            _eval("=IRR(100, 200, 300)")

    def test_irr_all_negative(self) -> None:
        """All-negative cashflows — no IRR exists."""
        with pytest.raises(FormulaFunctionError, match="did not converge"):
            _eval("=IRR(-100, -200, -300)")

    def test_npv_zero_rate(self) -> None:
        """NPV at 0% is just the sum of cashflows."""
        result = _eval("=NPV(0, 100, 200, 300)")
        assert result == pytest.approx(600.0)

    def test_irr_large_cashflows(self) -> None:
        """IRR with large magnitudes should still converge."""
        result = _eval("=IRR(-1000000, 500000, 500000, 200000)")
        assert isinstance(result, float)
        assert -1 < result < 10


# ────────────────────────────────────────────────────────────────
# Phase 1D: View transform immutability
# ────────────────────────────────────────────────────────────────


class TestViewTransformImmutability:
    def test_original_df_unchanged(self) -> None:
        """apply_view_transforms must not mutate the source DataFrame."""
        from fin123.ui.view_transforms import (
            NumericFilter,
            SortSpec,
            apply_view_transforms,
        )

        original = pl.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        })
        original_copy = original.clone()

        _ = apply_view_transforms(
            original,
            sorts=[SortSpec(column="age", descending=True)],
            filters=[NumericFilter(column="age", op=">", value=26)],
        )

        # Original must be identical to the clone
        assert original.equals(original_copy)

    def test_no_internal_column_leaked(self) -> None:
        from fin123.ui.view_transforms import SortSpec, apply_view_transforms

        df = pl.DataFrame({"x": [3, 1, 2]})
        result = apply_view_transforms(df, sorts=[SortSpec(column="x")])
        assert "__view_row_idx__" not in result.columns


# ────────────────────────────────────────────────────────────────
# Phase 2: Unsupported functions remain unsupported
# ────────────────────────────────────────────────────────────────


class TestUnsupportedFunctions:
    """Functions that must NOT be implemented in the engine."""

    @pytest.mark.parametrize("func", [
        'OFFSET("sheet", 1, 0)',
        'INDIRECT("A1")',
        "NOW()",
        "TODAY()",
        "RAND()",
        "RANDBETWEEN(1, 10)",
    ])
    def test_unsupported_raises(self, func: str) -> None:
        with pytest.raises(FormulaFunctionError, match="Unknown function"):
            _eval(f"={func}")
