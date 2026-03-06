"""Tests for the restricted row-local formula evaluator (Stage 0)."""

from __future__ import annotations

import datetime

import pytest

from fin123.worksheet.eval_row import (
    ROW_LOCAL_ALLOWLIST,
    evaluate_row_expression,
    evaluate_row_tree,
    parse_row_expression,
    validate_row_local,
)


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def row() -> dict:
    return {
        "revenue": 100,
        "cost": 40,
        "price": 25.0,
        "quantity": 4,
        "name": "Widget",
        "active": True,
        "start_date": "2025-01-15",
    }


# ────────────────────────────────────────────────────────────────
# Basic arithmetic
# ────────────────────────────────────────────────────────────────


class TestArithmetic:
    def test_subtraction(self, row: dict) -> None:
        result = evaluate_row_expression("revenue - cost", row)
        assert result == 60

    def test_multiplication(self, row: dict) -> None:
        result = evaluate_row_expression("price * quantity", row)
        assert result == 100.0

    def test_division(self, row: dict) -> None:
        result = evaluate_row_expression("revenue / cost", row)
        assert result == 2.5

    def test_parenthesized(self, row: dict) -> None:
        result = evaluate_row_expression("(revenue - cost) / revenue", row)
        assert result == 0.6

    def test_negation(self, row: dict) -> None:
        result = evaluate_row_expression("-cost", row)
        assert result == -40

    def test_exponentiation(self, row: dict) -> None:
        result = evaluate_row_expression("quantity ^ 2", row)
        assert result == 16

    def test_percent(self, row: dict) -> None:
        result = evaluate_row_expression("10%", row)
        assert result == pytest.approx(0.1)

    def test_literal_number(self, row: dict) -> None:
        result = evaluate_row_expression("42", row)
        assert result == 42

    def test_comparison_gt(self, row: dict) -> None:
        result = evaluate_row_expression("revenue > cost", row)
        assert result is True

    def test_comparison_eq(self, row: dict) -> None:
        result = evaluate_row_expression("revenue = 100", row)
        assert result is True

    def test_string_literal(self, row: dict) -> None:
        result = evaluate_row_expression('"hello"', row)
        assert result == "hello"

    def test_boolean_literal(self, row: dict) -> None:
        result = evaluate_row_expression("TRUE", row)
        assert result is True

    def test_equals_prefix_optional(self, row: dict) -> None:
        """Expressions work with or without = prefix."""
        assert evaluate_row_expression("revenue - cost", row) == 60
        assert evaluate_row_expression("=revenue - cost", row) == 60


# ────────────────────────────────────────────────────────────────
# Allowed functions
# ────────────────────────────────────────────────────────────────


class TestAllowedFunctions:
    def test_if_true(self, row: dict) -> None:
        result = evaluate_row_expression("IF(revenue > 50, revenue, 0)", row)
        assert result == 100

    def test_if_false(self, row: dict) -> None:
        result = evaluate_row_expression("IF(revenue < 50, revenue, 0)", row)
        assert result == 0

    def test_iferror_no_error(self, row: dict) -> None:
        result = evaluate_row_expression("IFERROR(revenue / cost, -1)", row)
        assert result == 2.5

    def test_iferror_with_error(self) -> None:
        result = evaluate_row_expression("IFERROR(revenue / cost, -1)", {"revenue": 10, "cost": 0})
        assert result == -1

    def test_iserror_false(self, row: dict) -> None:
        result = evaluate_row_expression("ISERROR(revenue)", row)
        assert result is False

    def test_abs(self, row: dict) -> None:
        result = evaluate_row_expression("ABS(-cost)", row)
        assert result == 40

    def test_round(self, row: dict) -> None:
        result = evaluate_row_expression("ROUND(revenue / 3, 2)", row)
        assert result == 33.33

    def test_and(self, row: dict) -> None:
        result = evaluate_row_expression("AND(revenue > 0, cost > 0)", row)
        assert result is True

    def test_or(self, row: dict) -> None:
        result = evaluate_row_expression("OR(revenue < 0, cost > 0)", row)
        assert result is True

    def test_not(self, row: dict) -> None:
        result = evaluate_row_expression("NOT(revenue < 0)", row)
        assert result is True

    def test_sum(self, row: dict) -> None:
        result = evaluate_row_expression("SUM(revenue, cost, price)", row)
        assert result == 165.0

    def test_average(self, row: dict) -> None:
        result = evaluate_row_expression("AVERAGE(revenue, cost)", row)
        assert result == 70.0

    def test_min(self, row: dict) -> None:
        result = evaluate_row_expression("MIN(revenue, cost, price)", row)
        assert result == 25.0

    def test_max(self, row: dict) -> None:
        result = evaluate_row_expression("MAX(revenue, cost, price)", row)
        assert result == 100

    def test_date(self, row: dict) -> None:
        result = evaluate_row_expression("DATE(2025, 6, 15)", row)
        assert result == datetime.date(2025, 6, 15)

    def test_year(self, row: dict) -> None:
        result = evaluate_row_expression('YEAR("2025-06-15")', row)
        assert result == 2025

    def test_nested_functions(self, row: dict) -> None:
        result = evaluate_row_expression(
            "IF(ABS(revenue - cost) > 50, ROUND(revenue / cost, 1), 0)", row
        )
        assert result == 2.5


# ────────────────────────────────────────────────────────────────
# Error handling — inline error dicts
# ────────────────────────────────────────────────────────────────


class TestErrorHandling:
    def test_division_by_zero(self) -> None:
        result = evaluate_row_expression("revenue / cost", {"revenue": 100, "cost": 0})
        assert result == {"error": "#DIV/0!"}

    def test_unknown_reference(self) -> None:
        result = evaluate_row_expression("nonexistent + 1", {"revenue": 100})
        assert result == {"error": "#REF!"}

    def test_parse_error(self) -> None:
        result = evaluate_row_expression("+++", {})
        assert isinstance(result, dict)
        assert "error" in result

    def test_disallowed_function_returns_error(self) -> None:
        result = evaluate_row_expression(
            'VLOOKUP(revenue, "table", "col")', {"revenue": 100}
        )
        assert isinstance(result, dict)
        assert "error" in result


# ────────────────────────────────────────────────────────────────
# Disallowed constructs — static validation
# ────────────────────────────────────────────────────────────────


class TestDisallowed:
    def test_vlookup_rejected(self) -> None:
        errors = validate_row_local('VLOOKUP(x, "t", "c")')
        assert any("VLOOKUP" in e for e in errors)

    def test_sumifs_rejected(self) -> None:
        errors = validate_row_local('SUMIFS("t", "s", "c", "=", 1)')
        assert any("SUMIFS" in e for e in errors)

    def test_countifs_rejected(self) -> None:
        errors = validate_row_local('COUNTIFS("t", "c", "=", 1)')
        assert any("COUNTIFS" in e for e in errors)

    def test_xlookup_rejected(self) -> None:
        errors = validate_row_local('XLOOKUP(x, "t", "l", "r")')
        assert any("XLOOKUP" in e for e in errors)

    def test_match_rejected(self) -> None:
        errors = validate_row_local('MATCH(x, "t", "c")')
        assert any("MATCH" in e for e in errors)

    def test_index_rejected(self) -> None:
        errors = validate_row_local('INDEX("t", "c", 1)')
        assert any("INDEX" in e for e in errors)

    def test_xnpv_rejected(self) -> None:
        errors = validate_row_local('XNPV(0.1, "t", "d", "v")')
        assert any("XNPV" in e for e in errors)

    def test_xirr_rejected(self) -> None:
        errors = validate_row_local('XIRR("t", "d", "v")')
        assert any("XIRR" in e for e in errors)

    def test_npv_rejected(self) -> None:
        errors = validate_row_local("NPV(0.1, 100, 200)")
        assert any("NPV" in e for e in errors)

    def test_irr_rejected(self) -> None:
        errors = validate_row_local("IRR(-100, 50, 60)")
        assert any("IRR" in e for e in errors)

    def test_param_rejected(self) -> None:
        errors = validate_row_local('PARAM("x")')
        assert any("PARAM" in e for e in errors)

    def test_cell_ref_rejected(self) -> None:
        errors = validate_row_local("A1 + 1")
        assert any("Cell reference" in e for e in errors)

    def test_sheet_ref_rejected(self) -> None:
        errors = validate_row_local("Sheet1!A1 + 1")
        assert any("Sheet reference" in e for e in errors)

    def test_allowed_functions_pass(self) -> None:
        errors = validate_row_local("IF(revenue > 0, ABS(cost), ROUND(price, 2))")
        assert errors == []


# ────────────────────────────────────────────────────────────────
# validate_row_local — column checking
# ────────────────────────────────────────────────────────────────


class TestValidationColumns:
    def test_valid_columns(self) -> None:
        errors = validate_row_local(
            "revenue - cost", available_columns=["revenue", "cost"]
        )
        assert errors == []

    def test_missing_column(self) -> None:
        errors = validate_row_local(
            "revenue - cost", available_columns=["revenue"]
        )
        assert any("cost" in e for e in errors)

    def test_no_column_check_when_none(self) -> None:
        errors = validate_row_local("whatever + stuff")
        assert errors == []


# ────────────────────────────────────────────────────────────────
# parse_row_expression — returns tree, raises on invalid
# ────────────────────────────────────────────────────────────────


class TestParseRowExpression:
    def test_returns_tree(self) -> None:
        tree = parse_row_expression("revenue - cost")
        assert tree is not None

    def test_raises_on_disallowed(self) -> None:
        with pytest.raises(Exception, match="Row-local validation failed"):
            parse_row_expression('VLOOKUP(x, "t", "c")')

    def test_raises_on_cell_ref(self) -> None:
        with pytest.raises(Exception, match="Row-local validation failed"):
            parse_row_expression("A1 + 1")

    def test_raises_on_parse_error(self) -> None:
        with pytest.raises(Exception):
            parse_row_expression("((((")


# ────────────────────────────────────────────────────────────────
# evaluate_row_tree — low-level, raises on error
# ────────────────────────────────────────────────────────────────


class TestEvaluateRowTree:
    def test_basic(self) -> None:
        tree = parse_row_expression("revenue * 2")
        result = evaluate_row_tree(tree, {"revenue": 50})
        assert result == 100

    def test_raises_on_div_zero(self) -> None:
        tree = parse_row_expression("revenue / cost")
        with pytest.raises(ZeroDivisionError):
            evaluate_row_tree(tree, {"revenue": 100, "cost": 0})

    def test_reusable_tree(self) -> None:
        """Same tree can be evaluated against different rows."""
        tree = parse_row_expression("price * quantity")
        assert evaluate_row_tree(tree, {"price": 10, "quantity": 5}) == 50
        assert evaluate_row_tree(tree, {"price": 20, "quantity": 3}) == 60
        assert evaluate_row_tree(tree, {"price": 0, "quantity": 100}) == 0


# ────────────────────────────────────────────────────────────────
# Allowlist completeness
# ────────────────────────────────────────────────────────────────


class TestAllowlistCompleteness:
    def test_allowlist_is_frozenset(self) -> None:
        assert isinstance(ROW_LOCAL_ALLOWLIST, frozenset)

    def test_expected_functions_present(self) -> None:
        expected = {"IF", "IFERROR", "ISERROR", "AND", "OR", "NOT",
                    "SUM", "AVERAGE", "MIN", "MAX", "ABS", "ROUND",
                    "DATE", "YEAR", "MONTH", "DAY", "EOMONTH"}
        assert expected == ROW_LOCAL_ALLOWLIST
