"""Phase 3 tests: Excel-like scalar formula parsing and evaluation."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml

from fin123.formulas import (
    FormulaError,
    FormulaFunctionError,
    FormulaParseError,
    FormulaRefError,
    evaluate_formula,
    extract_refs,
    parse_formula,
)
from fin123.scalars import ScalarGraph


# ────────────────────────────────────────────────────────────────
# Parser tests
# ────────────────────────────────────────────────────────────────


class TestParser:
    """Tests for parse_formula and extract_refs."""

    def test_simple_addition(self) -> None:
        tree = parse_formula("=1 + 2")
        assert tree is not None

    def test_arithmetic_precedence(self) -> None:
        """Multiplication binds tighter than addition: 2+3*4 = 14."""
        tree = parse_formula("=2 + 3 * 4")
        result = evaluate_formula(tree, {})
        assert result == 14

    def test_exponentiation_right_associative(self) -> None:
        """2^3^2 = 2^(3^2) = 2^9 = 512."""
        tree = parse_formula("=2^3^2")
        result = evaluate_formula(tree, {})
        assert result == 512

    def test_unary_minus_with_exponent(self) -> None:
        """-2^2 = -(2^2) = -4 (like Excel)."""
        tree = parse_formula("=-2^2")
        result = evaluate_formula(tree, {})
        assert result == -4

    def test_boolean_literals(self) -> None:
        tree = parse_formula("=TRUE")
        result = evaluate_formula(tree, {})
        assert result is True

        tree = parse_formula("=FALSE")
        result = evaluate_formula(tree, {})
        assert result is False

    def test_string_literal(self) -> None:
        tree = parse_formula('="hello"')
        result = evaluate_formula(tree, {})
        assert result == "hello"

    def test_ref_bare(self) -> None:
        tree = parse_formula("=revenue")
        refs = extract_refs(tree)
        assert refs == {"revenue"}
        result = evaluate_formula(tree, {"revenue": 100})
        assert result == 100

    def test_ref_dollar(self) -> None:
        tree = parse_formula("=$tax_rate")
        refs = extract_refs(tree)
        assert refs == {"tax_rate"}
        result = evaluate_formula(tree, {"tax_rate": 0.15})
        assert result == 0.15

    def test_extract_refs_multiple(self) -> None:
        tree = parse_formula("=a + b * $c")
        refs = extract_refs(tree)
        assert refs == {"a", "b", "c"}

    def test_syntax_error_no_equals(self) -> None:
        with pytest.raises(FormulaParseError, match="must start with"):
            parse_formula("1 + 2")

    def test_syntax_error_bad_expression(self) -> None:
        with pytest.raises(FormulaParseError):
            parse_formula("=1 2")

    def test_syntax_error_position(self) -> None:
        """Parse error should include position info."""
        with pytest.raises(FormulaParseError) as exc_info:
            parse_formula("=1 +")
        assert "position" in str(exc_info.value).lower() or exc_info.value.position is not None

    def test_parenthesized_expression(self) -> None:
        tree = parse_formula("=(1 + 2) * 3")
        result = evaluate_formula(tree, {})
        assert result == 9


# ────────────────────────────────────────────────────────────────
# Postfix percent tests
# ────────────────────────────────────────────────────────────────


class TestPostfixPercent:
    """Tests for postfix % operator (Excel-style: 3% = 0.03)."""

    def test_literal_percent(self) -> None:
        tree = parse_formula("=3%")
        assert evaluate_formula(tree, {}) == pytest.approx(0.03)

    def test_negative_percent(self) -> None:
        """-3% = -(3%) = -0.03."""
        tree = parse_formula("=-3%")
        assert evaluate_formula(tree, {}) == pytest.approx(-0.03)

    def test_multiply_by_percent(self) -> None:
        """100*3% = 3.0."""
        tree = parse_formula("=100*3%")
        assert evaluate_formula(tree, {}) == pytest.approx(3.0)

    def test_parenthesized_percent(self) -> None:
        """(2+1)% = 0.03."""
        tree = parse_formula("=(2+1)%")
        assert evaluate_formula(tree, {}) == pytest.approx(0.03)

    def test_cell_ref_percent(self) -> None:
        """F2% with F2=50 => 0.5."""
        from unittest.mock import MagicMock

        resolver = MagicMock()
        resolver.resolve_cell.return_value = 50
        tree = parse_formula("=F2%")
        result = evaluate_formula(tree, {}, resolver=resolver, current_sheet="Sheet1")
        assert result == pytest.approx(0.5)

    def test_percent_before_exponent(self) -> None:
        """3%^2 = (0.03)^2 = 0.0009."""
        tree = parse_formula("=3%^2")
        assert evaluate_formula(tree, {}) == pytest.approx(0.0009)

    def test_parenthesized_percent_exponent(self) -> None:
        """(3%)^2 matches 3%^2."""
        tree = parse_formula("=(3%)^2")
        assert evaluate_formula(tree, {}) == pytest.approx(0.0009)

    def test_max_with_negative_percent(self) -> None:
        """=+MAX(-F2,-3%) should parse and evaluate."""
        from unittest.mock import MagicMock

        resolver = MagicMock()
        resolver.resolve_cell.return_value = 0.05  # F2 = 0.05
        tree = parse_formula("=+MAX(-F2,-3%)")
        result = evaluate_formula(tree, {}, resolver=resolver, current_sheet="Sheet1")
        # MAX(-0.05, -0.03) = -0.03
        assert result == pytest.approx(-0.03)

    def test_hundred_percent(self) -> None:
        """100% = 1.0."""
        tree = parse_formula("=100%")
        assert evaluate_formula(tree, {}) == pytest.approx(1.0)

    def test_double_percent(self) -> None:
        """3%% = (3%)/100 = 0.0003."""
        tree = parse_formula("=3%%")
        assert evaluate_formula(tree, {}) == pytest.approx(0.0003)


# ────────────────────────────────────────────────────────────────
# Unary plus tests
# ────────────────────────────────────────────────────────────────


class TestUnaryPlus:
    """Tests for unary plus (identity) support."""

    def test_parse_plus_literal(self) -> None:
        tree = parse_formula("=+1")
        assert evaluate_formula(tree, {}) == 1

    def test_parse_plus_ref(self) -> None:
        tree = parse_formula("=+val")
        refs = extract_refs(tree)
        assert refs == {"val"}
        assert evaluate_formula(tree, {"val": 7}) == 7

    def test_parse_plus_function(self) -> None:
        tree = parse_formula("=+MAX(1,2)")
        assert evaluate_formula(tree, {}) == 2

    def test_parse_double_plus(self) -> None:
        tree = parse_formula("=++val")
        assert evaluate_formula(tree, {"val": 5}) == 5

    def test_parse_plus_parenthesized(self) -> None:
        tree = parse_formula("=+(val*2)")
        assert evaluate_formula(tree, {"val": 3}) == 6

    def test_unary_plus_precedence_with_exponent(self) -> None:
        """+2^2 = +(2^2) = 4 (identity, same precedence level as unary minus)."""
        tree = parse_formula("=+2^2")
        assert evaluate_formula(tree, {}) == 4

    def test_unary_minus_still_negates(self) -> None:
        """-2^2 = -(2^2) = -4 — unchanged."""
        tree = parse_formula("=-2^2")
        assert evaluate_formula(tree, {}) == -4

    def test_parenthesized_neg_squared(self) -> None:
        """(-2)^2 = 4."""
        tree = parse_formula("=(-2)^2")
        assert evaluate_formula(tree, {}) == 4

    def test_plus_minus_combo(self) -> None:
        """=+-5 = +(-5) = -5."""
        tree = parse_formula("=+-5")
        assert evaluate_formula(tree, {}) == -5

    def test_binary_plus_unary_plus(self) -> None:
        """=1 + +2 is now valid: 1 + (+2) = 3."""
        tree = parse_formula("=1 + +2")
        assert evaluate_formula(tree, {}) == 3


# ────────────────────────────────────────────────────────────────
# Evaluator tests
# ────────────────────────────────────────────────────────────────


class TestEvaluator:
    """Tests for evaluate_formula with arithmetic and comparisons."""

    def test_all_arithmetic_ops(self) -> None:
        cases = [
            ("=10 + 5", 15),
            ("=10 - 5", 5),
            ("=10 * 5", 50),
            ("=10 / 4", 2.5),
            ("=2 ^ 10", 1024),
        ]
        for formula, expected in cases:
            tree = parse_formula(formula)
            assert evaluate_formula(tree, {}) == expected, f"Failed: {formula}"

    def test_all_comparison_ops(self) -> None:
        cases = [
            ("=5 > 3", True),
            ("=3 > 5", False),
            ("=5 < 3", False),
            ("=3 < 5", True),
            ("=5 >= 5", True),
            ("=5 <= 5", True),
            ("=5 = 5", True),
            ("=5 <> 3", True),
            ("=5 <> 5", False),
        ]
        for formula, expected in cases:
            tree = parse_formula(formula)
            assert evaluate_formula(tree, {}) == expected, f"Failed: {formula}"

    def test_division_by_zero(self) -> None:
        tree = parse_formula("=1 / 0")
        with pytest.raises(ZeroDivisionError):
            evaluate_formula(tree, {})

    def test_unknown_reference(self) -> None:
        tree = parse_formula("=missing_ref")
        with pytest.raises(FormulaRefError, match="missing_ref"):
            evaluate_formula(tree, {"known": 1})


# ────────────────────────────────────────────────────────────────
# Function tests
# ────────────────────────────────────────────────────────────────


class TestFunctions:
    """Tests for built-in formula functions."""

    def test_sum(self) -> None:
        tree = parse_formula("=SUM(1, 2, 3)")
        assert evaluate_formula(tree, {}) == 6

    def test_average(self) -> None:
        tree = parse_formula("=AVERAGE(10, 20, 30)")
        assert evaluate_formula(tree, {}) == 20.0

    def test_min(self) -> None:
        tree = parse_formula("=MIN(5, 3, 8)")
        assert evaluate_formula(tree, {}) == 3

    def test_max(self) -> None:
        tree = parse_formula("=MAX(5, 3, 8)")
        assert evaluate_formula(tree, {}) == 8

    def test_abs(self) -> None:
        tree = parse_formula("=ABS(-42)")
        assert evaluate_formula(tree, {}) == 42

    def test_round_default(self) -> None:
        tree = parse_formula("=ROUND(3.14159)")
        assert evaluate_formula(tree, {}) == 3

    def test_round_with_digits(self) -> None:
        tree = parse_formula("=ROUND(3.14159, 2)")
        assert evaluate_formula(tree, {}) == 3.14

    def test_unknown_function(self) -> None:
        tree = parse_formula("=NOSUCHFUNC(1)")
        with pytest.raises(FormulaFunctionError):
            evaluate_formula(tree, {})


# ────────────────────────────────────────────────────────────────
# IF tests
# ────────────────────────────────────────────────────────────────


class TestIf:
    """Tests for IF with lazy evaluation."""

    def test_if_true_branch(self) -> None:
        tree = parse_formula("=IF(TRUE, 10, 20)")
        assert evaluate_formula(tree, {}) == 10

    def test_if_false_branch(self) -> None:
        tree = parse_formula("=IF(FALSE, 10, 20)")
        assert evaluate_formula(tree, {}) == 20

    def test_if_short_circuit(self) -> None:
        """IF(TRUE, 1, 1/0) should return 1 without division-by-zero error."""
        tree = parse_formula("=IF(TRUE, 1, 1/0)")
        assert evaluate_formula(tree, {}) == 1

    def test_if_two_arg_form(self) -> None:
        """IF(FALSE, 1) should return FALSE."""
        tree = parse_formula("=IF(FALSE, 1)")
        assert evaluate_formula(tree, {}) is False

    def test_nested_if(self) -> None:
        tree = parse_formula("=IF(x > 10, IF(x > 20, 3, 2), 1)")
        assert evaluate_formula(tree, {"x": 5}) == 1
        assert evaluate_formula(tree, {"x": 15}) == 2
        assert evaluate_formula(tree, {"x": 25}) == 3


# ────────────────────────────────────────────────────────────────
# IFERROR tests
# ────────────────────────────────────────────────────────────────


class TestIferror:
    """Tests for IFERROR with lazy evaluation."""

    def test_catches_division_by_zero(self) -> None:
        tree = parse_formula("=IFERROR(1/0, -1)")
        assert evaluate_formula(tree, {}) == -1

    def test_catches_missing_ref(self) -> None:
        tree = parse_formula("=IFERROR(missing, 0)")
        assert evaluate_formula(tree, {}) == 0

    def test_passthrough_on_no_error(self) -> None:
        tree = parse_formula("=IFERROR(42, -1)")
        assert evaluate_formula(tree, {}) == 42


# ────────────────────────────────────────────────────────────────
# VLOOKUP tests
# ────────────────────────────────────────────────────────────────


class TestVlookup:
    """Tests for VLOOKUP formula function."""

    @pytest.fixture
    def table_cache(self) -> dict[str, pl.DataFrame]:
        return {
            "prices": pl.DataFrame({
                "ticker": ["AAPL", "GOOG", "MSFT"],
                "price": [150.0, 2800.0, 300.0],
            })
        }

    def test_basic_lookup(self, table_cache: dict) -> None:
        tree = parse_formula('=VLOOKUP("GOOG", "prices", "ticker", "price")')
        result = evaluate_formula(tree, {}, table_cache)
        assert result == 2800.0

    def test_three_arg_form(self, table_cache: dict) -> None:
        """3-arg form uses first column as key_col."""
        tree = parse_formula('=VLOOKUP("MSFT", "prices", "price")')
        result = evaluate_formula(tree, {}, table_cache)
        assert result == 300.0

    def test_missing_table(self) -> None:
        tree = parse_formula('=VLOOKUP("X", "no_table", "k", "v")')
        with pytest.raises(FormulaFunctionError, match="not found"):
            evaluate_formula(tree, {}, {})

    def test_missing_key(self, table_cache: dict) -> None:
        tree = parse_formula('=VLOOKUP("NOPE", "prices", "ticker", "price")')
        with pytest.raises(ValueError, match="no row found"):
            evaluate_formula(tree, {}, table_cache)


# ────────────────────────────────────────────────────────────────
# SUMIFS / COUNTIFS tests
# ────────────────────────────────────────────────────────────────


class TestAggregateFilters:
    """Tests for SUMIFS and COUNTIFS."""

    @pytest.fixture
    def table_cache(self) -> dict[str, pl.DataFrame]:
        return {
            "sales": pl.DataFrame({
                "region": ["US", "US", "EU", "EU", "US"],
                "product": ["A", "B", "A", "B", "A"],
                "amount": [100, 200, 150, 250, 300],
            })
        }

    def test_sumifs_basic(self, table_cache: dict) -> None:
        tree = parse_formula('=SUMIFS("sales", "amount", "region", "=", "US")')
        result = evaluate_formula(tree, {}, table_cache)
        assert result == 600.0  # 100 + 200 + 300

    def test_sumifs_multiple_criteria(self, table_cache: dict) -> None:
        tree = parse_formula(
            '=SUMIFS("sales", "amount", "region", "=", "US", "product", "=", "A")'
        )
        result = evaluate_formula(tree, {}, table_cache)
        assert result == 400.0  # 100 + 300

    def test_countifs_basic(self, table_cache: dict) -> None:
        tree = parse_formula('=COUNTIFS("sales", "region", "=", "EU")')
        result = evaluate_formula(tree, {}, table_cache)
        assert result == 2

    def test_sumifs_bad_arity(self) -> None:
        tree = parse_formula('=SUMIFS("sales", "amount", "region")')
        with pytest.raises(FormulaFunctionError, match="SUMIFS"):
            evaluate_formula(tree, {}, {})

    def test_countifs_bad_arity(self) -> None:
        tree = parse_formula('=COUNTIFS("sales", "region")')
        with pytest.raises(FormulaFunctionError, match="COUNTIFS"):
            evaluate_formula(tree, {}, {})


# ────────────────────────────────────────────────────────────────
# ScalarGraph integration tests
# ────────────────────────────────────────────────────────────────


class TestScalarGraphIntegration:
    """Tests for parsed formulas integrated into ScalarGraph."""

    def test_parsed_formula_resolves(self) -> None:
        sg = ScalarGraph()
        sg.set_value("a", 10)
        sg.set_value("b", 20)
        tree = parse_formula("=a + b")
        deps = extract_refs(tree)
        sg.set_parsed_formula("result", tree, deps)
        values = sg.evaluate()
        assert values["result"] == 30

    def test_cross_dependency_structured_to_parsed(self) -> None:
        """Structured formula depends on a parsed formula."""
        sg = ScalarGraph()
        sg.set_value("price", 100)
        # Parsed formula
        tree = parse_formula("=price * 1.1")
        deps = extract_refs(tree)
        sg.set_parsed_formula("adjusted_price", tree, deps)
        # Structured formula depends on the parsed one
        sg.set_formula("double_price", "multiply", {"a": "$adjusted_price", "b": 2})
        values = sg.evaluate()
        assert values["adjusted_price"] == pytest.approx(110.0)
        assert values["double_price"] == pytest.approx(220.0)

    def test_cross_dependency_parsed_to_structured(self) -> None:
        """Parsed formula depends on a structured formula."""
        sg = ScalarGraph()
        sg.set_value("a", 5)
        sg.set_value("b", 3)
        sg.set_formula("s", "sum", {"values": ["$a", "$b"]})
        tree = parse_formula("=s * 10")
        deps = extract_refs(tree)
        sg.set_parsed_formula("result", tree, deps)
        values = sg.evaluate()
        assert values["s"] == 8.0
        assert values["result"] == 80.0

    def test_circular_dependency_detected(self) -> None:
        sg = ScalarGraph()
        tree_a = parse_formula("=b + 1")
        tree_b = parse_formula("=a + 1")
        sg.set_parsed_formula("a", tree_a, {"b"})
        sg.set_parsed_formula("b", tree_b, {"a"})
        with pytest.raises(ValueError, match="Circular"):
            sg.evaluate()


# ────────────────────────────────────────────────────────────────
# Workbook integration tests
# ────────────────────────────────────────────────────────────────


def _make_project(tmp_path: Path, spec: dict) -> Path:
    """Helper: create a minimal workbook project from a spec dict."""
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / "workbook.yaml").write_text(yaml.dump(spec))
    return project_dir


class TestWorkbookIntegration:
    """Tests for formula detection in the workbook layer."""

    def test_formula_key(self, tmp_path: Path) -> None:
        """formula: '=...' is parsed as a formula expression."""
        from fin123.workbook import Workbook

        spec = {
            "params": {"price": 100, "qty": 5},
            "outputs": [
                {
                    "name": "revenue",
                    "type": "scalar",
                    "formula": "=price * qty",
                },
            ],
        }
        proj = _make_project(tmp_path, spec)
        wb = Workbook(proj)
        result = wb.run()
        assert result.scalars["revenue"] == 500

    def test_value_equals_syntax(self, tmp_path: Path) -> None:
        """value: '=...' is detected as a formula."""
        from fin123.workbook import Workbook

        spec = {
            "params": {"x": 10},
            "outputs": [
                {
                    "name": "doubled",
                    "type": "scalar",
                    "value": "=x * 2",
                },
            ],
        }
        proj = _make_project(tmp_path, spec)
        wb = Workbook(proj)
        result = wb.run()
        assert result.scalars["doubled"] == 20

    def test_mixed_formats(self, tmp_path: Path) -> None:
        """Mix of formula, value, and func all coexist."""
        from fin123.workbook import Workbook

        spec = {
            "params": {"base": 100},
            "outputs": [
                {"name": "lit", "type": "scalar", "value": 42},
                {"name": "expr_result", "type": "scalar", "formula": "=base + lit"},
                {
                    "name": "doubled",
                    "type": "scalar",
                    "func": "multiply",
                    "args": {"a": "$expr_result", "b": 2},
                },
            ],
        }
        proj = _make_project(tmp_path, spec)
        wb = Workbook(proj)
        result = wb.run()
        assert result.scalars["lit"] == 42
        assert result.scalars["expr_result"] == 142
        assert result.scalars["doubled"] == 284.0

    def test_existing_demo_project_still_passes(self) -> None:
        """Existing demo project runs unchanged with Phase 3 code."""
        from fin123.project import scaffold_project
        from fin123.workbook import Workbook

        import tempfile

        with tempfile.TemporaryDirectory() as td:
            proj = Path(td) / "demo"
            scaffold_project(proj)
            wb = Workbook(proj)
            result = wb.run()
            assert "total_revenue" in result.scalars
            assert result.scalars["gross_revenue"] == 125000.0


# ────────────────────────────────────────────────────────────────
# No array spill test
# ────────────────────────────────────────────────────────────────


class TestNoArraySpill:
    """Formulas always return scalar values."""

    def test_formula_returns_scalar(self) -> None:
        tree = parse_formula("=SUM(1, 2, 3)")
        result = evaluate_formula(tree, {})
        assert isinstance(result, (int, float, bool, str))

    def test_nested_functions_return_scalar(self) -> None:
        tree = parse_formula("=ROUND(AVERAGE(10, 20, 30), 1)")
        result = evaluate_formula(tree, {})
        assert isinstance(result, (int, float))
        assert result == 20.0
