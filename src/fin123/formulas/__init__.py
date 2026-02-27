"""Excel-like scalar formula parsing and evaluation.

Public API::

    from fin123.formulas import parse_formula, extract_refs, evaluate_formula
"""

from fin123.formulas.errors import (
    ENGINE_ERRORS,
    FormulaError,
    FormulaFunctionError,
    FormulaParseError,
    FormulaRefError,
)
from fin123.formulas.evaluator import evaluate_formula
from fin123.formulas.parser import (
    extract_all_refs,
    extract_refs,
    parse_formula,
    parse_sheet_ref,
)

__all__ = [
    "ENGINE_ERRORS",
    "FormulaError",
    "FormulaFunctionError",
    "FormulaParseError",
    "FormulaRefError",
    "evaluate_formula",
    "extract_all_refs",
    "extract_refs",
    "parse_formula",
    "parse_sheet_ref",
]
