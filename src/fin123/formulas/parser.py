"""Lark-based parser for Excel-like scalar formulas.

Supports:
- Scalar references: ``name`` or ``$name``
- In-sheet cell references: ``F2``, ``AA10`` (bare A1-style)
- Cross-sheet cell references: ``Sheet1!A1`` or ``'My Sheet'!A1``
- Named ranges (resolved at evaluation time, parsed as ref_bare)
- Standard arithmetic, comparisons, functions, postfix percent (%)
"""

from __future__ import annotations

from lark import Lark, Tree, Visitor, Token

from fin123.formulas.errors import FormulaParseError

# LALR(1) grammar for Excel-like scalar formulas.
# Operator precedence (lowest to highest):
#   1. Comparison: > < >= <= = <>
#   2. Addition/subtraction: + -
#   3. Multiplication/division: * /
#   4. Unary plus/minus: + -
#   5. Exponentiation: ^ (right-associative)
#   6. Postfix percent: %  (3% = 0.03)
#   7. Atoms: number, bool, string, function call, reference, parenthesized expr
GRAMMAR = r"""
start: "=" expr

?expr: comparison

?comparison: addition
    | comparison ">" addition   -> gt
    | comparison "<" addition   -> lt
    | comparison ">=" addition  -> gte
    | comparison "<=" addition  -> lte
    | comparison "=" addition   -> eq
    | comparison "<>" addition  -> neq

?addition: multiplication
    | addition "+" multiplication  -> add
    | addition "-" multiplication  -> sub

?multiplication: unary
    | multiplication "*" unary  -> mul
    | multiplication "/" unary  -> div

?unary: exponentiation
    | "-" unary  -> neg
    | "+" unary  -> pos

?exponentiation: postfix
    | postfix "^" unary  -> pow

?postfix: atom
    | postfix "%"  -> percent

?atom: NUMBER                   -> number
    | BOOL                      -> boolean
    | ESCAPED_STRING            -> string
    | NAME "(" args ")"         -> func_call
    | QUOTED_SHEET_REF          -> sheet_cell_ref
    | SHEET_REF                 -> sheet_cell_ref
    | CELL_REF                  -> cell_ref
    | "$" NAME                  -> ref_dollar
    | NAME                      -> ref_bare
    | "(" expr ")"

args: expr ("," expr)*
    |

BOOL.2: "TRUE" | "FALSE"

// Cross-sheet cell ref: Sheet1!A1 (no spaces in unquoted form)
SHEET_REF.3: /[A-Za-z_][A-Za-z0-9_]*![A-Z]{1,3}[0-9]+/

// Quoted cross-sheet cell ref: 'My Sheet'!A1
QUOTED_SHEET_REF.3: /'[^']+'![A-Z]{1,3}[0-9]+/

// Bare in-sheet cell ref: A1, F2, AA10, AAA9999 (uppercase only)
CELL_REF.2: /[A-Z]{1,3}[0-9]+/

NAME.1: /[A-Za-z_][A-Za-z0-9_]*/

%import common.NUMBER
%import common.ESCAPED_STRING
%import common.WS
%ignore WS
"""

_parser = Lark(GRAMMAR, parser="lalr", start="start")


def parse_formula(text: str) -> Tree:
    """Parse a formula string (must start with ``=``) into a Lark Tree.

    Args:
        text: The formula text, e.g. ``"=revenue * (1 - tax_rate)"``.

    Returns:
        A Lark parse tree.

    Raises:
        FormulaParseError: If the formula has invalid syntax.
    """
    text = text.strip()
    if not text.startswith("="):
        raise FormulaParseError("Formula must start with '='", position=0)
    try:
        return _parser.parse(text)
    except Exception as exc:
        # Extract position info from Lark exception if available
        pos = getattr(exc, "column", None)
        raise FormulaParseError(str(exc), position=pos) from exc


def parse_sheet_ref(token_str: str) -> tuple[str, str]:
    """Parse a SHEET_REF or QUOTED_SHEET_REF token into (sheet_name, cell_addr).

    Examples:
        ``"Sheet1!A1"`` → ``("Sheet1", "A1")``
        ``"'My Sheet'!B2"`` → ``("My Sheet", "B2")``
    """
    s = token_str.strip()
    if s.startswith("'"):
        # Quoted: 'Sheet Name'!A1
        close_quote = s.index("'", 1)
        sheet_name = s[1:close_quote]
        cell_addr = s[close_quote + 2:]  # skip '!
    else:
        # Unquoted: Sheet1!A1
        bang = s.index("!")
        sheet_name = s[:bang]
        cell_addr = s[bang + 1:]
    return sheet_name, cell_addr.upper()


class _RefCollector(Visitor):
    """Visitor that collects all references from a parse tree."""

    def __init__(self) -> None:
        self.scalar_refs: set[str] = set()
        self.cell_refs: set[tuple[str | None, str]] = set()  # (sheet|None, addr)

    def ref_bare(self, tree: Tree) -> None:
        token = tree.children[0]
        if isinstance(token, Token) and token.type != "BOOL":
            self.scalar_refs.add(str(token))

    def ref_dollar(self, tree: Tree) -> None:
        token = tree.children[0]
        self.scalar_refs.add(str(token))

    def cell_ref(self, tree: Tree) -> None:
        token = tree.children[0]
        self.cell_refs.add((None, str(token).upper()))

    def sheet_cell_ref(self, tree: Tree) -> None:
        token = tree.children[0]
        sheet_name, cell_addr = parse_sheet_ref(str(token))
        self.cell_refs.add((sheet_name, cell_addr))


def extract_refs(tree: Tree) -> set[str]:
    """Extract all scalar reference names from a parsed formula tree.

    Args:
        tree: A parse tree from ``parse_formula()``.

    Returns:
        Set of referenced scalar names (without ``$`` prefix).
        Does NOT include cross-sheet cell refs (use ``extract_all_refs`` for those).
    """
    collector = _RefCollector()
    collector.visit(tree)
    return collector.scalar_refs


def extract_all_refs(tree: Tree) -> tuple[set[str], set[tuple[str | None, str]]]:
    """Extract all references from a parsed formula tree.

    Returns:
        Tuple of (scalar_refs, cell_refs) where:
        - scalar_refs: set of scalar/name reference strings
        - cell_refs: set of (sheet_name | None, cell_addr) tuples.
          ``None`` sheet means a bare in-sheet ref (e.g. ``F2``);
          a string sheet means a cross-sheet ref (e.g. ``Sheet1!A1``).
    """
    collector = _RefCollector()
    collector.visit(tree)
    return collector.scalar_refs, collector.cell_refs
