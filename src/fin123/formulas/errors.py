"""Error types for formula parsing and evaluation."""

from __future__ import annotations


class FormulaError(Exception):
    """Base class for all formula-related errors."""


class FormulaParseError(FormulaError):
    """Syntax error in a formula expression.

    Attributes:
        position: Character position where the error was detected.
        message: Human-readable description.
    """

    def __init__(self, message: str, position: int | None = None) -> None:
        self.position = position
        full = f"Formula parse error: {message}"
        if position is not None:
            full += f" (at position {position})"
        super().__init__(full)


class FormulaRefError(FormulaError):
    """Reference to an unknown scalar name.

    Attributes:
        ref_name: The unresolved reference.
        available: Names that are currently available.
    """

    def __init__(self, ref_name: str, available: list[str] | None = None) -> None:
        self.ref_name = ref_name
        self.available = available or []
        msg = f"Unknown reference: {ref_name!r}"
        if self.available:
            msg += f". Available: {self.available}"
        super().__init__(msg)


class FormulaFunctionError(FormulaError):
    """Unknown function or wrong number of arguments.

    Attributes:
        func_name: The function that caused the error.
    """

    def __init__(self, func_name: str, message: str | None = None) -> None:
        self.func_name = func_name
        msg = message or f"Unknown function: {func_name!r}"
        super().__init__(msg)
