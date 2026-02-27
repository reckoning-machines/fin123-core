"""Date formula functions: DATE, YEAR, MONTH, DAY, EOMONTH."""

from __future__ import annotations

import calendar
import datetime
from typing import Any

from fin123.formulas.errors import FormulaFunctionError

# Excel epoch: 1899-12-30 (Excel incorrectly treats 1900 as a leap year,
# so serial number 1 = 1900-01-01, and we use the standard offset)
_EXCEL_EPOCH = datetime.date(1899, 12, 30)


def _coerce_date(val: Any) -> datetime.date:
    """Convert a value to a datetime.date.

    Accepts:
    - datetime.date objects (returned as-is)
    - ISO format strings ("YYYY-MM-DD")
    - Excel serial numbers (int or float)
    """
    if isinstance(val, datetime.date):
        return val
    if isinstance(val, str):
        try:
            return datetime.date.fromisoformat(val)
        except ValueError:
            raise FormulaFunctionError(
                "DATE", f"Cannot parse date string: {val!r}"
            )
    if isinstance(val, (int, float)):
        serial = int(val)
        if serial < 1:
            raise FormulaFunctionError(
                "DATE", f"Invalid Excel serial number: {serial}"
            )
        return _EXCEL_EPOCH + datetime.timedelta(days=serial)
    raise FormulaFunctionError(
        "DATE", f"Cannot coerce {type(val).__name__} to date"
    )


def _fn_date(args: list, ctx: dict, tc: dict, resolver: Any) -> datetime.date:
    """DATE(year, month, day) — construct a date."""
    if len(args) != 3:
        raise FormulaFunctionError("DATE", "DATE requires exactly 3 arguments (year, month, day)")
    year, month, day = int(args[0]), int(args[1]), int(args[2])
    try:
        return datetime.date(year, month, day)
    except ValueError as exc:
        raise FormulaFunctionError("DATE", f"Invalid date: {exc}")


def _fn_year(args: list, ctx: dict, tc: dict, resolver: Any) -> int:
    """YEAR(date) — extract year from a date."""
    if len(args) != 1:
        raise FormulaFunctionError("YEAR", "YEAR requires exactly 1 argument")
    return _coerce_date(args[0]).year


def _fn_month(args: list, ctx: dict, tc: dict, resolver: Any) -> int:
    """MONTH(date) — extract month from a date."""
    if len(args) != 1:
        raise FormulaFunctionError("MONTH", "MONTH requires exactly 1 argument")
    return _coerce_date(args[0]).month


def _fn_day(args: list, ctx: dict, tc: dict, resolver: Any) -> int:
    """DAY(date) — extract day from a date."""
    if len(args) != 1:
        raise FormulaFunctionError("DAY", "DAY requires exactly 1 argument")
    return _coerce_date(args[0]).day


def _fn_eomonth(args: list, ctx: dict, tc: dict, resolver: Any) -> datetime.date:
    """EOMONTH(start_date, months) — end of month, offset by months.

    EOMONTH(DATE(2024,1,15), 1) => 2024-02-29 (last day of Feb 2024).
    """
    if len(args) != 2:
        raise FormulaFunctionError("EOMONTH", "EOMONTH requires exactly 2 arguments (start_date, months)")
    start = _coerce_date(args[0])
    months_offset = int(args[1])
    # Calculate target month
    total_months = (start.year * 12 + start.month - 1) + months_offset
    target_year = total_months // 12
    target_month = total_months % 12 + 1
    last_day = calendar.monthrange(target_year, target_month)[1]
    return datetime.date(target_year, target_month, last_day)


DATE_FUNCTIONS: dict[str, Any] = {
    "DATE": _fn_date,
    "YEAR": _fn_year,
    "MONTH": _fn_month,
    "DAY": _fn_day,
    "EOMONTH": _fn_eomonth,
}
