"""Financial formula functions: NPV, IRR, XNPV, XIRR."""

from __future__ import annotations

import datetime
from typing import Any

import polars as pl

from fin123.formulas.errors import FormulaFunctionError
from fin123.formulas.fn_date import _coerce_date


def _fn_npv(args: list, ctx: dict, tc: dict, resolver: Any) -> float:
    """NPV(rate, cf1, cf2, ...) — net present value.

    Discounts from t=1 (Excel semantics): NPV = sum(cf_i / (1+rate)^i).
    Does NOT include an initial investment at t=0.
    """
    if len(args) < 2:
        raise FormulaFunctionError("NPV", "NPV requires at least 2 arguments (rate, cf1, ...)")
    rate = float(args[0])
    cashflows = [float(cf) for cf in args[1:]]
    total = 0.0
    for i, cf in enumerate(cashflows, start=1):
        total += cf / (1 + rate) ** i
    return total


def _irr_newton(cashflows: list[float], guess: float = 0.1, max_iter: int = 100, tol: float = 1e-10) -> float | None:
    """Newton-Raphson method for IRR."""
    rate = guess
    for _ in range(max_iter):
        npv = sum(cf / (1 + rate) ** i for i, cf in enumerate(cashflows))
        dnpv = sum(-i * cf / (1 + rate) ** (i + 1) for i, cf in enumerate(cashflows))
        if abs(dnpv) < 1e-14:
            return None
        new_rate = rate - npv / dnpv
        if abs(new_rate - rate) < tol:
            return new_rate
        rate = new_rate
    return None


def _irr_bisection(cashflows: list[float], lo: float = -0.99, hi: float = 10.0, max_iter: int = 200, tol: float = 1e-10) -> float | None:
    """Bisection fallback for IRR."""
    def npv_at(r: float) -> float:
        return sum(cf / (1 + r) ** i for i, cf in enumerate(cashflows))

    f_lo = npv_at(lo)
    f_hi = npv_at(hi)
    if f_lo * f_hi > 0:
        return None
    for _ in range(max_iter):
        mid = (lo + hi) / 2
        f_mid = npv_at(mid)
        if abs(f_mid) < tol or (hi - lo) / 2 < tol:
            return mid
        if f_lo * f_mid < 0:
            hi = mid
            f_hi = f_mid
        else:
            lo = mid
            f_lo = f_mid
    return (lo + hi) / 2


def _fn_irr(args: list, ctx: dict, tc: dict, resolver: Any) -> float:
    """IRR(cf0, cf1, cf2, ...) — internal rate of return.

    All cashflows are at t=0, t=1, ... (equal periods).
    Uses Newton-Raphson with bisection fallback.
    """
    if len(args) < 2:
        raise FormulaFunctionError("IRR", "IRR requires at least 2 cashflows")
    cashflows = [float(cf) for cf in args]
    result = _irr_newton(cashflows)
    if result is None:
        result = _irr_bisection(cashflows)
    if result is None:
        raise FormulaFunctionError("IRR", "IRR: did not converge")
    return result


def _get_table_cols(tc: dict, table_name: str, dates_col: str, values_col: str, func_name: str) -> tuple[list[datetime.date], list[float]]:
    """Extract date and value columns from a table."""
    if not tc or table_name not in tc:
        raise FormulaFunctionError(func_name, f"{func_name}: table {table_name!r} not found")
    df: pl.DataFrame = tc[table_name]
    for col in (dates_col, values_col):
        if col not in df.columns:
            raise FormulaFunctionError(
                func_name, f"{func_name}: column {col!r} not found in {table_name!r}"
            )
    dates = [_coerce_date(d) for d in df[dates_col].to_list()]
    values = [float(v) for v in df[values_col].to_list()]
    return dates, values


def _fn_xnpv(args: list, ctx: dict, tc: dict, resolver: Any) -> float:
    """XNPV(rate, "table", "dates_col", "values_col") — NPV with specific dates.

    Day-count: Actual/365.
    """
    if len(args) != 4:
        raise FormulaFunctionError(
            "XNPV", "XNPV requires 4 arguments (rate, table, dates_col, values_col)"
        )
    rate = float(args[0])
    table_name, dates_col, values_col = args[1], args[2], args[3]
    dates, values = _get_table_cols(tc, table_name, dates_col, values_col, "XNPV")
    if not dates:
        raise FormulaFunctionError("XNPV", "XNPV: empty table")
    d0 = dates[0]
    total = 0.0
    for d, v in zip(dates, values):
        years = (d - d0).days / 365.0
        total += v / (1 + rate) ** years
    return total


def _xirr_npv(dates: list[datetime.date], values: list[float], rate: float) -> float:
    """Compute XNPV for a given rate (used internally by XIRR)."""
    d0 = dates[0]
    total = 0.0
    for d, v in zip(dates, values):
        years = (d - d0).days / 365.0
        total += v / (1 + rate) ** years
    return total


def _xirr_dnpv(dates: list[datetime.date], values: list[float], rate: float) -> float:
    """Derivative of XNPV with respect to rate."""
    d0 = dates[0]
    total = 0.0
    for d, v in zip(dates, values):
        years = (d - d0).days / 365.0
        if years == 0:
            continue
        total += -years * v / (1 + rate) ** (years + 1)
    return total


def _fn_xirr(args: list, ctx: dict, tc: dict, resolver: Any) -> float:
    """XIRR("table", "dates_col", "values_col") — IRR with specific dates.

    Day-count: Actual/365. Newton-Raphson with bisection fallback.
    """
    if len(args) != 3:
        raise FormulaFunctionError(
            "XIRR", "XIRR requires 3 arguments (table, dates_col, values_col)"
        )
    table_name, dates_col, values_col = args[0], args[1], args[2]
    dates, values = _get_table_cols(tc, table_name, dates_col, values_col, "XIRR")
    if len(dates) < 2:
        raise FormulaFunctionError("XIRR", "XIRR requires at least 2 data points")

    # Newton-Raphson
    rate = 0.1
    for _ in range(100):
        npv = _xirr_npv(dates, values, rate)
        dnpv = _xirr_dnpv(dates, values, rate)
        if abs(dnpv) < 1e-14:
            break
        new_rate = rate - npv / dnpv
        if abs(new_rate - rate) < 1e-10:
            return new_rate
        rate = new_rate
    else:
        # Bisection fallback
        lo, hi = -0.99, 10.0
        f_lo = _xirr_npv(dates, values, lo)
        f_hi = _xirr_npv(dates, values, hi)
        if f_lo * f_hi > 0:
            raise FormulaFunctionError("XIRR", "XIRR: did not converge")
        for _ in range(200):
            mid = (lo + hi) / 2
            f_mid = _xirr_npv(dates, values, mid)
            if abs(f_mid) < 1e-10 or (hi - lo) / 2 < 1e-10:
                return mid
            if f_lo * f_mid < 0:
                hi = mid
            else:
                lo = mid
                f_lo = f_mid
        return (lo + hi) / 2
    # Check if Newton converged close enough
    if abs(_xirr_npv(dates, values, rate)) < 1e-6:
        return rate
    raise FormulaFunctionError("XIRR", "XIRR: did not converge")


FINANCE_FUNCTIONS: dict[str, Any] = {
    "NPV": _fn_npv,
    "IRR": _fn_irr,
    "XNPV": _fn_xnpv,
    "XIRR": _fn_xirr,
}
