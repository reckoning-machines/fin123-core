"""Worksheet subsystem: deterministic business worksheet runtime.

Public API::

    from fin123.worksheet import (
        ViewTable, WorksheetView, CompiledWorksheet,
        compile_worksheet,
        from_fin123_run, from_polars, from_json_records,
    )
"""

from fin123.worksheet.compiled import CompiledWorksheet
from fin123.worksheet.compiler import compile_worksheet
from fin123.worksheet.spec import WorksheetView, load_worksheet_view, parse_worksheet_view
from fin123.worksheet.view_table import (
    ViewTable,
    from_fin123_run,
    from_json_records,
    from_polars,
    suggest_schema,
)

__all__ = [
    "CompiledWorksheet",
    "ViewTable",
    "WorksheetView",
    "compile_worksheet",
    "from_fin123_run",
    "from_json_records",
    "from_polars",
    "load_worksheet_view",
    "parse_worksheet_view",
    "suggest_schema",
]
