"""Shared types for the worksheet subsystem."""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel


class ColumnType(str, Enum):
    """Logical column types for worksheet columns."""

    STRING = "string"
    INT64 = "int64"
    FLOAT64 = "float64"
    BOOL = "bool"
    DATE = "date"
    DATETIME = "datetime"


class ColumnSchema(BaseModel):
    """Schema for a single column in a ViewTable."""

    name: str
    dtype: ColumnType
    nullable: bool = True


class DisplayFormat(BaseModel):
    """Structured display format for rendering."""

    type: Literal["decimal", "percent", "currency", "integer", "date", "text"]
    places: int | None = None
    symbol: str | None = None
    date_format: str | None = None
