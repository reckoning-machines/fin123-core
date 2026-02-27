"""Tests for view-only table sort/filter transforms."""

from __future__ import annotations

import polars as pl
import pytest

from fin123.ui.view_transforms import (
    BetweenFilter,
    BlanksFilter,
    NumericFilter,
    SortSpec,
    TextFilter,
    ValueListFilter,
    apply_view_transforms,
)


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [30, 25, 35, 28, 32],
        "city": ["NYC", "LA", "NYC", "LA", "Chicago"],
        "score": [90.5, 85.0, 92.3, 88.1, None],
    })


# ────────────────────────────────────────────────────────────────
# Sort tests
# ────────────────────────────────────────────────────────────────


class TestSort:
    def test_sort_asc(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(sample_df, sorts=[SortSpec(column="age")])
        assert result["name"].to_list() == ["Bob", "Diana", "Alice", "Eve", "Charlie"]

    def test_sort_desc(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(sample_df, sorts=[SortSpec(column="age", descending=True)])
        assert result["name"].to_list() == ["Charlie", "Eve", "Alice", "Diana", "Bob"]

    def test_sort_multi(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df,
            sorts=[SortSpec(column="city"), SortSpec(column="age")],
        )
        names = result["name"].to_list()
        assert names[0] == "Eve"  # Chicago
        assert names[1] == "Bob"  # LA, age 25
        assert names[2] == "Diana"  # LA, age 28
        assert names[3] == "Alice"  # NYC, age 30
        assert names[4] == "Charlie"  # NYC, age 35

    def test_sort_nulls_last(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(sample_df, sorts=[SortSpec(column="score")])
        # Null score (Eve) should be last
        assert result["name"].to_list()[-1] == "Eve"

    def test_sort_stable_tiebreak(self) -> None:
        """Rows with equal sort keys maintain original order."""
        df = pl.DataFrame({
            "group": ["A", "A", "A"],
            "val": [1, 2, 3],
        })
        result = apply_view_transforms(df, sorts=[SortSpec(column="group")])
        assert result["val"].to_list() == [1, 2, 3]

    def test_no_transforms(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(sample_df)
        assert result.shape == sample_df.shape
        assert result["name"].to_list() == sample_df["name"].to_list()


# ────────────────────────────────────────────────────────────────
# Filter tests
# ────────────────────────────────────────────────────────────────


class TestNumericFilter:
    def test_gt(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[NumericFilter(column="age", op=">", value=30)]
        )
        assert set(result["name"].to_list()) == {"Charlie", "Eve"}

    def test_eq(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[NumericFilter(column="age", op="=", value=25)]
        )
        assert result["name"].to_list() == ["Bob"]

    def test_lte(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[NumericFilter(column="age", op="<=", value=28)]
        )
        assert set(result["name"].to_list()) == {"Bob", "Diana"}

    def test_neq(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[NumericFilter(column="age", op="<>", value=30)]
        )
        assert "Alice" not in result["name"].to_list()
        assert len(result) == 4


class TestBetweenFilter:
    def test_between(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[BetweenFilter(column="age", low=28, high=32)]
        )
        assert set(result["name"].to_list()) == {"Alice", "Diana", "Eve"}


class TestTextFilter:
    def test_contains(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[TextFilter(column="name", op="contains", value="li")]
        )
        # Case-insensitive by default: Alice, Charlie
        assert set(result["name"].to_list()) == {"Alice", "Charlie"}

    def test_starts_with(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[TextFilter(column="name", op="starts_with", value="d")]
        )
        assert result["name"].to_list() == ["Diana"]

    def test_ends_with(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[TextFilter(column="name", op="ends_with", value="e")]
        )
        assert set(result["name"].to_list()) == {"Alice", "Charlie", "Eve"}

    def test_equals(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[TextFilter(column="name", op="equals", value="bob")]
        )
        assert result["name"].to_list() == ["Bob"]

    def test_case_sensitive(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df,
            filters=[TextFilter(column="name", op="contains", value="li", case_sensitive=True)],
        )
        # Only "Alice" and "Charlie" have lowercase "li"
        assert set(result["name"].to_list()) == {"Alice", "Charlie"}


class TestValueListFilter:
    def test_value_list(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[ValueListFilter(column="city", values=["NYC", "Chicago"])]
        )
        assert set(result["name"].to_list()) == {"Alice", "Charlie", "Eve"}


class TestBlanksFilter:
    def test_show_blanks(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[BlanksFilter(column="score", show_blanks=True)]
        )
        assert result["name"].to_list() == ["Eve"]

    def test_hide_blanks(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[BlanksFilter(column="score", show_blanks=False)]
        )
        assert len(result) == 4
        assert "Eve" not in result["name"].to_list()


# ────────────────────────────────────────────────────────────────
# Combined filter + sort
# ────────────────────────────────────────────────────────────────


class TestCombined:
    def test_filter_then_sort(self, sample_df: pl.DataFrame) -> None:
        """Filter first, then sort (matches Excel behavior)."""
        result = apply_view_transforms(
            sample_df,
            filters=[NumericFilter(column="age", op=">", value=28)],
            sorts=[SortSpec(column="age")],
        )
        assert result["name"].to_list() == ["Alice", "Eve", "Charlie"]

    def test_multiple_filters(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df,
            filters=[
                NumericFilter(column="age", op=">=", value=28),
                ValueListFilter(column="city", values=["NYC"]),
            ],
        )
        assert set(result["name"].to_list()) == {"Alice", "Charlie"}

    def test_no_internal_columns_leaked(self, sample_df: pl.DataFrame) -> None:
        """The __view_row_idx__ column must not appear in output."""
        result = apply_view_transforms(
            sample_df, sorts=[SortSpec(column="age")]
        )
        assert "__view_row_idx__" not in result.columns

    def test_empty_result(self, sample_df: pl.DataFrame) -> None:
        result = apply_view_transforms(
            sample_df, filters=[NumericFilter(column="age", op=">", value=100)]
        )
        assert len(result) == 0
        assert result.columns == sample_df.columns

    def test_original_not_mutated(self, sample_df: pl.DataFrame) -> None:
        """Verify that apply_view_transforms never alters the canonical table."""
        snapshot = sample_df.clone()
        _ = apply_view_transforms(
            sample_df,
            sorts=[SortSpec(column="age", descending=True)],
            filters=[NumericFilter(column="age", op=">", value=28)],
        )
        assert sample_df.equals(snapshot)
