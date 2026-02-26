"""Central registry for scalar and table functions."""

from __future__ import annotations

from typing import Any, Callable


_SCALAR_FUNCTIONS: dict[str, Callable[..., Any]] = {}
_TABLE_FUNCTIONS: dict[str, Callable[..., Any]] = {}


def register_scalar(name: str) -> Callable:
    """Decorator that registers a scalar function by name.

    Args:
        name: The lookup name for this function.

    Returns:
        The original function, unmodified.
    """

    def decorator(fn: Callable) -> Callable:
        _SCALAR_FUNCTIONS[name] = fn
        return fn

    return decorator


def register_table(name: str) -> Callable:
    """Decorator that registers a table function by name.

    Args:
        name: The lookup name for this function.

    Returns:
        The original function, unmodified.
    """

    def decorator(fn: Callable) -> Callable:
        _TABLE_FUNCTIONS[name] = fn
        return fn

    return decorator


def get_scalar_fn(name: str) -> Callable:
    """Look up a registered scalar function.

    Args:
        name: The function name.

    Returns:
        The callable.

    Raises:
        KeyError: If no function is registered under *name*.
    """
    if name not in _SCALAR_FUNCTIONS:
        raise KeyError(f"Unknown scalar function: {name!r}")
    return _SCALAR_FUNCTIONS[name]


def get_table_fn(name: str) -> Callable:
    """Look up a registered table function.

    Args:
        name: The function name.

    Returns:
        The callable.

    Raises:
        KeyError: If no function is registered under *name*.
    """
    if name not in _TABLE_FUNCTIONS:
        raise KeyError(f"Unknown table function: {name!r}")
    return _TABLE_FUNCTIONS[name]
