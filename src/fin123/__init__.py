"""fin123 -- Polars-backed financial workbook engine with native versioning."""

__version__ = "0.5.1"

# Stable API version for core↔pod handshake.
# Bump only when the core↔pod interface contract changes.
__core_api_version__ = "0.3"


def _check_namespace_overlap() -> None:
    """Warn if both fin123-core and fin123-pod are installed in the same venv.

    This guard runs at import time so that users get early visibility
    into namespace overlap, rather than encountering confusing import
    errors later. The warning is non-fatal.
    """
    import importlib.metadata
    import warnings

    try:
        importlib.metadata.version("fin123-core")
        importlib.metadata.version("fin123-pod")
    except importlib.metadata.PackageNotFoundError:
        return  # Only one (or neither) is installed -- no overlap.

    warnings.warn(
        "Both fin123-core and fin123-pod are installed in this environment. "
        "The 'fin123' namespace is shared and import shadowing may occur. "
        "Use separate virtualenvs for isolated evaluation. "
        "Run 'fin123 doctor --environment' for details.",
        stacklevel=2,
    )


_check_namespace_overlap()
