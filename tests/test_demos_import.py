"""Smoke tests: fin123.demos is importable and CLI demo group loads."""

from __future__ import annotations

import subprocess
import sys

from click.testing import CliRunner

from fin123.cli_core import main


def test_import_fin123_demos() -> None:
    """fin123.demos is importable as a proper package."""
    import fin123.demos

    assert hasattr(fin123.demos, "__name__")
    assert fin123.demos.__name__ == "fin123.demos"


def test_demo_help_runs_without_error() -> None:
    """``fin123 demo --help`` exits 0 with no ModuleNotFoundError."""
    runner = CliRunner()
    result = runner.invoke(main, ["demo", "--help"])
    assert result.exit_code == 0, result.output
    assert "ai-governance" in result.output


def test_demo_help_subprocess() -> None:
    """Console-script entrypoint resolves in a real subprocess."""
    result = subprocess.run(
        [sys.executable, "-c",
         "from fin123.cli_core import main; main()",
         "demo", "--help"],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "ai-governance" in result.stdout
