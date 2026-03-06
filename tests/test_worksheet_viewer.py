"""Python wrapper to run worksheet_viewer.js DOM tests via pytest."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture
def node_available() -> bool:
    try:
        result = subprocess.run(
            ["node", "--version"], capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


class TestWorksheetViewerDOM:
    def test_dom_assertions(self, node_available: bool) -> None:
        if not node_available:
            pytest.skip("Node.js not available")

        test_script = Path(__file__).parent / "test_worksheet_viewer_dom.js"
        assert test_script.exists(), f"DOM test script not found: {test_script}"

        result = subprocess.run(
            ["node", str(test_script)],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(Path(__file__).parent.parent),
        )

        if result.returncode != 0:
            # Include both stdout and stderr for diagnostics
            output = result.stdout + "\n" + result.stderr
            pytest.fail(f"DOM tests failed:\n{output}")

        # Verify test count in output
        assert "passed" in result.stdout
        assert "0 failed" in result.stdout
