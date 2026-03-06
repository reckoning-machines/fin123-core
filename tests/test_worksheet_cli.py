"""Tests for worksheet CLI commands (Stage 5)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
import yaml
from click.testing import CliRunner

from fin123.cli_core import EXIT_ERROR, EXIT_OK, EXIT_VERIFY_FAIL, main

# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────


def _make_run_dir(project_dir: Path, table_name: str, df: pl.DataFrame, run_id: str = "run_001") -> Path:
    """Scaffold a minimal fake run directory with a parquet table."""
    run_dir = project_dir / "runs" / run_id
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True)
    # Write run_meta.json so it's recognized as a completed run
    (run_dir / "run_meta.json").write_text(json.dumps({"run_id": run_id}))
    df.write_parquet(outputs_dir / f"{table_name}.parquet")
    return run_dir


def _make_spec_file(project_dir: Path, spec: dict, filename: str = "test_ws.yaml") -> Path:
    """Write a worksheet spec YAML file."""
    ws_dir = project_dir / "worksheets"
    ws_dir.mkdir(exist_ok=True)
    spec_path = ws_dir / filename
    spec_path.write_text(yaml.dump(spec))
    return spec_path


def _make_compiled_artifact(path: Path, **overrides) -> Path:
    """Create a valid compiled worksheet JSON artifact."""
    from fin123.worksheet.compiled import CompiledWorksheet
    from fin123.worksheet.compiler import compile_worksheet
    from fin123.worksheet.spec import parse_worksheet_view
    from fin123.worksheet.types import ColumnSchema, ColumnType
    from fin123.worksheet.view_table import from_polars

    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "revenue": [100.0, 200.0, 50.0],
    })
    schema = [
        ColumnSchema(name="id", dtype=ColumnType.INT64),
        ColumnSchema(name="name", dtype=ColumnType.STRING),
        ColumnSchema(name="revenue", dtype=ColumnType.FLOAT64),
    ]
    vt = from_polars(df, schema, row_key="id", source_label="test")
    spec_dict = overrides.pop("spec_dict", {
        "name": "test_artifact",
        "columns": [
            {"source": "name"},
            {"source": "revenue"},
        ],
    })
    spec = parse_worksheet_view(spec_dict)
    ws = compile_worksheet(vt, spec, compiled_at="2025-06-15T12:00:00+00:00")
    ws.to_file(path)
    return path


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner(mix_stderr=False)


@pytest.fixture
def project_with_run(tmp_path: Path) -> tuple[Path, Path]:
    """Create a project directory with a run containing a test table."""
    project = tmp_path / "proj"
    project.mkdir()
    df = pl.DataFrame({
        "ticker": ["AAPL", "MSFT", "GOOG"],
        "px_last": [180.0, 370.0, 175.0],
        "eps_ntm": [7.1, 12.5, 6.8],
    })
    _make_run_dir(project, "priced_estimates", df)

    spec = {
        "name": "valuation",
        "title": "Valuation Review",
        "columns": [
            {"source": "ticker", "label": "Ticker"},
            {"source": "px_last", "label": "Price"},
            {"source": "eps_ntm", "label": "EPS"},
            {"name": "pe_ratio", "expression": "px_last / eps_ntm", "label": "P/E"},
        ],
        "sorts": [{"column": "pe_ratio"}],
    }
    spec_path = _make_spec_file(project, spec)
    return project, spec_path


# ────────────────────────────────────────────────────────────────
# worksheet compile
# ────────────────────────────────────────────────────────────────


class TestWorksheetCompile:
    def test_compile_to_output_file(self, runner: CliRunner, project_with_run: tuple[Path, Path]) -> None:
        project, spec_path = project_with_run
        output = project / "out.worksheet.json"

        result = runner.invoke(main, [
            "worksheet", "compile", str(spec_path),
            "--table", "priced_estimates",
            "--project", str(project),
            "--output", str(output),
        ])
        assert result.exit_code == EXIT_OK, result.output + (result.stderr or "")
        assert output.exists()

        # Verify the artifact is valid
        from fin123.worksheet.compiled import CompiledWorksheet
        ws = CompiledWorksheet.from_file(output)
        assert ws.name == "valuation"
        assert len(ws.rows) == 3
        assert len(ws.columns) == 4  # ticker, px_last, eps_ntm, pe_ratio

    def test_compile_default_output_path(self, runner: CliRunner, project_with_run: tuple[Path, Path]) -> None:
        project, spec_path = project_with_run

        result = runner.invoke(main, [
            "worksheet", "compile", str(spec_path),
            "--table", "priced_estimates",
            "--project", str(project),
        ])
        assert result.exit_code == EXIT_OK, result.output + (result.stderr or "")
        default_path = project / "valuation.worksheet.json"
        assert default_path.exists()

    def test_compile_json_mode(self, runner: CliRunner, project_with_run: tuple[Path, Path]) -> None:
        project, spec_path = project_with_run
        output = project / "out.json"

        result = runner.invoke(main, [
            "--json", "worksheet", "compile", str(spec_path),
            "--table", "priced_estimates",
            "--project", str(project),
            "--output", str(output),
        ])
        assert result.exit_code == EXIT_OK, result.output + (result.stderr or "")

        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["cmd"] == "worksheet compile"
        assert envelope["data"]["name"] == "valuation"
        assert envelope["data"]["row_count"] == 3
        assert envelope["data"]["column_count"] == 4

    def test_compile_bad_table_name(self, runner: CliRunner, project_with_run: tuple[Path, Path]) -> None:
        project, spec_path = project_with_run

        result = runner.invoke(main, [
            "worksheet", "compile", str(spec_path),
            "--table", "nonexistent_table",
            "--project", str(project),
        ])
        assert result.exit_code == EXIT_ERROR

    def test_compile_bad_spec(self, runner: CliRunner, tmp_path: Path) -> None:
        project = tmp_path / "proj"
        project.mkdir()
        df = pl.DataFrame({"x": [1.0]})
        _make_run_dir(project, "t", df)

        bad_spec = project / "worksheets" / "bad.yaml"
        bad_spec.parent.mkdir()
        bad_spec.write_text("not: valid: yaml: [[[")

        result = runner.invoke(main, [
            "worksheet", "compile", str(bad_spec),
            "--table", "t",
            "--project", str(project),
        ])
        assert result.exit_code == EXIT_ERROR

    def test_compile_bad_spec_json_mode(self, runner: CliRunner, tmp_path: Path) -> None:
        project = tmp_path / "proj"
        project.mkdir()
        df = pl.DataFrame({"x": [1.0]})
        _make_run_dir(project, "t", df)

        # Spec that references a column not in the table
        spec = {"name": "bad", "columns": [{"source": "nonexistent"}]}
        spec_path = _make_spec_file(project, spec)

        result = runner.invoke(main, [
            "--json", "worksheet", "compile", str(spec_path),
            "--table", "t",
            "--project", str(project),
        ])
        assert result.exit_code == EXIT_ERROR
        envelope = json.loads(result.output)
        assert envelope["ok"] is False

    def test_compile_quiet_mode(self, runner: CliRunner, project_with_run: tuple[Path, Path]) -> None:
        project, spec_path = project_with_run
        output = project / "out.json"

        result = runner.invoke(main, [
            "--quiet", "worksheet", "compile", str(spec_path),
            "--table", "priced_estimates",
            "--project", str(project),
            "--output", str(output),
        ])
        assert result.exit_code == EXIT_OK
        assert result.output.strip() == ""
        assert output.exists()

    def test_compile_with_run_id(self, runner: CliRunner, tmp_path: Path) -> None:
        project = tmp_path / "proj"
        project.mkdir()
        df = pl.DataFrame({"val": [1.0, 2.0]})
        _make_run_dir(project, "t", df, run_id="run_specific")

        spec = {"name": "s", "columns": [{"source": "val"}]}
        spec_path = _make_spec_file(project, spec)

        result = runner.invoke(main, [
            "worksheet", "compile", str(spec_path),
            "--table", "t",
            "--project", str(project),
            "--run", "run_specific",
            "--output", str(project / "out.json"),
        ])
        assert result.exit_code == EXIT_OK


# ────────────────────────────────────────────────────────────────
# worksheet verify
# ────────────────────────────────────────────────────────────────


class TestWorksheetVerify:
    def test_verify_valid_artifact(self, runner: CliRunner, tmp_path: Path) -> None:
        artifact_path = tmp_path / "test.worksheet.json"
        _make_compiled_artifact(artifact_path)

        result = runner.invoke(main, ["worksheet", "verify", str(artifact_path)])
        assert result.exit_code == EXIT_OK
        assert "PASS" in result.output

    def test_verify_valid_json_mode(self, runner: CliRunner, tmp_path: Path) -> None:
        artifact_path = tmp_path / "test.worksheet.json"
        _make_compiled_artifact(artifact_path)

        result = runner.invoke(main, ["--json", "worksheet", "verify", str(artifact_path)])
        assert result.exit_code == EXIT_OK
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"]["status"] == "pass"
        checks = envelope["data"]["checks"]
        assert all(c["status"] == "pass" for c in checks)

    def test_verify_check_names(self, runner: CliRunner, tmp_path: Path) -> None:
        """Verify reports all expected check names."""
        artifact_path = tmp_path / "test.worksheet.json"
        _make_compiled_artifact(artifact_path)

        result = runner.invoke(main, ["--json", "worksheet", "verify", str(artifact_path)])
        envelope = json.loads(result.output)
        check_names = [c["check"] for c in envelope["data"]["checks"]]
        assert "parse" in check_names
        assert "provenance" in check_names
        assert "column_count" in check_names
        assert "row_count" in check_names
        assert "content_roundtrip" in check_names

    def test_verify_invalid_json(self, runner: CliRunner, tmp_path: Path) -> None:
        bad_path = tmp_path / "bad.json"
        bad_path.write_text("not valid json {{{")

        result = runner.invoke(main, ["worksheet", "verify", str(bad_path)])
        assert result.exit_code == EXIT_VERIFY_FAIL

    def test_verify_invalid_json_json_mode(self, runner: CliRunner, tmp_path: Path) -> None:
        bad_path = tmp_path / "bad.json"
        bad_path.write_text("not valid json {{{")

        result = runner.invoke(main, ["--json", "worksheet", "verify", str(bad_path)])
        assert result.exit_code == EXIT_VERIFY_FAIL
        envelope = json.loads(result.output)
        assert envelope["ok"] is False
        assert envelope["data"]["status"] == "fail"

    def test_verify_tampered_row_count(self, runner: CliRunner, tmp_path: Path) -> None:
        """Tampered provenance row_count should fail verification."""
        artifact_path = tmp_path / "tampered.json"
        _make_compiled_artifact(artifact_path)

        # Tamper with provenance
        data = json.loads(artifact_path.read_text())
        data["provenance"]["row_count"] = 999
        artifact_path.write_text(json.dumps(data))

        result = runner.invoke(main, ["--json", "worksheet", "verify", str(artifact_path)])
        assert result.exit_code == EXIT_VERIFY_FAIL
        envelope = json.loads(result.output)
        row_check = next(c for c in envelope["data"]["checks"] if c["check"] == "row_count")
        assert row_check["status"] == "fail"

    def test_verify_tampered_column_count(self, runner: CliRunner, tmp_path: Path) -> None:
        """Tampered provenance column_count should fail verification."""
        artifact_path = tmp_path / "tampered.json"
        _make_compiled_artifact(artifact_path)

        data = json.loads(artifact_path.read_text())
        data["provenance"]["column_count"] = 999
        artifact_path.write_text(json.dumps(data))

        result = runner.invoke(main, ["--json", "worksheet", "verify", str(artifact_path)])
        assert result.exit_code == EXIT_VERIFY_FAIL
        envelope = json.loads(result.output)
        col_check = next(c for c in envelope["data"]["checks"] if c["check"] == "column_count")
        assert col_check["status"] == "fail"

    def test_verify_human_output_shows_checks(self, runner: CliRunner, tmp_path: Path) -> None:
        artifact_path = tmp_path / "test.worksheet.json"
        _make_compiled_artifact(artifact_path)

        result = runner.invoke(main, ["worksheet", "verify", str(artifact_path)])
        assert result.exit_code == EXIT_OK
        # Human output should show check marks
        assert "parse" in result.output
        assert "provenance" in result.output


# ────────────────────────────────────────────────────────────────
# worksheet diff
# ────────────────────────────────────────────────────────────────


class TestWorksheetDiff:
    def test_diff_identical(self, runner: CliRunner, tmp_path: Path) -> None:
        left = tmp_path / "left.json"
        right = tmp_path / "right.json"
        _make_compiled_artifact(left)
        _make_compiled_artifact(right)

        result = runner.invoke(main, ["worksheet", "diff", str(left), str(right)])
        assert result.exit_code == EXIT_OK
        assert "IDENTICAL" in result.output

    def test_diff_identical_json_mode(self, runner: CliRunner, tmp_path: Path) -> None:
        left = tmp_path / "left.json"
        right = tmp_path / "right.json"
        _make_compiled_artifact(left)
        _make_compiled_artifact(right)

        result = runner.invoke(main, ["--json", "worksheet", "diff", str(left), str(right)])
        assert result.exit_code == EXIT_OK
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert envelope["data"]["content_identical"] is True
        assert envelope["data"]["changed_rows"] == 0
        assert envelope["data"]["changed_cells"] == 0

    def test_diff_different_data(self, runner: CliRunner, tmp_path: Path) -> None:
        from fin123.worksheet.compiled import CompiledWorksheet
        from fin123.worksheet.compiler import compile_worksheet
        from fin123.worksheet.spec import parse_worksheet_view
        from fin123.worksheet.types import ColumnSchema, ColumnType
        from fin123.worksheet.view_table import from_polars

        spec_dict = {
            "name": "test",
            "columns": [{"source": "name"}, {"source": "val"}],
        }
        spec = parse_worksheet_view(spec_dict)

        df1 = pl.DataFrame({"name": ["A", "B"], "val": [10.0, 20.0]})
        df2 = pl.DataFrame({"name": ["A", "B"], "val": [10.0, 99.0]})
        schema = [
            ColumnSchema(name="name", dtype=ColumnType.STRING),
            ColumnSchema(name="val", dtype=ColumnType.FLOAT64),
        ]

        vt1 = from_polars(df1, schema, source_label="v1")
        vt2 = from_polars(df2, schema, source_label="v2")

        ws1 = compile_worksheet(vt1, spec, compiled_at="2025-01-01T00:00:00")
        ws2 = compile_worksheet(vt2, spec, compiled_at="2025-01-01T00:00:00")

        left = tmp_path / "left.json"
        right = tmp_path / "right.json"
        ws1.to_file(left)
        ws2.to_file(right)

        result = runner.invoke(main, ["--json", "worksheet", "diff", str(left), str(right)])
        assert result.exit_code == EXIT_OK
        envelope = json.loads(result.output)
        assert envelope["data"]["content_identical"] is False
        assert envelope["data"]["changed_rows"] >= 1
        assert envelope["data"]["changed_cells"] >= 1

    def test_diff_column_changes(self, runner: CliRunner, tmp_path: Path) -> None:
        from fin123.worksheet.compiler import compile_worksheet
        from fin123.worksheet.spec import parse_worksheet_view
        from fin123.worksheet.types import ColumnSchema, ColumnType
        from fin123.worksheet.view_table import from_polars

        df = pl.DataFrame({"a": [1.0], "b": [2.0], "c": [3.0]})
        schema = [
            ColumnSchema(name="a", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="b", dtype=ColumnType.FLOAT64),
            ColumnSchema(name="c", dtype=ColumnType.FLOAT64),
        ]
        vt = from_polars(df, schema, source_label="t")

        spec1 = parse_worksheet_view({"name": "t", "columns": [{"source": "a"}, {"source": "b"}]})
        spec2 = parse_worksheet_view({"name": "t", "columns": [{"source": "a"}, {"source": "c"}]})

        ws1 = compile_worksheet(vt, spec1, compiled_at="2025-01-01T00:00:00")
        ws2 = compile_worksheet(vt, spec2, compiled_at="2025-01-01T00:00:00")

        left = tmp_path / "left.json"
        right = tmp_path / "right.json"
        ws1.to_file(left)
        ws2.to_file(right)

        result = runner.invoke(main, ["--json", "worksheet", "diff", str(left), str(right)])
        envelope = json.loads(result.output)
        assert "b" in envelope["data"]["columns_removed"]
        assert "c" in envelope["data"]["columns_added"]

    def test_diff_row_key_identity_mode(self, runner: CliRunner, tmp_path: Path) -> None:
        from fin123.worksheet.compiler import compile_worksheet
        from fin123.worksheet.spec import parse_worksheet_view
        from fin123.worksheet.types import ColumnSchema, ColumnType
        from fin123.worksheet.view_table import from_polars

        spec_dict = {"name": "t", "columns": [{"source": "id"}, {"source": "val"}]}
        spec = parse_worksheet_view(spec_dict)
        schema = [
            ColumnSchema(name="id", dtype=ColumnType.INT64),
            ColumnSchema(name="val", dtype=ColumnType.FLOAT64),
        ]

        df1 = pl.DataFrame({"id": [1, 2], "val": [10.0, 20.0]})
        df2 = pl.DataFrame({"id": [1, 2], "val": [10.0, 30.0]})

        vt1 = from_polars(df1, schema, row_key="id", source_label="v1")
        vt2 = from_polars(df2, schema, row_key="id", source_label="v2")

        ws1 = compile_worksheet(vt1, spec, compiled_at="2025-01-01T00:00:00")
        ws2 = compile_worksheet(vt2, spec, compiled_at="2025-01-01T00:00:00")

        left = tmp_path / "left.json"
        right = tmp_path / "right.json"
        ws1.to_file(left)
        ws2.to_file(right)

        result = runner.invoke(main, ["--json", "worksheet", "diff", str(left), str(right)])
        envelope = json.loads(result.output)
        assert envelope["data"]["identity_mode"] == "row_key"
        assert envelope["data"]["changed_rows"] == 1

    def test_diff_human_format(self, runner: CliRunner, tmp_path: Path) -> None:
        left = tmp_path / "left.json"
        right = tmp_path / "right.json"
        _make_compiled_artifact(left)
        _make_compiled_artifact(right)

        result = runner.invoke(main, ["worksheet", "diff", str(left), str(right)])
        assert result.exit_code == EXIT_OK
        assert "diff" in result.output.lower() or "IDENTICAL" in result.output

    def test_diff_bad_file(self, runner: CliRunner, tmp_path: Path) -> None:
        left = tmp_path / "left.json"
        _make_compiled_artifact(left)
        bad = tmp_path / "bad.json"
        bad.write_text("not json")

        result = runner.invoke(main, ["worksheet", "diff", str(left), str(bad)])
        assert result.exit_code == EXIT_ERROR


# ────────────────────────────────────────────────────────────────
# worksheet list
# ────────────────────────────────────────────────────────────────


class TestWorksheetList:
    def test_list_no_worksheets_dir(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(main, ["worksheet", "list", "--project", str(tmp_path)])
        assert result.exit_code == EXIT_OK
        assert "No worksheet specs" in result.output

    def test_list_empty_worksheets_dir(self, runner: CliRunner, tmp_path: Path) -> None:
        (tmp_path / "worksheets").mkdir()
        result = runner.invoke(main, ["worksheet", "list", "--project", str(tmp_path)])
        assert result.exit_code == EXIT_OK
        assert "No worksheet specs" in result.output

    def test_list_with_specs(self, runner: CliRunner, tmp_path: Path) -> None:
        spec1 = {"name": "margins", "title": "Margin Report", "columns": [{"source": "a"}]}
        spec2 = {"name": "valuation", "title": "Val Review", "columns": [{"source": "b"}, {"source": "c"}]}
        _make_spec_file(tmp_path, spec1, "margins.yaml")
        _make_spec_file(tmp_path, spec2, "valuation.yaml")

        result = runner.invoke(main, ["worksheet", "list", "--project", str(tmp_path)])
        assert result.exit_code == EXIT_OK
        assert "margins" in result.output
        assert "valuation" in result.output

    def test_list_json_mode(self, runner: CliRunner, tmp_path: Path) -> None:
        spec = {"name": "test_ws", "title": "Test", "columns": [{"source": "x"}]}
        _make_spec_file(tmp_path, spec, "test_ws.yaml")

        result = runner.invoke(main, ["--json", "worksheet", "list", "--project", str(tmp_path)])
        assert result.exit_code == EXIT_OK
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        specs = envelope["data"]["specs"]
        assert len(specs) == 1
        assert specs[0]["name"] == "test_ws"
        assert specs[0]["columns"] == 1

    def test_list_invalid_spec_reported(self, runner: CliRunner, tmp_path: Path) -> None:
        """Invalid YAML specs are listed with name='?' instead of crashing."""
        ws_dir = tmp_path / "worksheets"
        ws_dir.mkdir()
        (ws_dir / "bad.yaml").write_text("not: [a valid spec")

        result = runner.invoke(main, ["--json", "worksheet", "list", "--project", str(tmp_path)])
        assert result.exit_code == EXIT_OK
        envelope = json.loads(result.output)
        specs = envelope["data"]["specs"]
        assert len(specs) == 1
        assert specs[0]["name"] == "?"

    def test_list_no_worksheets_json_mode(self, runner: CliRunner, tmp_path: Path) -> None:
        result = runner.invoke(main, ["--json", "worksheet", "list", "--project", str(tmp_path)])
        assert result.exit_code == EXIT_OK
        envelope = json.loads(result.output)
        assert envelope["data"]["specs"] == []


# ────────────────────────────────────────────────────────────────
# Integration: compile → verify round-trip
# ────────────────────────────────────────────────────────────────


class TestCompileVerifyRoundtrip:
    def test_compile_then_verify(self, runner: CliRunner, project_with_run: tuple[Path, Path]) -> None:
        """Compiled artifact passes verification."""
        project, spec_path = project_with_run
        output = project / "compiled.worksheet.json"

        # Compile
        result = runner.invoke(main, [
            "worksheet", "compile", str(spec_path),
            "--table", "priced_estimates",
            "--project", str(project),
            "--output", str(output),
        ])
        assert result.exit_code == EXIT_OK

        # Verify
        result = runner.invoke(main, ["--json", "worksheet", "verify", str(output)])
        assert result.exit_code == EXIT_OK
        envelope = json.loads(result.output)
        assert envelope["ok"] is True
        assert all(c["status"] == "pass" for c in envelope["data"]["checks"])

    def test_compile_then_diff_with_self(self, runner: CliRunner, project_with_run: tuple[Path, Path]) -> None:
        """Diffing a compiled artifact with itself reports identical."""
        project, spec_path = project_with_run
        output = project / "compiled.worksheet.json"

        runner.invoke(main, [
            "worksheet", "compile", str(spec_path),
            "--table", "priced_estimates",
            "--project", str(project),
            "--output", str(output),
        ])

        result = runner.invoke(main, ["--json", "worksheet", "diff", str(output), str(output)])
        assert result.exit_code == EXIT_OK
        envelope = json.loads(result.output)
        assert envelope["data"]["content_identical"] is True
