"""Tests for fin123 template system."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from fin123.template_engine import (
    list_templates,
    scaffold_from_template,
    show_template,
)
from fin123.workbook import Workbook


# ---------------------------------------------------------------------------
# Template discovery
# ---------------------------------------------------------------------------


class TestTemplateList:
    """Test template listing and discovery."""

    @pytest.mark.pod
    def test_list_returns_bundled_templates(self) -> None:
        """list_templates() returns at least the 5 bundled templates."""
        templates = list_templates()
        names = {t["name"] for t in templates}
        assert "single_company" in names
        assert "universe_batch" in names
        assert "sql_datasheet" in names
        assert "demo_fin123" in names
        assert "plugin_example_connector" in names

    def test_list_sorted_alphabetically(self) -> None:
        """Templates are returned in alphabetical order."""
        templates = list_templates()
        names = [t["name"] for t in templates]
        assert names == sorted(names)

    def test_list_from_custom_dir(self, tmp_path: Path) -> None:
        """list_templates() works with a custom directory."""
        tpl_dir = tmp_path / "my_tpl"
        tpl_dir.mkdir()
        (tpl_dir / "template.yaml").write_text(
            yaml.dump({
                "name": "my_tpl",
                "description": "Custom template",
                "engine_compat": ">=0.1.0",
                "invariants": ["deterministic_build"],
            })
        )
        templates = list_templates(template_dir=tmp_path)
        assert len(templates) == 1
        assert templates[0]["name"] == "my_tpl"


class TestTemplateShow:
    """Test template detail display."""

    def test_show_single_company(self) -> None:
        """show_template returns metadata and file list."""
        info = show_template("single_company")
        assert info["meta"]["name"] == "single_company"
        assert "workbook.yaml" in info["files"]
        assert "fin123.yaml" in info["files"]

    def test_show_nonexistent_raises(self) -> None:
        """show_template raises FileNotFoundError for unknown template."""
        with pytest.raises(FileNotFoundError, match="not found"):
            show_template("does_not_exist")


# ---------------------------------------------------------------------------
# Scaffold — basic
# ---------------------------------------------------------------------------


class TestScaffoldBasic:
    """Test basic scaffold behavior for each template."""

    @pytest.mark.parametrize("tpl_name", [
        "single_company",
        "universe_batch",
        pytest.param("sql_datasheet", marks=pytest.mark.pod),
        "demo_fin123",
        pytest.param("plugin_example_connector", marks=pytest.mark.pod),
    ])
    def test_scaffold_creates_workbook(self, tmp_path: Path, tpl_name: str) -> None:
        """Scaffold from template creates workbook.yaml and snapshot."""
        project = tmp_path / tpl_name
        scaffold_from_template(target_dir=project, name=tpl_name)

        assert (project / "workbook.yaml").exists()
        assert (project / "fin123.yaml").exists()
        # template.yaml must be removed
        assert not (project / "template.yaml").exists()
        # model_id must be present
        spec = yaml.safe_load((project / "workbook.yaml").read_text())
        assert spec.get("model_id")
        # Initial snapshot v0001 must exist
        assert (project / "snapshots" / "workbook" / "v0001" / "workbook.yaml").exists()

    @pytest.mark.parametrize("tpl_name", [
        "single_company",
        "universe_batch",
        pytest.param("sql_datasheet", marks=pytest.mark.pod),
        "demo_fin123",
        pytest.param("plugin_example_connector", marks=pytest.mark.pod),
    ])
    def test_scaffold_and_build_succeeds(self, tmp_path: Path, tpl_name: str) -> None:
        """Scaffold + build produces outputs without errors."""
        project = tmp_path / tpl_name
        scaffold_from_template(target_dir=project, name=tpl_name)

        wb = Workbook(project)
        result = wb.run()
        assert result.run_dir.exists()
        assert (result.run_dir / "run_meta.json").exists()
        assert (result.run_dir / "outputs" / "scalars.json").exists()

    def test_scaffold_rejects_existing_workbook(self, tmp_path: Path) -> None:
        """Scaffold fails if workbook.yaml already exists."""
        project = tmp_path / "existing"
        project.mkdir()
        (project / "workbook.yaml").write_text("version: 1\n")

        with pytest.raises(FileExistsError):
            scaffold_from_template(target_dir=project, name="single_company")


# ---------------------------------------------------------------------------
# Scaffold — single_company specifics
# ---------------------------------------------------------------------------


class TestSingleCompany:
    """Test single_company template specifics."""

    def test_assertions_pass(self, tmp_path: Path) -> None:
        """Build with default params passes all assertions."""
        project = tmp_path / "sc"
        scaffold_from_template(target_dir=project, name="single_company")

        wb = Workbook(project)
        result = wb.run()
        meta = json.loads((result.run_dir / "run_meta.json").read_text())
        assert meta.get("assertions_status") == "pass"
        assert meta.get("assertions_failed_count", 0) == 0

    def test_deterministic_build(self, tmp_path: Path) -> None:
        """Two consecutive builds produce identical export_hash."""
        project = tmp_path / "sc"
        scaffold_from_template(target_dir=project, name="single_company")

        wb1 = Workbook(project)
        r1 = wb1.run()
        m1 = json.loads((r1.run_dir / "run_meta.json").read_text())

        wb2 = Workbook(project)
        r2 = wb2.run()
        m2 = json.loads((r2.run_dir / "run_meta.json").read_text())

        assert m1["export_hash"] == m2["export_hash"]


# ---------------------------------------------------------------------------
# Scaffold — sql_datasheet specifics
# ---------------------------------------------------------------------------


@pytest.mark.pod
class TestSqlDatasheet:
    """Test sql_datasheet template specifics."""

    def test_seed_parquet_has_expected_columns(self, tmp_path: Path) -> None:
        """Seed parquet matches expected_columns in workbook.yaml."""
        import polars as pl

        project = tmp_path / "sd"
        scaffold_from_template(target_dir=project, name="sql_datasheet")

        spec = yaml.safe_load((project / "workbook.yaml").read_text())
        expected = spec["tables"]["estimates"]["expected_columns"]
        df = pl.read_parquet(project / "inputs" / "estimates.parquet")
        assert set(expected).issubset(set(df.columns))


# ---------------------------------------------------------------------------
# Substitution
# ---------------------------------------------------------------------------


class TestSubstitution:
    """Test placeholder substitution logic."""

    def test_set_company_name(self, tmp_path: Path) -> None:
        """--set company_name=TSLA results in TSLA appearing in workbook.yaml."""
        project = tmp_path / "sub"
        scaffold_from_template(
            target_dir=project,
            name="single_company",
            overrides={"company_name": "TSLA"},
        )

        spec = yaml.safe_load((project / "workbook.yaml").read_text())
        assert spec["params"]["ticker"] == "TSLA"

    def test_set_multiple_params(self, tmp_path: Path) -> None:
        """Multiple --set params are all applied."""
        project = tmp_path / "sub2"
        scaffold_from_template(
            target_dir=project,
            name="single_company",
            overrides={"company_name": "GOOG", "currency": "EUR"},
        )

        spec = yaml.safe_load((project / "workbook.yaml").read_text())
        assert spec["params"]["ticker"] == "GOOG"
        assert spec["params"]["currency"] == "EUR"

    def test_unknown_set_key_errors(self, tmp_path: Path) -> None:
        """--set with an unknown key raises ValueError."""
        project = tmp_path / "bad"
        with pytest.raises(ValueError, match="Unknown template parameter"):
            scaffold_from_template(
                target_dir=project,
                name="single_company",
                overrides={"nonexistent_key": "value"},
            )

    def test_number_type_validation(self, tmp_path: Path) -> None:
        """--set for a number param with non-numeric value raises ValueError."""
        # Create a template with a number param
        tpl = tmp_path / "templates" / "num_tpl"
        tpl.mkdir(parents=True)
        (tpl / "template.yaml").write_text(yaml.dump({
            "name": "num_tpl",
            "description": "Template with number param",
            "engine_compat": ">=0.1.0",
            "invariants": ["deterministic_build"],
            "params": {
                "rate": {"type": "number", "default": 0.05, "description": "Rate"},
            },
        }))
        (tpl / "workbook.yaml").write_text('version: 1\nparams:\n  rate: "{{rate}}"\n')
        (tpl / "fin123.yaml").write_text("max_runs: 50\n")

        project = tmp_path / "p"
        with pytest.raises(ValueError, match="expects a number"):
            scaffold_from_template(
                target_dir=project,
                template_dir=tpl,
                overrides={"rate": "not_a_number"},
            )

    def test_placeholder_outside_quotes_errors(self, tmp_path: Path) -> None:
        """Placeholder outside double-quoted YAML scalar raises with file and line."""
        tpl = tmp_path / "templates" / "bad_tpl"
        tpl.mkdir(parents=True)
        (tpl / "template.yaml").write_text(yaml.dump({
            "name": "bad_tpl",
            "description": "Bad template",
            "engine_compat": ">=0.1.0",
            "invariants": ["deterministic_build"],
            "params": {
                "name": {"type": "string", "default": "test", "description": "Name"},
            },
        }))
        # Placeholder NOT inside double quotes
        (tpl / "workbook.yaml").write_text(
            "version: 1\n"
            "params:\n"
            "  ticker: {{name}}\n"  # line 3 — unquoted!
        )
        (tpl / "fin123.yaml").write_text("max_runs: 50\n")

        project = tmp_path / "p"
        with pytest.raises(ValueError, match=r"workbook\.yaml:3.*not inside a double-quoted"):
            scaffold_from_template(
                target_dir=project,
                template_dir=tpl,
                overrides={"name": "test"},
            )

    def test_default_params_used_when_no_overrides(self, tmp_path: Path) -> None:
        """Template defaults are applied when --set is not provided."""
        project = tmp_path / "defaults"
        scaffold_from_template(target_dir=project, name="single_company")

        spec = yaml.safe_load((project / "workbook.yaml").read_text())
        # Default company_name is ACME
        assert spec["params"]["ticker"] == "ACME"
        assert spec["params"]["currency"] == "USD"

    @pytest.mark.pod
    def test_template_without_params_scaffolds(self, tmp_path: Path) -> None:
        """Template with no params section scaffolds cleanly."""
        project = tmp_path / "sd"
        scaffold_from_template(target_dir=project, name="sql_datasheet")

        assert (project / "workbook.yaml").exists()
        assert not (project / "template.yaml").exists()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.mark.pod
class TestTemplateCLI:
    """Test template CLI commands."""

    def test_cli_template_list(self) -> None:
        """fin123 template list shows bundled templates."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["template", "list"])
        assert result.exit_code == 0
        assert "single_company" in result.output
        assert "universe_batch" in result.output
        assert "sql_datasheet" in result.output
        assert "demo_fin123" in result.output
        assert "plugin_example_connector" in result.output

    def test_cli_template_list_json(self) -> None:
        """fin123 template list --json returns valid JSON."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["template", "list", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) >= 5
        names = {t["name"] for t in data}
        assert "single_company" in names

    def test_cli_template_show(self) -> None:
        """fin123 template show single_company prints details."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        result = runner.invoke(main, ["template", "show", "single_company"])
        assert result.exit_code == 0
        assert "single_company" in result.output
        assert "workbook.yaml" in result.output

    def test_cli_new_with_template(self, tmp_path: Path) -> None:
        """fin123 new --template single_company scaffolds a buildable project."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        project = tmp_path / "cli_tpl"
        result = runner.invoke(main, ["new", str(project), "--template", "single_company"])
        assert result.exit_code == 0, result.output
        assert (project / "workbook.yaml").exists()

    def test_cli_new_with_set(self, tmp_path: Path) -> None:
        """fin123 new --template --set key=value applies substitution."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        project = tmp_path / "cli_set"
        result = runner.invoke(main, [
            "new", str(project),
            "--template", "single_company",
            "--set", "company_name=NVDA",
        ])
        assert result.exit_code == 0, result.output
        spec = yaml.safe_load((project / "workbook.yaml").read_text())
        assert spec["params"]["ticker"] == "NVDA"

    def test_cli_new_without_template_unchanged(self, tmp_path: Path) -> None:
        """fin123 new without --template uses existing demo scaffold."""
        from click.testing import CliRunner
        from fin123.cli import main

        runner = CliRunner()
        project = tmp_path / "demo"
        result = runner.invoke(main, ["new", str(project)])
        assert result.exit_code == 0
        assert "Created project at" in result.output
        # Verify it's the demo project, not a template
        spec = yaml.safe_load((project / "workbook.yaml").read_text())
        assert "prices" in spec.get("tables", {})
