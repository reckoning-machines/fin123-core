# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for fin123-core single-file CLI binary."""

import os
import sys
from pathlib import Path

block_cipher = None

# Resolve paths relative to this spec file
SPEC_DIR = Path(SPECPATH)
REPO_ROOT = SPEC_DIR.parent
SRC_DIR = REPO_ROOT / "src"

a = Analysis(
    [str(SRC_DIR / "fin123" / "cli_core.py")],
    pathex=[str(SRC_DIR)],
    binaries=[],
    datas=[
        (str(SRC_DIR / "fin123" / "ui" / "static"), "fin123/ui/static"),
        (str(SRC_DIR / "fin123" / "templates"), "fin123/templates"),
        (str(SRC_DIR / "fin123" / "demos"), "fin123/demos"),
    ],
    hiddenimports=[
        "fin123",
        "fin123.cli_core",
        "fin123.workbook",
        "fin123.project",
        "fin123.template_engine",
        "fin123.versioning",
        "fin123.batch",
        "fin123.diff",
        "fin123.verify",
        "fin123.gc",
        "fin123.xlsx_import",
        "fin123.formulas",
        "fin123.formulas.parser",
        "fin123.formulas.evaluator",
        "fin123.formulas.errors",
        # Formula function modules (lazy-imported by evaluator.py)
        "fin123.formulas.fn_logical",
        "fin123.formulas.fn_error",
        "fin123.formulas.fn_date",
        "fin123.formulas.fn_lookup",
        "fin123.formulas.fn_finance",
        "fin123.functions",
        "fin123.functions.registry",
        "fin123.functions.scalar",
        "fin123.functions.table",
        "fin123.logging",
        "fin123.logging.events",
        "fin123.logging.sink",
        "fin123.ui",
        "fin123.ui.server",
        "fin123.ui.service",
        "fin123.ui.view_transforms",
        "fin123.artifacts",
        "fin123.artifacts.store",
        "fin123.scalars",
        "fin123.tables",
        "fin123.cell_graph",
        "fin123.assertions",
        "fin123.utils",
        "fin123.utils.hash",
        # Worksheet runtime (lazy-imported by cli_core.py and ui/service.py)
        "fin123.worksheet",
        "fin123.worksheet.compiled",
        "fin123.worksheet.compiler",
        "fin123.worksheet.eval_row",
        "fin123.worksheet.spec",
        "fin123.worksheet.types",
        "fin123.worksheet.view_table",
        # Demo modules (lazy-imported by cli_core.py demo commands)
        "fin123.demos",
        "fin123.demos.ai_governance_demo",
        "fin123.demos.ai_governance_demo.run",
        "fin123.demos.ai_governance_demo.plugin_validator",
        "fin123.demos.ai_governance_demo.ai_generated_plugin_example",
        "fin123.demos.deterministic_build_demo",
        "fin123.demos.deterministic_build_demo.run",
        "fin123.demos.batch_sweep_demo",
        "fin123.demos.batch_sweep_demo.run",
        "fin123.demos.data_guardrails_demo",
        "fin123.demos.data_guardrails_demo.run",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="fin123-core",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
