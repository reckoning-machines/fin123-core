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
        "fin123.artifacts",
        "fin123.artifacts.store",
        "fin123.scalars",
        "fin123.tables",
        "fin123.cell_graph",
        "fin123.assertions",
        "fin123.utils",
        "fin123.utils.hash",
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
