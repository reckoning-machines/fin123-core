# Build fin123-core Windows binary and package into a release ZIP.
# Usage: scripts\build_windows.ps1 [-VersionOverride "0.3.0"]
param(
    [string]$VersionOverride = ""
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Push-Location $RepoRoot

try {
    # Resolve version
    if ($VersionOverride) {
        $Version = $VersionOverride
    } else {
        $Version = python -c "from importlib.metadata import version; print(version('fin123-core'))" 2>$null
        if (-not $Version) {
            $Version = python -c "import re, pathlib; print(re.search(r'version\s*=\s*\`"([^\`"]+)\`"', pathlib.Path('pyproject.toml').read_text()).group(1))"
        }
    }

    Write-Host "==> Building fin123-core $Version for Windows x86_64"

    # Clean previous build artifacts
    if (Test-Path "build\fin123-core") { Remove-Item -Recurse -Force "build\fin123-core" }
    if (Test-Path "dist\fin123-core.exe") { Remove-Item -Force "dist\fin123-core.exe" }

    # Run PyInstaller
    python -m PyInstaller --clean --noconfirm packaging\fin123_core.spec

    # Verify the binary works
    Write-Host "==> Verifying binary..."
    $Binary = "dist\fin123-core.exe"
    if (-not (Test-Path $Binary)) {
        Write-Error "Binary not found at $Binary"
        exit 1
    }
    & $Binary --version

    # Package into ZIP
    $ZipName = "fin123-core-$Version-windows-x86_64.zip"
    Write-Host "==> Packaging $ZipName"
    Push-Location dist
    Compress-Archive -Path "fin123-core.exe" -DestinationPath $ZipName -Force
    Pop-Location

    Write-Host "==> Built: dist\$ZipName"

    # Generate checksums
    python scripts\checksums.py

    Write-Host "==> Done."
} finally {
    Pop-Location
}
