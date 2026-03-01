# CLI Specification

Authoritative contract for the `fin123` command-line interface.
Both fin123-core and fin123-pod share this specification. Enterprise
commands are stubbed in core (exit 4) and implemented in pod.

## Purpose

fin123 is a deterministic, audit-grade financial modeling CLI designed for:

- **Reproducibility** -- identical inputs always produce identical outputs.
- **CI integration** -- every command supports `--json` for machine-readable output and standardized exit codes.
- **Governance** -- immutable builds, integrity verification, and release tracking.

## Command Tree

```
fin123 [--json] [--quiet] [--verbose] [--version] [--help]

  Core commands:
    init              Scaffold a new project from a template
    commit            Commit workbook snapshot
    build             Execute workbook (evaluate scalar + table graphs)
    verify            Verify build integrity (SHA-256 hash checks)
    diff run          Compare two build runs
    diff version      Compare two workbook versions
    export            Export run outputs
    doctor            Preflight and compliance checks
    template list     List available templates
    template show     Show template details
    artifact list     List artifacts
    artifact approve  Approve artifact version
    artifact reject   Reject artifact version
    artifact status   Check artifact status
    batch build       Batch build across parameter sets
    demo              Run built-in demos (ai-governance, deterministic-build,
                      batch-sweep, data-guardrails)
    gc                Garbage-collect old runs/artifacts
    clear-cache       Clear hash cache
    import-xlsx       Import an Excel workbook
    events            Show event log
    run-log           Show run log
    ui                Launch local browser UI

  Enterprise commands (require fin123-pod):
    sync              Sync SQL tables and connectors
    workflow run      Execute a named workflow
    registry status   Show registry backend status
    registry sync     Sync with registry
    server start      Start headless runner service
    server status     Show runner service status
    plugins list      List installed plugins
    plugins run       Run a plugin
    release build     Create a build release
    datasheet list    List SQL table caches
    sync-log          Show event log for a sync
```

### Hidden Aliases

For backward compatibility, these hidden aliases exist but are not
documented in `--help`:

| Alias | Canonical |
|-------|-----------|
| `new` | `init` |
| `verify-build` | `verify` |

## Global Flags

Every command inherits these flags from the root group:

| Flag | Behavior |
|------|----------|
| `--json` | Emit a single JSON object to stdout (see JSON contract below). Suppress human-formatted output. |
| `--quiet` | Suppress non-essential output (informational messages, progress). Errors still print to stderr. |
| `--verbose` | Enable diagnostic output. |
| `--version` | Print version string and exit. |
| `--help` | Print help text and exit. |

**Precedence:** `--json` takes priority over `--quiet`. When both are
set, output is JSON only. `--verbose` adds detail to both human and
JSON modes (via the `data` field).

**Placement:** Global flags are accepted before or after the subcommand
name. Canonical style in documentation is postfix:
`fin123 doctor --json` (not `fin123 --json doctor`).

## Exit Codes

| Code | Name | Meaning |
|------|------|---------|
| 0 | OK | Success |
| 1 | ERROR | Generic runtime error |
| 2 | USAGE | Invalid arguments or usage |
| 3 | VERIFY_FAIL | Verification failure (hash mismatch, non-determinism) |
| 4 | ENTERPRISE | Enterprise-only feature (install fin123-pod) |
| 5 | DEPENDENCY | Missing dependency or environment issue |

Exit codes are defined in `fin123.cli_core` and shared across
core and pod.

## JSON Output Contract

When `--json` is passed, every command prints exactly one JSON object
to stdout:

```json
{
  "ok": true,
  "cmd": "doctor",
  "version": "0.3.2",
  "data": { ... },
  "error": null
}
```

| Key | Type | Description |
|-----|------|-------------|
| `ok` | boolean | `true` if the command succeeded |
| `cmd` | string | Command name |
| `version` | string | fin123 package version |
| `data` | object | Command-specific payload (always present, may be `{}`) |
| `error` | object or null | Error details when `ok` is `false` |

Error object structure:

```json
{
  "code": 4,
  "message": "Enterprise feature: install fin123-pod"
}
```

JSON is serialized with `sort_keys=True` and `indent=2` for
human readability. Consumers should parse without depending on
key ordering.

## Doctor Specification

`fin123 doctor` runs preflight validation checks in a fixed order.

### Core Checks (always present)

| # | Check | Pass condition | Severity |
|---|-------|---------------|----------|
| 1 | Runtime | Python >= 3.11 | error |
| 2 | Determinism engine | SHA-256 self-test produces stable hash | error |
| 3 | Floating-point stability | Canonical float operations match expected values | error |
| 4 | Filesystem | Temp directory writable | error |
| 5 | Locale / encoding | stdout encoding is utf-8 | error |
| 6 | Timezone | UTC preferred; non-UTC emits warning | warning |
| 7 | Dependencies | All required packages importable | error |

### Enterprise Checks

| # | Check | Core behavior | Pod behavior |
|---|-------|--------------|-------------|
| 8 | Registry connectivity | Stub: returns `enterprise_only`, exit 4 | Checks `FIN123_REGISTRY_URL`, pings Postgres |
| 9 | Plugin integrity | Stub: returns `enterprise_only`, exit 4 | Verifies plugin manager importable |
| 10 | Server preflight | Stub: returns `enterprise_only`, exit 4 | Checks port availability, runner importable |

In core, enterprise checks are reported as `ENTERPRISE (core)` in
human output and excluded from the error count. The overall result
is based on core checks only. In pod, enterprise checks are fully
evaluated and included in the overall result.

### Doctor JSON Output

```json
{
  "ok": true,
  "cmd": "doctor",
  "version": "0.3.2",
  "data": {
    "checks": [
      {
        "name": "runtime",
        "ok": true,
        "severity": "error",
        "detail": "Python 3.12.0"
      }
    ],
    "summary": "PASS",
    "warnings": 1,
    "errors": 0
  },
  "error": null
}
```

### Doctor Human Output

```
Runtime ...................... OK
Determinism engine ........... OK
Floating-point stability ..... OK
Filesystem ................... OK
Locale / encoding ............ OK
Timezone ..................... WARNING (EST)
Dependencies ................. OK
Registry connectivity ........ ENTERPRISE (core)
Plugin integrity ............. ENTERPRISE (core)
Server preflight ............. ENTERPRISE (core)

Overall: PASS (1 warning(s))
```

## Examples

### Preflight check (JSON)

```bash
fin123 doctor --json
```

### Build a model (JSON)

```bash
fin123 build my_model --json
```

### Registry status (Enterprise)

Core (stub):
```bash
fin123 registry status --json
# Exit code: 4
# {"ok": false, "cmd": "registry status", ... "error": {"code": 4, "message": "Enterprise feature: install fin123-pod"}}
```

Pod (real):
```bash
fin123 registry status --json
# Exit code: 0
# {"ok": true, "cmd": "registry status", ... "data": {"backend": "postgres", "connected": true}}
```
