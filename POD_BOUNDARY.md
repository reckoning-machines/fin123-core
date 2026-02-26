# Pod Boundary

## What Pod Adds

- **SQL sync** — `fin123 sync` fetches data from Postgres/SQL databases into local parquet caches with provenance tracking, schema guards, and TTL-based refresh policies.
- **Connectors** — Built-in Bloomberg connector (BDP/BDH) and a connector plugin protocol for custom data feeds.
- **Plugin marketplace** — Discovery, installation, validation, activation, and version management for third-party extensions.
- **Postgres registry** — Centralized model version and build tracking via `fin123_model_versions`, `fin123_builds`, `fin123_releases` tables. Push/pull workflow with idempotent conflict handling.
- **Headless runner** — FastAPI service (`POST /run`) that executes model versions fetched from the registry without a local project directory.
- **Workflows** — YAML-defined multi-step pipelines (build, verify, sync, batch, scenario sweep) with idempotency keys and replay comparison. AI call step (OpenAI).
- **Release system** — `fin123 release build` marks verified runs for downstream consumption. Release sets group batch builds. `resolve_latest` finds newest matching release.
- **Production mode** — `mode: prod` gates that block builds on parse errors, missing schema guards, missing plugin pins, or unreachable registry. Enforces verify-pass for releases.

## Why Pod Exists

Core is a complete standalone product: engine + UI + CLI + templates. It works offline with no dependencies beyond local files.

Pod exists for teams and enterprises that need:

1. **Shared state** — a central registry so multiple users see the same model versions and builds.
2. **Automation** — headless runner for scheduled or CI-triggered model execution.
3. **External data** — SQL sync and vendor connectors (Bloomberg) to feed models from live databases.
4. **Governance** — production mode gates, release workflows, and audit trails.
5. **Extensibility** — plugin marketplace for custom connectors and workflow steps.

## Relationship

Pod depends on Core. Core never imports Pod.

```
fin123-pod (private)
  └── depends on: fin123-core (public)
```

- Pod CLI (`fin123`) imports Core's Click group and adds commands: `sync`, `workflow`, `runner`, `plugin`, `registry`, `release`, `datasheet`, `sync-log`.
- Pod modules use the `fin123_pod` Python namespace. Core modules remain at `fin123`.
- Core's UI service stubs Pod-only features with `try/except ImportError` — installing Pod activates them automatically.
