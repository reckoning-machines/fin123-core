# fin123-core

Deterministic financial modeling engine with a local browser UI.

fin123-core is the standalone open-source core of the fin123 platform. It provides:

- **Engine**: Polars-backed workbook engine with scalar DAG + table LazyFrame plans.
- **Formula language**: Lark LALR(1) parser with Excel-like syntax.
- **Local browser UI**: FastAPI-powered spreadsheet editor on localhost.
- **Versioning**: Snapshot, run, and artifact stores with hash-based integrity.
- **Offline-first**: `fin123-core build` reads only local files. No network required.

## Quick start

```bash
pip install fin123-core

# Create a demo project
fin123-core new my_model --template demo_fin123

# Build (execute) the workbook
fin123-core build my_model

# Launch the browser UI
fin123-core ui my_model
```

## Enterprise features

For database-backed registries, headless runner services, connectors (Bloomberg),
plugin marketplace, workflow automation, and SQL sync, install
[fin123-pod](https://github.com/yourorg/fin123-pod) (private).

## License

MIT -- see LICENSE file.
