# Test Boundary: Core vs Pod

fin123 is split into two packages:

| Package | Modules | Tests run by |
|---------|---------|-------------|
| **fin123-core** | `fin123.*` (engine, CLI, formulas, UI) | This repo |
| **fin123-pod** | `fin123.cli`, `fin123.sync`, `fin123.workflows`, `fin123.releases` | Pod repo |

## Running tests

```bash
# Core tests only (default via pyproject.toml addopts):
pytest

# Explicitly include pod-marked tests (requires pod modules installed):
pytest -m "" --ignore=tests/pod
# or to run ONLY pod tests:
pytest -m pod --no-header tests/ tests/pod/
```

## How the boundary is enforced

1. **`tests/pod/`** contains test files that fail at *import time* because they
   reference pod-only modules (`fin123.sync`, `fin123.workflows`).
   These are excluded via `--ignore=tests/pod` in `pyproject.toml` addopts.

2. **`@pytest.mark.pod`** marks individual test classes or methods that import
   pod-only modules at *runtime* (`fin123.cli`, `fin123.releases`) or depend on
   pod-only templates (`sql_datasheet`, `plugin_example_connector`).
   These are excluded via `-m 'not pod'` in `pyproject.toml` addopts.

## Adding new tests

- If your test only uses `fin123.*` modules that live in `src/fin123/`, it belongs
  in `tests/` with no marker.
- If your test imports `fin123.cli`, `fin123.sync`, `fin123.releases`, or
  `fin123.workflows`, add `@pytest.mark.pod` or place it in `tests/pod/`.
