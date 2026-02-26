# demo_fin123

Lifecycle demo for fin123.  Walks through commit, build, verify, diff, and
release using a tiny valuation model with parquet inputs.

## Quick start

```bash
# 1. Scaffold the project (--set ticker=MSFT to override the default)
fin123 new /tmp/demo --template demo_fin123

# 2. Commit the initial workbook snapshot
fin123 commit /tmp/demo

# 3. Build (default params: ticker=AAPL, multiple=15)
fin123 build /tmp/demo
# Note the run_id printed by the build command.

# 4. Verify the build (assertions must pass)
fin123 verify-build <run_id>

# 5. Build with a different multiple for diff comparison
fin123 build /tmp/demo --set multiple=25

# 6. Compare the two runs
fin123 diff run <run_id_1> <run_id_2>

# 7. Create a release (dev mode -- verify gate is advisory)
fin123 release build <run_id>
```

## Intentional failure

The `scenario_fail` workflow sets `discount_rate=0.95`, which violates the
`discount_rate_sane` assertion (`$discount_rate < 0.50`).

```bash
fin123 workflow run scenario_fail /tmp/demo
```

## What this template demonstrates

- **PARAM() proxy binding** -- `B1` and `B2` in the Valuation sheet are bound
  to `ticker` and `multiple` via `=PARAM("ticker")` and `=PARAM("multiple")`.
- **Commit / Build / Verify flow** -- deterministic builds with assertion
  gating.
- **Assertions** -- `eps_positive` (error) and `discount_rate_sane` (error)
  pass by default; `scenario_fail` workflow triggers a failure.
- **Diff** -- run two builds with different `multiple` values and compare.
- **Release** -- create a build release after verify passes.
- **Tables** -- parquet sources with `join_left` + `validate: many_to_one`.
- **Scalars** -- `lookup_scalar` and `"=..."` formula expressions.
