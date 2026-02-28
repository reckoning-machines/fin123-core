# Demo 4 -- Data Guardrails (Join/Lookup Violations)

## Command

```bash
fin123-core demo data-guardrails
```

## Files produced

- `guardrails_failure.json` -- structured failure output from bad fixtures
- `guardrails_success.json` -- success output from fixed fixtures

## Expected console output

```
Phase 1: Testing with bad fixtures (duplicate keys)...
  Expected failure: ValueError: join_left validate='many_to_one': ...
Phase 2: Testing with fixed fixtures (clean data)...
  Build passed. Export hash: <stable hash>...
```
