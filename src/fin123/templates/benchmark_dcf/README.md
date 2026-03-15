# Benchmark DCF Operating Model

Quarterly multi-segment operating model with DCF valuation.
66,000-row historical dataset. Designed to benchmark fin123 table graph
performance against spreadsheet recalculation.

## Dataset

10 tickers × 5 segments × 3 regions × 5 products × 22 years × 4 quarters = 66,000 rows.

Columns: ticker, segment, region, product, year, quarter, revenue, cogs,
opex, capex, da, interest, shares, price.

## Model

Table graph (Polars): derive gross_profit, ebit, ebitda, nopat, fcf, margins
across all 66K rows, then aggregate by ticker (10 summary rows).

Scalar graph: lookup base financials for active ticker, project 5 years of
revenue/EBIT/NOPAT/FCF, compute DCF enterprise value with terminal value.

## Quick start

```bash
fin123 init bench --template benchmark_dcf
fin123 build bench
fin123 --verbose build bench
```

## Change active ticker

```bash
fin123 build bench --set active_ticker=MSFT
```

The `active_ticker` param controls which ticker's financials are used
for the DCF valuation. All 10 tickers are always processed in the table
graph; the scalar lookups read from the selected ticker's summary row.

## Scenarios

```bash
fin123 build bench --scenario bull
fin123 build bench --all-scenarios
```

## Batch sweep (20 scenarios across tickers)

```bash
fin123 batch build bench --params-file bench/inputs/scenarios.csv
```

The batch CSV includes `active_ticker` so each run can target a
different company with different assumptions.

## Sensitivity grid (5×5 wacc × terminal_growth)

```bash
fin123 batch build bench --params-file bench/inputs/sensitivity.csv
```

## Timing

Use `--verbose` for per-phase timing or `--json` for machine-readable output:

```bash
fin123 --json build bench | python -m json.tool
```

The `timings_ms` field shows: resolve_params, hash_inputs, eval_tables,
eval_scalars, export_outputs.
