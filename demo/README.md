# fin123 Demo -- AAPL DCF Valuation

This demo runs a complete DCF valuation workflow in Terminal Mode:

- Commit a base case and a bull case
- Compare scenarios side by side
- Sweep revenue growth across 5 values
- Run a 2D sensitivity grid (revenue growth vs WACC)
- Generate an AI add-in, validate it, apply it, and build

Total runtime: under 2 minutes.

## Prerequisites

```bash
pip install -e ".[dev]"
```

For AI add-in generation, set your provider key:

```bash
export ANTHROPIC_API_KEY=sk-...
```

If no key is set, AI commands return a clear error. Everything else works.

## Quick Start

### Automated

```bash
bash demo/run_demo.sh
```

### Manual (Terminal Mode)

```bash
fin123 init demo_dcf --template benchmark_dcf
fin123 ui demo_dcf
```

Then switch to Terminal Mode and run the commands below.

## Demo Flow

### 1. Base case

```
set revenue_growth = 0.08
set ebit_margin = 0.22
set wacc = 0.10
commit
scenario save base
```

### 2. Bull case

```
set revenue_growth = 0.12
set ebit_margin = 0.26
set wacc = 0.08
commit
scenario save bull
```

### 3. Compare

```
compare base bull
```

Shows changed inputs and changed outputs in a structured diff.

### 4. Sweep

```
sweep revenue_growth 0.04 0.06 0.08 0.10 0.12 --outputs value_per_share enterprise_value implied_upside
```

Each row is a real execution with its own run_id.

### 5. Grid

```
grid revenue_growth 0.04 0.06 0.08 vs wacc 0.08 0.09 0.10 --output value_per_share
```

Matrix output: revenue growth on rows, WACC on columns, value per share in cells.

### 6. AI Add-in (requires API key)

```
ai draft addin "calculate compound annual growth rate from start and end values over n periods"
draft show draft_0001
validate draft draft_0001
apply draft draft_0001
commit
```

## What to highlight during a demo

1. **Every commit produces a run_id.** Not a recalculated cell -- a real execution.
2. **Compare shows exactly what changed.** Inputs and outputs, side by side.
3. **Sweep replaces Excel Data Tables.** Each row is independently verifiable.
4. **Grid replaces manual sensitivity tables.** Two parameters, one output, real builds.
5. **AI generates code, not mutations.** Draft, validate, apply -- explicit lifecycle.
6. **35 ms per build.** 20 scenarios in 1.4 seconds. 5x5 grid in 1.8 seconds.
