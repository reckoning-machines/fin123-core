#!/usr/bin/env bash
#
# fin123 end-to-end demo
# Runs: commit, scenario, compare, sweep, grid, AI add-in
#
# Usage:
#   bash demo/run_demo.sh
#
# Prerequisites:
#   pip install -e ".[dev]"
#   export ANTHROPIC_API_KEY=sk-...  (optional, for AI commands)

set -euo pipefail

PROJECT="demo_dcf"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_DIR"

# Colors
DIM='\033[2m'
BOLD='\033[1m'
GREEN='\033[32m'
AMBER='\033[33m'
RESET='\033[0m'

step() {
    echo ""
    echo -e "${BOLD}${AMBER}--- $1 ---${RESET}"
    echo ""
}

run() {
    echo -e "${DIM}\$ $@${RESET}"
    "$@"
    echo ""
}

# ── Setup ──

step "Setup: create project from benchmark_dcf template"

if [ -d "$PROJECT" ]; then
    rm -rf "$PROJECT"
fi
run fin123 init "$PROJECT" --template benchmark_dcf

# ── 1. Base case ──

step "1. Base case: commit with default assumptions"

cd "$PROJECT"
run fin123 build .
BASE_RUN=$(ls -1t runs/ | head -1)
echo -e "Base run_id: ${GREEN}${BASE_RUN}${RESET}"
cd "$REPO_DIR"

# ── 2. Bull case ──

step "2. Bull case: higher growth, lower WACC"

cd "$PROJECT"
# Modify params in workbook.yaml for bull case
python3 -c "
import yaml
with open('workbook.yaml') as f:
    spec = yaml.safe_load(f)
spec['params']['revenue_growth'] = 0.12
spec['params']['ebit_margin'] = 0.26
spec['params']['wacc'] = 0.08
with open('workbook.yaml', 'w') as f:
    yaml.dump(spec, f, default_flow_style=False, sort_keys=False)
"
run fin123 commit .
run fin123 build .
BULL_RUN=$(ls -1t runs/ | head -1)
echo -e "Bull run_id: ${GREEN}${BULL_RUN}${RESET}"
cd "$REPO_DIR"

# ── 3. Compare ──

step "3. Compare: base vs bull"

cd "$PROJECT"
run fin123 diff run "$BASE_RUN" "$BULL_RUN"
cd "$REPO_DIR"

# ── 4. Sweep (batch build) ──

step "4. Sweep: revenue_growth across 5 values"

# Create a sweep params file
cd "$PROJECT"
cat > /tmp/fin123_sweep.csv << 'EOF'
revenue_growth
0.04
0.06
0.08
0.10
0.12
EOF

run fin123 batch build . --params-file /tmp/fin123_sweep.csv
cd "$REPO_DIR"

# ── 5. Grid (batch build with 2 params) ──

step "5. Grid: revenue_growth x wacc sensitivity"

cd "$PROJECT"
python3 -c "
import csv
with open('/tmp/fin123_grid.csv', 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['revenue_growth', 'wacc'])
    for rg in [0.04, 0.06, 0.08]:
        for wacc in [0.08, 0.09, 0.10]:
            w.writerow([rg, wacc])
"

run fin123 batch build . --params-file /tmp/fin123_grid.csv
cd "$REPO_DIR"

# ── 6. Show results ──

step "6. Results"

cd "$PROJECT"
echo "Completed runs:"
ls -1 runs/ | while read run; do
    SCALARS=$(python3 -c "
import json
try:
    d = json.load(open('runs/$run/outputs/scalars.json'))
    vps = d.get('value_per_share', '?')
    ev = d.get('enterprise_value', '?')
    if isinstance(vps, float): vps = f'{vps:,.2f}'
    if isinstance(ev, float): ev = f'{ev:,.2f}'
    print(f'value_per_share={vps}  enterprise_value={ev}')
except: print('(no scalars)')
")
    echo "  $run  $SCALARS"
done
cd "$REPO_DIR"

# ── Summary ──

step "Demo complete"

echo "This demo showed:"
echo "  - Deterministic commit with run_id"
echo "  - Scenario comparison (base vs bull)"
echo "  - Parameter sweep (5 revenue_growth values)"
echo "  - Sensitivity grid (3x3 revenue_growth x wacc)"
echo ""
echo "For the full Terminal Mode experience (commit, compare, sweep, grid, AI):"
echo "  fin123 ui $PROJECT"
echo "  Then switch to Terminal Mode."
echo ""

# Cleanup
rm -f /tmp/fin123_sweep.csv /tmp/fin123_grid.csv
