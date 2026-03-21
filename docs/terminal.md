# Terminal Mode

Terminal Mode is a deterministic runner for analysts who operate financial models as programs.

Access Terminal Mode through the mode switcher in the browser UI. The terminal shell accepts structured commands and returns structured output — status blocks, tables, diffs, and cards.

## Runner Commands

### commit

Persist current state and execute a deterministic build.

```
commit
commit --name <scenario_name>
```

With `--name`, the committed run is also saved as a named scenario. Returns: status, build ID, timestamp, output summary.

### set / reset

```
set <name> = <value>
reset <name>
```

`set` updates a declared parameter. `reset` reports the current default value.

### inputs / outputs

```
inputs
outputs
show input <name>
show output <name>
```

List or inspect parameters and scalar outputs from the latest build.

### status

```
status
```

Workbook status: dirty/committed, snapshot version, last build ID, active sheet.

## Scenario Commands

### scenario save / load / list / show / delete

```
scenario save <name>
scenario load <name>
scenario list
scenario show <name>
scenario delete <name>
```

Scenarios label committed runs. `scenario save` captures current inputs, outputs, and the associated run_id. `scenario load` restores inputs and marks the workbook dirty.

### compare

```
compare <scenario_a> <scenario_b>
```

Structured tabular diff of two scenarios. Shows changed inputs and changed outputs with old/new values.

## Sweep Commands

### sweep

```
sweep <input> <v1> <v2> ...
sweep <input> range(start, stop, step)
sweep <input> <values...> --outputs <key1> <key2>
```

Runs the model for each value of the specified parameter. Each point is a real commit+build. Results are persisted and exportable.

### sweeps / show sweep / export sweep

```
sweeps
show sweep <id>
export sweep <id>
```

List, inspect, or export saved sweep results.

## Grid Commands

### grid

```
grid <inputX> <valsX...> vs <inputY> <valsY...> --output <name>
```

Two-dimensional sweep. Exactly one output displayed as a matrix. Maximum 100 cells. Both parameters must be distinct declared inputs.

### grids / show grid / export grid

```
grids
show grid <id>
export grid <id>
```

List, inspect, or export saved grid results. CSV export uses tidy long-form rows.

## AI Commands

AI commands are separated from deterministic runner commands. They generate artifacts; they do not directly modify workbook computation state.

### ai explain

```
ai explain formula <ref>
ai explain output <name>
```

Non-mutating. Sends workbook context to the configured LLM provider and returns a labeled explanation. Long explanations are truncated; use `show full last` to expand.

### ai draft addin / ai revise draft

```
ai draft addin "<description>"
ai revise draft <id> "<instruction>"
```

Generate or revise a plugin code draft. The result is saved as a new draft artifact. Revisions link back to the parent draft via `derived_from`.

### show full last

```
show full last
```

Re-render the last truncated AI explanation in full.

## Draft Commands

Draft commands manage the lifecycle of code artifacts.

```
draft list
draft show <id>
draft show full <id>
draft diff <id>
draft delete <id>
draft lineage <id>
draft validation <id>
validate draft <id>
apply draft <id>
reject draft <id>
```

`validate draft` runs a fresh policy scan. `draft validation` recalls the stored result without re-running. `apply draft` copies validated code to `plugins/` and marks the workbook dirty. The user must then `commit` to build with the new plugin.

## Configuration

Terminal Mode requires no configuration for deterministic runner commands.

AI commands require an LLM provider. Set via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `FIN123_LLM_PROVIDER` | `anthropic` | `anthropic` or `openai` |
| `ANTHROPIC_API_KEY` | — | Required if provider is anthropic |
| `OPENAI_API_KEY` | — | Required if provider is openai |
| `FIN123_LLM_MODEL` | Provider default | Model override |
