# Project-Lab

Project-Lab runs bounded software experiments and keeps their inputs, outputs,
measurements, and execution receipts together. It can use Codex to propose an
experiment, then asks Device Scheduler to run it. Generated candidates stay in
the experiment area; Project-Lab does not edit your working project.

## Try the API

```python
from project_lab import Lab, Project, RunRequest

lab = Lab.open("~/.local/state/project-lab")
lab.register(Project(name="demo", root="./demo", repository="demo"))
preview = lab.preview(RunRequest(
    project="demo", objective="Check the parser against malformed input",
    source_paths=("src/parser.py",), max_experiments=2,
))
print(preview)
```

`preview` inspects and captures the selected source. `run` is a separate,
explicit action that authorizes the finite scope described by that preview and
submits a durable, CPU-only session controller to Device Scheduler. The
controller plans without holding experiment GPU capacity, submits each effect
as a separate scheduler job, and continues only after cleanup is verified. The
API, command line, and MCP server use the same operation schemas.

## Try the command line

For a complete, copyable preview example, including a disposable source tree,
local wheel installation, configuration, and the exact command, see
[`docs/configuration.md`](docs/configuration.md).

The command line uses the same seven operation families as the MCP server:
`preview`, `run`, `status`, `results`, `compare`, `control`, and `schedule`.
Run `project-lab --help` or `project-lab preview --help` to see their options.
Preview and inspection work without a scheduler connection; starting or
scheduling a session needs the configured Device Scheduler integration and
worker described in the configuration guide.

Configuration names registered projects and scheduler access. See
[`docs/recovery.md`](docs/recovery.md) for interrupted sessions and
[`docs/measurements.md`](docs/measurements.md) for the candidate output contract
used by results and comparisons.

Project-Lab is a peer of Project Control. It depends on shared source/evidence
packages and Device Scheduler contracts, not on Project Control or a model
service. Tests use fake Codex and scheduler adapters; running the test suite
does not start experiments or contact services.
