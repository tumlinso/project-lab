# Set up Project-Lab

This guide installs Project-Lab and its sibling packages from local wheels,
then previews a tiny disposable CPU project. Preview reads and captures the
selected source; it does not call Codex or submit work to Device Scheduler.

## Install from local sibling repositories

Project-Lab depends on `project-evidence` and `project-source`, and scheduled
execution uses `device-scheduler-mcp`. This recipe builds and installs those
packages from their source repositories instead of assuming they are available
from the Python package index. In this example the three repositories are
checked out side by side; change the paths to match your machine.

```sh
export PROJECT_LAB_DIR="$HOME/src/project-lab"
export FOUNDATIONS_DIR="$HOME/src/project-foundations"
export DEVICE_SCHEDULER_DIR="$HOME/src/device-scheduler"
export WHEELHOUSE="$HOME/.cache/project-lab-wheels"
export VENV="$HOME/.venvs/project-lab"

python3 -m venv "$VENV"
. "$VENV/bin/activate"
python -m pip install build
mkdir -p "$WHEELHOUSE"

python -m build --wheel --outdir "$WHEELHOUSE" "$FOUNDATIONS_DIR/packages/project-evidence"
python -m build --wheel --outdir "$WHEELHOUSE" "$FOUNDATIONS_DIR/packages/project-source"
python -m build --wheel --outdir "$WHEELHOUSE" "$DEVICE_SCHEDULER_DIR/packages/device-scheduler-core"
python -m build --wheel --outdir "$WHEELHOUSE" "$DEVICE_SCHEDULER_DIR/packages/device-scheduler-adapters"
python -m build --wheel --outdir "$WHEELHOUSE" "$DEVICE_SCHEDULER_DIR/packages/device-scheduler-mcp"
python -m build --wheel --outdir "$WHEELHOUSE" "$PROJECT_LAB_DIR"

python -m pip install \
  "$WHEELHOUSE"/project_evidence-*.whl \
  "$WHEELHOUSE"/project_source-*.whl \
  "$WHEELHOUSE"/device_scheduler_core-*.whl \
  "$WHEELHOUSE"/device_scheduler_adapters-*.whl \
  "$WHEELHOUSE"/device_scheduler_mcp-*.whl \
  "$WHEELHOUSE"/project_lab-*.whl
```

The build and installation can fetch ordinary third-party Python dependencies
such as MCP and Pydantic. The Project-Lab, foundations, and scheduler wheels in
this recipe come from the local checkouts.

## Preview a disposable project

Create a small source tree and keep Project-Lab's state beside it, not inside
the registered repository:

```sh
mkdir -p /tmp/project-lab-quickstart/demo/src
cat > /tmp/project-lab-quickstart/demo/src/check.py <<'PY'
def answer():
    return 42
PY

cat > /tmp/project-lab-quickstart/config.toml <<'TOML'
[projects.demo]
root = "/tmp/project-lab-quickstart/demo"
repository = "demo"

[state]
directory = "/tmp/project-lab-quickstart/state"
TOML

project-lab \
  --config /tmp/project-lab-quickstart/config.toml \
  --state-dir /tmp/project-lab-quickstart/state \
  preview \
  --project demo \
  --source src \
  --objective "Check the small Python fixture" \
  --cpu 1 \
  --memory-mb 512 \
  --gpu-count 0 \
  --wall-seconds 30 \
  --max-experiments 1 \
  --tool python3
```

The same request, as passed to the MCP `preview` tool, is:

```json
{
  "request": {
    "project": "demo",
    "objective": "Check the small Python fixture",
    "source_paths": ["src"],
    "resource": {
      "kind": "cpu",
      "cpus": 1,
      "memory_mb": 512,
      "gpu_count": 0,
      "wall_seconds": 30
    },
    "max_experiments": 1,
    "max_wall_seconds": 30,
    "allowed_tools": ["python3"],
    "measurements": []
  }
}
```

The preview prints a JSON object containing a `session_id`. No model, scheduler,
or candidate process is started by this command.

## Configure scheduled runs

Add scheduler settings to the same TOML file to enable `run` and `schedule`:

```toml
[scheduler]
url = "http://127.0.0.1:8770"
token_file = "/home/you/.config/device-scheduler/client.token"
owner_id = "my-project-lab"
worker_executable = "/home/you/.local/bin/project-lab-worker"

[scheduler.resource_kinds]
cpu = "cpu"
memory = "memory"
gpu = "gpu"
```

Use absolute paths. The authenticated scheduler caller needs permission to
submit, inspect, wait for, read logs from, and control its jobs, plus permission
to create and control schedules when using recurring or delayed execution. Its
configured resource kinds must match the scheduler's registered CPU, memory,
and GPU resources.

`run` explicitly authorizes a finite session and submits a CPU/memory-only
controller job. The controller plans without holding experiment GPU capacity;
each proposed effect is submitted separately with the requested resources. The
Device Scheduler execution environment must have `project-lab-worker` and
`codex` installed, and the worker's configured Codex account must be
authenticated. Project-Lab sends an absolute worker path in controller jobs.
Set `scheduler.worker_executable` when the scheduler cannot resolve the worker
from its service `PATH`. Install the same Project-Lab and shared-package
versions in that execution environment.
The absolute config and state paths passed to the worker must exist and be
accessible from that environment, and the state directory must be shared by
the API process and worker so the worker can read the session journal, captured
source, and candidate artifacts. If the scheduler runs workers in containers,
mount those paths at the same locations. A run cannot continue if the worker
cannot access them.

After a preview, start its exact session with:

```sh
project-lab --config /tmp/project-lab-quickstart/config.toml \
  --state-dir /tmp/project-lab-quickstart/state \
  run --session lab_REPLACE_WITH_THE_SESSION_ID
```

The command returns a durable controller job and session ID. Use those IDs
with `status` and `results` to follow work. To start a fresh bounded run without
previewing first, supply the same `--project`, `--source`, `--objective`, and
budget flags to `run`; previewing first is recommended so you can review the
captured source and limits.

Without the `[scheduler]` section, local preview, status, results, and compare
remain available, while `run` and `schedule` report that no scheduler adapter
is configured.

## First experiment

1. Run the preview command above and review its source identity, requested
   resources, and stopping limits.
2. Configure the scheduler and worker environment before starting an actual
   session.
3. Call `run` with the returned preview session ID to authorize it.
4. Follow progress with `status`, inspect measurements and receipts with
   `results`, and compare recorded metrics with `compare`.

Recurring schedules use an elapsed interval in seconds. Device Scheduler's
interval contract does not accept a timezone; calendar expressions can be
added later as a separate typed mode if useful.
