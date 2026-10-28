"""Human-oriented JSON command line for Project-Lab."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tomllib

from .adapters import CodexExecPlanner, DeviceSchedulerAdapter
from .models import Project, RunRequest, ScheduleRequest
from .service import Lab


def _error_payload(exc: Exception) -> dict[str, object]:
    error: dict[str, object] = {"error": type(exc).__name__, "message": str(exc)}
    code = getattr(exc, "code", None)
    if isinstance(code, str):
        error["code"] = code
    if getattr(exc, "inspect_only", False) is True:
        error["inspect_only"] = True
    session_id = getattr(exc, "session_id", None)
    if isinstance(session_id, str):
        error["session_id"] = session_id
    return error


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="project-lab", description="Run and inspect bounded software experiments")
    parser.add_argument("--config", type=Path, default=Path("~/.config/project-lab/config.toml").expanduser())
    parser.add_argument("--state-dir", type=Path, help="private Project-Lab journal and artifact directory")
    sub = parser.add_subparsers(dest="operation", required=True)
    preview = sub.add_parser("preview", help="inspect a proposed bounded run")
    _scope_args(preview)
    run = sub.add_parser("run", help="explicitly start a bounded experiment")
    run.add_argument("--session", help="start an existing preview"); _scope_args(run, required=False)
    status = sub.add_parser("status", help="show session state"); status.add_argument("session_id", nargs="?")
    results = sub.add_parser("results", help="show experiment results"); results.add_argument("session_id"); results.add_argument("--limit", type=int, default=100)
    compare = sub.add_parser("compare", help="compare recorded sessions"); compare.add_argument("session_ids", nargs="+", help="two or more session IDs"); compare.add_argument("--metric", action="append", default=[])
    control = sub.add_parser("control", help="pause, cancel, or resume a session"); control.add_argument("session_id"); control.add_argument("--action", choices=("pause", "cancel", "resume"), required=True)
    schedule = sub.add_parser("schedule", help="schedule a one-time or recurring experiment"); _scope_args(schedule); schedule.add_argument("--start-at", type=float); schedule.add_argument("--every-seconds", type=int)
    sub.add_parser("serve", help="start the Project-Lab MCP server")
    return parser


def _scope_args(parser, *, required: bool = True):
    parser.add_argument("--project", required=required)
    parser.add_argument("--source", action="append", required=required, default=[])
    parser.add_argument("--objective", required=required)
    parser.add_argument("--cpu", type=int, default=1)
    parser.add_argument("--memory-mb", type=int, default=1024)
    parser.add_argument("--gpu-count", type=int, default=0)
    parser.add_argument("--wall-seconds", type=int, default=600)
    parser.add_argument("--max-experiments", type=int, default=1)
    parser.add_argument("--tool", action="append", default=None,
                        help="permitted executable; defaults to python3, repeat to add tools")


def _load(path: Path, state_dir: Path | None) -> Lab:
    if not path.is_file():
        raise ValueError(f"configuration file not found: {path}; see docs/configuration.md")
    config = tomllib.loads(path.read_text(encoding="utf-8"))
    projects = {name: Project(name=name, root=value["root"], repository=value.get("repository", name))
                for name, value in config.get("projects", {}).items()}
    root = state_dir or Path(os.environ.get("PROJECT_LAB_STATE_DIR", config.get("state", {}).get("directory", "~/.local/state/project-lab"))).expanduser()
    planner = CodexExecPlanner(cwd=root)
    scheduler = None
    scheduler_config = config.get("scheduler", {})
    if scheduler_config:
        from device_scheduler_mcp import DeviceSchedulerClient
        client = DeviceSchedulerClient(scheduler_config["url"], scheduler_config["token_file"])
        scheduler = DeviceSchedulerAdapter(client, owner_id=scheduler_config["owner_id"],
            resource_kinds=scheduler_config.get("resource_kinds", {}),
            controller_profile_id=scheduler_config.get("controller_profile_id", "project-lab-controller"),
            state_dir=root, config_path=path,
            worker_executable=scheduler_config.get("worker_executable"))
    return Lab(root, projects=projects, planner=planner, scheduler=scheduler)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        lab = _load(args.config, args.state_dir)
        if args.operation == "serve":
            from .mcp import create_server
            create_server(lab).run()
            return 0
        if args.operation in {"preview", "run", "schedule"} and getattr(args, "project", None):
            request = RunRequest(project=args.project, objective=args.objective,
                                 source_paths=tuple(args.source),
                                 resource={"cpus": args.cpu, "memory_mb": args.memory_mb,
                                           "gpu_count": args.gpu_count, "kind": "gpu" if args.gpu_count else "cpu",
                                           "wall_seconds": args.wall_seconds},
                                 max_experiments=args.max_experiments,
                                 max_wall_seconds=args.wall_seconds,
                                 allowed_tools=tuple(args.tool or ("python3",)))
            if args.operation == "preview":
                result = lab.preview(request)
            elif args.operation == "schedule":
                result = lab.schedule(ScheduleRequest(session=request, start_at=args.start_at,
                                                      recurrence_seconds=args.every_seconds))
            else:
                result = lab.run(request)
        elif args.operation == "run":
            result = lab.run(session_id=args.session)
        elif args.operation == "status":
            result = lab.status(args.session_id)
        elif args.operation == "results":
            result = lab.results(args.session_id, limit=args.limit)
        elif args.operation == "compare":
            result = lab.compare(args.session_ids, args.metric)
        elif args.operation == "control":
            result = lab.control({"session_id": args.session_id, "action": args.action})
        else:
            raise ValueError("invalid command")
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps(_error_payload(exc), ensure_ascii=False), file=sys.stderr)
        return 2


def worker_main(argv: list[str] | None = None) -> int:
    """Private scheduler entry point, separate from the seven user operations."""
    parser = argparse.ArgumentParser(prog="project-lab-worker")
    parser.add_argument("--config", type=Path, default=Path("~/.config/project-lab/config.toml").expanduser())
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--session")
    args = parser.parse_args(argv)
    try:
        lab = _load(args.config, args.state_dir)
        if args.session:
            result = lab.drive(args.session)
        else:
            raw = os.environ.get("PROJECT_LAB_RUN_REQUEST")
            if not raw:
                raise ValueError("scheduled worker request is missing")
            result = lab.run(RunRequest.model_validate_json(raw))
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps(_error_payload(exc), ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
