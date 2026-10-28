"""Independent consumer checks against Device Scheduler's public DTOs."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest

from device_scheduler_core.models import Attempt, AttemptState, Job, JobSpec, JobState, ResourceRequest, ScheduleSpec
from device_scheduler_core.observability import bound_output
from project_lab import Lab, Project, RunRequest
from project_lab.adapters import DeviceSchedulerAdapter


class RecordingSchedulerClient:
    """In-process SDK double: no daemon, job dispatch, or resource allocation."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.jobs: dict[str, dict[str, object]] = {}

    async def submit_job(self, spec):
        model = JobSpec.from_dict(spec)
        self.calls.append(("jobs.submit", model))
        job_id = f"job-{len(self.jobs) + 1}"
        self.jobs[job_id] = {"id": job_id, "state": "queued", "cleanup_verified": False}
        return {"id": job_id, "state": "queued"}

    async def put_schedule(self, spec):
        model = ScheduleSpec.from_dict(spec)
        self.calls.append(("schedules.put", model))
        return {"schedule_id": model.schedule_id, "state": "active"}

    async def get_job(self, job_id):
        self.calls.append(("jobs.get", job_id))
        return dict(self.jobs[job_id])

    async def wait_job(self, job_id, *, timeout_seconds):
        self.calls.append(("jobs.wait", (job_id, timeout_seconds)))
        job = dict(self.jobs[job_id])
        return {"job": job, "timed_out": job["state"] not in {"succeeded", "failed", "cancelled"}}

    async def control_job(self, job_id, action):
        self.calls.append(("jobs.control", (job_id, action)))
        return {"job_id": job_id, "state": action}

    async def call(self, operation, arguments):
        self.calls.append((operation, dict(arguments)))
        return {"schedule_id": arguments["schedule_id"], "state": arguments["action"]}


def _run(*, project: str = "demo", cpus: int = 3, memory_mb: int = 768,
         gpu_count: int = 0, wall_seconds: int = 90, max_wall_seconds: int = 180) -> dict:
    return {
        "project": project,
        "objective": "exercise the captured input",
        "source_paths": ["src"],
        "resource": {
            "kind": "gpu" if gpu_count else "cpu", "cpus": cpus,
            "memory_mb": memory_mb, "gpu_count": gpu_count,
            "wall_seconds": wall_seconds,
        },
        "max_wall_seconds": max_wall_seconds,
        "max_experiments": 2,
        "allowed_tools": ["python3"],
    }


def test_effect_job_uses_core_job_and_resource_contracts_with_byte_memory() -> None:
    client = RecordingSchedulerClient()
    adapter = DeviceSchedulerAdapter(client, owner_id="lab-owner")
    submitted = adapter.submit(request={
        "project": "demo", "candidate_dir": "/tmp/lab/candidate",
        "source_dir": "/tmp/lab/capture", "argv": ["python3", "test.py"],
        "resource": {"cpus": 3, "memory_mb": 768, "gpu_count": 0, "wall_seconds": 90},
        "effect_id": "effect-1",
    }, idempotency_key="project-lab:session:effect-1")

    assert submitted["job_id"] == "job-1"
    _, spec = client.calls[0]
    assert isinstance(spec, JobSpec)
    assert spec.idempotency_key == "project-lab:session:effect-1"
    assert spec.working_directory == "/tmp/lab/candidate"
    assert spec.resources[0].kind == "cpu" and spec.resources[0].quantities == {"cores": 3}
    assert spec.resources[1].kind == "memory" and spec.resources[1].quantities == {"bytes": 768 * 1024**2}
    assert spec.timeout_seconds == 90 and spec.max_attempts == 1


def test_gpu_effect_and_controller_specs_are_valid_core_jobs_with_separate_allocations(tmp_path: Path) -> None:
    client = RecordingSchedulerClient()
    config = tmp_path / "lab config.toml"
    state = tmp_path / "private state"
    worker = "/home/test/.local/bin/project-lab-worker"
    adapter = DeviceSchedulerAdapter(client, owner_id="lab-owner", state_dir=state,
                                     config_path=config, worker_executable=worker)
    adapter.submit(request={
        "project": "demo", "candidate_dir": "/tmp/lab/candidate", "source_dir": "/tmp/lab/capture",
        "argv": ["python3", "test.py"],
        "resource": {"cpus": 6, "memory_mb": 4096, "gpu_count": 2, "wall_seconds": 120},
    }, idempotency_key="effect-gpu")
    effect = client.calls[-1][1]
    assert isinstance(effect, JobSpec)
    assert effect.resources[1].quantities == {"bytes": 4096 * 1024**2}
    assert effect.resources[2].kind == "gpu" and effect.resources[2].quantities == {"count": 2}

    controller_result = adapter.submit_controller(request={
        "session_id": "session-1", "run": _run(cpus=6, memory_mb=4096, gpu_count=2),
        "source_identity": {"revision": "abc"}, "deadline": 1234,
    }, idempotency_key="controller-1")
    controller = client.calls[-1][1]
    assert controller_result["job_id"] == "job-2"
    assert isinstance(controller, JobSpec)
    assert controller.argv == (
        worker, "--config", str(config), "--state-dir", str(state),
        "--session", "session-1",
    )
    assert controller.working_directory == str(state)
    assert controller.resources[0].quantities == {"cores": 2}
    assert controller.resources[1].quantities == {"bytes": 2048 * 1024**2}
    assert all(item.kind != "gpu" for item in controller.resources)


def test_adapter_wait_inspect_preserve_queued_and_terminal_cleanup_receipts() -> None:
    client = RecordingSchedulerClient()
    adapter = DeviceSchedulerAdapter(client, owner_id="lab-owner",
                                     worker_executable="/home/test/.local/bin/project-lab-worker")
    submitted = adapter.submit_controller(request={"session_id": "session-1", "run": _run()}, idempotency_key="controller")
    job_id = submitted["job_id"]
    waited = adapter.wait(job_id, 4.5)
    assert waited["job"]["state"] == "queued" and waited["timed_out"] is True
    assert client.calls[-1] == ("jobs.wait", (job_id, 4.5))
    queued = adapter.inspect(job_id)
    assert queued["state"] == "queued" and queued["cleanup_verified"] is False

    client.jobs[job_id] = {
        "id": job_id, "state": "succeeded", "cleanup_verified": True,
        "result": {"measurements": {"elapsed_ms": 12}},
    }
    final = adapter.inspect(job_id)
    assert final["state"] == "succeeded"
    assert final["cleanup_verified"] is True
    assert final["result"]["measurements"] == {"elapsed_ms": 12}


def test_controller_resolves_installed_worker_to_absolute_executable(tmp_path: Path, monkeypatch) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    worker = bin_dir / "project-lab-worker"
    worker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    worker.chmod(0o755)
    monkeypatch.setenv("PATH", str(bin_dir))
    client = RecordingSchedulerClient()
    adapter = DeviceSchedulerAdapter(client, owner_id="lab-owner", state_dir=tmp_path / "state")

    adapter.submit_controller(request={"session_id": "session-1", "run": _run()},
                              idempotency_key="controller-absolute")
    spec = client.calls[-1][1]
    assert isinstance(spec, JobSpec)
    assert spec.argv[0] == str(worker.resolve())


def test_controller_fails_actionably_when_worker_cannot_be_resolved(monkeypatch) -> None:
    monkeypatch.setenv("PATH", "")
    client = RecordingSchedulerClient()
    adapter = DeviceSchedulerAdapter(client, owner_id="lab-owner")
    with pytest.raises(ValueError, match="absolute project-lab-worker executable"):
        adapter.submit_controller(request={"session_id": "session-1", "run": _run()},
                                  idempotency_key="controller-no-worker")
    assert client.calls == []


def test_delayed_schedule_emits_a_valid_not_before_job_template() -> None:
    client = RecordingSchedulerClient()
    worker = "/home/test/.local/bin/project-lab-worker"
    adapter = DeviceSchedulerAdapter(client, owner_id="lab-owner", state_dir="/tmp/lab-state",
                                     worker_executable=worker)
    start = datetime(2030, 1, 2, tzinfo=timezone.utc).timestamp()
    result = adapter.schedule(request={
        "run": _run(), "start_at": start, "recurrence_seconds": None,
        "timezone": "UTC", "capture_source_at_start": True,
    }, idempotency_key="delayed-1")
    job = client.calls[-1][1]
    assert result["job_id"] == "job-1"
    assert isinstance(job, JobSpec)
    assert job.not_before.astimezone(timezone.utc) == datetime(2030, 1, 2, tzinfo=timezone.utc)
    assert job.profile_id is None and job.argv[0] == worker
    assert job.environment["PROJECT_LAB_CAPTURE_AT_START"] == "1"
    assert json.loads(job.environment["PROJECT_LAB_RUN_REQUEST"])["resource"]["gpu_count"] == 0
    assert all(item.kind != "gpu" for item in job.resources)


def test_recurring_schedule_is_a_valid_schedule_spec_with_start_time_capture() -> None:
    client = RecordingSchedulerClient()
    worker = "/home/test/.local/bin/project-lab-worker"
    adapter = DeviceSchedulerAdapter(client, owner_id="lab-owner", state_dir="/tmp/lab-state",
                                     worker_executable=worker)
    result = adapter.schedule(request={
        "run": _run(gpu_count=1), "start_at": None, "recurrence_seconds": 3600,
        "timezone": "America/Los_Angeles", "capture_source_at_start": True,
    }, idempotency_key="recurring:1")
    schedule = client.calls[-1][1]
    assert isinstance(schedule, ScheduleSpec)
    assert result["job_id"] == schedule.schedule_id
    # Device Scheduler intervals are elapsed seconds, so calendar timezone
    # fields must be omitted; its canonical DTO rejects timezone + interval.
    assert schedule.interval_seconds == 3600 and schedule.timezone is None
    assert schedule.max_instances == 1
    assert schedule.job.environment["PROJECT_LAB_CAPTURE_AT_START"] == "1"
    assert json.loads(schedule.job.environment["PROJECT_LAB_RUN_REQUEST"])["resource"]["gpu_count"] == 1
    assert all(item.kind != "gpu" for item in schedule.job.resources)


def test_resource_request_dto_contract_is_available_for_supervised_reservation() -> None:
    request = ResourceRequest.from_dict({
        "request_id": "lab-reservation-1", "owner_id": "lab-owner",
        "requirements": [{"kind": "cpu", "quantities": {"cores": 2}}],
        "lease_ttl_seconds": 120, "idempotency_key": "lab-reservation-1", "usage": "supervised",
    })
    assert request.owner_id == "lab-owner"
    assert request.usage.value == "supervised"
    assert request.requirements[0].quantities == {"cores": 2}


class CoreShapedResultClient:
    """Core DTO-shaped job/attempt views plus the scheduler's bounded log envelope."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, object]] = {}
        self.outputs: dict[str, dict[str, object]] = {}
        self.effect_count = 0

    async def submit_job(self, raw_spec):
        spec = JobSpec.from_dict(raw_spec)
        job_id = f"job-{len(self.jobs) + 1}"
        is_controller = "project-lab-controller" in spec.tags
        attempt_id = None if is_controller else f"attempt-{self.effect_count + 1}"
        state = JobState.QUEUED if is_controller else JobState.SUCCEEDED
        job = Job(job_id=job_id, spec=spec, state=state,
                  current_attempt_id=attempt_id).to_dict()
        if not is_controller:
            self.effect_count += 1
            attempt = Attempt(attempt_id=attempt_id, job_id=job_id,
                              state=AttemptState.SUCCEEDED,
                              result={"state": "succeeded", "cleanup_verified": True, "exit_code": 0},
                              cleanup_verified=True).to_dict()
            # The read API adds these execution fields to its direct job detail.
            job.update({"id": job_id, "attempt_id": attempt_id,
                        "result": {"state": "succeeded", "cleanup_verified": True, "exit_code": 0},
                        "attempt": attempt})
            self.outputs[job_id] = {"measurements": {"elapsed_ms": 10 + self.effect_count}}
        self.jobs[job_id] = job
        return {"job": {"id": job_id, "state": str(state.value)}}

    async def get_job(self, job_id):
        return dict(self.jobs[job_id])

    async def wait_job(self, job_id, *, timeout_seconds):
        job = self.jobs[job_id]
        return {"job": dict(job), "timed_out": job["state"] not in {"succeeded", "failed", "cancelled"}}

    async def job_logs(self, job_id):
        output = json.dumps(self.outputs.get(job_id, {}), separators=(",", ":"))
        return {"job_id": job_id, "attempt_id": self.jobs[job_id].get("attempt_id"),
                "stdout": bound_output(output, max_bytes=4096),
                "stderr": bound_output("", max_bytes=4096), "cleanup_verified": True}

    async def control_job(self, job_id, action):
        return {"job_id": job_id, "state": action}


class ResultAwarePlanner:
    def __init__(self, digest: str) -> None:
        self.digest = digest
        self.calls: list[dict] = []

    def propose(self, request):
        self.calls.append(request)
        return {"hypothesis": "measure the fixture", "source_citations": [
                    {"path": "src/subject.py", "sha256": self.digest}],
                "artifacts": [{"path": "measure.py", "content": "print('{}')\n"}],
                "argv": ["python3", "measure.py"], "measurements": ["elapsed_ms"],
                "stop_rule": "bounded run", "done": False}


def test_scheduler_result_measurements_flow_to_results_compare_and_next_planner_turn(tmp_path: Path) -> None:
    source = tmp_path / "source"
    (source / "src").mkdir(parents=True)
    input_file = source / "src" / "subject.py"
    input_file.write_text("answer = 42\n", encoding="utf-8")
    digest = hashlib.sha256(input_file.read_bytes()).hexdigest()
    planner = ResultAwarePlanner(digest)
    client = CoreShapedResultClient()
    scheduler = DeviceSchedulerAdapter(client, owner_id="lab-owner", state_dir=tmp_path / "lab-state",
                                       worker_executable="/home/test/.local/bin/project-lab-worker")
    lab = Lab(tmp_path / "lab-state", projects={"demo": Project("demo", source, "demo-repo")},
              planner=planner, scheduler=scheduler)

    first = lab.preview(RunRequest(project="demo", objective="measure twice", source_paths=("src",),
                                   max_experiments=2))
    lab.run(session_id=first["session_id"])
    lab.drive(first["session_id"])
    first_results = lab.results(first["session_id"])
    receipts = [item["receipt"] for item in first_results["effects"]]
    assert [item["measurements"] for item in receipts] == [{"elapsed_ms": 11}, {"elapsed_ms": 12}]
    assert receipts[0]["measurement_provenance"] == {
        "source": "scheduler_logs.stdout_json", "job_id": receipts[0]["job_id"],
        "attempt_id": "attempt-1", "stdout_sha256": receipts[0]["logs"]["stdout"]["sha256"],
        "complete": True,
    }
    assert planner.calls[1]["prior_results"][0]["receipt"]["measurements"] == {"elapsed_ms": 11}

    second = lab.preview(RunRequest(project="demo", objective="compare once", source_paths=("src",),
                                    max_experiments=1))
    lab.run(session_id=second["session_id"])
    lab.drive(second["session_id"])
    comparison = lab.compare([first["session_id"], second["session_id"]], ["elapsed_ms"])
    assert [row["measurements"]["elapsed_ms"] for row in comparison["sessions"]] == [[11, 12], [13]]


def test_truncated_scheduler_stdout_is_never_used_as_complete_measurement_data() -> None:
    class TruncatedClient:
        async def get_job(self, job_id):
            return {"id": job_id, "state": "succeeded", "result": {"state": "succeeded"},
                    "attempt": {"attempt_id": "attempt-1", "cleanup_verified": True,
                                "result": {"state": "succeeded", "cleanup_verified": True}}}

        async def job_logs(self, job_id):
            return {"stdout": bound_output('{"measurements":{"elapsed_ms":12345}}', max_bytes=8)}

    receipt = DeviceSchedulerAdapter(TruncatedClient(), owner_id="lab-owner").collect("job-1")
    assert receipt["measurements"] == {}
    assert "measurement_provenance" not in receipt
