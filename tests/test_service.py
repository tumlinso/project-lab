from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from project_lab import Lab, Project, RunRequest
from project_lab.models import ControlRequest, ResourceRequest, ScheduleRequest


class Planner:
    def __init__(self, proposals):
        self.proposals = iter(proposals)
        self.calls = []

    def propose(self, request):
        self.calls.append(request)
        return next(self.proposals)


class RaisingPlanner:
    def __init__(self, error):
        self.error = error
        self.calls = 0

    def propose(self, request):
        self.calls += 1
        raise self.error


class Scheduler:
    def __init__(self, state="succeeded", *, raise_submit=False, cleanup_verified=True,
                 mutate_source=False):
        self.state = state
        self.raise_submit = raise_submit
        self.cleanup_verified = cleanup_verified
        self.mutate_source = mutate_source
        self.submissions = []
        self.jobs = {}
        self.schedules = []
        self.controller_submissions = []
        self.controller_jobs = {}

    def submit_controller(self, *, request, idempotency_key):
        self.controller_submissions.append((request, idempotency_key))
        job_id = f"controller-{len(self.controller_submissions)}"
        self.controller_jobs[job_id] = {"job_id": job_id, "state": "queued", "cleanup_verified": False}
        return {"job_id": job_id}

    def submit(self, *, request, idempotency_key):
        self.submissions.append((request, idempotency_key))
        if self.raise_submit:
            raise TimeoutError("fixture timeout")
        if self.mutate_source:
            captured = Path(request["source_dir"]) / "src" / "subject.py"
            captured.chmod(0o600)
            captured.write_text("answer = 43\n", encoding="utf-8")
        job_id = "job-1"
        self.jobs[job_id] = {"job_id": job_id, "state": self.state,
                             "measurements": {"elapsed_ms": 2}, "cleanup_verified": self.cleanup_verified}
        return {"job_id": job_id}

    def inspect(self, job_id):
        if job_id in self.controller_jobs:
            return self.controller_jobs[job_id]
        return {**self.jobs[job_id], "state": self.state}

    def collect(self, job_id):
        return self.inspect(job_id)

    def wait(self, job_id, timeout_seconds):
        return {"job": {**self.jobs[job_id], "state": self.state},
                "timed_out": self.state in {"queued", "running"}}

    def control(self, job_id, action):
        if job_id in self.controller_jobs:
            self.controller_jobs[job_id]["state"] = "cancelled"
        return {"job_id": job_id, "state": action}

    def schedule(self, *, request, idempotency_key):
        self.schedules.append((request, idempotency_key))
        return {"job_id": "schedule-job"}


def _fixture(tmp_path):
    source = tmp_path / "source"
    (source / "src").mkdir(parents=True)
    (source / "src" / "subject.py").write_text("answer = 42\n", encoding="utf-8")
    return source


def _proposal(digest, *, done=False):
    if done:
        return {"hypothesis": "", "source_citations": [], "artifacts": [], "argv": [],
                "measurements": [], "stop_rule": "", "done": True}
    return {"hypothesis": "check the example", "source_citations": [
                {"path": "src/subject.py", "sha256": digest}],
            "artifacts": [{"path": "test_generated.py", "content": "print('ok')\n"}],
            "argv": ["python3", "test_generated.py"], "measurements": ["elapsed_ms"],
            "stop_rule": "one execution", "done": False}


def _setup(tmp_path, *, planner=None, scheduler=None):
    root = tmp_path / "source"
    if not root.exists():
        root = _fixture(tmp_path)
    lab = Lab(tmp_path / "private-state", projects={
        "demo": Project("demo", root, "demo-repo")}, planner=planner,
        scheduler=scheduler)
    return lab, root


def test_preview_is_source_bound_and_does_not_infer_or_submit(tmp_path):
    planner, scheduler = Planner([]), Scheduler()
    lab, _ = _setup(tmp_path, planner=planner, scheduler=scheduler)
    preview = lab.preview(RunRequest(project="demo", objective="test source",
                                     source_paths=("src",), max_experiments=2))
    assert preview["authorized"] is False
    assert preview["source_count"] == 1
    assert planner.calls == [] and scheduler.submissions == [] and scheduler.controller_submissions == []
    captured = Path(lab._session(preview["session_id"])["capture"] and
                    json.loads(lab._session(preview["session_id"])["capture"])["root"])
    assert (captured / "src/subject.py").read_text() == "answer = 42\n"
    assert (captured / "src/subject.py").stat().st_mode & 0o222 == 0


def test_run_records_proposal_intent_and_scheduler_receipt(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()
    planner, scheduler = Planner([_proposal(digest)]), Scheduler()
    lab, _ = _setup(tmp_path, planner=planner, scheduler=scheduler)
    request = RunRequest(project="demo", objective="test it", source_paths=("src",), max_experiments=1)
    preview = lab.preview(request)
    queued = lab.run(session_id=preview["session_id"])
    assert queued["state"] == "queued" and queued["controller_job_id"] == "controller-1"
    assert planner.calls == [] and scheduler.submissions == []
    result = lab.drive(preview["session_id"])
    assert result["state"] == "budget_exhausted"
    assert len(planner.calls) == 1 and len(scheduler.submissions) == 1
    assert scheduler.submissions[0][1].startswith("project-lab:")
    assert scheduler.submissions[0][0]["source_dir"]
    assert scheduler.submissions[0][0]["argv"][1].endswith("test_generated.py")
    receipts = lab.results(preview["session_id"])
    assert receipts["effects"][0]["state"] == "succeeded"
    assert receipts["effects"][0]["receipt"]["measurements"] == {"elapsed_ms": 2}
    artifact_dir = Path(scheduler.submissions[0][0]["candidate_dir"])
    assert (artifact_dir / "test_generated.py").is_file()
    assert not (source / "test_generated.py").exists()


def test_unresolved_scheduler_submission_is_unknown_and_not_retried(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()
    planner, scheduler = Planner([_proposal(digest)]), Scheduler(raise_submit=True)
    lab, _ = _setup(tmp_path, planner=planner, scheduler=scheduler)
    preview = lab.preview(RunRequest(project="demo", objective="test", source_paths=("src",)))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "attention_required"
    assert lab.results(preview["session_id"])["effects"][0]["state"] == "unknown"
    assert len(scheduler.submissions) == 1
    with pytest.raises(ValueError):
        lab.run(session_id=preview["session_id"])
    assert len(scheduler.submissions) == 1


def test_done_proposal_finishes_without_effect(tmp_path):
    lab, _ = _setup(tmp_path, planner=Planner([_proposal("", done=True)]), scheduler=Scheduler())
    preview = lab.preview(RunRequest(project="demo", objective="done", source_paths=("src",)))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "completed"
    assert lab.results(preview["session_id"])["effects"] == []


def test_schedule_records_id_and_capture_at_occurrence_start(tmp_path):
    scheduler = Scheduler()
    lab, _ = _setup(tmp_path, scheduler=scheduler)
    request = ScheduleRequest(session=RunRequest(project="demo", objective="daily check",
        source_paths=("src",), resource=ResourceRequest()), recurrence_seconds=3600)
    result = lab.schedule(request)
    assert result["state"] == "scheduled"
    assert scheduler.schedules[0][0]["capture_source_at_start"] is True
    assert scheduler.schedules[0][1].startswith("project-lab:")


def test_compare_reports_recorded_metric_values(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()
    planner = Planner([_proposal(digest), _proposal(digest)])
    lab, _ = _setup(tmp_path, planner=planner, scheduler=Scheduler())
    ids = []
    for index in range(2):
        preview = lab.preview(RunRequest(project="demo", objective=f"run {index}", source_paths=("src",)))
        ids.append(preview["session_id"])
        lab.run(session_id=preview["session_id"])
        lab.drive(preview["session_id"])
    comparison = lab.compare(ids, ["elapsed_ms"])
    assert [row["measurements"]["elapsed_ms"] for row in comparison["sessions"]] == [[2], [2]]


def test_running_effect_continues_by_durable_controller_after_completion(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()
    planner = Planner([_proposal(digest), _proposal("", done=True)])
    scheduler = Scheduler(state="running")
    lab, _ = _setup(tmp_path, planner=planner, scheduler=scheduler)
    preview = lab.preview(RunRequest(project="demo", objective="measure then decide",
        source_paths=("src",), max_experiments=2))
    lab.run(session_id=preview["session_id"])
    first = lab.drive(preview["session_id"])
    assert first["state"] == "queued"
    assert len(planner.calls) == 1
    assert len(scheduler.controller_submissions) == 2  # start plus continuation

    scheduler.state = "succeeded"
    final = lab.drive(preview["session_id"])
    assert final["state"] == "completed"
    assert len(planner.calls) == 2
    assert planner.calls[1]["prior_results"][0]["receipt"]["cleanup_verified"] is True


def test_unverified_cleanup_stops_before_planning_next_iteration(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()
    planner = Planner([_proposal(digest), _proposal("", done=True)])
    scheduler = Scheduler(cleanup_verified=False)
    lab, _ = _setup(tmp_path, planner=planner, scheduler=scheduler)
    preview = lab.preview(RunRequest(project="demo", objective="do not reuse uncertain capacity",
        source_paths=("src",), max_experiments=2))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "attention_required"
    assert len(planner.calls) == 1
    assert lab.results(preview["session_id"])["effects"][0]["state"] == "unknown"


def test_pause_and_resume_reconcile_controller_before_scheduling_continuation(tmp_path):
    lab, _ = _setup(tmp_path, planner=Planner([]), scheduler=Scheduler())
    preview = lab.preview(RunRequest(project="demo", objective="pause safely", source_paths=("src",)))
    started = lab.run(session_id=preview["session_id"])
    paused = lab.control(ControlRequest(session_id=preview["session_id"], action="pause"))
    assert paused["state"] == "paused"
    resumed = lab.control(ControlRequest(session_id=preview["session_id"], action="resume"))
    assert resumed["state"] == "queued"
    assert resumed["controller_job_id"] != started["controller_job_id"]
    assert len(lab.scheduler.controller_submissions) == 2


def test_proposal_cannot_escape_authorized_executable_allowlist(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()
    proposal = _proposal(digest)
    proposal["argv"] = ["bash", "test_generated.py"]
    planner, scheduler = Planner([proposal]), Scheduler()
    lab, _ = _setup(tmp_path, planner=planner, scheduler=scheduler)
    preview = lab.preview(RunRequest(project="demo", objective="stay in Python",
        source_paths=("src",), allowed_tools=("python3",)))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "paused" and result["reason"].startswith("planner_failed:")
    assert scheduler.submissions == []


def test_planner_timeout_is_durable_and_explicitly_resumable(tmp_path):
    planner = RaisingPlanner(TimeoutError("Codex planner timed out after 30 seconds"))
    lab, _ = _setup(tmp_path, planner=planner, scheduler=Scheduler())
    preview = lab.preview(RunRequest(project="demo", objective="test timeout", source_paths=("src",)))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "paused"
    assert "Codex planner timed out" in result["reason"]
    assert "control(action='resume')" in result["reason"]
    failures = [e for e in lab.results(preview["session_id"])["events"] if e["kind"] == "failure_receipt"]
    assert failures[0]["payload"]["exception_type"] == "TimeoutError"
    assert "Codex planner timed out" in failures[0]["payload"]["traceback"]
    assert planner.calls == 1 and lab.results(preview["session_id"])["effects"] == []


def test_planner_finishing_after_wall_deadline_cannot_start_an_effect(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()
    now = [100.0]

    class SlowPlanner:
        def propose(self, request):
            now[0] += 6.0
            return _proposal(digest)

    scheduler = Scheduler()
    lab = Lab(tmp_path / "private-state", projects={"demo": Project("demo", source, "demo-repo")},
              planner=SlowPlanner(), scheduler=scheduler, clock=lambda: now[0])
    preview = lab.preview(RunRequest(project="demo", objective="bounded plan",
                                     source_paths=("src",), max_experiments=1,
                                     max_wall_seconds=5))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "budget_exhausted"
    assert result["reason"] == "planner_exceeded_wall_budget"
    assert scheduler.submissions == []
    events = lab.results(preview["session_id"])["events"]
    assert any(event["kind"] == "planner_result_discarded" for event in events)


def test_malformed_proposal_is_durable_and_resumable(tmp_path):
    lab, _ = _setup(tmp_path, planner=Planner([{"not": "a proposal"}]), scheduler=Scheduler())
    preview = lab.preview(RunRequest(project="demo", objective="test invalid output", source_paths=("src",)))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    failure = next(e for e in lab.results(preview["session_id"])["events"] if e["kind"] == "failure_receipt")
    assert result["state"] == "paused"
    assert failure["payload"]["exception_type"] == "LabError"
    assert "documented schema" in failure["payload"]["message"]
    assert lab.results(preview["session_id"])["effects"] == []


def test_capture_mutated_by_planner_is_rejected_before_effect_submission(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()

    class MutatingPlanner(Planner):
        def propose(self, request):
            self.calls.append(request)
            captured = Path(request["source_root"]) / "src" / "subject.py"
            captured.chmod(0o600)
            captured.write_text("answer = 99\n", encoding="utf-8")
            return _proposal(digest)

    scheduler = Scheduler()
    lab, _ = _setup(tmp_path, planner=MutatingPlanner([]), scheduler=scheduler)
    preview = lab.preview(RunRequest(project="demo", objective="detect changed capture",
        source_paths=("src",)))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "attention_required"
    assert "captured source changed" in result["reason"]
    assert scheduler.submissions == []


def test_capture_mutated_during_job_is_rejected_after_cleanup_receipt(tmp_path):
    source = _fixture(tmp_path)
    digest = hashlib.sha256((source / "src/subject.py").read_bytes()).hexdigest()
    scheduler = Scheduler(mutate_source=True)
    planner = Planner([_proposal(digest)])
    lab, _ = _setup(tmp_path, planner=planner, scheduler=scheduler)
    preview = lab.preview(RunRequest(project="demo", objective="detect job mutation",
        source_paths=("src",), max_experiments=2))
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "attention_required"
    assert "capture_integrity_failure:after_effect" in result["reason"]
    assert len(lab.results(preview["session_id"])["effects"]) == 1
    assert lab.results(preview["session_id"])["effects"][0]["state"] == "succeeded"
    assert len(planner.calls) == 1


def test_capture_manifest_identity_is_checked_not_reconstructed_from_changed_files(tmp_path):
    lab, _ = _setup(tmp_path, planner=Planner([_proposal("0" * 64)]), scheduler=Scheduler())
    preview = lab.preview(RunRequest(project="demo", objective="detect manifest tampering",
        source_paths=("src",)))
    row = lab._session(preview["session_id"])
    capture = json.loads(row["capture"])
    capture["files"][0]["sha256"] = "0" * 64
    db = lab.journal.connect()
    try:
        db.execute("UPDATE sessions SET capture=? WHERE id=?",
                   (json.dumps(capture), preview["session_id"]))
        db.commit()
    finally:
        db.close()
    lab.run(session_id=preview["session_id"])
    result = lab.drive(preview["session_id"])
    assert result["state"] == "attention_required"
    assert "manifest no longer matches" in result["reason"]
    assert lab.results(preview["session_id"])["effects"] == []
