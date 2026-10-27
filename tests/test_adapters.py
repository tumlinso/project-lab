from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from project_lab.adapters import CodexExecPlanner, DeviceSchedulerAdapter


def test_codex_adapter_is_bounded_read_only_and_captures_fake_events(tmp_path, monkeypatch):
    output = {"hypothesis": "", "source_citations": [], "artifacts": [], "argv": [],
              "measurements": [], "stop_rule": "", "done": True}
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        Path(argv[argv.index("-o") + 1]).write_text(json.dumps(output))
        return SimpleNamespace(returncode=0, stdout='{"type":"turn.completed"}\n', stderr="")

    monkeypatch.setattr("project_lab.adapters.subprocess.run", fake_run)
    planner = CodexExecPlanner(cwd=tmp_path)
    assert planner.propose({"objective": "stop"})["done"] is True
    argv, kwargs = calls[0]
    assert argv[:2] == ["codex", "exec"]
    assert "--sandbox" in argv and argv[argv.index("--sandbox") + 1] == "read-only"
    assert "--ignore-user-config" in argv
    assert argv[argv.index("-m") + 1] == "gpt-6-luna"
    assert argv[argv.index("-c") + 1] == "model_reasoning_effort=low"
    assert "env" not in kwargs
    assert planner.events == [{"type": "turn.completed"}]


def test_codex_timeout_is_capped_by_remaining_session_budget(tmp_path, monkeypatch):
    output = {"hypothesis": "", "source_citations": [], "artifacts": [], "argv": [],
              "measurements": [], "stop_rule": "", "done": True}
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(kwargs)
        Path(argv[argv.index("-o") + 1]).write_text(json.dumps(output))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("project_lab.adapters.subprocess.run", fake_run)
    planner = CodexExecPlanner(cwd=tmp_path, timeout_seconds=120)
    planner.propose({"objective": "stop", "remaining_seconds": 3})
    assert calls[0]["timeout"] == 3

    calls.clear()
    import pytest
    with pytest.raises(TimeoutError, match="wall budget is exhausted"):
        planner.propose({"objective": "stop", "remaining_seconds": 0})
    assert calls == []


def test_scheduler_adapter_passes_durable_idempotency_and_maps_paths():
    class Client:
        async def submit_job(self, spec):
            self.spec = spec
            return {"job": {"id": "job-9"}}

        async def get_job(self, job_id):
            return {"job": {"id": job_id, "state": "succeeded"}}

        async def job_logs(self, job_id, **options):
            return {"stdout": "ok", "stderr": ""}

        async def control_job(self, job_id, action):
            return {"job_id": job_id, "state": action}

    client = Client()
    scheduler = DeviceSchedulerAdapter(client, owner_id="lab-owner")
    result = scheduler.submit(request={"project": "demo", "candidate_dir": "/tmp/candidate",
        "argv": ["python3", "/tmp/candidate/test.py"], "resource": {
            "cpus": 2, "memory_mb": 2048, "gpu_count": 0, "wall_seconds": 60}},
        idempotency_key="project-lab:session-1:proposal-1")
    assert result["job_id"] == "job-9"
    assert client.spec["idempotency_key"] == "project-lab:session-1:proposal-1"
    assert client.spec["resources"][0]["quantities"]["cores"] == 2
    assert scheduler.inspect("job-9")["state"] == "succeeded"
    assert scheduler.collect("job-9")["logs"]["stdout"] == "ok"
