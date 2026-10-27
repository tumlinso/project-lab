"""Application API for bounded experiments, scheduling, and recovery."""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import time
import traceback
import uuid
from typing import Any, Mapping, Sequence

from project_evidence import EvidenceStore

from .adapters import Planner, Scheduler, SourceProvider
from .adapters import ProjectSourceProvider
from .models import (ControlRequest, PreviewResult, Project, RunRequest,
                     ScheduleRequest, SessionResult)
from .store import Journal


class LabError(ValueError):
    """Invalid scope or operation against a Project-Lab record."""


class CaptureIntegrityError(LabError):
    """Captured inputs no longer match their recorded identities."""


class Lab:
    """One application API used by Python, MCP, and CLI adapters."""
    def __init__(self, state_dir: str | Path, *, projects: Mapping[str, Project] | None = None,
                 planner: Planner | None = None, scheduler: Scheduler | None = None,
                 source: SourceProvider | None = None, clock=time.time):
        self.journal = Journal(state_dir)
        self.evidence = EvidenceStore(self.journal.root / "evidence.sqlite3",
                                      self.journal.root / "evidence-artifacts")
        self.projects = dict(projects or {})
        self.planner, self.scheduler = planner, scheduler
        self.source = source or ProjectSourceProvider(self.journal.root)
        self.clock = clock

    @classmethod
    def open(cls, state_dir: str | Path, **kwargs: Any) -> "Lab":
        return cls(Path(state_dir).expanduser(), **kwargs)

    def register(self, project: Project) -> None:
        project.resolved_root()
        self.projects[project.name] = project

    def preview(self, request: RunRequest | Mapping[str, Any]) -> dict[str, Any]:
        request = _request(request)
        project = self._project(request.project)
        session_id = "lab_" + uuid.uuid4().hex
        capture_dir = self.journal.root / "captures" / session_id
        capture_dir.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        captured = self.source.capture(project.name, project.resolved_root(), request.source_paths, capture_dir)
        now = float(self.clock())
        db = self.journal.connect()
        try:
            db.execute("INSERT INTO sessions(id,project,request,state,source_identity,capture,created,updated) VALUES(?,?,?,?,?,?,?,?)",
                       (session_id, project.name, request.model_dump_json(), "preview",
                        json.dumps(captured["identity"], sort_keys=True), json.dumps(captured, sort_keys=True), now, now))
            self.journal.emit(db, session_id, "previewed", {"source_identity": captured["identity"], "source_count": len(captured["files"])})
            db.commit()
        finally:
            db.close()
        return PreviewResult(session_id=session_id, project=project.name, repository=project.repository,
                             objective=request.objective, source_identity=dict(captured["identity"]),
                             source_count=len(captured["files"]), resource=request.resource,
                             max_experiments=request.max_experiments,
                             max_wall_seconds=request.max_wall_seconds).model_dump(mode="json")

    def run(self, request: RunRequest | Mapping[str, Any] | None = None, *, session_id: str | None = None,
            _resume: bool = False) -> dict[str, Any]:
        """Authorize a finite scope and submit its durable session controller."""
        if self.planner is None or self.scheduler is None:
            raise LabError("run requires configured Codex planner and Device Scheduler adapters")
        if not hasattr(self.scheduler, "submit_controller"):
            raise LabError("scheduler adapter cannot start a session controller")
        if session_id is None:
            if request is None:
                raise LabError("run requires a scope or preview session ID")
            preview = self.preview(request)
            session_id = preview["session_id"]
        db = self.journal.connect()
        try:
            row = db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            if row is None:
                raise LabError("session not found")
            resumable = {"paused", "running", "queued", "attention_required"}
            if row["state"] != "preview" and not (_resume and row["state"] in resumable):
                raise LabError(f"session cannot be run from state {row['state']}")
            if _resume:
                if not row["controller_job_id"]:
                    db.execute("UPDATE sessions SET state='attention_required',reason='controller_job_id_missing',updated=? WHERE id=?",
                               (float(self.clock()), session_id))
                    db.commit()
                    return self.status(session_id)["sessions"][0]
                try:
                    controller_state = str(self.scheduler.inspect(row["controller_job_id"]).get("state", "unknown"))
                except Exception as exc:
                    return self._finish(session_id, "attention_required", f"controller_inspection_unknown:{exc}")
                if controller_state in {"queued", "blocked", "admitted", "running", "suspend_requested", "suspended"}:
                    return self.status(session_id)["sessions"][0]
                if controller_state == "unknown":
                    return self._finish(session_id, "attention_required", "controller_outcome_unknown")
            scope = RunRequest.model_validate_json(row["request"])
            now = float(self.clock())
            deadline = float(row["deadline"]) if row["state"] != "preview" and row["deadline"] is not None else now + scope.max_wall_seconds
            controller_key = f"project-lab:controller:{session_id}:{'resume' if _resume else 'start'}"
            if row["state"] == "preview":
                db.execute("UPDATE sessions SET state='starting',deadline=?,controller_key=?,updated=? WHERE id=?",
                           (deadline, controller_key, now, session_id))
                self.journal.emit(db, session_id, "authorized", {"deadline": deadline,
                                  "scope": scope.model_dump(mode="json"), "controller_key": controller_key})
            else:
                controller_key = f"project-lab:controller:{session_id}:resume:{uuid.uuid4().hex}"
                db.execute("UPDATE sessions SET state='starting',controller_key=?,updated=? WHERE id=?",
                           (controller_key, now, session_id))
                self.journal.emit(db, session_id, "resumed", {"controller_key": controller_key})
            db.commit()
        finally:
            db.close()

        try:
            submitted = self.scheduler.submit_controller(
                request={"session_id": session_id, "run": scope.model_dump(mode="json"),
                         "source_identity": json.loads(row["source_identity"]),
                         "deadline": deadline}, idempotency_key=controller_key)
        except Exception as exc:
            return self._finish(session_id, "attention_required", f"controller_submission_unknown:{exc}")
        controller_id = submitted.get("job_id")
        if not isinstance(controller_id, str) or not controller_id:
            return self._finish(session_id, "attention_required", "controller_submission_missing_job_id")
        db = self.journal.connect()
        try:
            db.execute("UPDATE sessions SET state=CASE WHEN state='starting' THEN 'queued' ELSE state END,controller_job_id=?,updated=? WHERE id=?",
                       (controller_id, float(self.clock()), session_id))
            self.journal.emit(db, session_id, "controller_submitted", {"controller_job_id": controller_id,
                              "idempotency_key": controller_key})
            db.commit()
        finally:
            db.close()
        return self.status(session_id)["sessions"][0]

    def drive(self, session_id: str) -> dict[str, Any]:
        """Worker entry point: reconcile existing effects, then drive to a bounded boundary."""
        if self.planner is None or self.scheduler is None:
            raise LabError("worker requires configured Codex planner and Device Scheduler adapters")
        db = self.journal.connect()
        try:
            row = db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            if row is None:
                raise LabError("session not found")
            if row["state"] not in {"starting", "queued", "running", "paused"}:
                raise LabError(f"session worker cannot start from state {row['state']}")
            scope = RunRequest.model_validate_json(row["request"])
            deadline = float(row["deadline"] or self.clock())
            capture = json.loads(row["capture"])
            db.execute("UPDATE sessions SET state='running',updated=? WHERE id=?", (float(self.clock()), session_id))
            db.commit()
        finally:
            db.close()

        # Never replay an intent. Existing jobs are waited on and inspected by durable ID.
        existing = self._pending_effects(session_id)
        for effect in existing:
            if not effect["scheduler_job_id"]:
                return self._effect_unknown(effect["id"], session_id, "intent has no recorded scheduler job ID")
            outcome = self._wait_for_effect(effect["scheduler_job_id"], deadline)
            if outcome is None:
                return self._queue_continuation(session_id, "effect_in_progress")
            if outcome.get("cleanup_verified") is not True:
                return self._effect_unknown(effect["id"], session_id, "scheduler did not verify execution cleanup")
            self._save_receipt(effect["id"], session_id, effect["proposal_id"], outcome, capture)
            try:
                _verify_capture(capture)
            except CaptureIntegrityError as exc:
                return self._integrity_failure(session_id, "after_effect_reconciliation", exc)

        try:
            _verify_capture(capture)
        except CaptureIntegrityError as exc:
            return self._integrity_failure(session_id, "before_planning", exc)

        initial = int(self._session(session_id)["count"])
        for ordinal in range(initial, scope.max_experiments):
            if float(self.clock()) >= deadline:
                return self._finish(session_id, "budget_exhausted", "wall_budget_exhausted")
            current = self._session(session_id)
            if current["state"] == "cancel_requested":
                return self._finish(session_id, "cancelled", "cancel_requested")
            planner_input = {"session_id": session_id, "project": scope.project,
                             "objective": scope.objective, "sources": capture["files"],
                             "source_identity": capture["identity"], "source_root": capture["root"],
                             "resource": scope.resource.model_dump(mode="json"),
                             "measurements": scope.measurements,
                             "prior_results": self._prior_results(session_id),
                             "remaining_experiments": scope.max_experiments - ordinal,
                             "remaining_seconds": max(0, int(deadline - float(self.clock())))}
            try:
                proposal = _validate_proposal(self.planner.propose(planner_input), capture)
                if not proposal["done"] and proposal["argv"][0] not in scope.allowed_tools:
                    raise LabError("proposal executable is outside the run's allowed_tools")
                _verify_capture(capture)
            except Exception as exc:
                if isinstance(exc, CaptureIntegrityError):
                    return self._integrity_failure(session_id, "after_planning", exc)
                return self._failure(session_id, "planner", exc,
                                     state="budget_exhausted" if float(self.clock()) >= deadline else "paused")
            if float(self.clock()) >= deadline:
                self._event(session_id, "planner_result_discarded", {
                    "reason": "wall_budget_exhausted_during_planning",
                    "proposal_hypothesis": proposal["hypothesis"][:500],
                })
                return self._finish(session_id, "budget_exhausted", "planner_exceeded_wall_budget")
            for event in getattr(self.planner, "events", ()):
                self._event(session_id, "planner_event", event)
            proposal_id = "proposal_" + uuid.uuid4().hex
            db = self.journal.connect()
            try:
                db.execute("INSERT INTO proposals(id,session_id,ordinal,proposal,created) VALUES(?,?,?,?,?)",
                           (proposal_id, session_id, ordinal, json.dumps(proposal, sort_keys=True), float(self.clock())))
                db.execute("UPDATE sessions SET count=count+1,updated=? WHERE id=?", (float(self.clock()), session_id))
                self.journal.emit(db, session_id, "proposal_recorded", {"proposal_id": proposal_id, "ordinal": ordinal})
                db.commit()
            finally:
                db.close()
            if proposal["done"]:
                return self._finish(session_id, "completed", "planner_done")

            proposal_dir = self.journal.root / "candidates" / proposal_id
            _write_candidate(proposal_dir, proposal["artifacts"])
            if self.evidence is not None:
                self._record_proposal_evidence(session_id, proposal_id, proposal, capture)
            try:
                _verify_capture(capture)
            except CaptureIntegrityError as exc:
                return self._integrity_failure(session_id, "before_effect_submission", exc)
            if float(self.clock()) >= deadline:
                return self._finish(session_id, "budget_exhausted", "wall_budget_exhausted_before_effect_submission")
            effect_id = "effect_" + uuid.uuid4().hex
            idem = f"project-lab:{session_id}:{proposal_id}"
            # Commit intent and idempotency key before any external scheduler effect.
            db = self.journal.connect()
            try:
                db.execute("INSERT INTO effects(id,session_id,proposal_id,idempotency_key,state,created,updated) VALUES(?,?,?,?,?,?,?)",
                           (effect_id, session_id, proposal_id, idem, "intent", float(self.clock()), float(self.clock())))
                self.journal.emit(db, session_id, "effect_intent", {"effect_id": effect_id, "idempotency_key": idem})
                db.commit()
            finally:
                db.close()
            # Journal persistence must not allow a delayed planner turn to
            # cross the run deadline and start new external work.
            if float(self.clock()) >= deadline:
                db = self.journal.connect()
                try:
                    db.execute("UPDATE effects SET state='not_submitted',updated=? WHERE id=? AND state='intent'",
                               (float(self.clock()), effect_id))
                    self.journal.emit(db, session_id, "effect_not_submitted", {
                        "effect_id": effect_id, "reason": "wall_budget_exhausted_before_scheduler_submission"})
                    db.commit()
                finally:
                    db.close()
                return self._finish(session_id, "budget_exhausted", "wall_budget_exhausted_before_effect_submission")
            try:
                submitted = self.scheduler.submit(
                    request={"project": scope.project, "candidate_dir": str(proposal_dir),
                             "source_dir": capture["root"],
                             "argv": _map_candidate_argv(proposal["argv"], proposal_dir, capture["root"]),
                             "allowed_tools": list(scope.allowed_tools),
                             "resource": scope.resource.model_dump(mode="json"),
                             "source_identity": capture["identity"], "effect_id": effect_id},
                    idempotency_key=idem)
            except Exception as exc:
                return self._effect_unknown(effect_id, session_id, str(exc))
            job_id = submitted.get("job_id")
            if not isinstance(job_id, str) or not job_id:
                return self._effect_unknown(effect_id, session_id, "scheduler returned no durable job ID")
            db = self.journal.connect()
            try:
                db.execute("UPDATE effects SET state='submitted',scheduler_job_id=?,updated=? WHERE id=?",
                           (job_id, float(self.clock()), effect_id))
                self.journal.emit(db, session_id, "effect_submitted", {"effect_id": effect_id, "job_id": job_id})
                db.commit()
            finally:
                db.close()
            receipt = self._wait_for_effect(job_id, deadline)
            if receipt is None:
                return self._queue_continuation(session_id, "effect_in_progress")
            if receipt.get("cleanup_verified") is not True:
                return self._effect_unknown(effect_id, session_id, "scheduler did not verify execution cleanup")
            self._save_receipt(effect_id, session_id, proposal_id, receipt, capture)
            if receipt.get("state") not in {"succeeded", "failed", "cancelled"}:
                return self._effect_unknown(effect_id, session_id, "scheduler returned a non-terminal effect result")
            try:
                _verify_capture(capture)
            except CaptureIntegrityError as exc:
                return self._integrity_failure(session_id, "after_effect", exc)
        return self._finish(session_id, "budget_exhausted", "experiment_budget_exhausted")

    def _pending_effects(self, session_id: str):
        db = self.journal.connect()
        try:
            return [dict(row) for row in db.execute(
                "SELECT * FROM effects WHERE session_id=? AND state IN ('intent','submitted','running','queued','unknown') ORDER BY created",
                (session_id,)).fetchall()]
        finally:
            db.close()

    def _prior_results(self, session_id: str) -> list[dict[str, Any]]:
        db = self.journal.connect()
        try:
            rows = db.execute("SELECT id,proposal_id,state,scheduler_job_id,receipt FROM effects WHERE session_id=? ORDER BY created",
                              (session_id,)).fetchall()
            return [{"effect_id": row["id"], "proposal_id": row["proposal_id"],
                     "state": row["state"], "scheduler_job_id": row["scheduler_job_id"],
                     "receipt": json.loads(row["receipt"]) if row["receipt"] else None}
                    for row in rows]
        finally:
            db.close()

    def _wait_for_effect(self, job_id: str, deadline: float) -> dict[str, Any] | None:
        remaining = max(0.0, deadline - float(self.clock()))
        if remaining <= 0:
            return None
        if hasattr(self.scheduler, "wait"):
            waited = dict(self.scheduler.wait(job_id, min(300.0, remaining)))
            if waited.get("timed_out") is True and float(self.clock()) >= deadline:
                return None
        observed = dict(self.scheduler.inspect(job_id))
        state = str(observed.get("state", "unknown"))
        if state not in {"succeeded", "failed", "cancelled"}:
            return None
        if hasattr(self.scheduler, "collect"):
            observed = dict(self.scheduler.collect(job_id))
        return observed

    def _save_receipt(self, effect_id: str, session_id: str, proposal_id: str,
                      receipt: Mapping[str, Any], capture: Mapping[str, Any]) -> None:
        state = str(receipt.get("state", "unknown"))
        db = self.journal.connect()
        try:
            db.execute("UPDATE effects SET state=?,receipt=?,updated=? WHERE id=?",
                       (state, json.dumps(receipt, sort_keys=True), float(self.clock()), effect_id))
            self.journal.emit(db, session_id, "effect_receipt", {"effect_id": effect_id, "receipt": receipt})
            db.commit()
        finally:
            db.close()
        if self.evidence is not None:
            self._record_receipt_evidence(session_id, effect_id, proposal_id, receipt, capture)

    def _queue_continuation(self, session_id: str, reason: str) -> dict[str, Any]:
        row = self._session(session_id)
        if self.scheduler is None or not hasattr(self.scheduler, "submit_controller"):
            return self._finish(session_id, "attention_required", "controller_continuation_unavailable")
        key = f"project-lab:controller:{session_id}:continue:{uuid.uuid4().hex}"
        request = {"session_id": session_id,
                   "run": RunRequest.model_validate_json(row["request"]).model_dump(mode="json")}
        try:
            submitted = self.scheduler.submit_controller(request=request, idempotency_key=key)
        except Exception as exc:
            return self._finish(session_id, "attention_required", f"controller_continuation_unknown:{exc}")
        controller_id = submitted.get("job_id")
        if not isinstance(controller_id, str) or not controller_id:
            return self._finish(session_id, "attention_required", "controller_continuation_missing_job_id")
        db = self.journal.connect()
        try:
            db.execute("""UPDATE sessions SET state=CASE WHEN state IN ('running','starting') THEN 'queued' ELSE state END,
                         controller_job_id=?,controller_key=?,updated=?,reason=? WHERE id=?""",
                       (controller_id, key, float(self.clock()), reason, session_id))
            self.journal.emit(db, session_id, "controller_continued", {"controller_job_id": controller_id,
                              "idempotency_key": key, "reason": reason})
            db.commit()
        finally:
            db.close()
        return self.status(session_id)["sessions"][0]

    def status(self, session_id: str | None = None) -> dict[str, Any]:
        db = self.journal.connect()
        try:
            rows = ([db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()]
                    if session_id else db.execute("SELECT * FROM sessions ORDER BY created DESC LIMIT 100").fetchall())
            if session_id and rows[0] is None:
                raise LabError("session not found")
            sessions = [self._public_session(row) for row in rows if row is not None]
            if session_id and rows[0]["controller_job_id"] and self.scheduler is not None:
                try:
                    observed = dict(self.scheduler.inspect(rows[0]["controller_job_id"]))
                    sessions[0]["scheduler_controller"] = {
                        "job_id": rows[0]["controller_job_id"],
                        "state": observed.get("state", "unknown"),
                        "cleanup_verified": observed.get("cleanup_verified"),
                    }
                except Exception as exc:
                    sessions[0]["scheduler_controller"] = {
                        "job_id": rows[0]["controller_job_id"], "state": "unavailable",
                        "reason": type(exc).__name__,
                    }
            return {"sessions": sessions}
        finally:
            db.close()

    def results(self, session_id: str, *, limit: int = 100) -> dict[str, Any]:
        if limit < 1 or limit > 1000:
            raise LabError("limit must be between 1 and 1000")
        db = self.journal.connect()
        try:
            if db.execute("SELECT 1 FROM sessions WHERE id=?", (session_id,)).fetchone() is None:
                raise LabError("session not found")
            proposals = db.execute("SELECT id,ordinal,proposal,created FROM proposals WHERE session_id=? ORDER BY ordinal LIMIT ?", (session_id, limit)).fetchall()
            effects = db.execute("SELECT id,proposal_id,state,scheduler_job_id,receipt,created,updated FROM effects WHERE session_id=? ORDER BY created LIMIT ?", (session_id, limit)).fetchall()
            events = db.execute("SELECT kind,at,payload FROM events WHERE session_id=? ORDER BY id LIMIT ?", (session_id, limit)).fetchall()
            return {"session_id": session_id,
                    "proposals": [{**dict(p), "proposal": json.loads(p["proposal"])} for p in proposals],
                    "effects": [{**dict(e), "receipt": json.loads(e["receipt"]) if e["receipt"] else None} for e in effects],
                    "events": [{**dict(e), "payload": json.loads(e["payload"])} for e in events]}
        finally:
            db.close()

    def compare(self, session_ids: Sequence[str], metrics: Sequence[str] = ()) -> dict[str, Any]:
        if not 2 <= len(session_ids) <= 32:
            raise LabError("compare requires 2 to 32 session IDs")
        all_results = [self.results(item) for item in session_ids]
        rows = []
        for result in all_results:
            effects = result["effects"]
            receipts = [e["receipt"] for e in effects if e["receipt"]]
            measurements = {}
            for receipt in receipts:
                values = receipt.get("measurements", {})
                for key, value in values.items():
                    if not metrics or key in metrics:
                        measurements.setdefault(key, []).append(value)
            rows.append({"session_id": result["session_id"], "effects": len(effects), "measurements": measurements})
        return {"sessions": rows}

    def control(self, request: ControlRequest | Mapping[str, Any]) -> dict[str, Any]:
        req = request if isinstance(request, ControlRequest) else ControlRequest.model_validate(request)
        row = self._session(req.session_id)
        if req.action == "resume":
            if row["state"] not in {"paused", "running", "queued", "attention_required"}:
                raise LabError(f"cannot resume session in state {row['state']}")
            return self.run(session_id=req.session_id, _resume=True)
        db = self.journal.connect()
        try:
            active = db.execute("SELECT scheduler_job_id FROM effects WHERE session_id=? AND state IN ('submitted','running','queued') AND scheduler_job_id IS NOT NULL", (req.session_id,)).fetchall()
            controller = db.execute("SELECT controller_job_id FROM sessions WHERE id=?", (req.session_id,)).fetchone()
        finally:
            db.close()
        if controller and controller[0] and self.scheduler is not None:
            try:
                self.scheduler.control(controller[0], "cancel" if req.action == "pause" else req.action)
            except Exception as exc:
                return self._finish(req.session_id, "attention_required", f"controller_control_unknown:{exc}")
        for job in active:
            try:
                self.scheduler.control(job["scheduler_job_id"], "cancel" if req.action == "pause" else req.action)
            except Exception as exc:
                return self._finish(req.session_id, "attention_required", f"scheduler_control_unknown:{exc}")
        db = self.journal.connect()
        try:
            if req.action == "pause":
                db.execute("UPDATE sessions SET state='paused',updated=?,reason='operator_pause' WHERE id=? AND state IN ('starting','queued','running')", (float(self.clock()), req.session_id))
            else:
                db.execute("UPDATE sessions SET state='cancel_requested',updated=?,reason='operator_cancel' WHERE id=? AND state IN ('starting','queued','running','paused','preview')", (float(self.clock()), req.session_id))
            db.commit()
        finally:
            db.close()
        return self.status(req.session_id)["sessions"][0]

    def schedule(self, request: ScheduleRequest | Mapping[str, Any]) -> dict[str, Any]:
        if self.scheduler is None:
            raise LabError("schedule requires a Device Scheduler adapter")
        request_obj = request if isinstance(request, ScheduleRequest) else None
        request_map = request.model_dump(mode="json") if request_obj else request
        if isinstance(request_map, Mapping) and request_map.get("schedule_id"):
            schedule_id = str(request_map["schedule_id"])
            db = self.journal.connect()
            try:
                row = db.execute("SELECT * FROM schedules WHERE id=?", (schedule_id,)).fetchone()
            finally:
                db.close()
            if row is None:
                raise LabError("schedule not found")
            if request_map.get("action") in {"pause", "cancel", "resume"}:
                if row["kind"] == "recurring" and hasattr(self.scheduler, "control_schedule"):
                    result = self.scheduler.control_schedule(row["scheduler_job_id"], str(request_map["action"]))
                else:
                    result = self.scheduler.control(row["scheduler_job_id"], str(request_map["action"]))
                db = self.journal.connect()
                try:
                    db.execute("UPDATE schedules SET state=?,updated=? WHERE id=?",
                               (str(result.get("state", request_map["action"])), float(self.clock()), schedule_id))
                    db.commit()
                finally:
                    db.close()
                return {"schedule_id": schedule_id, **result}
            return {"schedule_id": schedule_id, "state": row["state"],
                    "scheduler_job_id": row["scheduler_job_id"],
                    "kind": row["kind"],
                    "request": json.loads(row["request"])}
        req = request if isinstance(request, ScheduleRequest) else ScheduleRequest.model_validate(request)
        if req.session is None:
            raise LabError("schedule creation requires a session request")
        self._project(req.session.project)
        schedule_id = "schedule_" + uuid.uuid4().hex
        idem = f"project-lab:{schedule_id}"
        db = self.journal.connect()
        try:
            schedule_kind = "recurring" if req.recurrence_seconds is not None else "delayed"
            db.execute("INSERT INTO schedules(id,request,state,kind,created,updated) VALUES(?,?,?,?,?,?)",
                       (schedule_id, req.model_dump_json(), "intent", schedule_kind,
                        float(self.clock()), float(self.clock())))
            db.commit()
        finally:
            db.close()
        # Scheduler captures source at occurrence start via this immutable run template.
        try:
            result = self.scheduler.schedule(request={"run": req.session.model_dump(mode="json"),
                                                      "start_at": req.start_at,
                                                      "recurrence_seconds": req.recurrence_seconds,
                                                      "capture_source_at_start": True}, idempotency_key=idem)
        except Exception as exc:
            db = self.journal.connect()
            try:
                db.execute("UPDATE schedules SET state='unknown',updated=? WHERE id=?", (float(self.clock()), schedule_id))
                db.commit()
            finally:
                db.close()
            return {"schedule_id": schedule_id, "state": "unknown", "error": str(exc), "idempotency_key": idem}
        db = self.journal.connect()
        try:
            db.execute("UPDATE schedules SET state='scheduled',scheduler_job_id=?,updated=? WHERE id=?",
                       (result.get("job_id"), float(self.clock()), schedule_id))
            db.commit()
        finally:
            db.close()
        return {"schedule_id": schedule_id, "state": "scheduled", **result}

    def _project(self, name: str) -> Project:
        project = self.projects.get(name)
        if project is None:
            raise LabError(f"project is not registered: {name}")
        project.resolved_root()
        return project

    def _session(self, session_id: str):
        db = self.journal.connect()
        try:
            row = db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            if row is None:
                raise LabError("session not found")
            return dict(row)
        finally:
            db.close()

    def _public_session(self, row) -> dict[str, Any]:
        return SessionResult(session_id=row["id"], state=row["state"], project=row["project"],
                             created_at=row["created"], updated_at=row["updated"],
                             controller_job_id=row["controller_job_id"],
                             experiment_count=row["count"],
                             effects=tuple(e[0] for e in self._effect_ids(row["id"])),
                             reason=row["reason"]).model_dump(mode="json")

    def _effect_ids(self, session_id):
        db = self.journal.connect()
        try:
            return db.execute("SELECT id FROM effects WHERE session_id=? ORDER BY created", (session_id,)).fetchall()
        finally:
            db.close()

    def _finish(self, session_id: str, state: str, reason: str) -> dict[str, Any]:
        db = self.journal.connect()
        try:
            db.execute("UPDATE sessions SET state=?,reason=?,updated=? WHERE id=?",
                       (state, reason, float(self.clock()), session_id))
            self.journal.emit(db, session_id, "session_finished", {"state": state, "reason": reason})
            db.commit()
        finally:
            db.close()
        return self.status(session_id)["sessions"][0]

    def _effect_unknown(self, effect_id: str, session_id: str, reason: str) -> dict[str, Any]:
        db = self.journal.connect()
        try:
            db.execute("UPDATE effects SET state='unknown',updated=? WHERE id=?", (float(self.clock()), effect_id))
            self.journal.emit(db, session_id, "effect_unknown", {"effect_id": effect_id, "reason": reason})
            db.commit()
        finally:
            db.close()
        return self._finish(session_id, "attention_required", "effect_outcome_unknown")

    def _failure(self, session_id: str, stage: str, error: BaseException, *, state: str) -> dict[str, Any]:
        detail = {
            "stage": stage,
            "exception_type": type(error).__name__,
            "message": str(error)[:2000],
            "traceback": "".join(traceback.format_exception(type(error), error, error.__traceback__))[-8192:],
        }
        self._event(session_id, "failure_receipt", detail)
        reason = (f"{stage}_failed:{type(error).__name__}:{str(error)[:300]}; "
                  "inspect results.failure_receipt, then control(action='resume') to retry safely")
        return self._finish(session_id, state, reason)

    def _integrity_failure(self, session_id: str, stage: str, error: CaptureIntegrityError) -> dict[str, Any]:
        self._event(session_id, "capture_integrity_failure", {
            "stage": stage, "exception_type": type(error).__name__, "message": str(error)[:2000],
        })
        return self._finish(session_id, "attention_required", f"capture_integrity_failure:{stage}:{str(error)[:400]}")

    def _event(self, session_id: str, kind: str, payload: object) -> None:
        db = self.journal.connect()
        try:
            self.journal.emit(db, session_id, kind, payload)
            db.commit()
        finally:
            db.close()

    def _record_proposal_evidence(self, session_id: str, proposal_id: str,
                                  proposal: Mapping[str, Any], capture: Mapping[str, Any]) -> None:
        from project_evidence import Observation, SourceRef
        refs = tuple(SourceRef(repository_id=item["repository_id"], path=item["path"],
                               sha256=item["sha256"], revision=item.get("revision"),
                               worktree_id=item.get("worktree_id"))
                     for item in capture.get("source_refs", ()))
        artifacts = tuple(self.evidence.put_artifact(item["content"].encode("utf-8"), "text/plain; charset=utf-8")
                          for item in proposal["artifacts"])
        record = Observation(observation_id=proposal_id, owner="project-lab", kind="experiment_proposal",
                             subject=session_id, source_refs=refs, artifact_refs=artifacts,
                             observed_at=datetime.fromtimestamp(float(self.clock()), timezone.utc).isoformat(),
                             payload={"hypothesis": proposal["hypothesis"], "measurements": proposal["measurements"],
                                      "stop_rule": proposal["stop_rule"], "done": proposal["done"]})
        self.evidence.record_observation(record)

    def _record_receipt_evidence(self, session_id: str, effect_id: str, proposal_id: str,
                                 receipt: Mapping[str, Any], capture: Mapping[str, Any]) -> None:
        from project_evidence import Observation, SourceRef
        refs = tuple(SourceRef(repository_id=item["repository_id"], path=item["path"],
                               sha256=item["sha256"], revision=item.get("revision"),
                               worktree_id=item.get("worktree_id"))
                     for item in capture.get("source_refs", ()))
        artifact = self.evidence.put_artifact(json.dumps(receipt, sort_keys=True).encode(), "application/json")
        record = Observation(observation_id=effect_id, owner="project-lab", kind="experiment_receipt",
                             subject=session_id, source_refs=refs, artifact_refs=(artifact,),
                             observed_at=datetime.fromtimestamp(float(self.clock()), timezone.utc).isoformat(),
                             payload={"proposal_id": proposal_id, "state": receipt.get("state"),
                                      "job_id": receipt.get("job_id")})
        self.evidence.record_observation(record)


def _request(request: RunRequest | Mapping[str, Any]) -> RunRequest:
    return request if isinstance(request, RunRequest) else RunRequest.model_validate(request)


def _validate_proposal(raw: Mapping[str, Any], capture: Mapping[str, Any]) -> dict[str, Any]:
    required = {"hypothesis", "source_citations", "artifacts", "argv", "measurements", "stop_rule", "done"}
    if not isinstance(raw, Mapping) or set(raw) != required:
        raise LabError("planner proposal fields do not match the documented schema")
    if not isinstance(raw["done"], bool):
        raise LabError("proposal done must be boolean")
    for field in ("hypothesis", "stop_rule"):
        if not isinstance(raw[field], str) or len(raw[field].encode("utf-8")) > 8192:
            raise LabError(f"proposal {field} must be bounded text")
    for field, maximum in (("source_citations", 128), ("artifacts", 64),
                           ("argv", 64), ("measurements", 64)):
        if not isinstance(raw[field], list) or len(raw[field]) > maximum:
            raise LabError(f"proposal {field} must be a bounded array")
    try:
        payload = json.dumps(dict(raw), ensure_ascii=False, allow_nan=False)
        if len(payload.encode("utf-8")) > 8 * 1024 * 1024:
            raise LabError("planner proposal exceeds 8 MiB")
    except (TypeError, ValueError) as exc:
        raise LabError("planner proposal must be bounded JSON") from exc
    files = {item["path"]: item for item in capture["files"]}
    if not raw["done"]:
        if not all(isinstance(raw[k], list) and raw[k] for k in ("source_citations", "artifacts", "argv", "measurements")):
            raise LabError("executable proposal needs citations, artifacts, argv, and measurements")
        if not isinstance(raw["argv"][0], str) or not raw["argv"][0]:
            raise LabError("proposal argv must begin with an executable")
    for citation in raw["source_citations"]:
        if (not isinstance(citation, Mapping) or set(citation) != {"path", "sha256"}
                or not isinstance(citation.get("path"), str)
                or files.get(citation["path"], {}).get("sha256") != citation["sha256"]):
            raise LabError("proposal citation does not match captured source")
    citation_paths = [item["path"] for item in raw["source_citations"]]
    if len(citation_paths) != len(set(citation_paths)):
        raise LabError("proposal source citations must be unique")
    if any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > 2048 for item in raw["argv"]):
        raise LabError("proposal argv must contain bounded strings")
    if any(not isinstance(item, str) or not item or len(item.encode("utf-8")) > 2048 for item in raw["measurements"]):
        raise LabError("proposal measurements must contain bounded strings")
    artifact_paths = []
    artifact_bytes = 0
    for item in raw["artifacts"]:
        if not isinstance(item, Mapping) or set(item) != {"path", "content"} or not isinstance(item["content"], str):
            raise LabError("artifact must contain a relative path and text content")
        if not isinstance(item["path"], str) or len(item["path"].encode("utf-8")) > 1024:
            raise LabError("candidate artifact path is invalid")
        path = Path(item["path"])
        if path.is_absolute() or ".." in path.parts or not path.parts or "." in path.parts:
            raise LabError("candidate artifact path must be relative")
        artifact_paths.append(path.as_posix())
        artifact_bytes += len(item["content"].encode("utf-8"))
        if artifact_bytes > 64 * 1024 * 1024:
            raise LabError("candidate artifacts exceed 64 MiB")
    if len(artifact_paths) != len(set(artifact_paths)):
        raise LabError("candidate artifact paths must be unique")
    return dict(raw)


def _write_candidate(destination: Path, artifacts: Sequence[Mapping[str, str]]) -> None:
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    for item in artifacts:
        target = destination.joinpath(*Path(item["path"]).parts)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        target.write_text(item["content"], encoding="utf-8")
        target.chmod(0o400)


def _map_candidate_argv(argv: Sequence[str], candidate_dir: Path, source_dir: str) -> list[str]:
    mapped = []
    for value in argv:
        if value == "/proposal":
            mapped.append(str(candidate_dir))
        elif value.startswith("/proposal/"):
            relative = Path(value[len("/proposal/"):])
            if relative.is_absolute() or ".." in relative.parts:
                raise LabError("proposal command path escapes its candidate directory")
            mapped.append(str(candidate_dir.joinpath(*relative.parts)))
        elif value == "/workspace":
            mapped.append(source_dir)
        elif value.startswith("/workspace/"):
            relative = Path(value[len("/workspace/"):])
            if relative.is_absolute() or ".." in relative.parts:
                raise LabError("source command path escapes captured source")
            mapped.append(str(Path(source_dir).joinpath(*relative.parts)))
        else:
            mapped.append(value)
    return mapped


def _verify_capture(capture: Mapping[str, Any]) -> None:
    """Verify each captured input against the manifest immediately before/after effects."""
    root_value = capture.get("root")
    if not isinstance(root_value, str) or not root_value:
        raise CaptureIntegrityError("captured source directory is missing from its identity")
    root = Path(root_value)
    if root.is_symlink() or not root.is_dir():
        raise CaptureIntegrityError("captured source directory is missing or linked")
    expected_files = capture.get("files")
    if not isinstance(expected_files, list) or not expected_files:
        raise CaptureIntegrityError("captured source manifest is missing")
    identity = capture.get("identity")
    if not isinstance(identity, Mapping) or not isinstance(identity.get("files_digest"), str):
        raise CaptureIntegrityError("captured source manifest identity is missing")
    manifest_digest = hashlib.sha256(json.dumps(expected_files, sort_keys=True).encode("utf-8")).hexdigest()
    if manifest_digest != identity["files_digest"]:
        raise CaptureIntegrityError("captured source manifest no longer matches its recorded identity")
    identity = capture.get("identity")
    if not isinstance(identity, Mapping) or not isinstance(identity.get("files_digest"), str):
        raise CaptureIntegrityError("captured source manifest identity is missing")
    manifest_digest = hashlib.sha256(json.dumps(expected_files, sort_keys=True).encode("utf-8")).hexdigest()
    if manifest_digest != identity["files_digest"]:
        raise CaptureIntegrityError("captured source manifest no longer matches its recorded identity")
    identity = capture.get("identity")
    if not isinstance(identity, Mapping) or not isinstance(identity.get("files_digest"), str):
        raise CaptureIntegrityError("captured source manifest identity is missing")
    manifest_digest = hashlib.sha256(json.dumps(expected_files, sort_keys=True).encode("utf-8")).hexdigest()
    if manifest_digest != identity["files_digest"]:
        raise CaptureIntegrityError("captured source manifest no longer matches its recorded identity")
    observed: set[str] = set()
    for entry in expected_files:
        if not isinstance(entry, Mapping) or not isinstance(entry.get("path"), str):
            raise CaptureIntegrityError("captured source manifest entry is malformed")
        relative = Path(entry["path"])
        if relative.is_absolute() or not relative.parts or ".." in relative.parts or "." in relative.parts:
            raise CaptureIntegrityError(f"captured source manifest path is unsafe: {entry['path']}")
        path = root.joinpath(*relative.parts)
        current = root
        try:
            for part in relative.parts:
                current = current / part
                info = current.lstat()
                if current.is_symlink():
                    raise CaptureIntegrityError(f"captured source path became a symlink: {entry['path']}")
            if not path.is_file() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise CaptureIntegrityError(f"captured source is no longer a regular single-link file: {entry['path']}")
            data = path.read_bytes()
        except CaptureIntegrityError:
            raise
        except OSError as exc:
            raise CaptureIntegrityError(f"captured source cannot be read: {entry['path']}: {type(exc).__name__}") from exc
        digest = hashlib.sha256(data).hexdigest()
        if digest != entry.get("sha256") or len(data) != entry.get("size"):
            raise CaptureIntegrityError(
                f"captured source changed: {entry['path']} expected {entry.get('sha256')} observed {digest}")
        observed.add(relative.as_posix())
    actual: set[str] = set()
    try:
        for path in root.rglob("*"):
            if path.is_symlink():
                raise CaptureIntegrityError(f"captured source tree contains an unrecorded symlink: {path.relative_to(root)}")
            if path.is_file():
                actual.add(path.relative_to(root).as_posix())
    except OSError as exc:
        raise CaptureIntegrityError(f"captured source inventory cannot be checked: {type(exc).__name__}") from exc
    if actual != observed:
        raise CaptureIntegrityError("captured source tree inventory differs from its manifest")

