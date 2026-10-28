from __future__ import annotations

import sqlite3
import json

from project_evidence import EvidenceStore

from project_lab.compat import import_legacy
from project_lab.cli import main
from project_lab.service import Lab, LegacySessionReadOnlyError


def test_import_copied_interrupted_lab_database_read_only_and_idempotent(tmp_path):
    source = tmp_path / "historical.sqlite3"
    db = sqlite3.connect(source)
    db.executescript("""
      CREATE TABLE lab_sessions(session_id TEXT PRIMARY KEY, state TEXT, scope_json TEXT);
      CREATE TABLE lab_session_events(event_id TEXT PRIMARY KEY, session_id TEXT, kind TEXT);
      INSERT INTO lab_sessions VALUES('session-old-17','running','{"goal":"old"}');
      INSERT INTO lab_session_events VALUES('event-4','session-old-17','authorized');
    """)
    db.commit(); db.close()
    before = source.read_bytes()
    evidence = EvidenceStore(tmp_path / "new-evidence.sqlite3", tmp_path / "blobs")

    assert import_legacy(source, evidence) == {"lab_sessions": 1, "lab_session_events": 1,
                                                "packets": 0, "notes": 0}
    record = evidence.get_observation("legacy:lab_sessions:session-old-17")
    assert record["payload"]["original_state"] == "running"
    assert record["payload"]["inspect_only"] is True
    assert record["payload"]["original_record"]["scope_json"] == '{"goal":"old"}'
    assert import_legacy(source, evidence)["lab_sessions"] == 0
    assert source.read_bytes() == before


def test_legacy_sessions_are_visible_but_never_resumable(tmp_path, capsys):
    import shutil

    original = tmp_path / "historical.sqlite3"
    db = sqlite3.connect(original)
    db.executescript("""
      CREATE TABLE lab_sessions(session_id TEXT PRIMARY KEY, project TEXT, repository TEXT,
        scope_json TEXT, state TEXT, created_at REAL, updated_at REAL);
      CREATE TABLE lab_session_events(event_id TEXT PRIMARY KEY, session_id TEXT, kind TEXT,
        payload TEXT, created_at REAL);
      CREATE TABLE lab_session_proposals(proposal_id TEXT PRIMARY KEY, session_id TEXT,
        ordinal INTEGER, proposal_json TEXT, created_at REAL);
      CREATE TABLE lab_session_effects(effect_id TEXT PRIMARY KEY, session_id TEXT,
        proposal_id TEXT, state TEXT, receipt_json TEXT, finished_at REAL);
      CREATE TABLE lab_experiments(experiment_id TEXT PRIMARY KEY, project TEXT, state TEXT);
      CREATE TABLE lab_candidates(candidate_id TEXT PRIMARY KEY, experiment_id TEXT, patch_sha256 TEXT);
      INSERT INTO lab_sessions VALUES('session-interrupted','demo','demo-repo','{}','running',1,2);
      INSERT INTO lab_sessions VALUES('session-other','demo','demo-repo','{}','cancelled',1,2);
      INSERT INTO lab_session_events VALUES('event-a','session-interrupted','run_interrupted',
        '{"experiment_id":"experiment-a","candidate_id":"candidate-a"}',3);
      INSERT INTO lab_session_events VALUES('event-b','session-interrupted','cleanup_unknown','{}',4);
      INSERT INTO lab_session_events VALUES('event-other','session-other','cancelled','{}',4);
      INSERT INTO lab_session_proposals VALUES('proposal-a','session-interrupted',0,'{"step":1}',3);
      INSERT INTO lab_session_effects VALUES('effect-a','session-interrupted','proposal-a','unknown','{}',NULL);
      INSERT INTO lab_experiments VALUES('experiment-a','demo','interrupted');
      INSERT INTO lab_candidates VALUES('candidate-a','experiment-a','abc');
      INSERT INTO lab_candidates VALUES('candidate-unrelated','experiment-orphan','def');
    """)
    db.commit()
    db.close()
    copied = tmp_path / "copied-history.sqlite3"
    shutil.copy2(original, copied)
    before = copied.read_bytes()
    evidence = EvidenceStore(tmp_path / "state/evidence.sqlite3", tmp_path / "state/blobs")
    import_legacy(copied, evidence)
    lab = Lab(tmp_path / "state")

    catalog = lab.status()
    assert catalog["sessions"] == []
    assert [item["session_id"] for item in catalog["legacy_sessions"]] == [
        "session-interrupted", "session-other"]
    assert all(item["inspect_only"] is True and item["record_type"] == "legacy"
               for item in catalog["legacy_sessions"])

    status = lab.status("session-interrupted")
    assert status["sessions"] == []
    assert status["legacy_sessions"][0]["state"] == "running"
    result = lab.results("session-interrupted", limit=1)
    assert result["inspect_only"] is True
    assert result["session"]["id"] == "session-interrupted"
    assert result["session"]["original_record"]["scope_json"] == "{}"
    assert [item["id"] for item in result["events"]] == ["event-a"]
    assert [item["id"] for item in result["proposals"]] == ["proposal-a"]
    assert [item["id"] for item in result["effects"]] == ["effect-a"]
    assert [item["id"] for item in result["experiments"]] == ["experiment-a"]
    assert [item["id"] for item in result["candidates"]] == ["candidate-a"]
    assert result["coverage"]["complete"] is False
    assert result["coverage"]["omitted"]["lab_session_events"] == 1
    assert all(item["inspect_only"] is True and item["provenance"]["source_database"]
               for item in result["events"] + result["proposals"] + result["effects"] + result["candidates"])

    for operation in (lambda: lab.run(session_id="session-interrupted"),
                      lambda: lab.drive("session-interrupted"),
                      *(lambda action=action: lab.control({"session_id": "session-interrupted", "action": action})
                        for action in ("resume", "pause", "cancel"))):
        try:
            operation()
        except LegacySessionReadOnlyError as exc:
            assert exc.code == "legacy_session_inspect_only"
            assert exc.inspect_only is True and exc.session_id == "session-interrupted"
        else:
            raise AssertionError("historical sessions must not run or resume")
    assert copied.read_bytes() == before

    config = tmp_path / "config.toml"
    config.write_text("[projects.demo]\nroot = \"/tmp\"\nrepository = \"demo\"\n", encoding="utf-8")
    rc = main(["--config", str(config), "--state-dir", str(tmp_path / "state"),
               "run", "--session", "session-interrupted"])
    assert rc == 2
    error = json.loads(capsys.readouterr().err)
    assert error == {"error": "LegacySessionReadOnlyError",
                     "message": "legacy session 'session-interrupted' is inspect-only; it cannot be run, resumed, or controlled",
                     "code": "legacy_session_inspect_only", "inspect_only": True,
                     "session_id": "session-interrupted"}


def test_legacy_results_reports_missing_ids_normally(tmp_path):
    lab = Lab(tmp_path / "state")
    try:
        lab.results("missing-session")
    except ValueError as exc:
        assert str(exc) == "session not found"
    else:
        raise AssertionError("unknown IDs should not be described as legacy")
