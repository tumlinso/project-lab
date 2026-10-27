from __future__ import annotations

import sqlite3

from project_evidence import EvidenceStore

from project_lab.compat import import_legacy


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
