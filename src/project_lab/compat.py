"""Read-only import of copied pre-extraction LAB and Project Control records."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3

from project_evidence import EvidenceStore, LegacyReader, Observation


_TABLE_KEYS = {
    "lab_experiments": "experiment_id", "lab_attempts": "attempt_id",
    "lab_candidates": "candidate_id", "lab_sessions": "session_id",
    "lab_session_events": "event_id", "lab_session_proposals": "proposal_id",
    "lab_session_effects": "effect_id",
}


def import_legacy(source_db: str | Path, evidence: EvidenceStore, *, limit: int = 10000) -> dict[str, int]:
    """Import copied historical records as attributed observations, never effects.

    The source is opened read-only. Original IDs and complete row payloads are
    retained; interrupted states stay visible and are never resumed here.
    """
    path = Path(source_db).expanduser().resolve(strict=True)
    imported: dict[str, int] = {}
    with _readonly(path) as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table, id_column in _TABLE_KEYS.items():
            if table not in tables:
                continue
            columns = {row[1] for row in db.execute(f'PRAGMA table_info("{table}")')}
            if id_column not in columns:
                continue
            rows = db.execute(f'SELECT * FROM "{table}" LIMIT ?', (limit,)).fetchall()
            count = 0
            for row in rows:
                payload = {key: _json_value(row[key]) for key in row.keys()}
                legacy_id = str(row[id_column])
                observation_id = f"legacy:{table}:{legacy_id}"
                if evidence.get_observation(observation_id) is not None:
                    continue
                state = str(payload.get("state", ""))
                kind = f"legacy_{table}"
                evidence.record_observation(Observation(
                    observation_id=observation_id, owner="project-lab-legacy",
                    kind=kind, subject=legacy_id,
                    payload={"source_database": path.name, "original_id": legacy_id,
                             "original_state": state, "inspect_only": True,
                             "original_record": payload}))
                count += 1
            imported[table] = count

    # Packet/note databases use the shared read-only decoder. Notes remain
    # attributed advisory content and are not upgraded into assessments.
    try:
        reader = LegacyReader(path)
        packets = reader.packets(limit=limit)
        notes = reader.notes(limit=limit)
    except sqlite3.DatabaseError:
        packets, notes = [], []
    for item in packets:
        observation_id = f"legacy:packet:{item.packet_id}"
        if evidence.get_observation(observation_id) is None:
            evidence.record_observation(Observation(observation_id=observation_id,
                owner="project-control-legacy", kind="legacy_packet", subject=item.alias,
                payload={"provenance": item.provenance, "packet_id": item.packet_id,
                         "metadata": dict(item.metadata), "payload": item.payload,
                         "expired": item.expired, "inspect_only": True}))
    for item in notes:
        observation_id = f"legacy:note:{item.note_id}"
        if evidence.get_observation(observation_id) is None:
            evidence.record_observation(Observation(observation_id=observation_id,
                owner="project-control-legacy", kind="legacy_note", subject=item.project,
                payload={"provenance": item.provenance, "note_id": item.note_id,
                         "metadata": dict(item.metadata), "advisory": item.advisory,
                         "independent_evidence": item.independent_evidence,
                         "inspect_only": True}))
    imported["packets"] = len(packets)
    imported["notes"] = len(notes)
    return imported


class _readonly:
    def __init__(self, path):
        self.path = path
        self.db = None

    def __enter__(self):
        self.db = sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA query_only=ON")
        return self.db

    def __exit__(self, *_):
        self.db.close()


def _json_value(value):
    if isinstance(value, bytes):
        return {"encoding": "base64", "data": __import__("base64").b64encode(value).decode("ascii")}
    return value
