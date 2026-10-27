"""Small durable journal; scheduler and evidence authorities remain external."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import time


class Journal:
    def __init__(self, state_dir: str | Path):
        self.root = Path(state_dir).expanduser()
        self.db_path = self.root / "project-lab.sqlite3"

    def connect(self) -> sqlite3.Connection:
        if self.root.is_symlink():
            raise PermissionError("Project-Lab state directory must not be a symlink")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        if self.db_path.is_symlink():
            raise PermissionError("Project-Lab journal must not be a symlink")
        db = sqlite3.connect(self.db_path, timeout=10)
        os.chmod(self.db_path, 0o600)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.executescript("""
          CREATE TABLE IF NOT EXISTS sessions(
            id TEXT PRIMARY KEY, project TEXT NOT NULL, request TEXT NOT NULL,
            state TEXT NOT NULL, source_identity TEXT NOT NULL, capture TEXT NOT NULL,
            created REAL NOT NULL, updated REAL NOT NULL, deadline REAL, controller_job_id TEXT,
            controller_key TEXT, count INTEGER NOT NULL DEFAULT 0,
            reason TEXT);
          CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, kind TEXT NOT NULL,
            at REAL NOT NULL, payload TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS proposals(
            id TEXT PRIMARY KEY, session_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
            proposal TEXT NOT NULL, created REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS effects(
            id TEXT PRIMARY KEY, session_id TEXT NOT NULL, proposal_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
            scheduler_job_id TEXT, receipt TEXT, created REAL NOT NULL, updated REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS schedules(
            id TEXT PRIMARY KEY, request TEXT NOT NULL, scheduler_job_id TEXT,
            state TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'recurring', created REAL NOT NULL, updated REAL NOT NULL);
        """)
        return db

    @staticmethod
    def emit(db: sqlite3.Connection, session_id: str, kind: str, payload: object) -> None:
        db.execute("INSERT INTO events(session_id,kind,at,payload) VALUES(?,?,?,?)",
                   (session_id, kind, time.time(), json.dumps(payload, sort_keys=True)))

