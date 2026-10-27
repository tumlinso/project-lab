"""Shared typed contracts for the Python, CLI, and MCP interfaces."""
from __future__ import annotations

from dataclasses import dataclass, field
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True, slots=True)
class Project:
    name: str
    root: Path | str
    repository: str
    default_cpu: int = 1
    default_memory_mb: int = 1024

    def resolved_root(self) -> Path:
        root = Path(self.root).expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError("project root must be a directory")
        return root


class ResourceRequest(StrictModel):
    kind: Literal["cpu", "gpu"] = "cpu"
    cpus: int = Field(default=1, ge=1, le=64)
    memory_mb: int = Field(default=1024, ge=128, le=262144)
    gpu_count: int = Field(default=0, ge=0, le=16)
    wall_seconds: int = Field(default=300, ge=1, le=86400)


class RunRequest(StrictModel):
    project: str = Field(min_length=1, max_length=128)
    objective: str = Field(min_length=1, max_length=8192)
    source_paths: tuple[str, ...] = Field(min_length=1, max_length=64)
    resource: ResourceRequest = Field(default_factory=ResourceRequest)
    max_experiments: int = Field(default=1, ge=1, le=100)
    max_wall_seconds: int = Field(default=600, ge=1, le=86400)
    allowed_tools: tuple[str, ...] = Field(default=("python3",), min_length=1, max_length=32)
    measurements: tuple[str, ...] = Field(default=(), max_length=64)
    schedule_id: str | None = None


class PreviewResult(StrictModel):
    session_id: str
    project: str
    repository: str
    objective: str
    source_identity: dict[str, str]
    source_count: int
    resource: ResourceRequest
    max_experiments: int
    max_wall_seconds: int
    authorized: bool = False


class SessionResult(StrictModel):
    session_id: str
    state: str
    project: str
    created_at: float
    updated_at: float
    controller_job_id: str | None = None
    experiment_count: int = 0
    effects: tuple[str, ...] = ()
    reason: str | None = None


class ControlRequest(StrictModel):
    session_id: str
    action: Literal["pause", "cancel", "resume"]


class ScheduleRequest(StrictModel):
    session: RunRequest | None = None
    schedule_id: str | None = None
    action: Literal["pause", "cancel", "resume"] | None = None
    start_at: float | None = None
    recurrence_seconds: int | None = Field(default=None, ge=60, le=31536000)

    @model_validator(mode="after")
    def validate_action(self):
        if (self.session is None) == (self.schedule_id is None):
            raise ValueError("schedule request requires exactly one of session or schedule_id")
        if self.action is not None and self.schedule_id is None:
            raise ValueError("schedule action requires schedule_id")
        if self.session is not None:
            if (self.start_at is None) == (self.recurrence_seconds is None):
                raise ValueError("new schedule needs exactly one of start_at or recurrence_seconds")
            if self.start_at is not None and self.start_at <= time.time():
                raise ValueError("start_at must be in the future")
        return self

