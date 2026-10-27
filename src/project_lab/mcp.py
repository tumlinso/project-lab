"""MCP adapter over the same Project-Lab application methods."""
from __future__ import annotations

from typing import Any, Literal

from mcp.server.fastmcp import FastMCP

from .models import ControlRequest, RunRequest, ScheduleRequest
from .service import Lab


def create_server(lab: Lab, *, name: str = "Project-Lab") -> FastMCP:
    server = FastMCP(name)

    @server.tool()
    def preview(request: RunRequest) -> dict[str, Any]:
        """Inspect source, resources, and stopping limits before authorizing a run."""
        return lab.preview(request)

    @server.tool()
    def run(session_id: str | None = None, request: RunRequest | None = None) -> dict[str, Any]:
        """Explicitly authorize and start a bounded experiment session."""
        return lab.run(request, session_id=session_id)

    @server.tool()
    def status(session_id: str | None = None) -> dict[str, Any]:
        """Read session state and durable identifiers."""
        return lab.status(session_id)

    @server.tool()
    def results(session_id: str, limit: int = 100) -> dict[str, Any]:
        """Read proposals, measurements, artifacts, scheduler receipts, and events."""
        return lab.results(session_id, limit=limit)

    @server.tool()
    def compare(session_ids: list[str], metrics: list[str] | None = None) -> dict[str, Any]:
        """Compare recorded measurements across sessions."""
        return lab.compare(session_ids, metrics or ())

    @server.tool()
    def control(session_id: str, action: Literal["pause", "cancel", "resume"]) -> dict[str, Any]:
        """Pause, cancel, or safely resume a session."""
        return lab.control(ControlRequest(session_id=session_id, action=action))

    @server.tool()
    def schedule(request: ScheduleRequest) -> dict[str, Any]:
        """Schedule a one-time or recurring experiment; source is captured at each start."""
        return lab.schedule(request)

    return server
