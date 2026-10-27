from __future__ import annotations

import asyncio

from project_lab import Lab
from project_lab.mcp import create_server


def test_mcp_exposes_one_typed_tool_per_operation_family(tmp_path):
    async def get_tools():
        return await create_server(Lab(tmp_path)).list_tools()

    tools = asyncio.run(get_tools())
    names = {tool.name for tool in tools}
    assert names == {"preview", "run", "status", "results", "compare", "control", "schedule"}
    preview = next(tool for tool in tools if tool.name == "preview")
    assert "request" in preview.inputSchema["required"]
    request_ref = preview.inputSchema["properties"]["request"]["$ref"].rsplit("/", 1)[-1]
    request_schema = preview.inputSchema["$defs"][request_ref]
    assert request_schema["additionalProperties"] is False
