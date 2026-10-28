#!/usr/bin/env python3
"""Submit-schema regression against an existing daemon; never restarts it.

Usage: python3 tests/mcp_submit_live.py /absolute/jobq /absolute/new-evidence-dir
Only submits short, non-emergency printf commands with 64MiB reservations.
"""
import json
import os
from pathlib import Path
import sys
import uuid

from mcp import McpClient, TOOLS


def main():
    root = Path(sys.argv[2]).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    socket = Path(f"/tmp/jobq-{os.getuid()}/socket")
    evidence = []
    client = McpClient(socket)

    def call(name, **args):
        result = client.call(name, **args)
        evidence.append(dict(tool=name, arguments=args, result=result))
        (root / "calls.json").write_text(json.dumps(evidence, indent=2))
        return result

    try:
        tools = client.request("tools/list")["tools"]
        (root / "tools.json").write_text(json.dumps(tools, indent=2))
        assert {t["name"] for t in tools} == TOOLS
        schema = next(t["inputSchema"] for t in tools if t["name"] == "jobq_submit")
        assert schema["type"] == "object" and schema["additionalProperties"] is False
        assert set(schema["required"]) == {"cwd", "submission_token"}
        assert not {"oneOf", "anyOf", "allOf"} & schema.keys()
        assert set(schema["properties"]) == {
            "cwd", "submission_token", "argv", "shell", "shell_executable",
            "timeout", "max_memory", "emergency", "env",
        }
        for invalid in [
            {}, {"argv": []}, {"argv": [""]}, {"shell": ""},
            {"argv": ["/usr/bin/true"], "shell": "true"},
            {"argv": ["/usr/bin/true"], "cwd": "relative"},
            {"argv": ["/usr/bin/true"], "timeout": "0s"},
            {"argv": ["/usr/bin/true"], "max_memory": "0MiB"},
        ]:
            token = str(uuid.uuid4())
            args = dict(cwd=str(root), submission_token=token)
            args.update(invalid)
            result = call("jobq_submit", error=True, **args)
            assert result["error"]["code"] == "invalid_input", result
            assert result["error"]["submission_token"] == token
            assert not result["error"]["outcome_unknown"]
        for args in [
            {"argv": ["/usr/bin/true"], "submission_token": "missing-cwd"},
            {"argv": ["/usr/bin/true"], "cwd": str(root)},
            {"argv": ["/usr/bin/true"], "cwd": str(root), "submission_token": "unknown", "unknown": True},
        ]:
            _, future = client.begin("tools/call", dict(name="jobq_submit", arguments=args))
            reply = future.result(12)
            assert "error" in reply or reply["result"].get("isError"), reply
            evidence.append(dict(invalid_arguments=args, reply=reply))

        expected = "arg with spaces;$HOME"
        for command, output in [
            ({"argv": ["/usr/bin/printf", "%s", expected]}, expected),
            ({"shell": 'printf "%s|%s" "$PWD" "$JOBQ_SCHEMA_PROBE"',
              "shell_executable": "/bin/sh"}, f"{root}|explicit value"),
        ]:
            args = dict(cwd=str(root), submission_token=str(uuid.uuid4()),
                        timeout="3s", max_memory="64MiB", emergency=False,
                        env={"JOBQ_SCHEMA_PROBE": "explicit value"}, **command)
            job = call("jobq_submit", **args)
            assert job["requested_timeout_ms"] == 3000
            assert job["max_memory_bytes"] == 64 << 20 and not job["emergency"]
            duplicate = call("jobq_submit", **args)
            assert duplicate["id"] == job["id"]
            result = call("jobq_wait", id=job["id"], wait_ms=30000)
            assert result["state"] == "succeeded", result
            logs = call("jobq_logs", id=job["id"], stream="stdout", max_bytes=4096)
            assert logs["text"] == output, logs
        print(f"PASS: schema, invalid inputs, argv/shell, context, limits and dedup; evidence: {root}")
    finally:
        (root / "calls.json").write_text(json.dumps(evidence, indent=2))
        client.close()


if __name__ == "__main__":
    main()
