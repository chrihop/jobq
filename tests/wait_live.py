#!/usr/bin/env python3
"""Blocking CLI/MCP checks against the existing daemon; never restart/reload it.

python3 tests/wait_live.py /absolute/jobq /absolute/new-evidence-directory
Creates only small owned jobs. The long-wait control takes at least 35 seconds.
"""
import json
from concurrent.futures import TimeoutError
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from mcp import McpClient
from smoke import BIN


def main():
    root = Path(sys.argv[2]).resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    client = McpClient(f"/tmp/jobq-{os.getuid()}/socket")
    jobs, records, processes = [], [], []

    def record(name, value):
        records.append(dict(check=name, result=value))
        (root / "results.json").write_text(json.dumps(records, indent=2))

    def submit(argv, timeout="10s"):
        job = client.submit(root, argv, timeout=timeout, max_memory="64MiB")
        jobs.append(job["id"])
        record("submitted", job)
        return job["id"]

    def begin(id):
        return client.begin("tools/call", dict(name="jobq_wait", arguments=dict(id=id, block=True)))

    def finish(future, state, timeout=15):
        reply = future.result(timeout)
        assert "error" not in reply and not reply["result"].get("isError"), reply
        value = client.decode(reply["result"])
        assert value["state"] == state and value["wait_expired"] is False, value
        record("blocking-final", value)
        return value

    def cli(*args):
        p = subprocess.Popen([str(BIN), "--json", *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append(p)
        return p

    try:
        tools = client.request("tools/list")["tools"]
        schema = next(t["inputSchema"] for t in tools if t["name"] == "jobq_wait")
        assert schema["properties"]["block"]["type"] == "boolean"
        assert schema["properties"]["block"]["default"] is False
        assert not {"oneOf", "anyOf", "allOf"} & schema.keys()
        record("wait-schema", schema)
        assert client.call("jobq_wait", error=True, id="missing", block=True, wait_ms=1)["error"]["code"] == "invalid_input"
        assert client.call("jobq_wait", error=True, id="missing", block=True)["error"]["code"] == "request_failed"

        # One request crosses the previous 30s ceiling; both interfaces block.
        id = submit(["/bin/sleep", "35"], timeout="60s")
        start = time.monotonic()
        _, waiting = begin(id)
        process = cli("wait", id)
        assert client.call("jobq_wait", id=id, wait_ms=50)["wait_expired"] is True
        assert client.call("jobq_status", id=id)["state"] in {"queued", "starting", "running"}
        time.sleep(31)
        assert not waiting.done() and process.poll() is None
        finish(waiting, "succeeded", timeout=60)
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0 and json.loads(stdout)["state"] == "succeeded", (stdout, stderr)
        record("cli-and-mcp-beyond-30s", dict(id=id, elapsed_s=time.monotonic()-start, cli=json.loads(stdout)))

        # Same adapter handles status/cancel while a blocking request is pending.
        id = submit(["/bin/sleep", "5"])
        _, waiting = begin(id)
        assert client.call("jobq_status", id=id)["state"] in {"queued", "starting", "running"}
        client.call("jobq_cancel", id=id)
        finish(waiting, "cancelled")

        # Cancelling only the MCP wait leaves the underlying job alive.
        id = submit(["/bin/sleep", "3"])
        number, waiting = begin(id)
        time.sleep(.1)
        client.notify("notifications/cancelled", dict(requestId=number, reason="test wait cancellation"))
        # MCP permits dropping a cancelled request without a response. rmcp
        # takes this path; do not confuse it with cancellation of the job.
        try:
            reply = waiting.result(.25)
            assert "error" in reply or reply["result"].get("isError"), reply
        except TimeoutError:
            reply = {"cancelled_request_response": "omitted"}
        record("wait-request-cancelled", reply)
        assert client.call("jobq_status", id=id)["state"] in {"queued", "starting", "running"}
        finish(begin(id)[1], "succeeded")

        # Disconnect a waiting adapter; another adapter can finish waiting.
        id = submit(["/bin/sleep", "2"])
        other = McpClient(f"/tmp/jobq-{os.getuid()}/socket")
        other.begin("tools/call", dict(name="jobq_wait", arguments=dict(id=id, block=True)))
        other.close(kill=True)
        finish(begin(id)[1], "succeeded")

        # Interrupt a CLI waiter, without sending any signal to the job.
        id = submit(["/bin/sleep", "2"])
        process = cli("wait", id)
        time.sleep(.1)
        process.send_signal(signal.SIGINT)
        process.communicate(timeout=3)
        finish(begin(id)[1], "succeeded")

        for argv, timeout, state, exit_code in [
            (["/bin/sh", "-c", "exit 7"], "10s", "failed", 7),
            (["/bin/sleep", "5"], "1s", "timed_out", 124),
        ]:
            id = submit(argv, timeout)
            finish(begin(id)[1], state)
            process = cli("wait", id)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == exit_code and json.loads(stdout)["state"] == state, (stdout, stderr)
            record("cli-exit", dict(id=id, exit_code=process.returncode))

        process = cli("submit", "--wait", "--token", "wait-live-"+root.name,
                      "--timeout", "1s", "--max-memory", "64MiB", "--", "/usr/bin/true")
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0 and json.loads(stdout)["state"] == "succeeded", (stdout, stderr)
        record("cli-submit-wait", json.loads(stdout))
        print("PASS: >30s CLI/MCP blocking, bounded compatibility, concurrent calls, cancellation, disconnect, failure and timeout")
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
                p.communicate(timeout=5)
        # Only this test's jobs can be cleaned up, never other users' work.
        for id in jobs:
            status = client.call("jobq_status", id=id)
            if status["state"] in {"queued", "starting", "running", "cancelling"}:
                client.call("jobq_cancel", id=id)
                finish(begin(id)[1], "cancelled")
        client.close()


if __name__ == "__main__":
    main()
