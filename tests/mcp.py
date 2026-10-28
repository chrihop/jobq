#!/usr/bin/env python3
"""Native MCP stdio checks, using an isolated daemon. No third-party Python modules.
Run: python3 tests/mcp.py [target/release/jobq]
Requires no other daemon for this account, like smoke.py.
"""
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from smoke import BIN, Fixture


class McpClient:
    """Small independent wire client, including simultaneous requests on one pipe."""
    def __init__(self, socket, env=None, protocol="2025-11-25"):
        self.stderr = tempfile.TemporaryFile()
        self.lock = threading.Lock()
        self.pending = {}
        self.sequence = 0
        self.reader_error = None
        self.process = subprocess.Popen(
            [str(BIN), "--socket", str(socket), "mcp"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
            env=env, cwd="/", umask=0o027,
        )
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        try:
            self.info = self.request("initialize", dict(protocolVersion=protocol, capabilities={},
                                clientInfo=dict(name="jobq-native-test", version="1")))
            self.notify("notifications/initialized")
        except BaseException:
            self.close(kill=True)
            raise

    def _read(self):
        try:
            for line in self.process.stdout:
                reply = json.loads(line)
                assert reply["jsonrpc"] == "2.0", reply
                if "id" in reply:
                    with self.lock:
                        future = self.pending.pop(reply["id"], None)
                    if future is not None:
                        future.set_result(reply)
        except BaseException as exc:
            self.reader_error = exc
        finally:
            with self.lock:
                pending = list(self.pending.values())
                self.pending.clear()
            for future in pending:
                future.set_exception(RuntimeError(f"MCP stdout closed: {self.reader_error}"))

    def begin(self, method, params=None):
        with self.lock:
            self.sequence += 1
            number = self.sequence
            future = concurrent.futures.Future()
            self.pending[number] = future
            message = dict(jsonrpc="2.0", id=number, method=method)
            if params is not None:
                message["params"] = params
            self.process.stdin.write(json.dumps(message).encode() + b"\n")
            self.process.stdin.flush()
        return number, future

    def notify(self, method, params=None):
        message = dict(jsonrpc="2.0", method=method)
        if params is not None:
            message["params"] = params
        with self.lock:
            self.process.stdin.write(json.dumps(message).encode() + b"\n")
            self.process.stdin.flush()

    def request(self, method, params=None, timeout=12):
        _, future = self.begin(method, params)
        reply = future.result(timeout)
        assert "error" not in reply, reply
        return reply["result"]

    @staticmethod
    def decode(result):
        value = result["structuredContent"]
        texts = [c["text"] for c in result["content"] if c["type"] == "text"]
        assert len(texts) == 1 and json.loads(texts[0]) == value, result
        return value

    def call(self, name, error=False, **arguments):
        result = self.request("tools/call", dict(name=name, arguments=arguments))
        assert bool(result.get("isError", False)) == error, result
        return self.decode(result)

    def submit(self, root, argv=None, **fields):
        args = dict(cwd=str(root), submission_token=str(uuid.uuid4()), timeout="500ms")
        if argv is not None:
            args["argv"] = argv
        elif "shell" not in fields:
            args["argv"] = ["/usr/bin/true"]
        args.update(fields)
        return self.call("jobq_submit", **args)

    def wait(self, job):
        value = self.call("jobq_wait", id=job["id"], wait_ms=8000)
        assert not value["wait_expired"], value
        return value

    def close(self, kill=False):
        if self.process.poll() is None:
            if kill:
                self.process.kill()
            else:
                self.process.stdin.close()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise AssertionError("MCP adapter did not stop on stdin EOF")
        self.reader.join(timeout=2)
        assert self.reader_error is None, self.reader_error
        self.stderr.close()
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        self.process.stdout.close()


TOOLS = {"jobq_submit", "jobq_status", "jobq_jobs", "jobq_queues", "jobq_logs", "jobq_cancel", "jobq_wait", "jobq_wait_many"}


def test(f):
    # Discovery remains available offline and the legacy MCP handshake negotiates.
    offline = McpClient(f.root / "absent")
    try:
        assert offline.info["protocolVersion"] == "2025-11-25", offline.info
        assert offline.info["serverInfo"]["name"] == "jobq"
        tools = offline.request("tools/list")["tools"]
        assert {t["name"] for t in tools} == TOOLS
        for tool in tools:
            assert tool["inputSchema"]["type"] == "object"
            assert tool["inputSchema"]["additionalProperties"] is False
        submit_schema = next(t["inputSchema"] for t in tools if t["name"] == "jobq_submit")
        assert set(submit_schema["required"]) == {"cwd", "submission_token"}
        assert not {"oneOf", "anyOf", "allOf"} & submit_schema.keys()
        assert set(submit_schema["properties"]) == {
            "cwd", "submission_token", "argv", "shell", "shell_executable",
            "timeout", "max_memory", "emergency", "env",
        }
        assert offline.call("jobq_queues", error=True)["error"]["code"] == "daemon_unavailable"
        failure = offline.call(
            "jobq_submit", error=True, cwd=str(f.root), argv=["true"], submission_token="offline-token")
        assert failure["error"]["submission_token"] == "offline-token"
        assert failure["error"]["outcome_unknown"] is False
    finally:
        offline.close()

    env = dict(os.environ, SHELL="/bin/sh", JOBQ_TEST_OVERRIDE="adapter", JOBQ_MCP_BASE="captured")
    a = McpClient(f.sock, env=env)
    b = McpClient(f.sock)
    try:
        assert a.call("jobq_queues")["config"]["runners"] == 1
        # Direct argv, cwd, inherited env, server overrides, per-call overrides, stdin and umask.
        code = "import json,os,sys; open('mcp-mode','w').close(); print(json.dumps([os.getcwd(),os.environ['JOBQ_MCP_BASE'],os.environ['JOBQ_TEST_OVERRIDE'],sys.argv[1],os.stat('mcp-mode').st_mode & 511,sys.stdin.read()]))"
        j = a.submit(f.root, [sys.executable, "-c", code, "arg with spaces;$HOME"], env={"JOBQ_TEST_OVERRIDE":"explicit"})
        assert a.wait(j)["state"] == "succeeded"
        values = json.loads(a.call("jobq_logs", id=j["id"])["text"])
        assert values == [str(f.root), "captured", "explicit", "arg with spaces;$HOME", 0o640, ""], values
        j = a.submit(f.root, shell="printf '%s' \"$JOBQ_TEST_OVERRIDE\"", shell_executable="/bin/sh")
        assert a.wait(j)["state"] == "succeeded"
        assert a.call("jobq_logs", id=j["id"])["text"] == "server"

        # Admission defaults and rounded timeout are supplied by the daemon.
        j = a.submit(f.root, timeout=None)
        assert j["requested_timeout_ms"] == 10000 and j["max_memory_bytes"] == 64 << 20
        a.wait(j)
        j = a.submit(f.root, ["/bin/sleep", ".1"], timeout="151ms", emergency=True)
        assert j["queue_limit_ms"] == 500 and j["emergency"] is True
        a.wait(j)

        # Validation and API errors retain useful machine-readable codes.
        invalid = [dict(cwd="relative"), dict(argv=[]), dict(argv=[""]), dict(shell="true"),
                   dict(argv=None, shell=""), dict(argv=None),
                   dict(shell_executable="/bin/sh"), dict(timeout="0s"), dict(max_memory="0GiB"),
                   dict(env={"BAD=KEY":"value"}), dict(submission_token="")]
        for override in invalid:
            args = dict(cwd=str(f.root), argv=["true"], submission_token="bad-token")
            args.update(override)
            response = a.call("jobq_submit", error=True, **args)
            assert response["error"]["code"] == "invalid_input", response
            assert "submission_token" in response["error"]
        for name, args in [("jobq_jobs", dict(limit=101)), ("jobq_jobs", dict(cursor="garbage")),
                           ("jobq_logs", dict(id=j["id"], max_bytes=16385)),
                           ("jobq_wait", dict(id=j["id"], block=True, wait_ms=1000)),
                           ("jobq_wait", dict(id=j["id"], wait_ms=30001))]:
            assert a.call(name, error=True, **args)["error"]["code"] in {"invalid_input", "request_failed"}
        _, unknown = a.begin("tools/call", dict(name="jobq_status", arguments=dict(id="x", extra=True)))
        reply = unknown.result(2)
        assert "error" in reply or reply["result"].get("isError"), reply
        assert a.call("jobq_status", error=True, id="missing")["error"]["code"] == "request_failed"
        j = a.submit(f.root, ["/bin/sh", "-c", "exit 7"])
        done = a.wait(j)
        assert done["state"] == "failed" and done["outcome"]["exit_code"] == 7
        assert a.call("jobq_status", id=j["id"])["state"] == "failed"

        # Bounded waits permit status and cancellation on the SAME MCP connection.
        j = a.submit(f.root, ["/bin/sleep", "8"], timeout="10s")
        f.wait(j, "running")
        start = time.monotonic()
        snap = a.call("jobq_wait", id=j["id"], wait_ms=120)
        assert snap["wait_expired"] and time.monotonic()-start < .8, snap
        wait_id, waiting = a.begin("tools/call", dict(name="jobq_wait", arguments=dict(id=j["id"], wait_ms=5000)))
        time.sleep(.05)
        start = time.monotonic()
        assert a.call("jobq_status", id=j["id"])["state"] == "running"
        assert time.monotonic()-start < 1 and not waiting.done()
        assert a.call("jobq_cancel", id=j["id"])["state"] in {"cancelling", "cancelled"}
        assert a.decode(waiting.result(3)["result"])["state"] == "cancelled"
        assert a.call("jobq_cancel", id=j["id"])["state"] == "cancelled"

        # MCP request cancellation ends a wait, and does not cancel a job.
        j = a.submit(f.root, ["/bin/sleep", "8"], timeout="10s")
        f.wait(j, "running")
        wait_id, waiting = a.begin("tools/call", dict(name="jobq_wait", arguments=dict(id=j["id"], wait_ms=5000)))
        time.sleep(.05)
        a.notify("notifications/cancelled", dict(requestId=wait_id, reason="stop waiting"))
        time.sleep(.05)
        assert a.call("jobq_status", id=j["id"])["state"] == "running"
        queued = a.submit(f.root)
        assert queued["state"] == "queued"
        assert b.call("jobq_cancel", id=queued["id"])["state"] == "cancelled"
        b.call("jobq_cancel", id=j["id"])
        a.wait(j)

        # Two adapters share the same durable token namespace under concurrent calls.
        token = str(uuid.uuid4())
        def duplicate(n):
            return (a if n % 2 else b).submit(f.root, submission_token=token)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            duplicates = list(pool.map(duplicate, range(16)))
        assert len({j["id"] for j in duplicates}) == 1
        a.wait(duplicates[0])

        # Fill more than one default page while a runner is occupied.
        blocker = a.submit(f.root, ["/bin/sleep", "8"], timeout="10s")
        f.wait(blocker, "running")
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            pending = list(pool.map(lambda _: a.submit(f.root), range(30)))
        default_page = a.call("jobq_jobs")
        assert len(default_page["jobs"]) == 25 and default_page["next_cursor"] is not None
        a.call("jobq_cancel", id=blocker["id"])
        a.wait(blocker)
        for job in pending:
            assert a.wait(job)["state"] == "succeeded"

        # Paginated summaries are bounded and preserve submission sequence order.
        cursor, rows, pages = None, [], 0
        while True:
            page = a.call("jobq_jobs", all=True, cursor=cursor, limit=2)
            assert len(page["jobs"]) <= 2
            rows += page["jobs"]
            pages += 1
            cursor = page["next_cursor"]
            if cursor is None:
                break
            assert pages < 100
        assert len({j["id"] for j in rows}) == len(rows)
        assert [j["sequence"] for j in rows] == sorted(j["sequence"] for j in rows)
        assert "stats" not in rows[0] and "outcome" not in rows[0]
        assert len(a.call("jobq_jobs")["jobs"]) <= 25

        # Byte offsets survive invalid UTF-8 and code points split across chunk boundaries.
        code = "import os; os.write(1,b'A\\xe2\\x82\\xacB\\xff'+b'X'*3000); os.write(2,b'error')"
        j = a.submit(f.root, [sys.executable, "-c", code])
        assert a.wait(j)["state"] == "succeeded"
        logs = a.call("jobq_logs", id=j["id"], max_bytes=3)
        assert logs["bytes_read"] == 3 and logs["next_offset"] == 3 and logs["decoding_lossy"]
        assert logs["terminal"] and logs["truncated"] and logs["has_more"]
        logs = a.call("jobq_logs", id=j["id"], offset=1, max_bytes=3)
        assert logs["text"] == "€" and not logs["decoding_lossy"] and logs["next_offset"] == 4
        logs = a.call("jobq_logs", id=j["id"], offset=1024)
        assert logs["bytes_read"] == 0 and not logs["has_more"] and logs["available_bytes"] == 1024
        assert a.call("jobq_logs", id=j["id"], stream="stderr")["text"] == "error"

        # Truncation is visible from live supervisor samples before the job exits.
        j = a.submit(f.root, [sys.executable, "-c", "import os,time; os.write(1,b'X'*3000); time.sleep(5)"], timeout="10s")
        deadline = time.monotonic() + 2
        while True:
            logs = a.call("jobq_logs", id=j["id"], max_bytes=1)
            if logs["truncated"]:
                break
            assert time.monotonic() < deadline, logs
            time.sleep(.02)
        assert not logs["terminal"]
        a.call("jobq_cancel", id=j["id"])
        a.wait(j)

        # MCP chunk limits apply independently of the daemon's storage cap.
        f.config.write_text(f.config.read_text().replace('log_cap = "1KiB"', 'log_cap = "64KiB"'))
        f.rpc("reload")
        j = a.submit(f.root, [sys.executable, "-c", "import os; os.write(1,b'Z'*100000)"])
        a.wait(j)
        assert a.call("jobq_logs", id=j["id"])["bytes_read"] == 4096
        assert a.call("jobq_logs", id=j["id"], max_bytes=16384)["bytes_read"] == 16384
        assert len(f.logs(j)) == 65536  # original socket default is preserved

        # Adapter death does not own job lifetime; a different adapter can observe it.
        j = b.submit(f.root, ["/bin/sleep", ".3"], timeout="2s")
        b.close(kill=True)
        b = None
        assert a.wait(j)["state"] == "succeeded"

        # One long-lived MCP connection survives daemon absence and restart.
        j = a.submit(f.root, ["/bin/sleep", "8"], timeout="10s")
        f.wait(j, "running")
        f.stop(crash=True)
        assert a.call("jobq_queues", error=True)["error"]["code"] == "daemon_unavailable"
        assert {t["name"] for t in a.request("tools/list")["tools"]} == TOOLS
        f.start()
        assert a.call("jobq_status", id=j["id"])["state"] == "interrupted"
        assert a.wait(a.submit(f.root))["state"] == "succeeded"
    finally:
        a.close()
        if b is not None:
            b.close()

    # A startup argument error never contaminates MCP stdout.
    p = subprocess.run([str(BIN), "--json", "mcp"], capture_output=True)
    assert p.returncode == 2 and not p.stdout and b"reserved" in p.stderr
    print("PASS: MCP discovery, schemas, context, limits, errors, waits, concurrency, cancellation, pagination, logs, disconnects, restart")


if __name__ == "__main__":
    fixture = Fixture()
    try:
        test(fixture)
    except BaseException:
        print((fixture.root / "server.log").read_text(), file=sys.stderr)
        raise
    finally:
        fixture.close()
