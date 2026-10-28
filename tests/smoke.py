#!/usr/bin/env python3
"""Real Unix subprocess/API tests. Uses temporary state; never touches production jobs.
Requires no existing jobq daemon for this account (the singleton is intentional).
Run: python3 tests/smoke.py [target/release/jobq]
"""
import base64
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid

BIN = Path(sys.argv[1] if len(sys.argv) > 1 else "target/release/jobq").resolve()
ACTIVE = {"queued", "starting", "running", "cancelling"}


class Fixture:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory(prefix="jq-", dir="/tmp")
        self.root = Path(self.temp.name).resolve()
        self.sock = self.root / "sock"
        self.config = self.root / "config.toml"
        self.state = self.root / "state"
        self.log = (self.root / "server.log").open("ab")
        self.process = None
        self.configure()
        try:
            self.start()
        except BaseException:
            self.close()
            raise

    def configure(self, runners=1, queues='["150ms", "500ms", "2s", "10s"]', cap="192MiB", aging="300ms"):
        self.config.write_text(f'''queues = {queues}
default_max_memory = "64MiB"
runners = {runners}
memory_cap = "{cap}"
aging_after = "{aging}"
monitor_interval = "20ms"
log_cap = "1KiB"
[env]
JOBQ_TEST_OVERRIDE = "server"
''')

    def start(self):
        self.process = subprocess.Popen([str(BIN), "--socket", str(self.sock), "server", "--config", str(self.config), "--state-dir", "state"], cwd=self.root, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 8
        last_error = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError((self.root / "server.log").read_text())
            try:
                self.rpc("queues")
                return
            except (OSError, AssertionError) as exc:
                last_error = exc
                time.sleep(.02)
        raise AssertionError(f"server did not become ready: {last_error}")

    def stop(self, crash=False):
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
            self.process.wait(timeout=8)

    def close(self):
        self.stop()
        self.log.close()
        self.temp.cleanup()

    def rpc(self, op, **kw):
        with socket.socket(socket.AF_UNIX) as conn:
            conn.settimeout(10)
            conn.connect(str(self.sock))
            conn.sendall(json.dumps(dict(version=1, op=op, **kw)).encode() + b"\n")
            line = conn.makefile("rb").readline()
        result = json.loads(line)
        assert result["ok"], result
        return result["data"]

    def submit(self, argv=None, timeout=500, memory=64 << 20, emergency=False, token=None, **kw):
        context = dict(command=dict(kind="exec", argv=argv or ["/usr/bin/true"]), cwd=str(self.root), env=dict(os.environ))
        return self.rpc("submit", token=token or str(uuid.uuid4()), context=context, timeout_ms=timeout, max_memory_bytes=memory, emergency=emergency, **kw)

    def status(self, j):
        return self.rpc("status", id=j["id"])

    def wait(self, j, state=None, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.status(j)
            if (state and value["state"] == state) or (not state and value["state"] not in ACTIVE):
                return value
            time.sleep(.01)
        raise AssertionError(f"wait timed out: {value}; {(self.root / 'server.log').read_text()}")

    def logs(self, j, stream="stdout"):
        return base64.b64decode(self.rpc("logs", id=j["id"], stream=stream)["data_base64"])


def test(f):
    # Real client, direct argv boundaries, exported env, cwd, and override precedence.
    env = dict(os.environ, JOBQ_TEST_OVERRIDE="client", JOBQ_CLIENT_ONLY="captured")
    code = "import os,sys,json; print(json.dumps([os.getcwd(),os.environ['JOBQ_TEST_OVERRIDE'],os.environ['JOBQ_CLIENT_ONLY'],sys.argv[1]]))"
    cmd = [str(BIN), "--socket", str(f.sock), "--json", "submit", "--env", "JOBQ_TEST_OVERRIDE=explicit", "--", sys.executable, "-c", code, "arg with spaces;$HOME"]
    cli = subprocess.run(cmd, cwd=f.root, env=env, check=True, capture_output=True)
    j = json.loads(cli.stdout)
    assert j["requested_timeout_ms"] == 10000
    result = f.wait(j)
    assert result["state"] == "succeeded", result

    # Multiple runners still obey the sum of declared memory reservations.
    f.configure(runners=3, cap="128MiB", aging="10s")
    f.rpc("reload")
    occupied = [f.submit(["/bin/sleep", "5"], timeout=10000) for _ in range(2)]
    for job in occupied:
        f.wait(job, "running")
    waiting = f.submit(["/bin/sleep", "5"], timeout=10000)
    assert f.status(waiting)["state"] == "queued"
    assert f.rpc("queues")["reserved_memory_bytes"] == 128 << 20
    f.rpc("cancel", id=occupied[0]["id"])
    f.wait(occupied[0])
    f.wait(waiting, "running")
    for job in [occupied[1], waiting]:
        f.rpc("cancel", id=job["id"])
        f.wait(job)
    f.configure()
    f.rpc("reload")
    assert json.loads(f.logs(j)) == [str(f.root), "explicit", "captured", "arg with spaces;$HOME"]
    original_mask = os.umask(0o027)
    try:
        mask_job = subprocess.run([str(BIN), "--socket", str(f.sock), "--json", "submit", "--", sys.executable, "-c", "open('job-created-file','w').close()"], cwd=f.root, capture_output=True, check=True)
    finally:
        os.umask(original_mask)
    assert f.wait(json.loads(mask_job.stdout))["state"] == "succeeded"
    assert (f.root / "job-created-file").stat().st_mode & 0o777 == 0o640
    env["SHELL"] = "/bin/sh"
    shell = subprocess.run([str(BIN), "--socket", str(f.sock), "--json", "submit", "--wait", "--shell", "printf foo | tr o a"], cwd=f.root, env=env, capture_output=True)
    assert shell.returncode == 0, shell.stderr
    assert f.logs(json.loads(shell.stdout)) == b"faa"

    # Routing rounds up; the exact request is not the enforced deadline.
    rounded = f.submit(["/bin/sleep", ".25"], timeout=151)
    assert rounded["queue_limit_ms"] == 500
    assert f.wait(rounded)["state"] == "succeeded"
    timed = f.submit(["/bin/sh", "-c", "sleep 5 & wait"], timeout=150)
    result = f.wait(timed)
    assert result["state"] == "timed_out", result
    assert 140 <= result["outcome"]["elapsed_ms"] < 1000
    failed = f.submit(["/bin/sh", "-c", "exit 7"])
    assert f.wait(failed)["outcome"]["exit_code"] == 7
    spawned = f.submit(["/definitely/missing/jobq-executable"])
    assert f.wait(spawned)["state"] == "spawn_failed"

    # Sustained aggregate memory, including children, exceeds one job's footprint cap.
    memcode = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c','import time; a=bytearray(80*1024*1024); time.sleep(5)']); time.sleep(5)"
    memory = f.submit([sys.executable, "-c", memcode], timeout=2000, memory=48 << 20)
    assert f.wait(memory)["state"] == "memory_exceeded"

    # Output flooding stays bounded and never postpones the timeout.
    flood = f.submit([sys.executable, "-c", "import os;\nwhile True: os.write(1,b'x'*8192); os.write(2,b'y'*8192)"], timeout=150)
    result = f.wait(flood)
    assert result["state"] == "timed_out", result
    assert result["outcome"]["stdout_truncated"] and result["outcome"]["stderr_truncated"]
    assert len(f.logs(flood)) == 1024 and len(f.logs(flood, "stderr")) == 1024

    # One global lock, even with different state and socket paths.
    second = subprocess.run([str(BIN), "--socket", str(f.root / "other.sock"), "server", "--state-dir", str(f.root / "other-state")], capture_output=True, timeout=3)
    assert second.returncode != 0 and b"already running" in second.stderr

    # Queued + running withdrawal, repetition, and immutable completed results.
    blocker = f.submit(["/bin/sleep", "5"], timeout=10000)
    f.wait(blocker, "running")
    pending = f.submit()
    assert f.rpc("cancel", id=pending["id"])["state"] == "cancelled"
    assert f.rpc("cancel", id=pending["id"])["state"] == "cancelled"
    f.rpc("cancel", id=blocker["id"])
    assert f.wait(blocker)["state"] == "cancelled"
    assert f.rpc("cancel", id=rounded["id"])["state"] == "succeeded"

    # Idempotency and simultaneous agent submissions.
    tok = str(uuid.uuid4())
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        duplicates = list(pool.map(lambda _: f.submit(token=tok), range(12)))
    assert len({j["id"] for j in duplicates}) == 1
    result = f.wait(duplicates[0])
    assert result["state"] == "succeeded", result

    # Emergency next-slot priority, FIFO, queue reassignment, and failed atomic reload.
    f.configure(aging="10s")
    f.rpc("reload")
    blocker = f.submit(["/bin/sleep", "5"], timeout=10000)
    f.wait(blocker, "running")
    a = f.submit(["/bin/echo", "a"], timeout=151)
    b = f.submit(["/bin/echo", "b"], timeout=501)
    urgent = f.submit(["/bin/echo", "urgent"], timeout=500, emergency=True)
    assert f.status(urgent)["state"] == "queued"
    f.configure(queues='["150ms", "2s", "10s"]', aging="10s")
    f.rpc("reload")
    assert f.status(a)["queue_limit_ms"] == 2000
    assert f.status(blocker)["queue_limit_ms"] == 10000
    old = f.config.read_text()
    f.config.write_text(old.replace('"10s"]', '"2s"]'))
    try:
        f.rpc("reload")
        raise AssertionError("invalid reload accepted")
    except AssertionError as e:
        assert "invalid reload accepted" not in str(e)
    assert f.rpc("queues")["config"]["queues_ms"] == [150, 2000, 10000]
    f.config.write_text(old)
    f.rpc("cancel", id=blocker["id"])
    urgent_done, a_done, b_done = f.wait(urgent), f.wait(a), f.wait(b)
    assert urgent_done["sequence"] > a_done["sequence"]
    assert urgent_done["started_at_ms"] <= a_done["started_at_ms"] <= b_done["started_at_ms"]

    # Crash kills running work, queued work survives, and executed side effects aren't retried.
    side_effect = f.root / "attempts"
    code = f"from pathlib import Path; import time; p=Path({str(side_effect)!r}); p.write_text(p.read_text()+'x' if p.exists() else 'x'); time.sleep(5)"
    running = f.submit([sys.executable, "-c", code], timeout=10000)
    f.wait(running, "running")
    deadline = time.monotonic() + 2
    while not side_effect.exists() and time.monotonic() < deadline:
        time.sleep(.01)
    queued = f.submit(["/bin/echo", "survived"], timeout=500)
    f.stop(crash=True)
    f.start()
    assert f.wait(running)["state"] == "interrupted"
    assert f.wait(queued)["state"] == "succeeded"
    assert side_effect.read_text() == "x"

    # An unexpectedly killed supervisor cannot leave its command consuming a reservation.
    running = f.submit(["/bin/sleep", "5"], timeout=10000)
    f.wait(running, "running")
    with sqlite3.connect(f.state / "jobs.sqlite") as conn:
        helper = conn.execute("SELECT helper_pid FROM jobs WHERE id=?", [running["id"]]).fetchone()[0]
    os.kill(helper, signal.SIGKILL)
    assert f.wait(running)["state"] == "monitoring_failed"
    assert f.rpc("queues")["reserved_memory_bytes"] == 0

    # Disconnect before receiving the submission response; retrying the token creates one job.
    token = str(uuid.uuid4())
    payload = dict(version=1, op="submit", token=token, context=dict(command=dict(kind="exec", argv=["/usr/bin/true"]), cwd=str(f.root), env=dict(os.environ)), timeout_ms=500, max_memory_bytes=64 << 20)
    with socket.socket(socket.AF_UNIX) as conn:
        conn.connect(str(f.sock))
        conn.sendall(json.dumps(payload).encode() + b"\n")
    retried = f.submit(token=token)
    assert f.wait(retried)["state"] == "succeeded"
    with sqlite3.connect(f.state / "jobs.sqlite") as conn:
        assert conn.execute("SELECT COUNT(*) FROM jobs WHERE token=?", [token]).fetchone()[0] == 1

    # A persistence/path failure becomes an explicit outcome, and later jobs remain runnable.
    blocker = f.submit(["/bin/sleep", "5"], timeout=10000)
    f.wait(blocker, "running")
    broken = f.submit()
    (f.state / "jobs" / broken["id"]).write_text("not a directory")
    f.rpc("cancel", id=blocker["id"])
    f.wait(blocker)
    assert f.wait(broken)["state"] == "spawn_failed"
    (f.state / "jobs" / broken["id"]).unlink()

    # Socket and all persisted job data are private.
    assert f.sock.stat().st_mode & 0o077 == 0
    for path in f.state.rglob("*"):
        assert path.stat().st_mode & 0o077 == 0, path

    # Historical estimates expose confidence and budget projections.
    queued = f.submit(["/bin/sleep", ".1"], timeout=500)
    status = f.status(queued)
    if status["estimate"]:
        assert status["estimate"]["historical"]["finish_at_ms"] <= status["estimate"]["budget"]["finish_at_ms"]
        assert "not guarantees" in status["estimate"]["assumptions"]
    f.wait(queued)

    # Aging overrides emergency priority once ordinary work has waited long enough.
    f.configure(aging="100ms")
    f.rpc("reload")
    blocker = f.submit(["/bin/sleep", "5"], timeout=10000)
    f.wait(blocker, "running")
    ordinary = f.submit(["/bin/echo", "aged"])
    time.sleep(.15)
    urgent = f.submit(["/bin/echo", "urgent"], emergency=True)
    f.rpc("cancel", id=blocker["id"])
    f.wait(blocker)
    ordinary_done, urgent_done = f.wait(ordinary), f.wait(urgent)
    assert ordinary_done["started_at_ms"] <= urgent_done["started_at_ms"]

    # Retention runs on startup; queued/running rows are not candidates for pruning.
    f.config.write_text(f.config.read_text().replace('[env]', 'retention = "50ms"\n[env]'))
    time.sleep(.1)
    f.stop()
    f.start()
    assert not f.rpc("jobs", all=True)["jobs"]
    print("PASS: native CLI/API, limits, cancellation, singleton, concurrency, reload, crash recovery, privacy, estimates")


if __name__ == "__main__":
    fixture = Fixture()
    try:
        test(fixture)
    except BaseException:
        print((fixture.root / "server.log").read_text(), file=sys.stderr)
        raise
    finally:
        fixture.close()
