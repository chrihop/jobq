#!/usr/bin/env python3
"""Native detached-session cleanup/recovery regressions. Exclusive daemon use.
Run: python3 tests/process_tree.py /absolute/jobq
"""
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import time

from smoke import Fixture


def table():
    rows = subprocess.check_output(["/bin/ps", "-axo", "pid=,stat="], text=True)
    return {int(pid): state for pid, state in (line.split() for line in rows.splitlines())}


def gone(pids):
    deadline = time.monotonic()+3
    while True:
        current = table()
        live = [pid for pid in pids if pid in current and not current[pid].startswith("Z")]
        if not live:
            return
        assert time.monotonic() < deadline, {"survivors": live}
        time.sleep(.02)


def launch(f, name, *, timeout=10000, memory=160 << 20, allocate=0, root_exit=False, reparent=False):
    marker = f.root / (name+".json")
    leaf = (
        "import os,json,time; from pathlib import Path; "
        f"Path({str(marker)!r}).write_text(json.dumps([os.getppid(),os.getpid()])); a=bytearray({allocate}); "
        "time.sleep(30)"
    )
    middle = (
        "import subprocess,sys,time; "
        f"p=subprocess.Popen([sys.executable,'-c',{leaf!r}],start_new_session=True); "
        + ("time.sleep(.25)" if reparent else "p.wait()")
    )
    root = (
        "import subprocess,sys,time; from pathlib import Path; "
        f"p=subprocess.Popen([sys.executable,'-c',{middle!r}],start_new_session=True); "
        + (f"\nwhile not Path({str(marker)!r}).exists(): time.sleep(.01)\ntime.sleep(.3)" if root_exit else "time.sleep(30)")
    )
    job = f.submit([sys.executable, "-c", root], timeout=timeout, memory=memory)
    deadline = time.monotonic()+3
    while not marker.exists():
        assert time.monotonic() < deadline, f.status(job)
        time.sleep(.01)
    pids = json.loads(marker.read_text())
    start = json.loads((f.state / "jobs" / job["id"] / "started.json").read_text())
    pids.append(start["pid"])
    return job, pids


def sampled(f, job, count=3):
    deadline = time.monotonic()+3
    while True:
        value = f.status(job)
        if value["stats"]["sampled_process_count"] >= count:
            return value
        assert value["state"] in {"starting", "running"}, value
        assert time.monotonic() < deadline, value
        time.sleep(.02)


def test(f):
    # An unrelated same-account process must never be included or signalled.
    unrelated = subprocess.Popen(["/bin/sleep", "30"])
    try:
        job, pids = launch(f, "cancel")
        sampled(f, job)
        f.rpc("cancel", id=job["id"])
        assert f.wait(job)["state"] == "cancelled"
        gone(pids)
        assert f.rpc("queues")["reserved_memory_bytes"] == 0
        assert unrelated.poll() is None
        print("PASS: detached cancellation and unrelated-process isolation", flush=True)

        job, pids = launch(f, "timeout", timeout=2000)
        assert f.wait(job)["state"] == "timed_out"
        gone(pids)
        print("PASS: detached wall timeout", flush=True)

        job, pids = launch(f, "memory", memory=64 << 20, allocate=96 << 20)
        result = f.wait(job)
        assert result["state"] == "memory_exceeded", result
        assert result["stats"]["peak_sampled_footprint_bytes"] > 64 << 20
        gone(pids)
        print("PASS: detached aggregate memory", flush=True)

        # Normal root exit cannot leave its previously observed detached tree.
        job, pids = launch(f, "normal-exit", root_exit=True)
        assert f.wait(job)["state"] == "succeeded"
        gone(pids)
        print("PASS: normal root exit cleans detached descendants", flush=True)

        zombie_code = "import subprocess,time; p=subprocess.Popen(['/bin/sleep','.15'],start_new_session=True); time.sleep(.8); p.wait(); time.sleep(30)"
        zombie = f.submit([sys.executable, "-c", zombie_code], timeout=10000, memory=64 << 20)
        time.sleep(1)
        assert f.status(zombie)["state"] == "running", f.status(zombie)
        f.rpc("cancel", id=zombie["id"])
        assert f.wait(zombie)["state"] == "cancelled"
        print("PASS: tracked detached zombie does not cause monitoring failure", flush=True)

        protected_code = "import subprocess,os,time; [subprocess.run(['/bin/ps','-p',str(os.getpid()),'-o','pid='],stdout=subprocess.DEVNULL,check=True) for _ in range(12)]"
        protected = f.submit([sys.executable, "-c", protected_code], timeout=2000, memory=64 << 20)
        assert f.wait(protected)["state"] == "succeeded", f.status(protected)
        protected = f.submit(['/bin/ps','-p',str(os.getpid()),'-o','pid='], timeout=500)
        assert f.wait(protected)["state"] == "succeeded", f.status(protected)
        print("PASS: short-lived setuid ps helpers and protected root exit", flush=True)
        protected_zombie_code = "import subprocess,os,time; p=subprocess.Popen(['/bin/ps','-p',str(os.getpid()),'-o','pid='],stdout=subprocess.DEVNULL,start_new_session=True); time.sleep(.8); p.wait(); time.sleep(.2)"
        protected = f.submit([sys.executable, "-c", protected_zombie_code], timeout=2000, memory=64 << 20)
        assert f.wait(protected)["state"] == "succeeded", f.status(protected)
        print("PASS: detached setuid zombie is confirmed through BSD status", flush=True)
        census_code = "import subprocess; [subprocess.run(['/bin/ps','-e','-o','pid='],stdout=subprocess.DEVNULL,check=True) for _ in range(5)]"
        census = f.submit([sys.executable, "-c", census_code], timeout=2000, memory=64 << 20)
        assert f.wait(census)["state"] == "succeeded", f.status(census)
        print("PASS: protected full-process census helpers", flush=True)

        # A recorded grandchild stays owned after its middle process exits.
        job, pids = launch(f, "reparent", reparent=True)
        sampled(f, job, count=2)
        inventory = json.loads((f.state / "jobs" / job["id"] / "processes.json").read_text())
        assert {p["pid"] for p in inventory["members"]} >= set(pids)
        time.sleep(.35)
        f.rpc("cancel", id=job["id"])
        assert f.wait(job)["state"] == "cancelled"
        gone(pids)
        print("PASS: reparented descendant cleanup", flush=True)

        # Stale/reused identity tokens do not authorize signalling a foreign PID.
        job, pids = launch(f, "supervisor-crash")
        sampled(f, job)
        directory = f.state / "jobs" / job["id"]
        inventory = json.loads((directory / "processes.json").read_text())
        assert {p["pid"] for p in inventory["members"]} >= set(pids)
        inventory["members"].append({"pid": unrelated.pid, "token": 1})
        (directory / "processes.json").write_text(json.dumps(inventory))
        with sqlite3.connect(f.state / "jobs.sqlite") as db:
            helper = db.execute("SELECT helper_pid FROM jobs WHERE id=?", [job["id"]]).fetchone()[0]
        os.kill(helper, signal.SIGKILL)
        assert f.wait(job)["state"] == "monitoring_failed"
        gone(pids)
        assert f.rpc("queues")["reserved_memory_bytes"] == 0
        assert unrelated.poll() is None
        print("PASS: persisted supervisor recovery and stale PID token isolation", flush=True)

        job, pids = launch(f, "daemon-crash")
        sampled(f, job)
        f.stop(crash=True)
        f.start()
        assert f.wait(job)["state"] == "interrupted"
        gone(pids)
        assert f.rpc("queues")["reserved_memory_bytes"] == 0
        assert unrelated.poll() is None
        print("PASS: daemon crash recovery", flush=True)

        # Missing new-format recovery evidence fails closed for that job only:
        # its reservation stays quarantined while the daemon keeps serving, and
        # is released only once a later cleanup confirms the tree is gone.
        job, pids = launch(f, "missing-inventory")
        sampled(f, job)
        directory = f.state / "jobs" / job["id"]
        with sqlite3.connect(f.state / "jobs.sqlite") as db:
            helper = db.execute("SELECT helper_pid FROM jobs WHERE id=?", [job["id"]]).fetchone()[0]
        os.kill(f.process.pid, signal.SIGSTOP)
        try:
            os.kill(helper, signal.SIGSTOP)
            original = (directory / "processes.json").read_bytes()
            (directory / "processes.json").unlink()
            os.kill(helper, signal.SIGKILL)
        finally:
            os.kill(f.process.pid, signal.SIGCONT)
        value = f.wait(job)
        assert value["state"] == "monitoring_failed", value
        assert value.get("reservation_quarantined") is True, value
        assert "cleanup unresolved" in value["outcome"]["message"], value
        assert "load process inventory" in value["outcome"]["message"], value
        assert f.process.poll() is None, "daemon must keep running"
        queues = f.rpc("queues")
        assert queues["reserved_memory_bytes"] > 0 and job["id"] in queues["quarantined_jobs"], queues
        assert all(pid in table() for pid in pids), "unconfirmed tree must not be reported as cleaned"
        # Restore only this disposable fault-injection fixture's original bytes.
        (directory / "processes.json").write_bytes(original)
        # Startup makes a non-waiting quarantine pass. If SIGKILL delivery is
        # still settling, release is confirmed by the next once-a-minute pass.
        f.stop()
        f.start()
        gone(pids)
        deadline = time.monotonic()+75
        while f.status(job).get("reservation_quarantined"):
            assert time.monotonic() < deadline, f.status(job)
            time.sleep(.02)
        assert f.rpc("queues")["reserved_memory_bytes"] == 0
        assert f.status(job)["state"] == "monitoring_failed"
        assert unrelated.poll() is None
        print("PASS: missing inventory quarantines the job, not the daemon", flush=True)
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=3)
    print("PASS: detached cancel, timeout, memory, root exit, reparenting, supervisor/daemon crash, PID identity isolation, fail-closed recovery")


if __name__ == "__main__":
    fixture = Fixture()
    try:
        test(fixture)
    except BaseException:
        print((fixture.root / "server.log").read_text(), file=sys.stderr)
        raise
    finally:
        fixture.close()
