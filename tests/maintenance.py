#!/usr/bin/env python3
"""Native maintenance regression. Requires exclusive daemon use, like smoke.py."""
import concurrent.futures
import json
import socket
import subprocess
import time
import uuid
from smoke import BIN, Fixture


def raw(f, op, **args):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(10)
        s.connect(str(f.sock))
        s.sendall(json.dumps(dict(version=1, op=op, **args)).encode()+b"\n")
        return json.loads(s.makefile("rb").readline())


def test(f):
    active = f.submit(["/bin/sleep", ".5"], timeout=2000)
    f.wait(active, "running")
    output = f.root / "fifo"
    queued = [f.submit(["/bin/sh", "-c", 'printf "%s" "$1" >> "$2"', "probe", letter, str(output)], token="fifo-"+letter) for letter in "ABC"]
    state = f.rpc("drain")
    assert state["mode"] == "paused" and not state["ready_to_restart"], state
    assert all(not q["accepting"] for q in f.rpc("queues")["ordinary"])
    assert f.wait(active)["state"] == "succeeded"
    assert f.rpc("maintenance")["ready_to_restart"]
    assert all(f.status(j)["state"] == "queued" for j in queued)
    assert f.status(queued[0])["estimate"] is None
    # Duplicate token lookup precedes admission rejection.
    assert f.submit(token="fifo-A")["id"] == queued[0]["id"]
    rejected = raw(f, "submit", token=str(uuid.uuid4()), context={
        "command": {"kind": "exec", "argv": ["/usr/bin/true"]},
        "cwd": str(f.root), "env": {}}, timeout_ms=500, max_memory_bytes=1024)
    assert rejected["error"]["code"] == "maintenance", rejected
    f.stop()
    f.start()
    assert f.rpc("maintenance")["mode"] == "paused"
    assert all(f.status(j)["state"] == "queued" for j in queued)
    assert f.rpc("reload")["reloaded"]
    assert f.rpc("maintenance")["mode"] == "paused"
    assert f.rpc("resume")["admission_open"]
    for j in queued:
        assert f.wait(j)["state"] == "succeeded"
    assert output.read_text() == "ABC"

    active = f.submit(["/bin/sleep", ".3"], timeout=2000)
    f.wait(active, "running")
    pending = f.submit(["/bin/sleep", ".1"], token="finish-pending", timeout=2000)
    # Blocking CLI drain --all completes only when queued work and cleanup finish.
    p = subprocess.Popen([str(BIN), "--socket", str(f.sock), "--json", "drain", "--all", "--wait"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(timeout=10)
    assert p.returncode == 0, stderr
    result = json.loads(stdout)
    assert result["mode"] == "finish_accepted" and result["ready_to_restart"], result
    assert result["active_jobs"] == result["queued_jobs"] == result["active_runners"] == 0
    assert f.status(pending)["state"] == "succeeded"
    f.stop()
    f.start()
    assert f.rpc("maintenance")["mode"] == "finish_accepted"
    assert f.submit(token="finish-pending")["id"] == pending["id"]
    f.rpc("resume")

    # Concurrent submissions racing drain must be either durable or rejected.
    blocker = f.submit(["/bin/sleep", ".5"], timeout=2000)
    f.wait(blocker, "running")
    def submit(n):
        if n == 4:
            return raw(f, "drain")
        return raw(f, "submit", token="race-"+str(n), context={
            "command":{"kind":"exec","argv":["/usr/bin/true"]},
            "cwd":str(f.root),"env":{}}, timeout_ms=500,max_memory_bytes=64<<20)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(12)))
    accepted = [v["data"]["id"] for v in results if v.get("ok") and "id" in v["data"]]
    for v in results:
        assert v.get("ok") or v["error"]["code"] == "maintenance", v
    assert f.wait(blocker)["state"] == "succeeded"
    f.stop()
    f.start()
    for id in accepted:
        assert f.rpc("status",id=id)["state"] == "queued"
    f.rpc("resume")
    for id in accepted:
        assert f.wait({"id":id})["state"] == "succeeded"
    print("PASS: drain/resume, atomic admission, FIFO persistence, idempotency, drain-all, CLI wait and reload")


if __name__ == "__main__":
    f = Fixture()
    try:
        test(f)
    finally:
        f.close()
