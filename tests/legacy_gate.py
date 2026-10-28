#!/usr/bin/env python3
"""A fake socket/database validates the migration gate without a jobq daemon."""
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time


with tempfile.TemporaryDirectory(prefix="jobq-gate-test-", dir="/tmp") as name:
    root = Path(name)
    socket_path, upstream = root/"socket", root/"upstream"
    db = sqlite3.connect(root/"db")
    db.execute("CREATE TABLE jobs(token TEXT)")
    db.execute("INSERT INTO jobs VALUES('committed')")
    db.commit()
    db.close()
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(socket_path))
    listener.listen()
    listener.settimeout(.1)
    stop = threading.Event()
    def serve():
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            with conn:
                request = json.loads(conn.makefile("rb").readline())
                conn.sendall(json.dumps({"version":1,"ok":True,"data":{"id":"original","ordinary":[],"op":request['op']}}).encode()+b"\n")
    thread = threading.Thread(target=serve)
    thread.start()
    def rpc(**args):
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(3)
            s.connect(str(socket_path))
            s.sendall(json.dumps(dict(version=1,**args)).encode()+b"\n")
            return json.loads(s.makefile("rb").readline())
    command = [sys.executable,str(Path(__file__).resolve().parents[1]/"scripts/legacy_maintenance_gate.py")]
    for key, value in dict(socket=socket_path,upstream=upstream,database=root/"db",control=root/"control",ready=root/"ready").items():
        command += ["--"+key,str(value)]
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        end=time.monotonic()+5
        while not (root/"ready").exists():
            assert time.monotonic()<end and p.poll() is None
            time.sleep(.02)
        assert rpc(op="submit", token="new")["error"]["code"]=="maintenance"
        assert rpc(op="submit", token="committed")["data"]["id"]=="original"
        assert rpc(op="status",id="original")["ok"]
        assert rpc(op="queues")["data"]["maintenance"]["admission_open"] is False
        assert rpc(op="reload")["error"]["code"]=="maintenance"
        socket_path.unlink()  # Simulate the legacy daemon's pathname cleanup.
        end=time.monotonic()+3
        while not socket_path.exists():
            assert time.monotonic()<end
            time.sleep(.05)
        assert rpc(op="submit", token="new")["error"]["code"]=="maintenance"
        (root/"control").write_text("restore")
        out,err=p.communicate(timeout=5)
        assert p.returncode==0,(out,err)
        assert rpc(op="submit",token="new")["ok"]
        assert not upstream.exists()
        print("PASS: maintenance gate rejects new work, preserves retries/reads, rebinds and restores safely")
    finally:
        if p.poll() is None:p.terminate();p.communicate(timeout=5)
        stop.set();thread.join(timeout=2);listener.close()
