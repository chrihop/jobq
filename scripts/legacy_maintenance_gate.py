#!/usr/bin/env python3
"""One-time migration gate for a pre-drain jobq daemon. Never signals jobs.

Move the live socket to --upstream and serve a maintenance gate at --socket.
Read/status/cancel requests and retries of committed tokens are forwarded.
New submissions and reloads are rejected. Keep this running until the old daemon
is drained and unloaded. Write 'remove' to --control to stop after unloading;
write 'restore' to abort maintenance and return the original socket to clients.
The private upstream is for the maintenance operator only, not a security boundary.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import sqlite3
import stat


def committed(database, token):
    if not isinstance(token, str):
        return False
    with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=2) as db:
        return db.execute("SELECT 1 FROM jobs WHERE token=?", (token,)).fetchone() is not None


async def run(args):
    socket, upstream = args.socket, args.upstream
    if args.adopt:
        if socket.exists() or not upstream.exists():
            raise RuntimeError("adopt requires absent public path and preserved upstream")
    else:
        if upstream.exists():
            raise RuntimeError("upstream already exists; inspect before retrying")
        if not stat.S_ISSOCK(socket.lstat().st_mode):
            raise RuntimeError("source is not a socket")
        os.rename(socket, upstream)
    inode = None

    async def handle(reader, writer):
        try:
            async with asyncio.timeout(12):
                line = await reader.readline()
                request = json.loads(line)
                op = request.get("op")
                allowed = op in {"status", "jobs", "queues", "logs", "cancel"}
                if op == "submit":
                    allowed = committed(args.database, request.get("token"))
                if allowed:
                    ur, uw = await asyncio.open_unix_connection(str(upstream), limit=64*1024*1024)
                    try:
                        uw.write(line)
                        await uw.drain()
                        response = await ur.readline()
                        if not response:
                            raise RuntimeError("upstream closed")
                    finally:
                        uw.close()
                        await uw.wait_closed()
                    if op == "queues":
                        value = json.loads(response)
                        if value.get("ok"):
                            value["data"]["maintenance"] = {"mode": "legacy_draining_all", "admission_open": False, "dispatch_open": True}
                            for q in value["data"].get("ordinary", []):
                                q["accepting"] = False
                        response = json.dumps(value).encode()+b"\n"
                else:
                    response = json.dumps({"version": 1, "ok": False, "error": {
                        "code": "maintenance", "message": "Admission closed for safe jobq upgrade. Existing jobs continue. Retry the same token after maintenance; this request was not accepted."}}).encode()+b"\n"
                writer.write(response)
                await writer.drain()
        except Exception as exc:
            # Never log request bodies or inherited environments.
            try:
                writer.write(json.dumps({"version": 1, "ok": False, "error": {
                    "code": "maintenance_unavailable", "message": f"Maintenance gate: {type(exc).__name__}; retry the same token after recovery."}}).encode()+b"\n")
                await writer.drain()
            except Exception:
                pass
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        server = await asyncio.start_unix_server(handle, path=str(socket), limit=4*1024*1024+1)
        socket.chmod(0o600)
        inode = socket.stat().st_ino
        args.ready.write_text(json.dumps({"pid": os.getpid(), "socket_inode": inode, "upstream": str(upstream)}))
        async with server:
            while not args.control.exists():
                await asyncio.sleep(.5)
                # The legacy daemon's socket guard removes the public pathname
                # when it exits, even though its listener was moved upstream.
                # Keep an actionable maintenance endpoint during native checks.
                if not socket.exists():
                    server.close()
                    await server.wait_closed()
                    server = await asyncio.start_unix_server(handle, path=str(socket), limit=4*1024*1024+1)
                    socket.chmod(0o600)
                    inode = socket.stat().st_ino
            mode = args.control.read_text().strip()
            if mode not in {"restore", "remove"}:
                raise RuntimeError("unknown control: admission remains closed; inspect manually")
        server.close()
        await server.wait_closed()
        if socket.exists() and socket.stat().st_ino == inode:
            socket.unlink()
        if mode == "restore":
            if socket.exists():
                raise RuntimeError("refusing to overwrite changed public socket")
            os.rename(upstream, socket)
    except BaseException:
        # Startup failure restores the untouched old daemon endpoint. After the
        # gate is ready, fail closed instead of accidentally reopening admission.
        if inode is None and not socket.exists():
            os.rename(upstream, socket)
        raise


if __name__ == "__main__":
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    for name in ["socket", "upstream", "database", "control", "ready"]:
        p.add_argument("--"+name, type=Path, required=True)
    p.add_argument("--adopt", action="store_true", help="rebind after the drained legacy daemon removed the public path")
    asyncio.run(run(p.parse_args()))
