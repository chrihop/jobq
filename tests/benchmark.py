#!/usr/bin/env python3
"""Short native overhead measurements; uses the same isolated fixture as smoke.py."""
import concurrent.futures
import ctypes
import datetime
import json
import platform
import sqlite3
import statistics
import subprocess
import time
from smoke import BIN, Fixture

lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)


class Usage(ctypes.Structure):
    _fields_ = [('uuid', ctypes.c_ubyte * 16)] + [(name, ctypes.c_uint64) for name in
        ['user', 'system', 'idle', 'interrupt', 'pageins', 'wired', 'resident', 'footprint', 'start', 'exit']]


def usage(pid):
    value = Usage()
    assert lib.proc_pid_rusage(pid, 0, ctypes.byref(value)) == 0
    return value


def sample(pids, seconds=3):
    before = [usage(pid) for pid in pids]
    start = time.monotonic()
    time.sleep(seconds)
    after = [usage(pid) for pid in pids]
    elapsed = time.monotonic() - start
    cpu = sum((b.user+b.system)-(a.user+a.system) for a, b in zip(before, after)) / 1e9
    return dict(window_seconds=elapsed, cpu_percent_one_core=cpu/elapsed*100,
                rss_bytes=sum(v.resident for v in after), footprint_bytes=sum(v.footprint for v in after),
                process_count=len(pids))


def distribution(values):
    values = sorted(values)
    return dict(count=len(values), p50_ms=statistics.median(values), p95_ms=values[int((len(values)-1)*.95)])


f = Fixture()
try:
    f.configure(runners=16, cap="2GiB", aging="30s")
    f.config.write_text(f.config.read_text().replace('"20ms"', '"100ms"'))
    f.rpc("reload")
    result = dict(measured_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  platform=platform.platform(), cpu_count=f.rpc("queues")["config"]["cpu_count"],
                  physical_memory_bytes=f.rpc("queues")["config"]["physical_memory_bytes"],
                  release_binary_bytes=BIN.stat().st_size, monitor_interval_ms=100)
    result['idle_daemon'] = sample([f.process.pid])

    jobs = [f.submit(["/bin/sleep", "8"], timeout=10000) for _ in range(16)]
    for job in jobs:
        f.wait(job, "running")
    with sqlite3.connect(f.state / "jobs.sqlite") as conn:
        helpers = [row[0] for row in conn.execute('SELECT helper_pid FROM jobs WHERE helper_pid IS NOT NULL')]
    result['daemon_and_16_supervisors'] = sample([f.process.pid] + helpers)
    for job in jobs:
        f.rpc("cancel", id=job["id"])
    for job in jobs:
        f.wait(job)

    direct = []
    for _ in range(40):
        start = time.perf_counter()
        subprocess.run(["/usr/bin/true"], check=True)
        direct.append((time.perf_counter()-start)*1000)
    result['direct_spawn_to_exit'] = distribution(direct)

    def submit(_):
        start = time.perf_counter()
        j = f.submit()
        return j, (time.perf_counter()-start)*1000
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        submissions = list(pool.map(submit, range(64)))
    done = [f.wait(j) for j, _ in submissions]
    assert all(j['state'] == 'succeeded' for j in done)
    result['8_concurrent_clients_admission'] = distribution([ms for _, ms in submissions])
    result['queued_to_launch'] = distribution([j['started_at_ms']-j['submitted_at_ms'] for j in done])
    result['supervised_command_elapsed'] = distribution([j['outcome']['elapsed_ms'] for j in done])
    print(json.dumps(result, indent=2))
finally:
    f.close()
