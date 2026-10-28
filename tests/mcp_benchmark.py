#!/usr/bin/env python3
"""Short, reproducible native MCP measurements. Requires no other jobq daemon.
Run: python3 tests/mcp_benchmark.py [target/release/jobq] > mcp-benchmark-results.json
"""
import concurrent.futures
import datetime
import json
import platform
import statistics
import time
from mcp import McpClient
from smoke import BIN, Fixture

from resource_usage import usage, MEMORY_METRIC


def sample(pids, seconds=3):
    before = [usage(pid) for pid in pids]
    start = time.monotonic()
    time.sleep(seconds)
    after = [usage(pid) for pid in pids]
    elapsed = time.monotonic()-start
    cpu = sum((b.user+b.system)-(a.user+a.system) for a, b in zip(before, after)) / 1e9
    result = dict(window_seconds=elapsed, cpu_percent_one_core=cpu/elapsed*100,
                rss_bytes=sum(v.resident for v in after), footprint_bytes=sum(v.footprint for v in after),
                process_count=len(pids))
    if all(hasattr(v, "pss") for v in after):
        result.update(pss_bytes=sum(v.pss for v in after),
                      private_bytes=sum(v.private for v in after))
    return result


def distribution(values):
    values = sorted(values)
    return dict(count=len(values), p50_ms=statistics.median(values), p95_ms=values[int((len(values)-1)*.95)])


def run(f):
    config = f.rpc("queues")["config"]
    result = dict(measured_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  platform=platform.platform(), memory_metric=MEMORY_METRIC, cpu_count=config["cpu_count"],
                  physical_memory_bytes=config["physical_memory_bytes"],
                  runners=config["runners"], memory_cap_bytes=config["memory_cap_bytes"],
                  default_job_memory_bytes=config["default_memory_bytes"],
                  monitor_interval_ms=config["monitor_ms"],
                  release_binary_bytes=BIN.stat().st_size)
    times = []
    for _ in range(20):
        start = time.perf_counter()
        client = McpClient(f.sock)
        client.request("tools/list")
        times.append((time.perf_counter()-start)*1000)
        client.close()
    result["adapter_spawn_initialize_discover"] = distribution(times)
    clients = [McpClient(f.sock) for _ in range(8)]
    try:
        result["idle_adapter"] = sample([clients[0].process.pid])
        result["eight_idle_adapters"] = sample([c.process.pid for c in clients])
        result["idle_daemon_with_mcp_binary"] = sample([f.process.pid])

        def read(n, mcp=True):
            start = time.perf_counter()
            if mcp:
                clients[n % 8].call("jobq_queues")
            else:
                f.rpc("queues")
            return (time.perf_counter()-start)*1000
        result["sequential_mcp_queues"] = distribution([read(n) for n in range(40)])
        result["sequential_socket_queues"] = distribution([read(n, False) for n in range(40)])
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            result["eight_concurrent_mcp_queues"] = distribution(list(pool.map(read, range(80))))

        def submit(n):
            start = time.perf_counter()
            job = clients[n % 8].submit(f.root)
            return job, (time.perf_counter()-start)*1000
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            jobs = list(pool.map(submit, range(64)))
        for job, _ in jobs:
            assert f.wait(job)["state"] == "succeeded"
        result["eight_concurrent_mcp_submissions"] = distribution([ms for _, ms in jobs])
    finally:
        for client in clients:
            client.close()
    return result


if __name__ == "__main__":
    fixture = Fixture()
    try:
        print(json.dumps(run(fixture), indent=2))
    finally:
        fixture.close()
