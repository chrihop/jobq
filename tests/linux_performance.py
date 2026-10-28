#!/usr/bin/env python3
"""Exclusive Linux CPU throughput control. No daemon restart or privilege changes.

python3 tests/linux_performance.py target/release/jobq [--workers N] [--rounds 3]
Runs fixed native work directly before/after each queued batch. Reports total
throughput (including durable admission and cleanup), not a CPU microbenchmark.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time
from smoke import Fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary')  # smoke.BIN uses the same first argument
    parser.add_argument('--workers', type=int, default=len(os.sched_getaffinity(0)))
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--cpu-seconds', type=float, default=8)
    args = parser.parse_args()
    assert sys.platform == 'linux'
    assert 1 <= args.workers <= 256 and 1 <= args.rounds <= 10
    assert 1 <= args.cpu_seconds <= 30
    f = Fixture()
    try:
        executable = f.root / 'cpu-work'
        source = Path(__file__).resolve().parent.parent / 'scripts/linux-cpu-work.c'
        subprocess.run(['cc', '-O2', '-Wall', '-Wextra', '-Werror', str(source),
                        '-o', str(executable)], check=True)
        calibration = json.loads(subprocess.check_output([str(executable), '64000000']))
        loops = max(1, int(64000000 * args.cpu_seconds / calibration['cpu_seconds']))
        command = [str(executable), str(loops)]
        f.configure(runners=args.workers, cap=f'{args.workers * 32}MiB',
                    queues='["10s", "60s", "300s"]', aging='30s')
        f.config.write_text(f.config.read_text().replace('"20ms"', '"100ms"'))
        f.rpc('reload')

        def steady(pids):
            if args.cpu_seconds < 4:
                return None
            def ticks(pid):
                fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
                return int(fields[11]) + int(fields[12])
            def host_ticks():
                busy = 0
                for line in Path('/proc/stat').read_text().splitlines():
                    fields = line.split()
                    if fields[0].startswith('cpu') and fields[0][3:].isdigit():
                        if int(fields[0][3:]) in calibration['affinity']:
                            values = [int(v) for v in fields[1:]]
                            busy += sum(values[i] for i in (0, 1, 2, 5, 6))
                return busy
            try:
                before = sum(ticks(pid) for pid in pids)
                host_before = host_ticks()
                start = time.monotonic()
                time.sleep(2)
                after = sum(ticks(pid) for pid in pids)
                host_after = host_ticks()
                elapsed = time.monotonic() - start
            except FileNotFoundError:
                return None  # Work ended before the measurement interval.
            cores = (after - before) / os.sysconf('SC_CLK_TCK') / elapsed
            host_cores = (host_after - host_before) / os.sysconf('SC_CLK_TCK') / elapsed
            return dict(window_seconds=elapsed, utilized_cpu_cores=cores,
                        available_cpu_percent=cores / len(calibration['affinity']) * 100,
                        host_busy_cores=host_cores,
                        other_busy_cores=max(0, host_cores - cores))

        def measure(queued):
            start = time.monotonic()
            if queued:
                jobs = [f.submit(command, timeout=300000, memory=32 << 20)
                        for _ in range(args.workers)]
                sample = None
                if args.cpu_seconds >= 4:
                    for job in jobs:
                        assert f.wait(job, state='running', timeout=10)['state'] == 'running'
                    pids = [json.loads((f.state / 'jobs' / j['id'] / 'started.json').read_text())['pid']
                            for j in jobs]
                    sample = steady(pids)
                for job in jobs:
                    status = f.wait(job, timeout=300)
                    assert status['state'] == 'succeeded', status
                values = [json.loads(f.logs(job)) for job in jobs]
            else:
                processes = []
                try:
                    for _ in range(args.workers):
                        processes.append(subprocess.Popen(command, stdout=subprocess.PIPE))
                    sample = steady([p.pid for p in processes])
                    values = []
                    for process in processes:
                        out, _ = process.communicate(timeout=300)
                        assert process.returncode == 0
                        values.append(json.loads(out))
                finally:
                    for process in processes:
                        if process.poll() is None:
                            process.kill()
                        process.wait()
            elapsed = time.monotonic() - start
            for v in values:
                for field in ('affinity', 'policy', 'nice'):
                    assert v[field] == calibration[field], (field, v, calibration)
            assert len({v['checksum'] for v in values}) == 1
            cpu = sum(v['cpu_seconds'] for v in values)
            return dict(wall_seconds=elapsed, job_cpu_seconds=cpu,
                        utilized_cpu_cores=cpu / elapsed,
                        available_cpu_percent=cpu / elapsed / len(calibration['affinity']) * 100,
                        steady_sample=sample,
                        checksum=values[0]['checksum'])

        runs = []
        for _ in range(args.rounds):
            before = measure(False)
            queued = measure(True)
            after = measure(False)
            assert before['checksum'] == queued['checksum'] == after['checksum']
            runs.append(dict(direct_before=before, queued=queued, direct_after=after,
                             wall_ratio=queued['wall_seconds'] /
                             statistics.mean([before['wall_seconds'], after['wall_seconds']])))
        print(json.dumps(dict(platform=platform.platform(), workers=args.workers,
                              iterations_per_job=loops, monitor_interval_ms=100,
                              inherited_affinity=calibration['affinity'],
                              inherited_nice=calibration['nice'], inherited_policy=calibration['policy'],
                              median_queued_to_direct_wall_ratio=statistics.median(r['wall_ratio'] for r in runs),
                              runs=runs), indent=2))
    finally:
        f.close()


if __name__ == '__main__':
    main()
