"""Comparable per-process CPU/RSS samples for the optional overhead scripts."""
import ctypes
import os
import sys
from pathlib import Path
from types import SimpleNamespace

MEMORY_METRIC = 'physical_footprint' if sys.platform == 'darwin' else 'rss'

if sys.platform == 'darwin':
    lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    class Usage(ctypes.Structure):
        _fields_ = [('uuid', ctypes.c_ubyte * 16)] + [(name, ctypes.c_uint64) for name in
            ['user', 'system', 'idle', 'interrupt', 'pageins', 'wired', 'resident', 'footprint', 'start', 'exit']]
    def usage(pid):
        value = Usage()
        assert lib.proc_pid_rusage(pid, 0, ctypes.byref(value)) == 0
        return value
else:
    def usage(pid):
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        tick = os.sysconf('SC_CLK_TCK')
        rss = int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
        # Benchmark-only accounting; never adds smaps work to job monitoring.
        memory = {}
        for line in Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines():
            key, _, value = line.partition(':')
            if key in {'Pss', 'Private_Clean', 'Private_Dirty'}:
                memory[key] = int(value.split()[0]) * 1024
        return SimpleNamespace(user=int(fields[11])*1_000_000_000//tick,
                               system=int(fields[12])*1_000_000_000//tick,
                               resident=rss, footprint=rss, pss=memory['Pss'],
                               private=memory['Private_Clean'] + memory['Private_Dirty'])
