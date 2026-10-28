#!/usr/bin/env python3
"""Linux-specific native checks. Exclusive daemon use; synthetic owned jobs only."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from smoke import Fixture
from process_tree import gone


def test(f):
    assert f.rpc('queues')['memory_metric']=='rss'
    f.configure(queues='["1s", "5s", "10s"]',cap='256MiB')
    f.rpc('reload')
    # A leader can exit before the first monitoring tick. Its double-forked,
    # setsid descendant is adopted by this job's supervisor, not leaked to init.
    for n in range(5):
        marker=f.root/f'orphan-{n}'
        code=f'''import os,time
pid=os.fork()
if pid:
    os.waitpid(pid,0)
    while not os.path.exists({str(marker)!r}): time.sleep(.001)
    os._exit(0)
os.setsid()
pid=os.fork()
if pid: os._exit(0)
open({str(marker)!r},'w').write(str(os.getpid()))
time.sleep(30)
'''
        j=f.submit([sys.executable,'-c',code],timeout=5000)
        v=f.wait(j)
        assert v['state']=='succeeded',v
        pid=int(marker.read_text());gone([pid])
        # Supervisor reaps adopted zombies too; they must not accumulate.
        assert not Path(f'/proc/{pid}').exists(),pid
    print('PASS: immediate double-fork/setsid adoption and reaping',flush=True)

    marker=f.root/'thread-child'
    leaf=f"import os,time; open({str(marker)!r},'w').write(str(os.getpid())); time.sleep(30)"
    code=f"import threading,subprocess,sys,time; t=threading.Thread(target=lambda:subprocess.run([sys.executable,'-c',{leaf!r}],start_new_session=True)); t.start(); time.sleep(30)"
    j=f.submit([sys.executable,'-c',code],timeout=10000,memory=128<<20)
    deadline=time.monotonic()+5
    while not marker.exists():assert time.monotonic()<deadline;time.sleep(.02)
    time.sleep(.15)
    f.rpc('cancel',id=j['id']);assert f.wait(j)['state']=='cancelled';gone([int(marker.read_text())])
    print('PASS: child created by non-leader thread is cancelled',flush=True)

    # Linux wait4 ru_maxrss is KiB; exported accounting must convert to bytes.
    code="import resource,json; a=bytearray(24*1024*1024); print(json.dumps(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))"
    j=f.submit([sys.executable,'-c',code],timeout=5000,memory=128<<20)
    v=f.wait(j);assert v['state']=='succeeded',v
    kib=json.loads(f.logs(j));observed=v['outcome']['command_max_rss_bytes']
    assert observed>=kib*1024 and observed<512<<20,(kib,observed)
    assert v['memory_metric']=='rss'
    assert v['stats']['peak_sampled_footprint_bytes']==v['stats']['peak_sampled_rss_bytes']
    print('PASS: RSS metric and wait4 KiB-to-byte accounting',flush=True)

    # Linux task names are bytes, even when submitted command arguments are UTF-8.
    code="import ctypes,time; assert ctypes.CDLL(None).prctl(15,ctypes.c_char_p(b'odd)\\xff name'),0,0,0)==0; time.sleep(.2)"
    j=f.submit([sys.executable,'-c',code],timeout=5000)
    v=f.wait(j);assert v['state']=='succeeded',v
    print('PASS: non-UTF-8 process names do not break monitoring',flush=True)

    code="import os,json; print(json.dumps(dict(affinity=sorted(os.sched_getaffinity(0)), nice=os.getpriority(os.PRIO_PROCESS,0), policy=os.sched_getscheduler(0), cgroup=open('/proc/self/cgroup').read())))"
    direct=json.loads(subprocess.check_output([sys.executable,'-c',code]))
    j=f.submit([sys.executable,'-c',code],timeout=5000)
    assert f.wait(j)['state']=='succeeded'
    assert json.loads(f.logs(j))==direct
    print('PASS: inherited CPU affinity, nice, scheduler policy and cgroup',flush=True)


if __name__=='__main__':
    assert sys.platform=='linux'
    f=Fixture()
    try:test(f)
    finally:f.close()
