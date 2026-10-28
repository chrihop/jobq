#!/usr/bin/env python3
"""Set-wait regressions without stopping the daemon.
Run: python3 tests/wait_many.py /absolute/jobq [--live]
Default: private protocol fixtures. --live also submits small owned jobs to the
existing daemon, with explicit limits, and tests CLI/MCP interoperability.
Add --long with --live to verify a >30s blocking set wait and CLI interruption.
"""
import json
import pathlib
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from mcp import BIN, McpClient
import signal


def snapshot(id, state="queued"):
    return dict(id=id, state=state, estimate=None,
                snapshot_at_ms=int(time.time()*1000),
                outcome=dict(exit_code=7 if state=="failed" else 0),
                maintenance=dict(mode="paused", admission_open=False, dispatch_open=False))


class Fixture:
    def __init__(self, responder):
        self.temp = tempfile.TemporaryDirectory(prefix="jq-set-", dir="/tmp")
        self.path = str(pathlib.Path(self.temp.name)/"socket")
        self.ops = []
        owner = self
        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                line = self.rfile.readline()
                if not line: return
                req = json.loads(line)
                owner.ops.append(req["op"])
                value = responder(req)
                try:
                    self.wfile.write(json.dumps(value).encode()+b"\n")
                except (BrokenPipeError, ConnectionResetError):
                    pass
        class Server(socketserver.ThreadingUnixStreamServer):
            daemon_threads = True
            request_queue_size = 128
        self.server = Server(self.path, Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.client = McpClient(self.path)
    def close(self):
        self.client.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()


def ok(data):
    return dict(version=1, ok=True, data=data)


def cli(socket, *args):
    return subprocess.run([str(BIN), "--socket", str(socket), "--json", "wait", *args],
                          capture_output=True, text=True, timeout=5)


def mock_tests():
    calls = {}
    states = {"done":"succeeded", "bad":"failed", "held":"queued", "cleanup":"cancelling"}
    def respond(req):
        if req["op"] == "queues": return ok({"maintenance":{"dispatch_open":False}})
        assert req["op"] == "status", req
        id = req["id"]
        calls[id] = calls.get(id,0)+1
        if id == "missing": return dict(version=1,ok=False,error=dict(code="not_found",message="unknown job"))
        return ok(snapshot(id, states.get(id,"queued")))
    f = Fixture(respond); c=f.client
    try:
        tools=c.request("tools/list")["tools"]
        t=next(t for t in tools if t["name"]=="jobq_wait_many")
        assert t["inputSchema"]["type"]=="object"
        assert not any(k in t["inputSchema"] for k in ["oneOf","anyOf","allOf"])
        for ids in [[],["x","x"],[""],[" "],["x"*257],[str(i) for i in range(101)]]:
            assert c.call("jobq_wait_many",error=True,ids=ids,mode="all",block=True)["error"]["code"]=="invalid_input"
        for fields in [dict(block=True,wait_ms=1),dict(wait_ms=0),dict(wait_ms=30001)]:
            assert c.call("jobq_wait_many",error=True,ids=["held"],mode="all",**fields)["error"]["code"]=="invalid_input"
        v=c.call("jobq_wait_many",ids=["held","bad","done","cleanup"],mode="any",block=True)
        assert v["condition_met"] and not v["wait_expired"]
        assert v["completed_ids"]==["bad","done"] and v["pending_ids"]==["held","cleanup"]
        assert [j["id"] for j in v["jobs"]]==["held","bad","done","cleanup"]
        assert c.call("jobq_wait_many",error=True,ids=["done","missing"],mode="any",block=True)["error"]["code"]=="not_found"
        start=time.monotonic()
        v=c.call("jobq_wait_many",ids=["held","cleanup"],mode="all",wait_ms=100)
        assert v["wait_expired"] and time.monotonic()-start<1
        assert v["pending_ids"]==["held","cleanup"]
        # any + previously completed IDs returns immediately; pending IDs avoid repeats.
        v=c.call("jobq_wait_many",ids=["done"],mode="any",block=True)
        assert v["completed_ids"]==["done"]
        states.update({state:state for state in ["succeeded","failed","cancelled","timed_out","memory_exceeded","spawn_failed","monitoring_failed","interrupted"]})
        ids=list(states)[4:]
        assert c.call("jobq_wait_many",ids=ids,mode="all",block=True)["completed_ids"]==ids
        hundred = ["batch-"+str(i) for i in range(100)]
        states.update({id:"succeeded" for id in hundred})
        assert c.call("jobq_wait_many",ids=hundred,mode="all",block=True)["completed_ids"]==hundred
        _,future=c.begin("tools/call",dict(name="jobq_wait_many",arguments=dict(ids=["held","cleanup"],mode="any",block=True)))
        time.sleep(.1)
        assert not future.done()
        states["held"]="succeeded"
        next_done=c.decode(future.result(3)["result"])
        assert next_done["completed_ids"]==["held"] and next_done["pending_ids"]==["cleanup"]
        states["held"]="queued"
        states["weird"]="unknown_state"
        assert c.call("jobq_wait_many",error=True,ids=["weird"],mode="all",block=True)["error"]["code"]=="invalid_response"
        # Cached terminal observations survive record pruning during a long wait.
        calls.clear()
        _,future=c.begin("tools/call",dict(name="jobq_wait_many",arguments=dict(ids=["done","held"],mode="all",block=True)))
        time.sleep(.3)
        assert not c.call("jobq_queues")["maintenance"]["dispatch_open"]
        states["held"]="succeeded"
        assert c.decode(future.result(3)["result"])["condition_met"]
        assert calls["done"]==1,calls
        states["held"]="queued"
        id,future=c.begin("tools/call",dict(name="jobq_wait_many",arguments=dict(ids=["held"],mode="all",block=True)))
        time.sleep(.05)
        c.notify("notifications/cancelled",dict(requestId=id,reason="test"))
        try: future.result(.5)
        except TimeoutError: pass  # MCP may suppress responses to cancelled requests.
        assert c.call("jobq_status",id="held")["state"]=="queued"
        assert cli(f.path,"--any","done","held").returncode==0
        assert cli(f.path,"--all","done","bad").returncode==1
        bounded=cli(f.path,"--all","--wait-ms","100","held")
        assert bounded.returncode==124 and json.loads(bounded.stdout)["wait_expired"]
        assert cli(f.path,"--any","--all","done").returncode!=0
        assert cli(f.path,"done","held").returncode!=0
        assert cli(f.path,"--any","done","missing").returncode!=0
        # Existing single-job CLI result/exit code remains unchanged.
        single=cli(f.path,"bad");assert single.returncode==7 and json.loads(single.stdout)["id"]=="bad"
        assert set(f.ops)<= {"status","queues"},f.ops
    finally:f.close()

    # One overall deadline, including initial and subsequent blocked daemon I/O.
    for initial_block in [True,False]:
        counts={}
        def stalled(req):
            id=req['id'];counts[id]=counts.get(id,0)+1
            if initial_block or counts[id]>1:time.sleep(.7)
            return ok(snapshot(id))
        f=Fixture(stalled)
        try:
            start=time.monotonic()
            v=f.client.call("jobq_wait_many",error=initial_block,ids=["x","y"],mode="all",wait_ms=350)
            assert time.monotonic()-start<.65
            if initial_block:assert v["error"]["code"]=="daemon_timeout"
            else:assert v["wait_expired"] and v["pending_ids"]==["x","y"]
        finally:f.close()
    print("PASS: set policy, schemas, validation, order, terminal failures, unknown IDs, paused waits, concurrency, cancellation, CLI exits, overall I/O deadlines",flush=True)


def live_tests():
    # Never drain, restart or cancel any job outside this test's owned IDs.
    c=McpClient('/tmp/jobq-'+str(__import__('os').getuid())+'/socket');owned=[]
    try:
        q=c.call('jobq_queues')
        assert q.get('maintenance',{}).get('admission_open',True),'maintenance: do not bypass'
        for argv in [['/bin/sleep','.2'],['/bin/sleep','2'],['/bin/sh','-c','exit 7']]:
            j=c.submit('/tmp',argv,timeout='5s',max_memory='32MiB',submission_token='wait-many-live-'+str(uuid.uuid4()));owned.append(j['id'])
        v=c.decode(c.request('tools/call',dict(name='jobq_wait_many',arguments=dict(ids=owned,mode='any',block=True)),timeout=60))
        assert v['completed_ids'] and v['condition_met']
        v=c.decode(c.request('tools/call',dict(name='jobq_wait_many',arguments=dict(ids=owned,mode='all',block=True)),timeout=60))
        assert v['completed_ids']==owned and v['pending_ids']==[]
        assert [j['state'] for j in v['jobs']]==['succeeded','succeeded','failed']
        assert cli(c.process.args[2],'--all',*owned).returncode==1
        # Adapter death drops only the waiter. Reattach through a fresh adapter.
        j=c.submit('/tmp',['/bin/sleep','1'],timeout='5s',max_memory='32MiB');owned.append(j['id'])
        c.begin('tools/call',dict(name='jobq_wait_many',arguments=dict(ids=[j['id']],mode='all',block=True)))
        c.close(kill=True);c=McpClient('/tmp/jobq-'+str(__import__('os').getuid())+'/socket')
        v=c.decode(c.request('tools/call',dict(name='jobq_wait_many',arguments=dict(ids=[j['id']],mode='all',block=True)),timeout=60))
        assert v['jobs'][0]['state']=='succeeded' and not v['jobs'][0]['cancellation_requested']
        if '--long' in sys.argv:
            j=c.submit('/tmp',['/bin/sleep','35'],timeout='60s',max_memory='32MiB');owned.append(j['id'])
            _,future=c.begin('tools/call',dict(name='jobq_wait_many',arguments=dict(ids=[j['id']],mode='all',block=True)))
            process=subprocess.Popen([str(BIN),'--json','wait','--all',j['id']],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                time.sleep(31)
                assert not future.done() and process.poll() is None
                process.send_signal(signal.SIGINT);process.communicate(timeout=3)
                assert c.call('jobq_status',id=j['id'])['state'] in ['queued','starting','running']
                value=c.decode(future.result(60)['result'])
                assert value['condition_met'] and value['jobs'][0]['state']=='succeeded'
            finally:
                if process.poll() is None:process.terminate();process.communicate(timeout=3)
        print('PASS: live daemon CLI/MCP any/all, mixed outcomes, adapter death leaves jobs alive; IDs '+','.join(owned),flush=True)
    finally:
        for id in owned:
            try:
                state=c.call('jobq_status',id=id)['state']
                if state in ['queued','starting','running','cancelling']:
                    c.call('jobq_cancel',id=id)
                    c.request('tools/call',dict(name='jobq_wait',arguments=dict(id=id,block=True)),timeout=60)
            except Exception:pass
        c.close()


if __name__=='__main__':
    mock_tests()
    if '--live' in sys.argv:live_tests()
