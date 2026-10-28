#!/usr/bin/env python3
"""Authenticated MCP HTTP integration. Exclusive test-owned daemon, no host exposure.
python3 tests/http_mcp.py target/release/jobq
"""
import concurrent.futures
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time
from smoke import BIN, Fixture


class HttpAdapter:
    def __init__(self, fixture):
        self.f = fixture
        self.key = secrets.token_urlsafe(32)
        self.token_file = fixture.root / ('tokens-' + secrets.token_hex(4))
        self.token_file.write_text(self.key + '\n')
        self.token_file.chmod(0o600)
        self.log_path = self.token_file.with_suffix('.log')
        self.log = self.log_path.open('wb')
        self.process = subprocess.Popen([str(BIN), '--socket', str(fixture.sock), 'mcp', '--http',
            '--listen', '127.0.0.1:0', '--token-file', str(self.token_file),
            '--allowed-origin', 'https://trusted.example:443'], stdout=subprocess.PIPE, stderr=self.log,
            env=dict(os.environ, JOBQ_HTTP_HOST='execution-host'))
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            match = re.search(r'http://127\.0\.0\.1:(\d+)/mcp', self.log_path.read_text())
            if match:
                self.port = int(match[1]); break
            assert self.process.poll() is None, self.log_path.read_text()
            time.sleep(.02)
        else: raise AssertionError('HTTP adapter startup timed out')

    def request(self, method='POST', message=None, key=None, headers=None, raw=None):
        h = {'Accept':'application/json, text/event-stream', 'Content-Type':'application/json',
             'MCP-Protocol-Version':'2025-11-25'}
        if key is not None: h['Authorization'] = 'Bearer ' + key
        h.update(headers or {})
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=45)
        try:
            conn.request(method, '/mcp', body=raw if raw is not None else json.dumps(message or {}), headers=h)
            response = conn.getresponse()
            body = response.read()
            if response.status == 200:
                if 'text/event-stream' in response.getheader('Content-Type', ''):
                    messages = [json.loads(line[5:].strip()) for line in body.decode().splitlines() if line.startswith('data:') and line[5:].strip()]
                    body = json.dumps(next(v for v in messages if 'id' in v)).encode()
                return response.status, json.loads(body)
            return response.status, body.decode(errors='replace')
        finally: conn.close()

    def rpc(self, method, params=None, key=None):
        code, result = self.request(message=dict(jsonrpc='2.0', id=1, method=method, params=params or {}), key=key or self.key)
        assert code == 200, (code, result)
        assert 'error' not in result, result
        return result['result']

    def call(self, name, error=False, **arguments):
        result = self.rpc('tools/call', dict(name=name, arguments=arguments))
        assert bool(result.get('isError')) == error, result
        return result.get('structuredContent') or json.loads(result['content'][0]['text'])

    def close(self):
        if self.process.poll() is None: self.process.terminate()
        stdout, _ = self.process.communicate(timeout=5)
        assert not stdout, 'HTTP adapter must not log credentials or messages to stdout'
        self.log.close()
        assert self.key not in self.log_path.read_text()


def initialize(h):
    result = h.rpc('initialize', dict(protocolVersion='2025-11-25', capabilities={}, clientInfo=dict(name='http-test', version='1')))
    assert result['serverInfo']['name'] == 'jobq'
    code, _ = h.request(message=dict(jsonrpc='2.0',method='notifications/initialized'),key=h.key)
    assert code == 202


def test(f, h):
    for args in [ ['--http'], ['--token-file',str(h.token_file)],
                  ['--http','--token-file',str(h.token_file),'--listen','0.0.0.0:8765'] ]:
        rejected = subprocess.run([str(BIN),'mcp',*args],capture_output=True,timeout=3)
        assert rejected.returncode != 0 and not rejected.stdout
    for method in ('GET','POST','DELETE'):
        assert h.request(method)[0] == 401
        assert h.request(method, key='x'*43)[0] == 401
        assert h.request(method, key=h.key, headers={'Origin':'https://evil.example'})[0] == 403
        assert h.request(method, headers={'Origin':'https://evil.example'})[0] == 403
    assert h.request(key=h.key, headers={'Host':'evil.example'})[0] == 403
    assert not f.rpc('jobs')['jobs']
    initialize(h)
    assert len(h.rpc('tools/list')['tools']) == 8
    assert h.request(key=h.key, headers={'Origin':'https://trusted.example'}, message=dict(jsonrpc='2.0',id=1,method='tools/list'))[0] == 200
    assert h.request('GET',key=h.key)[0] == 405
    assert h.request(key=h.key, raw=b'x'*(1024*1024+1))[0] == 413
    assert h.call('jobq_queues')['active_runners'] == 0
    token = secrets.token_hex(16)
    args = dict(cwd=str(f.root), submission_token=token, timeout='10s', max_memory='64MiB',
        argv=[sys.executable,'-c','import json,os,sys;print(json.dumps([os.getcwd(),os.environ["JOBQ_HTTP_HOST"],sys.argv[1]]))','arg with spaces;$HOME'])
    job = h.call('jobq_submit', **args)
    assert h.call('jobq_submit', **args)['id'] == job['id']
    assert h.call('jobq_wait', id=job['id'], block=True)['state'] == 'succeeded'
    assert h.call('jobq_status', id=job['id'])['state'] == 'succeeded'
    assert json.loads(h.call('jobq_logs', id=job['id'])['text']) == [str(f.root),'execution-host','arg with spaces;$HOME']
    assert h.call('jobq_jobs', all=True)['jobs']
    assert h.call('jobq_wait_many', ids=[job['id']], mode='all', block=True)['condition_met']
    print('PASS: HTTP authentication, discovery, Origin/Host, bounded body, execution context, idempotency and tools', flush=True)

    args.update(submission_token=secrets.token_hex(16), argv=['/bin/sleep','5'])
    job = h.call('jobq_submit', **args)
    with concurrent.futures.ThreadPoolExecutor() as pool:
        waiting = pool.submit(h.call, 'jobq_wait', id=job['id'], block=True)
        assert h.call('jobq_status',id=job['id'])['state'] in {'queued','starting','running'}
        h.call('jobq_cancel',id=job['id'])
        assert waiting.result(10)['state'] == 'cancelled'
    print('PASS: concurrent HTTP blocking wait, status and cancellation', flush=True)

    old, new = h.key, secrets.token_urlsafe(32)
    h.token_file.write_text(new+'\n');h.process.send_signal(signal.SIGHUP)
    deadline = time.monotonic()+3
    while h.request(key=old)[0] != 401:
        assert time.monotonic()<deadline;time.sleep(.02)
    h.key = new
    assert h.call('jobq_queues')
    h.token_file.write_text('invalid\n');h.process.send_signal(signal.SIGHUP)
    deadline = time.monotonic()+3
    while h.request(key=new)[0] != 401:
        assert time.monotonic()<deadline;time.sleep(.02)
    h.token_file.write_text(new+'\n');h.process.send_signal(signal.SIGHUP)
    deadline = time.monotonic()+3
    while h.request(key=new)[0] == 401:
        assert time.monotonic()<deadline;time.sleep(.02)
    assert h.call('jobq_queues')
    print('PASS: live token rotation/revocation and fail-closed reload', flush=True)

    f.stop()
    assert len(h.rpc('tools/list')['tools']) == 8
    assert h.call('jobq_queues', error=True)['error']['code'] == 'daemon_unavailable'
    f.start()
    args.update(submission_token=secrets.token_hex(16), argv=['/bin/sleep','1'])
    job = h.call('jobq_submit', **args)
    h.close()
    assert f.wait(job)['state'] == 'succeeded'
    print('PASS: offline/restarted daemon and adapter shutdown without job loss', flush=True)


if __name__ == '__main__':
    fixture = Fixture()
    adapter = None
    try:
        adapter = HttpAdapter(fixture)
        if '--remote-probe' in sys.argv:
            # A supervising test consumes these credentials through a private
            # SSH pipe; never publish this output in diagnostics or artifacts.
            print(json.dumps(dict(port=adapter.port, token=adapter.key, cwd=str(fixture.root))),flush=True)
            sys.stdin.readline()
        else:
            test(fixture, adapter)
    finally:
        if adapter: adapter.close()
        fixture.close()
