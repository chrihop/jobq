#!/usr/bin/env python3
"""Linux power-profile regression using a private D-Bus and fake power daemon.

Requires dbus-daemon and system Python with PyGObject. Never changes host power.
/usr/bin/python3 tests/power.py target/release/jobq
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from gi.repository import Gio, GLib

NAME = 'org.freedesktop.UPower.PowerProfiles'
PATH = '/org/freedesktop/UPower/PowerProfiles'
ADMIN = 'org.jobq.TestPower'
PROPS = 'org.freedesktop.DBus.Properties'
XML = f'''<node><interface name="{NAME}">
<method name="HoldProfile"><arg type="s" direction="in"/><arg type="s" direction="in"/><arg type="s" direction="in"/><arg type="u" direction="out"/></method>
<method name="ReleaseProfile"><arg type="u" direction="in"/></method>
<signal name="ProfileReleased"><arg type="u"/></signal>
<property name="ActiveProfile" type="s" access="read"/>
<property name="PerformanceDegraded" type="s" access="read"/>
</interface><interface name="{ADMIN}">
<method name="Info"><arg type="s" direction="out"/></method>
<method name="SetProfile"><arg type="s" direction="in"/></method>
<method name="SetDenied"><arg type="b" direction="in"/></method>
</interface></node>'''


def connection(address):
    return Gio.DBusConnection.new_for_address_sync(address,
        Gio.DBusConnectionFlags.AUTHENTICATION_CLIENT | Gio.DBusConnectionFlags.MESSAGE_BUS_CONNECTION,
        None, None)


def serve(address):
    bus = connection(address)
    bus.call_sync('org.freedesktop.DBus', '/org/freedesktop/DBus', 'org.freedesktop.DBus',
                  'RequestName', GLib.Variant('(su)', (NAME, 0)), None, 0, 1000, None)
    state = dict(active='balanced', baseline='balanced', denied=False, attempts=0, holds={})

    def update():
        if bus.is_closed(): return
        profiles = [v[1] for v in state['holds'].values()]
        state['active'] = 'power-saver' if 'power-saver' in profiles else 'performance' if profiles else state['baseline']
        bus.emit_signal(None, PATH, PROPS, 'PropertiesChanged', GLib.Variant('(sa{sv}as)',
            (NAME, {'ActiveProfile': GLib.Variant('s', state['active'])}, [])))

    def method(conn, sender, path, interface, member, parameters, invocation):
        args = parameters.unpack()
        if member == 'HoldProfile':
            state['attempts'] += 1
            if state['denied']:
                invocation.return_dbus_error('org.freedesktop.DBus.Error.AccessDenied', 'Not Authorized')
                return
            cookie = state['attempts']
            state['holds'][cookie] = (sender, args[0])
            update()
            invocation.return_value(GLib.Variant('(u)', (cookie,)))
        elif member == 'ReleaseProfile':
            state['holds'].pop(args[0], None)
            update()
            invocation.return_value(None)
        elif member == 'SetProfile':
            for cookie, (owner, _) in state['holds'].items():
                bus.emit_signal(owner, PATH, NAME, 'ProfileReleased', GLib.Variant('(u)', (cookie,)))
            state['holds'].clear()
            state['baseline'] = args[0]
            update()
            invocation.return_value(None)
        elif member == 'SetDenied':
            state['denied'] = args[0]
            invocation.return_value(None)
        elif member == 'Info':
            invocation.return_value(GLib.Variant('(s)', (json.dumps(state),)))
        else:
            invocation.return_dbus_error('org.freedesktop.DBus.Error.UnknownMethod', member)

    def prop(conn, sender, path, interface, name):
        return GLib.Variant('s', state['active'] if name == 'ActiveProfile' else '')

    for interface in Gio.DBusNodeInfo.new_for_xml(XML).interfaces:
        bus.register_object(PATH, interface, method, prop, None)

    def gone(conn, sender, path, interface, signal, parameters, user_data):
        name, old, new = parameters.unpack()
        if old and not new:
            state['holds'] = {k: v for k, v in state['holds'].items() if v[0] != name}
            update()

    bus.signal_subscribe('org.freedesktop.DBus', 'org.freedesktop.DBus', 'NameOwnerChanged',
        '/org/freedesktop/DBus', None, 0, gone, None)
    print('ready', flush=True)
    GLib.MainLoop().run()


def main():
    from smoke import Fixture
    bus_process = subprocess.Popen(['dbus-daemon', '--session', '--nofork', '--print-address=1'], stdout=subprocess.PIPE, text=True)
    service = fixture = None
    try:
        address = bus_process.stdout.readline().strip()
        assert address
        service = subprocess.Popen([sys.executable, __file__, '--serve', address], stdout=subprocess.PIPE, text=True)
        assert service.stdout.readline().strip() == 'ready'
        bus = connection(address)
        os.environ['DBUS_SYSTEM_BUS_ADDRESS'] = address

        def call(member, args=None, interface=ADMIN):
            result = bus.call_sync(NAME, PATH, interface, member, args, None, 0, 1000, None).unpack()
            return result[0] if result else None

        def info(): return json.loads(call('Info'))
        def eventually(predicate):
            deadline = time.monotonic() + 5
            while not predicate():
                assert time.monotonic() < deadline, info()
                time.sleep(.02)

        fixture = f = Fixture()
        j = f.submit(['/bin/sleep', '.1']); f.wait(j)
        assert info()['attempts'] == 0

        def configure(enabled, runners=2):
            f.configure(runners=runners, queues='["1s", "5s", "10s"]', cap='128MiB')
            f.config.write_text(f.config.read_text().replace('[env]', f'performance_while_running = {str(enabled).lower()}\n[env]'))
            f.rpc('reload')

        def submit(seconds='5'):
            return f.submit(['/bin/sleep', seconds], timeout=10000)
        def cancel(j):
            f.rpc('cancel', id=j['id']); f.wait(j)
        def idle(): eventually(lambda: not info()['holds'] and info()['active'] == info()['baseline'])
        def power(): return f.rpc('queues')['power_profile']

        configure(True)
        a, b = submit('.3'), submit()
        f.wait(b, 'running')
        assert len(info()['holds']) == 1 and info()['active'] == 'performance'
        assert power()['state'] == 'held'
        assert f.wait(a)['state'] == 'succeeded'
        assert len(info()['holds']) == 1
        cancel(b); idle()
        print('PASS: default-off, concurrent jobs share one hold, idle restores baseline', flush=True)

        configure(True, runners=1)
        a, queued = submit('.3'), submit('.1')
        f.rpc('drain'); f.wait(a); idle()
        assert f.status(queued)['state'] == 'queued'
        f.rpc('resume'); assert f.wait(queued)['state'] == 'succeeded'; idle()
        print('PASS: queued-only/paused work does not hold performance', flush=True)

        a = submit(); f.wait(a, 'running')
        attempts = info()['attempts']
        call('SetProfile', GLib.Variant('(s)', ('power-saver',)))
        eventually(lambda: power()['state'] == 'released_externally')
        time.sleep(.2)
        assert info()['attempts'] == attempts and info()['active'] == 'power-saver'
        cancel(a); idle()
        call('SetProfile', GLib.Variant('(s)', ('balanced',)))

        cookie = call('HoldProfile', GLib.Variant('(sss)', ('power-saver', 'test', 'test')), NAME)
        a = submit(); f.wait(a, 'running')
        assert len(info()['holds']) == 2
        assert power()['observed_profile'] == 'power-saver'
        cancel(a); eventually(lambda: len(info()['holds']) == 1)
        call('ReleaseProfile', GLib.Variant('(u)', (cookie,)), NAME); idle()
        print('PASS: manual changes and other applications power-saver holds respected', flush=True)

        configure(False)
        a = submit(); f.wait(a, 'running'); idle()
        configure(True); eventually(lambda: len(info()['holds']) == 1)
        configure(False); idle(); cancel(a)
        configure(True)
        call('SetDenied', GLib.Variant('(b)', (True,)))
        before = info()['attempts']
        a = submit('.3'); assert f.wait(a)['state'] == 'succeeded'
        assert info()['attempts'] == before + 1
        assert 'Not Authorized' in power()['last_error']
        call('SetDenied', GLib.Variant('(b)', (False,)))
        print('PASS: reload toggles active hold; denial is diagnostic and jobs still run', flush=True)

        a = submit(); f.wait(a, 'running')
        start = time.monotonic()
        f.stop(crash=True); idle()
        assert time.monotonic() - start < 2, 'hold must not leak into job children'
        f.start(); assert f.wait(a)['state'] == 'interrupted'
        a = submit(); f.wait(a, 'running')
        service.terminate(); service.wait(timeout=5)
        eventually(lambda: power()['state'] == 'unavailable')
        cancel(a)
        a = submit('.1'); assert f.wait(a)['state'] == 'succeeded'
        assert power()['last_error']
        print('PASS: SIGKILL releases hold; recovery does not rerun; service loss fails open', flush=True)

        service = subprocess.Popen([sys.executable, __file__, '--serve', address], stdout=subprocess.PIPE, text=True)
        assert service.stdout.readline().strip() == 'ready'
        a = submit('1'); f.wait(a, 'running')
        assert power()['state'] == 'held'
        bus_process.terminate(); bus_process.wait(timeout=5)
        deadline = time.monotonic() + 3
        while power()['state'] != 'unavailable':
            assert time.monotonic() < deadline
            time.sleep(.02)
        assert f.wait(a)['state'] == 'succeeded'
        print('PASS: system-bus disconnect does not terminate jobs or daemon', flush=True)
    finally:
        if fixture: fixture.close()
        if service and service.poll() is None: service.terminate(); service.wait(timeout=5)
        bus_process.terminate(); bus_process.wait(timeout=5)


if __name__ == '__main__':
    if sys.argv[1] == '--serve': serve(sys.argv[2])
    else: main()
