#!/usr/bin/env python3
"""Keep desktop components alive without ending a Seat on a transient exit."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time


class Session:
    def __init__(self, names, start, stop, display_alive, clock=time.monotonic):
        self.names, self.start, self.stop = names, start, stop
        self.display_alive, self.clock = display_alive, clock
        self.processes, self.restarts = {}, {name: [] for name in names}
        self.failure, self.changed = None, False
        self.started = set()
        self.ready = False
        self.browser_changed = False

    def tick(self):
        self.changed = False
        self.browser_changed = False
        if self.failure:
            return
        if not self.display_alive():
            self.failure = 'display_exited'
            return
        for name in self.names:
            process = self.processes.get(name)
            if process is not None and process.poll() is None:
                continue
            if process is not None:
                self.stop(name, process)
                self.processes.pop(name)
            if name in self.started:
                recent = [when for when in self.restarts[name] if self.clock() - when < 60]
                self.restarts[name] = recent
                if len(recent) >= 3:
                    self.failure = 'restart_limit'
                    return
                recent.append(self.clock())
            self.started.add(name)
            self.changed = True
            self.browser_changed |= name == 'browser'
            try:
                self.processes[name] = self.start(name)
            except OSError:
                # Retry a failed spawn on the next tick, spending one restart
                # per attempt rather than counting both exit and spawn error.
                if len(self.restarts[name]) >= 3:
                    self.failure = 'restart_limit'

    def poll_readiness(self, marker, probe):
        if self.browser_changed:
            self.ready = False
            marker.unlink(missing_ok=True)
        if not self.failure and not self.ready and probe():
            marker.touch()
            self.ready = True


def display_alive(pid):
    try:
        fields = Path('/proc/%d/status' % pid).read_text().splitlines()
        return not any(line.startswith('State:') and line.split()[1] in ('Z', 'X') for line in fields)
    except OSError:
        return False


def stop_process(_name, process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def browser_ready(detail=None):
    detail = {} if detail is None else detail
    detail.clear()
    detail['phase'] = 'client_list'
    try:
        windows = subprocess.run(['xprop', '-root', '_NET_CLIENT_LIST'], capture_output=True,
                                 text=True, timeout=1, check=True).stdout
        windows = re.findall(r'0x[0-9a-fA-F]+', windows)[:256]
        detail.update(phase='window_titles', window_count=len(windows), titles_checked=0)
        for window in windows:
            title = subprocess.run(['xprop', '-id', window, '_NET_WM_NAME'], capture_output=True,
                                   text=True, timeout=1).stdout
            detail['titles_checked'] += 1
            if 'wefty-seat-%s-ready' % os.environ['SEAT_ID'] in title:
                detail['phase'] = 'ready'
                return True
    except (OSError, subprocess.SubprocessError) as error:
        detail['error'] = type(error).__name__
    return False


def component_commands():
    return {
        'desktop': ['xfce4-session'],
        'browser': ['chromium', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
                    '--no-first-run', '--no-default-browser-check', '--start-fullscreen',
                    '--hide-crash-restore-bubble', '--profile-directory=Default',
                    '--user-data-dir=' + os.environ['HOME'] + '/.config/chromium',
                    'file:///opt/wefty-computer/seat.html?place=' + os.environ['SEAT_ID']],
        'driver': ['/usr/local/libexec/wefty-watch-driver'],
        'edges': ['/usr/local/libexec/wefty-seat-endpoints'],
    }


def launch_component(name, command, runtime):
    if name == 'browser':
        # The old browser's process group has stopped before this launch.
        # Keep cookies/preferences, but suppress crash-session UI on recovery.
        preferences = Path(os.environ['HOME']) / '.config/chromium/Default/Preferences'
        try:
            value = json.loads(preferences.read_text())
        except (FileNotFoundError, ValueError, UnicodeError):
            value = None  # Let Chromium create/repair a missing or invalid file.
        if isinstance(value, dict) and isinstance(value.get('profile', {}), dict):
            profile = value.setdefault('profile', {})
            profile.update(exit_type='Normal', exited_cleanly=True)
            temporary = preferences.with_name('Preferences.wefty-new')
            with temporary.open('w') as target:
                os.fchmod(target.fileno(), 0o600)
                json.dump(value, target, separators=(',', ':'))
            os.replace(temporary, preferences)
    process = subprocess.Popen(command, start_new_session=True)
    temporary = runtime / (name + '.pid.new')
    temporary.write_text(str(process.pid))
    os.replace(temporary, runtime / (name + '.pid'))
    return process


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--display-pid', type=int, required=True)
    args = parser.parse_args()
    runtime = Path(os.environ['XDG_RUNTIME_DIR'])
    commands = component_commands()

    def start(name):
        return launch_component(name, commands[name], runtime)

    session = Session(commands, start, stop_process, lambda: display_alive(args.display_pid))
    ready = runtime / 'desktop-ready'
    last_browser_log = None

    def probe_browser():
        nonlocal last_browser_log
        detail = {}
        result = browser_ready(detail)
        now = time.monotonic()
        if result or last_browser_log is None or now - last_browser_log >= 5:
            # Never log window titles or page content, only the condition
            # which prevents the desktop-ready marker from being written.
            try:
                print(json.dumps({'event': 'browser_readiness', 'place': int(os.environ['SEAT_ID']),
                                  'monotonic_ms': round(now * 1000, 3),
                                  'readiness': detail}, separators=(',', ':')), file=sys.stderr, flush=True)
            except OSError:
                pass  # A failed Computer log must not end the desktop.
            last_browser_log = now
        return result

    try:
        while not session.failure:
            session.tick()
            session.poll_readiness(ready, probe_browser)
            time.sleep(0.1)
    finally:
        ready.unlink(missing_ok=True)
        for name, process in session.processes.items():
            stop_process(name, process)
    (runtime / 'launcher-failure').write_text(session.failure or 'desktop_exited')
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
