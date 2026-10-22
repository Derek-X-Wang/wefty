#!/usr/bin/env python3
"""Keep desktop components alive without ending a Seat on a transient exit."""
import argparse
import os
from pathlib import Path
import re
import signal
import subprocess
import time


class Session:
    def __init__(self, names, start, stop, display_alive, clock=time.monotonic):
        self.names, self.start, self.stop = names, start, stop
        self.display_alive, self.clock = display_alive, clock
        self.processes, self.restarts = {}, {name: [] for name in names}
        self.failure, self.changed = None, False
        self.started = set()

    def tick(self):
        self.changed = False
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
            try:
                self.processes[name] = self.start(name)
            except OSError:
                # Retry a failed spawn on the next tick, spending one restart
                # per attempt rather than counting both exit and spawn error.
                if len(self.restarts[name]) >= 3:
                    self.failure = 'restart_limit'


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


def browser_ready():
    try:
        windows = subprocess.run(['xprop', '-root', '_NET_CLIENT_LIST'], capture_output=True,
                                 text=True, timeout=1, check=True).stdout
        for window in re.findall(r'0x[0-9a-fA-F]+', windows)[:256]:
            title = subprocess.run(['xprop', '-id', window, '_NET_WM_NAME'], capture_output=True,
                                   text=True, timeout=1).stdout
            if 'wefty-seat-%s-ready' % os.environ['SEAT_ID'] in title:
                return True
    except (OSError, subprocess.SubprocessError):
        pass
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--display-pid', type=int, required=True)
    args = parser.parse_args()
    runtime = Path(os.environ['XDG_RUNTIME_DIR'])
    commands = {
        'desktop': ['xfce4-session'],
        'browser': ['chromium', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
                    '--no-first-run', '--no-default-browser-check', '--start-fullscreen',
                    '--user-data-dir=' + os.environ['HOME'] + '/.config/chromium',
                    'file:///opt/wefty-computer/seat.html?place=' + os.environ['SEAT_ID']],
        'driver': ['/usr/local/libexec/wefty-watch-driver'],
        'edges': ['/usr/local/libexec/wefty-seat-endpoints'],
    }

    def start(name):
        process = subprocess.Popen(commands[name], start_new_session=True)
        temporary = runtime / (name + '.pid.new')
        temporary.write_text(str(process.pid))
        os.replace(temporary, runtime / (name + '.pid'))
        return process

    session = Session(commands, start, stop_process, lambda: display_alive(args.display_pid))
    ready = runtime / 'desktop-ready'
    try:
        while not session.failure:
            session.tick()
            if session.changed:
                ready.unlink(missing_ok=True)
            if not session.failure and browser_ready():
                ready.touch()
            time.sleep(0.1)
    finally:
        ready.unlink(missing_ok=True)
        for name, process in session.processes.items():
            stop_process(name, process)
    (runtime / 'launcher-failure').write_text(session.failure or 'desktop_exited')
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
