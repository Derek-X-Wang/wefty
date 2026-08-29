#!/usr/bin/env python3
"""Serve the focused Wayland surface and assertion-derived local receipts."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import time

ROOT = "/tmp/wefty-computer"
HOME = os.environ.get("HOME", "/home/wefty")
ORACLE = f"{ROOT}/input-oracle.json"
EVENTS = f"{ROOT}/native-input-events"
STATE = f"{HOME}/.local/state/wefty/agent-state.json"
THEME = f"{HOME}/.config/wefty/theme.json"
HTML = "/opt/wefty-computer-wayland/oracle.html"
LOCK = threading.Lock()
INPUT = {"version": 1, "generation": 0, "key_events": 0, "x": 0, "y": 0, "pointer_history": [[0, 0]]}
OBSERVED_STATES = []


def atomic_json(path, value):
    temporary = path + ".new"
    with open(temporary, "w", encoding="ascii") as target:
        json.dump(value, target, separators=(",", ":"))
        target.write("\n")
    os.replace(temporary, path)


def read_json(path, fallback):
    try:
        with open(path, "r", encoding="ascii") as source:
            value = json.load(source)
        return value if isinstance(value, dict) else fallback
    except (OSError, UnicodeError, json.JSONDecodeError):
        return fallback


def consume_native_input():
    offset = 0
    while True:
        try:
            with open(EVENTS, "r", encoding="ascii") as source:
                source.seek(offset)
                events = source.readlines()
                offset = source.tell()
        except (OSError, UnicodeError):
            events = []
        if events:
            with LOCK:
                for event in events:
                    fields = event.split()
                    if len(fields) == 2 and fields[0] in ("control", "view") and fields[1] == "key":
                        INPUT["key_events"] += 1
                        INPUT["generation"] += 1
                    elif len(fields) == 4 and fields[0] in ("control", "view") and fields[1] == "pointer":
                        try:
                            x, y = int(fields[2]), int(fields[3])
                        except ValueError:
                            continue
                        if fields[0] == "control":
                            INPUT["x"], INPUT["y"] = x, y
                        INPUT["pointer_history"].append([x, y])
                        INPUT["pointer_history"] = INPUT["pointer_history"][-64:]
                        INPUT["generation"] += 1
                atomic_json(ORACLE, INPUT)
        time.sleep(0.01)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply_json(self, value):
        payload = json.dumps(value, separators=(",", ":")).encode("ascii")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/":
            with open(HTML, "rb") as source:
                payload = source.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path == "/agent-state":
            value = read_json(STATE, {"version": 1, "generation": 0, "state": "idle"})
            state = value.get("state", "idle")
            if state not in ("idle", "working", "blocked", "done"):
                state = "idle"
            with LOCK:
                if not OBSERVED_STATES or OBSERVED_STATES[-1] != state:
                    OBSERVED_STATES.append(state)
                atomic_json(f"{ROOT}/agent-state-surface.json", {"version": 1, "state": state, "observed": OBSERVED_STATES})
            self.reply_json({"version": 1, "state": state})
            return
        if self.path == "/theme":
            value = read_json(THEME, {"version": 1, "theme": "graphite"})
            theme = value.get("theme", "graphite")
            if theme not in ("graphite", "amber"):
                theme = "graphite"
            atomic_json(f"{ROOT}/theme-surface.json", {"version": 1, "theme": theme})
            self.reply_json({"version": 1, "theme": theme})
            return
        self.send_error(404)

def main():
    os.makedirs(ROOT, exist_ok=True)
    with open(EVENTS, "w", encoding="ascii"):
        pass
    atomic_json(ORACLE, INPUT)
    threading.Thread(target=consume_native_input, daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", 18888), Handler).serve_forever()


if __name__ == "__main__":
    main()
