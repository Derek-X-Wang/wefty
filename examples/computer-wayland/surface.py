#!/usr/bin/env python3
"""Serve the focused Chromium Wayland surface and its guest input receipt."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading

ROOT = "/tmp/wefty-computer"
HOME = os.environ.get("HOME", "/home/wefty")
ORACLE = f"{ROOT}/input-oracle.json"
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


def wefty_record_input(value):
    """Record only events delivered through Sway to the focused Wayland client."""
    kind = value.get("kind") if isinstance(value, dict) else None
    with LOCK:
        if kind == "key" and value.get("version") == 1:
            INPUT["key_events"] += 1
        elif kind == "pointer" and value.get("version") == 1 and type(value.get("x")) is int and type(value.get("y")) is int:
            x, y = value["x"], value["y"]
            if not (0 <= x < 1280 and 0 <= y < 720):
                return False
            INPUT["x"], INPUT["y"] = x, y
            INPUT["pointer_history"].append([x, y])
            INPUT["pointer_history"] = INPUT["pointer_history"][-64:]
        else:
            return False
        INPUT["generation"] += 1
        atomic_json(ORACLE, INPUT)
    return True


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

    def do_POST(self):
        if self.path not in ("/surface-ready", "/input"):
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.send_error(400)
            return
        if length < 2 or length > 256:
            self.send_error(400)
            return
        try:
            value = json.loads(self.rfile.read(length).decode("ascii"))
        except (UnicodeError, json.JSONDecodeError):
            self.send_error(400)
            return
        if self.path == "/surface-ready":
            if value != {"version": 1}:
                self.send_error(400)
                return
            with open(f"{ROOT}/surface-ready", "w", encoding="ascii") as marker:
                marker.write("ready\n")
        else:
            if not wefty_record_input(value):
                self.send_error(400)
                return
        self.send_response(204)
        self.end_headers()

def main():
    os.makedirs(ROOT, exist_ok=True)
    atomic_json(ORACLE, INPUT)
    ThreadingHTTPServer(("127.0.0.1", 18888), Handler).serve_forever()


if __name__ == "__main__":
    main()
