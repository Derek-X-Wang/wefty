#!/usr/bin/env python3
"""Serve the furniture surface and record focused native Wayland input."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import subprocess
import threading
import time

ROOT = "/tmp/wefty-computer"
HOME = os.environ.get("HOME", "/home/wefty")
ORACLE = f"{ROOT}/input-oracle.json"
BROWSER_READY = f"{ROOT}/browser-ready"
SURFACE_READY = f"{ROOT}/surface-ready"
STATE = f"{HOME}/.local/state/wefty/agent-state.json"
THEME = f"{HOME}/.config/wefty/theme.json"
HTML = "/opt/wefty-computer-wayland/oracle.html"
LOCK = threading.Lock()
INPUT = {"version": 1, "generation": 0, "key_events": 0, "x": 0, "y": 0, "pointer_history": [[0, 0]], "observer_lines": 0}
OBSERVED_STATES = []
POINTER_EVENT = re.compile(r"wl_pointer\] motion: .*x, y: (-?[0-9]+(?:\.[0-9]+)?), (-?[0-9]+(?:\.[0-9]+)?)")


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


def wait_for_browser():
    while not os.path.exists(BROWSER_READY):
        time.sleep(0.05)


def observe_wayland_input():
    """Translate events from the focused native Wayland client into the oracle."""
    wait_for_browser()
    while True:
        process = subprocess.Popen(
            ["stdbuf", "-o0", "wev"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            with LOCK:
                INPUT["observer_lines"] += 1
                atomic_json(ORACLE, INPUT)
            pointer = POINTER_EVENT.search(line)
            if pointer:
                wefty_record_input({"version": 1, "kind": "pointer", "x": round(float(pointer.group(1))), "y": round(float(pointer.group(2)))})
            elif "wl_keyboard" in line and "] key:" in line:
                wefty_record_input({"version": 1, "kind": "key"})
        process.wait()
        time.sleep(0.1)


def tree_has_focused_oracle():
    try:
        result = subprocess.run(
            ["swaymsg", "--type", "get_tree", "--raw"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
        root = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return False
    pending = [root]
    while pending:
        node = pending.pop()
        if not isinstance(node, dict):
            continue
        rect = node.get("rect", {})
        if node.get("app_id") == "wev" and node.get("focused") is True and rect.get("width") == 1280 and rect.get("height") == 720:
            return True
        pending.extend(node.get("nodes", []))
        pending.extend(node.get("floating_nodes", []))
    return False


def publish_surface_readiness():
    wait_for_browser()
    while not tree_has_focused_oracle():
        time.sleep(0.05)
    with open(SURFACE_READY, "w", encoding="ascii") as marker:
        marker.write("ready\n")


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
        if self.path != "/surface-ready":
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
        if value != {"version": 1}:
            self.send_error(400)
            return
        with open(BROWSER_READY, "w", encoding="ascii") as marker:
            marker.write("ready\n")
        self.send_response(204)
        self.end_headers()

def main():
    os.makedirs(ROOT, exist_ok=True)
    atomic_json(ORACLE, INPUT)
    threading.Thread(target=observe_wayland_input, daemon=True).start()
    threading.Thread(target=publish_surface_readiness, daemon=True).start()
    ThreadingHTTPServer(("127.0.0.1", 18888), Handler).serve_forever()


if __name__ == "__main__":
    main()
