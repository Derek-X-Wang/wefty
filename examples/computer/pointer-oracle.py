#!/usr/bin/env python3
"""Publish observed X pointer coordinates and raw key events."""

import ctypes
import json
import os
import queue
import subprocess
import threading
import time

TARGET = "/tmp/wefty-computer/input-oracle.json"


def publish(generation: int, keys: int, state: tuple[int, int, int], history: list[list[int]]) -> None:
    temporary = TARGET + ".new"
    with open(temporary, "w", encoding="ascii") as target:
        json.dump(
            {"version": 1, "generation": generation, "key_events": keys,
             "x": state[0], "y": state[1], "buttons": state[2], "pointer_history": history[-64:]},
            target, separators=(",", ":"),
        )
        target.write("\n")
    os.replace(temporary, TARGET)


def main() -> None:
    x11 = ctypes.CDLL("libX11.so.6")
    x11.XOpenDisplay.argtypes = [ctypes.c_char_p]
    x11.XOpenDisplay.restype = ctypes.c_void_p
    x11.XDefaultRootWindow.argtypes = [ctypes.c_void_p]
    x11.XDefaultRootWindow.restype = ctypes.c_ulong
    x11.XQueryPointer.argtypes = [
        ctypes.c_void_p, ctypes.c_ulong, ctypes.POINTER(ctypes.c_ulong), ctypes.POINTER(ctypes.c_ulong),
        ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_uint),
    ]
    x11.XQueryPointer.restype = ctypes.c_int
    display = x11.XOpenDisplay(os.environ.get("DISPLAY", ":99").encode("ascii"))
    if not display:
        raise SystemExit("cannot open X display")
    root = x11.XDefaultRootWindow(display)

    def query() -> tuple[int, int, int]:
        root_return, child_return = ctypes.c_ulong(), ctypes.c_ulong()
        root_x, root_y, window_x, window_y = ctypes.c_int(), ctypes.c_int(), ctypes.c_int(), ctypes.c_int()
        mask = ctypes.c_uint()
        if not x11.XQueryPointer(display, root, ctypes.byref(root_return), ctypes.byref(child_return),
                                 ctypes.byref(root_x), ctypes.byref(root_y), ctypes.byref(window_x),
                                 ctypes.byref(window_y), ctypes.byref(mask)):
            raise RuntimeError("XQueryPointer failed")
        return root_x.value, root_y.value, mask.value

    monitor = subprocess.Popen(
        ["xinput", "test-xi2", "--root"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        text=True, bufsize=1,
    )
    if monitor.stdout is None:
        raise SystemExit("xinput monitor has no stdout")
    lines: queue.SimpleQueue[str] = queue.SimpleQueue()

    def read_events() -> None:
        for line in monitor.stdout:
            lines.put(line)

    threading.Thread(target=read_events, daemon=True).start()
    previous = query()
    history = [[previous[0], previous[1]]]
    generation = 0
    key_events = 0
    raw_key_press = False
    publish(generation, key_events, previous, history)
    while True:
        changed = False
        while not lines.empty():
            line = lines.get_nowait()
            if "RawKeyPress" in line:
                raw_key_press = True
            elif raw_key_press and "detail:" in line:
                key_events += 1
                generation += 1
                raw_key_press = False
                changed = True
        current = query()
        if current != previous:
            generation += 1
            history.append([current[0], current[1]])
            previous = current
            changed = True
        if changed:
            publish(generation, key_events, current, history)
        time.sleep(0.01)


if __name__ == "__main__":
    main()
