#!/usr/bin/env python3
"""Publish a deterministic receipt when the X pointer state changes."""

import ctypes
import json
import os
import time


TARGET = "/tmp/wefty-computer/input-oracle.json"


def publish(events: int, state: tuple[int, int, int], checksum: int) -> None:
    temporary = TARGET + ".new"
    with open(temporary, "w", encoding="ascii") as target:
        json.dump(
            {"version": 1, "events": events, "x": state[0], "y": state[1], "buttons": state[2], "hash": f"{checksum:08x}"},
            target,
            separators=(",", ":"),
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
        root_return = ctypes.c_ulong()
        child_return = ctypes.c_ulong()
        root_x = ctypes.c_int()
        root_y = ctypes.c_int()
        window_x = ctypes.c_int()
        window_y = ctypes.c_int()
        mask = ctypes.c_uint()
        if not x11.XQueryPointer(
            display, root, ctypes.byref(root_return), ctypes.byref(child_return),
            ctypes.byref(root_x), ctypes.byref(root_y), ctypes.byref(window_x), ctypes.byref(window_y), ctypes.byref(mask),
        ):
            raise RuntimeError("XQueryPointer failed")
        return root_x.value, root_y.value, mask.value

    previous = query()
    events = 0
    checksum = 2166136261
    publish(events, previous, checksum)
    while True:
        current = query()
        if current != previous:
            events += 1
            for value in current:
                for byte in value.to_bytes(4, "little"):
                    checksum = ((checksum ^ byte) * 16777619) & 0xFFFFFFFF
            publish(events, current, checksum)
            previous = current
        time.sleep(0.05)


if __name__ == "__main__":
    main()
