#!/usr/bin/env python3
"""Reopen the helper-owned driver signal and publish fail-closed local state."""

import json
import os
import time


SOURCE = "/wefty/control/driver.json"
TARGET = "/tmp/wefty-computer/driver-state.json"


def read_state() -> bool:
    try:
        with open(SOURCE, "r", encoding="utf-8") as source:
            value = json.load(source)
        return value == {"version": 1, "human_driving": True}
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return False


def publish(state: bool) -> None:
    temporary = TARGET + ".new"
    with open(temporary, "w", encoding="ascii") as target:
        json.dump({"version": 1, "human_driving": state}, target, separators=(",", ":"))
        target.write("\n")
    os.replace(temporary, TARGET)


def main() -> None:
    previous = None
    while True:
        current = read_state()
        if current != previous:
            publish(current)
            previous = current
        time.sleep(0.25)


if __name__ == "__main__":
    main()
