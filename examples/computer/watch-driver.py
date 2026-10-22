#!/usr/bin/env python3
"""Reopen the helper-owned driver signal and publish fail-closed local state."""

import hashlib
import json
import os
import time

SOURCE = os.environ.get("WEFTY_DRIVER_SOURCE", "/wefty/control/driver.json")
TARGET = os.environ.get("WEFTY_DRIVER_TARGET", "/tmp/wefty-computer/driver-state.json")
def read_document() -> tuple[bool, str, str]:
    try:
        with open(SOURCE, "rb") as source:
            payload = source.read()
        fingerprint = hashlib.sha256(payload).hexdigest()
        value = json.loads(payload)
        if not isinstance(value, dict) or set(value) != {"version", "human_driving"}:
            return False, fingerprint, "malformed"
        if type(value["version"]) is not int:
            return False, fingerprint, "malformed"
        if value["version"] != 1:
            return False, fingerprint, "unknown-version"
        if type(value["human_driving"]) is not bool:
            return False, fingerprint, "malformed"
        return value["human_driving"], fingerprint, "valid"
    except FileNotFoundError:
        return False, "missing", "missing"
    except (UnicodeError, json.JSONDecodeError):
        return False, hashlib.sha256(payload).hexdigest(), "malformed"
    except OSError:
        return False, "malformed", "malformed"


def publish(state: bool, generation: int, fingerprint: str, classification: str) -> None:
    temporary = TARGET + ".new"
    with open(temporary, "w", encoding="ascii") as target:
        json.dump({"version": 1, "human_driving": state, "generation": generation,
                   "fingerprint": fingerprint,
                   "classification": classification}, target, separators=(",", ":"))
        target.write("\n")
    os.replace(temporary, TARGET)


def main() -> None:
    previous = None
    generation = 0
    while True:
        state, fingerprint, classification = read_document()
        if fingerprint != previous:
            generation += 1
            publish(state, generation, fingerprint, classification)
            previous = fingerprint
        time.sleep(0.05)


if __name__ == "__main__":
    main()
