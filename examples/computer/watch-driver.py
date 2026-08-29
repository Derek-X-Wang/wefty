#!/usr/bin/env python3
"""Reopen the helper-owned driver signal and publish fail-closed local state."""

import hashlib
import json
import os
import time

SOURCE = "/wefty/control/driver.json"
TARGET = "/tmp/wefty-computer/driver-state.json"
MUTATION = os.environ.get("WEFTY_CONFORMANCE_MUTATION", "")


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
            accepted = MUTATION == "unknown-driver-version-accepted" and value.get("human_driving") is True
            return accepted, fingerprint, "unknown-version"
        if type(value["human_driving"]) is not bool:
            return MUTATION == "malformed-driver-accepted", fingerprint, "malformed"
        state = value["human_driving"]
        if MUTATION == "driver-json-ignored":
            state = False
        return state, fingerprint, "valid"
    except FileNotFoundError:
        return False, "missing", "missing"
    except (OSError, UnicodeError, json.JSONDecodeError):
        fingerprint = "malformed"
        try:
            with open(SOURCE, "rb") as source:
                fingerprint = hashlib.sha256(source.read()).hexdigest()
        except OSError:
            pass
        return MUTATION == "malformed-driver-accepted", fingerprint, "malformed"


def publish(state: bool, generation: int, classification: str) -> None:
    temporary = TARGET + ".new"
    with open(temporary, "w", encoding="ascii") as target:
        json.dump({"version": 1, "human_driving": state, "generation": generation,
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
            publish(state, generation, classification)
            previous = fingerprint
        time.sleep(0.05)


if __name__ == "__main__":
    main()
