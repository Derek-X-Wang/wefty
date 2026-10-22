#!/usr/bin/env python3
"""Regression tests for exact-byte driver observation."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
WATCHERS = (
    Path(__file__).with_name("watch-driver.py"),
    Path(__file__).parent / "fixtures" / "watch-driver.py",
    ROOT / "computer-wayland" / "watch-driver.py",
    ROOT / "computer-wayland" / "watch-driver-fixture.py",
)


def load_watcher(path: Path):
    spec = importlib.util.spec_from_file_location("watch_driver_" + path.parent.name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class DriverWatcherTest(unittest.TestCase):
    def test_transient_parse_error_fingerprints_the_bytes_already_read(self):
        original = b'{"version":2,"human_driving":true}'
        replacement = b'{"version":1,"human_driving":false}'
        for path in WATCHERS:
            with self.subTest(path=path):
                watcher = load_watcher(path)
                opened = 0

                def open_once(*_args, **_kwargs):
                    nonlocal opened
                    opened += 1
                    return io.BytesIO(original if opened == 1 else replacement)

                parse_error = json.JSONDecodeError("transient read", original.decode(), 0)
                with mock.patch.object(watcher, "open", open_once, create=True), \
                     mock.patch.object(watcher.json, "loads", side_effect=parse_error):
                    state, fingerprint, classification = watcher.read_document()

                self.assertFalse(state)
                self.assertEqual(fingerprint, hashlib.sha256(original).hexdigest())
                self.assertEqual(classification, "malformed")
                self.assertEqual(opened, 1, "watcher re-read a different driver generation")

class SeatDriverWatcherTest(unittest.TestCase):
    def test_seat_paths_select_only_its_driver_and_runtime_folder(self):
        with mock.patch.dict("os.environ", {
            "WEFTY_DRIVER_SOURCE": "/wefty/control/seats/2/driver.json",
            "WEFTY_DRIVER_TARGET": "/wefty/service/seats/2/run/driver-state.json",
        }):
            watcher = load_watcher(WATCHERS[0])
        self.assertEqual(watcher.SOURCE, "/wefty/control/seats/2/driver.json")
        self.assertEqual(watcher.TARGET, "/wefty/service/seats/2/run/driver-state.json")


if __name__ == "__main__":
    unittest.main()
