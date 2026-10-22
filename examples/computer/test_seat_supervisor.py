"""Directive and answer tests; clocks and desktop processes are injected."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("supervisor", Path(__file__).with_name("seat-supervisor.py"))
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class Backend:
    def __init__(self):
        self.live = set()
        self.ready = set()
        self.stopped = []

    def start(self, seat):
        if seat["id"] == 4:
            raise supervisor.SeatFailure("passwd_entry_missing")
        self.live.add(seat["id"])

    def stop(self, place):
        self.stopped.append(place)
        self.live.discard(place)

    def healthy(self, place):
        return place in self.live

    def is_ready(self, seat):
        return seat["id"] in self.ready


def directive(generation, *places):
    return {"version": 1, "generation": generation, "seats": [
        {"id": n, "user": "seat%d" % n, "view_port": 7100 + n * 2,
         "control_port": 7101 + n * 2} for n in places]}


class SupervisorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = 0
        self.backend = Backend()
        self.engine = supervisor.Supervisor(Path(self.tmp.name), self.backend,
                                             clock=lambda: self.clock)

    def answer(self, place):
        return json.loads((Path(self.tmp.name) / (str(place) + ".json")).read_text())

    def test_new_seat_has_its_own_deadline_and_late_ready_is_cleaned(self):
        self.engine.apply(directive(1, 1))
        self.backend.ready.add(1)
        self.engine.tick()
        self.assertEqual(self.answer(1)["state"], "ready")
        self.clock = 200
        self.engine.apply(directive(2, 1, 2))
        self.clock = 259.9
        self.engine.tick()
        self.assertNotIn(2, self.backend.stopped)
        self.clock = 260
        self.backend.ready.add(2)
        self.engine.tick()
        self.assertEqual(self.answer(2)["reason"], "readiness_deadline")
        self.assertEqual(self.answer(2)["state"], "failed")
        self.assertIn(2, self.backend.stopped)
        self.assertIn(1, self.backend.live)

    def test_readiness_probe_that_crosses_deadline_is_not_published(self):
        self.engine.apply(directive(1, 1))
        self.clock = 59.9
        def slow_ready(_seat):
            self.clock = 60.1
            return True
        self.backend.is_ready = slow_ready
        self.engine.tick()
        self.assertEqual(self.answer(1)["state"], "failed")
        self.assertEqual(self.answer(1)["reason"], "readiness_deadline")

    def test_missing_passwd_is_failed_without_retry_or_ready(self):
        self.engine.apply(directive(1, 4))
        self.assertEqual(self.answer(4)["reason"], "passwd_entry_missing")
        self.engine.tick()
        self.assertNotIn(4, self.backend.live)

    def test_removal_clears_answer_and_stops_uid(self):
        self.engine.apply(directive(1, 1, 2))
        self.backend.ready.update((1, 2))
        self.engine.tick()
        self.engine.apply(directive(2, 1))
        self.assertEqual(self.backend.stopped, [2])
        self.assertFalse((Path(self.tmp.name) / "2.json").exists())
        self.assertIn(1, self.backend.live)

    def test_dead_desktop_is_cleaned_and_failed(self):
        self.engine.apply(directive(1, 1))
        self.backend.live.clear()
        self.engine.tick()
        self.assertEqual(self.answer(1)["reason"], "desktop_exited")
        self.assertEqual(self.backend.stopped, [1])

    def test_malformed_directive_fails_closed(self):
        self.engine.apply(directive(1, 1))
        bad = directive(2, 1, 2)
        bad["seats"][1]["view_port"] = bad["seats"][0]["view_port"]
        with self.assertRaises(supervisor.SeatFailure):
            self.engine.apply(bad)
        self.assertFalse(self.backend.live)

    def test_replayed_generation_does_not_resurrect_removed_seat(self):
        self.engine.apply(directive(2, 1))
        self.engine.apply(directive(3))
        with self.assertRaises(supervisor.SeatFailure):
            self.engine.apply(directive(2, 1))
        self.assertFalse(self.backend.live)


if __name__ == "__main__":
    unittest.main()
