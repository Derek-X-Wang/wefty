"""Directive and answer tests; clocks and desktop processes are injected."""
from contextlib import contextmanager
import importlib.util
import errno
import json
import os
import resource
import stat
from unittest import mock
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
        if seat["place"] == 4:
            raise supervisor.SeatFailure("passwd_entry_missing")
        self.live.add(seat["place"])

    def stop(self, place):
        self.stopped.append(place)
        self.live.discard(place)

    def healthy(self, place):
        return place in self.live

    def failure_reason(self, _place):
        return "desktop_exited"

    def is_ready(self, seat):
        return seat["place"] in self.ready


def directive(generation, *places):
    return {"version": 1, "generation": generation, "seats": [
        {"place": n, "user": "seat%d" % n, "view_port": 7100 + n * 2,
         "control_port": 7101 + n * 2,
         "view_socket": "/wefty/service/seats/%d/run/view.ws" % n,
         "control_socket": "/wefty/service/seats/%d/run/control.ws" % n} for n in places]}


class SupervisorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = 0
        self.backend = Backend()
        self.engine = supervisor.Supervisor(Path(self.tmp.name).resolve(), self.backend,
                                             clock=lambda: self.clock)

    def answer(self, place):
        return json.loads((Path(self.tmp.name).resolve() / (str(place) + ".json")).read_text())

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
        self.assertEqual(self.answer(2)["state"], "removed")
        self.assertEqual(self.answer(2)["generation"], 2)
        self.assertTrue(self.answer(2)["cleanup_complete"])
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

    def test_empty_directive_has_positive_generation_ack_even_if_add_was_not_seen(self):
        self.engine.apply(directive(2))
        answer = json.loads((Path(self.tmp.name).resolve() / 'applied.json').read_text())
        self.assertEqual(answer['generation'], 2)
        self.assertEqual(answer['wanted_places'], [])
        self.assertTrue(answer['cleanup_complete'])

    def test_starting_seat_keeps_its_deadline_when_another_is_added(self):
        self.engine.apply(directive(1, 1))
        self.clock = 50
        self.engine.apply(directive(2, 1, 2))
        self.clock = 60
        self.engine.tick()
        self.assertEqual(self.answer(1)["reason"], "readiness_deadline")
        self.assertIn(2, self.backend.live)

    def test_refusal_publishes_positive_cleanup_proof(self):
        self.engine.apply(directive(1, 1))
        with self.assertRaises(supervisor.SeatFailure):
            self.engine.apply({"version": 9, "generation": 2, "seats": []})
        self.assertEqual(self.answer(1)["state"], "failed")
        self.assertEqual(self.answer(1)["reason"], "directive_refused")
        self.assertTrue(self.answer(1)["cleanup_complete"])
        self.assertEqual(self.answer(1)["generation"], 2)

    def test_cleanup_failure_blocks_only_that_place_and_keeps_other_seat_live(self):
        self.engine.apply(directive(1, 1, 2))
        original = self.backend.stop
        def fail(place):
            if place == 1:
                raise supervisor.SeatFailure("cannot_stop")
            original(place)
        self.backend.stop = fail
        self.engine.apply(directive(2, 2))
        self.engine.tick()
        self.assertEqual(self.answer(1)["reason"], "cleanup_failed")
        self.assertFalse(self.answer(1)["cleanup_complete"])
        self.assertIn(2, self.backend.live)
        self.engine.apply(directive(3, 1, 2))
        self.assertIn(1, self.engine.blocked)
        self.backend.stop = original
        self.clock = 30
        self.engine.tick()
        self.assertTrue(self.answer(1)["cleanup_complete"])
        self.assertEqual(self.answer(1)["state"], "failed")
        applied = json.loads((Path(self.tmp.name).resolve() / 'applied.json').read_text())
        self.assertEqual(applied['generation'], 3)

    def test_in_place_change_is_refused_without_removed_answer(self):
        self.engine.apply(directive(1, 1))
        changed = directive(2, 1)
        changed['seats'][0]['view_port'] = 7200
        with self.assertRaisesRegex(supervisor.SeatFailure, 'in_place_change_requires_remove'):
            self.engine.apply(changed)
        self.assertEqual(self.answer(1)['state'], 'failed')
        self.assertEqual(self.answer(1)['reason'], 'directive_refused')
        self.assertTrue(self.answer(1)['cleanup_complete'])
        changed['generation'] = 3
        with self.assertRaisesRegex(supervisor.SeatFailure, 'in_place_change_requires_remove'):
            self.engine.apply(changed)
        self.engine.apply(directive(4))
        changed['generation'] = 5
        self.engine.apply(changed)
        self.assertIn(1, self.backend.live)

    def test_unexpected_cleanup_exception_is_blocked_and_backed_off(self):
        self.engine.apply(directive(1, 1, 2))
        calls = []
        original = self.backend.stop
        def broken(place):
            if place == 1:
                calls.append(place)
                raise RuntimeError('unexpected cleanup error')
            original(place)
        self.backend.stop = broken
        self.engine.apply(directive(2, 2))
        for self.clock in range(1, 30):
            self.engine.tick()
        self.assertEqual(calls, [1])
        self.clock = 30
        self.engine.tick()
        self.assertEqual(calls, [1, 1])
        self.assertEqual(self.answer(1)['reason'], 'cleanup_failed')
        self.assertIn(2, self.backend.live)

    def test_launcher_reason_is_reported(self):
        self.engine.apply(directive(1, 1))
        self.backend.failure_reason = lambda _place: 'restart_limit'
        self.backend.live.clear()
        self.engine.tick()
        self.assertEqual(self.answer(1)['reason'], 'restart_limit')

    def test_startup_cleanup_failure_does_not_prevent_other_places(self):
        original = self.backend.stop
        def broken(place):
            if place == 1:
                raise RuntimeError('unexpected startup cleanup error')
            original(place)
        self.backend.stop = broken
        self.engine.initialize()
        self.assertEqual(self.answer(1)['reason'], 'cleanup_failed')
        self.engine.apply(directive(1, 2))
        self.assertIn(2, self.backend.live)
        self.assertIn(1, self.engine.blocked)

    def test_main_loop_logs_one_refusal_and_retries_after_a_read_error(self):
        control = Path(self.tmp.name).resolve() / "seats.json"
        control.write_text('{"version":9,"generation":2,"seats":[]}')
        self.engine.apply(directive(1, 1))
        turns, logs = [], []
        def sleep(_delay):
            turns.append(1)
            if len(turns) == 2:
                control.unlink()
            elif len(turns) == 3:
                control.write_text(json.dumps(directive(3, 2)))
        supervisor.run_loop(self.engine, control, lambda: len(turns) == 5,
                            sleep=sleep, log=logs.append)
        self.assertEqual(sum('refused' in line for line in logs), 1)
        self.assertIn(2, self.backend.live)


class ScratchCleanupTest(unittest.TestCase):
    def test_nested_scratch_removes_only_ending_uid_without_following_links(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            scratch = root / 'scratch'
            scratch.mkdir()
            foreign = scratch / 'other-seat'
            foreign.mkdir()
            nested = foreign / 'nested'
            nested.mkdir()
            ending_file = nested / 'big'
            ending_file.write_text('charged memory')
            foreign_file = nested / 'keep'
            foreign_file.write_text('other Seat')
            empty = foreign / 'ending-empty'
            empty.mkdir()
            outside = root / 'outside'
            outside.mkdir()
            target = outside / 'keep'
            target.write_text('link target')
            link = foreign / 'ending-link'
            link.symlink_to(outside, target_is_directory=True)
            foreign_link = scratch / 'foreign-link'
            foreign_link.symlink_to(outside, target_is_directory=True)
            dangling = foreign / 'ending-dangling'
            dangling.symlink_to(root / 'missing')
            owned = {p.lstat().st_ino for p in (nested, ending_file, empty, link, dangling, target)}
            uid = os.getuid() + 10000
            real_stat = os.stat
            real_open_directory = supervisor.open_directory

            def seat_stat(*args, **kwargs):
                info = real_stat(*args, **kwargs)
                values = list(info)
                values[4] = uid if info.st_ino in owned else uid + 1
                return os.stat_result(values)

            # Only ownership is simulated; traversal, unlink and rmdir use real files.
            def open_scratch(folder):
                if folder == '/dev/shm':
                    return real_open_directory(scratch)
                raise FileNotFoundError(folder)

            with mock.patch.object(supervisor, 'open_directory', side_effect=open_scratch), \
                    mock.patch.object(supervisor.os, 'stat', side_effect=seat_stat), \
                    mock.patch.object(supervisor.os, 'fchown'):
                supervisor.sweep_scratch(uid)
            self.assertFalse(ending_file.exists())
            self.assertFalse(empty.exists())
            self.assertFalse(link.is_symlink())
            self.assertFalse(dangling.is_symlink())
            self.assertEqual(foreign_file.read_text(), 'other Seat')
            self.assertTrue(foreign.is_dir())
            self.assertTrue(nested.is_dir())  # Still contains another uid's file.
            self.assertTrue(foreign_link.is_symlink())
            self.assertEqual(target.read_text(), 'link target')

    def test_scratch_does_not_cross_a_nested_mount(self):
        with tempfile.TemporaryDirectory() as d:
            scratch = Path(d).resolve()
            mount = scratch / 'mount'
            mount.mkdir()
            (mount / 'keep').write_text('different filesystem')
            real_stat = os.stat
            mount_inode = mount.stat().st_ino

            def mounted_stat(*args, **kwargs):
                info = real_stat(*args, **kwargs)
                if info.st_ino == mount_inode:
                    values = list(info)
                    values[2] = info.st_dev + 1
                    return os.stat_result(values)
                return info

            descriptor = supervisor.open_directory(scratch)
            with mock.patch.object(supervisor, 'open_directory', side_effect=lambda _: os.dup(descriptor)), \
                    mock.patch.object(supervisor.os, 'stat', side_effect=mounted_stat):
                try:
                    supervisor.sweep_scratch(os.getuid())
                finally:
                    os.close(descriptor)
            self.assertEqual((mount / 'keep').read_text(), 'different filesystem')

    def test_nested_cleanup_failure_withholds_positive_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            scratch = root / 'scratch'
            scratch.mkdir()
            foreign = scratch / 'other-seat'
            foreign.mkdir()
            target = foreign / 'blocked'
            target.write_text('charged memory')
            inode = target.stat().st_ino
            uid = 2001
            real_stat = os.stat
            real_unlink = os.unlink

            def seat_stat(*args, **kwargs):
                info = real_stat(*args, **kwargs)
                values = list(info)
                values[4] = uid if info.st_ino == inode else uid + 1
                return os.stat_result(values)

            def denied_unlink(name, **kwargs):
                if name == 'blocked':
                    raise PermissionError('scratch cleanup denied')
                return real_unlink(name, **kwargs)

            descriptor = supervisor.open_directory(scratch)
            backend = supervisor.DesktopBackend(mock.Mock())
            engine = supervisor.Supervisor(root, backend, clock=lambda: 0)
            real_open_directory = supervisor.open_directory

            def open_scratch(folder):
                if folder in ('/tmp', '/var/tmp', '/dev/shm', '/run', '/tmp/.X11-unix'):
                    return os.dup(descriptor)
                return real_open_directory(folder)

            with mock.patch.object(supervisor, 'open_directory', side_effect=open_scratch), \
                    mock.patch.object(supervisor.os, 'stat', side_effect=seat_stat), \
                    mock.patch.object(supervisor.os, 'unlink', side_effect=denied_unlink), \
                    mock.patch.object(supervisor, 'kill_uid'), \
                    mock.patch.object(supervisor.subprocess, 'run'):
                try:
                    stopped = engine.stop(1)
                finally:
                    os.close(descriptor)
            answer = json.loads((root / '1.json').read_text())
            self.assertFalse(stopped)
            self.assertEqual(answer['reason'], 'cleanup_failed')
            self.assertFalse(answer['cleanup_complete'])
            self.assertTrue(target.exists())
            backend.storage.remove_seat.assert_not_called()

    def test_nested_scratch_disappearing_during_unlink_is_quiet(self):
        with tempfile.TemporaryDirectory() as d:
            scratch = Path(d).resolve()
            foreign = scratch / 'other-seat'
            foreign.mkdir()
            target = foreign / 'gone'
            target.write_text('charged memory')
            inode = target.stat().st_ino
            uid = os.getuid() + 10000
            real_stat = os.stat
            real_unlink = os.unlink

            def seat_stat(*args, **kwargs):
                info = real_stat(*args, **kwargs)
                values = list(info)
                values[4] = uid if info.st_ino == inode else uid + 1
                return os.stat_result(values)

            def racing_unlink(name, **kwargs):
                real_unlink(name, **kwargs)
                if name == 'gone':
                    raise FileNotFoundError(name)

            descriptor = supervisor.open_directory(scratch)
            with mock.patch.object(supervisor, 'open_directory', side_effect=lambda _: os.dup(descriptor)), \
                    mock.patch.object(supervisor.os, 'stat', side_effect=seat_stat), \
                    mock.patch.object(supervisor.os, 'unlink', side_effect=racing_unlink):
                try:
                    supervisor.sweep_scratch(uid)
                finally:
                    os.close(descriptor)
            self.assertFalse(target.exists())
            self.assertTrue(foreign.is_dir())


class ScratchWalkLimitsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.scratch = self.root / 'scratch'
        self.scratch.mkdir()
        self.uid = os.getuid() + 10000
        self.owned = set()
        self.clock = 0

    @contextmanager
    def filesystem(self):
        real_stat = os.stat
        real_open_directory = supervisor.open_directory

        def seat_stat(*args, **kwargs):
            info = real_stat(*args, **kwargs)
            values = list(info)
            values[4] = self.uid if info.st_ino in self.owned else self.uid + 1
            return os.stat_result(values)

        def open_scratch(folder):
            if folder == '/dev/shm':
                return real_open_directory(self.scratch)
            if folder in ('/tmp', '/var/tmp', '/run', '/tmp/.X11-unix'):
                raise FileNotFoundError(folder)
            return real_open_directory(folder)

        with mock.patch.object(supervisor, 'open_directory', side_effect=open_scratch), \
                mock.patch.object(supervisor.os, 'stat', side_effect=seat_stat):
            yield

    def test_entry_budget_bounds_foreign_tree_enumeration(self):
        foreign = self.scratch / 'foreign'
        foreign.mkdir()
        for n in range(10):
            (foreign / str(n)).write_text('other Seat')
        with self.filesystem(), \
                mock.patch.object(supervisor, 'SCRATCH_SWEEP_ENTRIES', 3, create=True):
            with self.assertRaisesRegex(supervisor.SeatFailure, '^cleanup_scratch_budget$'):
                supervisor.sweep_scratch(self.uid)
        self.assertEqual(len(list(foreign.iterdir())), 10)

    def test_deadline_bounds_walk_with_injected_clock(self):
        foreign = self.scratch / 'foreign'
        foreign.mkdir()
        real_stat = os.stat

        def advance_clock(*args, **kwargs):
            info = real_stat(*args, **kwargs)
            self.clock = 2
            return info

        with self.filesystem(), \
                mock.patch.object(supervisor.os, 'stat', side_effect=advance_clock), \
                mock.patch.object(supervisor, 'SCRATCH_SWEEP_SECONDS', 1, create=True):
            with self.assertRaisesRegex(supervisor.SeatFailure, '^cleanup_scratch_budget$'):
                supervisor.sweep_scratch(self.uid, clock=lambda: self.clock)
        self.assertTrue(foreign.is_dir())

    def test_foreign_folder_type_or_identity_change_retries_and_finds_moved_files(self):
        for replacement in ('file', 'link', 'directory'):
            with self.subTest(replacement=replacement):
                self.owned.clear()
                foreign = self.scratch / ('foreign-' + str(replacement))
                foreign.mkdir()
                target = foreign / 'ending'
                target.write_text('charged memory')
                self.owned.add(target.stat().st_ino)
                moved = self.scratch / ('moved-' + str(replacement))
                outside = self.root / 'outside'
                outside.mkdir(exist_ok=True)
                (outside / 'keep').write_text('safe')
                real_open = os.open
                changed = []

                def replace_before_open(name, *args, **kwargs):
                    if name == foreign.name and not changed:
                        changed.append(1)
                        foreign.rename(moved)
                        if replacement == 'file':
                            foreign.write_text('foreign file')
                            raise NotADirectoryError(errno.ENOTDIR, 'changed to file')
                        if replacement == 'link':
                            foreign.symlink_to(outside, target_is_directory=True)
                            raise OSError(errno.ELOOP, 'changed to symlink')
                        foreign.mkdir()
                    return real_open(name, *args, **kwargs)

                with self.filesystem(), mock.patch.object(supervisor.os, 'open', side_effect=replace_before_open):
                    supervisor.sweep_scratch(self.uid)
                self.assertFalse((moved / 'ending').exists())
                self.assertEqual((outside / 'keep').read_text(), 'safe')
                if replacement == 'file':
                    self.assertEqual(foreign.read_text(), 'foreign file')
                elif replacement == 'link':
                    self.assertTrue(foreign.is_symlink())
                else:
                    self.assertTrue(foreign.is_dir())
                if replacement == 'directory':
                    foreign.rmdir()
                else:
                    foreign.unlink()
                moved.rmdir()

    def test_foreign_folder_renamed_on_ascent_retries(self):
        for replace in (False, True, 'reparent'):
            with self.subTest(replace=replace):
                self.owned.clear()
                foreign = self.scratch / ('foreign-' + str(replace))
                foreign.mkdir()
                target = foreign / 'ending'
                target.write_text('charged memory')
                self.owned.add(target.stat().st_ino)
                holder = self.scratch / 'holder'
                if replace == 'reparent':
                    holder.mkdir()
                moved = (holder if replace == 'reparent' else self.scratch) / ('moved-' + str(replace))
                real_unlink = os.unlink
                changed = []

                def rename_after_unlink(name, **kwargs):
                    real_unlink(name, **kwargs)
                    if name == 'ending' and not changed:
                        changed.append(1)
                        foreign.rename(moved)
                        late = moved / 'late'
                        late.write_text('also charged')
                        self.owned.add(late.stat().st_ino)
                        if replace:
                            foreign.mkdir()

                with self.filesystem(), mock.patch.object(supervisor.os, 'unlink', side_effect=rename_after_unlink):
                    supervisor.sweep_scratch(self.uid)
                self.assertFalse((moved / 'late').exists())
                self.assertTrue(moved.is_dir())
                moved.rmdir()
                if replace:
                    foreign.rmdir()
                if replace == 'reparent':
                    holder.rmdir()

    def test_foreign_folder_parent_disappearing_on_ascent_retries(self):
        foreign = self.scratch / 'foreign'
        foreign.mkdir()
        moved = self.scratch / 'moved'
        real_open = os.open
        changed = []

        def missing_parent(name, *args, **kwargs):
            if name == '..' and not changed:
                changed.append(1)
                foreign.rename(moved)
                late = moved / 'late'
                late.write_text('charged memory')
                self.owned.add(late.stat().st_ino)
                raise FileNotFoundError('foreign parent changed')
            return real_open(name, *args, **kwargs)

        with self.filesystem(), mock.patch.object(supervisor.os, 'open', side_effect=missing_parent):
            supervisor.sweep_scratch(self.uid)
        self.assertFalse((moved / 'late').exists())
        self.assertTrue(moved.is_dir())

    def test_continual_foreign_changes_stop_after_three_passes(self):
        foreign = self.scratch / 'foreign'
        foreign.mkdir()
        ending = self.scratch / 'ending'
        ending.write_text('charged memory')
        self.owned.add(ending.stat().st_ino)
        real_open = os.open
        attempts = []

        def changing_open(name, *args, **kwargs):
            if name == 'foreign':
                attempts.append(1)
                raise NotADirectoryError(errno.ENOTDIR, 'foreign folder changed')
            return real_open(name, *args, **kwargs)

        with self.filesystem(), mock.patch.object(supervisor.os, 'open', side_effect=changing_open):
            with self.assertRaisesRegex(supervisor.SeatFailure, '^cleanup_path_replaced$'):
                supervisor.sweep_scratch(self.uid)
        self.assertEqual(len(attempts), 3)
        self.assertFalse(ending.exists())  # Other top-level entries still get a pass.

    def test_owned_folder_type_change_does_not_retry(self):
        owned = self.scratch / 'owned'
        owned.mkdir()
        self.owned.add(owned.stat().st_ino)
        real_open = os.open
        attempts = []

        def changing_open(name, *args, **kwargs):
            if name == 'owned':
                attempts.append(1)
                raise NotADirectoryError(errno.ENOTDIR, 'owned folder changed')
            return real_open(name, *args, **kwargs)

        with self.filesystem(), mock.patch.object(supervisor.os, 'open', side_effect=changing_open):
            with self.assertRaisesRegex(supervisor.SeatFailure, '^cleanup_path_replaced$'):
                supervisor.sweep_scratch(self.uid)
        self.assertEqual(len(attempts), 1)

    def test_entry_budget_is_shared_across_retries(self):
        (self.scratch / 'foreign').mkdir()
        real_open = os.open
        attempts = []

        def changing_open(name, *args, **kwargs):
            if name == 'foreign':
                attempts.append(1)
                raise NotADirectoryError(errno.ENOTDIR, 'foreign folder changed')
            return real_open(name, *args, **kwargs)

        with self.filesystem(), mock.patch.object(supervisor.os, 'open', side_effect=changing_open), \
                mock.patch.object(supervisor, 'SCRATCH_SWEEP_ENTRIES', 2, create=True):
            with self.assertRaisesRegex(supervisor.SeatFailure, '^cleanup_scratch_budget$'):
                supervisor.sweep_scratch(self.uid)
        self.assertEqual(len(attempts), 2)

    def test_storage_vanished_entries_still_raise(self):
        for phase in ('unlink', 'open', 'ascent'):
            with self.subTest(phase=phase):
                target = self.scratch / 'tree'
                if phase == 'unlink':
                    target.write_text('temporary')
                    operation = 'unlink'
                else:
                    target.mkdir()
                    operation = 'open' if phase == 'open' else 'rmdir'
                descriptor = supervisor.open_directory(self.scratch)
                real_operation = getattr(os, operation)

                def vanish(name, *args, **kwargs):
                    if name == 'tree':
                        if phase == 'open':
                            os.rmdir(name, dir_fd=kwargs['dir_fd'])
                        else:
                            real_operation(name, *args, **kwargs)
                        raise FileNotFoundError(name)
                    return real_operation(name, *args, **kwargs)

                try:
                    with mock.patch.object(supervisor.os, operation, side_effect=vanish):
                        with self.assertRaises(FileNotFoundError):
                            supervisor.remove_at(descriptor, 'tree')
                finally:
                    os.close(descriptor)

    def test_retained_owned_folder_moves_to_root_and_keeps_mode_without_rescan(self):
        retained = self.scratch / 'retained'
        retained.mkdir()
        retained.chmod(0o7777)
        keep = retained / 'foreign'
        keep.write_text('other Seat')
        mode = stat.S_IMODE(retained.stat().st_mode)
        inode = retained.stat().st_ino
        self.owned.add(inode)
        ownership_changes = []

        def chown_clears_special_bits(descriptor, uid, gid):
            info = os.fstat(descriptor)
            self.assertEqual(info.st_ino, inode)
            ownership_changes.append((uid, gid))
            os.fchmod(descriptor, stat.S_IMODE(info.st_mode) & ~(stat.S_ISUID | stat.S_ISGID))
            self.owned.discard(inode)

        with self.filesystem(), mock.patch.object(supervisor.os, 'fchown', side_effect=chown_clears_special_bits), \
                mock.patch.object(supervisor.os, 'listdir', side_effect=AssertionError('uid walk rescanned a folder')):
            supervisor.sweep_scratch(self.uid)
        self.assertEqual(ownership_changes, [(0, 0)])
        self.assertEqual(stat.S_IMODE(retained.stat().st_mode), mode)
        self.assertEqual(keep.read_text(), 'other Seat')

    def test_deadline_is_shared_across_retries(self):
        (self.scratch / 'foreign').mkdir()
        real_open = os.open
        attempts = []

        def changing_open(name, *args, **kwargs):
            if name == 'foreign':
                attempts.append(1)
                self.clock += 4
                raise NotADirectoryError(errno.ENOTDIR, 'foreign folder changed')
            return real_open(name, *args, **kwargs)

        with self.filesystem(), mock.patch.object(supervisor.os, 'open', side_effect=changing_open), \
                mock.patch.object(supervisor, 'SCRATCH_SWEEP_SECONDS', 5, create=True):
            with self.assertRaisesRegex(supervisor.SeatFailure, '^cleanup_scratch_budget$'):
                supervisor.sweep_scratch(self.uid, clock=lambda: self.clock)
        self.assertEqual(len(attempts), 2)

    def test_scratch_budget_failure_withholds_positive_evidence(self):
        (self.scratch / 'entry').write_text('foreign scratch')
        backend = supervisor.DesktopBackend(mock.Mock())
        engine = supervisor.Supervisor(self.root, backend, clock=lambda: 0)
        with self.filesystem(), mock.patch.object(supervisor, 'SCRATCH_SWEEP_ENTRIES', 0, create=True), \
                mock.patch.object(supervisor, 'kill_uid'), mock.patch.object(supervisor.subprocess, 'run'):
            self.assertFalse(engine.stop(1))
        answer = json.loads((self.root / '1.json').read_text())
        self.assertEqual(answer['reason'], 'cleanup_failed')
        self.assertFalse(answer['cleanup_complete'])
        self.assertIn(1, engine.blocked)
        backend.storage.remove_seat.assert_not_called()


class StorageSafetyTest(unittest.TestCase):
    def test_writable_service_is_refused(self):
        with tempfile.TemporaryDirectory() as d:
            service = Path(d).resolve() / "service"
            service.mkdir(mode=0o777)
            service.chmod(0o777)
            with self.assertRaisesRegex(supervisor.SeatFailure, "unsafe_service"):
                supervisor.prepare_storage(service, owner=__import__('os').geteuid())

    def test_storage_symlinks_never_change_their_target(self):
        for name in ("seats", "shared"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as d:
                root = Path(d).resolve()
                service = root / "service"
                service.mkdir()
                outside = root / "outside"
                outside.mkdir(mode=0o700)
                (outside / "keep").write_text("safe")
                (service / name).symlink_to(outside, target_is_directory=True)
                with self.assertRaises(supervisor.SeatFailure):
                    supervisor.prepare_storage(service, owner=__import__('os').geteuid())
                self.assertEqual((outside / "keep").read_text(), "safe")
                self.assertEqual(outside.stat().st_mode & 0o777, 0o700)

    def test_recursive_delete_unlinks_links_and_keeps_outside_files(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            service = root / "service"
            service.mkdir()
            outside = root / "outside"
            outside.mkdir()
            (outside / "keep").write_text("safe")
            storage = supervisor.prepare_storage(service, owner=__import__('os').geteuid())
            self.addCleanup(storage.close)
            seat = service / "seats" / "1"
            seat.mkdir()
            (seat / "link").symlink_to(outside, target_is_directory=True)
            storage.remove_seat(1)
            self.assertFalse(seat.exists())
            self.assertEqual((outside / "keep").read_text(), "safe")

    def test_deep_tree_cleanup_uses_bounded_descriptors(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            parent = supervisor.open_directory(root)
            os.mkdir('tree', dir_fd=parent)
            leaf = os.open('tree', os.O_RDONLY | os.O_DIRECTORY, dir_fd=parent)
            depth = 1500
            for _ in range(depth):
                os.mkdir('d', dir_fd=leaf)
                child = os.open('d', os.O_RDONLY | os.O_DIRECTORY, dir_fd=leaf)
                os.close(leaf)
                leaf = child
            before = resource.getrlimit(resource.RLIMIT_NOFILE)
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (64, before[1]))
                supervisor.remove_at(parent, 'tree')
                self.assertFalse((root / 'tree').exists())
            finally:
                resource.setrlimit(resource.RLIMIT_NOFILE, before)
                # Keep test failure cleanup independent of the implementation.
                if (root / 'tree').exists():
                    for _ in range(depth):
                        upper = os.open('..', os.O_RDONLY | os.O_DIRECTORY, dir_fd=leaf)
                        os.close(leaf)
                        os.rmdir('d', dir_fd=upper)
                        leaf = upper
                    os.rmdir('tree', dir_fd=parent)
                os.close(leaf)
                os.close(parent)

    def test_ipc_cleaner_ignores_user_python_startup_and_cwd(self):
        backend = supervisor.DesktopBackend(mock.Mock())
        with mock.patch.object(supervisor, 'kill_uid'), mock.patch.object(supervisor, 'sweep_scratch'), mock.patch.object(supervisor.subprocess, 'run') as run:
            backend.stop(1)
        args, kwargs = run.call_args
        self.assertEqual(args[0], ['/usr/bin/python3', '-I', '-S', '/usr/local/libexec/wefty-seat-clean-ipc'])
        self.assertEqual(kwargs['cwd'], '/')
        self.assertEqual(kwargs['env'], {'PATH': '/usr/bin:/bin'})
        self.assertEqual(kwargs['user'], 2001)

    def test_missing_status_mount_has_a_typed_reason(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(supervisor.SeatFailure, "attempt_status_mount_required"):
                supervisor.require_status_mount(Path(d), mounts="")


if __name__ == "__main__":
    unittest.main()

class EndpointDirectiveTest(unittest.TestCase):
    def test_directive_names_only_its_own_socket_paths(self):
        doc = directive(1, 1)
        seat = doc['seats'][0]
        seat['view_socket'] = '/wefty/service/seats/1/run/view.ws'
        seat['control_socket'] = '/wefty/service/seats/1/run/control.ws'
        self.assertEqual(supervisor.validate(doc)[1]['view_socket'], seat['view_socket'])
        for path in ('/wefty/service/seats/2/run/view.ws', '/tmp/view.ws',
                     '/wefty/service/seats/1/run/../view.ws'):
            seat['view_socket'] = path
            with self.assertRaises(supervisor.SeatFailure):
                supervisor.validate(doc)

    def test_supervisor_probes_a_unix_endpoint(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            path = str(Path(root).resolve() / 'view.ws')
            with supervisor.socket.socket(supervisor.socket.AF_UNIX, supervisor.socket.SOCK_STREAM) as listener:
                listener.bind(path)
                os.chmod(path, 0o600)
                with mock.patch.object(supervisor.socket, 'socket') as make_socket:
                    make_socket.return_value.connect.side_effect = OSError('probe fixture')
                    supervisor.rfb_ready(path)
                    make_socket.assert_called_once_with(supervisor.socket.AF_UNIX, supervisor.socket.SOCK_STREAM)
                    make_socket.return_value.connect.assert_called_once_with(path)
