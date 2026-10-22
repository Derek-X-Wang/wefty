"""Exercise probe CLI outcomes with RFB framebuffers and an injected clock."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('seat_probe', Path(__file__).with_name('seat-probe.py'))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def framebuffer(colour, offset=140, noise=False):
    rgb = bytes.fromhex(colour)
    pixel = bytes([rgb[2], rgb[1], rgb[0], 0])
    background = bytes([0x30, 0x20, 0x18, 0])
    data = bytearray(background * (1280 * 800))
    for row in range(offset, offset + 160):
        data[row * 1280 * 4:(row + 1) * 1280 * 4] = pixel * 1280
        if noise:
            # A narrow overlay/cursor is less than one fifth of each row.
            data[(row * 1280 + 600) * 4:(row * 1280 + 700) * 4] = bytes([128, 128, 128, 0]) * 100
    return data


class ProbeTest(unittest.TestCase):
    def run_probe(self, mode, expected, frames):
        self.now, self.updates, self.sent = 0, 0, []
        client = probe.RFB.__new__(probe.RFB)
        client.w, client.h = 1280, 800
        client.ws = SimpleNamespace(deadline=15, send=self.sent.append,
            s=SimpleNamespace(settimeout=lambda _timeout: None, close=lambda: None))
        def update():
            client.fb = frames[min(self.updates, len(frames) - 1)]
            self.updates += 1
        client.read_update = update
        def sleep(delay):
            self.now += delay
        output = io.StringIO()
        with mock.patch.object(probe, 'RFB', return_value=client), mock.patch.object(probe.sys, 'argv', ['seat-probe', mode, '7102', expected]), mock.patch.object(probe.time, 'monotonic', side_effect=lambda: self.now), mock.patch.object(probe.time, 'sleep', side_effect=sleep), contextlib.redirect_stdout(output):
            probe.main()
        return json.loads(output.getvalue())

    def test_frame_accepts_shifted_colour_with_small_overlay(self):
        for offset, colour in ((0, 'ff0000'), (57, 'ff0000'), (140, 'ff0000'), (120, '0000aa')):
            with self.subTest(offset=offset, colour=colour):
                result = self.run_probe('frame', colour, [framebuffer(colour, offset, noise=True)])
                self.assertEqual(result['colour'], colour)
                self.assertEqual((result['width'], result['height']), (1280, 800))
        with self.assertRaises(TimeoutError):
            self.run_probe('frame', '182030', [framebuffer('ff0000', offset=0)])

    def test_view_observes_two_seconds_and_rejects_even_one_shifted_green_frame(self):
        self.run_probe('view-input', 'ff0000', [framebuffer('ff0000')])
        self.assertGreaterEqual(self.now, 2)
        self.assertEqual([packet[0] for packet in self.sent[:4]], [5, 5, 4, 4])
        with self.assertRaisesRegex(OSError, 'view.*00ff00'):
            self.run_probe('view-input', 'ff0000', [framebuffer('ff0000'), framebuffer('00ff00'), framebuffer('ff0000')])
        partial = framebuffer('ff0000')
        partial[180 * 1280 * 4:200 * 1280 * 4] = bytes([0, 255, 0, 0]) * (1280 * 20)
        with self.assertRaisesRegex(OSError, 'view.*00ff00'):
            self.run_probe('view-input', 'ff0000', [partial])

    def test_control_input_waits_for_shifted_green(self):
        result = self.run_probe('input', '00ff00', [framebuffer('ff0000'), framebuffer('00ff00', noise=True)])
        self.assertEqual(result['colour'], '00ff00')
        self.assertGreaterEqual(self.updates, 2)
        self.assertEqual([packet[0] for packet in self.sent[:4]], [5, 5, 4, 4])

    def test_failure_keeps_last_observed_colour(self):
        with self.assertRaisesRegex(TimeoutError, '0000aa'):
            self.run_probe('frame', 'ff0000', [framebuffer('0000aa')])
