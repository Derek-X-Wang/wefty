"""Exercise probe CLI outcomes with RFB framebuffers and an injected clock."""
import contextlib
import importlib.util
import io
import json
import struct
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
    def run_probe(self, mode, expected, frames, click_changes_page=False):
        self.now, self.updates, self.sent = 0, 0, []
        client = probe.RFB.__new__(probe.RFB)
        client.w, client.h = 1280, 800
        client.ws = SimpleNamespace(deadline=15, send=self.sent.append,
            s=SimpleNamespace(settimeout=lambda _timeout: None, close=lambda: None))
        def update():
            if click_changes_page:
                # The page responds only to a pointer hitting its visible
                # band, below the 57px Chromium infobar. Keys cannot mask a
                # misplaced click. A narrow overlay covers the band centre.
                hit = False
                for packet in self.sent:
                    if packet[0] == 5:
                        _, mask, x, y = struct.unpack('>BBHH', packet)
                        hit |= mask == 1 and 57 <= y < 217 and not 600 <= x < 700
                client.fb = frames[int(hit)]
            else:
                client.fb = frames[min(self.updates, len(frames) - 1)]
            self.updates += 1
        client.read_update = update
        def sleep(delay):
            self.now += delay
        output = io.StringIO()
        port = '7103' if mode == 'input' else '7102'
        with mock.patch.object(probe, 'RFB', return_value=client), mock.patch.object(probe.sys, 'argv', ['seat-probe', mode, port, expected]), mock.patch.object(probe.time, 'monotonic', side_effect=lambda: self.now), mock.patch.object(probe.time, 'sleep', side_effect=sleep), contextlib.redirect_stdout(output):
            probe.main()
        return json.loads(output.getvalue())

    def test_click_hits_visible_page_below_infobar_and_reports_before_and_after(self):
        result = self.run_probe('input', '00ff00', [framebuffer('ff0000', 57, noise=True), framebuffer('00ff00', 57, noise=True)], click_changes_page=True)
        self.assertEqual(result['before_colour'], 'ff0000')
        self.assertEqual(result['colour'], '00ff00')
        self.assertEqual(result['mode'], 'input')
        self.assertEqual(result['port'], 7103)
        self.assertEqual(self.sent[0][0], 3, 'observe the page before sending input')
        clicks = [struct.unpack('>BBHH', packet) for packet in self.sent if packet[0] == 5]
        self.assertEqual(clicks[0][1], 1)
        self.assertEqual(list(clicks[0][2:]), result['input_target'])

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
        self.assertEqual([packet[0] for packet in self.sent if packet[0] != 3], [5, 5, 4, 4])
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
        self.assertEqual([packet[0] for packet in self.sent if packet[0] != 3], [5, 5, 4, 4])

    def test_already_green_cannot_prove_control_input(self):
        with self.assertRaisesRegex(OSError, 'already present before input.*"before_colour":"00ff00"'):
            self.run_probe('input', '00ff00', [framebuffer('00ff00')])
        self.assertFalse(any(packet[0] in (4, 5) for packet in self.sent))

    def test_failure_keeps_last_observed_colour(self):
        with self.assertRaisesRegex(TimeoutError, '0000aa'):
            self.run_probe('frame', 'ff0000', [framebuffer('0000aa')])

class UnixTransportTest(unittest.TestCase):
    def test_probe_connects_only_the_selected_unix_socket(self):
        path = '/wefty/service/seats/1/run/view.ws'
        with mock.patch.object(probe.socket, 'socket') as factory:
            factory.return_value.connect.side_effect = OSError('fixture')
            with self.assertRaises(OSError):
                probe.WS(path)
            factory.assert_called_once_with(probe.socket.AF_UNIX, probe.socket.SOCK_STREAM)
            factory.return_value.connect.assert_called_once_with(path)
