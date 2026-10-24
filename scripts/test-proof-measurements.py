#!/usr/bin/env python3
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location("proof", pathlib.Path(__file__).with_name("measure-per-user-proof.py"))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)

class ProofSafetyTests(unittest.TestCase):
    def test_go_nanosecond_timestamps_use_report_microsecond_precision(self):
        self.assertEqual(proof.seconds("1970-01-01T00:00:01.123456789Z"), 1.123456)

    def test_daily_instance_is_refused(self):
        with self.assertRaises(ValueError):
            proof.validate_instance("wefty-oci")

    def test_low_free_memory_stops_the_proof(self):
        self.assertTrue(proof.guard_tripped(19, 0, 0))

    def test_swap_growth_stops_the_proof(self):
        self.assertTrue(proof.guard_tripped(50, (1 << 30) + 1, 0))

if __name__ == "__main__":
    unittest.main()
