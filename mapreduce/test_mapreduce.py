"""Tests du MapReduce (bibliothèque standard uniquement).

Vérifie le mapper, le reducer, et que la version Hadoop Streaming
(mapper | sort | reducer) et la version parallèle donnent les mêmes résultats.

Usage :
    python -m unittest discover -s mapreduce -v
"""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from mapper import map_line  # noqa: E402
from parallel_job import run_job  # noqa: E402
from reducer import reduce_sorted  # noqa: E402


def event(device, event_type, severity):
    return json.dumps({"device": device, "event_type": event_type, "severity": severity, "value": 1})


SAMPLE = [
    event("rtr-paris-01", "link_down", "critical"),
    event("rtr-paris-01", "high_latency", "critical"),
    event("rtr-paris-01", "cpu_high", "info"),
    event("srv-lyon-01", "cpu_high", "critical"),
    event("srv-lyon-01", "disk_full", "warning"),
    event("fw-lille-01", "port_scan", "info"),
    "ceci n'est pas du JSON",
    "",
]

EXPECTED = {
    "type:link_down": 1, "type:high_latency": 1, "type:cpu_high": 2,
    "type:disk_full": 1, "type:port_scan": 1,
    "device:rtr-paris-01": 3, "device:srv-lyon-01": 2, "device:fw-lille-01": 1,
    "critical:rtr-paris-01": 2, "critical:srv-lyon-01": 1,
}


class MapperTest(unittest.TestCase):
    def test_critical_event_emits_three_pairs(self):
        self.assertEqual(
            map_line(event("rtr-paris-01", "link_down", "critical")),
            [("type:link_down", 1), ("device:rtr-paris-01", 1), ("critical:rtr-paris-01", 1)],
        )

    def test_non_critical_event_emits_two_pairs(self):
        self.assertEqual(len(map_line(event("srv-lyon-01", "cpu_high", "warning"))), 2)

    def test_invalid_lines_are_ignored(self):
        self.assertEqual(map_line("pas du json"), [])
        self.assertEqual(map_line('{"device": "x"}'), [])
        self.assertEqual(map_line(""), [])


class ReducerTest(unittest.TestCase):
    def test_sums_consecutive_keys(self):
        lines = ["a\t1\n", "a\t1\n", "b\t1\r\n", "c\t2\n", "c\t3\n"]
        self.assertEqual(list(reduce_sorted(lines)), [("a", 2), ("b", 1), ("c", 5)])


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "events.jsonl"
        self.path.write_text("\n".join(SAMPLE) + "\n", encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def test_streaming_pipeline(self):
        """Équivalent de : cat events.jsonl | mapper.py | sort | reducer.py"""
        mapped = subprocess.run(
            [sys.executable, str(HERE / "mapper.py")],
            input=self.path.read_bytes(), capture_output=True, check=True,
        ).stdout
        shuffled = b"".join(sorted(mapped.splitlines(keepends=True)))
        reduced = subprocess.run(
            [sys.executable, str(HERE / "reducer.py")],
            input=shuffled, capture_output=True, check=True,
        ).stdout.decode("utf-8")
        results = {key: int(total) for key, total in
                   (line.split("\t") for line in reduced.splitlines())}
        self.assertEqual(results, EXPECTED)

    def test_parallel_job_matches(self):
        results = run_job([self.path], workers=2, reducers=3, split_size=2, log=lambda _msg: None)
        self.assertEqual(results, EXPECTED)


if __name__ == "__main__":
    unittest.main()
