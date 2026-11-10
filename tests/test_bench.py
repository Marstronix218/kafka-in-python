"""The benchmark's counting helpers, plus smoke runs of `bench` and `demo --fast` against
real subprocess clusters."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from eventlog.bench import (account, markdown_table, percentile, replica_mismatches, tag_of,
                            tagged_value)
from eventlog.storage import PartitionLog


ROOT = Path(__file__).resolve().parents[1]


class PercentileTests(unittest.TestCase):
    def test_nearest_rank_returns_a_measured_value(self) -> None:
        values = [5.0, 1.0, 4.0, 2.0, 3.0, 10.0, 9.0, 8.0, 7.0, 6.0]

        self.assertEqual(5.0, percentile(values, 50))
        self.assertEqual(10.0, percentile(values, 95))
        self.assertEqual(10.0, percentile(values, 100))
        self.assertEqual(1.0, percentile(values, 1))
        self.assertEqual(3.0, percentile([3.0], 99))

    def test_empty_list_and_bad_percentile(self) -> None:
        self.assertIsNone(percentile([], 50))
        for q in (0, -1, 101):
            with self.assertRaises(ValueError):
                percentile([1.0], q)


def stored_record(offset: int, value: str) -> dict:
    return {"offset": offset, "key": None, "value": value, "timestamp": 1.0}


class AccountTests(unittest.TestCase):
    def test_values_start_with_a_tag_and_keep_their_size(self) -> None:
        value = tagged_value(3, 17, 100)

        self.assertEqual(100, len(value))
        self.assertEqual("3/17", tag_of(value))
        self.assertEqual("3/17;", tagged_value(3, 17, 2))
        self.assertIsNone(tag_of("no tag"))
        self.assertIsNone(tag_of({"partition": 0}))

    def test_every_acknowledged_record_stored_once(self) -> None:
        values = [tagged_value(0, index, 10) for index in range(3)]
        acked = {tag_of(values[0]): (0, 0, values[0]), tag_of(values[1]): (0, 1, values[1]),
                 tag_of(values[2]): (1, 0, values[2])}
        stored = {0: [stored_record(0, values[0]), stored_record(1, values[1])],
                  1: [stored_record(0, values[2])]}

        counts = account(acked, set(), stored)

        self.assertEqual({"acknowledged": 3, "failed": 0, "stored": 3, "duplicates": 0,
                          "lost": 0, "failed_but_stored": 0, "unexpected": 0}, counts)

    def test_duplicates_lost_and_unknown_records_are_counted_apart(self) -> None:
        a, b, c, d, e = (tagged_value(0, index, 10) for index in range(5))
        acked = {tag_of(a): (0, 0, a), tag_of(b): (0, 3, b), tag_of(c): (0, 5, c)}
        failed = {tag_of(d), tag_of(e)}
        # a three times, b where it was acknowledged, c missing, failed d stored anyway,
        # failed e not stored, one record from another writer and one with no tag.
        stored = {0: [stored_record(offset, value) for offset, value in enumerate(
            [a, a, a, b, d, tagged_value(9, 0, 10), "untagged"])]}

        counts = account(acked, failed, stored)

        self.assertEqual({"acknowledged": 3, "failed": 2, "stored": 7, "duplicates": 2,
                          "lost": 1, "failed_but_stored": 1, "unexpected": 2}, counts)

    def test_acknowledged_offset_holding_another_record_counts_as_lost(self) -> None:
        first, a, b = (tagged_value(0, index, 10) for index in range(3))
        # a's produce call failed, but a was stored at offset 1 later. b was then
        # acknowledged as a duplicate with offset 1, and b itself is stored nowhere.
        acked = {tag_of(first): (0, 0, first), tag_of(b): (0, 1, b)}
        stored = {0: [stored_record(0, first), stored_record(1, a)]}

        counts = account(acked, {tag_of(a)}, stored)

        self.assertEqual((1, 0, 1), (counts["lost"], counts["duplicates"],
                                     counts["failed_but_stored"]))


class ReplicaMismatchTests(unittest.TestCase):
    def test_counts_partitions_whose_replicas_committed_different_records(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_dirs = {broker_id: Path(temporary) / broker_id for broker_id in ("b1", "b2")}
            for broker_id, values in (("b1", ["a", "b", "uncommitted"]), ("b2", ["a", "c"])):
                for partition in (0, 1):
                    log = PartitionLog(data_dirs[broker_id], "events", partition)
                    # Partition 0 differs at offset 1. Partition 1 has only "a" committed.
                    for offset, value in enumerate(values):
                        log.append(stored_record(offset, value))
                    log.commit(1 if partition == 0 else 0)
            routes = {("events", partition): {"leader": "b1", "replicas": ["b1", "b2"]}
                      for partition in (0, 1)}

            self.assertEqual(1, replica_mismatches(data_dirs, routes))


class TableTests(unittest.TestCase):
    def test_result_saved_before_replica_check_leaves_its_column_blank(self) -> None:
        result = {"config": {"events": 10, "partitions": 1, "acks": 2, "threads": 1,
                             "fault": "none"},
                  "metrics": {"throughput_per_second": 5.0,
                              "latency_ms": {"p50": 1, "p95": 2, "p99": 3, "max": 4},
                              "isr_lag_max": 0, "isr_lag_mean": 0.0, "acknowledged": 10,
                              "failed": 0, "stored": 10, "duplicates": 0, "lost": 0},
                  "wall_seconds": 3.0}

        row = markdown_table([result]).splitlines()[2]

        self.assertTrue(row.endswith("| 0 | 0 |  | 3.0 |"), row)


class SmokeTests(unittest.TestCase):
    """Each test starts a controller and three brokers, so they take a few seconds."""

    def run_cli(self, *arguments: str, timeout: float) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, "-m", "eventlog", *arguments], cwd=ROOT,
                              capture_output=True, text=True, timeout=timeout)

    def test_bench_with_lost_replies_saves_a_clean_result(self) -> None:
        with tempfile.TemporaryDirectory() as out:
            done = self.run_cli("bench", "--events", "120", "--partitions", "2",
                                "--threads", "2", "--fault", "lost-replies",
                                "--fault-probability", "0.2", "--out", out, timeout=60)
            self.assertEqual(0, done.returncode, done.stderr)
            [path] = Path(out).glob("*.json")
            result = json.loads(path.read_text())

        metrics = result["metrics"]
        self.assertEqual((120, 0, 120, 0, 0, 0, 0),
                         (metrics["acknowledged"], metrics["failed"], metrics["stored"],
                          metrics["duplicates"], metrics["lost"], metrics["unexpected"],
                          metrics["replica_mismatches"]))
        # About 24 lost replies are expected. Each retry finds the stored record.
        self.assertGreater(metrics["duplicate_replies"], 0)
        self.assertEqual({"events": 120, "partitions": 2, "acks": 2, "fault": "lost-replies"},
                         {name: result["config"][name]
                          for name in ("events", "partitions", "acks", "fault")})
        self.assertIn("git_commit", result["environment"])
        self.assertIn("| 120 | 2 | 2 | 2 | lost-replies |", done.stdout)
        self.assertEqual(markdown_table([result]).splitlines()[2],
                         next(line for line in done.stdout.splitlines()
                              if line.startswith("| 120 ")))

    def test_demo_fast_finds_every_write_stored_once(self) -> None:
        done = self.run_cli("demo", "--fast", timeout=90)

        self.assertEqual(0, done.returncode, done.stderr)
        self.assertIn("Step 8: stop writing and check every record", done.stdout)
        self.assertIn("stored more than once:        0", done.stdout)
        self.assertIn("acknowledged but missing:     0", done.stdout)
        self.assertIn("Every acknowledged write is stored exactly once.", done.stdout)


if __name__ == "__main__":
    unittest.main()
