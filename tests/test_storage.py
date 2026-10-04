import json
import tempfile
import unittest
from pathlib import Path

from eventlog.storage import PartitionLog


def record(offset: int, value: str = "value") -> dict:
    return {"offset": offset, "key": "key", "value": value, "timestamp": 123.0}


class PartitionLogTests(unittest.TestCase):
    def setUp(self):
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._temporary_directory.name)

    def tearDown(self):
        self._temporary_directory.cleanup()

    def test_append_commit_and_restart(self):
        log = PartitionLog(self.data_dir, "orders", 0)
        log.append(record(0, "first"))
        log.append(record(1, "second"))

        self.assertEqual(log.last_offset, 1)
        self.assertEqual(log.committed_offset, -1)
        self.assertEqual(log.safe_offset, -1)
        self.assertEqual(log.get(1), record(1, "second"))
        self.assertEqual(log.read(0), [])

        log.commit(0)
        self.assertEqual(log.read(0), [record(0, "first")])
        self.assertEqual(log.read(1), [])

        restarted = PartitionLog(self.data_dir, "orders", 0)
        self.assertEqual(restarted.last_offset, 1)
        self.assertEqual(restarted.committed_offset, 0)
        self.assertEqual(restarted.safe_offset, -1)
        self.assertEqual(restarted.read(0, limit=10), [record(0, "first")])

    def test_duplicate_append_is_idempotent_and_gaps_are_rejected(self):
        log = PartitionLog(self.data_dir, "events", 2)
        first = record(0)
        log.append(first)
        log.append(first.copy())

        self.assertEqual(log.last_offset, 0)
        self.assertEqual(log._data_path.read_text(encoding="utf-8").count("\n"), 1)
        with self.assertRaisesRegex(ValueError, "expected offset 1"):
            log.append(record(2))

    def test_conflicting_uncommitted_suffix_is_replaced(self):
        log = PartitionLog(self.data_dir, "events", 0)
        for offset in range(3):
            log.append(record(offset, f"old-{offset}"))
        log.commit(0, safe=True)

        log.append(record(1, "new-1"))

        self.assertEqual(log.last_offset, 1)
        self.assertEqual(log.get(1), record(1, "new-1"))
        self.assertIsNone(log.get(2))
        with self.assertRaisesRegex(ValueError, "safe"):
            log.append(record(0, "different"))

    def test_truncate_rewrites_durably_and_protects_committed_entries(self):
        log = PartitionLog(self.data_dir, "events", 0)
        for offset in range(4):
            log.append(record(offset))
        log.commit(1, safe=True)
        log.truncate_from(3)

        restarted = PartitionLog(self.data_dir, "events", 0)
        self.assertEqual(restarted.last_offset, 2)
        self.assertEqual(restarted.committed_offset, 1)
        with self.assertRaisesRegex(ValueError, "safe"):
            restarted.truncate_from(1)

    def test_committed_but_unsafe_record_can_be_replaced_after_restart(self):
        log = PartitionLog(self.data_dir, "events", 0)
        log.append(record(0, "leader-a"))
        log.commit(0)

        restarted = PartitionLog(self.data_dir, "events", 0)
        self.assertEqual(restarted.committed_offset, 0)
        self.assertEqual(restarted.safe_offset, -1)
        restarted.append(record(0, "leader-b"))

        self.assertEqual(restarted.get(0), record(0, "leader-b"))
        self.assertEqual(restarted.committed_offset, -1)
        self.assertEqual(restarted.read(0), [])

    def test_safe_record_is_persisted_and_cannot_be_replaced(self):
        log = PartitionLog(self.data_dir, "events", 0)
        log.append(record(0, "quorum-protected"))
        log.commit(0, safe=True)

        restarted = PartitionLog(self.data_dir, "events", 0)
        self.assertEqual(restarted.committed_offset, 0)
        self.assertEqual(restarted.safe_offset, 0)
        with self.assertRaisesRegex(ValueError, "safe"):
            restarted.append(record(0, "conflict"))
        with self.assertRaisesRegex(ValueError, "safe"):
            restarted.truncate_from(0)

    def test_startup_recovers_a_torn_trailing_line(self):
        log = PartitionLog(self.data_dir, "events", 0)
        log.append(record(0))
        with log._data_path.open("ab") as stream:
            stream.write(b'{"offset":1,"key":"key"')

        recovered = PartitionLog(self.data_dir, "events", 0)
        self.assertEqual(recovered.last_offset, 0)
        recovered.append(record(1, "recovered"))

        lines = recovered._data_path.read_text(encoding="utf-8").splitlines()
        self.assertEqual([json.loads(line)["offset"] for line in lines], [0, 1])

    def test_invalid_operations_do_not_change_state(self):
        log = PartitionLog(self.data_dir, "events", 0)
        log.append(record(0))
        log.commit(0)

        with self.assertRaisesRegex(ValueError, "backwards"):
            log.commit(-1)
        with self.assertRaisesRegex(ValueError, "beyond"):
            log.commit(1)
        with self.assertRaisesRegex(ValueError, "required fields"):
            log.append({"offset": 1})

        self.assertEqual(log.last_offset, 0)
        self.assertEqual(log.committed_offset, 0)


if __name__ == "__main__":
    unittest.main()
