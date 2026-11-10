"""Idempotent produce tests on the in-process cluster from test_replication.

The Producer tests swap eventlog.client.request_json for the cluster's client_request,
and the client's retry sleep for a no-op, so retries happen at once.
"""

import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from eventlog.broker import CONTROLLER
from eventlog.client import ClusterClient, Producer
from eventlog.common import BAD_SEQUENCE, UNAVAILABLE, APIError
from test_replication import TOPIC, InProcessCluster


class IdempotentProduceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.cluster = InProcessCluster(Path(self._temporary_directory.name))
        self.assertEqual("b1", self.cluster.route()["leader"])
        for patch in (mock.patch("eventlog.client.request_json", self.cluster.client_request),
                      mock.patch("eventlog.client.time", SimpleNamespace(sleep=lambda _: None))):
            patch.start()
            self.addCleanup(patch.stop)
        self.producer = Producer(ClusterClient("http://controller"), "p1")

    def tearDown(self) -> None:
        self._temporary_directory.cleanup()

    def sequences(self, broker_id: str) -> list:
        log = self.cluster.brokers[broker_id].log(TOPIC, 0)
        return [record.get("sequence") for record in log.read_range(0, log.last_offset + 1)]

    def test_out_of_order_and_too_old_sequences_are_rejected_without_appending(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", producer_id="p", sequence=0)

        with self.assertRaises(APIError) as gap:
            cluster.produce("b1", "c", producer_id="p", sequence=2)

        self.assertEqual(BAD_SEQUENCE, gap.exception.status)
        self.assertIn("out of order sequence", str(gap.exception))
        self.assertEqual({"last_sequence": 0}, gap.exception.fields)
        self.assertEqual(["a"], cluster.values("b1"))

        for sequence in range(1, 7):
            cluster.produce("b1", sequence, producer_id="p", sequence=sequence)
        # Sequences 2..6 are the 5 the log keeps, so 1 has no known offset any more.
        with self.assertRaisesRegex(APIError, "duplicate sequence"):
            cluster.produce("b1", "again", producer_id="p", sequence=1)
        reply = cluster.produce("b1", 4, producer_id="p", sequence=4)

        self.assertEqual((4, True), (reply["offset"], reply["duplicate"]))
        self.assertEqual(["a", 1, 2, 3, 4, 5, 6], cluster.values("b1"))

    def test_stored_sequence_with_different_content_is_rejected(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", producer_id="p", sequence=0)
        cluster.produce("b1", "b", producer_id="p", sequence=1)

        for key, value in ((None, "other"), ("k", "b"), (None, True)):
            with self.assertRaises(APIError) as caught:
                cluster.produce("b1", value, producer_id="p", sequence=1, key=key)
            self.assertEqual(BAD_SEQUENCE, caught.exception.status)
            self.assertIn("sequence reused with different content", str(caught.exception))
            self.assertEqual({"last_sequence": 1, "offset": 1}, caught.exception.fields)
        self.assertEqual(["a", "b"], cluster.values("b1"))

    def test_retry_after_append_without_enough_replicas_returns_the_first_offset(self) -> None:
        cluster = self.cluster
        cluster.stop("b2")
        cluster.stop("b3")
        with self.assertRaisesRegex(APIError, "not enough replicas after append"):
            cluster.produce("b1", "a", producer_id="p", sequence=0)
        # The ISR is now just b1, so the retry is refused before it gets anywhere.
        with self.assertRaisesRegex(APIError, "not enough replicas: acks=2"):
            cluster.produce("b1", "a", producer_id="p", sequence=0)

        cluster.start("b3")
        cluster.brokers["b1"].maintain(TOPIC, 0)
        reply = cluster.produce("b1", "a", producer_id="p", sequence=0)

        self.assertEqual((0, True), (reply["offset"], reply["duplicate"]))
        self.assertEqual(["a"], cluster.values("b1"))
        self.assertEqual(["a"], cluster.values("b3"))

    def test_lost_reply_retried_on_the_new_leader_returns_the_original_offset(self) -> None:
        cluster = self.cluster
        self.producer.produce(TOPIC, "a", partition=0)
        # The followers get "b" but not the /commit after it, so it is uncommitted on
        # b2 when b2 takes over.
        cluster.blocked |= {("b1", "b2", "/commit"), ("b1", "b3", "/commit")}
        cluster.brokers["b1"].faults.set_rules(
            [{"action": "drop_response", "direction": "in", "path": "/produce"}])
        committed_on_b2_at_failover = []

        def kill_leader(broker_id: str) -> None:
            committed_on_b2_at_failover.append(cluster.committed("b2"))
            cluster.stop(broker_id)

        cluster.after_dropped_reply = kill_leader

        reply = self.producer.produce(TOPIC, "b", partition=0)

        self.assertEqual([0], committed_on_b2_at_failover)

        self.assertEqual("b2", cluster.route()["leader"])
        self.assertEqual({"offset": 1, "sequence": 1, "duplicate": True},
                         {name: reply[name] for name in ("offset", "sequence", "duplicate")})
        for broker_id in ("b2", "b3"):
            self.assertEqual(["a", "b"], cluster.values(broker_id))
            self.assertEqual(1, cluster.committed(broker_id))

    def test_duplicated_requests_and_lost_replies_store_each_record_once(self) -> None:
        broker = self.cluster.brokers["b1"]
        broker.faults.set_rules([{"action": "duplicate", "direction": "in", "path": "/produce"}])
        reply = self.producer.produce(TOPIC, 0, partition=0)
        self.assertEqual((0, True), (reply["offset"], reply["duplicate"]))

        broker.faults.set_rules([{"action": "drop_response", "direction": "in",
                                  "path": "/produce", "probability": 0.5}], seed=3)
        replies = [self.producer.produce(TOPIC, value, partition=0, retries=10)
                   for value in range(1, 11)]

        self.assertEqual(list(range(1, 11)), [reply["offset"] for reply in replies])
        self.assertTrue(any(reply["duplicate"] for reply in replies))
        self.assertEqual(list(range(11)), self.cluster.values("b1"))
        self.assertEqual(list(range(11)), self.sequences("b1"))
        self.assertEqual(list(range(11)), self.sequences("b2"))

    def test_producer_moves_to_a_new_id_after_a_write_that_was_never_stored(self) -> None:
        cluster = self.cluster
        self.producer.produce(TOPIC, "a", partition=0)
        # b1 can't ask the controller whether it leads, so it refuses the write.
        cluster.blocked.add(("b1", CONTROLLER, "/route"))
        with self.assertRaises(APIError) as caught:
            self.producer.produce(TOPIC, "lost", partition=0, retries=1)
        self.assertEqual(UNAVAILABLE, caught.exception.status)
        cluster.blocked.clear()

        # Sequence 2 would leave a gap. Sequence 1 may still arrive from the failed
        # call, so the producer starts again at 0 under a new id.
        reply = self.producer.produce(TOPIC, "b", partition=0)
        following = self.producer.produce(TOPIC, "c", partition=0)

        self.assertNotEqual("p1", reply["producer_id"])
        self.assertEqual((1, 0), (reply["offset"], reply["sequence"]))
        self.assertEqual((reply["producer_id"], 2, 1), (following["producer_id"],
                                                        following["offset"],
                                                        following["sequence"]))
        self.assertEqual(["a", "b", "c"], cluster.values("b1"))
        self.assertEqual([0, 0, 1], self.sequences("b1"))

    def test_attempts_held_at_the_leader_store_each_record_once(self) -> None:
        cluster = self.cluster
        held = []
        attempts_for_b = []

        def request(url, method="GET", payload=None, timeout=1.5):
            if url.endswith("/produce") and payload["value"] == "A":
                # The client times out while the request still waits at the leader.
                held.append(payload)
                raise APIError(UNAVAILABLE, "no response (timeout)")
            if url.endswith("/produce") and payload["value"] == "B":
                attempts_for_b.append((payload["producer_id"], payload["sequence"]))
                if len(attempts_for_b) == 1:
                    held.append(payload)
                    raise APIError(UNAVAILABLE, "no response (timeout)")
            return cluster.client_request(url, method, payload, timeout)

        self.producer.produce(TOPIC, "first", partition=0)
        with mock.patch("eventlog.client.request_json", request):
            with self.assertRaises(APIError):
                self.producer.produce(TOPIC, "A", partition=0, retries=0)
            reply = self.producer.produce(TOPIC, "B", partition=0, retries=1)
        # Both of B's attempts use one new id and sequence 0, never A's sequence + 1.
        self.assertEqual([(reply["producer_id"], 0)] * 2, attempts_for_b)
        self.assertNotEqual("p1", reply["producer_id"])
        self.assertEqual((1, False), (reply["offset"], reply["duplicate"]))

        late_a, late_b = (cluster.brokers["b1"].produce(body) for body in held)

        self.assertEqual((2, False), (late_a["offset"], late_a["duplicate"]))
        self.assertEqual((1, True), (late_b["offset"], late_b["duplicate"]))
        self.assertEqual(["first", "B", "A"], cluster.values("b1"))

    def test_sequence_gap_is_raised_and_the_next_record_uses_a_new_id(self) -> None:
        self.producer.produce(TOPIC, "a", partition=0)
        # Started by hand at sequence 5 under the same id, so the leader sees a gap.
        producer = Producer(ClusterClient("http://controller"), "p1", first_sequence=5)

        with self.assertRaisesRegex(APIError, "out of order sequence") as caught:
            producer.produce(TOPIC, "b", partition=0)

        self.assertEqual(BAD_SEQUENCE, caught.exception.status)
        reply = producer.produce(TOPIC, "c", partition=0)
        self.assertNotEqual("p1", reply["producer_id"])
        self.assertEqual((1, 0), (reply["offset"], reply["sequence"]))
        self.assertEqual(["a", "c"], self.cluster.values("b1"))

    def test_produce_that_reaches_the_append_too_late_is_not_appended(self) -> None:
        cluster = self.cluster
        body = {"topic": TOPIC, "partition": 0, "key": None, "value": "late", "acks": 2}

        with self.assertRaisesRegex(APIError, "did not append it") as caught:
            cluster.brokers["b1"].produce(body, arrived=time.monotonic() - 3)

        self.assertEqual(UNAVAILABLE, caught.exception.status)
        self.assertEqual([], cluster.values("b1"))
        self.assertEqual(0, cluster.brokers["b1"].produce(body)["offset"])

    def test_failed_write_stored_before_the_reused_sequence_is_rejected(self) -> None:
        # The reviewer's sequence of events at the broker: the next record is first
        # told it leaves a gap, then the failed record lands with the missing sequence.
        # An older Producer then reused that sequence for the next record.
        cluster = self.cluster
        cluster.produce("b1", "first", producer_id="p", sequence=0)
        with self.assertRaisesRegex(APIError, "out of order sequence"):
            cluster.produce("b1", "B", producer_id="p", sequence=2)
        cluster.produce("b1", "A", producer_id="p", sequence=1)

        with self.assertRaisesRegex(APIError, "sequence reused with different content"):
            cluster.produce("b1", "B", producer_id="p", sequence=1)
        self.assertEqual(["first", "A"], cluster.values("b1"))

    def test_cluster_client_numbers_its_records_under_one_producer_id(self) -> None:
        client = ClusterClient("http://controller")
        replies = [client.produce(TOPIC, value, partition=0) for value in ("a", "b")]
        explicit = client.produce(TOPIC, "a", partition=0, producer_id=replies[0]["producer_id"],
                                  sequence=0)

        self.assertEqual(replies[0]["producer_id"], replies[1]["producer_id"])
        self.assertEqual([0, 1], [reply["sequence"] for reply in replies])
        self.assertEqual((0, True), (explicit["offset"], explicit["duplicate"]))
        self.assertEqual(["a", "b"], self.cluster.values("b1"))

    def test_producer_shared_by_threads_numbers_records_without_gaps(self) -> None:
        errors = []

        def write(thread: int) -> None:
            try:
                for index in range(25):
                    self.producer.produce(TOPIC, [thread, index], partition=0)
            except Exception as exc:  # reported below, a thread can't fail the test
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(thread,)) for thread in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual([], errors)
        self.assertEqual(list(range(100)), self.sequences("b1"))
        values = self.cluster.values("b1")
        self.assertEqual(100, len({tuple(value) for value in values}))
        for thread in range(4):
            # Each thread's records keep their order within the partition.
            self.assertEqual(list(range(25)), [index for t, index in values if t == thread])


if __name__ == "__main__":
    unittest.main()
