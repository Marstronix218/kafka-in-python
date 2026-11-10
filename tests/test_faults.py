"""Fault injection: the rule table, the broker's /faults endpoints and HTTP handler, and a
network partition on the in-process cluster from test_replication."""

import json
import tempfile
import threading
import unittest
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from pathlib import Path

from eventlog.broker import _handler_for
from eventlog.common import UNAVAILABLE, APIError, request_json
from eventlog.faults import FaultInjector
from test_replication import TOPIC, InProcessCluster


class FaultInjectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sleeps = []
        self.faults = FaultInjector(targets={"b1", "b2", "controller"},
                                    sleep=self.sleeps.append)
        self.sent = 0

    def send(self) -> dict:
        self.sent += 1
        return {"sent": self.sent}

    def test_empty_table_sends_once_and_returns_the_reply(self) -> None:
        self.assertEqual({"sent": 1}, self.faults.outgoing("b2", "/replicate", self.send, 1.0))
        self.assertIsNone(self.faults.incoming("/produce"))

    def test_each_action_on_outgoing_calls(self) -> None:
        cases = [("drop", 0, "cannot reach"), ("drop_response", 1, "no response")]
        for action, sent, message in cases:
            self.sent = 0
            self.faults.set_rules([{"action": action, "to": "b2"}])
            with self.assertRaisesRegex(APIError, message) as caught:
                self.faults.outgoing("b2", "/replicate", self.send, 1.0)
            self.assertEqual((UNAVAILABLE, sent), (caught.exception.status, self.sent))

        self.sent = 0
        self.faults.set_rules([{"action": "duplicate"}])
        self.assertEqual({"sent": 2}, self.faults.outgoing("b2", "/commit", self.send, 1.0))

        self.sent = 0
        self.faults.set_rules([{"action": "delay", "seconds": 0.25}])
        self.assertEqual({"sent": 1}, self.faults.outgoing("b2", "/commit", self.send, 1.0))
        # Held past the caller's timeout, the request is never sent.
        self.faults.set_rules([{"action": "delay", "seconds": 5}])
        with self.assertRaisesRegex(APIError, "past the 1s timeout"):
            self.faults.outgoing("b2", "/commit", self.send, 1.0)
        self.assertEqual(([0.25, 1.0], 1), (self.sleeps, self.sent))

    def test_rules_match_by_direction_target_and_path_without_query(self) -> None:
        self.faults.set_rules([
            {"action": "drop", "to": "controller", "path": "/route"},
            {"action": "drop_response", "direction": "in", "path": "/produce"},
        ])

        with self.assertRaises(APIError):
            self.faults.outgoing("controller", "/route?topic=events&partition=0", self.send, 1.0)
        self.faults.outgoing("controller", "/heartbeat", self.send, 1.0)
        self.faults.outgoing("b2", "/route", self.send, 1.0)
        self.faults.outgoing("b2", "/produce", self.send, 1.0)
        self.assertEqual(3, self.sent)
        self.assertEqual("drop_response", self.faults.incoming("/produce"))
        self.assertIsNone(self.faults.incoming("/fetch"))

    def test_probability_rolls_repeat_for_the_same_seed(self) -> None:
        def pattern() -> list:
            return [self.faults.incoming("/produce") for _ in range(20)]

        self.faults.set_rules([{"action": "drop", "direction": "in", "probability": 0.5}],
                              seed=7)
        first = pattern()
        self.faults.set_rules(self.faults.rules(), seed=7)

        self.assertEqual(first, pattern())
        self.assertEqual({"drop", None}, set(first))

    def test_invalid_rules_are_refused_and_the_table_is_kept(self) -> None:
        self.faults.set_rules([{"action": "drop", "to": "b2"}])
        invalid = [
            {"action": "explode"},
            {"action": "drop", "to": "b9"},
            {"action": "drop", "direction": "in", "to": "b2"},
            {"action": "delay"},
            {"action": "drop", "seconds": 1},
            {"action": "drop", "probability": 1.5},
            {"action": "drop", "path": "replicate"},
            {"action": "drop", "port": 9000},
            "drop",
        ]
        for rule in invalid:
            with self.assertRaises(ValueError, msg=rule):
                self.faults.set_rules([{"action": "drop"}, rule])

        self.assertEqual([{"action": "drop", "direction": "out", "to": "b2", "path": "*",
                           "probability": 1.0, "seconds": 0.0}], self.faults.rules())


class FaultsOnClusterTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary_directory.cleanup)
        self.cluster = InProcessCluster(Path(self._temporary_directory.name))
        self.assertEqual("b1", self.cluster.route()["leader"])

    def serve(self, broker_id: str) -> str:
        """Serve one broker's real HTTP handler on a free port and return its URL."""
        handler = _handler_for(self.cluster.brokers[broker_id])
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server.daemon_threads = True
        # A short poll interval, because shutdown() waits for the next poll.
        threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_address[1]}"

    def test_partition_between_leader_and_follower_shrinks_isr_then_heals(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        # Kafka reference: trogdor NetworkPartitionFaultSpec.
        cluster.brokers["b1"].faults.set_rules([{"action": "drop", "to": "b2"}])
        cluster.brokers["b2"].faults.set_rules([{"action": "drop", "to": "b1"}])

        for value in ("b", "c"):
            cluster.produce("b1", value, acks=2)
        cluster.brokers["b1"].maintain(TOPIC, 0)

        self.assertEqual(["b1", "b3"], cluster.route()["isr"])
        self.assertEqual(["a"], cluster.values("b2"))
        self.assertEqual(["a", "b", "c"], cluster.values("b3"))
        with self.assertRaisesRegex(APIError, "not enough replicas: acks=3"):
            cluster.produce("b1", "d", acks=3)

        for broker_id in ("b1", "b2"):
            cluster.brokers[broker_id].faults.set_rules([])
        cluster.brokers["b1"].maintain(TOPIC, 0)

        self.assertEqual(["b1", "b2", "b3"], cluster.route()["isr"])
        self.assertEqual((["a", "b", "c"], 2), (cluster.values("b2"), cluster.committed("b2")))

    def test_http_handler_applies_incoming_rules_except_to_faults(self) -> None:
        url = self.serve("b1")
        body = {"topic": TOPIC, "partition": 0, "key": None, "value": "a", "acks": 2,
                "producer_id": "p", "sequence": 0}
        reply = request_json(url + "/faults", "POST", {"rules": [
            {"action": "drop_response", "direction": "in", "path": "/produce"}]})
        self.assertEqual("drop_response", reply["rules"][0]["action"])

        with self.assertRaisesRegex(APIError, "no response") as lost:
            request_json(url + "/produce", "POST", body)
        self.assertEqual(UNAVAILABLE, lost.exception.status)
        self.assertEqual(["a"], self.cluster.values("b1"))

        request_json(url + "/faults", "POST", {"rules": [
            {"action": "duplicate", "direction": "in", "path": "/produce"}]})
        reply = request_json(url + "/produce", "POST", {**body, "value": "b", "sequence": 1})
        self.assertEqual((1, True), (reply["offset"], reply["duplicate"]))
        self.assertEqual(["a", "b"], self.cluster.values("b1"))

        # A rule that drops every request still lets /faults through, so it can be cleared.
        request_json(url + "/faults", "POST", {"rules": [{"action": "drop", "direction": "in"}]})
        with self.assertRaises(APIError):
            request_json(url + "/health")
        request_json(url + "/faults", "POST", {"rules": []})
        self.assertEqual({"broker_id": "b1", "rules": []}, request_json(url + "/faults"))
        self.assertTrue(request_json(url + "/health")["ok"])

    def test_faults_endpoint_rejects_an_invalid_table(self) -> None:
        url = self.serve("b1")
        for body in ({"rules": [{"action": "drop", "to": "nobody"}]}, {"rules": "drop"}, {}):
            with self.assertRaises(APIError) as caught:
                request_json(url + "/faults", "POST", body)
            self.assertEqual(HTTPStatus.BAD_REQUEST, caught.exception.status, json.dumps(body))
        self.assertEqual([], request_json(url + "/faults")["rules"])


if __name__ == "__main__":
    unittest.main()
