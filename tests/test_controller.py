import json
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer

from eventlog.controller import ControllerState, ISRUpdateRejected, _handler_for


CONFIG = {
    "brokers": {
        "b1": "http://127.0.0.1:9101",
        "b2": "http://127.0.0.1:9102",
        "b3": "http://127.0.0.1:9103",
    },
    "topics": {"orders": {"partitions": 3, "replication_factor": 3}},
}


class ControllerStateTests(unittest.TestCase):
    def test_assignment_rotates_and_initial_leader_is_preferred_alive_replica(self) -> None:
        state = ControllerState(CONFIG)
        self.assertEqual(["b1", "b2", "b3"], state.replicas_for("orders", 0))
        self.assertEqual(["b2", "b3", "b1"], state.replicas_for("orders", 1))
        self.assertEqual(["b3", "b1", "b2"], state.replicas_for("orders", 2))

        state.heartbeat("b3")
        state.heartbeat("b2")
        route = state.route("orders", 1)

        self.assertEqual(["b2", "b3", "b1"], route["replicas"])
        self.assertEqual("b2", route["leader"])
        self.assertEqual(CONFIG["brokers"], route["brokers"])
        self.assertEqual(0, route["leader_epoch"])
        self.assertEqual(["b2", "b3", "b1"], route["isr"])

    def test_failover_moves_to_next_alive_replica_in_assignment_order(self) -> None:
        state = ControllerState(CONFIG, heartbeat_timeout=0.05)
        for broker_id in CONFIG["brokers"]:
            state.heartbeat(broker_id)
        self.assertEqual("b1", state.route("orders", 0)["leader"])

        time.sleep(0.06)
        state.heartbeat("b3")
        state.heartbeat("b2")
        self.assertEqual("b2", state.route("orders", 0)["leader"])

    def test_recovered_former_leader_does_not_preempt(self) -> None:
        state = ControllerState(CONFIG, heartbeat_timeout=0.05)
        for broker_id in CONFIG["brokers"]:
            state.heartbeat(broker_id)
        self.assertEqual("b1", state.route("orders", 0)["leader"])

        time.sleep(0.06)
        state.heartbeat("b2")
        state.heartbeat("b3")
        self.assertEqual("b2", state.route("orders", 0)["leader"])

        state.heartbeat("b1")
        self.assertEqual("b2", state.route("orders", 0)["leader"])

    def test_route_is_none_when_no_replica_is_alive(self) -> None:
        state = ControllerState(CONFIG, heartbeat_timeout=0.03)
        self.assertIsNone(state.route("orders", 0))

        state.heartbeat("b1")
        self.assertEqual("b1", state.route("orders", 0)["leader"])

        time.sleep(0.04)
        self.assertIsNone(state.route("orders", 0))

    def test_leader_epoch_increments_on_each_failover(self) -> None:
        state = ControllerState(CONFIG, heartbeat_timeout=0.05)
        for broker_id in CONFIG["brokers"]:
            state.heartbeat(broker_id)
        self.assertEqual(("b1", 0), _leader_and_epoch(state))
        self.assertEqual(("b1", 0), _leader_and_epoch(state))

        time.sleep(0.06)
        state.heartbeat("b2")
        state.heartbeat("b3")
        self.assertEqual(("b2", 1), _leader_and_epoch(state))

        time.sleep(0.06)
        state.heartbeat("b3")
        self.assertEqual(("b3", 2), _leader_and_epoch(state))

    def test_election_skips_alive_replicas_outside_the_isr(self) -> None:
        state = ControllerState(CONFIG, heartbeat_timeout=0.05)
        for broker_id in CONFIG["brokers"]:
            state.heartbeat(broker_id)
        state.route("orders", 0)
        state.alter_isr("orders", 0, "b1", 0, ["b1", "b3"])

        time.sleep(0.06)
        state.heartbeat("b2")
        self.assertIsNone(state.route("orders", 0))
        self.assertEqual("no in-sync replica is alive", state.unavailable_reason("orders", 0))

        state.heartbeat("b3")
        route = state.route("orders", 0)
        self.assertEqual(("b3", 1), (route["leader"], route["leader_epoch"]))
        self.assertEqual(["b1", "b3"], route["isr"])

    def test_alter_isr_accepts_only_the_current_leader_and_epoch(self) -> None:
        state = ControllerState(CONFIG, heartbeat_timeout=0.05)
        for broker_id in CONFIG["brokers"]:
            state.heartbeat(broker_id)
        state.route("orders", 0)

        rejected = [("b2", 0, ["b2", "b3"]),           # not the leader
                    ("b1", 1, ["b1"]),                 # wrong epoch
                    ("b1", 0, ["b2", "b3"]),           # leaves out the leader
                    ("b1", 0, ["b1", "b9"]),           # not a replica
                    ("b1", 0, ["b1", "b1"])]           # duplicate
        for leader, epoch, isr in rejected:
            with self.subTest(leader=leader, epoch=epoch, isr=isr):
                with self.assertRaises(ISRUpdateRejected):
                    state.alter_isr("orders", 0, leader, epoch, isr)

        result = state.alter_isr("orders", 0, "b1", 0, ["b3", "b1"])
        self.assertEqual({"leader": "b1", "leader_epoch": 0, "isr": ["b1", "b3"]}, result)
        self.assertEqual(["b1", "b3"], state.route("orders", 0)["isr"])

    def test_unclean_election_elects_any_alive_replica_and_resets_isr(self) -> None:
        for unclean in (False, True):
            with self.subTest(unclean=unclean):
                state = ControllerState(CONFIG, heartbeat_timeout=0.05,
                                        unclean_leader_election=unclean)
                for broker_id in CONFIG["brokers"]:
                    state.heartbeat(broker_id)
                state.route("orders", 0)
                state.alter_isr("orders", 0, "b1", 0, ["b1"])

                time.sleep(0.06)
                state.heartbeat("b3")
                route = state.route("orders", 0)
                if unclean:
                    self.assertEqual(("b3", 1, ["b3"]),
                                     (route["leader"], route["leader_epoch"], route["isr"]))
                else:
                    self.assertIsNone(route)

    def test_unknown_broker_heartbeat_raises(self) -> None:
        state = ControllerState(CONFIG)
        with self.assertRaises(KeyError):
            state.heartbeat("b9")


def _leader_and_epoch(state: ControllerState) -> tuple[str, int]:
    route = state.route("orders", 0)
    return route["leader"], route["leader_epoch"]


class ControllerHTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state = ControllerState(CONFIG, heartbeat_timeout=0.05)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_for(self.state))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.connection = HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)

    def tearDown(self) -> None:
        self.connection.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method: str, path: str, body: dict | None = None):
        encoded = None if body is None else json.dumps(body)
        headers = {} if encoded is None else {"Content-Type": "application/json"}
        self.connection.request(method, path, encoded, headers)
        response = self.connection.getresponse()
        return response.status, json.loads(response.read())

    def test_http_api_and_unavailable_route(self) -> None:
        status, body = self.request("GET", "/route?topic=orders&partition=0")
        self.assertEqual(503, status)
        self.assertEqual("no replica is alive", body["error"])

        status, body = self.request("POST", "/heartbeat", {"broker_id": "b1"})
        self.assertEqual(200, status)
        self.assertEqual("ok", body["status"])

        status, route = self.request("GET", "/route?topic=orders&partition=0")
        self.assertEqual(200, status)
        self.assertEqual("b1", route["leader"])
        self.assertEqual(CONFIG["brokers"], route["brokers"])

        status, metadata = self.request("GET", "/metadata")
        self.assertEqual(200, status)
        self.assertEqual(CONFIG, metadata)

        status, health = self.request("GET", "/health")
        self.assertEqual(200, status)
        self.assertEqual(["b1"], health["alive_brokers"])

    def test_route_rejects_bad_input(self) -> None:
        status, _ = self.request("GET", "/route?topic=missing&partition=0")
        self.assertEqual(400, status)

        status, _ = self.request("GET", "/route?topic=orders&partition=abc")
        self.assertEqual(400, status)

    def test_heartbeat_requires_broker_id(self) -> None:
        status, body = self.request("POST", "/heartbeat", {})

        self.assertEqual(400, status)
        self.assertIn("broker_id", body["error"])

    def test_heartbeat_rejects_unknown_broker(self) -> None:
        status, body = self.request("POST", "/heartbeat", {"broker_id": "b9"})

        self.assertEqual(400, status)
        self.assertIn("b9", body["error"])

    def test_isr_endpoint_rejects_stale_leader_with_409_and_bad_body_with_400(self) -> None:
        # Long enough that b1 can't time out and lose leadership partway through.
        self.state.heartbeat_timeout = 60
        self.request("POST", "/heartbeat", {"broker_id": "b1"})
        self.request("POST", "/heartbeat", {"broker_id": "b2"})
        _, route = self.request("GET", "/route?topic=orders&partition=0")
        update = {"topic": "orders", "partition": 0, "leader": "b1",
                  "leader_epoch": route["leader_epoch"], "isr": ["b1", "b2"]}

        stale_status, stale = self.request("POST", "/isr", {**update, "leader_epoch": 7})
        other_status, other = self.request("POST", "/isr", {**update, "leader": "b2"})
        bad_status, _ = self.request("POST", "/isr", {**update, "isr": "b1"})
        status, result = self.request("POST", "/isr", update)

        self.assertEqual((409, 409, 400, 200), (stale_status, other_status, bad_status, status))
        self.assertIn("not the current leader", stale["error"])
        self.assertIn("not the current leader", other["error"])
        self.assertEqual(["b1", "b2"], result["isr"])
        _, route = self.request("GET", "/route?topic=orders&partition=0")
        self.assertEqual(["b1", "b2"], route["isr"])


if __name__ == "__main__":
    unittest.main()
