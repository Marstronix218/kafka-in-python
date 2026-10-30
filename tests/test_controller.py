import json
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer

from eventlog.controller import ControllerState, _handler_for


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
        self.assertNotIn("epoch", route)

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

    def test_unknown_broker_heartbeat_raises(self) -> None:
        state = ControllerState(CONFIG)
        with self.assertRaises(KeyError):
            state.heartbeat("b9")


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


if __name__ == "__main__":
    unittest.main()
