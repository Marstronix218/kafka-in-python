import json
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory

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
    def test_assignment_rotates_and_initial_election_uses_preferred_alive_replica(self) -> None:
        with TemporaryDirectory() as directory:
            state = ControllerState(CONFIG, Path(directory))
            state.heartbeat("b3", "boot-b3", {"orders:1": 99}, {"orders:1": 1})
            state.heartbeat("b2", "boot-b2", {"orders:1": 1}, {"orders:1": 1})

            route = state.route("orders", 1)

            self.assertEqual(["b2", "b3", "b1"], route["replicas"])
            self.assertEqual("b2", route["leader"])
            self.assertEqual(0, route["epoch"])

    def test_failover_selects_freshest_replica_and_does_not_preempt(self) -> None:
        with TemporaryDirectory() as directory:
            state = ControllerState(CONFIG, Path(directory), heartbeat_timeout=0.04)
            for broker_id in CONFIG["brokers"]:
                state.heartbeat(broker_id, f"boot-{broker_id}", {"orders:0": 5}, {"orders:0": 5})
            self.assertEqual("b1", state.route("orders", 0)["leader"])

            time.sleep(0.05)
            state.heartbeat("b2", "boot-b2", {"orders:0": 7}, {"orders:0": 7})
            state.heartbeat("b3", "boot-b3", {"orders:0": 12}, {"orders:0": 12})
            failed_over = state.route("orders", 0)
            self.assertEqual("b3", failed_over["leader"])
            self.assertEqual(1, failed_over["epoch"])

            state.heartbeat("b1", "boot-b1", {"orders:0": 20}, {"orders:0": 12})
            recovered = state.route("orders", 0)
            self.assertEqual("b3", recovered["leader"])
            self.assertEqual(1, recovered["epoch"])

    def test_tied_offsets_use_replica_assignment_order(self) -> None:
        with TemporaryDirectory() as directory:
            state = ControllerState(CONFIG, Path(directory), heartbeat_timeout=0.03)
            state.heartbeat("b1", "boot-b1", {"orders:0": 1}, {"orders:0": 1})
            state.heartbeat("b2", "boot-b2", {"orders:0": 1}, {"orders:0": 1})
            state.heartbeat("b3", "boot-b3", {"orders:0": 1}, {"orders:0": 1})
            state.route("orders", 0)

            time.sleep(0.04)
            state.heartbeat("b2", "boot-b2", {"orders:0": 8}, {"orders:0": 8})
            state.heartbeat("b3", "boot-b3", {"orders:0": 8}, {"orders:0": 8})
            self.assertEqual("b2", state.route("orders", 0)["leader"])

    def test_election_survives_controller_restart(self) -> None:
        with TemporaryDirectory() as directory:
            state_dir = Path(directory)
            state = ControllerState(CONFIG, state_dir, heartbeat_timeout=0.03)
            state.heartbeat("b1", "boot-b1", {"orders:0": 1}, {"orders:0": 1})
            state.route("orders", 0)
            time.sleep(0.04)
            state.heartbeat("b2", "boot-b2", {"orders:0": 2}, {"orders:0": 2})
            self.assertEqual(1, state.route("orders", 0)["epoch"])

            restarted = ControllerState(CONFIG, state_dir)
            restarted.heartbeat("b2", "boot-b2", {"orders:0": 2}, {"orders:0": 2})
            route = restarted.route("orders", 0)
            self.assertEqual("b2", route["leader"])
            self.assertEqual(1, route["epoch"])

    def test_trusted_live_leader_survives_follower_safe_watermark_race(self) -> None:
        with TemporaryDirectory() as directory:
            state = ControllerState(CONFIG, Path(directory))
            for broker_id in CONFIG["brokers"]:
                state.heartbeat(
                    broker_id,
                    f"boot-{broker_id}",
                    {"orders:0": 2},
                    {"orders:0": 2},
                )
            self.assertEqual("b1", state.route("orders", 0)["leader"])

            # A follower can report the quorum-safe commit before the leader's
            # next heartbeat observes the same local safe watermark.
            state.heartbeat("b2", "boot-b2", {"orders:0": 3}, {"orders:0": 3})
            route = state.route("orders", 0)

            self.assertEqual("b1", route["leader"])
            self.assertEqual(0, route["epoch"])

    def test_broker_reboot_clears_live_leader_trust(self) -> None:
        with TemporaryDirectory() as directory:
            state = ControllerState(CONFIG, Path(directory))
            state.heartbeat("b1", "boot-b1", {"orders:0": 5}, {"orders:0": 5})
            state.heartbeat("b2", "boot-b2", {"orders:0": 5}, {"orders:0": 5})
            self.assertEqual("b1", state.route("orders", 0)["leader"])

            state.heartbeat("b2", "boot-b2", {"orders:0": 6}, {"orders:0": 6})
            state.heartbeat("b1", "boot-b1-restarted", {"orders:0": 5}, {"orders:0": 5})
            route = state.route("orders", 0)

            self.assertEqual("b2", route["leader"])
            self.assertEqual(1, route["epoch"])

    def test_restart_will_not_promote_replica_below_persisted_safe_watermark(self) -> None:
        with TemporaryDirectory() as directory:
            state_dir = Path(directory)
            state = ControllerState(CONFIG, state_dir, heartbeat_timeout=0.03)
            state.heartbeat("b1", "boot-b1", {"orders:0": 10}, {"orders:0": 10})
            state.heartbeat("b2", "boot-b2", {"orders:0": 8}, {"orders:0": 8})
            self.assertEqual("b1", state.route("orders", 0)["leader"])

            restarted = ControllerState(CONFIG, state_dir)
            restarted.heartbeat("b2", "boot-b2", {"orders:0": 8}, {"orders:0": 8})
            restarted.heartbeat("b1", "boot-b1", {"orders:0": 8}, {"orders:0": 8})
            self.assertIsNone(restarted.route("orders", 0))

            restarted.heartbeat("b3", "boot-b3", {"orders:0": 10}, {"orders:0": 10})
            route = restarted.route("orders", 0)
            self.assertEqual("b3", route["leader"])
            self.assertEqual(1, route["epoch"])


class ControllerHTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        self.state = ControllerState(CONFIG, Path(self.directory.name), heartbeat_timeout=0.05)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_for(self.state))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.connection = HTTPConnection("127.0.0.1", self.server.server_port, timeout=2)

    def tearDown(self) -> None:
        self.connection.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.directory.cleanup()

    def request(self, method: str, path: str, body: dict | None = None):
        encoded = None if body is None else json.dumps(body)
        headers = {} if encoded is None else {"Content-Type": "application/json"}
        self.connection.request(method, path, encoded, headers)
        response = self.connection.getresponse()
        return response.status, json.loads(response.read())

    def test_http_api_and_unavailable_route(self) -> None:
        status, _ = self.request("GET", "/route?topic=orders&partition=0")
        self.assertEqual(503, status)

        status, body = self.request(
            "POST",
            "/heartbeat",
            {
                "broker_id": "b1",
                "boot_id": "boot-b1",
                "committed": {"orders:0": 4},
                "safe": {"orders:0": 4},
            },
        )
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

    def test_heartbeat_requires_explicit_safe_offsets(self) -> None:
        status, body = self.request(
            "POST",
            "/heartbeat",
            {
                "broker_id": "b1",
                "boot_id": "boot-b1",
                "committed": {"orders:0": 4},
            },
        )

        self.assertEqual(400, status)
        self.assertIn("safe", body["error"])

    def test_heartbeat_requires_boot_id(self) -> None:
        status, body = self.request(
            "POST",
            "/heartbeat",
            {
                "broker_id": "b1",
                "committed": {"orders:0": 4},
                "safe": {"orders:0": 4},
            },
        )

        self.assertEqual(400, status)
        self.assertIn("boot_id", body["error"])


if __name__ == "__main__":
    unittest.main()
