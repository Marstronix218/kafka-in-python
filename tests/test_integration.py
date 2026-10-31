"""End-to-end tests for the command-line event log cluster.

Each test starts a controller and three broker subprocesses on free local ports.
The tests deliberately use the public CLI and broker HTTP endpoints rather than
importing service state, so process boundaries, persistence, and routing are
covered together.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
TOPIC = "events"


def reserve_ports(count: int) -> list[int]:
    """Ask the OS for currently free ports while avoiding duplicates."""
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def request_json(url: str, method: str = "GET", body: dict | None = None) -> dict:
    encoded = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(
        url,
        data=encoded,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=1) as response:
        return json.loads(response.read())


class SubprocessClusterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        self.work_dir = Path(self.temporary_directory.name)
        controller_port, *broker_ports = reserve_ports(4)
        self.controller_url = f"http://127.0.0.1:{controller_port}"
        self.broker_urls = {
            f"b{index}": f"http://127.0.0.1:{port}"
            for index, port in enumerate(broker_ports, start=1)
        }
        self.config_path = self.work_dir / "config.json"
        self.config_path.write_text(
            json.dumps(
                {
                    "brokers": self.broker_urls,
                    "topics": {
                        TOPIC: {"partitions": 1, "replication_factor": 3}
                    },
                }
            ),
            encoding="utf-8",
        )
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self._start_cluster()

    def tearDown(self) -> None:
        for process in reversed(list(self.processes.values())):
            if process.poll() is None:
                process.terminate()
        for process in reversed(list(self.processes.values())):
            if process.poll() is None:
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
        self.temporary_directory.cleanup()

    def _spawn(self, name: str, *arguments: str) -> None:
        self.processes[name] = subprocess.Popen(
            [sys.executable, "-m", "eventlog", *arguments],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def _start_cluster(self) -> None:
        controller_port = self.controller_url.rsplit(":", 1)[1]
        self._spawn(
            "controller",
            "controller",
            "--config",
            str(self.config_path),
            "--port",
            controller_port,
            "--heartbeat-timeout",
            "0.8",
        )
        self._wait_for(lambda: request_json(self.controller_url + "/health"))

        for broker_id in self.broker_urls:
            self._start_broker(broker_id)
        for broker_id, broker_url in self.broker_urls.items():
            self._wait_for(
                lambda url=broker_url: request_json(url + "/health"),
                process_name=broker_id,
            )
        self._wait_for(
            lambda: len(request_json(self.controller_url + "/health")["alive_brokers"])
            == 3
        )

    def _start_broker(self, broker_id: str) -> None:
        broker_port = self.broker_urls[broker_id].rsplit(":", 1)[1]
        self._spawn(
            broker_id,
            "broker",
            "--config",
            str(self.config_path),
            "--id",
            broker_id,
            "--controller",
            self.controller_url,
            "--port",
            broker_port,
            "--data-dir",
            str(self.work_dir / broker_id),
        )

    def _wait_for(self, operation, process_name: str | None = None, timeout: float = 8):
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if process_name is not None:
                return_code = self.processes[process_name].poll()
                if return_code is not None:
                    self.fail(f"{process_name} exited during startup with {return_code}")
            try:
                result = operation()
                if result:
                    return result
            except (HTTPError, URLError, ConnectionError, TimeoutError) as exc:
                last_error = exc
            time.sleep(0.05)
        self.fail(f"condition was not met within {timeout} seconds: {last_error}")

    def _cli(self, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "eventlog", *arguments],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=15,
            check=check,
        )

    def _produce_command(self, value: object, acks: int, retries: int | None = None) -> list[str]:
        arguments = [
            "produce",
            "--controller", self.controller_url,
            "--topic", TOPIC,
            "--partition", "0",
            "--value", json.dumps(value),
            "--acks", str(acks),
        ]
        if retries is not None:
            arguments += ["--retries", str(retries)]
        return arguments

    def _produce(self, value: object, *, acks: int = 2) -> dict:
        return json.loads(self._cli(*self._produce_command(value, acks)).stdout)

    def _produce_without_retries(
        self, value: object, *, acks: int
    ) -> subprocess.CompletedProcess[str]:
        """Run produce once and return the finished process, even if it failed."""
        return self._cli(*self._produce_command(value, acks, retries=0), check=False)

    def _consume(self, consumer_id: str, *arguments: str) -> list[dict]:
        result = self._cli(
            "consume",
            "--controller",
            self.controller_url,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--consumer-id",
            consumer_id,
            "--state-dir",
            str(self.work_dir / "consumers"),
            *arguments,
        )
        return [json.loads(line) for line in result.stdout.splitlines()]

    def _stop_broker(self, broker_id: str) -> None:
        process = self.processes[broker_id]
        process.terminate()
        process.wait(timeout=3)

    def _status(self, broker_id: str) -> dict:
        return request_json(
            self.broker_urls[broker_id]
            + f"/status?topic={TOPIC}&partition=0"
        )

    def _route(self) -> dict:
        return request_json(
            self.controller_url + f"/route?topic={TOPIC}&partition=0"
        )

    def _stop_followers(self) -> dict:
        """Stop every replica except the leader, and return the route from before."""
        route = self._route()
        for broker_id in route["replicas"]:
            if broker_id != route["leader"]:
                self._stop_broker(broker_id)
        return route

    def _route_after_failover(self, original_leader: str) -> dict | None:
        route = self._route()
        return route if route["leader"] != original_leader else None

    def test_produce_and_fetch_preserve_partition_order(self) -> None:
        for index in range(5):
            acknowledgement = self._produce({"sequence": index})
            self.assertEqual(index, acknowledgement["offset"])

        records = self._consume("ordering")

        self.assertEqual(list(range(5)), [record["value"]["sequence"] for record in records])

    def test_restarted_consumer_resumes_from_committed_offset(self) -> None:
        for index in range(3):
            self._produce({"sequence": index})
        first_run = self._consume("durable", "--limit", "2", "--commit")

        restarted_run = self._consume("durable")

        self.assertEqual([0, 1], [record["value"]["sequence"] for record in first_run])
        self.assertEqual([2], [record["value"]["sequence"] for record in restarted_run])

    def test_seek_replays_records_from_requested_offset(self) -> None:
        for index in range(3):
            self._produce({"sequence": index})
        self._consume("replay", "--limit", "2", "--commit")

        replayed = self._consume("replay", "--seek", "0")

        self.assertEqual([0, 1, 2], [record["value"]["sequence"] for record in replayed])

    def test_acknowledged_records_survive_leader_failover(self) -> None:
        for index in range(4):
            self._produce({"sequence": index})
        original_leader = self._route()["leader"]
        time.sleep(0.6)

        self._stop_broker(original_leader)
        new_route = self._wait_for(
            lambda: self._route_after_failover(original_leader),
            timeout=5,
        )
        records = self._consume("after-failover")

        self.assertNotEqual(original_leader, new_route["leader"])
        self.assertEqual(list(range(4)), [record["value"]["sequence"] for record in records])

    def test_produce_is_rejected_when_acknowledgement_quorum_is_unavailable(self) -> None:
        self._stop_followers()

        result = self._produce_without_retries({"cannot": "commit"}, acks=2)

        self.assertNotEqual(0, result.returncode)
        self.assertIn("insufficient replicas", result.stderr)

    def test_acks_three_write_is_stored_and_committed_on_all_brokers(self) -> None:
        acknowledgement = self._produce({"sequence": 0}, acks=3)

        self.assertEqual(0, acknowledgement["offset"])
        for broker_id in self.broker_urls:
            self.assertEqual(
                {"last_offset": 0, "committed_offset": 0}, self._status(broker_id)
            )

    def test_acks_one_succeeds_without_followers_while_acks_two_fails(self) -> None:
        route = self._stop_followers()

        acknowledgement = self._produce({"sequence": 0}, acks=1)
        rejected = self._produce_without_retries({"sequence": 1}, acks=2)

        self.assertEqual(0, acknowledgement["offset"])
        self.assertEqual(0, self._status(route["leader"])["committed_offset"])
        self.assertNotEqual(0, rejected.returncode)
        self.assertIn("insufficient replicas", rejected.stderr)


if __name__ == "__main__":
    unittest.main()
