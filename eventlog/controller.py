"""Cluster metadata, heartbeat tracking, and partition leader election."""

from __future__ import annotations

import json
import os
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


class ControllerState:
    """Thread-safe controller state for a statically assigned broker cluster."""

    def __init__(
        self,
        config: dict[str, Any],
        state_dir: Path,
        heartbeat_timeout: float = 2.0,
    ) -> None:
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be positive")

        self.config = config
        self.brokers: dict[str, str] = dict(config.get("brokers", {}))
        self.topics: dict[str, dict[str, Any]] = dict(config.get("topics", {}))
        if not self.brokers:
            raise ValueError("config must contain at least one broker")

        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.heartbeat_timeout = float(heartbeat_timeout)
        self._state_path = self.state_dir / "controller-state.json"
        self._lock = threading.RLock()
        self._heartbeats: dict[str, float] = {}
        self._boot_ids: dict[str, str] = {}
        self._committed: dict[str, dict[str, int]] = {}
        self._safe: dict[str, dict[str, int]] = {}
        self._max_safe: dict[str, int] = {}
        self._leaders: dict[str, str] = {}
        # Trust is deliberately process-local. A controller restart must
        # re-establish that its persisted leader has the persisted safe data.
        self._trusted_leaders: set[str] = set()
        self._epochs: dict[str, int] = {}
        self._load()

    @staticmethod
    def _key(topic: str, partition: int) -> str:
        return f"{topic}:{partition}"

    def replicas_for(self, topic: str, partition: int) -> list[str]:
        topic_config = self.topics.get(topic)
        if topic_config is None:
            raise KeyError(f"unknown topic: {topic}")

        partitions = int(topic_config["partitions"])
        if partition < 0 or partition >= partitions:
            raise KeyError(f"unknown partition: {topic}:{partition}")

        broker_ids = list(self.brokers)
        replication_factor = min(int(topic_config["replication_factor"]), len(broker_ids))
        start = partition % len(broker_ids)
        return [broker_ids[(start + offset) % len(broker_ids)] for offset in range(replication_factor)]

    @staticmethod
    def _offsets(offsets: dict[str, int], field: str) -> dict[str, int]:
        clean: dict[str, int] = {}
        for partition_key, offset in offsets.items():
            if not isinstance(partition_key, str) or isinstance(offset, bool) or not isinstance(offset, int) or offset < -1:
                raise ValueError(f"{field} offsets must map partition strings to integers")
            clean[partition_key] = offset
        return clean

    def heartbeat(
        self,
        broker_id: str,
        boot_id: str,
        committed: dict[str, int],
        safe: dict[str, int],
    ) -> None:
        if broker_id not in self.brokers:
            raise KeyError(f"unknown broker: {broker_id}")
        if not isinstance(boot_id, str) or not boot_id:
            raise ValueError("boot_id must be a non-empty string")
        clean_committed = self._offsets(committed, "committed")
        clean_safe = self._offsets(safe, "safe")
        if any(offset > clean_committed.get(key, -1) for key, offset in clean_safe.items()):
            raise ValueError("safe offset cannot exceed committed offset")

        with self._lock:
            previous_boot_id = self._boot_ids.get(broker_id)
            if previous_boot_id is not None and previous_boot_id != boot_id:
                for partition_key, leader in self._leaders.items():
                    if leader == broker_id:
                        self._trusted_leaders.discard(partition_key)
            self._boot_ids[broker_id] = boot_id
            self._heartbeats[broker_id] = time.monotonic()
            self._committed[broker_id] = clean_committed
            self._safe[broker_id] = clean_safe
            watermark_advanced = False
            for partition_key, offset in clean_safe.items():
                if offset > self._max_safe.get(partition_key, -1):
                    self._max_safe[partition_key] = offset
                    watermark_advanced = True
            if watermark_advanced:
                self._persist()

    def _alive(self, broker_id: str, now: float) -> bool:
        last_seen = self._heartbeats.get(broker_id)
        return last_seen is not None and now - last_seen <= self.heartbeat_timeout

    def route(self, topic: str, partition: int) -> dict[str, Any] | None:
        replicas = self.replicas_for(topic, partition)
        key = self._key(topic, partition)

        with self._lock:
            now = time.monotonic()
            alive = [broker_id for broker_id in replicas if self._alive(broker_id, now)]
            if not alive:
                return None

            previous = self._leaders.get(key)
            if previous is not None and previous not in alive:
                self._trusted_leaders.discard(key)
            safe_watermark = self._max_safe.get(key, -1)
            eligible = [
                broker_id
                for broker_id in alive
                if self._safe.get(broker_id, {}).get(key, -1) >= safe_watermark
            ]
            if previous in alive and key in self._trusted_leaders:
                leader = previous
            elif previous in eligible:
                leader = previous
                self._trusted_leaders.add(key)
            elif previous is None:
                # Initial elections honor the preferred replica assignment.
                if not eligible:
                    return None
                leader = eligible[0]
                self._leaders[key] = leader
                self._trusted_leaders.add(key)
                self._epochs.setdefault(key, 0)
                self._persist()
            else:
                if not eligible:
                    return None
                # max() retains the first assigned replica when offsets tie.
                leader = max(
                    eligible,
                    key=lambda broker_id: self._committed.get(broker_id, {}).get(key, -1),
                )
                self._leaders[key] = leader
                self._trusted_leaders.add(key)
                self._epochs[key] = self._epochs.get(key, 0) + 1
                self._persist()

            return {
                "leader": leader,
                "epoch": self._epochs.get(key, 0),
                "replicas": replicas,
                "brokers": dict(self.brokers),
            }

    def metadata(self) -> dict[str, Any]:
        return {"topics": self.topics, "brokers": dict(self.brokers)}

    def health(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            alive = [broker_id for broker_id in self.brokers if self._alive(broker_id, now)]
        return {"status": "ok", "alive_brokers": alive}

    def _load(self) -> None:
        if not self._state_path.exists():
            return
        with self._state_path.open(encoding="utf-8") as state_file:
            saved = json.load(state_file)
        leaders = saved.get("leaders", {})
        epochs = saved.get("epochs", {})
        max_safe = saved.get("max_safe", {})
        if not isinstance(leaders, dict) or not isinstance(epochs, dict) or not isinstance(max_safe, dict):
            raise ValueError("invalid persisted controller state")
        self._leaders = {str(key): str(value) for key, value in leaders.items()}
        self._epochs = {str(key): int(value) for key, value in epochs.items()}
        self._max_safe = {str(key): int(value) for key, value in max_safe.items()}

    def _persist(self) -> None:
        temporary = self._state_path.with_suffix(".tmp")
        payload = {
            "leaders": self._leaders,
            "epochs": self._epochs,
            "max_safe": self._max_safe,
        }
        with temporary.open("w", encoding="utf-8") as state_file:
            json.dump(payload, state_file, sort_keys=True)
            state_file.flush()
            os.fsync(state_file.fileno())
        os.replace(temporary, self._state_path)


def _handler_for(state: ControllerState) -> type[BaseHTTPRequestHandler]:
    class ControllerHandler(BaseHTTPRequestHandler):
        def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            if urlparse(self.path).path != "/heartbeat":
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length))
                if not isinstance(body, dict):
                    raise ValueError("request body must be an object")
                broker_id = body["broker_id"]
                boot_id = body["boot_id"]
                committed = body.get("committed", {})
                safe = body["safe"]
                if (
                    not isinstance(broker_id, str)
                    or not isinstance(boot_id, str)
                    or not boot_id
                    or not isinstance(committed, dict)
                    or not isinstance(safe, dict)
                ):
                    raise ValueError("invalid heartbeat")
                state.heartbeat(broker_id, boot_id, committed, safe)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
                return
            self._json(HTTPStatus.OK, {"status": "ok"})

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            parsed = urlparse(self.path)
            if parsed.path == "/metadata":
                self._json(HTTPStatus.OK, state.metadata())
                return
            if parsed.path == "/health":
                self._json(HTTPStatus.OK, state.health())
                return
            if parsed.path != "/route":
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return

            try:
                query = parse_qs(parsed.query)
                topic = query["topic"][0]
                partition = int(query["partition"][0])
                route = state.route(topic, partition)
            except (KeyError, TypeError, ValueError) as error:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
                return
            if route is None:
                self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "no replica is alive"})
                return
            self._json(HTTPStatus.OK, route)

        def log_message(self, format: str, *args: Any) -> None:
            return

    return ControllerHandler


def run_controller(
    config: dict[str, Any],
    host: str,
    port: int,
    state_dir: Path,
    heartbeat_timeout: float = 2.0,
) -> None:
    """Run the controller HTTP server until it is interrupted."""

    state = ControllerState(config, state_dir, heartbeat_timeout)
    server = ThreadingHTTPServer((host, port), _handler_for(state))
    try:
        server.serve_forever()
    finally:
        server.server_close()
