"""Cluster metadata, heartbeat tracking, and partition leader routing."""

from __future__ import annotations

import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


class ControllerState:
    """Thread-safe controller state for a statically assigned broker cluster."""

    def __init__(self, config: dict[str, Any], heartbeat_timeout: float = 2.0) -> None:
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be positive")

        self.config = config
        self.brokers: dict[str, str] = dict(config.get("brokers", {}))
        self.topics: dict[str, dict[str, Any]] = dict(config.get("topics", {}))
        if not self.brokers:
            raise ValueError("config must contain at least one broker")

        self.heartbeat_timeout = float(heartbeat_timeout)
        self._lock = threading.RLock()
        self._heartbeats: dict[str, float] = {}
        self._leaders: dict[str, str] = {}

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

    def heartbeat(self, broker_id: str) -> None:
        if broker_id not in self.brokers:
            raise KeyError(f"unknown broker: {broker_id}")

        with self._lock:
            self._heartbeats[broker_id] = time.monotonic()

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

            # Keep a live leader so a recovered preferred replica does not
            # preempt it; otherwise fail over in assignment order.
            leader = self._leaders.get(key)
            if leader not in alive:
                leader = alive[0]
                self._leaders[key] = leader

            return {
                "leader": leader,
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
                if not isinstance(broker_id, str):
                    raise ValueError("broker_id must be a string")
                state.heartbeat(broker_id)
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
    heartbeat_timeout: float = 2.0,
) -> None:
    """Run the controller HTTP server until it is interrupted."""

    state = ControllerState(config, heartbeat_timeout)
    server = ThreadingHTTPServer((host, port), _handler_for(state))
    try:
        server.serve_forever()
    finally:
        server.server_close()
