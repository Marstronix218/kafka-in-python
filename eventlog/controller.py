"""Cluster metadata, heartbeat tracking, and partition leader routing.

Kafka references for this module are listed in REFERENCES.md."""

from __future__ import annotations

import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .common import replica_ids


class ISRUpdateRejected(Exception):
    """An /isr request from a broker that is not the current leader, or with a bad ISR."""


class ControllerState:
    """Thread-safe controller state for a statically assigned broker cluster."""

    def __init__(self, config: dict[str, Any], heartbeat_timeout: float = 2.0,
                 unclean_leader_election: bool = False) -> None:
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be positive")

        self.config = config
        self.brokers: dict[str, str] = dict(config.get("brokers", {}))
        self.topics: dict[str, dict[str, Any]] = dict(config.get("topics", {}))
        if not self.brokers:
            raise ValueError("config must contain at least one broker")

        self.heartbeat_timeout = float(heartbeat_timeout)
        self.unclean_leader_election = unclean_leader_election
        self._lock = threading.RLock()
        self._heartbeats: dict[str, float] = {}
        # Per partition: leader (None until the first election), leader_epoch, isr.
        # In memory only. The project assumes the controller does not fail.
        self._partitions: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _partition_key(topic: str, partition: int) -> str:
        return f"{topic}:{partition}"

    def replicas_for(self, topic: str, partition: int) -> list[str]:
        topic_config = self.topics.get(topic)
        if topic_config is None:
            known = ", ".join(self.topics)
            raise KeyError(f"unknown topic {topic!r} (known: {known})")

        partitions = int(topic_config["partitions"])
        if partition < 0 or partition >= partitions:
            raise KeyError(f"partition {partition} does not exist; {topic!r} has "
                           f"partitions 0..{partitions - 1}")

        return replica_ids(list(self.brokers), partition, int(topic_config["replication_factor"]))

    def heartbeat(self, broker_id: str) -> None:
        # Kafka reference: BrokerHeartbeatManager (session timeout, KIP-631).
        if broker_id not in self.brokers:
            raise KeyError(f"unknown broker {broker_id!r} (known: {', '.join(self.brokers)})")

        with self._lock:
            self._heartbeats[broker_id] = time.monotonic()

    def _alive(self, broker_id: str, now: float) -> bool:
        last_seen = self._heartbeats.get(broker_id)
        return last_seen is not None and now - last_seen <= self.heartbeat_timeout

    def _partition_state(self, key: str, replicas: list[str]) -> dict[str, Any]:
        # Epoch -1 means no leader was ever elected, so the first leader gets epoch 0.
        return self._partitions.setdefault(
            key, {"leader": None, "leader_epoch": -1, "isr": list(replicas)})

    def route(self, topic: str, partition: int) -> dict[str, Any] | None:
        """Return the leader, leader epoch, ISR, replicas and broker URLs, electing a
        leader first if the current one is dead. None means no replica can lead."""
        replicas = self.replicas_for(topic, partition)
        key = self._partition_key(topic, partition)

        with self._lock:
            now = time.monotonic()
            alive = [broker_id for broker_id in replicas if self._alive(broker_id, now)]
            state = self._partition_state(key, replicas)

            # Keep a live leader so a recovered preferred replica does not preempt
            # it. Otherwise elect the first alive ISR member in assignment order.
            # Every ISR member holds every committed record, so nothing acknowledged
            # is lost. With no ISR member alive the partition stays offline, unless
            # unclean election is on: then any alive replica leads and committed
            # records it lacks are lost.
            # Kafka reference: PartitionChangeBuilder.electLeader, which also elects
            # only from the ISR unless unclean leader election is enabled.
            if state["leader"] not in alive:
                candidates = [broker_id for broker_id in alive if broker_id in state["isr"]]
                if candidates:
                    state["leader"] = candidates[0]
                elif alive and self.unclean_leader_election:
                    state["leader"] = alive[0]
                    state["isr"] = [alive[0]]
                else:
                    return None
                state["leader_epoch"] += 1

            return {
                "leader": state["leader"],
                "leader_epoch": state["leader_epoch"],
                "isr": list(state["isr"]),
                "replicas": replicas,
                "brokers": dict(self.brokers),
            }

    def unavailable_reason(self, topic: str, partition: int) -> str:
        """Explain why route() returned None for a partition."""
        replicas = self.replicas_for(topic, partition)
        with self._lock:
            now = time.monotonic()
            if any(self._alive(broker_id, now) for broker_id in replicas):
                return "no in-sync replica is alive"
        return "no replica is alive"

    def alter_isr(self, topic: str, partition: int, leader: str, leader_epoch: int,
                  isr: list[str]) -> dict[str, Any]:
        """Replace a partition's ISR on behalf of its leader and return the new state.

        Raises ISRUpdateRejected if the sender is not the current leader at that epoch,
        or if the ISR leaves out the leader or names a broker that is not a replica.
        Kafka reference: AlterPartition, sent by Partition.maybeShrinkIsr and
        Partition.maybeExpandIsr.
        """
        replicas = self.replicas_for(topic, partition)
        key = self._partition_key(topic, partition)
        with self._lock:
            state = self._partitions.get(key)
            if state is None or (state["leader"], state["leader_epoch"]) != (leader, leader_epoch):
                current = "none" if state is None else (
                    f"{state['leader']} at epoch {state['leader_epoch']}")
                raise ISRUpdateRejected(f"{leader} at leader epoch {leader_epoch} is not the "
                                        f"current leader of {key} (current: {current})")
            if leader not in isr:
                raise ISRUpdateRejected(f"the ISR must contain the leader {leader}, got {isr}")
            if len(set(isr)) != len(isr) or not set(isr) <= set(replicas):
                raise ISRUpdateRejected(f"the ISR must be distinct replicas of {key} "
                                        f"({', '.join(replicas)}), got {isr}")
            state["isr"] = [broker_id for broker_id in replicas if broker_id in isr]
            return {"leader": leader, "leader_epoch": leader_epoch, "isr": list(state["isr"])}

    def metadata(self) -> dict[str, Any]:
        return {"topics": self.topics, "brokers": dict(self.brokers)}

    def health(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            alive = [broker_id for broker_id in self.brokers if self._alive(broker_id, now)]
        return {"status": "ok", "alive_brokers": alive}


def _error_message(exc: Exception) -> str:
    # str(KeyError("x")) is "'x'" with quotes, which looks broken in an error message.
    if isinstance(exc, KeyError) and exc.args:
        return str(exc.args[0])
    return str(exc)


def _isr_request(body: dict[str, Any]) -> tuple[str, int, str, int, list[str]]:
    topic, partition = body.get("topic"), body.get("partition")
    leader, leader_epoch, isr = body.get("leader"), body.get("leader_epoch"), body.get("isr")
    if not isinstance(topic, str) or type(partition) is not int:
        raise ValueError("/isr needs a string topic and an integer partition")
    if not isinstance(leader, str) or type(leader_epoch) is not int:
        raise ValueError("/isr needs a string leader and an integer leader_epoch")
    if not isinstance(isr, list) or not all(isinstance(broker_id, str) for broker_id in isr):
        raise ValueError("isr must be a list of broker ids")
    return topic, partition, leader, leader_epoch, isr


def _handler_for(state: ControllerState) -> type[BaseHTTPRequestHandler]:
    class ControllerHandler(BaseHTTPRequestHandler):
        def respond(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            path = urlparse(self.path).path
            if path not in ("/heartbeat", "/isr"):
                self.respond(HTTPStatus.NOT_FOUND, {"error": f"unknown endpoint POST {path}"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length))
                if not isinstance(body, dict):
                    raise ValueError("request body must be an object")
                if path == "/isr":
                    result = state.alter_isr(*_isr_request(body))
                else:
                    if "broker_id" not in body:
                        raise ValueError("request body needs a broker_id")
                    broker_id = body["broker_id"]
                    if not isinstance(broker_id, str):
                        raise ValueError("broker_id must be a string")
                    state.heartbeat(broker_id)
                    result = {"status": "ok"}
            except ISRUpdateRejected as exc:
                self.respond(HTTPStatus.CONFLICT, {"error": str(exc)})
                return
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                self.respond(HTTPStatus.BAD_REQUEST, {"error": _error_message(exc)})
                return
            self.respond(HTTPStatus.OK, result)

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            parsed = urlparse(self.path)
            if parsed.path == "/metadata":
                self.respond(HTTPStatus.OK, state.metadata())
                return
            if parsed.path == "/health":
                self.respond(HTTPStatus.OK, state.health())
                return
            if parsed.path != "/route":
                self.respond(HTTPStatus.NOT_FOUND, {"error": f"unknown endpoint GET {parsed.path}"})
                return

            try:
                query = parse_qs(parsed.query)
                if "topic" not in query or "partition" not in query:
                    raise ValueError("/route needs topic and partition query parameters")
                topic = query["topic"][0]
                partition = int(query["partition"][0])
                route = state.route(topic, partition)
            except (KeyError, TypeError, ValueError) as exc:
                self.respond(HTTPStatus.BAD_REQUEST, {"error": _error_message(exc)})
                return
            if route is None:
                # Tests match these exact texts.
                self.respond(HTTPStatus.SERVICE_UNAVAILABLE,
                             {"error": state.unavailable_reason(topic, partition)})
                return
            self.respond(HTTPStatus.OK, route)

        def log_message(self, format: str, *args: Any) -> None:
            return

    return ControllerHandler


def run_controller(
    config: dict[str, Any],
    host: str,
    port: int,
    heartbeat_timeout: float = 2.0,
    unclean_leader_election: bool = False,
) -> None:
    """Run the controller HTTP server until it is interrupted."""

    state = ControllerState(config, heartbeat_timeout, unclean_leader_election)
    server = ThreadingHTTPServer((host, port), _handler_for(state))
    try:
        server.serve_forever()
    finally:
        server.server_close()
