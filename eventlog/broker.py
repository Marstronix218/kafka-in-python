"""A broker process. It stores the partitions assigned to it, leads some of them,
and copies each new record to the followers before acknowledging the write."""

from __future__ import annotations

import json
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from .common import (DEFAULT_ACKS, MAX_RECORD_BYTES, NOT_LEADER, UNAVAILABLE, APIError,
                     replica_ids, request_json, validate_config)
from .storage import PartitionLog


HEARTBEAT_INTERVAL_SECONDS = 0.5
PEER_TIMEOUT_SECONDS = 1.0
MAX_FETCH_LIMIT = 1000
# A request body is one record plus the topic, partition and other fields around it.
MAX_REQUEST_BYTES = MAX_RECORD_BYTES + 4096


class BrokerState:
    def __init__(self, broker_id: str, config: dict, controller_url: str, data_dir: str | Path):
        self.config = validate_config(config)
        if broker_id not in config["brokers"]:
            known = ", ".join(config["brokers"])
            raise ValueError(f"broker id {broker_id!r} is not in the config (known: {known})")
        self.broker_id = broker_id
        self.controller_url = controller_url.rstrip("/")
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.stop_event = threading.Event()

        # Only partitions this broker is a replica of get a log and a lock.
        self.logs: dict[tuple[str, int], PartitionLog] = {}
        self.locks: dict[tuple[str, int], threading.RLock] = {}
        for topic, spec in config["topics"].items():
            for partition in range(spec["partitions"]):
                if broker_id in self._replicas(topic, partition):
                    self.logs[(topic, partition)] = PartitionLog(self.data_dir, topic, partition)
                    self.locks[(topic, partition)] = threading.RLock()

    def _replicas(self, topic: str, partition: int) -> list[str]:
        replication_factor = self.config["topics"][topic]["replication_factor"]
        return replica_ids(list(self.config["brokers"]), partition, replication_factor)

    def _lock_for(self, topic: str, partition: int) -> threading.RLock:
        # A partition this broker doesn't hold has no lock. Handing back a throwaway
        # one lets the caller reach its own checks, which reject the request: 409 from
        # _require_leader or 404 from self.log(), whichever runs first.
        return self.locks.get((topic, partition), threading.RLock())

    def log(self, topic: str, partition: int) -> PartitionLog:
        if (topic, partition) not in self.logs:
            raise APIError(HTTPStatus.NOT_FOUND, f"partition {topic}:{partition} is not stored "
                                                 f"on broker {self.broker_id}")
        return self.logs[(topic, partition)]

    def route(self, topic: str, partition: int) -> dict:
        query = urlencode({"topic": topic, "partition": partition})
        return request_json(self.controller_url + "/route?" + query)

    def _require_leader(self, topic: str, partition: int) -> dict:
        route = self.route(topic, partition)
        if route["leader"] != self.broker_id:
            raise APIError(NOT_LEADER, f"broker {self.broker_id} is not the leader for "
                                       f"{topic}:{partition}; the leader is {route['leader']}")
        return route

    def _require_sent_by_leader(self, body: dict, topic: str, partition: int) -> None:
        # A broker that has just lost leadership may still be sending. Only the broker
        # the controller names right now is allowed to write to followers.
        if body.get("leader") != self.route(topic, partition)["leader"]:
            raise APIError(NOT_LEADER, "request is not from the current leader")

    def heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                request_json(self.controller_url + "/heartbeat", "POST",
                             {"broker_id": self.broker_id})
            except (APIError, OSError):
                # Nothing to do here. Missed heartbeats are how the controller finds out.
                pass
            self.stop_event.wait(HEARTBEAT_INTERVAL_SECONDS)

    def produce(self, body: dict) -> dict:
        topic, partition, key, acks = self._validate_produce(body)
        with self._lock_for(topic, partition):
            route = self._require_leader(topic, partition)
            log = self.log(topic, partition)
            # Drop an uncommitted record left behind by an earlier failed write.
            log.truncate_from(log.committed_offset + 1)
            record = {"offset": log.last_offset + 1, "key": key, "value": body["value"],
                      "timestamp": time.time()}
            size = len(json.dumps(record, ensure_ascii=False).encode())
            if size > MAX_RECORD_BYTES:
                raise APIError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                               f"record is {size} bytes; the limit is {MAX_RECORD_BYTES} (1 MiB)")
            log.append(record)

            followers = [peer for peer in route["replicas"] if peer != self.broker_id]
            stored_on = []
            for peer in followers:
                if self._send_to_peer(peer, "/replicate", topic, partition, record=record):
                    stored_on.append(peer)
            # The leader's own copy counts toward acks, hence the 1.
            if 1 + len(stored_on) < acks:
                raise APIError(UNAVAILABLE, f"insufficient replicas for requested acknowledgments: "
                                            f"acks={acks}, but only {1 + len(stored_on)} of "
                                            f"{len(route['replicas'])} replicas stored the record")

            log.commit(record["offset"])
            # Best effort. A follower that misses this still holds the record, it just
            # doesn't know the record is committed (see "Current limitations" in the README).
            for peer in stored_on:
                self._send_to_peer(peer, "/commit", topic, partition, offset=record["offset"])
            return {"topic": topic, "partition": partition, "offset": record["offset"],
                    "acks": acks}

    def _validate_produce(self, body: dict) -> tuple[str, int, str | None, int]:
        topic = body.get("topic")
        partition = body.get("partition")
        if topic not in self.config["topics"]:
            known = ", ".join(self.config["topics"])
            raise APIError(HTTPStatus.BAD_REQUEST, f"unknown topic {topic!r} (known: {known})")
        if type(partition) is not int:
            raise APIError(HTTPStatus.BAD_REQUEST,
                           f"partition must be an integer, got {partition!r}")
        topic_config = self.config["topics"][topic]
        if not 0 <= partition < topic_config["partitions"]:
            last = topic_config["partitions"] - 1
            raise APIError(HTTPStatus.BAD_REQUEST, f"partition {partition} does not exist; "
                                                   f"{topic!r} has partitions 0..{last}")
        key = body.get("key")
        if key is not None and not isinstance(key, str):
            raise APIError(HTTPStatus.BAD_REQUEST,
                           f"key must be a string or null, got {type(key).__name__}")
        if "value" not in body:
            raise APIError(HTTPStatus.BAD_REQUEST, "value is required")
        acks = body.get("acks", DEFAULT_ACKS)
        if type(acks) is not int or not 1 <= acks <= topic_config["replication_factor"]:
            raise APIError(HTTPStatus.BAD_REQUEST,
                           f"acks must be between 1 and the replication factor "
                           f"({topic_config['replication_factor']}), got {acks!r}")
        return topic, partition, key, acks

    def _send_to_peer(self, peer: str, path: str, topic: str, partition: int, **fields) -> bool:
        """POST to another broker. Returns False instead of raising if it fails."""
        body = {"topic": topic, "partition": partition, "leader": self.broker_id, **fields}
        try:
            request_json(self.config["brokers"][peer] + path, "POST", body,
                         timeout=PEER_TIMEOUT_SECONDS)
            return True
        except APIError:
            return False

    def replicate(self, body: dict) -> dict:
        topic, partition = _topic_and_partition(body)
        with self._lock_for(topic, partition):
            log = self.log(topic, partition)
            self._require_sent_by_leader(body, topic, partition)
            record = body.get("record")
            if not isinstance(record, dict) or type(record.get("offset")) is not int:
                raise APIError(HTTPStatus.BAD_REQUEST,
                               "record must be a JSON object with an integer offset")
            try:
                log.append(record)
            except (ValueError, TypeError, KeyError) as exc:
                # The record doesn't fit this replica's log: a gap in offsets, or it
                # would overwrite a committed record.
                raise APIError(HTTPStatus.CONFLICT, str(exc)) from exc
            return {"offset": record["offset"]}

    def commit_replica(self, body: dict) -> dict:
        topic, partition = _topic_and_partition(body)
        with self._lock_for(topic, partition):
            log = self.log(topic, partition)
            self._require_sent_by_leader(body, topic, partition)
            try:
                log.commit(body["offset"])
            except (KeyError, ValueError, TypeError) as exc:
                raise APIError(HTTPStatus.CONFLICT, str(exc)) from exc
            return {"committed_offset": log.committed_offset}

    def fetch(self, topic: str, partition: int, offset: int, limit: int) -> dict:
        with self._lock_for(topic, partition):
            self._require_leader(topic, partition)
            if offset < 0 or not 1 <= limit <= MAX_FETCH_LIMIT:
                raise APIError(HTTPStatus.BAD_REQUEST,
                               f"offset must be non-negative and limit 1..{MAX_FETCH_LIMIT}, "
                               f"got offset={offset} limit={limit}")
            log = self.log(topic, partition)
            return {"records": log.read(offset, limit), "committed_offset": log.committed_offset}

    def status(self, topic: str, partition: int) -> dict:
        # This broker's own copy, leader or not. Used to check replication by hand and in tests.
        log = self.log(topic, partition)
        return {"last_offset": log.last_offset, "committed_offset": log.committed_offset}


def _topic_and_partition(body: dict) -> tuple[object, int]:
    topic, partition = body.get("topic"), body.get("partition")
    if type(partition) is not int:
        raise APIError(HTTPStatus.BAD_REQUEST, f"partition must be an integer, got {partition!r}")
    return topic, partition


def _handler_for(state: BrokerState) -> type[BaseHTTPRequestHandler]:
    post_routes = {
        "/produce": state.produce,
        "/replicate": state.replicate,
        "/commit": state.commit_replica,
    }

    class BrokerHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass  # don't print a line for every request

        def respond(self, code: int, data: dict) -> None:
            payload = json.dumps(data, separators=(",", ":")).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def respond_error(self, exc: Exception) -> None:
            # Anything that isn't an APIError is malformed input that got past
            # validation, like a non-numeric offset, so it's the client's fault.
            status = exc.status if isinstance(exc, APIError) else HTTPStatus.BAD_REQUEST
            self.respond(status, {"error": str(exc)})

        def read_body(self) -> dict:
            size = int(self.headers.get("Content-Length", "0"))
            if size < 0:
                raise APIError(HTTPStatus.BAD_REQUEST, f"invalid Content-Length {size}")
            if size > MAX_REQUEST_BYTES:
                raise APIError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                               f"request body is {size} bytes; the limit is {MAX_REQUEST_BYTES}")
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict):
                raise APIError(HTTPStatus.BAD_REQUEST, "request body must be a JSON object")
            return data

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)

            # parse_qs drops blank values, so "topic=" counts as missing here.
            def required(name: str) -> str:
                if name not in query:
                    raise APIError(HTTPStatus.BAD_REQUEST,
                                   f"{parsed.path} needs a {name} query parameter")
                return query[name][0]

            def integer(name: str, text: str) -> int:
                try:
                    return int(text)
                except ValueError:
                    raise APIError(HTTPStatus.BAD_REQUEST,
                                   f"{name} must be an integer, got {text!r}") from None

            try:
                if parsed.path == "/health":
                    result = {"broker_id": state.broker_id, "ok": True}
                elif parsed.path == "/fetch":
                    result = state.fetch(
                        required("topic"),
                        integer("partition", required("partition")),
                        integer("offset", query.get("offset", ["0"])[0]),
                        integer("limit", query.get("limit", ["100"])[0]))
                elif parsed.path == "/status":
                    result = state.status(required("topic"),
                                          integer("partition", required("partition")))
                else:
                    raise APIError(HTTPStatus.NOT_FOUND, f"unknown endpoint GET {parsed.path}")
                self.respond(HTTPStatus.OK, result)
            except (APIError, ValueError) as exc:
                self.respond_error(exc)

        def do_POST(self) -> None:
            try:
                body = self.read_body()
                handler = post_routes.get(self.path)
                if handler is None:
                    raise APIError(HTTPStatus.NOT_FOUND, f"unknown endpoint POST {self.path}")
                self.respond(HTTPStatus.OK, handler(body))
            except (APIError, ValueError, KeyError, TypeError) as exc:
                self.respond_error(exc)

    return BrokerHandler


def run_broker(broker_id: str, config: dict, controller_url: str, host: str, port: int,
               data_dir: str | Path) -> None:
    state = BrokerState(broker_id, config, controller_url, data_dir)
    heartbeats = threading.Thread(target=state.heartbeat_loop, daemon=True)
    heartbeats.start()
    server = ThreadingHTTPServer((host, port), _handler_for(state))
    server.daemon_threads = True
    try:
        server.serve_forever()
    finally:
        state.stop_event.set()
        server.server_close()
