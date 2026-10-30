"""Independent broker process with leader/follower replication and acknowledgments."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .common import APIError, MAX_RECORD_BYTES, request_json, validate_config
from .storage import PartitionLog


class BrokerState:
    def __init__(self, broker_id: str, config: dict, controller_url: str, data_dir: str | Path):
        self.config = validate_config(config)
        if broker_id not in config["brokers"]:
            raise ValueError("unknown broker")
        self.broker_id = broker_id
        self.controller_url = controller_url.rstrip("/")
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs: dict[tuple[str, int], PartitionLog] = {}
        self.locks: dict[tuple[str, int], threading.RLock] = {}
        self.stop_event = threading.Event()
        for topic, spec in config["topics"].items():
            for partition in range(spec["partitions"]):
                replicas = self._replicas(topic, partition)
                if broker_id in replicas:
                    key = (topic, partition)
                    self.logs[key] = PartitionLog(self.data_dir, topic, partition)
                    self.locks[key] = threading.RLock()

    def _replicas(self, topic: str, partition: int) -> list[str]:
        ids = list(self.config["brokers"])
        rf = self.config["topics"][topic]["replication_factor"]
        return [ids[(partition + i) % len(ids)] for i in range(rf)]

    def log(self, topic: str, partition: int) -> PartitionLog:
        if (topic, partition) not in self.logs:
            raise APIError(404, "topic partition is not assigned to this broker")
        return self.logs[(topic, partition)]

    def route(self, topic: str, partition: int) -> dict:
        from urllib.parse import urlencode
        return request_json(self.controller_url + "/route?" + urlencode({"topic": topic, "partition": partition}))

    def _require_leader(self, topic: str, partition: int) -> dict:
        route = self.route(topic, partition)
        if route["leader"] != self.broker_id:
            raise APIError(409, "not leader; current leader is " + route["leader"])
        return route

    def heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                request_json(self.controller_url + "/heartbeat", "POST", {"broker_id": self.broker_id})
            except (APIError, OSError):
                pass
            self.stop_event.wait(0.5)

    def produce(self, body: dict) -> dict:
        topic = body.get("topic")
        partition = body.get("partition")
        if topic not in self.config["topics"] or type(partition) is not int:
            raise APIError(400, "valid topic and integer partition are required")
        if not 0 <= partition < self.config["topics"][topic]["partitions"]:
            raise APIError(400, "invalid partition")
        key = body.get("key")
        if key is not None and not isinstance(key, str):
            raise APIError(400, "key must be a string or null")
        if "value" not in body:
            raise APIError(400, "value is required")
        acks = body.get("acks", 2)
        if type(acks) is not int or not 1 <= acks <= self.config["topics"][topic]["replication_factor"]:
            raise APIError(400, "acks must be between 1 and replication factor")
        with self.locks[(topic, partition)]:
            route = self._require_leader(topic, partition)
            log = self.log(topic, partition)
            # Drop an uncommitted record left behind by an earlier failed write.
            log.truncate_from(log.committed_offset + 1)
            record = {"offset": log.last_offset + 1, "key": key, "value": body["value"], "timestamp": time.time()}
            if len(json.dumps(record, ensure_ascii=False).encode()) > MAX_RECORD_BYTES:
                raise APIError(413, "record exceeds 1 MiB")
            log.append(record)
            persisted = []
            for peer in route["replicas"]:
                if peer == self.broker_id:
                    continue
                try:
                    request_json(self.config["brokers"][peer] + "/replicate", "POST", {"topic": topic, "partition": partition, "leader": self.broker_id, "record": record}, timeout=1.0)
                    persisted.append(peer)
                except APIError:
                    pass
            if 1 + len(persisted) < acks:
                raise APIError(503, "insufficient replicas for requested acknowledgments")
            log.commit(record["offset"])
            for peer in persisted:
                try:
                    request_json(self.config["brokers"][peer] + "/commit", "POST", {"topic": topic, "partition": partition, "leader": self.broker_id, "offset": record["offset"]}, timeout=1.0)
                except APIError:
                    pass
            return {"topic": topic, "partition": partition, "offset": record["offset"], "acks": acks}

    def replicate(self, body: dict) -> dict:
        topic, partition = body.get("topic"), body.get("partition")
        if type(partition) is not int:
            raise APIError(400, "invalid partition")
        with self.locks.get((topic, partition), threading.RLock()):
            log = self.log(topic, partition)
            if body.get("leader") != self.route(topic, partition)["leader"]:
                raise APIError(409, "request is not from the current leader")
            record = body.get("record")
            if not isinstance(record, dict) or type(record.get("offset")) is not int:
                raise APIError(400, "invalid record")
            try:
                log.append(record)
            except (ValueError, TypeError, KeyError) as exc:
                raise APIError(409, str(exc)) from exc
            return {"offset": record["offset"]}

    def commit_replica(self, body: dict) -> dict:
        topic, partition = body.get("topic"), body.get("partition")
        if type(partition) is not int:
            raise APIError(400, "invalid partition")
        with self.locks.get((topic, partition), threading.RLock()):
            log = self.log(topic, partition)
            if body.get("leader") != self.route(topic, partition)["leader"]:
                raise APIError(409, "request is not from the current leader")
            try:
                log.commit(body["offset"])
            except (KeyError, ValueError, TypeError) as exc:
                raise APIError(409, str(exc)) from exc
            return {"committed_offset": log.committed_offset}

    def fetch(self, topic: str, partition: int, offset: int, limit: int) -> dict:
        with self.locks.get((topic, partition), threading.RLock()):
            self._require_leader(topic, partition)
            if offset < 0 or not 1 <= limit <= 1000:
                raise APIError(400, "offset must be nonnegative and limit 1..1000")
            log = self.log(topic, partition)
            return {"records": log.read(offset, limit), "committed_offset": log.committed_offset}


def make_handler(state: BrokerState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def respond(self, code: int, data: dict) -> None:
            payload = json.dumps(data, separators=(",", ":")).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def body(self) -> dict:
            size = int(self.headers.get("Content-Length", "0"))
            if size > MAX_RECORD_BYTES + 4096 or size < 0:
                raise APIError(413, "request too large")
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict):
                raise APIError(400, "JSON object required")
            return data

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            q = parse_qs(parsed.query)
            try:
                if parsed.path == "/health":
                    result = {"broker_id": state.broker_id, "ok": True}
                elif parsed.path in ("/fetch", "/status"):
                    topic = q.get("topic", [""])[0]
                    partition = int(q.get("partition", ["-1"])[0])
                    if parsed.path == "/fetch":
                        result = state.fetch(topic, partition, int(q.get("offset", ["0"])[0]), int(q.get("limit", ["100"])[0]))
                    else:
                        log = state.log(topic, partition)
                        result = {"last_offset": log.last_offset, "committed_offset": log.committed_offset}
                else:
                    raise APIError(404, "unknown endpoint")
                self.respond(200, result)
            except (APIError, ValueError) as exc:
                self.respond(exc.status if isinstance(exc, APIError) else 400, {"error": str(exc)})

        def do_POST(self) -> None:
            try:
                body = self.body()
                if self.path == "/produce":
                    result = state.produce(body)
                elif self.path == "/replicate":
                    result = state.replicate(body)
                elif self.path == "/commit":
                    result = state.commit_replica(body)
                else:
                    raise APIError(404, "unknown endpoint")
                self.respond(200, result)
            except (APIError, ValueError, KeyError, TypeError) as exc:
                self.respond(exc.status if isinstance(exc, APIError) else 400, {"error": str(exc)})

    return Handler


def run_broker(broker_id: str, config: dict, controller_url: str, host: str, port: int, data_dir: str | Path) -> None:
    state = BrokerState(broker_id, config, controller_url, data_dir)
    thread = threading.Thread(target=state.heartbeat_loop, daemon=True)
    thread.start()
    server = ThreadingHTTPServer((host, port), make_handler(state))
    server.daemon_threads = True
    try:
        server.serve_forever()
    finally:
        state.stop_event.set()
        server.server_close()
