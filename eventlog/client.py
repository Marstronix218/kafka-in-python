"""Producer and durable single-consumer offset helpers."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import urlencode

from .common import APIError, partition_for_key, request_json


class ClusterClient:
    def __init__(self, controller_url: str):
        self.controller_url = controller_url.rstrip("/")

    def metadata(self) -> dict:
        return request_json(self.controller_url + "/metadata")

    def route(self, topic: str, partition: int) -> dict:
        return request_json(self.controller_url + "/route?" + urlencode({"topic": topic, "partition": partition}))

    def produce(self, topic: str, value: object, key: str | None = None, partition: int | None = None,
                acks: int = 2, event_id: str | None = None, retries: int = 3) -> dict:
        if partition is None:
            metadata = self.metadata()
            try:
                count = metadata["topics"][topic]["partitions"]
            except KeyError as exc:
                raise ValueError("unknown topic") from exc
            partition = partition_for_key(key, count, value)
        body = {"topic": topic, "partition": partition, "key": key, "value": value, "acks": acks, "event_id": event_id}
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                route = self.route(topic, partition)
                return request_json(route["brokers"][route["leader"]] + "/produce", "POST", body, timeout=3.0)
            except APIError as exc:
                last_error = exc
                if exc.status not in (409, 503) or attempt == retries:
                    raise
                time.sleep(min(0.2 * (attempt + 1), 0.8))
        raise last_error or RuntimeError("produce failed")

    def fetch(self, topic: str, partition: int, offset: int = 0, limit: int = 100) -> dict:
        last_error = None
        for _ in range(3):
            try:
                route = self.route(topic, partition)
                return request_json(route["brokers"][route["leader"]] + "/fetch?" + urlencode({"topic": topic, "partition": partition, "offset": offset, "limit": limit}))
            except APIError as exc:
                last_error = exc
                if exc.status != 409:
                    raise
        raise last_error or RuntimeError("fetch failed")


class Consumer:
    """Offsets are next-to-read positions persisted on the consumer's own disk."""

    def __init__(self, client: ClusterClient, consumer_id: str, state_dir: str | Path):
        if not consumer_id or "/" in consumer_id or ".." in consumer_id:
            raise ValueError("invalid consumer id")
        self.client = client
        self.path = Path(state_dir) / f"{consumer_id}.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.offsets = json.loads(self.path.read_text()) if self.path.exists() else {}

    @staticmethod
    def _key(topic: str, partition: int) -> str:
        return f"{topic}:{partition}"

    def position(self, topic: str, partition: int) -> int:
        return self.offsets.get(self._key(topic, partition), 0)

    def fetch(self, topic: str, partition: int, limit: int = 100) -> list[dict]:
        return self.client.fetch(topic, partition, self.position(topic, partition), limit)["records"]

    def commit(self, topic: str, partition: int, next_offset: int) -> None:
        if type(next_offset) is not int or next_offset < self.position(topic, partition):
            raise ValueError("committed offset cannot move backward; use seek")
        high_watermark = self.client.fetch(topic, partition, next_offset, 1)["committed_offset"]
        if next_offset > high_watermark + 1:
            raise ValueError("cannot commit beyond the next available offset")
        self.offsets[self._key(topic, partition)] = next_offset
        self._save()

    def seek(self, topic: str, partition: int, next_offset: int) -> None:
        if type(next_offset) is not int or next_offset < 0:
            raise ValueError("seek offset must be nonnegative")
        self.offsets[self._key(topic, partition)] = next_offset
        self._save()

    def _save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        with tmp.open("w") as out:
            json.dump(self.offsets, out, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, self.path)
