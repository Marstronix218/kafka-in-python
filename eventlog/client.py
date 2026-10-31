"""Client side of the event log: a producer that finds partition leaders through
the controller, and a single consumer that keeps its own offsets on disk.

Kafka references for this module are listed in REFERENCES.md."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import urlencode

from .common import DEFAULT_ACKS, NOT_LEADER, UNAVAILABLE, APIError, partition_for_key, request_json


PRODUCE_TIMEOUT_SECONDS = 3.0
FETCH_ATTEMPTS = 3


class ClusterClient:
    """Kafka reference: Metadata.currentLeader (Java client), kafka-python
    ClusterMetadata.leader_for_partition, confluent-kafka-python PartitionMetadata."""

    def __init__(self, controller_url: str):
        self.controller_url = controller_url.rstrip("/")

    def metadata(self) -> dict:
        return request_json(self.controller_url + "/metadata")

    def route(self, topic: str, partition: int) -> dict:
        query = urlencode({"topic": topic, "partition": partition})
        return request_json(self.controller_url + "/route?" + query)

    def produce(self, topic: str, value: object, key: str | None = None,
                partition: int | None = None, acks: int = DEFAULT_ACKS, retries: int = 3) -> dict:
        if partition is None:
            partition = partition_for_key(key, self._partition_count(topic), value)
        body = {"topic": topic, "partition": partition, "key": key, "value": value, "acks": acks}

        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                url = self._leader_url(topic, partition) + "/produce"
                return request_json(url, "POST", body, timeout=PRODUCE_TIMEOUT_SECONDS)
            except APIError as exc:
                last_error = exc
                # Both errors can clear on their own: the controller promotes a new
                # leader, or a follower comes back. Anything else is the caller's problem.
                # Kafka reference: Sender retries retriable errors after Metadata.requestUpdate.
                if exc.status not in (NOT_LEADER, UNAVAILABLE) or attempt == retries:
                    raise
                time.sleep(min(0.2 * (attempt + 1), 0.8))
        raise last_error or RuntimeError("produce failed")

    def fetch(self, topic: str, partition: int, offset: int = 0, limit: int = 100) -> dict:
        query = urlencode({"topic": topic, "partition": partition,
                           "offset": offset, "limit": limit})
        last_error: Exception | None = None
        for _ in range(FETCH_ATTEMPTS):
            try:
                return request_json(self._leader_url(topic, partition) + "/fetch?" + query)
            except APIError as exc:
                last_error = exc
                # Only retry when leadership moved between asking the controller and
                # asking the broker. Reads don't wait on acks, so a 503 here means there
                # is no reachable replica to read from.
                if exc.status != NOT_LEADER:
                    raise
        raise last_error or RuntimeError("fetch failed")

    def _partition_count(self, topic: str) -> int:
        topics = self.metadata()["topics"]
        if topic not in topics:
            raise ValueError(f"unknown topic {topic!r} (known: {', '.join(topics)})")
        return topics[topic]["partitions"]

    def _leader_url(self, topic: str, partition: int) -> str:
        route = self.route(topic, partition)
        return route["brokers"][route["leader"]]


class Consumer:
    """A consumer's offsets are next-to-read positions, stored as one JSON file
    per consumer ID ({"events:0": 12, ...}) on the consumer's own disk.

    Kafka reference: KafkaConsumer.position / seek / commit (Java client,
    kafka-python, confluent-kafka-python). Kafka's committed offset is also
    "the offset of the next message that your application will read".
    """

    def __init__(self, client: ClusterClient, consumer_id: str, state_dir: str | Path):
        # The ID becomes a file name, so it must not be able to point outside state_dir.
        if not consumer_id or "/" in consumer_id or ".." in consumer_id:
            raise ValueError(f"invalid consumer id {consumer_id!r}: it is used as a file name, "
                             "so it must be non-empty and contain no '/' or '..'")
        self.client = client
        self.offsets_path = Path(state_dir) / f"{consumer_id}.json"
        self.offsets_path.parent.mkdir(parents=True, exist_ok=True)
        self.offsets = {}
        if self.offsets_path.exists():
            try:
                self.offsets = json.loads(self.offsets_path.read_text())
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"offset file {self.offsets_path} is not valid JSON: {exc}") from exc

    @staticmethod
    def _offset_key(topic: str, partition: int) -> str:
        return f"{topic}:{partition}"

    def position(self, topic: str, partition: int) -> int:
        return self.offsets.get(self._offset_key(topic, partition), 0)

    def fetch(self, topic: str, partition: int, limit: int = 100) -> list[dict]:
        start = self.position(topic, partition)
        return self.client.fetch(topic, partition, start, limit)["records"]

    def commit(self, topic: str, partition: int, next_offset: int) -> None:
        # `type(...) is int` rather than isinstance, because True and False are ints too.
        if type(next_offset) is not int:
            raise ValueError(f"offset must be an integer, got {next_offset!r}")
        current = self.position(topic, partition)
        if next_offset < current:
            raise ValueError(f"cannot commit {next_offset} for {topic}:{partition}: it is behind "
                             f"the current position {current}. Use seek to move backward.")
        # committed_offset is the last committed record, so the furthest a consumer
        # can have read up to is one past it.
        last_committed = self.client.fetch(topic, partition, next_offset, 1)["committed_offset"]
        if next_offset > last_committed + 1:
            raise ValueError(f"cannot commit {next_offset} for {topic}:{partition}: the last "
                             f"committed record is {last_committed}, so the highest valid "
                             f"offset to commit is {last_committed + 1}")
        self.offsets[self._offset_key(topic, partition)] = next_offset
        self._save()

    def seek(self, topic: str, partition: int, next_offset: int) -> None:
        if type(next_offset) is not int or next_offset < 0:
            raise ValueError(f"seek offset must be a non-negative integer, got {next_offset!r}")
        self.offsets[self._offset_key(topic, partition)] = next_offset
        self._save()

    def _save(self) -> None:
        # Write a temp file, fsync it, then rename over the old one. A crash at any
        # point leaves either the old offsets or the new ones, never half a file.
        temp_path = self.offsets_path.with_suffix(".tmp")
        with temp_path.open("w") as out:
            json.dump(self.offsets, out, sort_keys=True)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temp_path, self.offsets_path)
