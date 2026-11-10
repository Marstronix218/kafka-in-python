"""Client side of the event log: a cluster client that finds partition leaders through
the controller, an idempotent producer, and a single consumer that keeps its own
offsets on disk.

Kafka references for this module are listed in REFERENCES.md."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

from .common import (DEFAULT_ACKS, NOT_LEADER, UNAVAILABLE, APIError,
                     partition_for_key, replica_ids, request_json)


PRODUCE_TIMEOUT_SECONDS = 3.0
FETCH_ATTEMPTS = 3
STATUS_TIMEOUT_SECONDS = 0.5


class ClusterClient:
    """Kafka reference: Metadata.currentLeader (Java client), kafka-python
    ClusterMetadata.leader_for_partition, confluent-kafka-python PartitionMetadata."""

    def __init__(self, controller_url: str):
        self.controller_url = controller_url.rstrip("/")
        self._lock = threading.Lock()
        self._producer: Producer | None = None

    def metadata(self) -> dict:
        return request_json(self.controller_url + "/metadata")

    def route(self, topic: str, partition: int) -> dict:
        query = urlencode({"topic": topic, "partition": partition})
        return request_json(self.controller_url + "/route?" + query)

    def produce(self, topic: str, value: object, key: str | None = None,
                partition: int | None = None, acks: int = DEFAULT_ACKS, retries: int = 3,
                producer_id: str | None = None, sequence: int | None = None) -> dict:
        """Write one record and return the leader's reply (topic, partition, offset, acks,
        producer_id, sequence, duplicate).

        Without producer_id the record goes through this client's own Producer, so all
        such calls share one producer id and are numbered per partition. With
        producer_id, exactly that id and sequence are sent. Every retry resends the
        same pair, so a retry after a lost reply gets the stored offset back with
        duplicate=true instead of storing a second copy.
        Raises APIError when the last attempt fails.
        """
        if producer_id is None:
            if sequence is not None:
                raise ValueError("a sequence needs a producer_id")
            with self._lock:
                if self._producer is None:
                    self._producer = Producer(self)
            return self._producer.produce(topic, value, key, partition, acks, retries)
        if sequence is None:
            raise ValueError("a producer_id needs a sequence")
        if partition is None:
            partition = partition_for_key(key, self._partition_count(topic), value)
        body = {"topic": topic, "partition": partition, "key": key, "value": value, "acks": acks,
                "producer_id": producer_id, "sequence": sequence}

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

    def faults(self, broker_id: str) -> dict:
        """Return a broker's fault rules as {"broker_id": ..., "rules": [...]}."""
        return request_json(self._broker_url(broker_id) + "/faults")

    def set_faults(self, broker_id: str, rules: list[dict], seed: int | None = None) -> dict:
        """Replace a broker's fault rules (an empty list clears them) and return the new
        table. A seed makes the broker's probability rolls repeatable."""
        body: dict = {"rules": rules}
        if seed is not None:
            body["seed"] = seed
        return request_json(self._broker_url(broker_id) + "/faults", "POST", body)

    def cluster_status(self) -> dict:
        """The controller's view (alive brokers, partition leaders, leader epochs, ISRs)
        next to each replica's own log, for inspecting the cluster by hand. A replica
        that can't be reached shows as None. A partition the controller can't route
        has no leader, no epoch, and an empty ISR. "faults" maps each broker to its
        fault rules, or None if it can't be reached."""
        metadata = self.metadata()
        brokers = metadata["brokers"]
        alive = request_json(self.controller_url + "/health")["alive_brokers"]
        partitions = []
        for topic, spec in metadata["topics"].items():
            for partition in range(spec["partitions"]):
                try:
                    route = self.route(topic, partition)
                except APIError:
                    route = {"leader": None, "leader_epoch": None, "isr": []}
                logs = {}
                for broker_id in replica_ids(list(brokers), partition,
                                             spec["replication_factor"]):
                    query = urlencode({"topic": topic, "partition": partition})
                    try:
                        logs[broker_id] = request_json(brokers[broker_id] + "/status?" + query,
                                                       timeout=STATUS_TIMEOUT_SECONDS)
                    except APIError:
                        logs[broker_id] = None
                partitions.append({"topic": topic, "partition": partition,
                                   "leader": route["leader"],
                                   "leader_epoch": route["leader_epoch"],
                                   "isr": route["isr"], "replicas": logs})
        faults = {}
        for broker_id, url in brokers.items():
            try:
                faults[broker_id] = request_json(url + "/faults",
                                                 timeout=STATUS_TIMEOUT_SECONDS)["rules"]
            except APIError:
                faults[broker_id] = None
        return {"brokers": brokers, "alive_brokers": alive, "partitions": partitions,
                "faults": faults}

    def _partition_count(self, topic: str) -> int:
        topics = self.metadata()["topics"]
        if topic not in topics:
            raise ValueError(f"unknown topic {topic!r} (known: {', '.join(topics)})")
        return topics[topic]["partitions"]

    def _leader_url(self, topic: str, partition: int) -> str:
        route = self.route(topic, partition)
        return route["brokers"][route["leader"]]

    def _broker_url(self, broker_id: str) -> str:
        brokers = self.metadata()["brokers"]
        if broker_id not in brokers:
            raise ValueError(f"unknown broker {broker_id!r} (known: {', '.join(brokers)})")
        return brokers[broker_id]


class Producer:
    """Writes records under one producer id, numbering them per (topic, partition).

    Each record gets the partition's next sequence, and retries resend that same
    sequence, so a record is stored at most once even when replies are lost or the
    leader changes. Safe to share between threads. Writes to one partition run one
    at a time, so the leader always sees a sequence after the one before it. Writes
    to different partitions run in parallel.

    producer_id is the id every partition starts with. A partition moves to a new id
    after a failed write (see produce), so read the id of a record from the reply.

    Kafka reference: TransactionManager (producer id and per-partition sequence numbers).
    """

    def __init__(self, client: ClusterClient, producer_id: str | None = None,
                 first_sequence: int = 0):
        if type(first_sequence) is not int or first_sequence < 0:
            raise ValueError(f"first_sequence must be a non-negative integer, "
                             f"got {first_sequence!r}")
        self.client = client
        self.producer_id = producer_id or uuid.uuid4().hex
        self.first_sequence = first_sequence
        self._lock = threading.Lock()
        self._partitions: dict[tuple[str, int], dict] = {}
        self._partition_counts: dict[str, int] = {}

    def produce(self, topic: str, value: object, key: str | None = None,
                partition: int | None = None, acks: int = DEFAULT_ACKS, retries: int = 3) -> dict:
        """Write one record and return the leader's reply, as ClusterClient.produce does.

        Every attempt for one record carries the same producer id and sequence. If it
        raises, the record may or may not be stored, and an attempt may still be queued
        at the leader and stored later. Neither that id and sequence nor the next
        sequence can then be given to another record, so the next record on this
        partition goes out under a new producer id at sequence 0, which the leader
        accepts as a producer it has not seen.
        Kafka reference: TransactionManager (bumps the producer epoch in this case).
        """
        if partition is None:
            partition = partition_for_key(key, self._partition_count(topic), value)
        state = self._partition_state(topic, partition)
        with state["lock"]:
            try:
                reply = self.client.produce(topic, value, key, partition, acks, retries,
                                            state["producer_id"], state["next"])
            except BaseException:
                state.update(producer_id=uuid.uuid4().hex, next=0)
                raise
            state["next"] += 1
            return reply

    def _partition_state(self, topic: str, partition: int) -> dict:
        with self._lock:
            return self._partitions.setdefault(
                (topic, partition),
                {"lock": threading.Lock(), "producer_id": self.producer_id,
                 "next": self.first_sequence})

    def _partition_count(self, topic: str) -> int:
        # Topics are fixed in the config, so one metadata request per topic is enough.
        with self._lock:
            count = self._partition_counts.get(topic)
        if count is None:
            count = self.client._partition_count(topic)
            with self._lock:
                self._partition_counts[topic] = count
        return count


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
