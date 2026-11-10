"""Configuration, replica placement, and the HTTP helper shared by every process.

Kafka references for this module are listed in REFERENCES.md."""

from __future__ import annotations

import hashlib
from http import HTTPStatus
from http.client import HTTPException
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# Kafka reference: producer max.request.size, default 1048576 (ProducerConfig.java).
MAX_RECORD_BYTES = 1_048_576
DEFAULT_ACKS = 2

# Two HTTP codes carry a narrower meaning inside the cluster, and clients retry on them.
# Kafka reference: Errors.NOT_LEADER_OR_FOLLOWER and NOT_ENOUGH_REPLICAS (Errors.java).
NOT_LEADER = HTTPStatus.CONFLICT  # 409: this broker is not the leader for the partition
UNAVAILABLE = HTTPStatus.SERVICE_UNAVAILABLE  # 503: can't reach it, or too few replicas for acks
# A producer sequence the leader can't accept: a gap after the last one it stored, or a
# number older than the ones it keeps. Not 409, because clients retry 409 and sending
# the same sequence again can't fix this.
# Kafka reference: Errors.OUT_OF_ORDER_SEQUENCE_NUMBER and DUPLICATE_SEQUENCE_NUMBER.
BAD_SEQUENCE = HTTPStatus.UNPROCESSABLE_ENTITY  # 422


class APIError(Exception):
    """An HTTP error reply. ``fields`` are extra keys sent next to "error" in the JSON
    body, such as the follower's offsets in a /replicate conflict."""

    def __init__(self, status: int, message: str, fields: dict | None = None):
        super().__init__(message)
        self.status = status
        self.fields = fields or {}


def validate_config(config: dict) -> dict:
    brokers = config.get("brokers")
    topics = config.get("topics")
    if not isinstance(brokers, dict) or not brokers or not isinstance(topics, dict) or not topics:
        raise ValueError('config needs non-empty "brokers" and "topics" objects')

    for name, url in brokers.items():
        if not isinstance(name, str) or not name:
            raise ValueError(f"broker name must be a non-empty string, got {name!r}")
        if not isinstance(url, str) or not url.startswith("http://"):
            raise ValueError(f"broker {name!r} URL must start with http://, got {url!r}")

    for name, spec in topics.items():
        # Topic names end up in file paths and in "topic:partition" keys.
        if not isinstance(name, str) or not name or "/" in name or ":" in name:
            raise ValueError(f"invalid topic name {name!r}: it must be non-empty and "
                             "contain no '/' or ':'")
        if not isinstance(spec, dict):
            raise ValueError(f"topic {name!r} must be an object with partitions and "
                             "replication_factor")
        partitions = spec.get("partitions")
        replication_factor = spec.get("replication_factor")
        if type(partitions) is not int or type(replication_factor) is not int:
            raise ValueError(f"topic {name!r} needs integer partitions and replication_factor")
        if partitions < 1:
            raise ValueError(f"topic {name!r} needs at least 1 partition, got {partitions}")
        if not 1 <= replication_factor <= len(brokers):
            raise ValueError(f"topic {name!r} replication_factor must be between 1 and the "
                             f"number of brokers ({len(brokers)}), got {replication_factor}")
    return config


def load_config(path: str | Path) -> dict:
    try:
        config = json.loads(Path(path).read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"config file {path} is not valid JSON: {exc}") from exc
    return validate_config(config)


def replica_ids(broker_ids: list[str], partition: int, replication_factor: int) -> list[str]:
    """Brokers holding a partition, preferred leader first.

    Partition p starts at broker p and wraps around, so leadership is spread
    across the cluster. The broker and the controller both call this, which is
    what keeps their views of who holds what in agreement.

    Kafka reference: StripedReplicaPlacer (round-robin placement) and
    PartitionChangeBuilder.electLeader (first replica is the preferred leader).
    """
    count = min(replication_factor, len(broker_ids))
    return [broker_ids[(partition + i) % len(broker_ids)] for i in range(count)]


def partition_for_key(key: str | None, partition_count: int, value: object | None = None) -> int:
    # Python's built-in hash() is salted per process, so two producers would disagree.
    # SHA-256 gives every process the same answer. Records without a key are placed
    # by their value instead.
    # Kafka reference: BuiltInPartitioner.partitionForKey (murmur2(key) % partitions),
    # kafka-python DefaultPartitioner, librdkafka's "partitioner" setting.
    if key is not None:
        material = key
    else:
        material = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(material.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % partition_count


def request_json(url: str, method: str = "GET", payload: dict | None = None,
                 timeout: float = 1.5) -> dict:
    data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    request = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except HTTPError as exc:
        try:
            reply = json.loads(exc.read())
        except (ValueError, OSError):
            reply = None
        if not isinstance(reply, dict):
            raise APIError(exc.code, str(exc)) from exc
        fields = {name: value for name, value in reply.items() if name != "error"}
        raise APIError(exc.code, reply.get("error", str(exc)), fields) from exc
    except URLError as exc:
        # The request never got there: connection refused, unknown host, and so on.
        raise APIError(UNAVAILABLE, f"cannot reach {url}: {exc.reason}") from exc
    except (OSError, HTTPException) as exc:
        # The request was sent, but the reply timed out or the connection dropped.
        # The peer may have done the work anyway (a produce may be committed), so
        # don't word this as if the request never arrived.
        raise APIError(UNAVAILABLE, f"no response from {url}: {exc}") from exc
