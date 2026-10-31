"""Configuration, replica placement, and the HTTP helper shared by every process."""

from __future__ import annotations

import hashlib
from http import HTTPStatus
from http.client import HTTPException
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


MAX_RECORD_BYTES = 1_048_576
DEFAULT_ACKS = 2

# Two HTTP codes carry a narrower meaning inside the cluster, and clients retry on them.
NOT_LEADER = HTTPStatus.CONFLICT  # 409: this broker is not the leader for the partition
UNAVAILABLE = HTTPStatus.SERVICE_UNAVAILABLE  # 503: can't reach it, or too few replicas for acks


class APIError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def validate_config(config: dict) -> dict:
    brokers = config.get("brokers")
    topics = config.get("topics")
    if not isinstance(brokers, dict) or not brokers or not isinstance(topics, dict) or not topics:
        raise ValueError("config requires nonempty brokers and topics mappings")

    for name, url in brokers.items():
        if not isinstance(name, str) or not name:
            raise ValueError("broker names and HTTP URLs must be nonempty")
        if not isinstance(url, str) or not url.startswith("http://"):
            raise ValueError("broker names and HTTP URLs must be nonempty")

    for name, spec in topics.items():
        # Topic names end up in file paths and in "topic:partition" keys.
        if not isinstance(name, str) or not name or "/" in name or ":" in name:
            raise ValueError("invalid topic name")
        if not isinstance(spec, dict):
            raise ValueError("topic requires integer partitions and replication_factor")
        partitions = spec.get("partitions")
        replication_factor = spec.get("replication_factor")
        if type(partitions) is not int or type(replication_factor) is not int:
            raise ValueError("topic requires integer partitions and replication_factor")
        if partitions < 1 or not 1 <= replication_factor <= len(brokers):
            raise ValueError("invalid partition count or replication factor")
    return config


def load_config(path: str | Path) -> dict:
    return validate_config(json.loads(Path(path).read_text()))


def replica_ids(broker_ids: list[str], partition: int, replication_factor: int) -> list[str]:
    """Brokers holding a partition, preferred leader first.

    Partition p starts at broker p and wraps around, so leadership is spread
    across the cluster. The broker and the controller both call this, which is
    what keeps their views of who holds what in agreement.
    """
    count = min(replication_factor, len(broker_ids))
    return [broker_ids[(partition + i) % len(broker_ids)] for i in range(count)]


def partition_for_key(key: str | None, partition_count: int, value: object | None = None) -> int:
    # Python's built-in hash() is salted per process, so two producers would disagree.
    # SHA-256 gives every process the same answer. Records without a key are placed
    # by their value instead.
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
            message = json.loads(exc.read()).get("error", str(exc))
        except (ValueError, OSError):
            message = str(exc)
        raise APIError(exc.code, message) from exc
    except (URLError, OSError, HTTPException) as exc:
        # A peer we cannot reach looks the same to callers as one that is up but
        # refusing the request, so both become UNAVAILABLE.
        raise APIError(UNAVAILABLE, str(exc)) from exc
