"""Shared configuration and HTTP helpers."""

from __future__ import annotations

import hashlib
from http.client import HTTPException
import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


MAX_RECORD_BYTES = 1_048_576


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
        if not isinstance(name, str) or not name or not isinstance(url, str) or not url.startswith("http://"):
            raise ValueError("broker names and HTTP URLs must be nonempty")
    for name, spec in topics.items():
        if not isinstance(name, str) or not name or "/" in name or ":" in name:
            raise ValueError("invalid topic name")
        if not isinstance(spec, dict) or type(spec.get("partitions")) is not int or type(spec.get("replication_factor")) is not int:
            raise ValueError("topic requires integer partitions and replication_factor")
        if spec["partitions"] < 1 or not 1 <= spec["replication_factor"] <= len(brokers):
            raise ValueError("invalid partition count or replication factor")
    return config


def load_config(path: str | Path) -> dict:
    return validate_config(json.loads(Path(path).read_text()))


def partition_for_key(key: str | None, count: int, value: object | None = None) -> int:
    material = key if key is not None else json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return int.from_bytes(hashlib.sha256(material.encode("utf-8")).digest()[:8], "big") % count


def request_json(url: str, method: str = "GET", payload: dict | None = None, timeout: float = 1.5) -> dict:
    data = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    req = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=timeout) as response:
            return json.loads(response.read())
    except HTTPError as exc:
        try:
            message = json.loads(exc.read()).get("error", str(exc))
        except (ValueError, OSError):
            message = str(exc)
        raise APIError(exc.code, message) from exc
    except (URLError, OSError, HTTPException) as exc:
        raise APIError(503, str(exc)) from exc
