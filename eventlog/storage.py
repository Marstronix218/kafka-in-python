"""Durable storage for a single topic partition."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any


class PartitionLog:
    """A durable, append-only JSONL log for one topic partition.

    Entries may exist beyond the committed high watermark while replication is in
    progress.  ``read`` exposes only committed entries; ``get`` can inspect the
    complete local replica.
    """

    _REQUIRED_FIELDS = frozenset({"offset", "key", "value", "timestamp"})

    def __init__(self, data_dir: str | Path, topic: str, partition: int):
        if not isinstance(topic, str) or not topic or topic in {".", ".."}:
            raise ValueError("topic must be a non-empty name")
        if Path(topic).name != topic:
            raise ValueError("topic must not contain path separators")
        if not isinstance(partition, int) or isinstance(partition, bool) or partition < 0:
            raise ValueError("partition must be a non-negative integer")

        self._directory = Path(data_dir) / topic
        self._directory.mkdir(parents=True, exist_ok=True)
        self._data_path = self._directory / f"partition-{partition}.jsonl"
        self._commit_path = self._directory / f"partition-{partition}.commit.json"
        self._lock = threading.RLock()
        self._records: list[dict[str, Any]] = []

        self._load_records()
        self._committed_offset = self._load_committed_offset()
        if self._committed_offset > self.last_offset:
            raise ValueError("committed offset is beyond the end of the log")

    @property
    def last_offset(self) -> int:
        """Return the largest locally stored offset, or -1 for an empty log."""

        return len(self._records) - 1

    @property
    def committed_offset(self) -> int:
        """Return the committed high watermark, or -1 when nothing is committed."""

        return self._committed_offset

    def append(self, record: dict[str, Any]) -> None:
        """Durably append ``record``, accepting retries idempotently.

        A conflicting entry beyond the committed high watermark is replaced along
        with all later entries.  Committed history cannot be changed.
        """

        canonical = self._canonical_record(record)
        offset = canonical["offset"]

        with self._lock:
            if offset <= self.last_offset:
                if self._records[offset] == canonical:
                    return
                if offset <= self._committed_offset:
                    raise ValueError("cannot replace a committed record")
                self.truncate_from(offset)

            if offset != self.last_offset + 1:
                raise ValueError(
                    f"expected offset {self.last_offset + 1}, got {offset}"
                )

            encoded = self._encode(canonical)
            with self._data_path.open("ab") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            self._records.append(canonical)

    def get(self, offset: int) -> dict[str, Any] | None:
        """Return a stored entry, including an uncommitted entry, if present."""

        self._validate_offset(offset)
        with self._lock:
            if offset > self.last_offset:
                return None
            return self._copy_record(self._records[offset])

    def read(self, start: int, limit: int = 100) -> list[dict[str, Any]]:
        """Read at most ``limit`` committed records beginning at ``start``."""

        self._validate_offset(start)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
            raise ValueError("limit must be a non-negative integer")
        if limit == 0:
            return []

        with self._lock:
            stop = min(start + limit, self._committed_offset + 1)
            if start >= stop:
                return []
            return [self._copy_record(record) for record in self._records[start:stop]]

    def commit(self, offset: int) -> None:
        """Durably advance the committed high watermark."""

        if not isinstance(offset, int) or isinstance(offset, bool) or offset < -1:
            raise ValueError("committed offset must be an integer greater than or equal to -1")

        with self._lock:
            if offset < self._committed_offset:
                raise ValueError("committed offset cannot move backwards")
            if offset > self.last_offset:
                raise ValueError("cannot commit beyond the end of the log")
            if offset == self._committed_offset:
                return

            self._persist_committed_offset(offset)
            self._committed_offset = offset

    def truncate_from(self, offset: int) -> None:
        """Durably remove ``offset`` and all later records outside the committed prefix."""

        self._validate_offset(offset)
        with self._lock:
            if offset <= self._committed_offset:
                raise ValueError("cannot truncate committed records")
            if offset > self.last_offset:
                return

            retained = self._records[:offset]
            contents = b"".join(self._encode(record) for record in retained)
            self._atomic_replace(self._data_path, contents)
            self._records = retained

    def _load_records(self) -> None:
        if not self._data_path.exists():
            self._create_empty_file()
            return

        data = self._data_path.read_bytes()
        valid_end = 0
        records: list[dict[str, Any]] = []
        lines = data.splitlines(keepends=True)

        for index, line in enumerate(lines):
            is_last = index == len(lines) - 1
            try:
                if not line.endswith(b"\n"):
                    raise ValueError("incomplete line")
                decoded = json.loads(line)
                record = self._canonical_record(decoded)
                if record["offset"] != len(records):
                    raise ValueError("log offsets must be contiguous and start at zero")
            except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
                if not is_last or line.endswith(b"\n"):
                    raise ValueError(f"corrupt log record at line {index + 1}") from None
                self._truncate_file_to(valid_end)
                break
            records.append(record)
            valid_end += len(line)

        self._records = records

    def _load_committed_offset(self) -> int:
        if not self._commit_path.exists():
            return -1
        try:
            metadata = json.loads(self._commit_path.read_text(encoding="utf-8"))
            committed_offset = metadata["committed_offset"]
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError):
            raise ValueError("invalid committed offset metadata") from None
        if (
            not isinstance(committed_offset, int)
            or isinstance(committed_offset, bool)
            or committed_offset < -1
        ):
            raise ValueError("invalid committed offset metadata")
        return committed_offset

    def _persist_committed_offset(self, committed_offset: int) -> None:
        payload = json.dumps({"committed_offset": committed_offset}, separators=(",", ":"))
        self._atomic_replace(self._commit_path, (payload + "\n").encode("utf-8"))

    def _create_empty_file(self) -> None:
        try:
            descriptor = os.open(self._data_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            return
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._fsync_directory()

    def _truncate_file_to(self, length: int) -> None:
        with self._data_path.open("r+b") as stream:
            stream.truncate(length)
            stream.flush()
            os.fsync(stream.fileno())

    def _atomic_replace(self, path: Path, contents: bytes) -> None:
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            with temporary.open("wb") as stream:
                stream.write(contents)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            self._fsync_directory()
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def _fsync_directory(self) -> None:
        descriptor = os.open(self._directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @classmethod
    def _canonical_record(cls, record: object) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise TypeError("record must be a dictionary")
        missing = cls._REQUIRED_FIELDS.difference(record)
        if missing:
            raise ValueError(f"record is missing required fields: {', '.join(sorted(missing))}")
        offset = record["offset"]
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("record offset must be a non-negative integer")
        try:
            return json.loads(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
        except (TypeError, ValueError):
            raise ValueError("record must contain only JSON-serializable values") from None

    @staticmethod
    def _validate_offset(offset: int) -> None:
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("offset must be a non-negative integer")

    @staticmethod
    def _encode(record: dict[str, Any]) -> bytes:
        return (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )

    @classmethod
    def _copy_record(cls, record: dict[str, Any]) -> dict[str, Any]:
        return json.loads(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
