"""A broker process. It stores the partitions assigned to it, leads some of them,
and copies each new record to the in-sync followers before acknowledging the write.

Kafka references for this module are listed in REFERENCES.md."""

from __future__ import annotations

import json
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from .common import (BAD_SEQUENCE, DEFAULT_ACKS, MAX_RECORD_BYTES, NOT_LEADER, UNAVAILABLE,
                     APIError, replica_ids, request_json, validate_config)
from .faults import FaultInjector
from .storage import PRODUCER_STATE_ENTRIES, PartitionLog


HEARTBEAT_INTERVAL_SECONDS = 0.5
MAINTENANCE_INTERVAL_SECONDS = 0.5
# Two brokers that both think they lead can each hold a partition lock while waiting
# for the other's reply during a failover. This timeout is what breaks that wait.
PEER_TIMEOUT_SECONDS = 1.0
MAX_FETCH_LIMIT = 1000
# A request body is one record plus the topic, partition and other fields around it.
MAX_REQUEST_BYTES = MAX_RECORD_BYTES + 4096
# A /replicate batch stops at either limit, but always carries at least one record.
# Sizes are of the JSON as sent. /produce bodies are capped at MAX_REQUEST_BYTES, so
# no single stored record is over the byte limit.
REPLICATE_BATCH_RECORDS = 100
REPLICATE_BATCH_BYTES = 4 * MAX_RECORD_BYTES
MAX_REPLICATE_REQUEST_BYTES = REPLICATE_BATCH_BYTES + 4096
# One push sends at most this many batches. An ISR follower still behind after that
# is dropped from the ISR. One outside the ISR continues on the next maintenance step.
MAX_BATCHES_PER_PUSH = 10
# A /produce that waits longer than this for the partition lock gets 503 and is not
# applied.
PRODUCE_LOCK_TIMEOUT_SECONDS = 2.0
# A /produce that reaches the append later than this after it arrived gets 503 and is
# not appended. The client gives up after 3 s, and an incoming delay fault, the lock
# wait and the controller lookup all come before the append.
PRODUCE_DEADLINE_SECONDS = 2.5
MAX_PRODUCER_ID_LENGTH = 128
CONTROLLER = "controller"


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

        # Leader side, per partition: for each follower, the next offset to send and the
        # committed offset it last reported. "next" is only set from a follower's reply,
        # so everything before it is known to match this log. The entry is only valid
        # for the leader epoch it was built in. Guarded by the partition lock.
        self.followers: dict[tuple[str, int], dict] = {}
        # Follower side, per partition: (leader_epoch, highest leader_last_offset) of the
        # /replicate requests applied. Within one epoch the leader's log end never goes
        # down: the leader does not truncate its own log, and it fsyncs every record
        # before sending it, so a restart keeps them. A batch with a lower
        # leader_last_offset was sent before one already applied, and its truncation
        # would remove records sent after it. Kept in memory only. After a restart any
        # batch is accepted, and requests sent to the old process can no longer arrive.
        self.applied_leader_ends: dict[tuple[str, int], tuple[int, int]] = {}
        self.faults = FaultInjector(targets=set(config["brokers"]) | {CONTROLLER})

    def _replicas(self, topic: str, partition: int) -> list[str]:
        replication_factor = self.config["topics"][topic]["replication_factor"]
        return replica_ids(list(self.config["brokers"]), partition, replication_factor)

    def _lock_for(self, topic: str, partition: int) -> threading.RLock:
        # A partition this broker doesn't hold has no lock. Handing back a throwaway
        # one lets the caller reach its own checks, which reject the request: 409 from
        # _require_leader or 404 from self.log(), whichever runs first.
        return self.locks.get((topic, partition), threading.RLock())

    @contextmanager
    def _locked(self, topic: str, partition: int, timeout: float):
        """Hold the partition lock, or raise 503 with nothing changed if it is not free
        within timeout seconds. Request handlers use this, so a request whose sender
        has already given up is not applied long afterwards."""
        lock = self._lock_for(topic, partition)
        if not lock.acquire(timeout=timeout):
            raise APIError(UNAVAILABLE, f"partition {topic}:{partition} is busy on broker "
                                        f"{self.broker_id}: waited {timeout:g}s for its lock")
        try:
            yield
        finally:
            lock.release()

    def log(self, topic: str, partition: int) -> PartitionLog:
        if (topic, partition) not in self.logs:
            raise APIError(HTTPStatus.NOT_FOUND, f"partition {topic}:{partition} is not stored "
                                                 f"on broker {self.broker_id}")
        return self.logs[(topic, partition)]

    def _request(self, target: str, path: str, body: dict | None = None,
                 timeout: float = 1.5) -> dict:
        """Send a request to the controller (target CONTROLLER) or to a peer broker id.

        GET when body is None, POST otherwise. Every outgoing call from a broker goes
        through here, so the fault rules apply to all of them. Raises APIError on any
        failure.
        """
        return self.faults.outgoing(target, path, lambda: self._send(target, path, body, timeout),
                                    timeout)

    def _send(self, target: str, path: str, body: dict | None, timeout: float) -> dict:
        # The in-process tests replace this method to skip HTTP.
        base = self.controller_url if target == CONTROLLER else self.config["brokers"][target]
        return request_json(base + path, "GET" if body is None else "POST", body, timeout=timeout)

    def route(self, topic: str, partition: int) -> dict:
        query = urlencode({"topic": topic, "partition": partition})
        return self._request(CONTROLLER, "/route?" + query)

    def _require_leader(self, topic: str, partition: int) -> dict:
        # Kafka reference: brokers answer non-leader requests with NOT_LEADER_OR_FOLLOWER.
        route = self.route(topic, partition)
        if route["leader"] != self.broker_id:
            raise APIError(NOT_LEADER, f"broker {self.broker_id} is not the leader for "
                                       f"{topic}:{partition}; the leader is {route['leader']}")
        return route

    def _require_sent_by_leader(self, body: dict, topic: str, partition: int) -> dict:
        # A broker that has just lost leadership may still be sending. Only the broker
        # the controller names right now, at the current epoch, may write to followers.
        # The check and the write that follows run under the partition lock, and the
        # epoch only grows, so once a new leader's write is accepted here, writes from
        # older epochs are refused.
        # Kafka reference: FENCED_LEADER_EPOCH (Errors.java).
        route = self.route(topic, partition)
        sender = (body.get("leader"), body.get("leader_epoch"))
        if sender != (route["leader"], route["leader_epoch"]):
            raise APIError(NOT_LEADER, f"request is from {sender[0]} at leader epoch "
                                       f"{sender[1]}, but the current leader is "
                                       f"{route['leader']} at epoch {route['leader_epoch']}")
        return route

    def heartbeat_loop(self) -> None:
        # Kafka reference: BrokerHeartbeat requests to the controller (KIP-631).
        while not self.stop_event.is_set():
            try:
                self._request(CONTROLLER, "/heartbeat", {"broker_id": self.broker_id})
            except (APIError, OSError):
                # Nothing to do here. Missed heartbeats are how the controller finds out.
                pass
            self.stop_event.wait(HEARTBEAT_INTERVAL_SECONDS)

    def maintenance_loop(self) -> None:
        # Separate from heartbeat_loop, so a slow follower can't delay heartbeats
        # long enough for the controller to declare this broker dead.
        # Waiting before the first step gives the other brokers time to send their
        # first heartbeat. The first /route call elects a leader, and otherwise the
        # broker that started first would lead instead of the preferred replica.
        while not self.stop_event.wait(MAINTENANCE_INTERVAL_SECONDS):
            for topic, partition in list(self.logs):
                try:
                    self.maintain(topic, partition)
                except (APIError, OSError, KeyError, TypeError, ValueError):
                    # The controller or a peer is unreachable, or this broker just lost
                    # leadership. The next step starts over from the controller's route.
                    pass
                except Exception:
                    # A bug. Print it and keep going, or this broker would stop
                    # replicating and committing without any sign of it.
                    print(f"broker {self.broker_id}: maintenance of {topic}:{partition} "
                          f"failed:", file=sys.stderr)
                    traceback.print_exc()

    def maintain(self, topic: str, partition: int) -> None:
        """Run one replication step for a partition, if this broker leads it.

        Sends ISR followers anything they lack and commits what they all hold. This
        commits records a new leader inherited, and resends a /commit a follower missed.
        Then pushes records to alive replicas outside the ISR and adds each one that
        reaches this log's end back to the ISR.
        Kafka followers fetch from the leader (ReplicaFetcherThread). Here the leader
        pushes, so a restarted broker catches up without a loop of its own.
        """
        with self._lock_for(topic, partition):
            route = self.route(topic, partition)
            if route["leader"] != self.broker_id:
                self.followers.pop((topic, partition), None)
                return
            log = self.log(topic, partition)
            isr = self._sync_isr(topic, partition, route, log)

        alive = self._request(CONTROLLER, "/health")["alive_brokers"]
        for peer in route["replicas"]:
            if peer in isr or peer not in alive:
                continue
            # The lock is taken for one batch at a time, so a produce waits for at most
            # one peer timeout here, not MAX_BATCHES_PER_PUSH of them.
            for _ in range(MAX_BATCHES_PER_PUSH):
                with self._lock_for(topic, partition):
                    current = self.route(topic, partition)
                    if (current["leader"], current["leader_epoch"]) != (
                            self.broker_id, route["leader_epoch"]):
                        return
                    if peer in current["isr"]:
                        break
                    if self._push(peer, topic, partition, current, log, batches=1,
                                  verify=True):
                        # Kafka reference: Partition.maybeExpandIsr.
                        isr = [broker_id for broker_id in current["replicas"]
                               if broker_id in current["isr"] or broker_id == peer]
                        self._alter_isr(topic, partition, current, isr)
                        break
                    if peer not in self._follower_state(topic, partition,
                                                        current["leader_epoch"]):
                        # The push failed (_push forgets a peer it can't reach). The
                        # next step tries again.
                        break

    def produce(self, body: dict, arrived: float | None = None) -> dict:
        """Append one record, copy it to the ISR and return the /produce reply.
        arrived is time.monotonic() when the request arrived; None means now."""
        # Kafka reference: ReplicaManager.appendRecords -> Partition.appendRecordsToLeader,
        # then Partition.maybeIncrementLeaderHW and Partition.checkEnoughReplicasReachOffset;
        # producer "acks" (ProducerConfig ACKS_DOC), used the way acks=all uses
        # min.insync.replicas.
        # Unlike Kafka, the leader pushes to followers instead of followers fetching.
        if arrived is None:
            arrived = time.monotonic()
        topic, partition, key, acks, producer = self._validate_produce(body)
        with self._locked(topic, partition, PRODUCE_LOCK_TIMEOUT_SECONDS):
            route = self._require_leader(topic, partition)
            log = self.log(topic, partition)
            # acks is the smallest ISR, leader included, that may accept a write.
            # Kafka reference: NOT_ENOUGH_REPLICAS (Errors.java).
            if len(route["isr"]) < acks:
                raise APIError(UNAVAILABLE, f"not enough replicas: acks={acks}, but only "
                                            f"{len(route['isr'])} of {len(route['replicas'])} "
                                            f"replicas are in sync ({', '.join(route['isr'])})")
            reply = {"topic": topic, "partition": partition, "acks": acks, "duplicate": False}
            if producer is not None:
                reply["producer_id"], reply["sequence"] = producer
                offset = self._check_sequence(topic, partition, log, *producer, key,
                                              body["value"])
                if offset is not None:
                    # A retry of a record this log already holds. It may still be
                    # uncommitted (a lost reply, or a tail inherited from the last
                    # leader), so it goes through the same replicate and commit step
                    # as a new record, without a second append.
                    self._replicate_for_acks(topic, partition, route, log, acks, offset)
                    return {**reply, "offset": offset, "duplicate": True}

            # Uncommitted records already in this log stay. They may have been
            # acknowledged by an earlier leader whose /commit this broker missed.
            record = {"offset": log.last_offset + 1, "key": key, "value": body["value"],
                      "timestamp": time.time()}
            if producer is not None:
                record["producer_id"], record["sequence"] = producer
            size = len(json.dumps(record, ensure_ascii=False).encode())
            if size > MAX_RECORD_BYTES:
                raise APIError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                               f"record is {size} bytes; the limit is {MAX_RECORD_BYTES} (1 MiB)")
            waited = time.monotonic() - arrived
            if waited > PRODUCE_DEADLINE_SECONDS:
                raise APIError(UNAVAILABLE, f"request to {topic}:{partition} arrived {waited:.1f}s "
                                            f"ago, over the {PRODUCE_DEADLINE_SECONDS:g}s limit, "
                                            f"so broker {self.broker_id} did not append it")
            log.append(record)
            self._replicate_for_acks(topic, partition, route, log, acks, record["offset"])
            return {**reply, "offset": record["offset"]}

    def _replicate_for_acks(self, topic: str, partition: int, route: dict, log: PartitionLog,
                            acks: int, offset: int) -> None:
        isr = self._sync_isr(topic, partition, route, log)
        if len(isr) < acks:
            # The record is in the log and may be committed already, since the
            # smaller ISR all hold it. The producer can't tell, as in Kafka.
            # Kafka reference: NOT_ENOUGH_REPLICAS_AFTER_APPEND (Errors.java).
            raise APIError(UNAVAILABLE, f"not enough replicas after append: acks={acks}, "
                                        f"but the ISR shrank to {len(isr)} "
                                        f"({', '.join(isr)}) while writing offset {offset}")

    def _check_sequence(self, topic: str, partition: int, log: PartitionLog, producer_id: str,
                        sequence: int, key: str | None, value: object) -> int | None:
        """Return the offset already holding this producer's sequence, or None if the
        record is new. Raises 422 if the sequence leaves a gap after the last stored one,
        is older than the ones the log keeps, or is stored with a different key or value."""
        # Kafka reference: ProducerAppendInfo (sequence checks on the leader).
        entries = log.producer_sequences(producer_id)
        # A producer this log has no record of may start at any sequence.
        if not entries:
            return None
        last = entries[-1][0]
        where = f"producer {producer_id} on {topic}:{partition}"
        for stored_sequence, offset in entries:
            if stored_sequence != sequence:
                continue
            # A retry carries the same key and value. A different record with this
            # sequence means the producer reused it, and answering "duplicate" would
            # acknowledge a record that is not stored. The stored record is compared
            # directly: the log holds every record in memory, so a digest saves nothing.
            stored = log.get(offset)
            if _content(stored["key"], stored["value"]) != _content(key, value):
                raise APIError(BAD_SEQUENCE, f"sequence reused with different content: {where} "
                                             f"sent sequence {sequence}, but offset {offset} "
                                             f"holds a different record with that sequence",
                               {"last_sequence": last, "offset": offset})
            return offset
        if sequence == last + 1:
            return None
        if sequence > last + 1:
            # Kafka reference: Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.
            raise APIError(BAD_SEQUENCE, f"out of order sequence: {where} sent sequence "
                                         f"{sequence}, but the last one stored is {last}",
                           {"last_sequence": last})
        # Kafka reference: Errors.DUPLICATE_SEQUENCE_NUMBER.
        raise APIError(BAD_SEQUENCE, f"duplicate sequence: {where} sent sequence {sequence}, "
                                     f"which is older than the last {PRODUCER_STATE_ENTRIES} "
                                     f"this broker keeps ({entries[0][0]}..{last}), so its "
                                     f"offset is unknown", {"last_sequence": last})

    def _validate_produce(self, body: dict) -> tuple[str, int, str | None, int,
                                                     tuple[str, int] | None]:
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
        producer_id, sequence = body.get("producer_id"), body.get("sequence")
        if producer_id is None:
            if sequence is not None:
                raise APIError(HTTPStatus.BAD_REQUEST, "sequence needs a producer_id")
            return topic, partition, key, acks, None
        if not isinstance(producer_id, str) or not 1 <= len(producer_id) <= MAX_PRODUCER_ID_LENGTH:
            raise APIError(HTTPStatus.BAD_REQUEST,
                           f"producer_id must be a string of 1 to {MAX_PRODUCER_ID_LENGTH} "
                           f"characters, got {producer_id!r}")
        if type(sequence) is not int or sequence < 0:
            raise APIError(HTTPStatus.BAD_REQUEST,
                           f"sequence must be a non-negative integer, got {sequence!r}")
        return topic, partition, key, acks, (producer_id, sequence)

    def _sync_isr(self, topic: str, partition: int, route: dict, log: PartitionLog) -> list[str]:
        """Bring every ISR follower to this log's end, then commit the whole log.

        Followers that fail are first removed from the ISR through the controller.
        Returns the resulting ISR. Raises APIError, with nothing committed, if the
        controller refuses the change (this broker no longer leads) or can't be reached.
        Call with the partition lock held.
        """
        isr = list(route["isr"])
        failed = [peer for peer in isr if peer != self.broker_id
                  and not self._push(peer, topic, partition, route, log)]
        if failed:
            # Kafka reference: Partition.maybeShrinkIsr.
            isr = [broker_id for broker_id in isr if broker_id not in failed]
            self._alter_isr(topic, partition, route, isr)
        # Every ISR member now holds the whole log, so all of it is committed. This
        # includes records an earlier leader wrote that this broker held uncommitted.
        if log.last_offset > log.committed_offset:
            log.commit(log.last_offset)
        followers = self._follower_state(topic, partition, route["leader_epoch"])
        # Set only once a sync at this epoch has committed, so fetch() can read
        # without one. Raising above leaves it unset.
        self.followers[(topic, partition)]["synced"] = True

        # Best effort. A follower that misses this gets it again on the next maintenance
        # step, and its copy of the records is safe either way.
        for peer in isr:
            known = followers.get(peer)
            if known is None or known["committed"] >= log.committed_offset:
                continue
            try:
                reply = self._send_to_peer(peer, "/commit", topic, partition, route,
                                           offset=log.committed_offset)
                known["committed"] = reply["committed_offset"]
            except APIError:
                # A 409 here means the follower does not hold the records we think
                # it does. Forget what we know, so the next push checks its log end
                # instead of skipping it.
                followers.pop(peer, None)
        return isr

    def _alter_isr(self, topic: str, partition: int, route: dict, isr: list[str]) -> None:
        # Kafka reference: AlterPartition.
        body = {"topic": topic, "partition": partition, "leader": self.broker_id,
                "leader_epoch": route["leader_epoch"], "isr": isr}
        try:
            self._request(CONTROLLER, "/isr", body)
        except APIError as exc:
            if exc.status == HTTPStatus.CONFLICT:
                raise APIError(NOT_LEADER, f"broker {self.broker_id} is no longer the leader "
                                           f"for {topic}:{partition}: {exc}") from exc
            raise
        route["isr"] = isr

    def _follower_state(self, topic: str, partition: int, leader_epoch: int) -> dict:
        entry = self.followers.get((topic, partition))
        if entry is None or entry["leader_epoch"] != leader_epoch:
            entry = {"leader_epoch": leader_epoch, "peers": {}, "synced": False}
            self.followers[(topic, partition)] = entry
        return entry["peers"]

    def _push(self, peer: str, topic: str, partition: int, route: dict, log: PartitionLog,
              batches: int = MAX_BATCHES_PER_PUSH, verify: bool = False) -> bool:
        """Send peer the records it lacks, in at most `batches` requests. Returns True
        once its log matches this log up to the end, False if it failed or is still
        behind. After a failure the peer has no entry in _follower_state.

        Without verify, a peer this leader already brought to the end in this epoch is
        not contacted. With verify there is always a request, possibly with no records,
        and only the peer's reply can say it has caught up.
        """
        followers = self._follower_state(topic, partition, route["leader_epoch"])
        known = followers.get(peer)
        if not verify and known is not None and known["next"] > log.last_offset:
            return True
        # With no reply yet in this epoch, start after this leader's committed offset.
        # An ISR member holds every committed record, so it matches up to there. A
        # follower outside the ISR answers with a conflict, and we start after its own
        # committed offset instead.
        start = known["next"] if known is not None else log.committed_offset + 1
        for _ in range(batches):
            start = min(start, log.last_offset + 1)
            records = self._batch(log, start)
            try:
                reply = self._send_to_peer(peer, "/replicate", topic, partition, route,
                                           records=records, leader_last_offset=log.last_offset,
                                           committed_offset=log.committed_offset)
            except APIError as exc:
                if "committed_offset" in exc.fields:
                    # The peer's committed records match ours, so it is safe to go on
                    # from there, in this call or the next one.
                    start = exc.fields["committed_offset"] + 1
                    followers[peer] = {"next": start, "committed": exc.fields["committed_offset"]}
                    continue
                followers.pop(peer, None)
                return False
            # The peer may hold more than we sent, but only what we sent is known to
            # match. It can't hold less after a successful reply, but if its log end
            # says otherwise, trust that.
            start = min(start + len(records), reply["last_offset"] + 1)
            followers[peer] = {"next": start, "committed": reply["committed_offset"]}
            if start > log.last_offset:
                return True
        return False

    @staticmethod
    def _batch(log: PartitionLog, start: int) -> list[dict]:
        records = []
        size = 0
        for record in log.read_range(start, REPLICATE_BATCH_RECORDS):
            # Measured as request_json sends it, plus a comma.
            size += len(json.dumps(record, separators=(",", ":"))) + 1
            if records and size > REPLICATE_BATCH_BYTES:
                break
            records.append(record)
        return records

    def _send_to_peer(self, peer: str, path: str, topic: str, partition: int, route: dict,
                      **fields) -> dict:
        """POST to another broker as the leader at route's epoch. Raises APIError."""
        body = {"topic": topic, "partition": partition, "leader": self.broker_id,
                "leader_epoch": route["leader_epoch"], **fields}
        return self._request(peer, path, body, timeout=PEER_TIMEOUT_SECONDS)

    def replicate(self, body: dict) -> dict:
        """Apply a batch of the leader's records and return this log's last and committed
        offsets.

        Raises 409 with committed_offset and last_offset fields when the batch starts
        past the point where this log is known to match the leader's. The leader then
        resends from committed_offset + 1. Raises 409 with no extra fields, and changes
        nothing, when leader_last_offset is lower than in a batch already applied at this
        leader epoch: the leader sent it before that batch.
        """
        # Kafka reference: the follower side of ReplicaFetcherThread / AbstractFetcherThread.
        topic, partition = _topic_and_partition(body)
        with self._locked(topic, partition, PEER_TIMEOUT_SECONDS):
            log = self.log(topic, partition)
            route = self._require_sent_by_leader(body, topic, partition)
            records, leader_last, leader_committed = _replication_batch(body)
            # A batch the leader gave up on can run here after a later one, for example
            # when its handler waited for this lock. Applied then, its truncation
            # below would remove records the later batches added, which the leader
            # may have committed and acknowledged since. A batch with the same
            # leader_last_offset is safe to apply again: it only rewrites records the
            # leader still holds unchanged, so a duplicated request does no harm.
            epoch = route["leader_epoch"]
            applied_epoch, applied_end = self.applied_leader_ends.get((topic, partition),
                                                                      (None, -1))
            if applied_epoch == epoch and leader_last < applied_end:
                raise APIError(HTTPStatus.CONFLICT,
                               f"stale batch: leader_last_offset {leader_last} at leader epoch "
                               f"{epoch} is below {applied_end}, which this replica already "
                               f"applied")
            first = records[0]["offset"] if records else leader_last + 1
            # The leader only sends from where it believes this log matches its own.
            # In the ISR that holds up to this log's end (the leader checked). Outside
            # it, only committed records are known to match: an uncommitted tail may be
            # from an older epoch and differ from the leader's log.
            in_sync = self.broker_id in route["isr"]
            known_good = log.last_offset if in_sync else log.committed_offset
            if first > known_good + 1:
                raise APIError(HTTPStatus.CONFLICT,
                               f"batch starts at offset {first}, but this replica only "
                               f"matches the leader up to {known_good}",
                               {"committed_offset": log.committed_offset,
                                "last_offset": log.last_offset})
            try:
                # append() replaces a different uncommitted record at the same offset,
                # along with everything after it.
                for record in records:
                    log.append(record)
                # Anything past the leader's end is from an older epoch.
                if log.last_offset > leader_last:
                    log.truncate_from(leader_last + 1)
                # Offsets up to the end of this batch now match the leader. Later ones
                # may not yet, so they can't be committed.
                matched = first - 1 + len(records)
                if min(leader_committed, matched) > log.committed_offset:
                    log.commit(min(leader_committed, matched))
            except (ValueError, TypeError, KeyError) as exc:
                # A malformed record, or a change to a committed record. The second
                # only happens after an unclean leader election.
                raise APIError(HTTPStatus.CONFLICT, str(exc)) from exc
            self.applied_leader_ends[(topic, partition)] = (epoch, leader_last)
            return {"last_offset": log.last_offset, "committed_offset": log.committed_offset}

    def commit_replica(self, body: dict) -> dict:
        topic, partition = _topic_and_partition(body)
        with self._locked(topic, partition, PEER_TIMEOUT_SECONDS):
            log = self.log(topic, partition)
            self._require_sent_by_leader(body, topic, partition)
            offset = body["offset"]
            # This replica may be ahead: an earlier leader's /commit reached it but not
            # the current leader. Committed offsets never move back.
            try:
                if offset > log.committed_offset:
                    log.commit(offset)
            except (ValueError, TypeError) as exc:
                raise APIError(HTTPStatus.CONFLICT, str(exc)) from exc
            return {"committed_offset": log.committed_offset}

    def fetch(self, topic: str, partition: int, offset: int, limit: int) -> dict:
        # Kafka reference: ReplicaManager.fetchMessages (reads stop at the high watermark).
        with self._lock_for(topic, partition):
            route = self._require_leader(topic, partition)
            if offset < 0 or not 1 <= limit <= MAX_FETCH_LIMIT:
                raise APIError(HTTPStatus.BAD_REQUEST,
                               f"offset must be non-negative and limit 1..{MAX_FETCH_LIMIT}, "
                               f"got offset={offset} limit={limit}")
            log = self.log(topic, partition)
            entry = self.followers.get((topic, partition))
            if entry is None or entry["leader_epoch"] != route["leader_epoch"] \
                    or not entry["synced"]:
                # A new leader's committed offset can be behind the old leader's until
                # its first sync, and a consumer that read up to the old one would
                # then be told its position is past the end. Syncing commits every
                # record the ISR holds, which includes all the old leader committed.
                self._sync_isr(topic, partition, route, log)
            return {"records": log.read(offset, limit), "committed_offset": log.committed_offset}

    def fault_rules(self) -> dict:
        return {"broker_id": self.broker_id, "rules": self.faults.rules()}

    def set_fault_rules(self, body: dict) -> dict:
        """Replace this broker's fault rules with body["rules"] (a list; empty clears).
        body["seed"], if given, restarts the probability rolls from that seed."""
        if "rules" not in body:
            raise APIError(HTTPStatus.BAD_REQUEST, 'POST /faults needs a "rules" list')
        try:
            rules = self.faults.set_rules(body["rules"], body.get("seed"))
        except ValueError as exc:
            raise APIError(HTTPStatus.BAD_REQUEST, str(exc)) from exc
        return {"broker_id": self.broker_id, "rules": rules}

    def status(self, topic: str, partition: int) -> dict:
        # This broker's own copy, leader or not. Used to check replication by hand and in tests.
        log = self.log(topic, partition)
        return {"last_offset": log.last_offset, "committed_offset": log.committed_offset}


def _topic_and_partition(body: dict) -> tuple[object, int]:
    topic, partition = body.get("topic"), body.get("partition")
    if type(partition) is not int:
        raise APIError(HTTPStatus.BAD_REQUEST, f"partition must be an integer, got {partition!r}")
    return topic, partition


def _content(key: str | None, value: object) -> str:
    # JSON text rather than ==, because in Python true == 1 and 1 == 1.0.
    return json.dumps([key, value], sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _replication_batch(body: dict) -> tuple[list[dict], int, int]:
    records = body.get("records")
    leader_last, leader_committed = body.get("leader_last_offset"), body.get("committed_offset")
    if type(leader_last) is not int or type(leader_committed) is not int:
        raise APIError(HTTPStatus.BAD_REQUEST,
                       "leader_last_offset and committed_offset must be integers")
    if not isinstance(records, list) or not all(
            isinstance(record, dict) and type(record.get("offset")) is int for record in records):
        raise APIError(HTTPStatus.BAD_REQUEST,
                       "records must be a list of JSON objects with integer offsets")
    offsets = [record["offset"] for record in records]
    if offsets and offsets != list(range(offsets[0], offsets[0] + len(offsets))):
        raise APIError(HTTPStatus.BAD_REQUEST, "record offsets must be consecutive")
    if offsets and offsets[-1] > leader_last:
        raise APIError(HTTPStatus.BAD_REQUEST, "records go past leader_last_offset")
    return records, leader_last, leader_committed


def _handler_for(state: BrokerState) -> type[BaseHTTPRequestHandler]:
    post_routes = {
        "/replicate": state.replicate,
        "/commit": state.commit_replica,
        "/faults": state.set_fault_rules,
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

        def error_reply(self, exc: Exception) -> tuple[int, dict]:
            # Anything that isn't an APIError is malformed input that got past
            # validation, like a non-numeric offset, so it's the client's fault.
            if isinstance(exc, APIError):
                return exc.status, {"error": str(exc), **exc.fields}
            return HTTPStatus.BAD_REQUEST, {"error": str(exc)}

        def answer(self, handle) -> tuple[int, dict]:
            try:
                return HTTPStatus.OK, handle()
            except (APIError, ValueError, KeyError, TypeError) as exc:
                return self.error_reply(exc)

        def serve(self, path: str, handle) -> None:
            """Reply with handle()'s result, after applying any incoming fault rule.

            Rules never apply to /faults, so a table that drops everything can still
            be cleared.
            """
            action = None if path == "/faults" else state.faults.incoming(path)
            if action == "drop":
                # Closing without a reply is what the client sees when the request
                # is lost on the way.
                self.close_connection = True
                return
            reply = self.answer(handle)
            if action == "duplicate":
                reply = self.answer(handle)
            if action == "drop_response":
                self.close_connection = True
                return
            self.respond(*reply)

        def read_body(self) -> dict:
            size = int(self.headers.get("Content-Length", "0"))
            if size < 0:
                raise APIError(HTTPStatus.BAD_REQUEST, f"invalid Content-Length {size}")
            # /replicate carries a batch of records, so it gets a larger limit.
            limit = MAX_REPLICATE_REQUEST_BYTES if self.path == "/replicate" else MAX_REQUEST_BYTES
            if size > limit:
                raise APIError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                               f"request body is {size} bytes; the limit is {limit}")
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

            def handle() -> dict:
                if parsed.path == "/health":
                    return {"broker_id": state.broker_id, "ok": True}
                if parsed.path == "/fetch":
                    return state.fetch(
                        required("topic"),
                        integer("partition", required("partition")),
                        integer("offset", query.get("offset", ["0"])[0]),
                        integer("limit", query.get("limit", ["100"])[0]))
                if parsed.path == "/status":
                    return state.status(required("topic"),
                                        integer("partition", required("partition")))
                if parsed.path == "/faults":
                    return state.fault_rules()
                raise APIError(HTTPStatus.NOT_FOUND, f"unknown endpoint GET {parsed.path}")

            self.serve(parsed.path, handle)

        def do_POST(self) -> None:
            # Taken before any incoming delay fault, which produce() counts as waiting.
            arrived = time.monotonic()
            # The body is read once, before any fault rule, so a duplicated request
            # is handled twice with the same body.
            try:
                body = self.read_body()
            except (APIError, ValueError) as exc:
                self.respond(*self.error_reply(exc))
                return

            def handle() -> dict:
                if self.path == "/produce":
                    return state.produce(body, arrived)
                handler = post_routes.get(self.path)
                if handler is None:
                    raise APIError(HTTPStatus.NOT_FOUND, f"unknown endpoint POST {self.path}")
                return handler(body)

            self.serve(self.path, handle)

    return BrokerHandler


def run_broker(broker_id: str, config: dict, controller_url: str, host: str, port: int,
               data_dir: str | Path) -> None:
    state = BrokerState(broker_id, config, controller_url, data_dir)
    for loop in (state.heartbeat_loop, state.maintenance_loop):
        threading.Thread(target=loop, daemon=True).start()
    server = ThreadingHTTPServer((host, port), _handler_for(state))
    server.daemon_threads = True
    try:
        server.serve_forever()
    finally:
        state.stop_event.set()
        server.server_close()
