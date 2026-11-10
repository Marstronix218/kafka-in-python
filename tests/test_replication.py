"""Replication and failover tests with three brokers in one process.

Each broker's _send is replaced so that its calls go straight to a ControllerState
or to another BrokerState's methods. Fault rules still apply, since they sit in
_request around _send. A test can then stop a broker, block one kind of request, and
run maintenance steps one at a time instead of waiting for threads.

client_request stands in for eventlog.client.request_json, so a ClusterClient or a
Producer can talk to the same brokers.
"""

import json
import tempfile
import threading
import unittest
from http import HTTPStatus
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from eventlog.broker import CONTROLLER, BrokerState
from eventlog.common import NOT_LEADER, UNAVAILABLE, APIError
from eventlog.controller import ControllerState, ISRUpdateRejected


TOPIC = "events"
CONFIG = {
    "brokers": {"b1": "http://b1", "b2": "http://b2", "b3": "http://b3"},
    "topics": {TOPIC: {"partitions": 1, "replication_factor": 3}},
}


class InProcessCluster:
    def __init__(self, data_dir: Path):
        # Heartbeats never time out here. stop() removes a broker's heartbeat instead.
        self.controller = ControllerState(CONFIG, heartbeat_timeout=3600)
        self.data_dir = data_dir
        self.brokers: dict[str, BrokerState] = {}
        self.down: set[str] = set()
        # (sender, target, path) triples whose requests fail as if the network lost them.
        self.blocked: set[tuple[str, str, str]] = set()
        # Called with the broker id after an incoming fault rule drops a reply.
        self.after_dropped_reply = None
        for broker_id in CONFIG["brokers"]:
            self.start(broker_id)

    def start(self, broker_id: str) -> BrokerState:
        """Start a broker, or restart it from its data directory with fresh memory."""
        broker = BrokerState(broker_id, CONFIG, "http://controller", self.data_dir / broker_id)

        def deliver(target, path, body, timeout):
            return self._deliver(broker_id, target, path, body)

        broker._send = deliver
        self.brokers[broker_id] = broker
        self.down.discard(broker_id)
        self.controller.heartbeat(broker_id)
        return broker

    def stop(self, broker_id: str) -> None:
        self.down.add(broker_id)
        self.controller._heartbeats.pop(broker_id, None)

    def _deliver(self, sender: str, target: str, path: str, body: dict | None) -> dict:
        endpoint = urlparse(path).path
        if sender in self.down or target in self.down or (sender, target, endpoint) in self.blocked:
            raise APIError(UNAVAILABLE, f"{sender} cannot reach {target}{endpoint}")
        # A JSON round trip, so no dict is shared between brokers.
        body = None if body is None else json.loads(json.dumps(body))
        if target == CONTROLLER:
            return self._controller(path, body)
        handlers = {"/replicate": self.brokers[target].replicate,
                    "/commit": self.brokers[target].commit_replica}
        return handlers[endpoint](body)

    def _controller(self, path: str, body: dict | None) -> dict:
        parsed = urlparse(path)
        if parsed.path == "/route":
            query = parse_qs(parsed.query)
            topic, partition = query["topic"][0], int(query["partition"][0])
            route = self.controller.route(topic, partition)
            if route is None:
                raise APIError(UNAVAILABLE, self.controller.unavailable_reason(topic, partition))
            return route
        if parsed.path == "/health":
            return self.controller.health()
        if parsed.path == "/metadata":
            return self.controller.metadata()
        if parsed.path == "/heartbeat":
            self.controller.heartbeat(body["broker_id"])
            return {"status": "ok"}
        try:
            return self.controller.alter_isr(body["topic"], body["partition"], body["leader"],
                                             body["leader_epoch"], body["isr"])
        except ISRUpdateRejected as exc:
            raise APIError(HTTPStatus.CONFLICT, str(exc)) from exc

    def client_request(self, url: str, method: str = "GET", payload: dict | None = None,
                       timeout: float = 1.5) -> dict:
        """Answer a client's request to the controller or a broker's /produce.

        Incoming fault rules on the broker apply the way its HTTP handler applies them.
        """
        parsed = urlparse(url)
        body = None if payload is None else json.loads(json.dumps(payload))
        if parsed.netloc == "controller":
            return self._controller(parsed.path + "?" + parsed.query, body)
        broker_id = parsed.netloc
        if broker_id in self.down:
            raise APIError(UNAVAILABLE, f"cannot reach {url}")
        broker = self.brokers[broker_id]
        handler = {"/produce": broker.produce}[parsed.path]
        action = broker.faults.incoming(parsed.path)
        if action == "drop":
            raise APIError(UNAVAILABLE, f"cannot reach {url}")
        if action is None:
            return handler(body)
        try:
            handler(body)
        except APIError:
            pass
        if action == "duplicate":
            return handler(body)
        # drop_response: the request was handled, and the client never hears back.
        if self.after_dropped_reply is not None:
            self.after_dropped_reply(broker_id)
        raise APIError(UNAVAILABLE, f"no response from {url}")

    def route(self) -> dict:
        return self._controller(f"/route?topic={TOPIC}&partition=0", None)

    def produce(self, broker_id: str, value: object, acks: int = 2, **fields) -> dict:
        return self.brokers[broker_id].produce(
            {"topic": TOPIC, "partition": 0, "key": None, "value": value, "acks": acks,
             **fields})

    def values(self, broker_id: str) -> list:
        """Every record value in a broker's log, committed or not."""
        log = self.brokers[broker_id].log(TOPIC, 0)
        return [record["value"] for record in log.read_range(0, log.last_offset + 1)]

    def committed(self, broker_id: str) -> int:
        return self.brokers[broker_id].log(TOPIC, 0).committed_offset


class CountingLock:
    """An RLock that counts how many times it was acquired."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.holds = 0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        acquired = self._lock.acquire(blocking, timeout)
        self.holds += acquired
        return acquired

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> "CountingLock":
        self.acquire()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()


class ReplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self.cluster = InProcessCluster(Path(self._temporary_directory.name))
        self.assertEqual("b1", self.cluster.route()["leader"])

    def tearDown(self) -> None:
        self._temporary_directory.cleanup()

    def test_new_leader_keeps_and_commits_records_whose_commit_it_missed(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        cluster.blocked |= {("b1", "b2", "/commit"), ("b1", "b3", "/commit")}
        cluster.produce("b1", "b", acks=3)
        cluster.produce("b1", "c", acks=3)
        # Without /commit a follower hears of a commit only in the next batch, so "c"
        # is acknowledged but uncommitted on the followers.
        self.assertEqual((["a", "b", "c"], 1), (cluster.values("b2"), cluster.committed("b2")))

        cluster.stop("b1")
        self.assertEqual(("b2", 1), (cluster.route()["leader"], cluster.route()["leader_epoch"]))
        # The old code dropped b2's uncommitted tail here and wrote "d" at offset 1.
        acknowledgement = cluster.produce("b2", "d")

        self.assertEqual(3, acknowledgement["offset"])
        for broker_id in ("b2", "b3"):
            self.assertEqual(["a", "b", "c", "d"], cluster.values(broker_id))
            self.assertEqual(3, cluster.committed(broker_id))
        self.assertEqual(["b2", "b3"], cluster.route()["isr"])

    def test_maintenance_commits_an_inherited_tail_without_new_writes(self) -> None:
        cluster = self.cluster
        cluster.blocked |= {("b1", "b2", "/commit"), ("b1", "b3", "/commit")}
        cluster.produce("b1", "a", acks=3)
        cluster.stop("b1")
        self.assertEqual("b2", cluster.route()["leader"])
        self.assertEqual(-1, cluster.committed("b2"))

        cluster.brokers["b2"].maintain(TOPIC, 0)

        self.assertEqual([0, 0], [cluster.committed("b2"), cluster.committed("b3")])
        fetched = cluster.brokers["b2"].fetch(TOPIC, 0, 0, 10)["records"]
        self.assertEqual(["a"], [record["value"] for record in fetched])

    def test_new_leader_commits_its_inherited_tail_before_the_first_fetch(self) -> None:
        cluster = self.cluster
        cluster.blocked |= {("b1", "b2", "/commit"), ("b1", "b3", "/commit")}
        cluster.produce("b1", "a", acks=3)
        cluster.produce("b1", "b", acks=3)
        cluster.stop("b1")
        self.assertEqual(("b2", 0), (cluster.route()["leader"], cluster.committed("b2")))

        # No maintenance step has run on b2. A consumer that read both records from
        # b1 must still be able to commit position 2.
        reply = cluster.brokers["b2"].fetch(TOPIC, 0, 0, 10)

        self.assertEqual(1, reply["committed_offset"])
        self.assertEqual(["a", "b"], [record["value"] for record in reply["records"]])

    def test_follower_outside_isr_replaces_old_epoch_tail_before_rejoining(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        # b1 reaches b3 but not b2, and can't tell the controller to shrink the ISR,
        # so its writes fail and stay uncommitted on b1 and b3.
        cluster.blocked |= {("b1", "b2", "/replicate"), ("b1", CONTROLLER, "/isr")}
        for value in ("old-1", "old-2", "old-3", "old-4", "old-5"):
            with self.assertRaises(APIError):
                cluster.produce("b1", value)
        self.assertEqual(["a", "old-1", "old-2", "old-3", "old-4", "old-5"], cluster.values("b3"))
        self.assertEqual(0, cluster.committed("b3"))

        cluster.stop("b1")
        cluster.stop("b3")
        self.assertEqual("b2", cluster.route()["leader"])
        for value in ("new-1", "new-2", "new-3"):
            cluster.produce("b2", value, acks=1)
        self.assertEqual(["b2"], cluster.route()["isr"])

        # b3 comes back with a longer log than the leader's committed offset. Its
        # records 1-3 are not the leader's, so it must not commit them.
        cluster.start("b3")
        cluster.brokers["b2"].maintain(TOPIC, 0)

        self.assertEqual(["a", "new-1", "new-2", "new-3"], cluster.values("b3"))
        self.assertEqual(3, cluster.committed("b3"))
        self.assertEqual(["b2", "b3"], cluster.route()["isr"])

        cluster.stop("b2")
        self.assertEqual("b3", cluster.route()["leader"])
        fetched = cluster.brokers["b3"].fetch(TOPIC, 0, 0, 10)["records"]
        self.assertEqual(["a", "new-1", "new-2", "new-3"], [record["value"] for record in fetched])

    def test_stale_leader_is_fenced_and_its_record_is_replaced(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        old_route = cluster.route()

        # b1 misses heartbeats while still running, and b2 takes over at epoch 1.
        cluster.controller._heartbeats.pop("b1")
        self.assertEqual(("b2", 1), (cluster.route()["leader"], cluster.route()["leader_epoch"]))
        cluster.controller.heartbeat("b1")
        # b1 read the route just before the election and writes as if it still led.
        cluster.brokers["b1"].route = lambda topic, partition: dict(old_route)
        with self.assertRaises(APIError) as caught:
            cluster.produce("b1", "stale")

        self.assertEqual(NOT_LEADER, caught.exception.status)
        self.assertEqual(["a", "stale"], cluster.values("b1"))
        self.assertEqual(0, cluster.committed("b1"))
        self.assertEqual(["a"], cluster.values("b2"))

        del cluster.brokers["b1"].route
        cluster.produce("b2", "fresh", acks=3)
        self.assertEqual(["a", "fresh"], cluster.values("b1"))
        self.assertEqual(1, cluster.committed("b1"))

    def test_produce_is_rejected_before_append_when_isr_is_smaller_than_acks(self) -> None:
        cluster = self.cluster
        cluster.stop("b2")
        cluster.stop("b3")
        with self.assertRaisesRegex(APIError, "not enough replicas after append"):
            cluster.produce("b1", "a", acks=2)
        self.assertEqual(["b1"], cluster.route()["isr"])

        with self.assertRaisesRegex(APIError, "not enough replicas: acks=2") as caught:
            cluster.produce("b1", "b", acks=2)

        self.assertEqual(UNAVAILABLE, caught.exception.status)
        self.assertEqual(["a"], cluster.values("b1"))
        self.assertEqual(1, cluster.produce("b1", "c", acks=1)["offset"])

    def test_restarted_follower_catches_up_in_batches_and_rejoins_isr(self) -> None:
        cluster = self.cluster
        cluster.stop("b3")
        for index in range(250):
            cluster.produce("b1", index)
        self.assertEqual(["b1", "b2"], cluster.route()["isr"])

        cluster.start("b3")
        cluster.brokers["b1"].maintain(TOPIC, 0)

        self.assertEqual(list(range(250)), cluster.values("b3"))
        self.assertEqual(249, cluster.committed("b3"))
        self.assertEqual(["b1", "b2", "b3"], cluster.route()["isr"])

    def test_late_replicate_batch_does_not_truncate_records_sent_after_it(self) -> None:
        cluster = self.cluster
        deliver = cluster._deliver
        held = []
        late_results = []

        def late_batch(sender, target, path, body):
            if (sender, target, path) == ("b1", "b3", "/replicate") and body["records"] \
                    and body["records"][0]["value"] == "c" and not held:
                # b3's handler is stuck (on its lock, or an incoming delay), and b1
                # gives up after PEER_TIMEOUT_SECONDS.
                held.append(body)
                raise APIError(UNAVAILABLE, "no response (timeout)")
            if (sender, target, path) == ("b1", "b3", "/commit") and held:
                # The stuck handler gets b3's lock just before the /commit for "d".
                try:
                    late_results.append(cluster.brokers["b3"].replicate(held.pop()))
                except APIError as exc:
                    late_results.append(exc)
            return deliver(sender, target, path, body)

        cluster._deliver = late_batch
        cluster.produce("b1", "a")
        cluster.produce("b1", "b")
        cluster.produce("b1", "c")
        self.assertEqual(["b1", "b2"], cluster.route()["isr"])
        cluster.brokers["b1"].maintain(TOPIC, 0)
        self.assertEqual(["b1", "b2", "b3"], cluster.route()["isr"])

        self.assertEqual(3, cluster.produce("b1", "d")["offset"])

        [late] = late_results
        self.assertIsInstance(late, APIError)
        self.assertEqual(HTTPStatus.CONFLICT, late.status)
        self.assertIn("stale batch", str(late))
        self.assertEqual({}, late.fields)
        self.assertEqual(["a", "b", "c", "d"], cluster.values("b3"))
        cluster.brokers["b1"].maintain(TOPIC, 0)
        self.assertEqual(3, cluster.committed("b3"))

        # The leader and the other follower fail. b3 must still have "d".
        cluster.stop("b1")
        cluster.stop("b2")
        self.assertEqual("b3", cluster.route()["leader"])
        fetched = cluster.brokers["b3"].fetch(TOPIC, 0, 0, 10)["records"]
        self.assertEqual(["a", "b", "c", "d"], [record["value"] for record in fetched])

    def test_failed_commit_makes_the_leader_check_the_follower_again(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        deliver = cluster._deliver

        def lose_tail(sender, target, path, body):
            if (sender, target, path) == ("b1", "b3", "/commit") and \
                    cluster.values("b3") == ["a", "b"]:
                # b3 loses "b" after b1 sent it. The 409 from this /commit is all
                # b1 hears about it.
                cluster.brokers["b3"].log(TOPIC, 0).truncate_from(1)
            return deliver(sender, target, path, body)

        cluster._deliver = lose_tail
        cluster.produce("b1", "b", acks=3)
        self.assertEqual((["a"], 0), (cluster.values("b3"), cluster.committed("b3")))
        cluster._deliver = deliver

        cluster.brokers["b1"].maintain(TOPIC, 0)

        self.assertEqual((["a", "b"], 1), (cluster.values("b3"), cluster.committed("b3")))

    def test_replicate_with_a_lower_leader_last_offset_changes_nothing(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        # b2 gets "b" but not the /commit after it, so "b" is uncommitted there.
        cluster.blocked.add(("b1", "b2", "/commit"))
        cluster.produce("b1", "b", acks=3)
        route = cluster.route()
        body = {"topic": TOPIC, "partition": 0, "leader": "b1",
                "leader_epoch": route["leader_epoch"], "leader_last_offset": 0,
                "committed_offset": 0, "records": []}

        with self.assertRaises(APIError) as caught:
            cluster.brokers["b2"].replicate(body)

        self.assertEqual(HTTPStatus.CONFLICT, caught.exception.status)
        self.assertIn("stale batch", str(caught.exception))
        self.assertEqual({}, caught.exception.fields)
        self.assertEqual((["a", "b"], 0), (cluster.values("b2"), cluster.committed("b2")))
        # A restarted follower keeps no record of earlier batches and accepts this one.
        cluster.start("b2")
        reply = cluster.brokers["b2"].replicate({**body, "leader_last_offset": 1})
        self.assertEqual({"last_offset": 1, "committed_offset": 0}, reply)

    def test_duplicated_replicate_requests_are_applied_twice_without_harm(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        cluster.brokers["b1"].faults.set_rules(
            [{"action": "duplicate", "direction": "out", "path": "/replicate"}])

        self.assertEqual(1, cluster.produce("b1", "b", acks=3)["offset"])
        cluster.produce("b1", "c", acks=3)

        self.assertEqual(["b1", "b2", "b3"], cluster.route()["isr"])
        for broker_id in ("b2", "b3"):
            self.assertEqual((["a", "b", "c"], 2),
                             (cluster.values(broker_id), cluster.committed(broker_id)))

    def test_late_batch_from_before_a_leader_restart_in_the_same_epoch_changes_nothing(
            self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        deliver = cluster._deliver
        held = []

        def hold(sender, target, path, body):
            if (sender, target, path) == ("b1", "b3", "/replicate") and not held:
                held.append(body)
                raise APIError(UNAVAILABLE, "no response (timeout)")
            return deliver(sender, target, path, body)

        cluster._deliver = hold
        cluster.produce("b1", "b")
        cluster._deliver = deliver
        epoch = cluster.route()["leader_epoch"]
        # b1 restarts before the controller notices, so its epoch stays the same.
        cluster.start("b1")
        cluster.brokers["b1"].maintain(TOPIC, 0)
        cluster.blocked.add(("b1", "b3", "/commit"))
        self.assertEqual(2, cluster.produce("b1", "c", acks=3)["offset"])
        self.assertEqual(epoch, cluster.route()["leader_epoch"])

        with self.assertRaisesRegex(APIError, "stale batch"):
            cluster.brokers["b3"].replicate(held[0])

        self.assertEqual(["a", "b", "c"], cluster.values("b3"))

    def test_first_fetch_on_a_new_leader_syncs_again_after_a_failed_sync(self) -> None:
        cluster = self.cluster
        # "a" reaches every replica, but only b1 commits it.
        cluster.blocked |= {("b1", "b2", "/commit"), ("b1", "b3", "/commit")}
        cluster.produce("b1", "a", acks=3)
        cluster.stop("b1")
        self.assertEqual("b2", cluster.route()["leader"])
        self.assertEqual(-1, cluster.committed("b2"))
        # The first fetch fails to push to b3 and then to tell the controller.
        cluster.blocked |= {("b2", "b3", "/replicate"), ("b2", CONTROLLER, "/isr")}
        with self.assertRaises(APIError):
            cluster.brokers["b2"].fetch(TOPIC, 0, 0, 10)
        cluster.blocked.clear()

        reply = cluster.brokers["b2"].fetch(TOPIC, 0, 0, 10)

        self.assertEqual((["a"], 0), ([record["value"] for record in reply["records"]],
                                      reply["committed_offset"]))

    def test_catch_up_takes_the_partition_lock_once_per_batch(self) -> None:
        cluster = self.cluster
        cluster.stop("b3")
        for index in range(250):
            cluster.produce("b1", index)
        cluster.start("b3")
        lock = CountingLock()
        cluster.brokers["b1"].locks[(TOPIC, 0)] = lock
        deliver = cluster._deliver
        holds_per_batch = []

        def count(sender, target, path, body):
            if (sender, target, path) == ("b1", "b3", "/replicate"):
                holds_per_batch.append(lock.holds)
            return deliver(sender, target, path, body)

        cluster._deliver = count
        cluster.brokers["b1"].maintain(TOPIC, 0)

        # 250 records at 100 per batch, after one request that finds where b3 is.
        self.assertGreaterEqual(len(holds_per_batch), 3)
        self.assertEqual(len(holds_per_batch), len(set(holds_per_batch)))
        self.assertEqual(list(range(250)), cluster.values("b3"))
        self.assertEqual(["b1", "b2", "b3"], cluster.route()["isr"])

    def test_replicate_conflict_reports_follower_offsets(self) -> None:
        cluster = self.cluster
        cluster.produce("b1", "a", acks=3)
        route = cluster.route()
        body = {"topic": TOPIC, "partition": 0, "leader": "b1",
                "leader_epoch": route["leader_epoch"], "leader_last_offset": 5,
                "committed_offset": 5,
                "records": [{"offset": 5, "key": None, "value": "x", "timestamp": 1.0}]}

        with self.assertRaises(APIError) as gap:
            cluster.brokers["b2"].replicate(body)
        with self.assertRaises(APIError) as stale:
            cluster.brokers["b2"].replicate({**body, "leader_epoch": route["leader_epoch"] - 1})

        self.assertEqual(HTTPStatus.CONFLICT, gap.exception.status)
        self.assertEqual({"committed_offset": 0, "last_offset": 0}, gap.exception.fields)
        self.assertEqual(NOT_LEADER, stale.exception.status)
        self.assertIn("leader epoch", str(stale.exception))


if __name__ == "__main__":
    unittest.main()
