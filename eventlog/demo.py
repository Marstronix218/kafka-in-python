"""The week-15 demo: faults injected into a local cluster while a producer keeps writing.

Run it with `python -m eventlog demo` (about 3 minutes), `--fast` (about 15 seconds),
or `--pause` (waits for Enter before each step, for a narrated recording).
"""

from __future__ import annotations

import signal
import sys
import textwrap
import threading
import time
from typing import Callable

from .bench import account, read_partition, tag_of, tagged_value, wait_until_settled
from .client import Producer
from .cluster import LocalCluster, wait_until
from .common import APIError


TOPIC = "events"
PARTITIONS = 3
ACKS = 2
WRITES_PER_SECOND_PER_PARTITION = 10
LOST_REPLY_PROBABILITY = 0.3
# Pauses in the normal (not --fast) mode, sized for a 3 to 4 minute recording.
READING_SECONDS = 5.0
PROGRESS_SECONDS = 5.0


class Writer:
    """One thread per partition writing small records through a shared idempotent
    Producer, and counting what came back."""

    def __init__(self, cluster: LocalCluster):
        self.producer = Producer(cluster.client)
        self.lock = threading.Lock()
        # tag -> (partition, offset, value) of every acknowledged record.
        self.acked: dict = {}
        self.failed: set = set()
        self.duplicate_replies = 0
        self.stop_event = threading.Event()
        self.threads = [threading.Thread(target=self._run, args=(partition,), daemon=True)
                        for partition in range(PARTITIONS)]

    def start(self) -> None:
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        for thread in self.threads:
            # A write in progress can take a few seconds to give up during a failover.
            thread.join(timeout=30)

    def counts(self) -> tuple[int, int, int]:
        """Return (acknowledged, failed, answered as duplicate) so far."""
        with self.lock:
            return len(self.acked), len(self.failed), self.duplicate_replies

    def _run(self, partition: int) -> None:
        number = 0
        while not self.stop_event.wait(1 / WRITES_PER_SECOND_PER_PARTITION):
            value = tagged_value(partition, number, 0)
            number += 1
            try:
                reply = self.producer.produce(TOPIC, value, partition=partition, acks=ACKS,
                                              retries=10)
            except APIError:
                with self.lock:
                    self.failed.add(tag_of(value))
                continue
            with self.lock:
                self.acked[tag_of(value)] = (partition, reply["offset"], value)
                self.duplicate_replies += bool(reply["duplicate"])


class Demo:
    def __init__(self, fast: bool, pause: bool, format_status: Callable[[dict], str]):
        self.fast = fast
        self.pause = pause
        self.format_status = format_status
        self.step_number = 0
        self.cluster: LocalCluster | None = None
        self.writer: Writer | None = None
        self.result: dict | None = None

    # Output helpers.

    def say(self, text: str) -> None:
        """Print a paragraph, then give the viewer time to read it."""
        print(textwrap.fill(text, width=88), flush=True)
        if not self.fast:
            time.sleep(READING_SECONDS)

    def step(self, title: str) -> None:
        self.step_number += 1
        print(flush=True)
        heading = f"Step {self.step_number}: {title}"
        print(heading + "\n" + "-" * len(heading), flush=True)
        if self.pause:
            input("(press Enter) ")

    def hold(self, seconds: float) -> None:
        """Let the producer run for a while, printing its counts every few seconds."""
        if self.fast:
            seconds = min(seconds, 1.5)
        deadline = time.monotonic() + seconds
        while (left := deadline - time.monotonic()) > 0:
            time.sleep(min(left, PROGRESS_SECONDS))
            self.progress()

    def progress(self) -> None:
        if self.writer is None:
            return
        acked, failed, duplicates = self.writer.counts()
        print(f"  writes so far: {acked} acknowledged, {failed} failed, "
              f"{duplicates} retries answered as duplicates", flush=True)

    def status(self) -> None:
        text = self.format_status(self.cluster.client.cluster_status())
        print("\n" + textwrap.indent(text, "  ") + "\n", flush=True)

    # Cluster helpers.

    def route(self, partition: int = 0) -> dict:
        return self.cluster.client.route(TOPIC, partition)

    def wait_for_isr_without(self, broker_id: str, partitions: range | list[int],
                             timeout: float = 20) -> None:
        wait_until(lambda: all(broker_id not in self.route(p)["isr"] for p in partitions),
                   timeout, f"{broker_id} to leave the ISR")

    # The demo itself.

    def run(self) -> None:
        heartbeat_timeout = 1.0 if self.fast else 2.0
        topics = {TOPIC: {"partitions": PARTITIONS, "replication_factor": 3}}
        self.step("start the cluster")
        self.say(f"Starting a controller and three brokers (b1, b2, b3) as separate "
                 f"processes on local ports. Topic {TOPIC} has {PARTITIONS} partitions, "
                 f"each stored on all three brokers. The controller declares a broker dead "
                 f"after {heartbeat_timeout:g}s without a heartbeat.")
        self.cluster = LocalCluster(3, topics, heartbeat_timeout)
        self.cluster.start()
        self.status()

        self.step("start writing")
        self.say(f"One producer thread per partition, about "
                 f"{WRITES_PER_SECOND_PER_PARTITION} writes per second each, with "
                 f"acks={ACKS}: a write is accepted only while at least {ACKS} replicas "
                 f"are in sync, and acknowledged once every in-sync replica has it. Every "
                 f"record carries the producer's id and a sequence number.")
        self.writer = Writer(self.cluster)
        self.writer.start()
        self.hold(20)
        self.status()

        self.step("kill a leader")
        victim = self.route(0)["leader"]
        led = [p for p in range(PARTITIONS) if self.route(p)["leader"] == victim]
        names = ", ".join(f"{TOPIC}:{p}" for p in led)
        self.say(f"Killing {victim}, the leader of {names}, with SIGKILL. Writes to the "
                 f"other partitions keep going. Writes to {names} wait and retry until the "
                 f"controller elects a new leader from the in-sync replicas.")
        self.cluster.kill_broker(victim)
        killed_at = time.monotonic()
        new_route = wait_until(lambda: (lambda r: r if r["leader"] != victim else None)(
            self.route(0)), 30, "a new leader for partition 0")
        self.say(f"{new_route['leader']} leads {TOPIC}:0 after "
                 f"{time.monotonic() - killed_at:.1f}s, at leader epoch "
                 f"{new_route['leader_epoch']}. The new leaders drop {victim} from their "
                 f"ISRs when they fail to reach it.")
        self.wait_for_isr_without(victim, range(PARTITIONS))
        self.hold(20)
        self.status()
        self.say(f"While {names} had no leader, the producer retried those writes with "
                 f"the same sequence numbers.")

        self.step("restart the broker")
        self.say(f"Restarting {victim} on its old data directory. It still has the records "
                 f"it stored before the kill. Each leader pushes it the records it missed, "
                 f"then adds it back to the ISR. Leadership does not move back.")
        self.cluster.start_broker(victim)
        self.status()
        restarted_at = time.monotonic()
        self.cluster.wait_for_full_isr(60)
        self.say(f"{victim} caught up and is back in every ISR after "
                 f"{time.monotonic() - restarted_at:.1f}s.")
        self.hold(10)
        self.status()

        self.step("partition the network")
        route = self.route(0)
        leader = route["leader"]
        follower = next(b for b in route["replicas"] if b not in (leader, victim))
        self.say(f"Cutting the network between {leader} (leader of {TOPIC}:0) and "
                 f"{follower}: {leader} drops every request to {follower} and the other way "
                 f"round. Both still reach the controller, so neither is declared dead. "
                 f"Each removes the other from the ISR of the partitions it leads, because "
                 f"it can no longer replicate to it.")
        self.cluster.client.set_faults(leader, [{"action": "drop", "to": follower}])
        self.cluster.client.set_faults(follower, [{"action": "drop", "to": leader}])
        leaders = [self.route(p)["leader"] for p in range(PARTITIONS)]
        self.wait_for_isr_without(follower, [p for p, b in enumerate(leaders) if b == leader])
        self.wait_for_isr_without(leader, [p for p, b in enumerate(leaders) if b == follower])
        self.hold(20)
        self.status()
        self.say(f"Writes continue: the ISR of {TOPIC}:0 still has 2 members, which "
                 f"acks={ACKS} allows.")

        self.step("heal the network")
        self.say("Clearing both fault tables. The leader pushes the missed records and adds "
                 f"{follower} back to the ISR.")
        self.cluster.client.set_faults(leader, [])
        self.cluster.client.set_faults(follower, [])
        self.cluster.wait_for_full_isr(60)
        self.hold(10)
        self.status()

        self.step("lose produce replies")
        self.say(f"Every broker now handles each /produce but drops the reply "
                 f"{LOST_REPLY_PROBABILITY:.0%} of the time. The producer can't tell whether "
                 f"the record was stored, so it sends it again with the same producer id and "
                 f"sequence. The leader finds that sequence in its log and returns the stored "
                 f"offset instead of appending a second copy.")
        _, _, duplicates_before = self.writer.counts()
        for offset, broker_id in enumerate(self.cluster.broker_urls):
            self.cluster.client.set_faults(
                broker_id, [{"action": "drop_response", "direction": "in", "path": "/produce",
                             "probability": LOST_REPLY_PROBABILITY}], seed=offset)
        self.hold(30)
        self.status()
        for broker_id in self.cluster.broker_urls:
            self.cluster.client.set_faults(broker_id, [])
        _, _, duplicates_after = self.writer.counts()
        self.say(f"Rules cleared. {duplicates_after - duplicates_before} retries were "
                 f"answered with an offset the leader had already stored.")

        self.step("stop writing and check every record")
        self.writer.stop()
        self.progress()
        self.say("Waiting until every partition has a full ISR and has committed its whole "
                 "log, then reading every partition back from offset 0.")
        wait_until_settled(self.cluster)
        stored = {partition: read_partition(self.cluster.client, TOPIC, partition)
                  for partition in range(PARTITIONS)}
        counts = account(self.writer.acked, self.writer.failed, stored)
        self.status()
        print(f"  acknowledged writes:          {counts['acknowledged']}")
        print(f"  writes that gave up:          {counts['failed']}")
        print(f"  records stored:               {counts['stored']}")
        print(f"  stored more than once:        {counts['duplicates']}")
        print(f"  acknowledged but missing:     {counts['lost']}")
        if counts["failed_but_stored"]:
            print(f"  gave up but stored anyway:    {counts['failed_but_stored']}")
        print(flush=True)
        if counts["duplicates"] == 0 and counts["lost"] == 0:
            self.say("Every acknowledged write is stored exactly once.")
        else:
            self.say("Some writes were lost or stored twice. See the counts above.")
        self.result = counts

    def close(self) -> None:
        if self.writer is not None:
            self.writer.stop_event.set()
        if self.cluster is not None:
            self.cluster.stop()


def run_demo(args, format_status: Callable[[dict], str]) -> dict:
    """Entry point for `python -m eventlog demo`. Returns the final counts."""
    # SIGTERM gets the same cleanup as Ctrl-C: the finally below stops every process.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    demo = Demo(args.fast, args.pause, format_status)
    try:
        demo.run()
        return demo.result
    except KeyboardInterrupt:
        print("\nInterrupted. Stopping the cluster.", file=sys.stderr, flush=True)
        raise SystemExit(130)
    finally:
        demo.close()
