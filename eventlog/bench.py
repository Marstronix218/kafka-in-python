"""Benchmark: write N records to a local cluster, optionally under a fault, then read
every partition back and check that each acknowledged record is stored exactly once, at
the offset it was acknowledged with. Then stop the cluster and check that every replica
committed the same records as its leader.

Run it with `python -m eventlog bench --help`. Results go to results/*.json.

Kafka reference: ProducerPerformance (tools; per-record latency percentiles and records/s).
"""

from __future__ import annotations

import json
import math
import os
import platform
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from .client import ClusterClient, Producer
from .cluster import ROOT, LocalCluster, wait_until
from .common import APIError, request_json
from .storage import PartitionLog


TOPIC = "events"
FAULTS = ("none", "kill-leader", "lost-replies", "slow-follower")
SAMPLE_INTERVAL_SECONDS = 0.25
SETTLE_SECONDS = 120.0
FETCH_PAGE = 1000
# Every value starts with "<worker>/<index>;", which identifies the record. The
# producer id and sequence can't: a record retried under a new producer id has two.
TAG_END = ";"


def percentile(values: list[float], q: float) -> float | None:
    """Return the q-th percentile (0 < q <= 100) by nearest rank, or None for no values.

    Nearest rank always returns one of the measured values, so p100 is the maximum.
    """
    if not 0 < q <= 100:
        raise ValueError(f"percentile must be in (0, 100], got {q}")
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(q / 100 * len(ordered)) - 1]


def tagged_value(worker: int, index: int, size: int) -> str:
    """Return a value of `size` characters that starts with the tag "<worker>/<index>;".
    A size shorter than the tag gives the tag alone."""
    tag = f"{worker}/{index}{TAG_END}"
    return tag + "x" * (size - len(tag))


def tag_of(value: object) -> str | None:
    """Return the tag at the start of a value from tagged_value, or None if it has none."""
    if not isinstance(value, str) or TAG_END not in value:
        return None
    return value.split(TAG_END, 1)[0]


def account(acked: dict, failed: set, stored: dict) -> dict:
    """Compare what producers were told with what the log holds.

    acked maps the tag of each acknowledged record to (partition, offset, value) from
    its reply. failed holds the tags of records whose produce call gave up; each may
    or may not be stored. stored maps each partition to its records as read back.
    lost counts acknowledged records whose acknowledged offset is missing or holds
    another value. duplicates counts extra copies of a tag (stored 3 times adds 2).
    unexpected counts stored records with no tag or a tag no producer in this run
    wrote, which should never happen.
    """
    by_offset = {(partition, record["offset"]): record["value"]
                 for partition, records in stored.items() for record in records}
    tags = [tag_of(record["value"]) for records in stored.values() for record in records]
    counts = Counter(tag for tag in tags if tag is not None)
    unique = set(counts)
    return {
        "acknowledged": len(acked),
        "failed": len(failed),
        "stored": len(tags),
        "duplicates": sum(count - 1 for count in counts.values()),
        "lost": sum(by_offset.get((partition, offset)) != value
                    for partition, offset, value in acked.values()),
        "failed_but_stored": len(failed & unique),
        "unexpected": tags.count(None) + len(unique - set(acked) - failed),
    }


def read_partition(client: ClusterClient, topic: str, partition: int) -> list[dict]:
    """Return every committed record of a partition, read from its leader in pages."""
    records: list[dict] = []
    while True:
        page = client.fetch(topic, partition, len(records), FETCH_PAGE)
        records.extend(page["records"])
        if not page["records"] or len(records) > page["committed_offset"]:
            return records


def wait_until_settled(cluster: LocalCluster, timeout: float = SETTLE_SECONDS) -> None:
    """Wait until every partition has a full ISR, its leader has committed its whole
    log, and every follower has committed up to the same offset. A read then finds
    every record that will ever be committed, and the replicas can be compared.
    Raises TimeoutError after `timeout` seconds."""
    cluster.wait_for_full_isr(timeout)

    def committed() -> bool:
        for topic, partition in cluster.partitions():
            route = cluster.client.route(topic, partition)
            log = _status(route["brokers"][route["leader"]], topic, partition)
            if log["committed_offset"] != log["last_offset"]:
                return False
            for broker_id in route["replicas"]:
                follower = _status(route["brokers"][broker_id], topic, partition)
                if follower["committed_offset"] != log["committed_offset"]:
                    return False
        return True
    wait_until(committed, timeout, "every replica to commit the leader's whole log")


def committed_records(data_dir: Path, topic: str, partition: int) -> list[dict]:
    """Return the committed records of a partition, read from a broker's data directory.
    The broker must be stopped."""
    log = PartitionLog(data_dir, topic, partition)
    return log.read(0, log.committed_offset + 1)


def replica_mismatches(data_dirs: dict[str, Path], routes: dict) -> int:
    """Return how many partitions have a replica whose committed records differ from
    the leader's. routes maps (topic, partition) to its route from the controller, and
    data_dirs maps each broker id to its data directory."""
    mismatched = 0
    for (topic, partition), route in routes.items():
        leader = committed_records(data_dirs[route["leader"]], topic, partition)
        mismatched += any(committed_records(data_dirs[broker_id], topic, partition) != leader
                          for broker_id in route["replicas"] if broker_id != route["leader"])
    return mismatched


def _status(broker_url: str, topic: str, partition: int, timeout: float = 0.5) -> dict:
    query = urlencode({"topic": topic, "partition": partition})
    return request_json(broker_url + "/status?" + query, timeout=timeout)


class LagSampler(threading.Thread):
    """Polls every partition's replicas through /status while the benchmark runs.

    For each sample and partition, isr_lag is how many records the slowest in-sync
    follower is behind the leader's log end, and replica_lag is the same over every
    replica that answered, in sync or not. A replica that doesn't answer is skipped.
    It also counts samples where a partition's ISR was smaller than its replica list.
    """

    def __init__(self, cluster: LocalCluster):
        super().__init__(daemon=True)
        self.cluster = cluster
        self.stop_event = threading.Event()
        self.isr_lags: list[int] = []
        self.replica_lags: list[int] = []
        self.min_isr_size: int | None = None
        self.short_isr_samples = 0

    def run(self) -> None:
        while not self.stop_event.wait(SAMPLE_INTERVAL_SECONDS):
            for topic, partition in self.cluster.partitions():
                try:
                    self._sample(topic, partition)
                except (APIError, OSError, KeyError):
                    pass

    def _sample(self, topic: str, partition: int) -> None:
        route = self.cluster.client.route(topic, partition)
        ends = {}
        for broker_id in route["replicas"]:
            try:
                ends[broker_id] = _status(route["brokers"][broker_id], topic,
                                          partition)["last_offset"]
            except APIError:
                pass
        if route["leader"] not in ends:
            return
        leader_end = ends.pop(route["leader"])
        in_sync = [end for broker_id, end in ends.items() if broker_id in route["isr"]]
        # The leader appends before it replicates, and /status doesn't wait for the
        # partition lock, so a lag of 1 is a write caught in flight.
        self.isr_lags.append(max([leader_end - end for end in in_sync], default=0))
        self.replica_lags.append(max([leader_end - end for end in ends.values()], default=0))
        size = len(route["isr"])
        self.short_isr_samples += size < len(route["replicas"])
        self.min_isr_size = size if self.min_isr_size is None else min(self.min_isr_size, size)


class Workload:
    """Producer threads that share out the events, plus the counters they fill in."""

    def __init__(self, client: ClusterClient, args):
        self.client = client
        self.args = args
        self.lock = threading.Lock()
        # tag -> (partition, offset, value) of every acknowledged record.
        self.acked: dict = {}
        self.failed: set = set()
        self.done = 0
        self.latencies: list[float] = []
        self.duplicate_replies = 0
        self.errors: Counter = Counter()
        self.stop_event = threading.Event()

    def progress(self) -> int:
        with self.lock:
            return self.done

    def run_thread(self, worker: int) -> None:
        # One Producer per thread: a Producer sends one write at a time per partition,
        # so threads sharing one would queue behind each other.
        producer = Producer(self.client)
        args = self.args
        for index in range(worker, args.events, args.threads):
            if self.stop_event.is_set():
                return
            partition = index % args.partitions
            value = tagged_value(worker, index, args.value_size)
            started = time.perf_counter()
            try:
                reply = producer.produce(TOPIC, value, partition=partition,
                                         acks=args.acks, retries=args.retries)
            except Exception as exc:
                # Other errors (a bug, a reply that is not JSON) would otherwise end
                # this thread, and its remaining records would be missing from the counts.
                error = str(exc) if isinstance(exc, APIError) else f"{type(exc).__name__}: {exc}"
                with self.lock:
                    self.failed.add(tag_of(value))
                    self.done += 1
                    self.errors[error[:200]] += 1
                continue
            elapsed = time.perf_counter() - started
            with self.lock:
                self.acked[tag_of(value)] = (partition, reply["offset"], value)
                self.done += 1
                self.latencies.append(elapsed)
                self.duplicate_replies += bool(reply.get("duplicate"))


def _fault_rules(fault: str, cluster: LocalCluster, args) -> dict[str, list[dict]]:
    """Return {broker_id: rules} for a fault that is a rule table rather than a kill."""
    if fault == "lost-replies":
        # The leader stores and replicates the record, then closes the connection
        # without replying. The producer retries with the same sequence.
        rule = {"action": "drop_response", "direction": "in", "path": "/produce",
                "probability": args.fault_probability}
        return {broker_id: [rule] for broker_id in cluster.broker_urls}
    if fault == "slow-follower":
        # Every /replicate the last broker receives waits this long before it is handled.
        slow = list(cluster.broker_urls)[-1]
        return {slow: [{"action": "delay", "direction": "in", "path": "/replicate",
                        "seconds": args.fault_delay}]}
    return {}


def _inject(cluster: LocalCluster, workload: Workload, args, timeline: list) -> None:
    """Apply args.fault while the workload runs, and log what happened to timeline."""
    start = time.monotonic()

    def note(event: str, **fields) -> None:
        timeline.append({"t": round(time.monotonic() - start, 3), "event": event, **fields})

    def wait_for_progress(fraction: float) -> bool:
        while workload.progress() < args.events * fraction:
            if workload.stop_event.wait(0.05):
                return False
        return True

    if args.fault == "kill-leader":
        if not wait_for_progress(1 / 3):
            return
        victim = cluster.leader(TOPIC, 0)
        cluster.kill_broker(victim)
        killed_at = time.monotonic()
        note("killed", broker=victim, leader_of=f"{TOPIC}:0")
        route = wait_until(lambda: (lambda r: r if r["leader"] != victim else None)(
            cluster.client.route(TOPIC, 0)), 30, "a new leader for partition 0")
        note("new_leader", broker=route["leader"], leader_epoch=route["leader_epoch"],
             seconds_after_kill=round(time.monotonic() - killed_at, 3))
        if not wait_for_progress(2 / 3):
            return
        cluster.start_broker(victim)
        restarted_at = time.monotonic()
        note("restarted", broker=victim)
        try:
            cluster.wait_for_full_isr(60)
            note("full_isr_again", broker=victim,
                 seconds_after_restart=round(time.monotonic() - restarted_at, 3))
        except TimeoutError as exc:
            note("full_isr_timeout", error=str(exc))
        return

    rules = _fault_rules(args.fault, cluster, args)
    if not rules:
        return
    for offset, (broker_id, broker_rules) in enumerate(rules.items()):
        cluster.client.set_faults(broker_id, broker_rules, seed=args.seed + offset)
        note("rules_set", broker=broker_id, rules=broker_rules)
    workload.stop_event.wait()
    for broker_id in rules:
        cluster.client.set_faults(broker_id, [])
    note("rules_cleared")


def run_benchmark(args) -> dict:
    """Run one benchmark and return its result (config, metrics, environment)."""
    if args.fault not in FAULTS:
        raise ValueError(f"--fault must be one of {', '.join(FAULTS)}, got {args.fault!r}")
    for name in ("events", "partitions", "threads", "brokers"):
        if getattr(args, name) < 1:
            raise ValueError(f"--{name} must be at least 1")
    if not 1 <= args.acks <= args.replication_factor <= args.brokers:
        raise ValueError("need 1 <= --acks <= --replication-factor <= --brokers")
    if args.value_size < 0:
        raise ValueError("--value-size must not be negative")

    config = {name: getattr(args, name) for name in (
        "events", "partitions", "acks", "threads", "value_size", "fault", "brokers",
        "replication_factor", "retries", "heartbeat_timeout")}
    if args.fault == "lost-replies":
        config["fault_probability"] = args.fault_probability
    if args.fault == "slow-follower":
        config["fault_delay"] = args.fault_delay
    if args.fault != "none":
        config["seed"] = args.seed
    started_at = datetime.now(timezone.utc)
    wall_start = time.monotonic()
    topics = {TOPIC: {"partitions": args.partitions,
                      "replication_factor": args.replication_factor}}

    with LocalCluster(args.brokers, topics, args.heartbeat_timeout) as cluster:
        workload = Workload(cluster.client, args)
        sampler = LagSampler(cluster)
        timeline: list[dict] = []
        injector = threading.Thread(target=_inject, args=(cluster, workload, args, timeline),
                                    daemon=True)
        threads = [threading.Thread(target=workload.run_thread, args=(worker,), daemon=True)
                   for worker in range(args.threads)]
        start_epochs = _leader_epochs(cluster)
        sampler.start()
        injector.start()
        produce_start = time.perf_counter()
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            produce_seconds = time.perf_counter() - produce_start
            workload.stop_event.set()
            sampler.stop_event.set()
        injector.join(timeout=90)
        sampler.join(timeout=5)

        wait_until_settled(cluster)
        end_epochs = _leader_epochs(cluster)
        stored = {partition: read_partition(cluster.client, TOPIC, partition)
                  for partition in range(args.partitions)}
        routes = {key: cluster.client.route(*key) for key in cluster.partitions()}
        # The brokers are local processes, so once they stop, their partition files can
        # be read directly, followers included.
        for broker_id in cluster.broker_urls:
            cluster.stop_broker(broker_id)
        mismatches = replica_mismatches(
            {broker_id: cluster.data_dir(broker_id) for broker_id in cluster.broker_urls},
            routes)

    counts = account(workload.acked, workload.failed, stored)
    latencies_ms = [seconds * 1000 for seconds in workload.latencies]
    metrics = {
        **counts,
        "replica_mismatches": mismatches,
        "duplicate_replies": workload.duplicate_replies,
        "produce_seconds": round(produce_seconds, 3),
        "throughput_per_second": round(counts["acknowledged"] / produce_seconds, 1),
        "latency_ms": {name: _round(percentile(latencies_ms, q))
                       for name, q in (("p50", 50), ("p95", 95), ("p99", 99), ("max", 100))},
        "latency_ms_mean": _round(sum(latencies_ms) / len(latencies_ms)
                                  if latencies_ms else None),
        "isr_lag_max": max(sampler.isr_lags, default=None),
        "isr_lag_mean": _round(sum(sampler.isr_lags) / len(sampler.isr_lags)
                               if sampler.isr_lags else None, 3),
        "replica_lag_max": max(sampler.replica_lags, default=None),
        "lag_samples": len(sampler.isr_lags),
        "min_isr_size": sampler.min_isr_size,
        "short_isr_samples": sampler.short_isr_samples,
        # Each leader change raises the partition's leader epoch by one.
        "leader_changes": sum(end_epochs[key] - start_epochs[key] for key in end_epochs),
        "errors": dict(workload.errors.most_common(5)),
    }
    return {"config": config, "metrics": metrics, "timeline": timeline,
            "started_at": started_at.isoformat(timespec="seconds"),
            "wall_seconds": round(time.monotonic() - wall_start, 1),
            "environment": environment()}


def _leader_epochs(cluster: LocalCluster) -> dict:
    return {(topic, partition): cluster.client.route(topic, partition)["leader_epoch"]
            for topic, partition in cluster.partitions()}


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(value, digits)


def environment() -> dict:
    """Git commit, OS, CPU and Python version, so a result can be traced to a setup."""
    def run(*command: str) -> str | None:
        try:
            return subprocess.run(command, cwd=ROOT, capture_output=True, text=True,
                                  timeout=5, check=True).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None

    cpu = run("sysctl", "-n", "machdep.cpu.brand_string") if sys.platform == "darwin" else None
    status = run("git", "status", "--porcelain", "--untracked-files=no")
    return {
        "git_commit": run("git", "rev-parse", "--short", "HEAD"),
        # True means the code that ran had uncommitted changes on top of git_commit.
        "git_dirty": None if status is None else bool(status),
        "os": platform.platform(),
        "machine": platform.machine(),
        "cpu": cpu or platform.processor() or None,
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
    }


TABLE_COLUMNS = [
    ("events", lambda r: r["config"]["events"]),
    ("partitions", lambda r: r["config"]["partitions"]),
    ("acks", lambda r: r["config"]["acks"]),
    ("threads", lambda r: r["config"]["threads"]),
    ("fault", lambda r: r["config"]["fault"]),
    ("records/s", lambda r: f"{r['metrics']['throughput_per_second']:.0f}"),
    ("p50 ms", lambda r: r["metrics"]["latency_ms"]["p50"]),
    ("p95 ms", lambda r: r["metrics"]["latency_ms"]["p95"]),
    ("p99 ms", lambda r: r["metrics"]["latency_ms"]["p99"]),
    ("max ms", lambda r: r["metrics"]["latency_ms"]["max"]),
    ("isr lag max/mean", lambda r: f"{r['metrics']['isr_lag_max']}/"
                                   f"{r['metrics']['isr_lag_mean']}"),
    ("acked", lambda r: r["metrics"]["acknowledged"]),
    ("failed", lambda r: r["metrics"]["failed"]),
    ("stored", lambda r: r["metrics"]["stored"]),
    ("dup", lambda r: r["metrics"]["duplicates"]),
    ("lost", lambda r: r["metrics"]["lost"]),
    # Results saved before this column existed leave it blank.
    ("replica diff", lambda r: r["metrics"].get("replica_mismatches", "")),
    ("wall s", lambda r: r["wall_seconds"]),
]


def markdown_table(results: list[dict]) -> str:
    """One Markdown row per result, with the columns in TABLE_COLUMNS."""
    lines = ["| " + " | ".join(name for name, _ in TABLE_COLUMNS) + " |",
             "|" + "|".join("---" for _ in TABLE_COLUMNS) + "|"]
    for result in results:
        lines.append("| " + " | ".join(str(cell(result)) for _, cell in TABLE_COLUMNS) + " |")
    return "\n".join(lines)


def result_path(out_dir: Path, result: dict) -> Path:
    config = result["config"]
    stamp = result["started_at"].replace(":", "").replace("-", "")[:15]
    name = (f"{stamp}-p{config['partitions']}-acks{config['acks']}-t{config['threads']}"
            f"-{config['fault']}-{config['events']}.json")
    return out_dir / name


def run_bench(args) -> None:
    """Entry point for `python -m eventlog bench`."""
    if args.report:
        results = [json.loads(Path(path).read_text()) for path in args.report]
        print(markdown_table(results))
        return
    # SIGTERM gets the same cleanup as Ctrl-C: LocalCluster stops every process on the
    # way out.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        result = run_benchmark(args)
    except KeyboardInterrupt:
        print("\nInterrupted. The cluster was stopped and nothing was saved.", file=sys.stderr)
        raise SystemExit(130)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = result_path(out_dir, result)
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(markdown_table([result]))
    metrics = result["metrics"]
    for line in (
        f"replication lag: slowest in-sync follower up to {metrics['isr_lag_max']} records "
        f"behind (mean {metrics['isr_lag_mean']}), any replica up to "
        f"{metrics['replica_lag_max']}, over {metrics['lag_samples']} samples",
        f"leader changes: {metrics['leader_changes']}; samples with a partition's ISR "
        f"below the replica count: {metrics['short_isr_samples']} (smallest ISR "
        f"{metrics['min_isr_size']})",
        f"retried writes answered as duplicates: {metrics['duplicate_replies']}; failed "
        f"writes found stored anyway: {metrics['failed_but_stored']}",
        f"partitions where a replica's committed records differ from the leader's: "
        f"{metrics['replica_mismatches']}",
    ):
        print(line)
    for event in result["timeline"]:
        print(f"fault at {event['t']}s: " + ", ".join(
            f"{name}={value}" for name, value in event.items() if name != "t"))
    for message, count in metrics["errors"].items():
        print(f"error x{count}: {message}")
    print(f"saved {path}")
