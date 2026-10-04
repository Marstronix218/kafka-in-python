"""Run controller, brokers, clients, and a simple workload benchmark."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from .broker import run_broker
from .client import ClusterClient, Consumer
from .common import APIError, load_config, request_json
from .controller import run_controller


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m eventlog")
    sub = parser.add_subparsers(dest="command", required=True)

    controller = sub.add_parser("controller")
    controller.add_argument("--config", required=True)
    controller.add_argument("--host", default="127.0.0.1")
    controller.add_argument("--port", type=int, default=9000)
    controller.add_argument("--state-dir", default=".data/controller")
    controller.add_argument("--heartbeat-timeout", type=float, default=2.0)

    broker = sub.add_parser("broker")
    broker.add_argument("--config", required=True)
    broker.add_argument("--id", required=True)
    broker.add_argument("--controller", default="http://127.0.0.1:9000")
    broker.add_argument("--host", default="127.0.0.1")
    broker.add_argument("--port", type=int, required=True)
    broker.add_argument("--data-dir", required=True)

    producer = sub.add_parser("produce")
    producer.add_argument("--controller", default="http://127.0.0.1:9000")
    producer.add_argument("--topic", required=True)
    producer.add_argument("--value", required=True, help="JSON value")
    producer.add_argument("--key")
    producer.add_argument("--partition", type=int)
    producer.add_argument("--acks", type=int, default=2)
    producer.add_argument("--event-id")
    producer.add_argument("--retries", type=int, default=3)

    consumer = sub.add_parser("consume")
    consumer.add_argument("--controller", default="http://127.0.0.1:9000")
    consumer.add_argument("--topic", required=True)
    consumer.add_argument("--partition", type=int, required=True)
    consumer.add_argument("--consumer-id", required=True)
    consumer.add_argument("--state-dir", default=".data/consumers")
    consumer.add_argument("--limit", type=int, default=100)
    consumer.add_argument("--commit", action="store_true", help="commit next offset after printing records")
    consumer.add_argument("--seek", type=int)

    benchmark = sub.add_parser("benchmark")
    benchmark.add_argument("--controller", default="http://127.0.0.1:9000")
    benchmark.add_argument("--topic", required=True)
    benchmark.add_argument("--count", type=int, default=100_000)
    benchmark.add_argument("--acks", type=int, default=2)

    args = parser.parse_args()
    if args.command == "controller":
        run_controller(load_config(args.config), args.host, args.port, Path(args.state_dir), args.heartbeat_timeout)
    elif args.command == "broker":
        run_broker(args.id, load_config(args.config), args.controller, args.host, args.port, args.data_dir)
    elif args.command == "produce":
        result = ClusterClient(args.controller).produce(args.topic, json.loads(args.value), args.key, args.partition, args.acks, args.event_id, args.retries)
        print(json.dumps(result))
    elif args.command == "consume":
        consumer = Consumer(ClusterClient(args.controller), args.consumer_id, args.state_dir)
        if args.seek is not None:
            consumer.seek(args.topic, args.partition, args.seek)
        records = consumer.fetch(args.topic, args.partition, args.limit)
        for record in records:
            print(json.dumps(record, ensure_ascii=False))
        if args.commit and records:
            consumer.commit(args.topic, args.partition, records[-1]["offset"] + 1)
    elif args.command == "benchmark":
        run_benchmark(ClusterClient(args.controller), args.topic, args.count, args.acks)


def run_benchmark(client: ClusterClient, topic: str, count: int, acks: int) -> None:
    if count < 1:
        raise ValueError("count must be positive")
    timings = []
    successes = {}
    started = time.monotonic()
    for index in range(count):
        event_id = f"bench-{index}"
        before = time.monotonic()
        try:
            client.produce(topic, {"index": index}, event_id=event_id, acks=acks)
            successes[event_id] = True
        except APIError:
            successes[event_id] = False
        timings.append((time.monotonic() - before) * 1000)
    elapsed = time.monotonic() - started
    metadata = client.metadata()
    observed = {}
    max_lag = 0
    for partition in range(metadata["topics"][topic]["partitions"]):
        route = client.route(topic, partition)
        offset = 0
        while True:
            batch = client.fetch(topic, partition, offset, 1000)["records"]
            if not batch:
                break
            for record in batch:
                if record.get("event_id"):
                    observed[record["event_id"]] = observed.get(record["event_id"], 0) + 1
            offset = batch[-1]["offset"] + 1
        for replica in route["replicas"]:
            try:
                status = request_json(route["brokers"][replica] + f"/status?topic={topic}&partition={partition}")
                max_lag = max(max_lag, offset - 1 - status["committed_offset"])
            except APIError:
                pass
    ordered = sorted(timings)
    percentile = lambda p: ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]
    print(json.dumps({"attempted": count, "acknowledged": sum(successes.values()), "throughput_events_per_sec": sum(successes.values()) / elapsed,
                      "p50_latency_ms": percentile(0.5), "p95_latency_ms": percentile(0.95),
                      "max_replication_lag_events": max_lag,
                      "duplicate_events": sum(max(0, n - 1) for n in observed.values()),
                      "acknowledged_data_loss": sum(1 for event_id, acked in successes.items() if acked and event_id not in observed)}, sort_keys=True))


if __name__ == "__main__":
    main()
