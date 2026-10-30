"""Run the controller, brokers, producer, and consumer from the command line."""

from __future__ import annotations

import argparse
import json

from .broker import run_broker
from .client import ClusterClient, Consumer
from .common import load_config
from .controller import run_controller


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m eventlog")
    sub = parser.add_subparsers(dest="command", required=True)

    controller = sub.add_parser("controller")
    controller.add_argument("--config", required=True)
    controller.add_argument("--host", default="127.0.0.1")
    controller.add_argument("--port", type=int, default=9000)
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

    args = parser.parse_args()
    if args.command == "controller":
        run_controller(load_config(args.config), args.host, args.port, args.heartbeat_timeout)
    elif args.command == "broker":
        run_broker(args.id, load_config(args.config), args.controller, args.host, args.port, args.data_dir)
    elif args.command == "produce":
        result = ClusterClient(args.controller).produce(args.topic, json.loads(args.value), args.key, args.partition, args.acks, args.retries)
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


if __name__ == "__main__":
    main()
