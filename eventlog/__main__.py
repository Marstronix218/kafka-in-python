"""Run the controller, brokers, producer, and consumer from the command line."""

from __future__ import annotations

import argparse
import json
import sys

from .broker import run_broker
from .client import ClusterClient, Consumer
from .common import DEFAULT_ACKS, APIError, load_config
from .controller import run_controller


DEFAULT_CONTROLLER_URL = "http://127.0.0.1:9000"


def json_value(text: str) -> object:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise argparse.ArgumentTypeError(
            f"{text!r} is not valid JSON (strings need quotes, e.g. '\"hello\"' or "
            "'{\"status\":\"placed\"}')") from None


def build_parser() -> argparse.ArgumentParser:
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
    broker.add_argument("--controller", default=DEFAULT_CONTROLLER_URL)
    broker.add_argument("--host", default="127.0.0.1")
    broker.add_argument("--port", type=int, required=True)
    broker.add_argument("--data-dir", required=True)

    producer = sub.add_parser("produce")
    producer.add_argument("--controller", default=DEFAULT_CONTROLLER_URL)
    producer.add_argument("--topic", required=True)
    producer.add_argument("--value", required=True, type=json_value, help="JSON value")
    producer.add_argument("--key")
    producer.add_argument("--partition", type=int)
    producer.add_argument("--acks", type=int, default=DEFAULT_ACKS)
    producer.add_argument("--retries", type=int, default=3)

    consumer = sub.add_parser("consume")
    consumer.add_argument("--controller", default=DEFAULT_CONTROLLER_URL)
    consumer.add_argument("--topic", required=True)
    consumer.add_argument("--partition", type=int, required=True)
    consumer.add_argument("--consumer-id", required=True)
    consumer.add_argument("--state-dir", default=".data/consumers")
    consumer.add_argument("--limit", type=int, default=100)
    consumer.add_argument("--commit", action="store_true",
                          help="commit next offset after printing records")
    consumer.add_argument("--seek", type=int)
    return parser


def consume(args: argparse.Namespace) -> None:
    consumer = Consumer(ClusterClient(args.controller), args.consumer_id, args.state_dir)
    if args.seek is not None:
        consumer.seek(args.topic, args.partition, args.seek)
    records = consumer.fetch(args.topic, args.partition, args.limit)
    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    # Commit only after printing. If we crash in between, the records are read
    # again next time, which is the at-least-once behavior the README promises.
    if args.commit and records:
        consumer.commit(args.topic, args.partition, records[-1]["offset"] + 1)


def run(args: argparse.Namespace) -> None:
    if args.command == "controller":
        run_controller(load_config(args.config), args.host, args.port, args.heartbeat_timeout)
    elif args.command == "broker":
        run_broker(args.id, load_config(args.config), args.controller, args.host, args.port,
                   args.data_dir)
    elif args.command == "produce":
        result = ClusterClient(args.controller).produce(
            args.topic, args.value, key=args.key, partition=args.partition,
            acks=args.acks, retries=args.retries)
        print(json.dumps(result))
    elif args.command == "consume":
        consume(args)


def main() -> None:
    args = build_parser().parse_args()
    try:
        run(args)
    except (APIError, ValueError, OSError) as exc:
        # These are mistakes a user can fix (bad config, cluster not started, bad
        # offset), so print the message without a traceback.
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
