"""Run the controller, brokers, producer, and consumer from the command line."""

from __future__ import annotations

import argparse
import json
import sys
import time

from .bench import run_bench
from .broker import run_broker
from .client import ClusterClient, Consumer, Producer
from .common import DEFAULT_ACKS, APIError, load_config
from .controller import run_controller
from .demo import run_demo


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
    controller.add_argument("--unclean-leader-election", action="store_true",
                            help="if no in-sync replica is alive, let any alive replica "
                                 "lead (committed records it lacks are lost)")

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
    producer.add_argument("--producer-id",
                          help="producer id to write under (default: a new random id)")
    producer.add_argument("--sequence", type=int, default=0,
                          help="sequence number of the first record (default 0); resending "
                               "a stored producer id and sequence returns the stored offset")
    producer.add_argument("--count", type=int, default=1,
                          help="write the value this many times as separate records, "
                               "printing one acknowledgement per line")

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

    status = sub.add_parser("status", help="show brokers, leaders, and each replica's log")
    status.add_argument("--controller", default=DEFAULT_CONTROLLER_URL)
    status.add_argument("--watch", type=float, metavar="SECONDS",
                        help="redraw every SECONDS until interrupted")

    faults = sub.add_parser(
        "faults", help="show, replace, or clear a broker's fault injection rules",
        description="With no --rule or --clear, print the broker's rules. Each --rule "
                    "is an action (drop, drop_response, delay, duplicate) followed by "
                    "KEY=VALUE fields: direction=out|in, to=BROKER|controller|*, "
                    "path=/replicate|/produce|...|*, probability=0..1, seconds=N (delay). "
                    "The given rules replace the broker's whole table.")
    faults.add_argument("--controller", default=DEFAULT_CONTROLLER_URL)
    faults.add_argument("--broker", required=True, help="broker id, or 'all'")
    change = faults.add_mutually_exclusive_group()
    change.add_argument("--rule", nargs="+", action="append", metavar="FIELD",
                        help="e.g. --rule drop to=b2 path=/replicate")
    change.add_argument("--clear", action="store_true", help="remove every rule")
    faults.add_argument("--seed", type=int,
                        help="restart the broker's probability rolls from this seed")

    bench = sub.add_parser(
        "bench", help="start a local cluster, write records, and measure the result",
        description="Starts a controller and brokers as subprocesses, writes --events "
                    "records with idempotent producers, then reads every partition back "
                    "and counts duplicates and lost records. Saves a JSON file under --out "
                    "and prints a Markdown table.")
    bench.add_argument("--events", type=int, default=10_000)
    bench.add_argument("--partitions", type=int, default=3)
    bench.add_argument("--acks", type=int, default=DEFAULT_ACKS,
                       help="smallest ISR, leader included, that may accept a write")
    bench.add_argument("--threads", type=int, default=6,
                       help="producer threads, each with its own producer id")
    bench.add_argument("--value-size", type=int, default=100,
                       help="length of each record's string value")
    bench.add_argument("--fault", default="none",
                       choices=["none", "kill-leader", "lost-replies", "slow-follower"],
                       help="kill-leader: SIGKILL the leader of partition 0 a third of the "
                            "way in, restart it two thirds in. lost-replies: every broker "
                            "drops the reply to --fault-probability of /produce requests. "
                            "slow-follower: the last broker waits --fault-delay before "
                            "handling each /replicate")
    bench.add_argument("--fault-probability", type=float, default=0.05)
    bench.add_argument("--fault-delay", type=float, default=0.05, metavar="SECONDS")
    bench.add_argument("--seed", type=int, default=1, help="seed for fault rule rolls")
    bench.add_argument("--brokers", type=int, default=3)
    bench.add_argument("--replication-factor", type=int, default=3)
    bench.add_argument("--retries", type=int, default=10,
                       help="retries per record before the producer gives up")
    bench.add_argument("--heartbeat-timeout", type=float, default=2.0,
                       help="controller's heartbeat timeout in seconds")
    bench.add_argument("--out", default="results", help="directory for the JSON result")
    bench.add_argument("--report", nargs="+", metavar="JSON",
                       help="print one table for these saved results instead of running")

    demo = sub.add_parser(
        "demo", help="narrated fault-injection demo on a local cluster",
        description="Starts a local cluster and a background producer, then kills and "
                    "restarts a leader, partitions the network, and loses produce replies, "
                    "showing the cluster status after each step. Ends by reading every "
                    "record back.")
    pace = demo.add_mutually_exclusive_group()
    pace.add_argument("--fast", action="store_true", help="short pauses (about 15 seconds)")
    pace.add_argument("--pause", action="store_true", help="wait for Enter before each step")
    return parser


def produce(args: argparse.Namespace) -> None:
    if args.count < 1:
        raise ValueError(f"--count must be at least 1, got {args.count}")
    # An idempotent Producer, so this command's own retries never store a record twice.
    producer = Producer(ClusterClient(args.controller), args.producer_id, args.sequence)
    for _ in range(args.count):
        result = producer.produce(args.topic, args.value, key=args.key,
                                  partition=args.partition, acks=args.acks,
                                  retries=args.retries)
        print(json.dumps(result), flush=True)


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


def parse_rule(words: list[str]) -> dict:
    """Turn ["drop", "to=b2", "probability=0.5"] into a rule dict for /faults."""
    rule: dict = {"action": words[0]}
    for word in words[1:]:
        name, separator, text = word.partition("=")
        if not separator:
            raise ValueError(f"fault rule field {word!r} must look like KEY=VALUE")
        if name in ("probability", "seconds"):
            try:
                rule[name] = float(text)
            except ValueError:
                raise ValueError(f"{name} must be a number, got {text!r}") from None
        else:
            rule[name] = text
    return rule


def faults(args: argparse.Namespace) -> None:
    client = ClusterClient(args.controller)
    broker_ids = list(client.metadata()["brokers"]) if args.broker == "all" else [args.broker]
    rules = [parse_rule(words) for words in args.rule] if args.rule else None
    for broker_id in broker_ids:
        if args.clear:
            reply = client.set_faults(broker_id, [], args.seed)
        elif rules is not None:
            reply = client.set_faults(broker_id, rules, args.seed)
        elif args.seed is not None:
            # Only a new seed: keep the broker's current rules.
            reply = client.set_faults(broker_id, client.faults(broker_id)["rules"], args.seed)
        else:
            reply = client.faults(broker_id)
        print(json.dumps(reply))


def format_rule(rule: dict) -> str:
    text = f"{rule['action']} {rule['direction']}"
    if rule["direction"] == "out":
        text += f" to={rule['to']}"
    text += f" path={rule['path']}"
    if rule["probability"] < 1:
        text += f" probability={rule['probability']:g}"
    if rule["action"] == "delay":
        text += f" seconds={rule['seconds']:g}"
    return text


def format_status(status: dict) -> str:
    alive = set(status["alive_brokers"])
    broker_ids = list(status["brokers"])
    lines = ["brokers   " + "   ".join(
        f"{broker_id} {'up' if broker_id in alive else 'DOWN'}" for broker_id in broker_ids), ""]

    # Each cell is "records held / records committed" in that broker's own log.
    # The leader's cell is starred. A follower behind the leader has missed records.
    rows = []
    for entry in status["partitions"]:
        cells = []
        for broker_id in broker_ids:
            if broker_id not in entry["replicas"]:
                cell = ""
            elif entry["replicas"][broker_id] is None:
                cell = "down"
            else:
                log = entry["replicas"][broker_id]
                cell = f"{log['last_offset'] + 1}/{log['committed_offset'] + 1}"
                if broker_id == entry["leader"]:
                    cell = "*" + cell
            cells.append(cell)
        rows.append((entry, cells))
    # Columns are at least 10 wide and grow so that "*1020/1020" style cells
    # keep two spaces between them.
    width = max([10] + [len(cell) + 2 for _, cells in rows for cell in cells])
    lines.append(f"{'partition':<12}{'leader':<8}{'epoch':<7}"
                 + "".join(f"{b:<{width}}" for b in broker_ids) + "isr")
    for entry, cells in rows:
        name = f"{entry['topic']}:{entry['partition']}"
        epoch = "-" if entry["leader_epoch"] is None else str(entry["leader_epoch"])
        lines.append(f"{name:<12}{entry['leader'] or 'NONE':<8}{epoch:<7}"
                     + "".join(f"{cell:<{width}}" for cell in cells)
                     + (",".join(entry["isr"]) or "-"))
    lines += ["", "cells: records held / committed on that broker; * = leader",
              "isr: replicas the controller counts as in sync with the leader"]

    active = {broker_id: rules for broker_id, rules in status.get("faults", {}).items() if rules}
    if active:
        lines += ["", "fault rules:"]
        for broker_id, rules in active.items():
            lines += [f"  {broker_id:<8}{format_rule(rule)}" for rule in rules]
    return "\n".join(lines)


def show_status(args: argparse.Namespace) -> None:
    client = ClusterClient(args.controller)
    if args.watch is None:
        print(format_status(client.cluster_status()))
        return
    try:
        while True:
            try:
                text = format_status(client.cluster_status())
            except APIError as exc:
                text = f"error: {exc}"
            # Clear the screen and redraw at the top, like watch(1).
            print("\033[2J\033[H" + time.strftime("%H:%M:%S") + "\n\n" + text, flush=True)
            time.sleep(args.watch)
    except KeyboardInterrupt:
        pass


def run(args: argparse.Namespace) -> None:
    if args.command == "controller":
        run_controller(load_config(args.config), args.host, args.port, args.heartbeat_timeout,
                       args.unclean_leader_election)
    elif args.command == "broker":
        run_broker(args.id, load_config(args.config), args.controller, args.host, args.port,
                   args.data_dir)
    elif args.command == "produce":
        produce(args)
    elif args.command == "consume":
        consume(args)
    elif args.command == "status":
        show_status(args)
    elif args.command == "faults":
        faults(args)
    elif args.command == "bench":
        run_bench(args)
    elif args.command == "demo":
        run_demo(args, format_status)


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
