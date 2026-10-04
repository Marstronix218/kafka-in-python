# Kafka-inspired distributed event log

A Python distributed event log with three independent HTTP brokers, partitioned append-only storage, and a controller that promotes a surviving replica when a leader stops responding.

## Quick start

Requires Python 3.12 or Docker Compose. The application itself uses only the Python standard library.

```sh
docker compose up --build -d
docker compose exec broker1 python -m eventlog produce --controller http://controller:8000 --topic events --key order-1 --value '{"status":"placed"}'
docker compose exec broker1 python -m eventlog consume --controller http://controller:8000 --topic events --partition 0 --consumer-id demo --state-dir /data/consumers --limit 10
```

The key chooses a partition by SHA-256. The produced result prints its partition; use that number for `consume`. Add `--commit` to save the next offset after printing records. The consumer offset file must stay on a persistent volume if the consumer container is recreated. In the example, broker1's `/data` volume is persistent. Commands run inside a container because the compose broker URLs use Docker network names.

Run locally instead by starting four terminals:

```sh
python3 -m eventlog controller --config config/local.json --port 9000 --state-dir .data/controller
python3 -m eventlog broker --config config/local.json --id b1 --port 9001 --data-dir .data/b1
python3 -m eventlog broker --config config/local.json --id b2 --port 9002 --data-dir .data/b2
python3 -m eventlog broker --config config/local.json --id b3 --port 9003 --data-dir .data/b3
```

Then use `python3 -m eventlog produce --topic events --value '{"hello":"world"}'` and `python3 -m eventlog consume --topic events --partition 0 --consumer-id demo --commit`. Run `python3 -m unittest discover -s tests -v` for tests.

## Model and guarantees

- Topics and partition counts are set in the JSON config at startup. Replica placement rotates across configured brokers. The sample topic has three partitions and replication factor three.
- Offsets start at zero per partition. The leader serializes writes to a partition. Reads show committed records only and are ordered by offset. There is no ordering guarantee across partitions.
- Each broker `fsync`s its JSONL records and stores visible and quorum-safe high watermarks atomically. A torn final record is discarded on restart. An unsafe suffix (such as an isolated `acks=1` write) may be replaced after failover; the safe prefix is never overwritten.
- `acks` is the number of replicas that must persist and commit a write before success is returned. It defaults to 2. With replication factor 3 and `acks >= 2`, a successful write survives one broker crash. `acks=1` is faster but cannot promise that. If the requested replica count is unavailable, the producer receives an error.
- The controller persists the highest quorum-safe offset it has observed. After heartbeat timeout (2 seconds by default), it promotes only an alive replica that contains that safe prefix, then increases the leader epoch. If none is available, writes pause. Brokers reject replication from stale epochs. A recovered former leader remains a follower and catches up from the new leader.
- Producer retries after an ambiguous error can duplicate records. `event_id` is a tracing field, not an idempotency key. Consumers own durable *next-to-read* offset files; commit after processing to get at-least-once behavior. `seek` can replay old records. Seeking beyond the end yields an empty read.

The controller is a single coordination process, so controller failure pauses route discovery and writes. Its leader and epoch state is persisted for restart. This project uses a custom HTTP/JSON protocol, not Kafka's wire protocol. It does not provide consumer groups, exactly-once delivery, dynamic partition reassignment, retention, or multi-controller consensus.

## HTTP API

Controller:

| Method | Path | Result |
| --- | --- | --- |
| GET | `/metadata` | Configured topics and broker URLs |
| GET | `/route?topic=events&partition=0` | Leader, epoch, replicas, broker URLs |
| GET | `/health` | Alive broker IDs |
| POST | `/heartbeat` | Broker status and per-partition committed offsets |

Broker:

| Method | Path | Result |
| --- | --- | --- |
| POST | `/produce` | Body: `topic`, `partition`, `key`, `value`, `acks`, optional `event_id`; returns offset |
| GET | `/fetch?topic=events&partition=0&offset=0&limit=100` | Committed records |
| GET | `/status?topic=events&partition=0` | Local last and committed offsets |
| POST | `/fault` | Set `replication_delay_ms`, `blocked_peers`, or `drop_ack_once` |

The payload is JSON and each record is limited to 1 MiB. Internal `/replicate` and `/commit` calls use leader epochs and are intended for the local trusted cluster.

## Failure demonstration

Produce a few records and note their partition and current leader in `/route`. Stop that broker with `docker compose stop broker1` (or whichever broker leads the partition). Within the heartbeat timeout, `/route` promotes a surviving replica. Fetch the old records and produce another event. Start the stopped broker again with `docker compose start broker1`; it will catch up in the background.

To inject faults on a local broker, send `POST /fault` with JSON such as `{"drop_ack_once":true}`. The next write commits but returns an error; retrying it can create a duplicate. `{"blocked_peers":["b2","b3"]}` prevents outgoing replication and makes an `acks=2` write fail. `{"replication_delay_ms":500}` delays follower replication. Clear these settings with `{"blocked_peers":[],"replication_delay_ms":0}`.

## Benchmark

```sh
python3 -m eventlog benchmark --topic events --count 100000 --acks 2
```

Run that command against the local config, or inside a Compose broker with `--controller http://controller:8000`. It reports throughput, p50/p95 request latency, maximum observed replica lag, duplicate event IDs, and acknowledged records absent from a final committed read. The 100,000-event run is an evaluation workload, not a speed target.
