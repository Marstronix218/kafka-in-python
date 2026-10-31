# Kafka-inspired distributed event log

A Python distributed event log with three independent HTTP brokers, partitioned append-only storage, synchronous leader-to-follower replication with configurable acknowledgments, and a controller that promotes a surviving replica when a leader stops responding.

This repository is at the end of project week 8 (see [Progress](#progress)). [REFERENCES.md](REFERENCES.md) lists the Kafka source files and docs that each part of the code is based on.

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
python3 -m eventlog controller --config config/local.json --port 9000
python3 -m eventlog broker --config config/local.json --id b1 --port 9001 --data-dir .data/b1
python3 -m eventlog broker --config config/local.json --id b2 --port 9002 --data-dir .data/b2
python3 -m eventlog broker --config config/local.json --id b3 --port 9003 --data-dir .data/b3
```

Then use `python3 -m eventlog produce --topic events --value '{"hello":"world"}'` and `python3 -m eventlog consume --topic events --partition 0 --consumer-id demo --commit`. Run `python3 -m unittest discover -s tests -v` for tests.

## Model and guarantees

- Topics and partition counts are set in the JSON config at startup. Replica placement rotates across configured brokers. The sample topic has three partitions and replication factor three.
- Offsets start at zero per partition. The leader serializes writes to a partition. Reads are served by the leader and show committed records only, ordered by offset. There is no ordering guarantee across partitions.
- Each broker `fsync`s its JSONL records and stores its committed high watermark atomically. A torn final record is discarded on restart.
- The leader appends a new record locally, then synchronously sends it to every follower with `/replicate`. `acks` is the number of replicas (leader included) that must persist the record before success is returned. It defaults to 2 and is limited by the replication factor. If fewer replicas persisted it, the producer receives a 503 error and the record is not committed.
- Once enough replicas persisted the record, the leader advances its committed high watermark and tells the followers that persisted it with `/commit`. A failed write leaves an uncommitted record that the leader drops before its next write.
- Consumers own durable *next-to-read* offset files; commit after processing to get at-least-once behavior. `seek` can replay old records. Seeking beyond the end yields an empty read.
- The controller marks a broker dead after the heartbeat timeout (2 seconds by default) and routes each partition to its first alive replica.

### Current limitations

- Failover picks the first alive replica, not necessarily the most up-to-date one, so an acknowledged write can still be lost on failover (for example after an `acks=1` write, or if a follower missed a commit).
- A restarted or lagging follower does not catch up yet; it only receives records written after it is reachable again.
- Producer retries after an ambiguous error can duplicate records. There are no idempotent producers.
- The controller is a single coordination process, so controller failure pauses route discovery and writes. Its state is in memory only. This project uses a custom HTTP/JSON protocol, not Kafka's wire protocol. It does not provide consumer groups, exactly-once delivery, dynamic partition reassignment, retention, or multi-controller consensus.

## HTTP API

Controller:

| Method | Path | Result |
| --- | --- | --- |
| GET | `/metadata` | Configured topics and broker URLs |
| GET | `/route?topic=events&partition=0` | Leader, replicas, broker URLs (503 if no replica is alive) |
| GET | `/health` | Alive broker IDs |
| POST | `/heartbeat` | Body: `broker_id` |

Broker:

| Method | Path | Result |
| --- | --- | --- |
| POST | `/produce` | Body: `topic`, `partition`, `key`, `value`, `acks`; returns offset |
| GET | `/fetch?topic=events&partition=0&offset=0&limit=100` | Committed records and `committed_offset` (leader only) |
| GET | `/status?topic=events&partition=0` | Local `last_offset` and `committed_offset` |
| GET | `/health` | Broker ID |

The payload is JSON and each record is limited to 1 MiB. Internal `/replicate` and `/commit` calls carry the sending leader's ID, are rejected unless it is the current leader, and are intended for the local trusted cluster.

## Failure demonstration

Produce a few records and note their partition and current leader in `/route`. Stop that broker with `docker compose stop broker1` (or whichever broker leads the partition). Within the heartbeat timeout, `/route` promotes a surviving replica. Fetch the old records and produce another event. Start the stopped broker again with `docker compose start broker1`; it rejoins as a follower but does not yet catch up on the records it missed.

## Progress

- Week 4: three independent HTTP brokers, a controller that stores partition-to-leader assignments, producer/consumer clients, and key-based partitioning with per-partition ordering.
- Week 5: broker heartbeats; the controller marks a broker failed after a timeout and stops routing clients to it.
- Weeks 6-7: durable fsynced append-only logs with torn-record recovery, and consumer offsets with commit, restart, and `seek` replay.
- Week 8: synchronous leader-to-follower replication, configurable `acks`, and a committed high watermark.

Next (weeks 9-15):

- ISR (in-sync replica) tracking.
- Promote only up-to-date replicas, with leader epochs.
- Catch-up for restarted or lagging brokers.
- Fault injection and idempotent producers.
- Scaling and evaluation.
- Final demo.
