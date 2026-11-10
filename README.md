# Kafka-inspired distributed event log

A Python distributed event log with three independent HTTP brokers, partitioned append-only storage, and a controller that tracks each partition's leader, leader epoch and in-sync replicas (ISR). Leaders copy every write to the in-sync followers before acknowledging it, a failed leader is replaced only by an in-sync replica, and a restarted broker is caught up by the leaders and added back to the ISR. Producers are idempotent, and each broker has a fault-injection table for dropping, delaying or duplicating its network traffic.

This repository covers project weeks 4 to 15 (see [Progress](#progress)). [REFERENCES.md](REFERENCES.md) lists the Kafka source files and docs that each part of the code is based on.

## Quick start

Requires Python 3.11 or newer, or Docker Compose. The application itself uses only the Python standard library.

```sh
docker compose up --build -d
docker compose exec broker1 python -m eventlog produce --controller http://controller:8000 --topic events --key order-1 --value '{"status":"placed"}'
docker compose exec broker1 python -m eventlog consume --controller http://controller:8000 --topic events --partition 0 --consumer-id demo --state-dir /data/consumers --limit 10
docker compose exec controller python -m eventlog status --controller http://localhost:8000 --watch 1
```

The key chooses a partition by SHA-256. The produced result prints its partition; use that number for `consume`. Add `--commit` to save the next offset after printing records. The consumer offset file must stay on a persistent volume if the consumer container is recreated. In the example, broker1's `/data` volume is persistent. Commands run inside a container because the compose broker URLs use Docker network names.

Run locally instead by starting four terminals:

```sh
python3 -m eventlog controller --config config/local.json --port 9000
python3 -m eventlog broker --config config/local.json --id b1 --port 9001 --data-dir .data/b1
python3 -m eventlog broker --config config/local.json --id b2 --port 9002 --data-dir .data/b2
python3 -m eventlog broker --config config/local.json --id b3 --port 9003 --data-dir .data/b3
```

Then use `python3 -m eventlog produce --topic events --value '{"hello":"world"}'` and `python3 -m eventlog consume --topic events --partition 0 --consumer-id demo --commit`, and `python3 -m eventlog status` to inspect the cluster. `produce --count 5` writes the value as five separate records. Run `python3 -m unittest discover -s tests -v` for tests.

`python3 -m eventlog demo` starts its own local cluster and runs the scripted failure demonstration, and `python3 -m eventlog bench` runs one benchmark. Neither needs the four terminals above.

## Model and guarantees

- Topics and partition counts are set in the JSON config at startup. Replica placement rotates across configured brokers. The sample topic has three partitions and replication factor three.
- Offsets start at zero per partition. The leader serializes writes to a partition. Reads are served by the leader and show committed records only, ordered by offset. There is no ordering guarantee across partitions.
- Each broker `fsync`s its JSONL records and stores its committed high watermark atomically. A torn final record is discarded on restart.
- For each partition the controller keeps a leader, a leader epoch and an ISR. The ISR starts as all replicas. The first election sets the epoch to 0, and every leader change adds 1.
- The controller marks a broker dead after the heartbeat timeout (2 seconds by default). A live leader keeps its partitions, even when the preferred replica comes back. When the leader is dead, the controller elects the first alive ISR member in replica order and leaves the ISR as it is. If no ISR member is alive, the partition has no leader and `/route` answers 503 `no in-sync replica is alive`, even if an out-of-sync replica is running. Starting the controller with `--unclean-leader-election` lets any alive replica lead in that case and resets the ISR to that replica alone.
- `acks` is the smallest ISR, leader included, that may accept a write. It defaults to 2 and can be 1 up to the replication factor, which is how Kafka uses `min.insync.replicas` with `acks=all`. If the ISR is smaller than `acks`, the leader answers 503 `not enough replicas` and appends nothing.
- Otherwise the leader appends the record and sends it with `/replicate` to every ISR follower, whatever `acks` is. A follower that fails, times out (1 second) or is still behind after 10 batches is removed from the ISR through the controller's `/isr`. A record is committed once every remaining ISR member holds it, so the leader's commit point is the minimum log end over the ISR. The leader then saves its new committed offset and sends `/commit` to the followers.
- If the ISR shrank below `acks` during that write, the producer gets 503 `not enough replicas after append`. The record stays in the leader's log and may already be committed, so the outcome is ambiguous, as with Kafka's `NOT_ENOUGH_REPLICAS_AFTER_APPEND`. Retrying with the same producer id and sequence is safe.
- Every ISR member holds every committed record, and only ISR members can lead, so a committed record survives any failure that leaves one ISR member alive. Unclean election gives up that guarantee.
- Each broker runs a maintenance step every 0.5 seconds for the partitions it leads. It sends ISR followers anything they lack and commits what they all hold, which commits a tail of uncommitted records a new leader inherited and repeats a `/commit` a follower missed. Then it pushes records to alive replicas outside the ISR, at most 10 batches of up to 100 records each per step, and adds a replica back to the ISR once its log matches the leader's to the end. The leader pushes; Kafka followers fetch instead. A restarted broker needs no catch-up logic of its own.
- A follower in the ISR accepts a batch that starts at or before its own log end + 1. A follower outside the ISR only accepts a batch that starts at or before its committed offset + 1, because its uncommitted tail may come from an older epoch. Otherwise it answers 409 with its offsets and the leader resends from its committed offset + 1. The follower replaces conflicting uncommitted records, drops anything past the leader's log end, and commits only up to the end of the batch it received.
- `/replicate` and `/commit` carry the sender's broker ID and leader epoch. The follower compares both with the controller's current route and answers 409 if either differs, so a deposed leader can't write to followers. The controller accepts `/isr` only from the current leader at the current epoch.
- `Producer` in `client.py` writes under a producer id (a random UUID unless one is given) and numbers records per partition from 0. A retry resends the same id and sequence. For each partition the log keeps the last 5 sequences and offsets of every producer, rebuilt from the records on startup and after truncation, so a new leader has them too. A sequence the leader already holds returns the original offset with `"duplicate": true`; if that record is not committed yet, the leader replicates and commits it first. A sequence after a gap, or one older than the 5 it keeps, gets 422 with `last_sequence`. The first record of a producer the log has never seen may use any sequence.
- `ClusterClient.produce` and the `produce` command go through a `Producer`. Each run of `produce` uses a new producer id unless `--producer-id` is given.
- Consumers own durable *next-to-read* offset files; commit after processing to get at-least-once behavior. `seek` can replay old records. Seeking beyond the end yields an empty read.
- Each broker has a table of fault rules for its own traffic, changed at runtime. See [Fault injection](#fault-injection).

### Current limitations

- There is no equivalent of Kafka's `replica.lag.time.max.ms`. A follower is only removed from the ISR when a push to it fails, times out, or leaves it behind after 10 batches, so a slow follower that answers within the 1-second peer timeout stays in the ISR and slows every write to its partitions (50 ms per `/replicate` cut throughput from about 300 to 46 records/s in the [evaluation](#evaluation)).
- A follower that dies while its partition is idle stays in that partition's ISR until the next write. That write passes the `acks` check, is appended, and can then fail with `not enough replicas after append`.
- Unclean leader election can lose acknowledged records. A replica whose committed records go past or differ from the new leader's log is refused on every push and never rejoins the ISR.
- Producer state never expires, and there is no producer epoch. Two live producers sharing one id get each other's sequences rejected or answered as duplicates.
- A `Producer` sends one write at a time per partition, and the leader holds the partition lock while it replicates, so writes to one partition never overlap.
- Every HTTP request opens a new TCP connection, and every `/produce`, `/fetch`, `/replicate` and `/commit` asks the controller for the current route. One produce with two in-sync followers takes 11 requests. On macOS, above roughly 300 records/s for more than about 30 seconds, closed connections in TIME_WAIT use up the local ports, calls to the controller fail with `EADDRNOTAVAIL`, heartbeats are missed and leadership moves ([results/SUMMARY.md](results/SUMMARY.md) has the details).
- Outgoing fault rules also match the broker's own `/route` and `/heartbeat` calls to the controller. A rule with `to=*` or `to=controller` can get the broker declared dead.
- The controller is a single process that is assumed not to fail. Its leaders, epochs and ISRs are in memory only; after a restart every ISR is reset to all replicas and epochs start again, so a stale replica could be elected. This project uses a custom HTTP/JSON protocol, not Kafka's wire protocol. It does not provide consumer groups, exactly-once delivery or transactions, dynamic partition reassignment, retention, or multi-controller consensus.

## HTTP API

Controller:

| Method | Path | Result |
| --- | --- | --- |
| GET | `/metadata` | Configured topics and broker URLs |
| GET | `/route?topic=events&partition=0` | `leader`, `leader_epoch`, `isr`, `replicas` and broker URLs, after electing a leader if the current one is dead. 503 `no in-sync replica is alive` if replicas are up but none is in the ISR, 503 `no replica is alive` if none is up |
| GET | `/health` | Alive broker IDs |
| POST | `/heartbeat` | Body: `broker_id` |
| POST | `/isr` | Body: `topic`, `partition`, `leader`, `leader_epoch`, `isr`. Replaces the ISR and returns it. 409 unless the sender is the current leader at that epoch and the ISR holds the leader and only replicas |

Broker:

| Method | Path | Result |
| --- | --- | --- |
| POST | `/produce` | Body: `topic`, `partition`, `key`, `value`, `acks`, and optionally `producer_id` with `sequence`. Returns `offset`, `duplicate` and the request's `topic`, `partition`, `acks`, `producer_id`, `sequence` |
| GET | `/fetch?topic=events&partition=0&offset=0&limit=100` | Committed records and `committed_offset` (leader only) |
| GET | `/status?topic=events&partition=0` | Local `last_offset` and `committed_offset` |
| GET | `/health` | Broker ID |
| GET | `/faults` | The broker's fault rules |
| POST | `/faults` | Body: `rules` (a list; empty clears) and optional `seed`. Replaces the whole table |

The payload is JSON and each record is limited to 1 MiB. `/produce` errors:

| Status | Meaning |
| --- | --- |
| 400 | Malformed request, or `acks` outside 1 to the replication factor |
| 409 | This broker is not the leader. Clients ask the controller again and retry |
| 413 | Record over 1 MiB |
| 422 | Sequence after a gap (`out of order sequence`) or older than the last 5 the leader keeps (`duplicate sequence`). The body has `last_sequence`. Retrying the same sequence can't fix it |
| 503 | `not enough replicas` (nothing appended), `not enough replicas after append` (appended, possibly committed), no leader, or the controller can't be reached. Clients retry |

`/replicate` and `/commit` are internal. Both carry the sending leader's ID and `leader_epoch` and are rejected with 409 unless that pair matches the controller's route. `/replicate` sends a batch of consecutive `records` with `leader_last_offset` and the leader's `committed_offset`, and the follower answers with its own `last_offset` and `committed_offset`. A batch that starts past the point where the follower's log is known to match gets 409 with those two offsets in the body. A `/replicate` body may be up to 4 MiB plus 4 KiB. `/commit` sends an `offset` and gets the follower's `committed_offset` back. These calls are intended for the local trusted cluster.

## Inspecting the cluster

`status` puts the controller's view next to every broker's own log. This is a local three-broker cluster after 5 records were written to partition 0 and one each to partitions 1 and 2, b1 was killed, 2 more records went to partition 0, and b3 was given a delay rule:

```text
brokers   b1 DOWN   b2 up   b3 up

partition   leader  epoch  b1        b2        b3        isr
events:0    b2      1      down      *7/7      7/7       b2,b3
events:1    b2      0      down      *1/1      1/1       b2,b3,b1
events:2    b3      0      down      1/1       *1/1      b3,b1,b2

cells: records held / committed on that broker; * = leader
isr: replicas the controller counts as in sync with the leader

fault rules:
  b3      delay in path=/replicate seconds=0.2
```

Each cell is *records held / records committed* on that broker, and `*` marks the leader. `epoch` is the partition's leader epoch: events:0 is at 1 because b2 replaced b1, and the other two partitions never changed leader. `isr` is the controller's ISR, in replica order. b2 dropped b1 from the ISR of events:0 when it took over. events:1 and events:2 still list b1 because nothing was written to them after the kill (the second limitation above). A follower outside the ISR that holds fewer records is still catching up. A leader holding more records than it has committed is in the middle of a write, or has just taken over records from the previous leader that its next maintenance step will commit. `fault rules` lists each broker's active rules and is left out when there are none. `--watch 1` redraws the table every second, which is handy in a second terminal while stopping brokers.

## Fault injection

Every request a broker sends to a peer or to the controller, and every request it receives except `/faults`, is checked against its rule table. The first rule that matches and passes its probability roll is applied. A rule has these fields:

| Field | Values | Default |
| --- | --- | --- |
| `action` | `drop` (the request never arrives), `drop_response` (it is handled, the reply is lost), `delay`, `duplicate` (handled twice) | required |
| `direction` | `out` for calls this broker makes, `in` for requests it receives | `out` |
| `to` | a broker ID, `controller` or `*`; outgoing rules only | `*` |
| `path` | an endpoint such as `/replicate`, or `*` | `*` |
| `probability` | 0 to 1 | 1 |
| `seconds` | delay length, more than 0 and at most 60; `delay` only | |

An outgoing `drop` or `drop_response` fails with 503, worded like a network error. An outgoing delay at least as long as the caller's timeout waits out the timeout and fails. A dropped incoming request, or a dropped reply, closes the connection without an answer.

The `faults` command looks up the broker's URL from the controller and calls its `/faults`. Each `--rule` is an action followed by `KEY=VALUE` fields, and the rules given replace the broker's whole table:

```sh
python3 -m eventlog faults --broker b1                                   # show b1's rules
python3 -m eventlog faults --broker b1 --rule drop to=b2                 # b1 can't send to b2

# a network partition between b1 and b2 needs a rule on each side
python3 -m eventlog faults --broker b1 --rule drop to=b2
python3 -m eventlog faults --broker b2 --rule drop to=b1

# every broker handles /produce but loses 30% of the replies
python3 -m eventlog faults --broker all --rule drop_response direction=in path=/produce probability=0.3 --seed 1

# two rules at once: b3 sends nothing to b1 and is slow to handle replication
python3 -m eventlog faults --broker b3 --rule drop to=b1 --rule delay direction=in path=/replicate seconds=0.05

python3 -m eventlog faults --broker all --clear
```

Probability rolls come from a random generator per broker. `--seed` restarts it, so the same rules drop the same requests on every run; `--seed` alone keeps the current rules. With `drop_response` on `/produce`, the producer retries, and the leader answers the retry with the stored offset and `"duplicate": true` instead of writing a second copy.

## Failure demonstration

`python3 -m eventlog demo` is the scripted version for a screen recording. It starts a controller and three brokers as local processes and takes about 3 minutes; `--fast` runs it in about 15 seconds, and `--pause` waits for Enter before each step. It prints the `status` table after each step:

1. Start the cluster: topic `events` with 3 partitions on all three brokers.
2. Start writing: one thread per partition, about 10 writes per second each with `acks=2`, through one idempotent `Producer`.
3. Kill the leader of events:0 with SIGKILL. The demo prints how long the controller took to elect a new leader and the new epoch, then waits until the killed broker has left every ISR. Writes to its partitions retry with the same sequence numbers in the meantime.
4. Restart it on its old data directory and print how long it took to catch up and rejoin every ISR. Leadership does not move back.
5. Partition the network between the leader of events:0 and one follower with a pair of `drop` rules. Both still reach the controller, so neither is declared dead, but each drops the other from the ISRs it leads. Writes continue with 2 ISR members.
6. Clear the rules. The follower catches up and rejoins.
7. Make every broker lose 30% of its `/produce` replies, and count the retries answered as duplicates.
8. Stop writing, wait until every partition has a full ISR and has committed its whole log, read every partition from offset 0, and print the counts of acknowledged writes, writes that gave up, records stored, records stored more than once, and acknowledged records missing.

To do it by hand with Docker Compose, run `status --watch 1` in one terminal. In another, define a shortcut that runs commands in the controller container, so they keep working while brokers are down:

```sh
el() { docker compose exec controller python -m eventlog "$@" --controller http://localhost:8000; }
el produce --topic events --partition 0 --value '"x"' --count 3
docker compose kill broker1
```

Within the 2-second heartbeat timeout b1 shows `DOWN` and b2 leads events:0 at epoch 1. By its next maintenance step, half a second later, b2 has dropped b1 from that ISR. The old records are readable and new `acks=2` writes succeed. Run `docker compose kill broker2` as well: b3 leads events:0 at epoch 2 with ISR `b3`, and `el produce --topic events --partition 0 --value '"z"'` fails with `not enough replicas: acks=2, but only 1 of 3 replicas are in sync (b3)` without storing anything. With `--acks 1` the write succeeds. A partition that b3 already led and that nobody wrote to since the kills still lists the dead brokers in its ISR. Its first `acks=2` write passes the up-front check, is appended and committed on b3 alone, and is readable, even though the command prints an error.

Now `docker compose kill broker3`, then start only the broker that is furthest behind: `docker compose start broker1`. b1 is no longer in the ISR of events:0 or events:1, so both show leader `NONE`, and `produce` and `consume` on them fail with `no in-sync replica is alive`. Before week 9 the controller promoted b1 at this point and the records it lacked were lost. Start the others with `docker compose start broker3 broker2`. b3 leads events:0 and events:1 again, pushes the missed records to b1 and b2, and adds them back to the ISRs. `consume` on partition 0 then returns every record written above.

## Evaluation

`python3 -m eventlog bench` starts a fresh local cluster in subprocesses, writes `--events` records from `--threads` producer threads (each with its own idempotent `Producer`), samples every broker's `/status` every 0.25 seconds for replication lag, then reads every partition back and counts duplicates and lost acknowledged records. `--fault` is `kill-leader` (SIGKILL the leader of events:0 a third of the way in, restart it two thirds in), `lost-replies` (every broker drops the reply to 5% of `/produce` requests) or `slow-follower` (the last broker waits 50 ms before handling each `/replicate`). Each run is saved as JSON under `results/` and printed as a Markdown table. `python3 -m eventlog bench --report results/*.json` prints one table from saved runs, and `sh scripts/sweep.sh` repeats the whole sweep (about 40 minutes).

The runs in [results/](results/) were made on 10 November 2026 on an Apple M2 (8 cores, 16 GB RAM, macOS 27.0.1, Python 3.11.3), with 6 producer threads, 100-character values and replication factor 3. [results/SUMMARY.md](results/SUMMARY.md) has the full tables, column definitions and the per-run numbers.

10,000 records per run, no faults, two runs per setting (records/s is the median, with both runs in parentheses):

| partitions | acks | records/s | p50 ms | p95 ms | p99 ms | max ms | failed |
|---|---|---|---|---|---|---|---|
| 1 | 1 | 232 (227, 237) | 23.2 | 40.7 | 71.5 | 514 | 0 |
| 1 | 2 | 237 (221, 253) | 22.4 | 39.1 | 85.1 | 619 | 0 |
| 1 | 3 | 222 (214, 230) | 22.9 | 43.7 | 102.9 | 740 | 0 |
| 3 | 1 | 303 (294, 312) | 10.9 | 20.4 | 40.6 | 5377 | 12 |
| 3 | 2 | 308 (302, 313) | 11.6 | 19.8 | 36.5 | 6336 | 4 |
| 3 | 3 | 344 (289, 400) | 12.1 | 36.4 | 108.2 | 3425 | 0 |
| 6 | 1 | 330 (178, 482) | 15.7 | 40.6 | 171.0 | 2930 | 0 |
| 6 | 2 | 376 (269, 482) | 13.6 | 29.1 | 68.2 | 641 | 0 |
| 6 | 3 | 483 (374, 592) | 11.8 | 20.2 | 36.7 | 654 | 0 |

10,000 records, 3 partitions, `acks=2`, one run per fault:

| fault | records/s | p50 ms | p95 ms | p99 ms | max ms | acked | duplicates | lost |
|---|---|---|---|---|---|---|---|---|
| kill-leader | 564 | 9.3 | 13.1 | 23.4 | 2022 | 10000 | 0 | 0 |
| lost-replies (5%) | 294 | 5.0 | 205.3 | 215.0 | 1225 | 10000 | 0 | 0 |
| slow-follower (50 ms) | 46 | 124.1 | 141.0 | 155.3 | 827 | 10000 | 0 | 0 |

A large run of 200,000 records on 6 partitions with `acks=2` took 760 seconds: 265 records/s, p50 16.7 ms, p99 110.1 ms, 199,995 acknowledged, 5 failed (3 of them stored anyway), 0 duplicates, 0 lost.

No run lost an acknowledged record or stored a record twice: 409,979 acknowledged records over 22 runs. In the kill-leader run the controller named a new leader for events:0 1.57 seconds after the kill, and the restarted broker was back in every ISR 0.88 seconds after its restart. In the lost-replies run the p95 and p99 are the producer's 0.2-second retry backoff; a total of 503 retries were answered with `"duplicate": true` and the stored offset, and the log holds one copy of each record.

`acks` does not change throughput or latency in a consistent direction, because the leader replicates every write to every ISR member whatever `acks` is. The differences in the table are smaller than the gap between two runs of one setting (178 and 482 records/s for 6 partitions, `acks=1`). More partitions raise throughput, but not in proportion: one partition takes one write at a time, so 6 threads on 1 partition queue behind each other, and past that all partitions share three Python broker processes and one controller. In-sync followers were never more than 1 record behind the leader (2 once, in the large run). The slow follower stayed in the ISR because it answered within the 1-second timeout, and throughput fell from about 300 to 46 records/s.

The failed writes and the multi-second maximum latencies in the fault-free runs are the connection problem from the limitations: 20 of the 21 failed writes ended with `EADDRNOTAVAIL` on a call to the controller, and the other with `no in-sync replica is alive` after the controller had declared every ISR member dead.

## Progress

- Week 4: three independent HTTP brokers, a controller that stores partition-to-leader assignments, producer/consumer clients, and key-based partitioning with per-partition ordering.
- Week 5: broker heartbeats; the controller marks a broker failed after a timeout and stops routing clients to it.
- Weeks 6-7: durable fsynced append-only logs with torn-record recovery, and consumer offsets with commit, restart, and `seek` replay.
- Week 8: synchronous leader-to-follower replication, configurable `acks`, and a committed high watermark.
- Weeks 9-10: ISR tracking through the controller's `/isr`, leader epochs, election only from alive ISR members (unclean election as an option), commit once every ISR member holds a record, epoch fencing of `/replicate` and `/commit`, and leader-driven catch-up that adds restarted or lagging brokers back to the ISR.
- Weeks 11-12: idempotent producers with producer IDs and per-partition sequence numbers, and per-broker fault injection (drop, lost reply, delay, duplicate, and network partitions as pairs of drop rules) through `/faults` and the `faults` command.
- Weeks 13-14: the `bench` command and `scripts/sweep.sh`, with 22 runs from 10,000 to 200,000 records measuring latency, throughput, replication lag, duplicates and lost records across 1, 3 and 6 partitions, `acks` 1 to 3, and three injected faults. Results are in [results/](results/).
- Week 15: the `demo` command, a narrated run that stops, restarts and partitions brokers and loses produce replies while producers keep writing, then checks every record.

The optional parts of the proposal were not built: the controller is still one in-memory process (no etcd), and there is no demo with ML inference workers as consumers.
