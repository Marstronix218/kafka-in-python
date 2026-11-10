# Benchmark results

These are the runs from `sh scripts/sweep.sh` on 10 November 2026, one JSON file per run in
this directory. The sweep ran from 01:58:39 to 02:34:25 PST (35 min 46 s of wall-clock time)
on the code with the fixes described in the next section.

## Changes since the first sweep

A first sweep was run earlier the same day. It is archived outside the repository and none
of its numbers are used here. It ran on code with two bugs that a code review found and
reproduced, and both could lose an acknowledged record:

- A late `/replicate` batch could truncate records from an in-sync follower. A batch the
  leader had already given up on (for example while the follower's handler waited for the
  partition lock) could still be applied after later batches. The follower then cut its
  log back to that batch's `leader_last_offset` and removed records the leader had since
  committed and acknowledged. A follower now refuses a batch whose `leader_last_offset` is
  lower than one it already applied at the same leader epoch, and changes nothing.
- A reused producer sequence could acknowledge a record that was never stored. After a
  failed write the producer could give the failed record's sequence to the next record. If
  the failed attempt was still queued at the leader and stored first, the leader answered
  the new record as a duplicate, with the offset of the old one. Now a partition moves to a
  new producer id after a failed write, so a sequence is never given to two records, and
  the leader refuses a repeated sequence whose key or value differs.

The first sweep's accounting could not detect either bug. It read the records back from
each leader only, so a follower missing records went unseen, and it identified records by
producer id and sequence, so a record acknowledged under another record's sequence counted
as stored. The benchmark now identifies records by a tag in the value and checks the value
at each acknowledged offset. It also compares every replica's committed records after the
run (the replica diff column). Both bugs have regression tests.

## Setup

- Machine: Apple M2, 8 cores, 16 GB RAM, macOS 27.0.1 (arm64), Python 3.11.3.
- Code: commit e03e614 plus the uncommitted weeks 9-15 changes and the two fixes
  (`git_dirty: true` in every file). The first sweep's files name the same commit, because
  none of these changes are committed yet.
- Cluster: one controller and three brokers, each a separate process on 127.0.0.1, started
  fresh for every run by `eventlog/cluster.py`. Topic `events`, replication factor 3,
  controller heartbeat timeout 2 s.
- Workload: 6 producer threads, each with its own idempotent `Producer`. Record i goes to
  partition i mod P, and thread w writes records w, w+6, w+12 and so on. Each value is a
  100-character string that starts with the tag `<worker>/<index>;`, which identifies the
  record. A producer retries a record up to 10 times before giving up.
- Every record is fsynced on the leader and on each follower, and every request is a new
  HTTP connection.
- After the producers stop, the benchmark waits until every partition has a full ISR and
  every replica has committed the leader's whole log. Then it reads every partition from
  its leader, stops the brokers and reads each replica's committed records from its data
  directory.

Columns:

- records/s: acknowledged records divided by the time the producer threads ran.
- latency: time for one `produce()` call, retries included, over acknowledged records only.
- isr lag: records the slowest in-sync follower was behind the leader's log end, sampled
  from `/status` every 0.25 s for every partition.
- leader changes: total rise in leader epochs across all partitions during the run.
- failed: records whose producer gave up. The producer can't know whether they were stored,
  and some were.
- duplicates: extra copies of the same tag found when every partition was read back from
  offset 0 (a record stored three times counts 2).
- lost: acknowledged records whose acknowledged offset, read back from the leader, is
  missing or holds a different value.
- replica diff: partitions where a replica's committed records, read from disk after the
  cluster settled, differ from the leader's.

## Partitions and acks

10,000 records per run, no faults, two runs per setting. Throughput is the median of the
two runs with both values in parentheses (first pass, then second pass). Latencies and the
ISR lag mean are the median of the two runs, except max, which is the larger. Counts are
totals over both runs (20,000 records).

| partitions | acks | records/s | p50 ms | p95 ms | p99 ms | max ms | isr lag max / mean | leader changes | failed | duplicates | lost | replica diff |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 | 254 (245, 264) | 22.6 | 32.1 | 47.8 | 256 | 1 / 0.20 | 0 | 0 | 0 | 0 | 0 |
| 1 | 2 | 249 (250, 249) | 22.5 | 34.7 | 55.1 | 266 | 1 / 0.18 | 0 | 0 | 0 | 0 | 0 |
| 1 | 3 | 238 (217, 259) | 22.8 | 38.5 | 82.7 | 525 | 1 / 0.22 | 0 | 0 | 0 | 0 | 0 |
| 3 | 1 | 303 (310, 296) | 10.8 | 22.2 | 61.2 | 7024 | 1 / 0.20 | 1 | 9 | 0 | 0 | 0 |
| 3 | 2 | 358 (352, 365) | 12.1 | 31.0 | 81.0 | 1537 | 1 / 0.25 | 0 | 0 | 0 | 0 | 0 |
| 3 | 3 | 323 (308, 338) | 11.8 | 28.8 | 70.5 | 3788 | 1 / 0.24 | 1 | 6 | 0 | 0 | 0 |
| 6 | 1 | 562 (548, 576) | 9.4 | 16.4 | 30.8 | 271 | 1 / 0.21 | 0 | 0 | 0 | 0 | 0 |
| 6 | 2 | 569 (552, 587) | 9.5 | 15.5 | 31.3 | 381 | 1 / 0.23 | 0 | 0 | 0 | 0 | 0 |
| 6 | 3 | 498 (578, 418) | 9.9 | 20.0 | 49.1 | 783 | 1 / 0.21 | 0 | 0 | 0 | 0 | 0 |

## Faults

10,000 records, 3 partitions, acks=2, one run each.

| fault | records/s | p50 ms | p95 ms | p99 ms | max ms | isr lag max / mean | any replica lag max | acked | failed | duplicates | lost | replica diff | wall s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| kill-leader | 356 | 13.5 | 29.1 | 58.5 | 2034 | 1 / 0.20 | 1381 | 10000 | 0 | 0 | 0 | 0 | 28.8 |
| lost-replies (5%) | 268 | 6.9 | 206.8 | 220.1 | 726 | 1 / 0.13 | 1 | 10000 | 0 | 0 | 0 | 0 | 38.0 |
| slow-follower (50 ms) | 45 | 124.6 | 143.9 | 183.5 | 2353 | 1 / 0.54 | 1 | 10000 | 0 | 0 | 0 | 0 | 223.9 |

- kill-leader: b1, the leader of events:0, was killed with SIGKILL 8.5 s in, a third of the
  way through. The controller named b2 leader of events:0 (epoch 1) 1.58 s later. b1 was
  restarted 16.4 s in, two thirds of the way through, and was back in every ISR 1.39 s
  after the restart. The 2.0 s maximum latency fits a write to events:0 that waited out
  the failover. The sampler skips a replica that doesn't answer, so the replica lag of 1381
  is b1 after its restart, before it had caught up.
- lost-replies: every broker dropped the reply to 5% of `/produce` requests after handling
  them. 508 retries were answered with `"duplicate": true` and the stored offset, and the
  log holds one copy of each record. The p95 and p99 are the producer's 0.2 s retry backoff.
- slow-follower: b3 waited 50 ms before handling each `/replicate`.

## Large run

200,000 records, 6 partitions, acks=2, no injected faults: 672.8 s wall-clock time (produce
phase 662.1 s).

| records/s | p50 ms | p95 ms | p99 ms | max ms | isr lag max / mean | leader changes | smallest ISR | acked | failed | failed but stored | stored | duplicates | lost | replica diff |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 302 | 14.1 | 34.1 | 80.6 | 14416 | 1 / 0.25 | 18 | 1 | 199991 | 9 | 6 | 199997 | 0 | 0 | 0 |

7 of the 9 failed records ended with `[Errno 49] Can't assign requested address` on a
`/route` call to the controller. A result keeps only its 5 most common error messages,
which here cover 7 failures, so the error text of the other 2 was not saved. 6 of the 9
were stored before the producer gave up, so the read found 199,997 records: every
acknowledged one once, plus those 6.

## What the numbers show

No run lost an acknowledged record, stored a record twice, or ended with a replica whose
committed records differ from its leader's: 409,976 acknowledged records over 22 runs, and
0 in the lost, duplicates and replica diff columns of every run. No run stored a record
with a missing tag or a tag no producer sent. Of the 24 failed records, 12 were stored
anyway. Retries answered as duplicates also happened without injected faults (20 in the
fault-free 10,000-record runs, 103 in the large run): an earlier attempt had stored the
record but the producer never got a success reply for it, and the sequence check answered
the retry with the stored offset.

acks still has no consistent effect on throughput or latency. The leader always replicates
each write to every ISR member before replying (`_sync_isr` in broker.py), and acks only
sets the smallest ISR that may accept a write, so while the ISR is full the work per write
is the same for acks 1, 2 and 3. At 1 partition the medians fall slightly from acks=1 to
acks=3 (254, 249, 238), by less than the gap between the two acks=3 runs (217 and 259). At
6 partitions acks=3 has the lowest median because one of its runs did 418 records/s; the
other did 578, more than either run at acks=1. At 3 partitions acks=2 is the fastest
setting (358 against 303 and 323). Its two runs had no failed writes and a full ISR
throughout, while 3 of the 4 runs at acks 1 and 3 had failed writes and an ISR down to 1
(see the next section). The one acks=3 run without failures did 338 records/s, against 352
and 365 for acks=2.

More partitions raise throughput, but not in proportion: 217-264 records/s with 1
partition, 296-365 with 3, and 418-587 with 6 (548-587 in five of the six runs). A
partition takes one write at a time (the leader holds the partition lock while it
replicates), so with 6 threads on 1 partition each write waits behind the others: 6 in
flight at 250/s is 24 ms, close to the 22.6 ms p50. With 6 partitions each thread has a
partition to itself and nothing queues. The p50 of 9.4 ms is then the time of one write
while six run at once on the same three broker processes, each limited by Python's GIL,
and one controller that every request consults.

The in-sync followers were never more than 1 record behind in any sample of any run. The
leader replicates inside the produce request, so a lag of 1 is a write sampled between the
leader's append and its `/replicate`. A mean of about 0.2 means a write was caught in flight
in about one sample in five. The slow-follower run has the highest mean, 0.54.

A slow follower slows every write to the partitions it follows: 50 ms per `/replicate`
brought throughput from 352 and 365 records/s (the two fault-free runs with the same
settings) to 45, and p50 to 124.6 ms (two threads write to each partition, so a write
usually waits for one other). The follower stays in the ISR because it answers within the
1 s peer timeout. Kafka removes such a follower after `replica.lag.time.max.ms`; this
project has no equivalent yet.

## Connection exhaustion

Every HTTP request is a new TCP connection, and on macOS each closed one stays in
TIME_WAIT for 30 s (net.inet.tcp.msl is 15000 ms) while holding one of the 16,384
ephemeral ports (49152-65535). One produce with two in-sync followers takes 11 requests, 6
of them `/route` calls to the controller (from the client, the leader, and each follower
for `/replicate` and `/commit`). When the ports run out, connects to the controller fail
with `EADDRNOTAVAIL` (errno 49). Heartbeats then fail, the controller declares brokers dead
and moves leadership, and leaders drop followers they can't reach.

In this sweep 22 of the 24 failed writes ended with that error on a `/route` call to the
controller. The other 2 are the large-run failures whose error text was not saved, and no
saved error says `no in-sync replica is alive`. Every run that went above 290 records/s
with its producers running for more than 30 s had failed writes: both 3-partition runs at
acks=1, the first at acks=3, and the large run. No run that finished within 30 s had a
failed write, including all six 6-partition runs at 418-587 records/s, though four of
those dropped a follower from an ISR for at least one sample. The three fault-free
10,000-record runs with failed writes also have the three largest maximum latencies of
those runs (3.8, 6.2 and 7.0 s) and their only two leader changes.

The large run shows the cost over a longer time: 302 records/s with 18 leader changes and
an ISR down to 1, against 552 and 587 records/s for the two 10,000-record runs with the
same settings, which finished in 17-18 s. Reusing connections (HTTP keep-alive) or letting
brokers cache routes would cut the number of connections per produce.

## Reproducing

    sh scripts/sweep.sh                                  # the whole sweep, 36 minutes here
    python3 -m eventlog bench --help                     # one run with other settings
    python3 -m eventlog bench --report results/*.json    # one table from saved runs
