# References

None of the code in this repo is copied from Apache Kafka, kafka-python or confluent-kafka-python. The project only uses the Python standard library, and the processes talk to each other over plain HTTP/JSON. The design is borrowed, though. Most of the mechanisms are simplified versions of things Kafka does, and this file lists where each one comes from and how this version differs.

Every class or function with an entry here also has a `Kafka reference:` comment in the code, on the class or function itself or, for `faults.py` and `bench.py`, in the module docstring. To list them:

```sh
grep -rn "Kafka reference" eventlog/
```

Source links are pinned to these commits (checked 31 October 2026; the links for weeks 9-15 were checked on 10 November 2026), so the line numbers stay valid:

- Apache Kafka [0b021e8](https://github.com/apache/kafka/tree/0b021e8c786eda5158ff8e5de142ae1fe32b52ef)
- kafka-python [4b9518e](https://github.com/dpkp/kafka-python/tree/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b)
- confluent-kafka-python [4d30686](https://github.com/confluentinc/confluent-kafka-python/tree/4d306865a29fdec0162136265ed02ef2a745b146)
- librdkafka [8c03f75](https://github.com/confluentinc/librdkafka/tree/8c03f75a31ca61d08b2cc2ddc5b233b80d14bdad), the C library confluent-kafka-python is built on

The documentation pages mentioned below are the [Kafka design docs](https://kafka.apache.org/43/design/design/) (mostly the Replication, Persistence and Message Delivery Semantics sections), the [Kafka introduction](https://kafka.apache.org/42/getting-started/introduction/), [KIP-631](https://cwiki.apache.org/confluence/display/KAFKA/KIP-631%3A+The+Quorum-based+Kafka+Controller) on the KRaft controller, [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) on idempotent and transactional producers, and the original Kafka paper (Kreps, Narkhede & Rao, 2011).

## eventlog/common.py

### `replica_ids()`

Replicas are placed round-robin: partition `p` starts at broker `p` and wraps around the broker list. Kafka's [`StripedReplicaPlacer`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/metadata/placement/StripedReplicaPlacer.java#L77-L117) is round-robin too (the class comment explains it), but it also spreads replicas across racks and starts at a random offset. This code does neither. The first broker in the list is the preferred leader, which is the same convention [`PartitionChangeBuilder.electLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java#L216-L229) uses. Kafka stores the assignment in cluster metadata. Here it is recomputed from the config each time.

### `partition_for_key()`

The Java producer picks a partition for a keyed record with `murmur2(key) % numPartitions` in [`BuiltInPartitioner.partitionForKey()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L413-L414). kafka-python's [`DefaultPartitioner`](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/partitioner/default.py#L6-L28) is a port of the same function, and librdkafka has a [`partitioner` setting](https://github.com/confluentinc/librdkafka/blob/8c03f75a31ca61d08b2cc2ddc5b233b80d14bdad/CONFIGURATION.md?plain=1#L196) with several hash options. This function does the same thing with SHA-256 instead of murmur2, because Python's built-in `hash()` gives different answers in different processes. Records without a key are hashed by their value. Kafka uses sticky partitioning for those instead.

### `NOT_LEADER` and `UNAVAILABLE`

Kafka has separate error codes for "this broker isn't the leader" (`NOT_LEADER_OR_FOLLOWER`, 6) and "not enough replicas" (`NOT_ENOUGH_REPLICAS`, 19), both in [`Errors.java`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L203-L231). Clients treat both as retriable: refresh the metadata and try again. kafka-python has matching exceptions, [`NotLeaderForPartitionError` and `NotEnoughReplicasError`](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/errors.py#L211-L306). The brokers here return HTTP 409 and 503 for the same two cases, and the client retries on both. A produce is rejected with 503 before anything is appended when the partition's ISR is smaller than `acks`, which is when Kafka returns `NOT_ENOUGH_REPLICAS`. 503 also covers a broker that can't be reached and a partition with no leader.

### `BAD_SEQUENCE`

HTTP 422 stands for two Kafka errors, [`OUT_OF_ORDER_SEQUENCE_NUMBER`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L287) (45) and [`DUPLICATE_SEQUENCE_NUMBER`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L289) (46). Here the first means a producer's sequence leaves a gap after the last one the leader stored, and the second means the sequence is older than the 5 the leader keeps, so the leader can't say where that record is. The client doesn't retry 422, because sending the same sequence again gets the same answer. A sequence the leader does keep is not an error here: the leader answers with the stored offset and `"duplicate": true`. 422 is also the answer when the leader holds the sequence with a different key or value (`sequence reused with different content`), because answering "duplicate" would acknowledge a record that is not stored.

### `MAX_RECORD_BYTES`

1 MiB is the default for the producer setting `max.request.size` ([`ProducerConfig.java`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L461)). The broker has a similar limit called `message.max.bytes`. A record over the limit gets a 413 here, which is roughly Kafka's `MESSAGE_TOO_LARGE`.

## eventlog/storage.py

### `PartitionLog`

In Kafka a partition's log is a [`UnifiedLog`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java) made of [`LogSegment`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java) files, which hold binary record batches plus offset and time indexes. `PartitionLog` keeps the basic idea, one append-only file per partition where a record's offset is its position. It stores everything in a single JSONL file, with no indexes, retention or compaction. The matching methods in `UnifiedLog` are `appendAsLeader` (line 1033), `read` (line 1663) and `truncateTo` (line 2356). The Persistence section of the design docs explains why Kafka is built around an append-only log in the first place.

### `committed_offset`, `commit()` and `read()`

These do the job of Kafka's high watermark ([`UnifiedLog.highWatermark()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java#L506)). The design docs say "Only committed messages are ever given out to the consumer", and `read()` follows that rule: records past the committed offset are stored but not returned. One difference is that `committed_offset` is the offset of the last committed record, while Kafka's high watermark is one past it.

### `_persist_committed_offset()` and `_atomic_replace()`

Kafka writes its checkpoint files (including `replication-offset-checkpoint`, where the high watermarks are kept) by writing a `.tmp` file, fsyncing it and renaming it over the old one. See [`CheckpointFile.write()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/server-common/src/main/java/org/apache/kafka/server/common/CheckpointFile.java#L71-L85), which calls [`Utils.atomicMoveWithFallback()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L928). This code does the same thing, so after a crash the file holds either the old value or the new one.

### `_load_records()`

After an unclean shutdown Kafka re-reads the log and truncates at the first invalid batch ([`LogSegment.recover()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L482), run from [`LogLoader`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/storage/src/main/java/org/apache/kafka/storage/internals/log/LogLoader.java)). This code is stricter. It only drops a damaged last line, which is what a write cut off by a crash looks like. Damage anywhere else stops the broker with a corruption error.

### `truncate_from()`

Similar to [`Partition.truncateTo()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L1550) and `UnifiedLog.truncateTo()`. In both projects it is the follower that truncates, to make its log match the leader's. A Kafka follower finds the point to cut at from leader epochs: [`LeaderEpochFileCache`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java#L53) keeps the first offset of each epoch on every replica. This project keeps no epoch history in the log. Instead, `BrokerState.replicate()` drops anything past the leader's last offset, `append()` replaces a conflicting uncommitted record along with everything after it, and a follower outside the ISR only trusts its committed records, so the leader resends everything after those. The leader never truncates its own log.

### `PRODUCER_STATE_ENTRIES` and `producer_sequences()`

Kafka's [`ProducerStateManager`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java#L70) maps each producer id to its last appended entries (epoch, sequence, last offset), keeping the last 5 batches per producer. This log keeps the last 5 `(sequence, offset)` pairs per producer, one per record, since there are no batches. Kafka writes snapshots of this state to disk and expires producer ids that haven't been used for a while. Here the state is rebuilt by reading the whole log when a `PartitionLog` loads and after every truncation, and it never expires. Because the state comes from the records themselves, any replica that becomes leader has it.

## eventlog/broker.py

### `BrokerState.produce()`

Kafka's write path is [`ReplicaManager.appendRecords()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/server/ReplicaManager.scala#L638), which calls [`Partition.appendRecordsToLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L1222). Later, [`Partition.maybeIncrementLeaderHW()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L1013) moves the high watermark once enough replicas have the data. `produce()` goes in the same order: append on the leader, copy to the followers, then commit. The `acks` setting is described in Kafka's [`ACKS_DOC`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L133), in the [kafka-python `KafkaProducer` docstring](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/producer/kafka.py#L48) and in [librdkafka's config docs](https://github.com/confluentinc/librdkafka/blob/8c03f75a31ca61d08b2cc2ddc5b233b80d14bdad/CONFIGURATION.md?plain=1#L189-L190).

The biggest difference is that Kafka followers pull new records from the leader, while this leader pushes each record to the followers with `/replicate`. `acks` also works differently. In Kafka it is 0, 1 or `all`, and `all` is combined with `min.insync.replicas`. Here every write waits for the whole ISR, as with `acks=all`, and `acks` is a number between 1 and the replication factor that plays the part of `min.insync.replicas`: the smallest ISR, leader included, that may accept a write.

[`Partition.checkEnoughReplicasReachOffset()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L949) is where Kafka decides an `acks=all` produce is done: once the high watermark has reached the record, the produce succeeds if the ISR is at least `min.insync.replicas`, and otherwise fails with [`NOT_ENOUGH_REPLICAS_AFTER_APPEND`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L233) (20) although the record is in the log. `_replicate_for_acks()` has the same two outcomes. The record is committed when every ISR member that is left holds it, and if the ISR shrank below `acks` on the way the producer gets 503 `not enough replicas after append`. A retry with the same producer id and sequence is answered from the log instead of appending again.

### `BrokerState._check_sequence()`

The leader-side sequence checks in Kafka are in [`ProducerAppendInfo`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java#L47). Both accept the next sequence after the last stored one and reject a gap with `OUT_OF_ORDER_SEQUENCE_NUMBER`. Kafka's checks also cover the producer epoch and transactions, and they reject a first sequence other than 0 from an unknown producer when the log is empty. Here the first record from an unknown producer may use any sequence. `_check_sequence()` also compares the key and value of a repeated sequence with the stored record and answers 422 if they differ.

### `BrokerState._sync_isr()` and `_alter_isr()`

[`Partition.maybeShrinkIsr()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L1092) removes the followers that [`getOutOfSyncReplicas()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L1158) reports, which are followers that have not caught up to the leader's log end within `replica.lag.time.max.ms`. The leader then asks the controller to change the ISR with an AlterPartition request ([`AlterPartitionRequest.json`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/resources/common/message/AlterPartitionRequest.json)). `_sync_isr()` drops a follower when a push to it fails, times out after 1 second, or leaves it still behind after 10 batches, and `_alter_isr()` sends the new ISR to the controller's `/isr`. There is no time-based lag check, so a slow follower that keeps answering within the timeout stays in the ISR. A dead follower that is already known to be caught up is not contacted again until the next write, so it also stays in the ISR until then.

### `BrokerState.maintain()` and `_push()`

[`Partition.maybeExpandIsr()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L878) adds a follower back to the ISR once its log end has reached the high watermark within the current leader epoch, and it runs when a follower's fetch moves its log end forward. Here the leader's maintenance loop pushes records to alive replicas outside the ISR every 0.5 seconds and adds a replica once its log matches the leader's all the way to the log end, which is stricter. The same loop resends records and `/commit` requests to ISR followers that missed them, and commits records a new leader inherited.

When a Kafka broker becomes leader, [`Partition.makeLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L590) resets what it knows about the other replicas' log ends. `_push()` does the same through `_follower_state()`, which discards the per-follower offsets whenever the leader epoch changes.

### `BrokerState.replicate()` and `commit_replica()`

This is the follower side. In Kafka it lives in [`ReplicaFetcherThread`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala) and [`AbstractFetcherThread`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/server/AbstractFetcherThread.scala). The follower fetches from the leader, and the leader's high watermark comes back in the fetch response. Here the leader pushes batches of up to 100 records, read with `PartitionLog.read_range()`, and the leader's committed offset travels with each batch. A follower commits only up to the end of the batch it received, because later records in its log may not match the leader's yet. A batch that starts past the point where the follower knows its log matches (its log end in the ISR, its committed offset outside it) gets a 409 carrying the follower's offsets, and the leader resends from there. A Kafka follower finds that point itself, from leader epochs. A follower here also refuses, with 409 and no change, a batch whose `leader_last_offset` is lower than one it already applied at the same leader epoch. Such a batch was sent before the one applied, and its truncation would remove records sent after it. `/commit` is still a separate request, sent after a successful produce and repeated by the maintenance loop.

### `BrokerState._require_leader()` and `_require_sent_by_leader()`

Kafka brokers reject leader-only requests with `NOT_LEADER_OR_FOLLOWER`, and they reject requests from an old leader with [`FENCED_LEADER_EPOCH`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L357) (74). `/replicate` and `/commit` carry the sender's ID and leader epoch, and the follower rejects them with 409 unless both match the controller's current route. In Kafka each broker stores the current leader epoch of its partitions, set by [`Partition.makeLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L590) and [`Partition.makeFollower()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L696) when the controller's metadata changes. Brokers here store no epoch and ask the controller on every request instead.

### `BrokerState.fetch()`

Corresponds to [`ReplicaManager.fetchMessages()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/server/ReplicaManager.scala#L1665). Consumer reads stop at the high watermark in both.

### `BrokerState.heartbeat_loop()`

Kafka brokers send a `BrokerHeartbeat` request to the controller every `broker.heartbeat.interval.ms` (KIP-631). This loop sends one every 0.5 seconds.

## eventlog/controller.py

### `ControllerState.heartbeat()` and `_alive()`

[`BrokerHeartbeatManager`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/controller/BrokerHeartbeatManager.java#L167-L245) in the KRaft controller fences a broker when its session runs out (`broker.session.timeout.ms` in KIP-631). `_alive()` does the same check: a broker is alive if its last heartbeat arrived within `heartbeat_timeout`, which is 2 seconds by default.

### `ControllerState.route()`

Leader election is modeled on [`PartitionChangeBuilder.electLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java#L216-L229), which is called from [`ReplicationControlManager`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java). A leader that is still alive stays leader, even if the preferred replica comes back. Otherwise the first live ISR member in assignment order takes over, and if no ISR member is alive the partition has no leader. As in Kafka, `--unclean-leader-election` allows any live replica to lead in that case, which can lose committed records; this code then sets the ISR to the new leader alone. A clean election leaves the ISR unchanged, and the new leader removes unreachable members the first time it fails to replicate to them.

Every leader change adds 1 to the partition's leader epoch, which `/route` returns next to the leader and the ISR. The controller is one process with its state in memory, not a Raft quorum like KRaft. If it restarts, every epoch and ISR starts over.

### `ControllerState.alter_isr()`

The controller side of AlterPartition ([`AlterPartitionRequest.json`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/resources/common/message/AlterPartitionRequest.json)), which leaders send from `Partition.maybeShrinkIsr()` and `Partition.maybeExpandIsr()`. `/isr` replaces a partition's ISR only if the sender is the current leader at the current leader epoch, and only with an ISR that contains the leader and nothing but replicas. A request that fails these checks gets a 409, and the broker treats that as having lost leadership.

### `ControllerState.metadata()`

A very small version of Kafka's Metadata API, which clients use to find topics, partitions and brokers.

## eventlog/faults.py

### `FaultInjector`

Kafka's fault-injection framework, Trogdor, describes faults as task specs. [`NetworkPartitionFaultSpec`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/trogdor/src/main/java/org/apache/kafka/trogdor/fault/NetworkPartitionFaultSpec.java#L35) splits nodes into groups that can't reach each other, and [`DegradedNetworkFaultSpec`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/trogdor/src/main/java/org/apache/kafka/trogdor/fault/DegradedNetworkFaultSpec.java#L29) adds latency and a rate limit to a node's network device. Trogdor applies them on the hosts with `iptables` and `tc`. `FaultInjector` is a rule table inside each broker that drops, delays, duplicates or loses the reply to that broker's HTTP calls and requests. A network partition between two brokers is a `drop` rule on each side, and a delay rule on incoming `/replicate` plays the part of a degraded link. Because the rules work on HTTP requests, one can match a single endpoint such as `/produce`.

Trogdor's process fault ([`ProcessStopFaultController`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/trogdor/src/main/java/org/apache/kafka/trogdor/fault/ProcessStopFaultController.java#L25) picks the target nodes) pauses processes with SIGSTOP and resumes them with SIGCONT. The `kill-leader` fault in `bench.py` and step 3 of the demo kill a broker with SIGKILL and later start it again on the same data directory, which is a crash and restart rather than a pause.

## eventlog/bench.py

### `run_benchmark()`

Kafka's [`ProducerPerformance`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java#L52) tool writes a number of records and prints records/s with the 50th, 95th, 99th and 99.9th percentile latency ([lines 564-565](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java#L564-L565)). The benchmark here reports records/s and p50, p95, p99 and maximum latency of each `produce()` call, retries included. It also starts its own cluster, can inject a fault, samples replication lag from every broker's `/status`, and reads every partition back to count duplicate and lost records. [`EndToEndLatency`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/tools/src/main/java/org/apache/kafka/tools/EndToEndLatency.java#L65) measures the time from producing a record to consuming it. This benchmark doesn't measure that.

## eventlog/client.py

### `ClusterClient.route()` and `_leader_url()`

Kafka clients look up the leader in cached metadata: [`Metadata.currentLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/Metadata.java#L296) in the Java client and [`ClusterMetadata.leader_for_partition()`](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/cluster.py#L302) in kafka-python. In confluent-kafka-python the same information is in [`PartitionMetadata`](https://github.com/confluentinc/confluent-kafka-python/blob/4d306865a29fdec0162136265ed02ef2a745b146/src/confluent_kafka/admin/_metadata.py#L100-L121), which has `leader`, `replicas` and `isrs` fields. The `/route` response uses the same `leader` and `replicas` names. This client doesn't cache anything and asks the controller before every request.

### `ClusterClient.produce()`

The Java producer's [`Sender`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java) retries retriable errors after asking for fresh metadata ([`Metadata.requestUpdate()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/Metadata.java#L201)). The [kafka-python `KafkaProducer` docstring](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/producer/kafka.py#L48-L53) explains the `retries` setting and warns that retries can create duplicates. The Message Delivery Semantics section of the design docs covers the same problem. Here every retry resends the same producer id and sequence, and without an explicit `producer_id` the call goes through the client's own `Producer`, so retries don't store a record twice.

### `Producer`

The producer id and the per-partition sequence numbers come from [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging). In the Java client they are kept by [`TransactionManager`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L99), which also handles transactions. A Kafka producer gets its id from the cluster and carries a producer epoch, which brokers use to fence an old instance of the same producer. Here the client makes up a random UUID and there is no epoch, so two live producers with the same id interfere. Kafka allows several batches in flight per partition. This `Producer` sends one record at a time per partition. After a failed write it doesn't know whether the record was stored, and an attempt may still be queued at the leader and stored later. So it never gives that sequence to another record: the partition's next record goes out under a new random producer id at sequence 0, which the leader accepts as a producer it has not seen.

### `Consumer.position()`, `seek()` and `commit()`

The method names come from the Kafka consumer API, which has the same three methods in the [Java client](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java#L1304-L1378), [kafka-python](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/consumer/group.py#L685-L1053) and [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python/blob/4d306865a29fdec0162136265ed02ef2a745b146/src/confluent_kafka/src/Consumer.c#L1666-L1743). What a committed offset means comes from the [`KafkaConsumer` javadoc](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java#L287): "The committed offset should always be the offset of the next message that your application will read." Committing after processing gives at-least-once delivery. Kafka keeps committed offsets in the `__consumer_offsets` topic through a group coordinator. This consumer writes them to a local JSON file, and there are no consumer groups.
