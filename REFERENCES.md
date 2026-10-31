# References

None of the code in this repo is copied from Apache Kafka, kafka-python or confluent-kafka-python. The project only uses the Python standard library, and the processes talk to each other over plain HTTP/JSON. The design is borrowed, though. Most of the mechanisms are simplified versions of things Kafka does, and this file lists where each one comes from and how this version differs.

Every class or function with an entry here also has a `Kafka reference:` comment in the code. To list them:

```sh
grep -rn "Kafka reference" eventlog/
```

Source links are pinned to these commits (checked 31 October 2026), so the line numbers stay valid:

- Apache Kafka [0b021e8](https://github.com/apache/kafka/tree/0b021e8c786eda5158ff8e5de142ae1fe32b52ef)
- kafka-python [4b9518e](https://github.com/dpkp/kafka-python/tree/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b)
- confluent-kafka-python [4d30686](https://github.com/confluentinc/confluent-kafka-python/tree/4d306865a29fdec0162136265ed02ef2a745b146)
- librdkafka [8c03f75](https://github.com/confluentinc/librdkafka/tree/8c03f75a31ca61d08b2cc2ddc5b233b80d14bdad), the C library confluent-kafka-python is built on

The documentation pages mentioned below are the [Kafka design docs](https://kafka.apache.org/43/design/design/) (mostly the Replication, Persistence and Message Delivery Semantics sections), the [Kafka introduction](https://kafka.apache.org/42/getting-started/introduction/), [KIP-631](https://cwiki.apache.org/confluence/display/KAFKA/KIP-631%3A+The+Quorum-based+Kafka+Controller) on the KRaft controller, and the original Kafka paper (Kreps, Narkhede & Rao, 2011).

## eventlog/common.py

### `replica_ids()`

Replicas are placed round-robin: partition `p` starts at broker `p` and wraps around the broker list. Kafka's [`StripedReplicaPlacer`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/metadata/placement/StripedReplicaPlacer.java#L77-L117) is round-robin too (the class comment explains it), but it also spreads replicas across racks and starts at a random offset. This code does neither. The first broker in the list is the preferred leader, which is the same convention [`PartitionChangeBuilder.electLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java#L216-L229) uses. Kafka stores the assignment in cluster metadata. Here it is recomputed from the config each time.

### `partition_for_key()`

The Java producer picks a partition for a keyed record with `murmur2(key) % numPartitions` in [`BuiltInPartitioner.partitionForKey()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L413-L414). kafka-python's [`DefaultPartitioner`](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/partitioner/default.py#L6-L28) is a port of the same function, and librdkafka has a [`partitioner` setting](https://github.com/confluentinc/librdkafka/blob/8c03f75a31ca61d08b2cc2ddc5b233b80d14bdad/CONFIGURATION.md?plain=1#L196) with several hash options. This function does the same thing with SHA-256 instead of murmur2, because Python's built-in `hash()` gives different answers in different processes. Records without a key are hashed by their value. Kafka uses sticky partitioning for those instead.

### `NOT_LEADER` and `UNAVAILABLE`

Kafka has separate error codes for "this broker isn't the leader" (`NOT_LEADER_OR_FOLLOWER`, 6) and "not enough replicas" (`NOT_ENOUGH_REPLICAS`, 19), both in [`Errors.java`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java#L203-L231). Clients treat both as retriable: refresh the metadata and try again. kafka-python has matching exceptions, [`NotLeaderForPartitionError` and `NotEnoughReplicasError`](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/errors.py#L211-L306). The brokers here return HTTP 409 and 503 for the same two cases, and the client retries on both.

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

Similar to [`Partition.truncateTo()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L1550) and `UnifiedLog.truncateTo()`, but used in a different place. In Kafka, followers truncate their logs to match the leader, using leader epochs. Here the leader drops its own uncommitted records before its next write. Leader epochs are on the plan in the README.

## eventlog/broker.py

### `BrokerState.produce()`

Kafka's write path is [`ReplicaManager.appendRecords()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/server/ReplicaManager.scala#L638), which calls [`Partition.appendRecordsToLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L1222). Later, [`Partition.maybeIncrementLeaderHW()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/cluster/Partition.scala#L1013) moves the high watermark once enough replicas have the data. `produce()` goes in the same order: append on the leader, copy to the followers, then commit. The `acks` setting is described in Kafka's [`ACKS_DOC`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java#L133), in the [kafka-python `KafkaProducer` docstring](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/producer/kafka.py#L48) and in [librdkafka's config docs](https://github.com/confluentinc/librdkafka/blob/8c03f75a31ca61d08b2cc2ddc5b233b80d14bdad/CONFIGURATION.md?plain=1#L189-L190).

The biggest difference is that Kafka followers pull new records from the leader, while this leader pushes each record to the followers with `/replicate`. `acks` also works differently. In Kafka it is 0, 1 or `all`, and `all` is combined with `min.insync.replicas`. Here it is a number of replicas between 1 and the replication factor. There is no ISR (in-sync replica set) yet.

### `BrokerState.replicate()` and `commit_replica()`

This is the follower side. In Kafka it lives in [`ReplicaFetcherThread`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala) and [`AbstractFetcherThread`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/server/AbstractFetcherThread.scala). The follower fetches from the leader, and the leader's high watermark comes back in the fetch response. Since records are pushed here, the commit has to be a separate `/commit` request.

### `BrokerState._require_leader()` and `_require_sent_by_leader()`

Kafka brokers reject leader-only requests with `NOT_LEADER_OR_FOLLOWER`, and they reject requests from an old leader with `FENCED_LEADER_EPOCH`. There are no epochs here yet, so the broker asks the controller who the leader is on every request.

### `BrokerState.fetch()`

Corresponds to [`ReplicaManager.fetchMessages()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/core/src/main/scala/kafka/server/ReplicaManager.scala#L1665). Consumer reads stop at the high watermark in both.

### `BrokerState.heartbeat_loop()`

Kafka brokers send a `BrokerHeartbeat` request to the controller every `broker.heartbeat.interval.ms` (KIP-631). This loop sends one every 0.5 seconds.

## eventlog/controller.py

### `ControllerState.heartbeat()` and `_alive()`

[`BrokerHeartbeatManager`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/controller/BrokerHeartbeatManager.java#L167-L245) in the KRaft controller fences a broker when its session runs out (`broker.session.timeout.ms` in KIP-631). `_alive()` does the same check: a broker is alive if its last heartbeat arrived within `heartbeat_timeout`, which is 2 seconds by default.

### `ControllerState.route()`

Leader election is modeled on [`PartitionChangeBuilder.electLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/controller/PartitionChangeBuilder.java#L216-L229), which is called from [`ReplicationControlManager`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java). A leader that is still alive stays leader, even if the preferred replica comes back. Otherwise the next live replica in assignment order takes over. Kafka only elects from the ISR unless unclean leader election is turned on. This code will pick any live replica, which is why the README warns that failover can lose data. The controller is also one process with its state in memory, not a Raft quorum like KRaft.

### `ControllerState.metadata()`

A very small version of Kafka's Metadata API, which clients use to find topics, partitions and brokers.

## eventlog/client.py

### `ClusterClient.route()` and `_leader_url()`

Kafka clients look up the leader in cached metadata: [`Metadata.currentLeader()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/Metadata.java#L296) in the Java client and [`ClusterMetadata.leader_for_partition()`](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/cluster.py#L302) in kafka-python. In confluent-kafka-python the same information is in [`PartitionMetadata`](https://github.com/confluentinc/confluent-kafka-python/blob/4d306865a29fdec0162136265ed02ef2a745b146/src/confluent_kafka/admin/_metadata.py#L100-L121), which has `leader`, `replicas` and `isrs` fields. The `/route` response uses the same `leader` and `replicas` names. This client doesn't cache anything and asks the controller before every request.

### `ClusterClient.produce()`

The Java producer's [`Sender`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java) retries retriable errors after asking for fresh metadata ([`Metadata.requestUpdate()`](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/Metadata.java#L201)). The [kafka-python `KafkaProducer` docstring](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/producer/kafka.py#L48-L53) explains the `retries` setting and warns that retries can create duplicates. That is true here as well, because there is no idempotent producer yet. The Message Delivery Semantics section of the design docs covers the same problem.

### `Consumer.position()`, `seek()` and `commit()`

The method names come from the Kafka consumer API, which has the same three methods in the [Java client](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java#L1304-L1378), [kafka-python](https://github.com/dpkp/kafka-python/blob/4b9518e2c8d79613c129e7feb20d93e7fe9bcf9b/kafka/consumer/group.py#L685-L1053) and [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python/blob/4d306865a29fdec0162136265ed02ef2a745b146/src/confluent_kafka/src/Consumer.c#L1666-L1743). What a committed offset means comes from the [`KafkaConsumer` javadoc](https://github.com/apache/kafka/blob/0b021e8c786eda5158ff8e5de142ae1fe32b52ef/clients/src/main/java/org/apache/kafka/clients/consumer/KafkaConsumer.java#L287): "The committed offset should always be the offset of the next message that your application will read." Committing after processing gives at-least-once delivery. Kafka keeps committed offsets in the `__consumer_offsets` topic through a group coordinator. This consumer writes them to a local JSON file, and there are no consumer groups.
