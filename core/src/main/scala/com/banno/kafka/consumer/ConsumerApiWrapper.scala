/*
 * Copyright 2019 Jack Henry & Associates, Inc.®
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.banno.kafka.consumer

import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._

import scala.concurrent.duration._
import java.util.regex.Pattern

import scala.collection.mutable

trait ConsumerApiWrapper[F[_], K, V] extends ConsumerApi[F, K, V] {
  def api: ConsumerApi[F, K, V]
  def assign(partitions: Iterable[TopicPartition]): F[Unit] = api.assign(partitions)
  def assignment: F[Set[TopicPartition]] = api.assignment
  def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    api.beginningOffsets(partitions)
  def close: F[Unit] = api.close
  def close(timeout: FiniteDuration): F[Unit] = api.close(timeout)
  def commitAsync: F[Unit] = api.commitAsync
  def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): F[Unit] = api.commitAsync(offsets, callback)
  def commitAsync(callback: OffsetCommitCallback): F[Unit] = api.commitAsync(callback)
  def commitSync: F[Unit] = api.commitSync
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] = api.commitSync(offsets)
  def committed(partition: Set[TopicPartition]): F[mutable.Map[TopicPartition, OffsetAndMetadata]] =
    api.committed(partition)
  def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    api.endOffsets(partitions)
  def listTopics: F[Map[String, Seq[PartitionInfo]]] = api.listTopics
  def metrics: F[Map[MetricName, Metric]] = api.metrics
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndTimestamp]] =
    api.offsetsForTimes(timestampsToSearch)
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] = api.partitionsFor(topic)
  def pause(partitions: Iterable[TopicPartition]): F[Unit] = api.pause(partitions)
  def paused: F[Set[TopicPartition]] = api.paused
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] = api.poll(timeout)
  def position(partition: TopicPartition): F[Long] = api.position(partition)
  def resume(partitions: Iterable[TopicPartition]): F[Unit] = api.resume(partitions)
  def seek(partition: TopicPartition, offset: Long): F[Unit] = api.seek(partition, offset)
  def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
    api.seekToBeginning(partitions)
  def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] = api.seekToEnd(partitions)
  def subscribe(topics: Iterable[String]): F[Unit] = api.subscribe(topics)
  def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
    api.subscribe(topics, callback)
  def subscribe(pattern: Pattern): F[Unit] = api.subscribe(pattern)
  def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
    api.subscribe(pattern, callback)
  def subscription: F[Set[String]] = api.subscription
  def unsubscribe: F[Unit] = api.unsubscribe
  def wakeup: F[Unit] = api.wakeup
}
