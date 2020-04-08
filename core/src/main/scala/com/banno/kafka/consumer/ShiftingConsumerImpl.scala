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

import cats.effect._
import java.util.regex.Pattern

import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._

case class ShiftingConsumerImpl[F[_]: Async, K, V](
    c: ConsumerApi[F, K, V],
    blockingContext: Blocker
)(implicit CS: ContextShift[F])
    extends ConsumerApi[F, K, V] {
  def assign(partitions: Iterable[TopicPartition]): F[Unit] =
    this.blockingContext.blockOn(c.assign(partitions))
  def assignment: F[Set[TopicPartition]] = this.blockingContext.blockOn(c.assignment)
  def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    this.blockingContext.blockOn(c.beginningOffsets(partitions))
  def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]] =
    this.blockingContext.blockOn(c.beginningOffsets(partitions, timeout))
  def close: F[Unit] = this.blockingContext.blockOn(c.close)
  def close(timeout: FiniteDuration): F[Unit] = this.blockingContext.blockOn(c.close(timeout))
  def commitAsync: F[Unit] = this.blockingContext.blockOn(c.commitAsync)
  def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): F[Unit] =
    this.blockingContext.blockOn(c.commitAsync(offsets, callback))
  def commitAsync(callback: OffsetCommitCallback): F[Unit] =
    this.blockingContext.blockOn(c.commitAsync(callback))
  def commitSync: F[Unit] = this.blockingContext.blockOn(c.commitSync)
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    this.blockingContext.blockOn(c.commitSync(offsets))
  def committed(partition: Set[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]] =
    this.blockingContext.blockOn(c.committed(partition))
  def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    this.blockingContext.blockOn(c.endOffsets(partitions))
  def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]] =
    this.blockingContext.blockOn(c.endOffsets(partitions, timeout))
  def listTopics: F[Map[String, Seq[PartitionInfo]]] = this.blockingContext.blockOn(c.listTopics)
  def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
    this.blockingContext.blockOn(c.listTopics(timeout))
  def metrics: F[Map[MetricName, Metric]] = this.blockingContext.blockOn(c.metrics)
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndTimestamp]] =
    this.blockingContext.blockOn(c.offsetsForTimes(timestampsToSearch))
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, OffsetAndTimestamp]] =
    this.blockingContext.blockOn(c.offsetsForTimes(timestampsToSearch, timeout))
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    this.blockingContext.blockOn(c.partitionsFor(topic))
  def partitionsFor(topic: String, timeout: FiniteDuration): F[Seq[PartitionInfo]] =
    this.blockingContext.blockOn(c.partitionsFor(topic, timeout))
  def pause(partitions: Iterable[TopicPartition]): F[Unit] =
    this.blockingContext.blockOn(c.pause(partitions))
  def paused: F[Set[TopicPartition]] = this.blockingContext.blockOn(c.paused)
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
    this.blockingContext.blockOn(c.poll(timeout))
  def position(partition: TopicPartition): F[Long] =
    this.blockingContext.blockOn(c.position(partition))
  def resume(partitions: Iterable[TopicPartition]): F[Unit] =
    this.blockingContext.blockOn(c.resume(partitions))
  def seek(partition: TopicPartition, offset: Long): F[Unit] =
    this.blockingContext.blockOn(c.seek(partition, offset))
  def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
    this.blockingContext.blockOn(c.seekToBeginning(partitions))
  def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
    this.blockingContext.blockOn(c.seekToEnd(partitions))
  def subscribe(topics: Iterable[String]): F[Unit] = this.blockingContext.blockOn(c.subscribe(topics))
  def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
    this.blockingContext.blockOn(c.subscribe(topics, callback))
  def subscribe(pattern: Pattern): F[Unit] = this.blockingContext.blockOn(c.subscribe(pattern))
  def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
    this.blockingContext.blockOn(c.subscribe(pattern, callback))
  def subscription: F[Set[String]] = this.blockingContext.blockOn(c.subscription)
  def unsubscribe: F[Unit] = this.blockingContext.blockOn(c.unsubscribe)
  def wakeup: F[Unit] = c.wakeup //TODO wakeup is the one method that is thread-safe, right?
}

object ShiftingConsumerImpl {
  def create[F[_]: Async: ContextShift, K, V](
      c: ConsumerApi[F, K, V],
      b: Blocker
  ): ConsumerApi[F, K, V] =
    ShiftingConsumerImpl(c, b)
}
