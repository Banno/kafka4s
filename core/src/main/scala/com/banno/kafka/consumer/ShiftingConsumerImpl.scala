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

import scala.concurrent.ExecutionContext

case class ShiftingConsumerImpl[F[_]: Async, K, V](
    c: ConsumerApi[F, K, V],
    // TODO use Sync[F].blocking instead of passing explicit EC?
    blockingContext: ExecutionContext
) extends ConsumerApi[F, K, V] {
  def assign(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.assign(partitions), blockingContext)
  def assignment: F[Set[TopicPartition]] = Async[F].evalOn(c.assignment, blockingContext)
  def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    Async[F].evalOn(c.beginningOffsets(partitions), blockingContext)
  def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]] =
    Async[F].evalOn(c.beginningOffsets(partitions, timeout), blockingContext)
  def close: F[Unit] = Async[F].evalOn(c.close, blockingContext)
  def close(timeout: FiniteDuration): F[Unit] = Async[F].evalOn(c.close(timeout), blockingContext)
  def commitAsync: F[Unit] = Async[F].evalOn(c.commitAsync, blockingContext)
  def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): F[Unit] =
    Async[F].evalOn(c.commitAsync(offsets, callback), blockingContext)
  def commitAsync(callback: OffsetCommitCallback): F[Unit] =
    Async[F].evalOn(c.commitAsync(callback), blockingContext)
  def commitSync: F[Unit] = Async[F].evalOn(c.commitSync, blockingContext)
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    Async[F].evalOn(c.commitSync(offsets), blockingContext)
  def committed(partition: Set[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]] =
    Async[F].evalOn(c.committed(partition), blockingContext)
  def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    Async[F].evalOn(c.endOffsets(partitions), blockingContext)
  def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]] =
    Async[F].evalOn(c.endOffsets(partitions, timeout), blockingContext)
  def listTopics: F[Map[String, Seq[PartitionInfo]]] = Async[F].evalOn(c.listTopics, blockingContext)
  def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
    Async[F].evalOn(c.listTopics(timeout), blockingContext)
  def metrics: F[Map[MetricName, Metric]] = Async[F].evalOn(c.metrics, blockingContext)
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndTimestamp]] =
    Async[F].evalOn(c.offsetsForTimes(timestampsToSearch), blockingContext)
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, OffsetAndTimestamp]] =
    Async[F].evalOn(c.offsetsForTimes(timestampsToSearch, timeout), blockingContext)
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    Async[F].evalOn(c.partitionsFor(topic), blockingContext)
  def partitionsFor(topic: String, timeout: FiniteDuration): F[Seq[PartitionInfo]] =
    Async[F].evalOn(c.partitionsFor(topic, timeout), blockingContext)
  def pause(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.pause(partitions), blockingContext)
  def paused: F[Set[TopicPartition]] = Async[F].evalOn(c.paused, blockingContext)
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
    Async[F].evalOn(c.poll(timeout), blockingContext)
  def position(partition: TopicPartition): F[Long] =
    Async[F].evalOn(c.position(partition), blockingContext)
  def resume(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.resume(partitions), blockingContext)
  def seek(partition: TopicPartition, offset: Long): F[Unit] =
    Async[F].evalOn(c.seek(partition, offset), blockingContext)
  def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.seekToBeginning(partitions), blockingContext)
  def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.seekToEnd(partitions), blockingContext)
  def subscribe(topics: Iterable[String]): F[Unit] = Async[F].evalOn(c.subscribe(topics), blockingContext)
  def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
    Async[F].evalOn(c.subscribe(topics, callback), blockingContext)
  def subscribe(pattern: Pattern): F[Unit] = Async[F].evalOn(c.subscribe(pattern), blockingContext)
  def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
    Async[F].evalOn(c.subscribe(pattern, callback), blockingContext)
  def subscription: F[Set[String]] = Async[F].evalOn(c.subscription, blockingContext)
  def unsubscribe: F[Unit] = Async[F].evalOn(c.unsubscribe, blockingContext)
  def wakeup: F[Unit] = c.wakeup //TODO wakeup is the one method that is thread-safe, right?
}

object ShiftingConsumerImpl {
  //returns the type expected when creating a Resource
  def create[F[_]: Async, K, V](
      c: ConsumerApi[F, K, V],
      e: ExecutionContext
  ): ConsumerApi[F, K, V] =
    ShiftingConsumerImpl(c, e)
}
