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

import cats.effect.{Async, ContextShift}
import java.util.concurrent.Executors
import java.util.regex.Pattern

import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._

import scala.concurrent.ExecutionContext

case class ShiftingConsumerImpl[F[_]: Async, K, V](c: ConsumerApi[F, K, V])(
    implicit CS: ContextShift[F])
    extends ConsumerApi[F, K, V] {
  val consumerContext: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1))

  def assign(partitions: Iterable[TopicPartition]): F[Unit] =
    CS.evalOn(consumerContext)(c.assign(partitions))
  def assignment: F[Set[TopicPartition]] = CS.evalOn(consumerContext)(c.assignment)
  def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    CS.evalOn(consumerContext)(c.beginningOffsets(partitions))
  def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration): F[Map[TopicPartition, Long]] =
    CS.evalOn(consumerContext)(c.beginningOffsets(partitions, timeout))
  def close: F[Unit] = CS.evalOn(consumerContext)(c.close)
  def close(timeout: FiniteDuration): F[Unit] = CS.evalOn(consumerContext)(c.close(timeout))
  def commitAsync: F[Unit] = CS.evalOn(consumerContext)(c.commitAsync)
  def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback): F[Unit] =
    CS.evalOn(consumerContext)(c.commitAsync(offsets, callback))
  def commitAsync(callback: OffsetCommitCallback): F[Unit] =
    CS.evalOn(consumerContext)(c.commitAsync(callback))
  def commitSync: F[Unit] = CS.evalOn(consumerContext)(c.commitSync)
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    CS.evalOn(consumerContext)(c.commitSync(offsets))
  def committed(partition: TopicPartition): F[OffsetAndMetadata] =
    CS.evalOn(consumerContext)(c.committed(partition))
  def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    CS.evalOn(consumerContext)(c.endOffsets(partitions))
  def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration): F[Map[TopicPartition, Long]] =
    CS.evalOn(consumerContext)(c.endOffsets(partitions, timeout))
  def listTopics: F[Map[String, Seq[PartitionInfo]]] = CS.evalOn(consumerContext)(c.listTopics)
  def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
    CS.evalOn(consumerContext)(c.listTopics(timeout))
  def metrics: F[Map[MetricName, Metric]] = CS.evalOn(consumerContext)(c.metrics)
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]): F[Map[TopicPartition, OffsetAndTimestamp]] =
    CS.evalOn(consumerContext)(c.offsetsForTimes(timestampsToSearch))
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration): F[Map[TopicPartition, OffsetAndTimestamp]] =
    CS.evalOn(consumerContext)(c.offsetsForTimes(timestampsToSearch, timeout))
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
    CS.evalOn(consumerContext)(c.partitionsFor(topic))
  def partitionsFor(topic: String, timeout: FiniteDuration): F[Seq[PartitionInfo]] =
    CS.evalOn(consumerContext)(c.partitionsFor(topic, timeout))
  def pause(partitions: Iterable[TopicPartition]): F[Unit] =
    CS.evalOn(consumerContext)(c.pause(partitions))
  def paused: F[Set[TopicPartition]] = CS.evalOn(consumerContext)(c.paused)
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
    CS.evalOn(consumerContext)(c.poll(timeout))
  def position(partition: TopicPartition): F[Long] =
    CS.evalOn(consumerContext)(c.position(partition))
  def resume(partitions: Iterable[TopicPartition]): F[Unit] =
    CS.evalOn(consumerContext)(c.resume(partitions))
  def seek(partition: TopicPartition, offset: Long): F[Unit] =
    CS.evalOn(consumerContext)(c.seek(partition, offset))
  def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
    CS.evalOn(consumerContext)(c.seekToBeginning(partitions))
  def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
    CS.evalOn(consumerContext)(c.seekToEnd(partitions))
  def subscribe(topics: Iterable[String]): F[Unit] = CS.evalOn(consumerContext)(c.subscribe(topics))
  def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
    CS.evalOn(consumerContext)(c.subscribe(topics, callback))
  def subscribe(pattern: Pattern): F[Unit] = CS.evalOn(consumerContext)(c.subscribe(pattern))
  def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
    CS.evalOn(consumerContext)(c.subscribe(pattern, callback))
  def subscription: F[Set[String]] = CS.evalOn(consumerContext)(c.subscription)
  def unsubscribe: F[Unit] = CS.evalOn(consumerContext)(c.unsubscribe)
  def wakeup: F[Unit] = CS.evalOn(consumerContext)(c.wakeup)
}
