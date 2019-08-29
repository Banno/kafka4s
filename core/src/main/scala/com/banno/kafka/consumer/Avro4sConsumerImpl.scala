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

import cats.implicits._
import java.util.regex.Pattern
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._
import org.apache.avro.generic.GenericRecord
import com.sksamuel.avro4s.FromRecord
import cats.Functor
import com.banno.kafka._

//this is a Bifunctor[ConsumerApi]

case class Avro4sConsumerImpl[F[_]: Functor, K: FromRecord, V: FromRecord](
    c: ConsumerApi[F, GenericRecord, GenericRecord]
) extends ConsumerApi[F, K, V] {
  def assign(partitions: Iterable[TopicPartition]): F[Unit] = c.assign(partitions)
  def assignment: F[Set[TopicPartition]] = c.assignment
  def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    c.beginningOffsets(partitions)
  def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]] =
    c.beginningOffsets(partitions, timeout)
  def close: F[Unit] = c.close
  def close(timeout: FiniteDuration): F[Unit] = c.close(timeout)
  def commitAsync: F[Unit] = c.commitAsync
  def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): F[Unit] = c.commitAsync(offsets, callback)
  def commitAsync(callback: OffsetCommitCallback): F[Unit] = c.commitAsync(callback)
  def commitSync: F[Unit] = c.commitSync
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] = c.commitSync(offsets)
  def committed(partition: TopicPartition): F[Option[OffsetAndMetadata]] = c.committed(partition)
  def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
    c.endOffsets(partitions)
  def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]] = c.endOffsets(partitions, timeout)
  def listTopics: F[Map[String, Seq[PartitionInfo]]] = c.listTopics
  def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
    c.listTopics(timeout)
  def metrics: F[Map[MetricName, Metric]] = c.metrics
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndTimestamp]] =
    c.offsetsForTimes(timestampsToSearch)
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, OffsetAndTimestamp]] =
    c.offsetsForTimes(timestampsToSearch, timeout)
  def partitionsFor(topic: String): F[Seq[PartitionInfo]] = c.partitionsFor(topic)
  def partitionsFor(topic: String, timeout: FiniteDuration): F[Seq[PartitionInfo]] =
    c.partitionsFor(topic, timeout)
  def pause(partitions: Iterable[TopicPartition]): F[Unit] = c.pause(partitions)
  def paused: F[Set[TopicPartition]] = c.paused
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
    c.poll(timeout).map(_.fromGenericRecords[K, V])
  def position(partition: TopicPartition): F[Long] = c.position(partition)
  def resume(partitions: Iterable[TopicPartition]): F[Unit] = c.resume(partitions)
  def seek(partition: TopicPartition, offset: Long): F[Unit] = c.seek(partition, offset)
  def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] = c.seekToBeginning(partitions)
  def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] = c.seekToEnd(partitions)
  def subscribe(topics: Iterable[String]): F[Unit] = c.subscribe(topics)
  def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
    c.subscribe(topics, callback)
  def subscribe(pattern: Pattern): F[Unit] = c.subscribe(pattern)
  def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
    c.subscribe(pattern, callback)
  def subscription: F[Set[String]] = c.subscription
  def unsubscribe: F[Unit] = c.unsubscribe
  def wakeup: F[Unit] = c.wakeup
}
