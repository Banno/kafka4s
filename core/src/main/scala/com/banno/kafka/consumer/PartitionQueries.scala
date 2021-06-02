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

import scala.concurrent.duration._

import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._

trait PartitionQueries[F[_]] {
  def beginningOffsets(
      partitions: Iterable[TopicPartition]
  ): F[Map[TopicPartition, Long]]
  def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]

  def committed(
      partition: Set[TopicPartition]
  ): F[Map[TopicPartition, OffsetAndMetadata]]

  def endOffsets(
      partitions: Iterable[TopicPartition]
  ): F[Map[TopicPartition, Long]]
  def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]

  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndTimestamp]]
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, OffsetAndTimestamp]]

  def partitionsFor(topic: String): F[Seq[PartitionInfo]]
  def partitionsFor(
      topic: String,
      timeout: FiniteDuration
  ): F[Seq[PartitionInfo]]
}

object PartitionQueries {
  def apply[F[_]](
      consumer: ConsumerApi[F, _, _]
  ): PartitionQueries[F] =
    new PartitionQueries[F] {
      override def beginningOffsets(
          partitions: Iterable[TopicPartition]
      ): F[Map[TopicPartition, Long]] = consumer.beginningOffsets(partitions)
      override def beginningOffsets(
          partitions: Iterable[TopicPartition],
          timeout: FiniteDuration
      ): F[Map[TopicPartition, Long]] = consumer.beginningOffsets(partitions, timeout)

      override def committed(
          partition: Set[TopicPartition]
      ): F[Map[TopicPartition, OffsetAndMetadata]] = consumer.committed(partition)

      override def endOffsets(
          partitions: Iterable[TopicPartition]
      ): F[Map[TopicPartition, Long]] = consumer.endOffsets(partitions)
      override def endOffsets(
          partitions: Iterable[TopicPartition],
          timeout: FiniteDuration
      ): F[Map[TopicPartition, Long]] = consumer.endOffsets(partitions, timeout)

      override def offsetsForTimes(
          timestampsToSearch: Map[TopicPartition, Long]
      ): F[Map[TopicPartition, OffsetAndTimestamp]] =
        consumer.offsetsForTimes(timestampsToSearch)
      override def offsetsForTimes(
          timestampsToSearch: Map[TopicPartition, Long],
          timeout: FiniteDuration
      ): F[Map[TopicPartition, OffsetAndTimestamp]] =
        consumer.offsetsForTimes(timestampsToSearch, timeout)

      override def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
        consumer.partitionsFor(topic)
      override def partitionsFor(
          topic: String,
          timeout: FiniteDuration
      ): F[Seq[PartitionInfo]] =
        consumer.partitionsFor(topic, timeout)
    }
}
