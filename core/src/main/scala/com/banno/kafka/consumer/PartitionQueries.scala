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

import cats._
import cats.effect._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._
import scala.jdk.CollectionConverters._

trait PartitionQueries[F[_]] {
  def beginningOffsets(
      partitions: Iterable[TopicPartition]
  ): F[Map[TopicPartition, Long]]
  def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration,
  ): F[Map[TopicPartition, Long]]

  def committed(
      partition: Set[TopicPartition]
  ): F[Map[TopicPartition, OffsetAndMetadata]]

  def endOffsets(
      partitions: Iterable[TopicPartition]
  ): F[Map[TopicPartition, Long]]
  def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration,
  ): F[Map[TopicPartition, Long]]

  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndTimestamp]]
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration,
  ): F[Map[TopicPartition, OffsetAndTimestamp]]

  def partitionsFor(topic: String): F[Seq[PartitionInfo]]
  def partitionsFor(
      topic: String,
      timeout: FiniteDuration,
  ): F[Seq[PartitionInfo]]

  final def mapK[G[_]](f: F ~> G): PartitionQueries[G] = {
    val self = this
    new PartitionQueries[G] {
      override def beginningOffsets(
          partitions: Iterable[TopicPartition]
      ): G[Map[TopicPartition, Long]] =
        f(self.beginningOffsets(partitions))

      override def beginningOffsets(
          partitions: Iterable[TopicPartition],
          timeout: FiniteDuration,
      ): G[Map[TopicPartition, Long]] =
        f(self.beginningOffsets(partitions, timeout))

      override def committed(
          partition: Set[TopicPartition]
      ): G[Map[TopicPartition, OffsetAndMetadata]] =
        f(self.committed(partition))

      override def endOffsets(
          partitions: Iterable[TopicPartition]
      ): G[Map[TopicPartition, Long]] =
        f(self.endOffsets(partitions))
      override def endOffsets(
          partitions: Iterable[TopicPartition],
          timeout: FiniteDuration,
      ): G[Map[TopicPartition, Long]] =
        f(self.endOffsets(partitions, timeout))

      override def offsetsForTimes(
          timestampsToSearch: Map[TopicPartition, Long]
      ): G[Map[TopicPartition, OffsetAndTimestamp]] =
        f(self.offsetsForTimes(timestampsToSearch))
      override def offsetsForTimes(
          timestampsToSearch: Map[TopicPartition, Long],
          timeout: FiniteDuration,
      ): G[Map[TopicPartition, OffsetAndTimestamp]] =
        f(self.offsetsForTimes(timestampsToSearch, timeout))

      override def partitionsFor(topic: String): G[Seq[PartitionInfo]] =
        f(self.partitionsFor(topic))
      override def partitionsFor(
          topic: String,
          timeout: FiniteDuration,
      ): G[Seq[PartitionInfo]] =
        f(self.partitionsFor(topic, timeout))
    }
  }
}

object PartitionQueries {
  def valueIsNotNull[A](keyValPair: (TopicPartition, A)): Boolean =
    keyValPair._2 != null

  def apply[F[_]: Sync](c: Consumer[_, _]): PartitionQueries[F] =
    new PartitionQueries[F] {
      override def beginningOffsets(
          partitions: Iterable[TopicPartition]
      ): F[Map[TopicPartition, Long]] =
        Sync[F].delay(
          c.beginningOffsets(partitions.asJavaCollection)
            .asScala
            .toMap
            .view
            .mapValues(Long.unbox)
            .toMap
        )

      override def beginningOffsets(
          partitions: Iterable[TopicPartition],
          timeout: FiniteDuration,
      ): F[Map[TopicPartition, Long]] =
        Sync[F].delay(
          c.beginningOffsets(
            partitions.asJavaCollection,
            java.time.Duration.ofMillis(timeout.toMillis),
          ).asScala
            .toMap
            .view
            .mapValues(Long.unbox)
            .toMap
        )

      override def committed(
          partitions: Set[TopicPartition]
      ): F[Map[TopicPartition, OffsetAndMetadata]] =
        Sync[F].delay(
          c.committed(partitions.asJava)
            .asScala
            .filter(valueIsNotNull)
            .toMap
        )

      override def endOffsets(
          partitions: Iterable[TopicPartition]
      ): F[Map[TopicPartition, Long]] =
        Sync[F].delay(
          c.endOffsets(partitions.asJavaCollection)
            .asScala
            .toMap
            .view
            .mapValues(Long.unbox)
            .toMap
        )
      override def endOffsets(
          partitions: Iterable[TopicPartition],
          timeout: FiniteDuration,
      ): F[Map[TopicPartition, Long]] =
        Sync[F].delay(
          c.endOffsets(
            partitions.asJavaCollection,
            java.time.Duration.ofMillis(timeout.toMillis),
          ).asScala
            .toMap
            .view
            .mapValues(Long.unbox)
            .toMap
        )

      override def offsetsForTimes(
          timestampsToSearch: Map[TopicPartition, Long]
      ): F[Map[TopicPartition, OffsetAndTimestamp]] =
        Sync[F].delay(
          c.offsetsForTimes(
            timestampsToSearch.view.mapValues(Long.box).toMap.asJava
          ).asScala
            .filter(valueIsNotNull)
            .toMap
        )
      override def offsetsForTimes(
          timestampsToSearch: Map[TopicPartition, Long],
          timeout: FiniteDuration,
      ): F[Map[TopicPartition, OffsetAndTimestamp]] =
        Sync[F].delay(
          c.offsetsForTimes(
            timestampsToSearch.view.mapValues(Long.box).toMap.asJava,
            java.time.Duration.ofMillis(timeout.toMillis),
          ).asScala
            .filter(valueIsNotNull)
            .toMap
        )

      override def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
        Sync[F].delay(c.partitionsFor(topic).asScala.toSeq)
      override def partitionsFor(
          topic: String,
          timeout: FiniteDuration,
      ): F[Seq[PartitionInfo]] =
        Sync[F].delay(
          c.partitionsFor(topic, java.time.Duration.ofMillis(timeout.toMillis))
            .asScala
            .toSeq
        )
    }
}
