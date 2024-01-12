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

import cats.effect.*
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.*
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import java.time.{Duration => JDuration}

case class ConsumerImpl[F[_], K, V](c: Consumer[K, V])(implicit F: Sync[F])
    extends ConsumerApi[F, K, V] {

  override def assign(partitions: Iterable[TopicPartition]): F[Unit] =
    F.interruptible(c.assign(partitions.asJavaCollection))

  override def assignment: F[Set[TopicPartition]] =
    F.delay(c.assignment().asScala.toSet)

  override def close: F[Unit] =
    F.interruptible(c.close())

  override def close(timeout: FiniteDuration): F[Unit] =
    F.interruptible(c.close(JDuration.ofMillis(timeout.toMillis)))

  override def commitAsync: F[Unit] =
    F.interruptible(c.commitAsync())

  override def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback,
  ): F[Unit] =
    F.interruptible(c.commitAsync(offsets.asJava, callback))

  override def commitAsync(callback: OffsetCommitCallback): F[Unit] =
    F.interruptible(c.commitAsync(callback))

  override def commitSync: F[Unit] =
    F.interruptible(c.commitSync())

  override def commitSync(
      offsets: Map[TopicPartition, OffsetAndMetadata]
  ): F[Unit] =
    F.interruptible(c.commitSync(offsets.asJava))

  override def listTopics: F[Map[String, Seq[PartitionInfo]]] =
    F.interruptible(
      c.listTopics().asScala.toMap.view.mapValues(_.asScala.toSeq).toMap
    )

  override def listTopics(
      timeout: FiniteDuration
  ): F[Map[String, Seq[PartitionInfo]]] =
    F.interruptible(
      c.listTopics(JDuration.ofMillis(timeout.toMillis))
        .asScala
        .toMap
        .view
        .mapValues(_.asScala.toSeq)
        .toMap
    )

  override def metrics: F[Map[MetricName, Metric]] =
    F.delay(c.metrics().asScala.toMap)

  override def pause(partitions: Iterable[TopicPartition]): F[Unit] =
    F.interruptible(c.pause(partitions.asJavaCollection))

  override def paused: F[Set[TopicPartition]] =
    F.delay(c.paused().asScala.toSet)

  override def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
    F.interruptible(
      c.poll(JDuration.ofMillis(timeout.toMillis))
    )

  override def position(partition: TopicPartition): F[Long] =
    F.interruptible(c.position(partition))

  override def resume(partitions: Iterable[TopicPartition]): F[Unit] =
    F.interruptible(c.resume(partitions.asJavaCollection))

  override def seek(partition: TopicPartition, offset: Long): F[Unit] =
    F.interruptible(c.seek(partition, offset))

  override def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
    F.interruptible(c.seekToBeginning(partitions.asJavaCollection))

  override def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
    F.interruptible(c.seekToEnd(partitions.asJavaCollection))

  override def subscribe(topics: Iterable[String]): F[Unit] =
    F.interruptible(c.subscribe(topics.asJavaCollection))

  override def subscribe(
      topics: Iterable[String],
      callback: ConsumerRebalanceListener,
  ): F[Unit] =
    F.interruptible(c.subscribe(topics.asJavaCollection, callback))

  override def subscribe(pattern: Pattern): F[Unit] =
    F.interruptible(c.subscribe(pattern))

  override def subscribe(
      pattern: Pattern,
      callback: ConsumerRebalanceListener,
  ): F[Unit] =
    F.interruptible(c.subscribe(pattern, callback))

  override def subscription: F[Set[String]] =
    F.delay(c.subscription().asScala.toSet)

  override def unsubscribe: F[Unit] =
    F.interruptible(c.unsubscribe())

  override def partitionQueries: PartitionQueries[F] =
    PartitionQueries(c)
}

object ConsumerImpl {
  // returns the type expected when creating a Resource
  def create[F[_]: Sync, K, V](
      c: Consumer[K, V]
  ): ConsumerApi[F, K, V] =
    ConsumerImpl(c)
}
