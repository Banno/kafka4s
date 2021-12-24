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

import cats.syntax.all._
import cats.effect.Sync
import java.util.regex.Pattern

import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._

case class ConsumerImpl[F[_], K, V](c: Consumer[K, V])(implicit F: Sync[F])
    extends ConsumerApi[F, K, V] {
  private[this] val log = Slf4jLogger.getLoggerFromClass(this.getClass)
  override def assign(partitions: Iterable[TopicPartition]): F[Unit] =
    F.delay(c.assign(partitions.asJavaCollection)) *> log.debug(
      s"Assigned $partitions"
    )
  override def assignment: F[Set[TopicPartition]] =
    F.delay(c.assignment().asScala.toSet)

  override def close: F[Unit] =
    log.debug(s"${Thread.currentThread.getId} consumer.close()...") *> F.delay(
      c.close()
    ) *> log
      .debug(s"${Thread.currentThread.getId} consumer.close()")
  override def close(timeout: FiniteDuration): F[Unit] =
    log.debug(s"${Thread.currentThread.getId} consumer.close($timeout)...") *> F
      .delay(
        c.close(java.time.Duration.ofMillis(timeout.toMillis))
      ) *> log.debug(s"${Thread.currentThread.getId} consumer.close($timeout)")
  override def commitAsync: F[Unit] = F.delay(c.commitAsync())
  override def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback,
  ): F[Unit] = F.delay(c.commitAsync(offsets.asJava, callback))
  override def commitAsync(callback: OffsetCommitCallback): F[Unit] =
    F.delay(c.commitAsync(callback))
  override def commitSync: F[Unit] = F.delay(c.commitSync())
  override def commitSync(
      offsets: Map[TopicPartition, OffsetAndMetadata]
  ): F[Unit] =
    F.delay(c.commitSync(offsets.asJava))

  override def listTopics: F[Map[String, Seq[PartitionInfo]]] =
    F.delay(c.listTopics().asScala.toMap.view.mapValues(_.asScala.toSeq).toMap)
  override def listTopics(
      timeout: FiniteDuration
  ): F[Map[String, Seq[PartitionInfo]]] =
    F.delay(
      c.listTopics(java.time.Duration.ofMillis(timeout.toMillis))
        .asScala
        .toMap
        .view
        .mapValues(_.asScala.toSeq)
        .toMap
    )
  override def metrics: F[Map[MetricName, Metric]] =
    F.delay(c.metrics().asScala.toMap)

  override def pause(partitions: Iterable[TopicPartition]): F[Unit] =
    F.delay(c.pause(partitions.asJavaCollection))
  override def paused: F[Set[TopicPartition]] =
    F.delay(c.paused().asScala.toSet)
  override def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
    log.trace(s"${Thread.currentThread.getId} poll($timeout)...") *> F.delay(
      c.poll(java.time.Duration.ofMillis(timeout.toMillis))
    )
  override def position(partition: TopicPartition): F[Long] =
    F.delay(c.position(partition))
  override def resume(partitions: Iterable[TopicPartition]): F[Unit] =
    F.delay(c.resume(partitions.asJavaCollection))
  override def seek(partition: TopicPartition, offset: Long): F[Unit] =
    F.delay(c.seek(partition, offset)) *> log.debug(
      s"Seeked $partition to $offset"
    )
  override def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
    F.delay(c.seekToBeginning(partitions.asJavaCollection)) *> log.debug(
      s"Seeked to beginning: $partitions"
    )
  override def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
    F.delay(c.seekToEnd(partitions.asJavaCollection)) *> log.debug(
      s"Seeked to end: $partitions"
    )
  override def subscribe(topics: Iterable[String]): F[Unit] =
    F.delay(c.subscribe(topics.asJavaCollection))
  override def subscribe(
      topics: Iterable[String],
      callback: ConsumerRebalanceListener,
  ): F[Unit] =
    F.delay(c.subscribe(topics.asJavaCollection, callback))
  override def subscribe(pattern: Pattern): F[Unit] =
    F.delay(c.subscribe(pattern))
  override def subscribe(
      pattern: Pattern,
      callback: ConsumerRebalanceListener,
  ): F[Unit] =
    F.delay(c.subscribe(pattern, callback))
  override def subscription: F[Set[String]] =
    F.delay(c.subscription().asScala.toSet)
  override def unsubscribe: F[Unit] = F.delay(c.unsubscribe())

  override def partitionQueries: PartitionQueries[F] = PartitionQueries(c)
}

object ConsumerImpl {
  // returns the type expected when creating a Resource
  def create[F[_]: Sync, K, V](
      c: Consumer[K, V]
  ): ConsumerApi[F, K, V] =
    ConsumerImpl(c)
}
