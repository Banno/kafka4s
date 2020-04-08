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
import cats.effect._
import cats.effect.concurrent._
import java.util.regex.Pattern

import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._


object ConsumerImpl {
  private[this] final class ConsumerImpl[F[_], K, V](c: Consumer[K, V])(implicit F: Sync[F])
      extends ConsumerApi[F, K, V] {

    private[this] val log = Slf4jLogger.getLoggerFromClass(this.getClass)
    def assign(partitions: Iterable[TopicPartition]): F[Unit] =
      F.delay(c.assign(partitions.asJavaCollection)) *> log.debug(s"Assigned $partitions")
    def assignment: F[Set[TopicPartition]] = F.delay(c.assignment().asScala.toSet)
    def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
      F.delay(c.beginningOffsets(partitions.asJavaCollection).asScala.toMap.mapValues(Long.unbox))
    def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
    ): F[Map[TopicPartition, Long]] =
      F.delay(
        c.beginningOffsets(
          partitions.asJavaCollection,
          java.time.Duration.ofMillis(timeout.toMillis)
        )
          .asScala
          .toMap
          .mapValues(Long.unbox)
      )
    def close: F[Unit] =
      log.debug(s"${Thread.currentThread.getId} consumer.close()...") *> F.delay(c.close()) *> log
        .debug(s"${Thread.currentThread.getId} consumer.close()")
    def close(timeout: FiniteDuration): F[Unit] =
      log.debug(s"${Thread.currentThread.getId} consumer.close($timeout)...") *> F.delay(
        c.close(java.time.Duration.ofMillis(timeout.toMillis))
      ) *> log.debug(s"${Thread.currentThread.getId} consumer.close($timeout)")
    def commitAsync: F[Unit] = F.delay(c.commitAsync())
    def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
    ): F[Unit] = F.delay(c.commitAsync(offsets.asJava, callback))
    def commitAsync(callback: OffsetCommitCallback): F[Unit] = F.delay(c.commitAsync(callback))
    def commitSync: F[Unit] = F.delay(c.commitSync())
    def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      F.delay(c.commitSync(offsets.asJava))
    def committed(
      partitions: Set[TopicPartition]
    ): F[Map[TopicPartition, OffsetAndMetadata]] =
      F.delay(
        c.committed(partitions.asJava)
          .asScala
          .filter(valueIsNotNull)
          .toMap
      )
    def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
      F.delay(c.endOffsets(partitions.asJavaCollection).asScala.toMap.mapValues(Long.unbox))
    def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
    ): F[Map[TopicPartition, Long]] =
      F.delay(
        c.endOffsets(partitions.asJavaCollection, java.time.Duration.ofMillis(timeout.toMillis))
          .asScala
          .toMap
          .mapValues(Long.unbox)
      )
    def listTopics: F[Map[String, Seq[PartitionInfo]]] =
      F.delay(c.listTopics().asScala.toMap.mapValues(_.asScala))
    def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
      F.delay(
        c.listTopics(java.time.Duration.ofMillis(timeout.toMillis))
          .asScala
          .toMap
          .mapValues(_.asScala)
      )
    def metrics: F[Map[MetricName, Metric]] = F.delay(c.metrics().asScala.toMap)
    def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
    ): F[Map[TopicPartition, OffsetAndTimestamp]] =
      F.delay(
        c.offsetsForTimes(timestampsToSearch.mapValues(Long.box).asJava)
          .asScala
          .filter(valueIsNotNull)
          .toMap
      )
    def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
    ): F[Map[TopicPartition, OffsetAndTimestamp]] =
      F.delay(
        c.offsetsForTimes(
          timestampsToSearch.mapValues(Long.box).asJava,
          java.time.Duration.ofMillis(timeout.toMillis)
        )
          .asScala
          .filter(valueIsNotNull)
          .toMap
      )
    def partitionsFor(topic: String): F[Seq[PartitionInfo]] = F.delay(c.partitionsFor(topic).asScala)
    def partitionsFor(topic: String, timeout: FiniteDuration): F[Seq[PartitionInfo]] =
      F.delay(c.partitionsFor(topic, java.time.Duration.ofMillis(timeout.toMillis)).asScala)
    def pause(partitions: Iterable[TopicPartition]): F[Unit] =
      F.delay(c.pause(partitions.asJavaCollection))
    def paused: F[Set[TopicPartition]] = F.delay(c.paused().asScala.toSet)
    def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
      log.trace(s"${Thread.currentThread.getId} poll($timeout)...") *> F.delay(
        c.poll(java.time.Duration.ofMillis(timeout.toMillis))
      )
    def position(partition: TopicPartition): F[Long] = F.delay(c.position(partition))
    def resume(partitions: Iterable[TopicPartition]): F[Unit] =
      F.delay(c.resume(partitions.asJavaCollection))
    def seek(partition: TopicPartition, offset: Long): F[Unit] =
      F.delay(c.seek(partition, offset)) *> log.debug(s"Seeked $partition to $offset")
    def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
      F.delay(c.seekToBeginning(partitions.asJavaCollection)) *> log.debug(
        s"Seeked to beginning: $partitions"
      )
    def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
      F.delay(c.seekToEnd(partitions.asJavaCollection)) *> log.debug(s"Seeked to end: $partitions")
    def subscribe(topics: Iterable[String]): F[Unit] = F.delay(c.subscribe(topics.asJavaCollection))
    def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
      F.delay(c.subscribe(topics.asJavaCollection, callback))
    def subscribe(pattern: Pattern): F[Unit] = F.delay(c.subscribe(pattern))
    def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
      F.delay(c.subscribe(pattern, callback))
    def subscription: F[Set[String]] = F.delay(c.subscription().asScala.toSet)
    def unsubscribe: F[Unit] = F.delay(c.unsubscribe())
    def wakeup: F[Unit] = F.delay(c.wakeup())
  }

  private[this] final class SynchronizedConsumerImpl[F[_], K, V](consumerImpl: ConsumerImpl[F, K, V], semaphore: Semaphore[F]) extends ConsumerApi[F, K, V] {

    override final def assign(partitions: Iterable[TopicPartition]): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.assign(partitions))
    override final def assignment: F[Set[TopicPartition]] =
      this.semaphore.withPermit(this.consumerImpl.assignment)
    def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
      this.semaphore.withPermit(this.consumerImpl.beginningOffsets(partitions))
    def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
    ): F[Map[TopicPartition, Long]] =
      this.semaphore.withPermit(this.consumerImpl.beginningOffsets(partitions, timeout))
    def close: F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.close)
    def close(timeout: FiniteDuration): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.close(timeout))
    def commitAsync: F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.commitAsync)
    def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
    ): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.commitAsync(offsets, callback))
    def commitAsync(callback: OffsetCommitCallback): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.commitAsync(callback))
    def commitSync: F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.commitSync)
    def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.commitSync(offsets))
    def committed(partition: Set[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]] =
      this.semaphore.withPermit(this.consumerImpl.committed(partition))
    def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]] =
      this.semaphore.withPermit(this.consumerImpl.endOffsets(partitions))
    def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
    ): F[Map[TopicPartition, Long]] =
      this.semaphore.withPermit(this.consumerImpl.endOffsets(partitions, timeout))
    def listTopics: F[Map[String, Seq[PartitionInfo]]] =
      this.semaphore.withPermit(this.consumerImpl.listTopics)
    def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
      this.semaphore.withPermit(this.consumerImpl.listTopics(timeout))
    def metrics: F[Map[MetricName, Metric]] =
      this.semaphore.withPermit(this.consumerImpl.metrics)
    def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
    ): F[Map[TopicPartition, OffsetAndTimestamp]] =
      this.semaphore.withPermit(this.consumerImpl.offsetsForTimes(timestampsToSearch))
    def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
    ): F[Map[TopicPartition, OffsetAndTimestamp]] =
      this.semaphore.withPermit(this.consumerImpl.offsetsForTimes(timestampsToSearch, timeout))
    def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
      this.semaphore.withPermit(this.consumerImpl.partitionsFor(topic))
    def partitionsFor(topic: String, timeout: FiniteDuration): F[Seq[PartitionInfo]] =
      this.semaphore.withPermit(this.consumerImpl.partitionsFor(topic, timeout))
    def pause(partitions: Iterable[TopicPartition]): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.pause(partitions))
    def paused: F[Set[TopicPartition]] =
      this.semaphore.withPermit(this.consumerImpl.paused)
    def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
      this.semaphore.withPermit(this.consumerImpl.poll(timeout))
    def position(partition: TopicPartition): F[Long] =
      this.semaphore.withPermit(this.consumerImpl.position(partition))
    def resume(partitions: Iterable[TopicPartition]): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.resume(partitions))
    def seek(partition: TopicPartition, offset: Long): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.seek(partition, offset))
    def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.seekToBeginning(partitions))
    def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.seekToEnd(partitions))
    def subscribe(topics: Iterable[String]): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.subscribe(topics))
    def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.subscribe(topics, callback))
    def subscribe(pattern: Pattern): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.subscribe(pattern))
    def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.subscribe(pattern, callback))
    def subscription: F[Set[String]] =
      this.semaphore.withPermit(this.consumerImpl.subscription)
    def unsubscribe: F[Unit] =
      this.semaphore.withPermit(this.consumerImpl.unsubscribe)
    def wakeup: F[Unit] =
      // Thread safe
      this.consumerImpl.wakeup
  }

  //returns the type expected when creating a Resource
  def create[F[_]: Concurrent, K, V](
      c: Consumer[K, V]
  ): F[ConsumerApi[F, K, V]] =
    Semaphore(1L).map(s =>
      new SynchronizedConsumerImpl(new ConsumerImpl(c), s)
    )

  def valueIsNotNull[A](keyValPair: (TopicPartition, A)): Boolean =
    keyValPair._2 != null

}
