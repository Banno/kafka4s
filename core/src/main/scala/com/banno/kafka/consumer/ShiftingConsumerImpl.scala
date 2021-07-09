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

import cats.arrow._
import cats.effect._
import java.util.regex.Pattern

import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.clients.consumer._

import scala.concurrent.ExecutionContext

case class ShiftingConsumerImpl[F[_]: Async, K, V](
    c: ConsumerApi[F, K, V],
    blockingContext: ExecutionContext
) extends ConsumerApi[F, K, V] {
  override def assign(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.assign(partitions), blockingContext)
  override def assignment: F[Set[TopicPartition]] = Async[F].evalOn(c.assignment, blockingContext)

  override def close: F[Unit] = Async[F].evalOn(c.close, blockingContext)
  override def close(timeout: FiniteDuration): F[Unit] = Async[F].evalOn(c.close(timeout), blockingContext)
  override def commitAsync: F[Unit] = Async[F].evalOn(c.commitAsync, blockingContext)
  override def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): F[Unit] =
    Async[F].evalOn(c.commitAsync(offsets, callback), blockingContext)
  override def commitAsync(callback: OffsetCommitCallback): F[Unit] =
    Async[F].evalOn(c.commitAsync(callback), blockingContext)
  override def commitSync: F[Unit] = Async[F].evalOn(c.commitSync, blockingContext)
  override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
    Async[F].evalOn(c.commitSync(offsets), blockingContext)

  override def listTopics: F[Map[String, Seq[PartitionInfo]]] = Async[F].evalOn(c.listTopics, blockingContext)
  override def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
    Async[F].evalOn(c.listTopics(timeout), blockingContext)
  override def metrics: F[Map[MetricName, Metric]] = Async[F].evalOn(c.metrics, blockingContext)

  override def pause(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.pause(partitions), blockingContext)
  override def paused: F[Set[TopicPartition]] = Async[F].evalOn(c.paused, blockingContext)
  override def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
    Async[F].evalOn(c.poll(timeout), blockingContext)
  override def position(partition: TopicPartition): F[Long] =
    Async[F].evalOn(c.position(partition), blockingContext)
  override def resume(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.resume(partitions), blockingContext)
  override def seek(partition: TopicPartition, offset: Long): F[Unit] =
    Async[F].evalOn(c.seek(partition, offset), blockingContext)
  override def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.seekToBeginning(partitions), blockingContext)
  override def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
    Async[F].evalOn(c.seekToEnd(partitions), blockingContext)
  override def subscribe(topics: Iterable[String]): F[Unit] = Async[F].evalOn(c.subscribe(topics), blockingContext)
  override def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
    Async[F].evalOn(c.subscribe(topics, callback), blockingContext)
  override def subscribe(pattern: Pattern): F[Unit] = Async[F].evalOn(c.subscribe(pattern), blockingContext)
  override def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
    Async[F].evalOn(c.subscribe(pattern, callback), blockingContext)
  override def subscription: F[Set[String]] = Async[F].evalOn(c.subscription, blockingContext)
  override def unsubscribe: F[Unit] = Async[F].evalOn(c.unsubscribe, blockingContext)

  override def partitionQueries: PartitionQueries[F] =
    c.partitionQueries.mapK(
      FunctionK.liftFunction(
        Async[F].evalOn(_, blockingContext)
      )
    )
}

object ShiftingConsumerImpl {
  //returns the type expected when creating a Resource
  def create[F[_]: Async, K, V](
      c: ConsumerApi[F, K, V],
      e: ExecutionContext
  ): ConsumerApi[F, K, V] =
    ShiftingConsumerImpl(c, e)
}
