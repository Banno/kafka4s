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
  override def assign(partitions: Iterable[TopicPartition]): F[Unit] = c.assign(partitions)
  override def assignment: F[Set[TopicPartition]] = c.assignment

  override def close: F[Unit] = c.close
  override def close(timeout: FiniteDuration): F[Unit] = c.close(timeout)
  override def commitAsync: F[Unit] = c.commitAsync
  override def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): F[Unit] = c.commitAsync(offsets, callback)
  override def commitAsync(callback: OffsetCommitCallback): F[Unit] = c.commitAsync(callback)
  override def commitSync: F[Unit] = c.commitSync
  override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] = c.commitSync(offsets)

  override def listTopics: F[Map[String, Seq[PartitionInfo]]] = c.listTopics
  override def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
    c.listTopics(timeout)
  override def metrics: F[Map[MetricName, Metric]] = c.metrics

  override def pause(partitions: Iterable[TopicPartition]): F[Unit] = c.pause(partitions)
  override def paused: F[Set[TopicPartition]] = c.paused
  override def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]] =
    c.poll(timeout).map(_.fromGenericRecords[K, V])
  override def position(partition: TopicPartition): F[Long] = c.position(partition)
  override def resume(partitions: Iterable[TopicPartition]): F[Unit] = c.resume(partitions)
  override def seek(partition: TopicPartition, offset: Long): F[Unit] = c.seek(partition, offset)
  override def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] = c.seekToBeginning(partitions)
  override def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] = c.seekToEnd(partitions)
  override def subscribe(topics: Iterable[String]): F[Unit] = c.subscribe(topics)
  override def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
    c.subscribe(topics, callback)
  override def subscribe(pattern: Pattern): F[Unit] = c.subscribe(pattern)
  override def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
    c.subscribe(pattern, callback)
  override def subscription: F[Set[String]] = c.subscription
  override def unsubscribe: F[Unit] = c.unsubscribe

  override def partitionQueries: PartitionQueries[F] = c.partitionQueries
}
