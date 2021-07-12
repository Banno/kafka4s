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

import cats._
import cats.arrow._
import cats.effect._
import cats.syntax.all._
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.clients.consumer._
import com.banno.kafka._
import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

trait ConsumerApi[F[_], K, V] {
  def assign(partitions: Iterable[TopicPartition]): F[Unit]
  def assignment: F[Set[TopicPartition]]

  def close: F[Unit]
  def close(timeout: FiniteDuration): F[Unit]
  def commitAsync: F[Unit]
  def commitAsync(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      callback: OffsetCommitCallback
  ): F[Unit]
  def commitAsync(callback: OffsetCommitCallback): F[Unit]
  def commitSync: F[Unit]
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  def listTopics: F[Map[String, Seq[PartitionInfo]]]
  def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]]
  def metrics: F[Map[MetricName, Metric]]

  def pause(partitions: Iterable[TopicPartition]): F[Unit]
  def paused: F[Set[TopicPartition]]
  def poll(timeout: FiniteDuration): F[ConsumerRecords[K, V]]
  def position(partition: TopicPartition): F[Long]
  def resume(partitions: Iterable[TopicPartition]): F[Unit]
  def seek(partition: TopicPartition, offset: Long): F[Unit]
  def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit]
  def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit]
  def subscribe(topics: Iterable[String]): F[Unit]
  def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit]
  def subscribe(pattern: Pattern): F[Unit]
  def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit]
  def subscription: F[Set[String]]
  def unsubscribe: F[Unit]

  def partitionQueries: PartitionQueries[F]

  final def mapK[G[_]](f: F ~> G): ConsumerApi[G, K, V] = {
    val self = this
    new ConsumerApi[G, K, V] {
      override def assign(partitions: Iterable[TopicPartition]): G[Unit] =
        f(self.assign(partitions))
      override def assignment: G[Set[TopicPartition]] =
        f(self.assignment)

      override def close: G[Unit] =
        f(self.close)
      override def close(timeout: FiniteDuration): G[Unit] =
        f(self.close(timeout))

      override def commitAsync: G[Unit] =
        f(self.commitAsync)
      override def commitAsync(
          offsets: Map[TopicPartition, OffsetAndMetadata],
          callback: OffsetCommitCallback
      ): G[Unit] =
        f(self.commitAsync(offsets, callback))
      override def commitAsync(callback: OffsetCommitCallback): G[Unit] =
        f(self.commitAsync(callback))
      override def commitSync: G[Unit] =
        f(self.commitSync)
      override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): G[Unit] =
        f(self.commitSync(offsets))

      override def listTopics: G[Map[String, Seq[PartitionInfo]]] =
        f(self.listTopics)
      override def listTopics(timeout: FiniteDuration): G[Map[String, Seq[PartitionInfo]]] =
        f(self.listTopics(timeout))
      override def metrics: G[Map[MetricName, Metric]] =
        f(self.metrics)

      override def pause(partitions: Iterable[TopicPartition]): G[Unit] =
        f(self.pause(partitions))
      override def paused: G[Set[TopicPartition]] =
        f(self.paused)
      override def poll(timeout: FiniteDuration): G[ConsumerRecords[K, V]] =
        f(self.poll(timeout))
      override def position(partition: TopicPartition): G[Long] =
        f(self.position(partition))
      override def resume(partitions: Iterable[TopicPartition]): G[Unit] =
        f(self.resume(partitions))
      override def seek(partition: TopicPartition, offset: Long): G[Unit] =
        f(self.seek(partition, offset))
      override def seekToBeginning(partitions: Iterable[TopicPartition]): G[Unit] =
        f(self.seekToBeginning(partitions))
      override def seekToEnd(partitions: Iterable[TopicPartition]): G[Unit] =
        f(self.seekToEnd(partitions))
      override def subscribe(topics: Iterable[String]): G[Unit] =
        f(self.subscribe(topics))
      override def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): G[Unit] =
        f(self.subscribe(topics, callback))
      override def subscribe(pattern: Pattern): G[Unit] =
        f(self.subscribe(pattern))
      override def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): G[Unit] =
        f(self.subscribe(pattern, callback))
      override def subscription: G[Set[String]] =
        f(self.subscription)
      override def unsubscribe: G[Unit] =
        f(self.unsubscribe)

      override def partitionQueries: PartitionQueries[G] =
        self.partitionQueries.mapK(f)
    }
  }
}

object ShiftingConsumer {
  def apply[F[_]: Async, K, V](
    c: ConsumerApi[F, K, V],
    blockingContext: ExecutionContext,
  ): ConsumerApi[F, K, V] =
    c.mapK(
      FunctionK.liftFunction(
        Async[F].evalOn(_, blockingContext)
      )
    )
}

object ConsumerApi {
  implicit def bifunctor[F[_]: Functor]: Bifunctor[ConsumerApi[F, *, *]] =
    new Bifunctor[ConsumerApi[F, *, *]] {
      override def bimap[A, B, C, D](
        fab: ConsumerApi[F,A,B]
      )(
        f: A => C,
        g: B => D
      ): ConsumerApi[F,C,D] =
        new ConsumerApi[F, C, D] {
          override def assign(partitions: Iterable[TopicPartition]): F[Unit] =
            fab.assign(partitions)
          override def assignment: F[Set[TopicPartition]] =
            fab.assignment

          override def close: F[Unit] =
            fab.close
          override def close(timeout: FiniteDuration): F[Unit] =
            fab.close(timeout)

          override def commitAsync: F[Unit] =
            fab.commitAsync
          override def commitAsync(
              offsets: Map[TopicPartition, OffsetAndMetadata],
              callback: OffsetCommitCallback
          ): F[Unit] =
            fab.commitAsync(offsets, callback)
          override def commitAsync(callback: OffsetCommitCallback): F[Unit] =
            fab.commitAsync(callback)
          override def commitSync: F[Unit] =
            fab.commitSync
          override def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit] =
            fab.commitSync(offsets)

          override def listTopics: F[Map[String, Seq[PartitionInfo]]] =
            fab.listTopics
          override def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]] =
            fab.listTopics(timeout)
          override def metrics: F[Map[MetricName, Metric]] =
            fab.metrics

          override def pause(partitions: Iterable[TopicPartition]): F[Unit] =
            fab.pause(partitions)
          override def paused: F[Set[TopicPartition]] =
            fab.paused
          override def poll(timeout: FiniteDuration): F[ConsumerRecords[C, D]] =
            fab.poll(timeout).map(_.bimap(f, g))
          override def position(partition: TopicPartition): F[Long] =
            fab.position(partition)
          override def resume(partitions: Iterable[TopicPartition]): F[Unit] =
            fab.resume(partitions)
          override def seek(partition: TopicPartition, offset: Long): F[Unit] =
            fab.seek(partition, offset)
          override def seekToBeginning(partitions: Iterable[TopicPartition]): F[Unit] =
            fab.seekToBeginning(partitions)
          override def seekToEnd(partitions: Iterable[TopicPartition]): F[Unit] =
            fab.seekToEnd(partitions)
          override def subscribe(topics: Iterable[String]): F[Unit] =
            fab.subscribe(topics)
          override def subscribe(topics: Iterable[String], callback: ConsumerRebalanceListener): F[Unit] =
            fab.subscribe(topics, callback)
          override def subscribe(pattern: Pattern): F[Unit] =
            fab.subscribe(pattern)
          override def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): F[Unit] =
            fab.subscribe(pattern, callback)
          override def subscription: F[Set[String]] =
            fab.subscription
          override def unsubscribe: F[Unit] =
            fab.unsubscribe

          override def partitionQueries: PartitionQueries[F] =
            fab.partitionQueries
        }
    }

  // private[this] def createKafkaConsumer[F[_]: Sync, K, V](
  //     configs: (String, AnyRef)*
  // ): F[KafkaConsumer[K, V]] =
  //   Sync[F].delay(new KafkaConsumer[K, V](configs.toMap.asJava))

  private[this] def createKafkaConsumer[F[_]: Sync, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): F[KafkaConsumer[K, V]] =
    Sync[F].delay(new KafkaConsumer[K, V](configs.toMap.asJava, keyDeserializer, valueDeserializer))

  object BlockingContext {

    def resource[F[_]: Sync]: Resource[F, ExecutionContextExecutorService] =
      Resource.make(
        Sync[F].delay(
          ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor(new ThreadFactory {
            val factory = Executors.defaultThreadFactory()
            def newThread(r: Runnable): Thread = {
              val t = factory.newThread(r)
              t.setName("kafka4s-consumer")
              t
            }
          }))
        )
      )(a => Sync[F].delay(a.shutdown()))
  }

  def resource[F[_]: Async, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    BlockingContext.resource.flatMap(
      e =>
        Resource.make(
          createKafkaConsumer[F, K, V](keyDeserializer, valueDeserializer, configs: _*)
            .map(c => ShiftingConsumer(ConsumerImpl(c), e))
        )(_.close)
    )

  def resource[F[_]: Async, K: Deserializer, V: Deserializer](
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    resource[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)

  object ByteArray {
    def resource[F[_]: Async](
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, Array[Byte], Array[Byte]]] =
      ConsumerApi.resource[F, Array[Byte], Array[Byte]](configs: _*)
  }

  object NonShifting {

    def resource[F[_]: Sync, K, V](
        keyDeserializer: Deserializer[K],
        valueDeserializer: Deserializer[V],
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, K, V]] =
      Resource.make(
        createKafkaConsumer[F, K, V](keyDeserializer, valueDeserializer, configs: _*)
          .map(ConsumerImpl.create(_))
      )(_.close)

    def resource[F[_]: Sync, K: Deserializer, V: Deserializer](
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, K, V]] =
      resource[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)
  }
}
