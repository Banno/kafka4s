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

import fs2.Stream
import cats.effect._
import cats.effect.concurrent._
import cats.implicits._
import java.util.regex.Pattern
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.clients.consumer._
import org.apache.avro.generic.GenericRecord
import com.sksamuel.avro4s.FromRecord
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import com.banno.kafka._
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

trait ConsumerApi[F[_], K, V] {
  def assign(partitions: Iterable[TopicPartition]): F[Unit]
  def assignment: F[Set[TopicPartition]]
  def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]]
  def beginningOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]
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
  def committed(partition: Set[TopicPartition]): F[Map[TopicPartition, OffsetAndMetadata]]
  def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]]
  def endOffsets(
      partitions: Iterable[TopicPartition],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, Long]]
  def listTopics: F[Map[String, Seq[PartitionInfo]]]
  def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]]
  def metrics: F[Map[MetricName, Metric]]
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long]
  ): F[Map[TopicPartition, OffsetAndTimestamp]]
  def offsetsForTimes(
      timestampsToSearch: Map[TopicPartition, Long],
      timeout: FiniteDuration
  ): F[Map[TopicPartition, OffsetAndTimestamp]]
  def partitionsFor(topic: String): F[Seq[PartitionInfo]]
  def partitionsFor(topic: String, timeout: FiniteDuration): F[Seq[PartitionInfo]]
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
  def wakeup: F[Unit]
}

object ConsumerApi {

  private[this] def createKafkaConsumer[F[_]: Sync, K, V](
      configs: (String, AnyRef)*
  ): F[KafkaConsumer[K, V]] =
    Sync[F].delay(new KafkaConsumer[K, V](configs.toMap.asJava))

  private[this] def createKafkaConsumer[F[_]: Sync, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): F[KafkaConsumer[K, V]] =
    Sync[F].delay(new KafkaConsumer[K, V](configs.toMap.asJava, keyDeserializer, valueDeserializer))

  /** Provides a default `cats.effect.Blocker` for use with a Kafka4s Consumer. */
  object BlockingContext {

    private[this] val poolCounter: Ref[IO, Long] =
      Ref.unsafe[IO, Long](0L)

    private[this] def newThreadFactory[F[_]](implicit F: LiftIO[F], FS: Sync[F]): F[ThreadFactory] =
      // Because poolCounter is statically initialized global variable, we
      // need to suspend before access in order to not violate RF.
      //
      // https://github.com/typelevel/cats-effect/blob/v2.0.0/core/shared/src/main/scala/cats/effect/concurrent/Ref.scala#L176
      FS.suspend(
        F.liftIO(this.poolCounter.modify(l => (l + 1L, l))).map((poolCounter: Long) =>
          new ThreadFactory {

            // We are firmly in Java land here. So we need to use Java level atomic primitives.
            private val threadCounter: AtomicLong =
              new AtomicLong(0L)
            private val backingFactory: ThreadFactory =
              Executors.defaultThreadFactory()

            override final def newThread(r: Runnable): Thread = {
              val t: Thread = this.backingFactory.newThread(r)
              val id: Long = this.threadCounter.incrementAndGet()
              t.setName(s"kafka4s-consumer-pool${poolCounter}-${id}")
              t
            }
          }
        )
      )

    /** A default `cats.effect.Blocker`. */
    def defaultBlocker[F[_]: LiftIO: Sync]: Resource[F, Blocker] =
      Blocker.fromExecutorService(this.newThreadFactory[F].map(tf => Executors.newCachedThreadPool(tf)))
  }

  def resource[F[_]: Async: ContextShift, K, V](
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    BlockingContext.defaultBlocker.flatMap((b: Blocker) =>
      this.resourceWithBlocker[F, K, V](b, keyDeserializer, valueDeserializer, configs: _*)
    )

  def resourceWithBlocker[F[_]: Async: ContextShift, K, V](
      blocker: Blocker,
      keyDeserializer: Deserializer[K],
      valueDeserializer: Deserializer[V],
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    Resource.make(
      createKafkaConsumer[F, K, V](keyDeserializer, valueDeserializer, configs: _*)
        .map(c => ShiftingConsumerImpl.create(ConsumerImpl(c), blocker))
    )(_.close)

  def resource[F[_]: Async: ContextShift, K: Deserializer, V: Deserializer](
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    BlockingContext.defaultBlocker.flatMap((b: Blocker) =>
      this.resourceWithBlocker[F, K, V](b, configs: _*)
    )

  def resourceWithBlocker[F[_]: Async: ContextShift, K: Deserializer, V: Deserializer](
      blocker: Blocker,
      configs: (String, AnyRef)*
  ): Resource[F, ConsumerApi[F, K, V]] =
    resourceWithBlocker[F, K, V](blocker, implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)

  object Avro {

    def resource[F[_]: Async: ContextShift, K, V](
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, K, V]] =
      BlockingContext.defaultBlocker.flatMap(
        (blocker: Blocker) =>
          Resource.make(
            createKafkaConsumer[F, K, V](
              (
                configs.toMap +
                  KeyDeserializerClass(classOf[KafkaAvroDeserializer]) +
                  ValueDeserializerClass(classOf[KafkaAvroDeserializer])
              ).toSeq: _*
            ).map(c => ShiftingConsumerImpl.create(ConsumerImpl(c), blocker))
          )(_.close)
      )

    object Generic {

      def resource[F[_]: Async: ContextShift](
          configs: (String, AnyRef)*
      ): Resource[F, ConsumerApi[F, GenericRecord, GenericRecord]] =
        ConsumerApi.Avro.resource[F, GenericRecord, GenericRecord](configs: _*)

      def stream[F[_]: Async: ContextShift](
          configs: (String, AnyRef)*
      ): Stream[F, ConsumerApi[F, GenericRecord, GenericRecord]] =
        Stream.resource(resource[F](configs: _*))
    }

    object Specific {

      def resource[F[_]: Async: ContextShift, K, V](
          configs: (String, AnyRef)*
      ): Resource[F, ConsumerApi[F, K, V]] =
        ConsumerApi.Avro.resource[F, K, V]((configs.toMap + SpecificAvroReader(true)).toSeq: _*)
    }
  }

  object Avro4s {

    def resource[F[_]: Async: ContextShift, K: FromRecord, V: FromRecord](
        configs: (String, AnyRef)*
    ): Resource[F, ConsumerApi[F, K, V]] =
      ConsumerApi.Avro.Generic.resource[F](configs: _*).map(Avro4sConsumerImpl(_))
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
