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

package com.banno.kafka.producer

import cats.*
import cats.arrow.*
import cats.effect.*
import cats.syntax.all.*
import com.banno.kafka.*
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.*
import org.apache.kafka.common.serialization.Serializer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

trait ProducerApi[F[_], K, V] {
  def abortTransaction: F[Unit]
  def beginTransaction: F[Unit]
  def close: F[Unit]
  def close(timeout: FiniteDuration): F[Unit]
  def commitTransaction: F[Unit]
  def flush: F[Unit]
  def initTransactions: F[Unit]
  def metrics: F[Map[MetricName, Metric]]
  def partitionsFor(topic: String): F[Seq[PartitionInfo]]
  def sendOffsetsToTransaction(
      offsets: Map[TopicPartition, OffsetAndMetadata],
      groupMetadata: ConsumerGroupMetadata,
  ): F[Unit]

  def sendAndForget(record: ProducerRecord[K, V]): F[Unit]
  def sendSync(record: ProducerRecord[K, V]): F[RecordMetadata]
  def sendAsync(record: ProducerRecord[K, V]): F[RecordMetadata]

  // TODO obviously need to rename this
  def send(record: ProducerRecord[K, V]): F[F[RecordMetadata]]

  // Cats doesn't have `Bicontravariant`
  final def contrabimap[A, B](f: A => K, g: B => V): ProducerApi[F, A, B] = {
    val self = this
    new ProducerApi[F, A, B] {
      override def abortTransaction: F[Unit] =
        self.abortTransaction
      override def beginTransaction: F[Unit] =
        self.beginTransaction
      override def close: F[Unit] =
        self.close
      override def close(timeout: FiniteDuration): F[Unit] =
        self.close(timeout)
      override def commitTransaction: F[Unit] =
        self.commitTransaction
      override def flush: F[Unit] =
        self.flush
      override def initTransactions: F[Unit] =
        self.initTransactions
      override def metrics: F[Map[MetricName, Metric]] =
        self.metrics
      override def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
        self.partitionsFor(topic)
      override def sendOffsetsToTransaction(
          offsets: Map[TopicPartition, OffsetAndMetadata],
          groupMetadata: ConsumerGroupMetadata,
      ): F[Unit] =
        self.sendOffsetsToTransaction(offsets, groupMetadata)

      override def sendAndForget(record: ProducerRecord[A, B]): F[Unit] =
        self.sendAndForget(record.bimap(f, g))
      override def sendSync(record: ProducerRecord[A, B]): F[RecordMetadata] =
        self.sendSync(record.bimap(f, g))
      override def sendAsync(record: ProducerRecord[A, B]): F[RecordMetadata] =
        self.sendAsync(record.bimap(f, g))
      override def send(record: ProducerRecord[A, B]): F[F[RecordMetadata]] =
        self.send(record.bimap(f, g))
    }
  }

  final def semiflatContrabimap[A, B](
      f: A => F[K],
      g: B => F[V],
  )(implicit F: Monad[F]): ProducerApi[F, A, B] = {
    val self = this
    new ProducerApi[F, A, B] {
      override def abortTransaction: F[Unit] =
        self.abortTransaction
      override def beginTransaction: F[Unit] =
        self.beginTransaction
      override def close: F[Unit] =
        self.close
      override def close(timeout: FiniteDuration): F[Unit] =
        self.close(timeout)
      override def commitTransaction: F[Unit] =
        self.commitTransaction
      override def flush: F[Unit] =
        self.flush
      override def initTransactions: F[Unit] =
        self.initTransactions
      override def metrics: F[Map[MetricName, Metric]] =
        self.metrics
      override def partitionsFor(topic: String): F[Seq[PartitionInfo]] =
        self.partitionsFor(topic)
      override def sendOffsetsToTransaction(
          offsets: Map[TopicPartition, OffsetAndMetadata],
          groupMetadata: ConsumerGroupMetadata,
      ): F[Unit] =
        self.sendOffsetsToTransaction(offsets, groupMetadata)

      override def sendAndForget(record: ProducerRecord[A, B]): F[Unit] =
        record.bitraverse(f, g) >>= self.sendAndForget
      override def sendSync(record: ProducerRecord[A, B]): F[RecordMetadata] =
        record.bitraverse(f, g) >>= self.sendSync
      override def sendAsync(record: ProducerRecord[A, B]): F[RecordMetadata] =
        record.bitraverse(f, g) >>= self.sendAsync
      override def send(record: ProducerRecord[A, B]): F[F[RecordMetadata]] =
        record.bitraverse(f, g) >>= self.send
    }
  }

  final def mapK[G[_]: Functor](f: F ~> G): ProducerApi[G, K, V] = {
    val self = this
    new ProducerApi[G, K, V] {
      override def abortTransaction: G[Unit] = f(self.abortTransaction)
      override def beginTransaction: G[Unit] = f(self.beginTransaction)
      override def close: G[Unit] = f(self.close)
      override def close(timeout: FiniteDuration): G[Unit] = f(
        self.close(timeout)
      )
      override def commitTransaction: G[Unit] = f(self.commitTransaction)
      override def flush: G[Unit] = f(self.flush)
      override def initTransactions: G[Unit] = f(self.initTransactions)
      override def metrics: G[Map[MetricName, Metric]] = f(self.metrics)
      override def partitionsFor(topic: String): G[Seq[PartitionInfo]] =
        f(self.partitionsFor(topic))
      override def sendOffsetsToTransaction(
          offsets: Map[TopicPartition, OffsetAndMetadata],
          groupMetadata: ConsumerGroupMetadata,
      ): G[Unit] =
        f(self.sendOffsetsToTransaction(offsets, groupMetadata))

      override def sendAndForget(record: ProducerRecord[K, V]): G[Unit] =
        f(self.sendAndForget(record))
      override def sendSync(record: ProducerRecord[K, V]): G[RecordMetadata] =
        f(self.sendSync(record))
      override def sendAsync(record: ProducerRecord[K, V]): G[RecordMetadata] =
        f(self.sendAsync(record))
      override def send(record: ProducerRecord[K, V]): G[G[RecordMetadata]] =
        f(self.send(record)).map(f(_))
    }
  }
}

object ShiftingProducer {
  def apply[F[_]: Async, K, V](
      c: ProducerApi[F, K, V],
      blockingContext: ExecutionContext,
  ): ProducerApi[F, K, V] =
    c.mapK(
      FunctionK.liftFunction(
        Async[F].evalOn(_, blockingContext)
      )
    )
}

object ProducerApi {

  private[this] def createKafkaProducer[F[_]: Sync, K, V](
      configs: (String, AnyRef)*
  ): F[KafkaProducer[K, V]] =
    Sync[F].delay(new KafkaProducer[K, V](configs.toMap.asJava))

  private[this] def createKafkaProducer[F[_]: Sync, K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      configs: (String, AnyRef)*
  ): F[KafkaProducer[K, V]] =
    Sync[F].delay(
      new KafkaProducer[K, V](
        configs.toMap.asJava,
        keySerializer,
        valueSerializer,
      )
    )

  def resource[F[_]: Async, K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      configs: (String, AnyRef)*
  ): Resource[F, ProducerApi[F, K, V]] =
    Resource.make(
      createKafkaProducer[F, K, V](keySerializer, valueSerializer, configs: _*)
        .map(ProducerImpl.create[F, K, V](_))
    )(_.close)

  def resource[F[_]: Async, K: Serializer, V: Serializer](
      configs: (String, AnyRef)*
  ): Resource[F, ProducerApi[F, K, V]] =
    resource[F, K, V](
      implicitly[Serializer[K]],
      implicitly[Serializer[V]],
      configs: _*
    )

  object Avro {
    def resource[F[_]: Async, K, V](
        configs: (String, AnyRef)*
    ): Resource[F, ProducerApi[F, K, V]] =
      Resource.make(
        createKafkaProducer[F, K, V](
          (
            configs.toMap +
            KeySerializerClass(classOf[KafkaAvroSerializer]) +
            ValueSerializerClass(classOf[KafkaAvroSerializer])
          ).toSeq: _*
        ).map(ProducerImpl.create[F, K, V](_))
      )(_.close)

    object Generic {
      def resource[F[_]: Async](
          configs: (String, AnyRef)*
      ): Resource[F, ProducerApi[F, GenericRecord, GenericRecord]] =
        ProducerApi.Avro.resource[F, GenericRecord, GenericRecord](configs: _*)
    }
  }
}
