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

package com.banno.kafka
package producer

import cats._
import cats.arrow._
import cats.syntax.all._
import cats.effect.{Async, Resource, Sync}
import java.util.concurrent.{Future => JFuture}
import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import scala.concurrent.ExecutionContext

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
      consumerGroupId: String
  ): F[Unit]

  private[producer] def sendRaw(record: ProducerRecord[K, V]): JFuture[RecordMetadata]
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Callback
  ): JFuture[RecordMetadata]
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Either[Exception, RecordMetadata] => Unit
  ): Unit

  def sendAndForget(record: ProducerRecord[K, V]): F[Unit]
  def sendSync(record: ProducerRecord[K, V]): F[RecordMetadata]
  def sendAsync(record: ProducerRecord[K, V]): F[RecordMetadata]

  // Implementing directly here since Cats doesn't have `Bicontravariant`, and
  // we technically don't need that level of abstraction, just this method.
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
          consumerGroupId: String
      ): F[Unit] =
        self.sendOffsetsToTransaction(offsets, consumerGroupId)

      override private[producer] def sendRaw(
        record: ProducerRecord[A, B]
      ): JFuture[RecordMetadata] =
        self.sendRaw(record.bimap(f, g))

      override private[producer] def sendRaw(
          record: ProducerRecord[A, B],
          callback: Callback
      ): JFuture[RecordMetadata] =
        self.sendRaw(record.bimap(f, g), callback)

      override private[producer] def sendRaw(
          record: ProducerRecord[A, B],
          callback: Either[Exception, RecordMetadata] => Unit
      ): Unit =
        self.sendRaw(record.bimap(f, g), callback)

      override def sendAndForget(record: ProducerRecord[A, B]): F[Unit] =
        self.sendAndForget(record.bimap(f, g))
      override def sendSync(record: ProducerRecord[A, B]): F[RecordMetadata] =
        self.sendSync(record.bimap(f, g))
      override def sendAsync(record: ProducerRecord[A, B]): F[RecordMetadata] =
        self.sendAsync(record.bimap(f, g))
    }
  }

  final def mapK[G[_]](f: F ~> G): ProducerApi[G, K, V] = {
    val self = this
    new ProducerApi[G, K, V] {
      override def abortTransaction: G[Unit] = f(self.abortTransaction)
      override def beginTransaction: G[Unit] = f(self.beginTransaction)
      override def close: G[Unit] = f(self.close)
      override def close(timeout: FiniteDuration): G[Unit] = f(self.close(timeout))
      override def commitTransaction: G[Unit] = f(self.commitTransaction)
      override def flush: G[Unit] = f(self.flush)
      override def initTransactions: G[Unit] = f(self.initTransactions)
      override def metrics: G[Map[MetricName, Metric]] = f(self.metrics)
      override def partitionsFor(topic: String): G[Seq[PartitionInfo]] =
        f(self.partitionsFor(topic))
      override def sendOffsetsToTransaction(
          offsets: Map[TopicPartition, OffsetAndMetadata],
          consumerGroupId: String
      ): G[Unit] =
        f(self.sendOffsetsToTransaction(offsets, consumerGroupId))

      override private[producer] def sendRaw(
        record: ProducerRecord[K, V]
      ): JFuture[RecordMetadata] =
        self.sendRaw(record)
      override private[producer] def sendRaw(
          record: ProducerRecord[K, V],
          callback: Callback
      ): JFuture[RecordMetadata] =
        self.sendRaw(record, callback)
      override private[producer] def sendRaw(
          record: ProducerRecord[K, V],
          callback: Either[Exception, RecordMetadata] => Unit
      ): Unit =
        self.sendRaw(record, callback)

      override def sendAndForget(record: ProducerRecord[K, V]): G[Unit] =
        f(self.sendAndForget(record))
      override def sendSync(record: ProducerRecord[K, V]): G[RecordMetadata] =
        f(self.sendSync(record))
      override def sendAsync(record: ProducerRecord[K, V]): G[RecordMetadata] =
        f(self.sendAsync(record))
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

  // private[this] def createKafkaProducer[F[_]: Sync, K, V](
  //     configs: (String, AnyRef)*
  // ): F[KafkaProducer[K, V]] =
  //   Sync[F].delay(new KafkaProducer[K, V](configs.toMap.asJava))

  private[this] def createKafkaProducer[F[_]: Sync, K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      configs: (String, AnyRef)*
  ): F[KafkaProducer[K, V]] =
    Sync[F].delay(new KafkaProducer[K, V](configs.toMap.asJava, keySerializer, valueSerializer))

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
    resource[F, K, V](implicitly[Serializer[K]], implicitly[Serializer[V]], configs: _*)
}
