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

import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import fs2.Stream
import java.util.concurrent.{Future => JFuture}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.apache.kafka.common._
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.avro.generic.GenericRecord
import com.sksamuel.avro4s.ToRecord
import io.confluent.kafka.serializers.KafkaAvroSerializer
import com.banno.kafka._

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

  def stream[F[_]: Async, K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      configs: (String, AnyRef)*
  ): Stream[F, ProducerApi[F, K, V]] =
    Stream.resource(resource[F, K, V](keySerializer, valueSerializer, configs: _*))

  def stream[F[_]: Async, K: Serializer, V: Serializer](
      configs: (String, AnyRef)*
  ): Stream[F, ProducerApi[F, K, V]] =
    stream[F, K, V](implicitly[Serializer[K]], implicitly[Serializer[V]], configs: _*)

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

    def stream[F[_]: Async, K, V](configs: (String, AnyRef)*): Stream[F, ProducerApi[F, K, V]] =
      Stream.resource(resource[F, K, V](configs: _*))

    object Generic {

      def resource[F[_]: Async](
          configs: (String, AnyRef)*
      ): Resource[F, ProducerApi[F, GenericRecord, GenericRecord]] =
        ProducerApi.Avro.resource[F, GenericRecord, GenericRecord](configs: _*)

      def stream[F[_]: Async](
          configs: (String, AnyRef)*
      ): Stream[F, ProducerApi[F, GenericRecord, GenericRecord]] =
        Stream.resource(resource[F](configs: _*))
    }

    object Specific {
      //TODO
    }
  }

  object Avro4s {

    def resource[F[_]: Async, K: ToRecord, V: ToRecord](
        configs: (String, AnyRef)*
    ): Resource[F, ProducerApi[F, K, V]] =
      ProducerApi.Avro.Generic.resource[F](configs: _*).map(Avro4sProducerImpl(_))

    def stream[F[_]: Async, K: ToRecord, V: ToRecord](
        configs: (String, AnyRef)*
    ): Stream[F, ProducerApi[F, K, V]] =
      Stream.resource(resource[F, K, V](configs: _*))
  }
}
