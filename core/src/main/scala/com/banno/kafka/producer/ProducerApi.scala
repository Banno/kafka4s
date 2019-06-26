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
import cats.effect.{Async, ContextShift, Sync}
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
      consumerGroupId: String): F[Unit]

  private[producer] def sendRaw(record: ProducerRecord[K, V]): JFuture[RecordMetadata]
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Callback): JFuture[RecordMetadata]
  private[producer] def sendRaw(
      record: ProducerRecord[K, V],
      callback: Either[Exception, RecordMetadata] => Unit): Unit

  def sendAndForget(record: ProducerRecord[K, V]): F[Unit]
  def sendSync(record: ProducerRecord[K, V]): F[RecordMetadata]
  def sendAsync(record: ProducerRecord[K, V]): F[RecordMetadata]
}

object ProducerApi {

  def createProducer[F[_]: Sync, K, V](configs: (String, AnyRef)*): F[KafkaProducer[K, V]] =
    Sync[F].delay(new KafkaProducer[K, V](configs.toMap.asJava))
  def createProducer[F[_]: Sync, K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      configs: (String, AnyRef)*): F[KafkaProducer[K, V]] =
    Sync[F].delay(new KafkaProducer[K, V](configs.toMap.asJava, keySerializer, valueSerializer))

  def apply[F[_]: Async, K, V](configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    createProducer[F, K, V](configs: _*).map(ProducerImpl[F, K, V](_))
  def apply[F[_]: Async, K, V](
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    createProducer[F, K, V](keySerializer, valueSerializer, configs: _*)
      .map(ProducerImpl[F, K, V](_))

  def create[F[_]: Async, K: Serializer, V: Serializer](
      configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    apply[F, K, V](implicitly[Serializer[K]], implicitly[Serializer[V]], configs: _*)

  def shifting[F[_]: Async: ContextShift, K, V](
      producerContext: ExecutionContext,
      configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    apply[F, K, V](configs: _*).map(ShiftingProducerImpl[F, K, V](_, producerContext))
  def shifting[F[_]: Async: ContextShift, K, V](
      producerContext: ExecutionContext,
      keySerializer: Serializer[K],
      valueSerializer: Serializer[V],
      configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    apply[F, K, V](keySerializer, valueSerializer, configs: _*)
      .map(ShiftingProducerImpl[F, K, V](_, producerContext))

  def createShifting[F[_]: Async: ContextShift, K: Serializer, V: Serializer](
      producerContext: ExecutionContext,
      configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    create[F, K, V](configs: _*).map(ShiftingProducerImpl[F, K, V](_, producerContext))

  //TODO createSpecificProducer

  def avro[F[_]: Async, K, V](configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    apply[F, K, V](
      (
        configs.toMap +
          KeySerializerClass(classOf[KafkaAvroSerializer]) +
          ValueSerializerClass(classOf[KafkaAvroSerializer])
      ).toSeq: _*)

  def createGenericProducer[F[_]: Sync](
      configs: (String, AnyRef)*): F[KafkaProducer[GenericRecord, GenericRecord]] =
    createProducer[F, GenericRecord, GenericRecord](
      (
        configs.toMap +
          KeySerializerClass(classOf[KafkaAvroSerializer]) +
          ValueSerializerClass(classOf[KafkaAvroSerializer])
      ).toSeq: _*)

  def generic[F[_]: Async](
      configs: (String, AnyRef)*): F[ProducerApi[F, GenericRecord, GenericRecord]] =
    createGenericProducer[F](configs: _*).map(ProducerImpl[F, GenericRecord, GenericRecord](_))

  def avro4s[F[_]: Async, K: ToRecord, V: ToRecord](
      configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    generic[F](configs: _*).map(Avro4sProducerImpl[F, K, V](_))

  def avro4sShifting[F[_]: Async: ContextShift, K: ToRecord, V: ToRecord](
      producerContext: ExecutionContext,
      configs: (String, AnyRef)*): F[ProducerApi[F, K, V]] =
    avro4s[F, K, V](configs: _*).map(ShiftingProducerImpl[F, K, V](_, producerContext))
}
