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
import cats.effect.{Async, ContextShift, Sync}
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

trait ConsumerApi[F[_], K, V] {
  def assign(partitions: Iterable[TopicPartition]): F[Unit]
  def assignment: F[Set[TopicPartition]]
  def beginningOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]]
  def beginningOffsets(partitions: Iterable[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Long]]
  def close: F[Unit]
  def close(timeout: FiniteDuration): F[Unit]
  def commitAsync: F[Unit]
  def commitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): F[Unit]
  def commitAsync(callback: OffsetCommitCallback): F[Unit]
  def commitSync: F[Unit]
  def commitSync(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]
  def committed(partition: TopicPartition): F[OffsetAndMetadata]
  def endOffsets(partitions: Iterable[TopicPartition]): F[Map[TopicPartition, Long]]
  def endOffsets(partitions: Iterable[TopicPartition], timeout: FiniteDuration): F[Map[TopicPartition, Long]]
  def listTopics: F[Map[String, Seq[PartitionInfo]]]
  def listTopics(timeout: FiniteDuration): F[Map[String, Seq[PartitionInfo]]]
  def metrics: F[Map[MetricName, Metric]]
  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long]): F[Map[TopicPartition, OffsetAndTimestamp]]
  def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Long], timeout: FiniteDuration): F[Map[TopicPartition, OffsetAndTimestamp]]
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

  def createConsumer[F[_]: Sync, K, V](configs: (String, AnyRef)*): F[KafkaConsumer[K, V]] = 
    Sync[F].delay(new KafkaConsumer[K, V](configs.toMap.asJava))
  def createConsumer[F[_]: Sync, K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V], configs: (String, AnyRef)*): F[KafkaConsumer[K, V]] = 
    Sync[F].delay(new KafkaConsumer[K, V](configs.toMap.asJava, keyDeserializer, valueDeserializer))

  def apply[F[_]: Sync, K, V](configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] = 
    createConsumer[F, K, V](configs: _*).map(ConsumerImpl(_))
  def apply[F[_]: Sync, K, V](keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V], configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] = 
    createConsumer[F, K, V](keyDeserializer, valueDeserializer, configs: _*).map(ConsumerImpl(_))

  def create[F[_]: Sync, K: Deserializer, V: Deserializer](configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] = 
    apply[F, K, V](implicitly[Deserializer[K]], implicitly[Deserializer[V]], configs: _*)

  def shifting[F[_]: Async: ContextShift, K, V](configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] =
    apply[F, K, V](configs: _*).map(ShiftingConsumerImpl(_))

  def createShifting[F[_]: Async: ContextShift, K: Deserializer, V: Deserializer](configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] =
    create[F, K, V](configs: _*).map(ShiftingConsumerImpl(_))

  def avro[F[_]: Async, K, V](configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] = 
    apply[F, K, V]((
      configs.toMap + 
      KeyDeserializerClass(classOf[KafkaAvroDeserializer]) + 
      ValueDeserializerClass(classOf[KafkaAvroDeserializer])
    ).toSeq: _*)

  def avroSpecific[F[_]: Async, K, V](configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] = 
    avro[F, K, V]((configs.toMap + SpecificAvroReader(true)).toSeq: _*)

  def createGenericConsumer[F[_]: Sync](configs: (String, AnyRef)*): F[KafkaConsumer[GenericRecord, GenericRecord]] = 
    createConsumer[F, GenericRecord, GenericRecord]((
      configs.toMap + 
      KeyDeserializerClass(classOf[KafkaAvroDeserializer]) + 
      ValueDeserializerClass(classOf[KafkaAvroDeserializer])
    ).toSeq: _*)

  def generic[F[_]: Async](configs: (String, AnyRef)*): F[ConsumerApi[F, GenericRecord, GenericRecord]] = 
    createGenericConsumer[F](configs: _*).map(ConsumerImpl(_))

  def avro4s[F[_]: Async, K: FromRecord, V: FromRecord](configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] = 
    generic[F](configs: _*).map(Avro4sConsumerImpl(_))

  def avro4sShifting[F[_]: Async: ContextShift, K: FromRecord, V: FromRecord](configs: (String, AnyRef)*): F[ConsumerApi[F, K, V]] =
    avro4s[F, K, V](configs: _*).map(ShiftingConsumerImpl(_))
}
