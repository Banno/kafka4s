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

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.metrics.MetricsReporter
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import scala.reflect.ClassTag
import io.confluent.kafka.serializers.subject.{
  RecordNameStrategy => KRecordNameStrategy,
  TopicNameStrategy => KTopicNameStrategy,
  TopicRecordNameStrategy => KTopicRecordNameStrategy
}
import scala.concurrent.duration.FiniteDuration

//TODO other configs... maybe we could auto generate these somehow?

//helpers to make it simpler to specify configs, is this the best way to do this?
case class BootstrapServers(bs: String)
object BootstrapServers {
  implicit def toConfig(bs: BootstrapServers): (String, AnyRef) =
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> bs.bs
}

object ClientId {
  def apply(id: String): (String, String) = CommonClientConfigs.CLIENT_ID_CONFIG -> id
}

case class EnableIdempotence(e: Boolean)
object EnableIdempotence {
  implicit def toConfig(ei: EnableIdempotence): (String, AnyRef) =
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> ei.e.toString
}

case class CompressionType(c: String)
object CompressionType {
  val none = CompressionType("none")
  val gzip = CompressionType("gzip")
  val snappy = CompressionType("snappy")
  val lz4 = CompressionType("lz4")
  implicit def toConfig(ct: CompressionType): (String, AnyRef) =
    ProducerConfig.COMPRESSION_TYPE_CONFIG -> ct.c
}

case class GroupId(id: String)
object GroupId {
  implicit def toConfig(g: GroupId): (String, AnyRef) = ConsumerConfig.GROUP_ID_CONFIG -> g.id
}

case class AutoOffsetReset(aor: String)
object AutoOffsetReset {
  val earliest = AutoOffsetReset("earliest")
  val latest = AutoOffsetReset("latest")
  val none = AutoOffsetReset("none")
  implicit def toConfig(aor: AutoOffsetReset): (String, AnyRef) =
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> aor.aor
}

case class EnableAutoCommit(b: Boolean)
object EnableAutoCommit {
  implicit def toConfig(eac: EnableAutoCommit): (String, AnyRef) =
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> eac.b.toString
}

object AutoCommitInterval {
  def apply(d: FiniteDuration): (String, AnyRef) =
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> d.toMillis.toString
}

case class KeySerializerClass(c: Class[?])
object KeySerializerClass {
  implicit def toConfig(ksc: KeySerializerClass): (String, AnyRef) =
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> ksc.c.getName
}

case class ValueSerializerClass(c: Class[?])
object ValueSerializerClass {
  implicit def toConfig(vsc: ValueSerializerClass): (String, AnyRef) =
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> vsc.c.getName
}

case class KeyDeserializerClass(c: Class[?])
object KeyDeserializerClass {
  implicit def toConfig(kdc: KeyDeserializerClass): (String, AnyRef) =
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> kdc.c.getName
}

case class ValueDeserializerClass(c: Class[?])
object ValueDeserializerClass {
  implicit def toConfig(vdc: ValueDeserializerClass): (String, AnyRef) =
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> vdc.c.getName
}

case class SchemaRegistryUrl(url: String)
object SchemaRegistryUrl {
  implicit def toConfig(sru: SchemaRegistryUrl): (String, AnyRef) =
    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> sru.url
}

case class MaxSchemasPerSubject(m: Int)
object MaxSchemasPerSubject {
  implicit def toConfig(msps: MaxSchemasPerSubject): (String, AnyRef) =
    AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG -> msps.m.toString
}

case class AutoRegisterSchemas(r: Boolean)
object AutoRegisterSchemas {
  implicit def toConfig(ars: AutoRegisterSchemas): (String, AnyRef) =
    AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS -> ars.r.toString
}

case class SpecificAvroReader(s: Boolean)
object SpecificAvroReader {
  implicit def toConfig(sar: SpecificAvroReader): (String, AnyRef) =
    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG -> sar.s.toString
}

object MetricReporters {
  //TODO need to support multiple reporter types
  def apply[T <: MetricsReporter](implicit ct: ClassTag[T]): (String, String) =
    CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG -> ct.runtimeClass.getName
}

object TransactionalId {
  def apply(id: String): (String, AnyRef) = ProducerConfig.TRANSACTIONAL_ID_CONFIG -> id
}

object IsolationLevel {
  val ReadCommitted
      : (String, String) = (org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_committed")
  val ReadUncommitted
      : (String, String) = (org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG -> "read_uncommitted")
}

object MaxPollRecords {
  def apply(count: Int): (String, AnyRef) = ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> count.toString
}

object KeySubjectNameStrategy {
  val strategy = AbstractKafkaAvroSerDeConfig.KEY_SUBJECT_NAME_STRATEGY
  val RecordNameStrategy: (String, AnyRef) = strategy -> classOf[KRecordNameStrategy].getName()
  val TopicNameStrategy: (String, AnyRef) = strategy -> classOf[KTopicNameStrategy].getName()
  val TopicRecordNameStrategy: (String, AnyRef) = strategy -> classOf[KTopicRecordNameStrategy]
    .getName()
}
object ValueSubjectNameStrategy {
  val strategy = AbstractKafkaAvroSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY
  val RecordNameStrategy: (String, AnyRef) = strategy -> classOf[KRecordNameStrategy].getName()
  val TopicNameStrategy: (String, AnyRef) = strategy -> classOf[KTopicNameStrategy].getName()
  val TopicRecordNameStrategy: (String, AnyRef) = strategy -> classOf[KTopicRecordNameStrategy]
    .getName()
}
