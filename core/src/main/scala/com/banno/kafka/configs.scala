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

//TODO other configs... maybe we could auto generate these somehow?

sealed abstract class Config(val _1: String, val _2: AnyRef) extends Product2[String, AnyRef]

object Config {
  implicit def toTuple(c: Config): (String, AnyRef) = c._1 -> c._2
}

//helpers to make it simpler to specify configs, is this the best way to do this?
case class BootstrapServers(bs: String) extends Config(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bs)

case class ClientId(id: String) extends Config(CommonClientConfigs.CLIENT_ID_CONFIG, id)

case class EnableIdempotence(e: Boolean) extends Config(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, e.toString)

case class CompressionType(c: String) extends Config(ProducerConfig.COMPRESSION_TYPE_CONFIG, c)
object CompressionType {
  val none = CompressionType("none")
  val gzip = CompressionType("gzip")
  val snappy = CompressionType("snappy")
  val lz4 = CompressionType("lz4")
}

case class GroupId(id: String) extends Config(ConsumerConfig.GROUP_ID_CONFIG, id)

case class AutoOffsetReset(aor: String) extends Config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, aor)
object AutoOffsetReset {
  val earliest = AutoOffsetReset("earliest")
  val latest = AutoOffsetReset("latest")
  val none = AutoOffsetReset("none")
}

case class EnableAutoCommit(b: Boolean) extends Config(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, b.toString)

case class KeySerializerClass(c: Class[_]) extends Config(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, c.getName)

case class ValueSerializerClass(c: Class[_])
object ValueSerializerClass {
  implicit def toConfig(vsc: ValueSerializerClass): (String, AnyRef) =
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> vsc.c.getName
}

case class KeyDeserializerClass(c: Class[_]) extends Config(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, c.getName)

case class ValueDeserializerClass(c: Class[_]) extends Config(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, c.getName)

case class SchemaRegistryUrl(url: String) extends Config(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url)

case class MaxSchemasPerSubject(m: Int) extends Config(AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_CONFIG, m.toString)

case class AutoRegisterSchemas(r: Boolean) extends Config(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, r.toString)

case class SpecificAvroReader(s: Boolean) extends Config(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, s.toString)

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
