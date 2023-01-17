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

import com.banno.kafka.producer.*
import com.banno.kafka.schemaregistry.*
import com.sksamuel.avro4s.*
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

package object avro4s {
  implicit final class SchemaRegistryAvro4sOpsOps[F[_]](
      private val r: SchemaRegistryApi[F]
  ) extends AnyVal {
    def avro4s: SchemaRegistryAvro4sOps[F] =
      new SchemaRegistryAvro4sOps(r)
  }

  implicit final class SchemaRegistryObjectOpsOps(
      private val x: SchemaRegistryApi.type
  ) extends AnyVal {
    def avro4s: SchemaRegistryApiObjectOps.type =
      SchemaRegistryApiObjectOps
  }

  implicit final class TopicObjectOpsOps(
      private val x: Topic.type
  ) extends AnyVal {
    def avro4s: TopicObjectOps.type = TopicObjectOps
  }

  implicit final class SchemaObjectOpsOps(
      private val x: Schema.type
  ) extends AnyVal {
    def avro4s: SchemaObjectOps.type = SchemaObjectOps
  }

  implicit final class GenericProducerAvro4sOps[F[_]](
      private val producer: ProducerApi[F, GenericRecord, GenericRecord]
  ) extends AnyVal {
    def toAvro4s[K: ToRecord, V: ToRecord]: ProducerApi[F, K, V] =
      Avro4sProducer[F, K, V](producer)
  }

  implicit final class GenericConsumerRecordAvro4sOps(
      private val cr: ConsumerRecord[GenericRecord, GenericRecord]
  ) extends AnyVal {
    def maybeKeyAs[K](implicit kfr: FromRecord[K]): Option[K] =
      cr.maybeKey.map(kfr.from)
    def maybeValueAs[V](implicit vfr: FromRecord[V]): Option[V] =
      cr.maybeValue.map(vfr.from)

    // note that these will probably throw NPE if key/value is null
    def keyAs[K](implicit kfr: FromRecord[K]): K = kfr.from(cr.key)
    def valueAs[V](implicit vfr: FromRecord[V]): V = vfr.from(cr.value)
  }

  implicit final class ProducerRecordAvro4sOps[K, V](
      private val pr: ProducerRecord[K, V]
  ) extends AnyVal {

    /** This only works when both key and value are non-null. */
    def toGenericRecord(implicit
        ktr: ToRecord[K],
        vtr: ToRecord[V],
    ): ProducerRecord[GenericRecord, GenericRecord] =
      pr.bimap(ktr.to, vtr.to)
  }
}
