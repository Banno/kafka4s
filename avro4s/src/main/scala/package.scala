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

import cats.syntax.all.*
import com.banno.kafka.producer.*
import com.banno.kafka.schemaregistry.*
import com.sksamuel.avro4s.*
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import scala.util.*

package object avro4s {
  implicit final class SchemaRegistryAvro4sOpsOps[F[_]](
      private val r: SchemaRegistryApi[F]
  ) extends AnyVal {
    def avro4s: SchemaRegistryAvro4sOps[F] =
      new SchemaRegistryAvro4sOps(r)
  }

  implicit final class SchemaRegistryObjectAvro4sOpsOps(
      private val x: SchemaRegistryApi.type
  ) extends AnyVal {
    def avro4s = SchemaRegistryApiObjectAvro4sOps
  }

  implicit final class TopicObjectAvro4sOpsOps(
      private val x: Topic.type
  ) extends AnyVal {
    def avro4s = TopicObjectAvro4sOps
  }

  implicit final class SchemaObjectAvro4sOpsOps(
      private val x: Schema.type
  ) extends AnyVal {
    def avro4s = SchemaObjectAvro4sOps
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
      cr.maybeKey.flatMap(k => Try(kfr.from(k)).toOption)
    def maybeValueAs[V](implicit vfr: FromRecord[V]): Option[V] =
      cr.maybeValue.flatMap(v => Try(vfr.from(v)).toOption)

    def keyAs[K](implicit kfr: FromRecord[K]): Try[K] = Try(kfr.from(cr.key))
    def valueAs[V](implicit vfr: FromRecord[V]): Try[V] =
      Try(vfr.from(cr.value))
  }

  implicit final class ProducerRecordAvro4sOps[K, V](
      private val pr: ProducerRecord[K, V]
  ) extends AnyVal {

    /** This only works when both key and value are non-null. */
    def toGenericRecord(implicit
        ktr: ToRecord[K],
        vtr: ToRecord[V],
    ): Try[ProducerRecord[GenericRecord, GenericRecord]] =
      pr.bitraverse(k => Try(ktr.to(k)), v => Try(vtr.to(v)))
  }
}
