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

import cats.effect._
import cats.syntax.all._
import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry._
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.serializers._;
import org.apache.avro.generic.GenericRecord
import scala.jdk.CollectionConverters._

package avro4s {
  object Avro4sSchematic {
    sealed trait Factory {
      def schematic[A: ToRecord: FromRecord: SchemaFor]: Schematic[A]
    }

    final case class UnexpectedTypeFromDeserializer(
      `class`: Class[_]
    ) extends RuntimeException(
      s"Expected `GenericRecord` from deserializer but got `${`class`}` instead"
    )

    private def apply[A](
      deserializer: KafkaAvroDeserializer,
      serializer: KafkaAvroSerializer,
    )(implicit
      T: ToRecord[A],
      F: FromRecord[A],
      S: SchemaFor[A],
    ): Schematic[A] =
      new Schematic[A] {
        override def fromBytes[F[_]: Sync](name: TopicName, bytes: Array[Byte]): F[A] =
          for {
            obj <- Sync[F].delay(deserializer.deserialize(name.show, bytes))
            record <- obj match {
              case r: GenericRecord => r.pure[F]
              case r =>
                UnexpectedTypeFromDeserializer(r.getClass())
                  .raiseError[F, GenericRecord]
            }
            a <- Sync[F].delay(F.from(record))
          } yield a

        override def schema: ParsedSchema =
          new AvroSchema(S.schema(DefaultFieldMapper))

        override def toBytes[F[_]: Sync](name: TopicName, x: A): F[Array[Byte]] =
          for {
            record <- Sync[F].delay(T.to(x))
            bytes <- Sync[F].delay(serializer.serialize(name.show, record))
          } yield bytes
      }

    def apply[F[_]: Sync](
      serconfigs: Map[String, ?],
      deconfigs: Map[String, ?],
    ): F[Factory] =
      for {
        // TODO schema registry client---what's the right thing?
        deserializer <- Sync[F].delay(new KafkaAvroDeserializer())
        // It's called "isKey", but if I can read Java, is unused. It is enforced
        // by the `Deserializer` interface.
        seemsUnused = false
        _ <- Sync[F].delay(deserializer.configure(deconfigs.asJava, seemsUnused))
        serializer <- Sync[F].delay(new KafkaAvroSerializer())
        _ <- Sync[F].delay(serializer.configure(serconfigs.asJava, seemsUnused))
        factory = new Factory {
          override def schematic[A: ToRecord: FromRecord: SchemaFor]: Schematic[A] =
            apply(deserializer, serializer)
        }
      } yield factory

    def default[F[_]: Sync]: F[Factory] =
      apply(Map.empty, Map.empty)
  }
}

package object avro4s {
  implicit def schematic[A: ToRecord: FromRecord: SchemaFor](
    implicit F: Avro4sSchematic.Factory
  ): Schematic[A] = F.schematic
}
