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

import _root_.vulcan.*
import cats.*
import cats.syntax.all.*
import com.banno.kafka.schemaregistry.*
import org.apache.avro.generic.GenericRecord
import scala.util.control.NoStackTrace

package object vulcan {
  implicit final class SchemaRegistryVulcanOpsOps[F[_]](
      private val r: SchemaRegistryApi[F]
  ) extends AnyVal {
    def vulcan: SchemaRegistryVulcanOps[F] =
      new SchemaRegistryVulcanOps(r)
  }

  implicit final class SchemaRegistryObjectVulcanOpsOps(
      private val x: SchemaRegistryApi.type
  ) extends AnyVal {
    def vulcan = SchemaRegistryApiObjectVulcanOps
  }

  implicit final class TopicObjectVulcanOpsOps(
      private val x: Topic.type
  ) extends AnyVal {
    def vulcan = TopicObjectVulcanOps
  }

  implicit final class SchemaObjectVulcanOpsOps(
      private val x: Schema.type
  ) extends AnyVal {
    def vulcan = SchemaObjectVulcanOps
  }

  implicit final class CodecObjectOpsOps(
      private val x: Codec.type
  ) extends AnyVal {
    def decodeGenericRecord[F[_]: ApplicativeThrow, A: Codec](
        x: GenericRecord
    ): F[A] =
      Codec
        .decode[A](x)
        .leftMap(_.throwable)
        .liftTo[F]

    def encodeGenericRecord[F[_]: MonadThrow, A: Codec](
        x: A
    ): F[GenericRecord] =
      Codec
        .encode[A](x)
        .leftMap(_.throwable)
        .liftTo[F]
        .flatMap {
          case x: GenericRecord => x.pure[F]
          case _ => UnableToDecodeToGenericRecord(x.getClass).raiseError
        }
  }
}

package vulcan {
  final case class UnableToDecodeToGenericRecord(
      cls: java.lang.Class[_]
  ) extends RuntimeException(s"Unable to decode $cls to a GenericRecord")
      with NoStackTrace
}
