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

import scala.util._
import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry._
import io.confluent.kafka.schemaregistry.avro.AvroSchema

package object avro4s {
  implicit def schematic[A](implicit
    T: ToRecord[A],
    F: FromRecord[A],
    S: SchemaFor[A],
  ): Schematic[A] =
    new Schematic[A] {
      override def fromByteArray(x: Array[Byte]): Try[A] = {
        Try(F.from(null)) // TODO
      }

      override def schema: ParsedSchema =
        new AvroSchema(S.schema(DefaultFieldMapper))

      override def toByteArray(x: A): Array[Byte] = {
        val _ = T.to(x)
        ???
      }
    }
}
