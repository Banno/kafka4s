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

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.{Schema as JSchema}
import org.apache.avro.generic.GenericRecord
import scala.util.Try

final case class Schema[A](
    ast: JSchema, // Abstract Syntax Tree
    parse: GenericRecord => Try[A],
    unparse: A => Try[GenericRecord],
) {
  def parsed: ParsedSchema =
    new AvroSchema(ast)
}
