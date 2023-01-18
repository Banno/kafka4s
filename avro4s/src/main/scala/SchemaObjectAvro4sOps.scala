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
package avro4s

import com.sksamuel.avro4s.*
import org.apache.avro.{Schema as JSchema}
import org.apache.avro.generic.GenericRecord
import scala.util.*

object SchemaObjectAvro4sOps {
  private def fromGeneric[A](
      gr: GenericRecord
  )(implicit FR: FromRecord[A]): Try[A] =
    Try(FR.from(gr))

  private def toGeneric[A](
      x: A
  )(implicit TR: ToRecord[A]): GenericRecord =
    TR.to(x)

  private def schema[A](implicit SF: SchemaFor[A]): JSchema =
    SF.schema(DefaultFieldMapper)

  def apply[A: FromRecord: ToRecord: SchemaFor]: Schema[A] =
    Schema(
      schema,
      fromGeneric(_),
      toGeneric(_),
    )
}
