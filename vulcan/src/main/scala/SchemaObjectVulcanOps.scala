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
package vulcan

import _root_.vulcan.*
import cats.*
import cats.syntax.all.*
import org.apache.avro.{Schema as JSchema}
import scala.util.*

object SchemaObjectVulcanOps {
  private def schema[F[_]: ApplicativeThrow, A: Codec]: F[JSchema] =
    Codec[A].schema
      .leftMap(_.throwable)
      .liftTo[F]

  def apply[F[_]: ApplicativeThrow, A: Codec]: F[Schema[A]] =
    Schema.tryInit(
      schema,
      Codec.decodeGenericRecord[Try, A](_),
      Codec.encodeGenericRecord[Try, A](_),
    )
}
