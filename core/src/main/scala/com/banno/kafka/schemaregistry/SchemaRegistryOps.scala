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
package schemaregistry

import cats._
import cats.syntax.all._

case class SchemaRegistryOps[F[_]](registry: SchemaRegistryApi[F]) {

  def keySubject(topic: String): String = topic + "-key"
  def valueSubject(topic: String): String = topic + "-value"

  def registerKey[K: HasParsedSchema](topic: String): F[Int] =
    registry.register(keySubject(topic), HasParsedSchema[K].schema)

  def registerValue[V: HasParsedSchema](topic: String): F[Int] =
    registry.register(valueSubject(topic), HasParsedSchema[V].schema)

  def register[K: HasParsedSchema, V: HasParsedSchema](topic: String)(implicit F: FlatMap[F]): F[(Int, Int)] =
    for {
      k <- registerKey[K](topic)
      v <- registerValue[V](topic)
    } yield (k, v)
}
