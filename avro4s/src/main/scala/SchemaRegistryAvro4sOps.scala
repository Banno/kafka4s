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

package com.banno.kafka.avro4s

import cats.*
import cats.effect.*
import cats.syntax.all.*
import com.banno.kafka.schemaregistry.*
import com.sksamuel.avro4s.*

final class SchemaRegistryAvro4sOps[F[_]](
    private val registry: SchemaRegistryApi[F]
) extends AnyVal {
  def register[A](subject: String)(implicit SF: SchemaFor[A]): F[Int] =
    registry.register(subject, SF.schema(DefaultFieldMapper).asParsedSchema)

  def registerKey[K: SchemaFor](topic: String): F[Int] =
    register[K](registry.keySubject(topic))

  def registerValue[V: SchemaFor](topic: String): F[Int] =
    register[V](registry.valueSubject(topic))

  def register[K: SchemaFor, V: SchemaFor](
      topic: String
  )(implicit F: FlatMap[F]): F[(Int, Int)] =
    for {
      k <- registerKey[K](topic)
      v <- registerValue[V](topic)
    } yield (k, v)

  def isCompatible[A](subject: String)(implicit SF: SchemaFor[A]): F[Boolean] =
    registry.isCompatible(subject, SF.schema(DefaultFieldMapper))

  def isKeyCompatible[K: SchemaFor](topic: String): F[Boolean] =
    isCompatible[K](registry.keySubject(topic))

  def isValueCompatible[V: SchemaFor](topic: String): F[Boolean] =
    isCompatible[V](registry.valueSubject(topic))

  def isCompatible[K: SchemaFor, V: SchemaFor](
      topic: String
  )(implicit F: FlatMap[F]): F[(Boolean, Boolean)] =
    for {
      k <- isKeyCompatible[K](topic)
      v <- isValueCompatible[V](topic)
    } yield (k, v)
}

object SchemaRegistryApiObjectAvro4sOps {
  def register[F[_]: Sync, K: SchemaFor, V: SchemaFor](
      baseUrl: String,
      topic: String,
      configs: Map[String, Object] = Map.empty,
  ) =
    for {
      schemaRegistry <- SchemaRegistryApi(baseUrl, configs)
      k <- schemaRegistry.avro4s.registerKey[K](topic)
      v <- schemaRegistry.avro4s.registerValue[V](topic)
    } yield (k, v)
}
