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

package com.banno.kafka.schemaregistry

import org.apache.avro.Schema
import com.sksamuel.avro4s.{DefaultFieldMapper, SchemaFor}
import cats.FlatMap
import cats.syntax.all._

case class SchemaRegistryOps[F[_]](registry: SchemaRegistryApi[F]) {

  def keySubject(topic: String): String = topic + "-key"
  def valueSubject(topic: String): String = topic + "-value"

  def register[A](subject: String)(implicit SF: SchemaFor[A]): F[Int] =
    registry.register(subject, SF.schema(DefaultFieldMapper).asParsedSchema)

  def registerKey[K: SchemaFor](topic: String): F[Int] =
    register[K](keySubject(topic))

  def registerValue[V: SchemaFor](topic: String): F[Int] =
    register[V](valueSubject(topic))

  def register[K: SchemaFor, V: SchemaFor](topic: String)(implicit F: FlatMap[F]): F[(Int, Int)] =
    for {
      k <- registerKey[K](topic)
      v <- registerValue[V](topic)
    } yield (k, v)

  def isCompatible(subject: String, schema: Schema): F[Boolean] =
    registry.testCompatibility(subject, schema.asParsedSchema)

  def isCompatible[A](subject: String)(implicit SF: SchemaFor[A]): F[Boolean] =
    isCompatible(subject, SF.schema(DefaultFieldMapper))

  def isKeyCompatible[K: SchemaFor](topic: String): F[Boolean] =
    isCompatible[K](keySubject(topic))

  def isValueCompatible[V: SchemaFor](topic: String): F[Boolean] =
    isCompatible[V](valueSubject(topic))

  def isCompatible[K: SchemaFor, V: SchemaFor](
      topic: String
  )(implicit F: FlatMap[F]): F[(Boolean, Boolean)] =
    for {
      k <- isKeyCompatible[K](topic)
      v <- isValueCompatible[V](topic)
    } yield (k, v)

}
