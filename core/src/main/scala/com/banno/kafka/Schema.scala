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
import io.confluent.kafka.schemaregistry.ParsedSchema

trait Serialize[A] {
  def toBytes[F[_]: Sync](topic: TopicName, x: A): F[Array[Byte]]
}

object Serialize {
  def apply[A](implicit ev: Serialize[A]): Serialize[A] = ev
}

trait Deserialize[A] {
  def fromBytes[F[_]: Sync](topic: TopicName, x: Array[Byte]): F[A]
}

object Deserialize {
  def apply[A](implicit ev: Deserialize[A]): Deserialize[A] = ev
}

trait Serde[A] extends Serialize[A] with Deserialize[A]

object Serde {
  def apply[A](implicit ev: Serde[A]): Serde[A] = ev
}

trait HasParsedSchema[A] {
  def schema: ParsedSchema
}

object HasParsedSchema {
  def apply[A](implicit ev: HasParsedSchema[A]): HasParsedSchema[A] = ev
}

trait Schematic[A] extends HasParsedSchema[A] with Serde[A]

object Schematic {
  def apply[A](implicit ev: Schematic[A]): Schematic[A] = ev
}
