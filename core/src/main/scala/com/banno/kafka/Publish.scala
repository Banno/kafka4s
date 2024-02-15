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

import cats.*
import cats.syntax.all.*
import com.banno.kafka.*
import com.banno.kafka.producer.ProducerApi
import org.apache.avro.generic.GenericRecord
import shapeless.*

object Publish {
  type T[F[_], A] = A => F[Unit]
  type KV[F[_], K, V] = T[F, (K, V)]

  def to[F[_]: MonadThrow, A, B](
      topical: Topical[A, B],
      producer: ProducerApi[F, GenericRecord, GenericRecord],
  ): T[F, B] = kv =>
    topical
      .coparse(kv)
      .liftTo[F]
      .flatMap(producer.send)
      .flatten
      .void

  def many[F[_]: Parallel: MonadThrow, A, B](
      topical: Topical[A, B],
      producer: ProducerApi[F, GenericRecord, GenericRecord],
  ): List[B] => F[Unit] =
    _.traverse(kv =>
      topical
        .coparse(kv)
        .liftTo[F]
        .flatMap(producer.send)
    ).flatMap(_.parSequence_)

  trait Builder[F[_], A <: Coproduct, B <: Coproduct] {
    type P <: HList
    def build(
        topics: Topics[A, B],
        producer: ProducerApi[F, GenericRecord, GenericRecord],
    ): P
  }
}
