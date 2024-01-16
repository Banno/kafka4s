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
      .flatMap(producer.sendAsync)
      .void

  trait Builder[F[_], A <: Coproduct, B <: Coproduct] {
    type P <: HList
    def build(
        topics: Topics[A, B],
        producer: ProducerApi[F, GenericRecord, GenericRecord],
    ): P
  }

  @deprecated(
    "Exists for Publish.toMany, which fails with a match error on a singleton topic.  Will be removed in 6.x.",
    "5.0.6",
  )
  object Builder {
    implicit def buildNPublishers[
        F[_]: MonadThrow,
        K,
        V,
        X <: Coproduct,
        Y <: Coproduct,
    ](implicit buildTail: Builder[F, X, Y]) =
      new Builder[F, IncomingRecord[K, V] :+: X, (K, V) :+: Y] {
        override type P = T[F, (K, V)] :: buildTail.P

        override def build(
            topics: Topics[IncomingRecord[K, V] :+: X, (K, V) :+: Y],
            producer: ProducerApi[F, GenericRecord, GenericRecord],
        ): P = {
          val (topic, topicsTail) = Topics.uncons(topics)
          val head = to(topic, producer)
          val tail = buildTail.build(topicsTail, producer)
          head :: tail
        }
      }

    implicit def build0Publishers[F[_]] =
      new Builder[F, CNil, CNil] {
        type P = HNil
        override def build(
            topics: Topics[CNil, CNil],
            producer: ProducerApi[F, GenericRecord, GenericRecord],
        ): P = HNil
      }
  }

  @deprecated(
    "Fails with a match error on a singleton topic.  Will be removed in 6.x.",
    "5.0.6",
  )
  def toMany[F[_], A <: Coproduct, B <: Coproduct](
      topics: Topics[A, B],
      producer: ProducerApi[F, GenericRecord, GenericRecord],
  )(implicit builder: Builder[F, A, B]): builder.P =
    builder.build(topics, producer)
}
